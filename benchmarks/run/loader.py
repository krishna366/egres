"""The load: a workload's table filled through parallel psql streams,
with readers alongside, progress published as Prometheus metrics.

The id range is split into one contiguous slice per stream; each stream
inserts its slice in chunks, one `generator | psql` pipeline per chunk,
so progress is known to the chunk and a failed chunk is retried on its
own. Every few seconds the driver writes `load.prom` into the textfile
dir, where node exporter picks it up:

    egres_load_rows_total          rows committed so far
    egres_load_target_rows         rows in the whole range
    egres_load_streams_running     streams still working
    egres_load_chunks_failed_total chunks that failed all retries

The load stops early once the data dir passes the env's stop_bytes, or
when the file `STOP` appears in the output dir; running chunks finish
first.

`--resume` continues a load stopped that way: it reads load.log for the
last "load start" and each stream's "done at id" line after it, and
starts every stream after its last committed id.

The readers: the env's `readers` pgbench clients on the workload's
read-only scripts for the whole load, in rounds of ten minutes each
started with `-D rows=<rows loaded so far>` so the ids they draw exist.
Their progress goes to results/readers.log and readers.prom under
run="load-readers". Reads during writes are part of the measurement:
work a server defers to a later pass is paid here.
"""

import fcntl
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

import pgbench

HERE = Path(__file__).resolve().parent
WORKLOADS_PY = HERE / "workloads.py"
READER_ROUND_SECS = 600


def utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class Progress:
    def __init__(self):
        self.lock = threading.Lock()
        self.rows = 0
        self.failed = 0
        self.running = 0
        self.stop = False
        self.threads: list = []

    def loading(self) -> bool:
        return any(t.is_alive() for t in self.threads)


def dir_bytes(root: Path) -> int:
    total = 0
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            try:
                total += os.lstat(os.path.join(dirpath, name)).st_size
            except OSError:
                pass
    return total


def run_chunk(run, workload: str, a: int, b: int, log: Path) -> bool:
    """One chunk: generate SQL for ids [a, b] and pipe it to psql."""
    with open(log, "ab") as lf:
        g = subprocess.Popen([sys.executable, str(WORKLOADS_PY), workload,
                              str(a), str(b)], stdout=subprocess.PIPE)
        # A 64 KiB pipe is smaller than one 200-row statement, so the
        # generator and psql would take turns; 1 MiB lets the generator
        # run a few statements ahead while the server works.
        try:
            fcntl.fcntl(g.stdout.fileno(), fcntl.F_SETPIPE_SZ, 1 << 20)
        except (OSError, AttributeError):
            pass
        rc = subprocess.run(
            [str(run.psql), run.conn, "-X", "-q", "-v", "ON_ERROR_STOP=1"],
            stdin=g.stdout, stdout=lf, stderr=subprocess.STDOUT,
        ).returncode
        g.stdout.close()
        g.wait()
    return rc == 0 and g.returncode == 0


def slices(first: int, last: int, streams: int):
    total = last - first + 1
    per = (total + streams - 1) // streams
    out = []
    for i in range(streams):
        a = first + i * per
        b = min(a + per - 1, last)
        if a > last:
            break
        out.append((i, a, b))
    return out


def resume_points(events: Path, first: int, last: int, streams: int):
    """Per stream, the id after its last committed one, from the last
    "load start" section of load.log; and the rows already loaded."""
    lines = events.read_text().splitlines()
    starts = [i for i, l in enumerate(lines)
              if " load start " in l or " load resume " in l]
    if not starts:
        sys.exit("resume: no load start in load.log")
    done = {}
    for line in lines[starts[-1]:]:
        m = re.search(r" stream (\d+) done at id (\d+)", line)
        if m:
            done[int(m.group(1))] = int(m.group(2))
    points, rows = [], 0
    for i, a, b in slices(first, last, streams):
        if i not in done:
            sys.exit(f"resume: stream {i} has no 'done at id' line; "
                     "the stopped run did not stop cleanly")
        rows += done[i] - a + 1
        points.append((i, done[i] + 1, b))
    return points, rows


def stream(run, workload: str, idx: int, a: int, b: int, chunk: int,
           prog: Progress):
    log = run.results / f"load-{idx}.log"
    events = run.out / "load.log"
    rid = a
    while rid <= b and not prog.stop:
        hi = min(rid + chunk - 1, b)
        ok = False
        for attempt in range(1, 4):
            if run_chunk(run, workload, rid, hi, log):
                ok = True
                break
            with open(events, "a") as ef:
                ef.write(f"{utc()} stream {idx} chunk {rid}..{hi} "
                         f"failed attempt {attempt}\n")
            time.sleep(5 * attempt)
        with prog.lock:
            if ok:
                prog.rows += hi - rid + 1
            else:
                prog.failed += 1
        if not ok:
            # A chunk that fails three times is left out; ids stay
            # unique so the rest of the load is still valid.
            with open(events, "a") as ef:
                ef.write(f"{utc()} stream {idx} chunk {rid}..{hi} "
                         f"given up\n")
        rid = hi + 1
    with prog.lock:
        prog.running -= 1
    with open(events, "a") as ef:
        ef.write(f"{utc()} stream {idx} done at id {rid - 1}\n")


def readers(run, prog: Progress, total: int):
    """Rounds of pgbench over the rows loaded so far until the load is
    over. The first round waits for a first slice of rows, so the ids
    it draws exist."""
    n = run.env["readers"]
    log = run.results / "readers.log"
    events = run.out / "load.log"
    min_rows = min(1_000_000, total // 10)
    while not prog.stop and prog.loading():
        with prog.lock:
            rows = prog.rows
        if rows < min_rows:
            time.sleep(10)
            continue
        h = pgbench.start(run, log, "readers", "load-readers", n,
                          READER_ROUND_SECS, run.env["workload"]["reads"],
                          {"rows": rows})
        with open(events, "a") as ef:
            ef.write(f"{utc()} readers round rows={rows} pid={h.pid}\n")
        # A round ends with the load, not ten minutes after it.
        pgbench.follow(h, stop=lambda: prog.stop or not prog.loading())


def write_prom(path: Path, prog: Progress, target: int):
    with prog.lock:
        body = (
            "# TYPE egres_load_rows_total counter\n"
            f"egres_load_rows_total {prog.rows}\n"
            "# TYPE egres_load_target_rows gauge\n"
            f"egres_load_target_rows {target}\n"
            "# TYPE egres_load_streams_running gauge\n"
            f"egres_load_streams_running {prog.running}\n"
            "# TYPE egres_load_chunks_failed_total counter\n"
            f"egres_load_chunks_failed_total {prog.failed}\n"
        )
    tmp = path.with_suffix(".tmp")
    tmp.write_text(body)
    os.replace(tmp, path)


def create_table(run, workload: str):
    import workloads
    rc = subprocess.run(
        [str(run.psql), run.conn, "-X", "-q", "-v", "ON_ERROR_STOP=1"],
        input=workloads.WORKLOADS[workload]["ddl"], text=True).returncode
    if rc != 0:
        sys.exit("create table failed")


def run_load(run, resume: bool):
    cfg = run.env["load"]
    workload = run.env["workload"]["name"]
    # Its own pid: the watchdog ends the load phase when it exits.
    (run.out / "load.pid").write_text(f"{os.getpid()}\n")
    run.results.mkdir(parents=True, exist_ok=True)
    run.textfile.mkdir(parents=True, exist_ok=True)
    events = run.out / "load.log"
    total = cfg["last"] - cfg["first"] + 1
    prog = Progress()
    if resume:
        points, prog.rows = resume_points(events, cfg["first"], cfg["last"],
                                          cfg["streams"])
        with open(events, "a") as ef:
            ef.write(f"{utc()} load resume rows={prog.rows} from "
                     + " ".join(f"{i}:{a}" for i, a, _ in points) + "\n")
    else:
        create_table(run, workload)
        points = slices(cfg["first"], cfg["last"], cfg["streams"])
        with open(events, "a") as ef:
            ef.write(f"{utc()} load start ids {cfg['first']}..{cfg['last']} "
                     f"streams={cfg['streams']} chunk={cfg['chunk']}\n")
    for i, a, b in points:
        prog.running += 1
        t = threading.Thread(target=stream,
                             args=(run, workload, i, a, b, cfg["chunk"],
                                   prog),
                             daemon=True)
        t.start()
        prog.threads.append(t)
    reader = None
    if run.env["readers"] > 0:
        reader = threading.Thread(target=readers, args=(run, prog, total),
                                  daemon=True)
        reader.start()

    prom = run.textfile / "load.prom"
    t0 = time.time()
    last_log = 0.0
    last_rows = 0
    while prog.loading():
        write_prom(prom, prog, total)
        now = time.time()
        if now - last_log >= 60:
            with prog.lock:
                rows = prog.rows
            rate = (rows - last_rows) / max(now - last_log, 1)
            size = dir_bytes(run.data)
            with open(events, "a") as ef:
                ef.write(f"{utc()} rows={rows} rate={rate:.0f}/s "
                         f"data={size / 2**30:.1f}GiB "
                         f"streams={prog.running} failed={prog.failed}\n")
            last_log, last_rows = now, rows
            if cfg["stop_bytes"] and size >= cfg["stop_bytes"]:
                prog.stop = True
                with open(events, "a") as ef:
                    ef.write(f"{utc()} stop: data dir reached "
                             f"{cfg['stop_bytes']} bytes\n")
        if (run.out / "STOP").exists() and not prog.stop:
            prog.stop = True
            with open(events, "a") as ef:
                ef.write(f"{utc()} stop: STOP file\n")
        time.sleep(5)
    write_prom(prom, prog, total)
    if reader is not None:
        reader.join()
    with open(events, "a") as ef:
        ef.write(f"{utc()} load done rows={prog.rows} failed={prog.failed} "
                 f"in {time.time() - t0:.0f}s\n")
