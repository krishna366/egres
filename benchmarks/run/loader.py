# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

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
    egres_load_insert_seconds      histogram: one INSERT statement's
                                   round trip as psql timed it

Insert latency is per statement (ROWS_PER_STMT rows), measured by the
client: psql runs with `\timing on` and its "Time:" lines, kept in the
stream's log, are read back after each chunk. The time starts when the
whole statement is in psql's hands, so the generator is not in it.

The load stops early once the data dir passes the env's stop_bytes or
the load has run for its stop_secs (when it has either), or when the
file `STOP` appears in the output dir; running chunks finish first.

When the load ends, `load-summary.md` and `.json` in the output dir
say what it cost: rows and rate, insert and reader latency
percentiles, and the bytes each disk read and wrote meanwhile (see
iostat.py), which is also logged per minute for the data dir's disk.

`--resume` continues a load stopped that way: it reads load.log for the
last "load start" and each stream's "done at id" line after it, and
starts every stream after its last committed id. A load that was
killed (`bench.py stop`) logged no such lines; its streams' last ids
are then found on the server (`last_loaded`).

`--set load.create=false` with a new `load.first` and `load.last`
loads a further id range into the table an earlier load made.

The readers: the workload's reader groups, each a pgbench of its own
on its own scripts for the whole load (a group per kind of query, so a
scan that takes minutes does not hold the clients that should be
doing point reads). A group runs in rounds of a minute, each started
with the ids loaded so far (`pgbench.id_vars`) so the ids they draw
exist. A round is left to end on its own, the last one too: pgbench
writes its latency log through a buffer and has no flush, so a killed
round loses its last transactions, which for a slow script is all of
them. A group's progress goes to results/readers-<group>.log and
readers-<group>.prom under run="load-<group>", each transaction's
latency to results/latency/. Reads during writes are part of the
measurement: work a server defers to a later pass is paid here.

After the load the groups run on for the load's `after_secs` over
every id, so the summary also shows how reads settle once the
writes stop, per five-minute slice (`Readers after the load`).
"""

import array
import fcntl
import glob
import json
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

import iostat
import pgbench

HERE = Path(__file__).resolve().parent
WORKLOADS_PY = HERE / "workloads.py"
READER_ROUND_SECS = 60
GIB = 2**30
# psql prints one of these per statement under \timing; a time of a
# second or more carries a clock form after it, which is not needed.
TIME_LINE = re.compile(rb"^Time: ([\d.]+) ms", re.M)


def utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class Latency:
    """Insert statement latencies, in milliseconds: all of them for the
    final percentiles (4 bytes each, a few million), the ones since
    the last log line, and histogram buckets for Prometheus."""
    BUCKETS_MS = (1, 2, 5, 10, 15, 20, 25, 30, 40, 50, 75, 100, 150, 200,
                  300, 500, 1000, 2000, 5000, 10000, 30000)

    def __init__(self):
        self.all = array.array("f")
        self.window: list = []
        self.counts = [0] * len(self.BUCKETS_MS)
        self.sum_ms = 0.0

    def add(self, ms: float):
        self.all.append(ms)
        self.window.append(ms)
        self.sum_ms += ms
        for i, le in enumerate(self.BUCKETS_MS):
            if ms <= le:
                self.counts[i] += 1
                break

    def prom(self) -> str:
        body = "# TYPE egres_load_insert_seconds histogram\n"
        seen = 0
        for le, n in zip(self.BUCKETS_MS, self.counts):
            seen += n
            body += (f'egres_load_insert_seconds_bucket{{le="{le / 1000}"}}'
                     f" {seen}\n")
        body += (f'egres_load_insert_seconds_bucket{{le="+Inf"}} '
                 f"{len(self.all)}\n"
                 f"egres_load_insert_seconds_sum {self.sum_ms / 1000}\n"
                 f"egres_load_insert_seconds_count {len(self.all)}\n")
        return body


def spread(vals) -> dict:
    """n, p50, p95, p99, p999 and max of a list of latencies."""
    ordered = sorted(vals)
    if not ordered:
        return {"n": 0}
    out = {"n": len(ordered), "max": ordered[-1]}
    for key, pct in (("p50", 50), ("p95", 95), ("p99", 99), ("p999", 99.9)):
        out[key] = pgbench.percentile(ordered, pct)
    return out


class Progress:
    def __init__(self):
        self.lock = threading.Lock()
        self.rows = 0
        self.failed = 0
        self.running = 0
        self.stop = False
        # When the last stream finished; the readers' settle phase
        # counts from it.
        self.load_end = None
        self.threads: list = []
        self.insert = Latency()
        # Per stream: its slice's first id and the last id it has
        # committed. `per` is what the readers may draw from.
        self.first: dict = {}
        self.done: dict = {}

    def loading(self) -> bool:
        return any(t.is_alive() for t in self.threads)

    def reading(self, after_secs: float) -> bool:
        """Whether the readers start another round: during the load,
        and for `after_secs` past its end."""
        if self.stop:
            return False
        if self.loading() or self.load_end is None:
            return True
        return time.time() - self.load_end < after_secs

    def per(self) -> int:
        """Ids loaded in every slice: the shortest loaded prefix."""
        if not self.first:
            return 0
        return min(self.done[i] - a + 1 for i, a in self.first.items())


def server_mem(pid) -> int:
    """Resident bytes of the server: the process and its children (a
    Postgres postmaster's backends), private memory summed and shared
    memory counted once. Page cache is not in it. 0 when unreadable."""
    if not pid:
        return 0
    anon, shared = 0, 0
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            text = (proc / "status").read_text()
        except OSError:
            continue
        f = dict(line.split(":", 1) for line in text.splitlines()
                 if ":" in line)

        def kb(key):
            return int(f.get(key, "0").split()[0]) * 1024

        if int(proc.name) != pid and \
                int(f.get("PPid", "0").split()[0]) != pid:
            continue
        anon += kb("RssAnon")
        shared = max(shared, kb("RssFile") + kb("RssShmem"))
    return anon + shared


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
        # \timing first, then the statements from stdin: the log gets
        # a "Time:" line per statement. A C numeric locale keeps the
        # decimal point a point.
        rc = subprocess.run(
            [str(run.psql), run.conn, "-X", "-q", "-v", "ON_ERROR_STOP=1",
             "-c", "\\timing on", "-f", "-"],
            stdin=g.stdout, stdout=lf, stderr=subprocess.STDOUT,
            env={**os.environ, "LC_NUMERIC": "C"},
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


def last_loaded(run, workload: str, a: int, b: int) -> int:
    """The last id of [a, b] the server holds, a - 1 when it holds
    none. A stream loads its slice in id order, so the ids present are
    a prefix of the slice, and a binary search of point lookups finds
    where it ends."""
    import workloads
    query = workloads.WORKLOADS[workload]["exists"]

    def present(rid: int) -> bool:
        res = subprocess.run(
            [str(run.psql), run.conn, "-X", "-q", "-t", "-A", "-c",
             query.format(id=rid)], capture_output=True, text=True)
        if res.returncode != 0:
            sys.exit(f"resume: lookup of id {rid} failed: "
                     f"{res.stderr.strip()}")
        return bool(res.stdout.strip())

    lo, hi = a - 1, b
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if present(mid):
            lo = mid
        else:
            hi = mid - 1
    return lo


def resume_points(events: Path, first: int, last: int, streams: int,
                  probe=None):
    """Per stream, the id after its last committed one, from the last
    "load start" section of load.log; and the rows already loaded. A
    stream with no "done at id" line (the load was killed) gets its
    last id from `probe(a, b)`, when there is one."""
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
    # A chunk that was given up left a hole: the ids present are then
    # not a prefix, which is what the probe searches for.
    holes = any(" given up" in line for line in lines[starts[-1]:])
    points, rows = [], 0
    for i, a, b in slices(first, last, streams):
        if i not in done:
            if probe is None or holes:
                sys.exit(f"resume: stream {i} has no 'done at id' line; "
                         "the stopped run did not stop cleanly")
            done[i] = probe(a, b)
        rows += done[i] - a + 1
        points.append((i, done[i] + 1, b))
    return points, rows


def new_times(log: Path, pos: int):
    """The statement times psql logged past byte `pos`, and the new
    position. A failed attempt's statements count too: they ran."""
    try:
        with open(log, "rb") as f:
            f.seek(pos)
            chunk = f.read()
    except OSError:
        return [], pos
    return ([float(m) for m in TIME_LINE.findall(chunk)],
            pos + len(chunk))


def stream(run, workload: str, idx: int, a: int, b: int, chunk: int,
           prog: Progress):
    log = run.results / f"load-{idx}.log"
    events = run.out / "load.log"
    rid = a
    # Only this run's lines: a resumed load appends to the same log.
    log_pos = log.stat().st_size if log.exists() else 0
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
        times, log_pos = new_times(log, log_pos)
        with prog.lock:
            for ms in times:
                prog.insert.add(ms)
            if ok:
                prog.rows += hi - rid + 1
                prog.done[idx] = hi
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


def readers(run, prog: Progress, total: int, group: dict):
    """One reader group: rounds of pgbench over the rows loaded so far
    until the load is over. The first round waits for a first slice of
    rows, so the ids it draws exist."""
    name = group["name"]
    n = run.env["readers"][name]
    log = run.results / f"readers-{name}.log"
    events = run.out / "load.log"
    min_rows = min(1_000_000, total // 10)
    after = run.env["load"].get("after_secs") or 0
    round_no = 0
    while prog.reading(after):
        with prog.lock:
            rows, per = prog.rows, prog.per()
        # Every slice needs rows before a script may draw from it.
        if rows < min_rows or per == 0:
            time.sleep(10)
            continue
        round_no += 1
        h = pgbench.start(run, log, f"readers-{name}", f"load-{name}", n,
                          READER_ROUND_SECS, group["scripts"],
                          pgbench.id_vars(run.env["load"], rows, per),
                          reader_prefix(run, name, round_no),
                          group.get("sample", 1.0))
        with open(events, "a") as ef:
            ef.write(f"{utc()} readers {name} round rows={rows} per={per} "
                     f"pid={h.pid}\n")
        # Never killed: however the load ends, the round finishes what
        # it is doing, so its log is whole.
        pgbench.follow(h)


def reader_prefix(run, group: str, round_no) -> str:
    """The latency log prefix of one round of a reader group; `*`
    globs the group's rounds. A resumed load's rounds get their own
    names by the time."""
    d = run.results / "latency"
    if round_no == "*":
        return str(d / f"readers-{group}-*")
    return str(d / f"readers-{group}-{int(time.time())}-{round_no:03d}")


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
        ) + prog.insert.prom()
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
        points, prog.rows = resume_points(
            events, cfg["first"], cfg["last"], cfg["streams"],
            lambda a, b: last_loaded(run, workload, a, b))
        with open(events, "a") as ef:
            ef.write(f"{utc()} load resume rows={prog.rows} from "
                     + " ".join(f"{i}:{a}" for i, a, _ in points) + "\n")
    else:
        if cfg.get("create", True):
            create_table(run, workload)
        points = slices(cfg["first"], cfg["last"], cfg["streams"])
        with open(events, "a") as ef:
            ef.write(f"{utc()} load start ids {cfg['first']}..{cfg['last']} "
                     f"streams={cfg['streams']} chunk={cfg['chunk']}\n")
    firsts = {i: a for i, a, _ in slices(cfg["first"], cfg["last"],
                                         cfg["streams"])}
    for i, a, b in points:
        prog.running += 1
        prog.first[i] = firsts[i]
        prog.done[i] = a - 1
        t = threading.Thread(target=stream,
                             args=(run, workload, i, a, b, cfg["chunk"],
                                   prog),
                             daemon=True)
        t.start()
        prog.threads.append(t)
    reader_threads = []
    for group in run.env["workload"]["reads"]:
        if run.env["readers"][group["name"]] > 0:
            t = threading.Thread(target=readers,
                                 args=(run, prog, total, group), daemon=True)
            t.start()
            reader_threads.append(t)

    prom = run.textfile / "load.prom"
    t0 = time.time()
    rows0 = prog.rows
    data_disks = iostat.device_of(run.data)
    io0 = io_snapshot(run)
    io_prev = io0
    last_log = t0
    last_rows = rows0
    mem = mem_peak = 0
    while prog.loading():
        write_prom(prom, prog, total)
        now = time.time()
        mem = server_mem(run.pid("server.pid"))
        mem_peak = max(mem_peak, mem)
        if now - last_log >= 60:
            with prog.lock:
                rows = prog.rows
                window, prog.insert.window = prog.insert.window, []
            rate = (rows - last_rows) / max(now - last_log, 1)
            size = dir_bytes(run.data)
            ins = spread(window)
            io_now = io_snapshot(run)
            io = iostat.delta(io_prev, io_now)
            io_prev = io_now
            mib = 2**20 * max(io["secs"], 1)
            rd = sum(io["disks"].get(d, (0, 0))[0] for d in data_disks)
            wr = sum(io["disks"].get(d, (0, 0))[1] for d in data_disks)
            with open(events, "a") as ef:
                ef.write(f"{utc()} rows={rows} rate={rate:.0f}/s "
                         f"data={size / GIB:.1f}GiB "
                         f"streams={prog.running} failed={prog.failed}"
                         + (f" insert_ms p50={ins['p50']:.1f} "
                            f"p95={ins['p95']:.1f} "
                            f"p99={ins['p99']:.1f} max={ins['max']:.1f}"
                            if ins["n"] else "")
                         + f" disk_MiBps read={rd / mib:.0f} "
                           f"write={wr / mib:.0f} "
                           f"mem={mem / GIB:.1f}GiB"
                         + cpu_text(io["cpu"]) + "\n")
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
        if cfg.get("stop_secs") and now - t0 >= cfg["stop_secs"] and \
                not prog.stop:
            prog.stop = True
            with open(events, "a") as ef:
                ef.write(f"{utc()} stop: ran {cfg['stop_secs']} s\n")
        time.sleep(5)
    # The load's own span: the snapshot before the readers wind down.
    io1 = io_snapshot(run)
    secs = time.time() - t0
    prog.load_end = time.time()
    write_prom(prom, prog, total)
    # `per` lets a later workload run draw only ids that exist.
    with open(events, "a") as ef:
        ef.write(f"{utc()} load done rows={prog.rows} failed={prog.failed} "
                 f"per={prog.per()} in {secs:.0f}s\n")
    summarize(run, prog, prog.rows - rows0, t0, secs,
              iostat.delta(io0, io1), {"peak": mem_peak, "end": mem})

    # The settle phase: the readers go on while the server catches up
    # on the load's deferred work. A STOP file ends it early; a round
    # in progress still finishes.
    after = cfg.get("after_secs") or 0
    if after and reader_threads and not prog.stop:
        with open(events, "a") as ef:
            ef.write(f"{utc()} readers settle for {after} s\n")
        while prog.reading(after):
            if (run.out / "STOP").exists():
                prog.stop = True
                with open(events, "a") as ef:
                    ef.write(f"{utc()} stop: STOP file\n")
            time.sleep(5)
    for t in reader_threads:
        t.join()
    if after and reader_threads:
        summarize_after(run, prog.load_end, time.time(),
                        iostat.delta(io1, io_snapshot(run)))


def io_snapshot(run) -> dict:
    """Disk and CPU counters now. The server's CPU comes from its
    cgroup under a memory cap (`bench.py` records the path), else from
    its process when the server is one process (Egres)."""
    cg = run.out / "cgroup.path"
    cgroup = cg.read_text().strip() if cg.exists() else None
    return iostat.snapshot(run.pid("server.pid"), cgroup,
                           run.kind == "egres")


def cpu_text(cpu: dict) -> str:
    """` cpu host=12.3/32 server=8.1` cores, server `-` when unknown."""
    server = cpu["server_cores"]
    return (f" cpu host={cpu['host_cores_busy']:.1f}/{cpu['host_cores']}"
            f" server={server:.1f}" if server is not None else
            f" cpu host={cpu['host_cores_busy']:.1f}/{cpu['host_cores']}"
            " server=-")


def summarize_after(run, since: float, until: float, io: dict):
    """Appends `Readers after the load` to load-summary.md and
    `readers_after` to .json: each group's latency over the settle
    phase, whole and per five-minute slice of it, and the bytes each
    disk read and wrote meanwhile."""
    step = 300.0
    groups = []
    for group in run.env["workload"]["reads"]:
        name = group["name"]
        if run.env["readers"][name] <= 0:
            continue
        paths = glob.glob(reader_prefix(run, name, "*") + ".*")
        scripts = [script for script, _weight in group["scripts"]]
        groups.append({
            "name": name, "clients": run.env["readers"][name],
            "sample": group.get("sample", 1.0),
            "scripts": pgbench.latency_summary(
                paths, scripts, whole_paths=True, until=until,
                since=since),
            "slices": pgbench.latency_slices(
                paths, scripts, since, until, step),
        })
    secs = until - since
    path = run.out / "load-summary.json"
    data = json.loads(path.read_text())
    disks = {d: dict(data["disks"].get(d, {"roles": []}),
                     read_bytes=r, written_bytes=w, reads=ro, writes=wo)
             for d, (r, w, ro, wo) in sorted(io["disks"].items())}
    data["readers_after"] = {"secs": secs, "slice_secs": step,
                             "groups": groups, "disks": disks,
                             "data_dir_bytes": dir_bytes(run.data),
                             "cpu": io["cpu"]}
    path.write_text(json.dumps(data, indent=2) + "\n")

    def ms(d, key):
        return f"{d[key]:.2f}" if key in d else "-"

    md = [
        "\n## Readers after the load\n",
        f"The reader groups ran on for {secs:.0f} s after the load "
        "ended, over every id. Latency in ms per script: the whole "
        f"period, then each {step / 60:.0f}-minute slice of it by when "
        "the transaction started.\n",
        "| group | script | minutes | txns/s | p50 | p95 | p99 | p99.9 "
        "| max |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for g in groups:
        for script, d in g["scripts"].items():
            rows = [("all", d, secs)]
            rows += [(f"{s['from'] / 60:.0f}-{(s['from'] + step) / 60:.0f}",
                      s, min(step, secs - s["from"]))
                     for s in g["slices"].get(script, [])]
            for label, s, span in rows:
                txns = s["n"] / g["sample"]
                md.append(
                    f"| {g['name']} | {script} | {label} | "
                    f"{txns / max(span, 1):,.1f} | {ms(s, 'p50')} | "
                    f"{ms(s, 'p95')} | {ms(s, 'p99')} | {ms(s, 'p999')} | "
                    f"{ms(s, 'max')} |")
    md += [
        "\nDisk bytes meanwhile, whole disks:\n",
        "| disk | holds | read GiB | written GiB | read MiB/s "
        "| write MiB/s | reads | writes | read/s | write/s |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for disk, d in disks.items():
        md.append(disk_row(disk, d, secs))
    md.append(f"\nData dir at the end: "
              f"{data['readers_after']['data_dir_bytes'] / GIB:.1f} GiB.")
    md.append(cpu_line(io["cpu"]))
    with open(run.out / "load-summary.md", "a") as f:
        f.write("\n".join(md) + "\n")


def disk_row(disk: str, d: dict, secs: float) -> str:
    """One disk's row of the summary's disk table: bytes and completed
    requests, totals and per second."""
    r, w = d["read_bytes"], d["written_bytes"]
    ro, wo = d["reads"], d["writes"]
    s = max(secs, 1)
    return (f"| {disk} | {', '.join(d['roles']) or '-'} | {r / GIB:.1f} | "
            f"{w / GIB:.1f} | {r / 2**20 / s:.0f} | {w / 2**20 / s:.0f} | "
            f"{ro / 1e6:.1f}M | {wo / 1e6:.1f}M | {ro / s:,.0f} | "
            f"{wo / s:,.0f} |")


def cpu_line(cpu: dict) -> str:
    """The summary's CPU line: cores busy on average, host and server
    (user and system time, from the cgroup or the process)."""
    server = cpu["server_cores"]
    return (f"- CPU, cores busy on average: host "
            f"{cpu['host_cores_busy']:.1f} of {cpu['host_cores']}; "
            + (f"server {server:.1f}." if server is not None else
               "server unknown (no cgroup, more than one process)."))


def zdb_bytes(run) -> dict:
    """Egres's own byte counters by source (user, flush, compact), from
    the admin `zdb` metrics; {} for Postgres or when unreachable."""
    if run.kind != "egres":
        return {}
    import watchdog
    try:
        text = watchdog.admin("zdb", run.env["server"]["admin_port"])
    except OSError:
        return {}
    # One series per source, tree and level; summed here by source.
    out: dict = {}
    for source, val in re.findall(
            r'^zdb_bytes_written_total\{[^}]*source="(\w+)"[^}]*\} (\d+)',
            text, re.M):
        out[source] = out.get(source, 0) + int(val)
    return out


def summarize(run, prog: Progress, rows: int, t0: float, secs: float,
              io: dict, mem: dict):
    """load-summary.md and .json: what this load (or this resumed part
    of one) cost, from `t0` for `secs` seconds. The readers' last round
    runs on past the load; only what they started within it counts."""
    ins = spread(prog.insert.all)
    groups = []
    for group in run.env["workload"]["reads"]:
        name = group["name"]
        if run.env["readers"][name] <= 0:
            continue
        sample = group.get("sample", 1.0)
        scripts = [script for script, _weight in group["scripts"]]
        lat = pgbench.latency_summary(
            glob.glob(reader_prefix(run, name, "*") + ".*"), scripts,
            whole_paths=True, until=t0 + secs)
        groups.append({"name": name, "clients": run.env["readers"][name],
                       "sample": sample, "scripts": lat})
    roles = {}
    for role, path in (("data", run.data), ("wal", run.wal)):
        for disk in iostat.device_of(path):
            roles.setdefault(disk, []).append(role)
    zdb = zdb_bytes(run)
    data = {
        "env": run.name, "rows": rows, "secs": secs,
        "rows_per_sec": rows / max(secs, 1), "failed_chunks": prog.failed,
        "data_dir_bytes": dir_bytes(run.data),
        "wal_dir_bytes": dir_bytes(run.wal),
        "insert_ms": ins, "readers": groups,
        "disks": {d: {"roles": roles.get(d, []), "read_bytes": r,
                      "written_bytes": w, "reads": ro, "writes": wo}
                  for d, (r, w, ro, wo) in sorted(io["disks"].items())},
        "server_process": io["proc"], "zdb_bytes_written": zdb,
        "server_memory_bytes": mem, "cpu": io["cpu"],
    }
    (run.out / "load-summary.json").write_text(
        json.dumps(data, indent=2) + "\n")

    def ms(d, key):
        return f"{d[key]:.2f}" if key in d else "-"

    md = [
        "# Load summary\n",
        f"Env `{run.name}`, written {utc()}.\n",
        f"- Rows: {rows:,} in {secs:.0f} s, {rows / max(secs, 1):,.0f} "
        f"rows/s on average; {prog.failed} chunks failed.",
        f"- Data dir: {data['data_dir_bytes'] / GIB:.1f} GiB, "
        f"{data['data_dir_bytes'] / max(prog.rows, 1):.0f} bytes per row "
        f"loaded, as the load ended (compaction may still shrink it). "
        f"WAL dir: {data['wal_dir_bytes'] / GIB:.1f} GiB.",
        f"- Server memory, resident (`server_mem`; page cache not "
        f"counted): peak {mem['peak'] / GIB:.1f} GiB, "
        f"{mem['end'] / GIB:.1f} GiB as the load ended.",
        cpu_line(io["cpu"]) + "\n",
        "## Insert latency\n",
        "One INSERT statement's round trip as psql timed it, in ms.\n",
        "| statements | p50 | p95 | p99 | p99.9 | max |",
        "|---|---|---|---|---|---|",
        f"| {ins['n']:,} | {ms(ins, 'p50')} | {ms(ins, 'p95')} | "
        f"{ms(ins, 'p99')} | {ms(ins, 'p999')} | {ms(ins, 'max')} |\n",
        "## Readers during the load\n",
        "Latency in ms per script, from pgbench's per-transaction log, "
        "for transactions started while the load ran. `txns` is the "
        "logged count over the sampled fraction.\n",
        "| group | script | clients | txns | txns/s | p50 | p95 | p99 "
        "| p99.9 | max |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for g in groups:
        for script, d in g["scripts"].items():
            txns = d["n"] / g["sample"]
            md.append(
                f"| {g['name']} | {script} | {g['clients']} | {txns:,.0f} | "
                f"{txns / max(secs, 1):,.1f} | {ms(d, 'p50')} | "
                f"{ms(d, 'p95')} | {ms(d, 'p99')} | {ms(d, 'p999')} | "
                f"{ms(d, 'max')} |")
    md += [
        "\n## Disk bytes during the load\n",
        "Whole disks, from /proc/diskstats: everything the device "
        "served, bytes and completed requests.\n",
        "| disk | holds | read GiB | written GiB | read MiB/s "
        "| write MiB/s | reads | writes | read/s | write/s |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for disk, d in data["disks"].items():
        md.append(disk_row(disk, d, secs))
    if io["proc"]:
        r, w = io["proc"]
        # Buffered writes count when a page is dirtied, not when it
        # reaches a disk, so this can exceed the disks' sum.
        md.append(f"\nThe server process's own counters (/proc/<pid>/io, "
                  f"which count a buffered write when it is made): read "
                  f"{r / GIB:.1f} GiB, wrote {w / GIB:.1f} GiB.")
    if zdb.get("user"):
        user = zdb["user"]
        total = sum(zdb.values())
        wrote = sum(d["written_bytes"] for d in data["disks"].values()
                    if "data" in d["roles"])
        md.append(
            f"\nEgres counters since the server started: "
            + ", ".join(f"{k} {v / GIB:.1f} GiB" for k, v in zdb.items())
            + f". Write amplification: {total / user:.2f} by those "
            f"counters, {wrote / user:.2f} as data disk bytes written "
            f"per user byte.")
    (run.out / "load-summary.md").write_text("\n".join(md) + "\n")
