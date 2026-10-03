# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

"""Run pgbench and publish its progress.

`start` launches pgbench in its own session, appending to a log, and
returns a handle; `follow` tails the log every five seconds, rewriting
`<prom_name>.prom` in the textfile dir with the latest progress line,
so Grafana shows tps and latency over time:

    pgbench_tps{run="..."}                transactions per second
    pgbench_latency_ms{run="..."}         average latency
    pgbench_latency_stddev_ms{run="..."}  latency standard deviation
    pgbench_failed_total{run="..."}       failed transactions so far
    pgbench_clients{run="..."}            client count

An average hides the slow transactions, so pgbench also logs each
transaction's latency (`-l`, or a sample of them for fast scripts),
and `follow` publishes percentiles per script over the transactions
since its last look:

    pgbench_latency_p50_ms{run="...",script="..."}
    pgbench_latency_p95_ms{run="...",script="..."}
    pgbench_latency_p99_ms{run="...",script="..."}
    pgbench_latency_max_ms{run="...",script="..."}

`latency_summary` gives the same per script over whole log files, for
a report. Percentiles are per script because a mix's scripts differ by
orders of magnitude: a point read and a scan share no distribution.

When pgbench exits the metrics are cleared and its final report parsed
into a dict (tps, latency, transactions, failed, per script).

`workload` is one workload run: the env's mix for `secs` seconds. The
run number reaches the scripts as `:run`, so a workload that inserts
keeps its ids apart from an earlier run's (the orders insert script);
`:n` starts at 0 for such per-client counters; `:rows` is the count the
load reached.
"""

import array
import glob
import math
import os
import re
import subprocess
import time
from pathlib import Path

import workloads

PROGRESS = re.compile(
    r"progress: [\d.]+ s, ([\d.]+) tps, lat ([\d.]+) ms stddev ([\d.]+|NaN)"
    r"(?:, (\d+) failed)?")
FINAL_TPS = re.compile(r"^tps = ([\d.]+)", re.M)
FINAL_LAT = re.compile(r"^latency average = ([\d.]+) ms", re.M)
FINAL_TXNS = re.compile(r"^number of transactions actually processed: (\d+)",
                        re.M)
FINAL_FAILED = re.compile(r"^number of failed transactions: (\d+)", re.M)
SCRIPT = re.compile(r"^SQL script \d+: (\S+)\n(?: - .*\n)*?"
                    r" - (\d+) transactions \(([\d.]+)% of total\)", re.M)


class Handle:
    def __init__(self, proc, log: Path, prom: Path, run_label: str,
                 clients: int, lat_prefix, scripts: list):
        self.proc = proc
        self.log = log
        self.prom = prom
        self.run_label = run_label
        self.clients = clients
        self.pos = 0
        self.failed = 0
        self.last = None
        # The per-transaction logs: pgbench writes one file per
        # thread, `<prefix>.<pid>[.<n>]`; how far each has been read.
        self.lat_prefix = lat_prefix
        self.lat_pos: dict = {}
        self.scripts = scripts
        self.lat_last: dict = {}

    @property
    def pid(self) -> int:
        return self.proc.pid

    def alive(self) -> bool:
        return self.proc.poll() is None


def script_args(run, scripts, extra_vars: dict) -> list[str]:
    args = []
    for key, val in {**run.env["workload"]["vars"], **extra_vars}.items():
        args += ["-D", f"{key}={val}"]
    for name, weight in scripts:
        args += ["-f", f"{workloads.script_path(run, name)}@{weight}"]
    return args


def start(run, log: Path, prom_name: str, run_label: str, clients: int,
          secs: int, scripts, extra_vars: dict, lat_prefix=None,
          sample: float = 1.0) -> Handle:
    """`lat_prefix`: where the per-transaction latency logs go (a path
    prefix, its dir created here), or None for none. `sample`: the
    fraction of transactions logged; fast scripts at full rate would
    write gigabytes onto the disk under test."""
    # Appending, never truncating: a second start with the same name
    # must not erase a finished run's numbers.
    cmd = [str(run.pgbench), run.conn, "-n", "-c", str(clients), "-j",
           str(min(clients, 16)), "-T", str(secs), "-P", "5"]
    if lat_prefix is not None:
        Path(lat_prefix).parent.mkdir(parents=True, exist_ok=True)
        cmd += ["-l", f"--log-prefix={lat_prefix}"]
        if sample < 1.0:
            cmd.append(f"--sampling-rate={sample}")
    cmd += script_args(run, scripts, extra_vars)
    with open(log, "ab") as lf:
        proc = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT,
                                start_new_session=True)
    return Handle(proc, log, run.textfile / f"{prom_name}.prom",
                  run_label, clients, lat_prefix,
                  [name for name, _weight in scripts])


def percentile(sorted_vals, pct: float):
    """Nearest rank: the ceil(n * pct / 100)-th smallest value."""
    n = len(sorted_vals)
    rank = min(max(math.ceil(n * pct / 100.0), 1), n)
    return sorted_vals[rank - 1]


def parse_latencies(chunk: bytes, into: dict, until=None, since=None,
                    started: dict = None):
    """Add a log chunk's latencies (microseconds) to `into`, a dict of
    script number -> array. A line is
    `client txn_no time_us script_no epoch epoch_us [...]`, the epoch
    being when the transaction ended; a failed transaction has a word
    where the time is, and is skipped. `until` and `since` (epoch
    seconds) leave out transactions that started after, or before,
    them. `started`, when given, collects each kept transaction's
    start time the same way."""
    for line in chunk.splitlines():
        parts = line.split()
        if len(parts) < 6:
            continue
        try:
            us, script = int(parts[2]), int(parts[3])
            ended = int(parts[4]) + int(parts[5]) / 1e6
        except ValueError:
            continue
        t0 = ended - us / 1e6
        if until is not None and t0 > until:
            continue
        if since is not None and t0 < since:
            continue
        if script not in into:
            into[script] = array.array("L")
        into[script].append(us)
        if started is not None:
            if script not in started:
                started[script] = array.array("d")
            started[script].append(t0)


def poll_latencies(h: Handle):
    """Percentiles per script over the log lines written since the
    last call. Only whole lines are consumed: pgbench flushes these
    files a buffer at a time, mid-line."""
    fresh: dict = {}
    for path in glob.glob(f"{h.lat_prefix}.*"):
        pos = h.lat_pos.get(path, 0)
        with open(path, "rb") as f:
            f.seek(pos)
            chunk = f.read()
        end = chunk.rfind(b"\n") + 1
        h.lat_pos[path] = pos + end
        parse_latencies(chunk[:end], fresh)
    for script, vals in fresh.items():
        ordered = sorted(vals)
        name = h.scripts[script] if script < len(h.scripts) else str(script)
        h.lat_last[name] = (percentile(ordered, 50) / 1000.0,
                            percentile(ordered, 95) / 1000.0,
                            percentile(ordered, 99) / 1000.0,
                            ordered[-1] / 1000.0)


def latency_summary(prefixes: list, scripts: list,
                    whole_paths: bool = False, until=None,
                    since=None) -> dict:
    """Per script name, over every log file under the given prefixes
    (or the files themselves with `whole_paths`): n (logged
    transactions; a sample when the run samples) and p50, p95, p99,
    p999, max in milliseconds. `until` and `since`: only transactions
    started by, or from, that time (epoch seconds)."""
    vals: dict = {}
    for prefix in prefixes:
        paths = [prefix] if whole_paths else sorted(glob.glob(f"{prefix}.*"))
        for path in paths:
            with open(path, "rb") as f:
                parse_latencies(f.read(), vals, until, since)
    out = {}
    for script, arr in sorted(vals.items()):
        name = scripts[script] if script < len(scripts) else str(script)
        out[name] = spread(arr)
    return out


def latency_slices(paths: list, scripts: list, since: float,
                   until: float, step: float) -> dict:
    """Per script name, the latency spread of each `step`-second slice
    of [since, until), by when the transaction started: a list of
    {"from": seconds after `since`, "n", "p50", ...}, oldest first,
    with a slice left out when nothing started in it."""
    vals: dict = {}
    starts: dict = {}
    for path in paths:
        with open(path, "rb") as f:
            parse_latencies(f.read(), vals, until, since, starts)
    out = {}
    for script, arr in sorted(vals.items()):
        name = scripts[script] if script < len(scripts) else str(script)
        by_slice: dict = {}
        for us, t0 in zip(arr, starts[script]):
            by_slice.setdefault(int((t0 - since) // step), []).append(us)
        out[name] = [dict(spread(v), **{"from": k * step})
                     for k, v in sorted(by_slice.items())]
    return out


def spread(vals) -> dict:
    """n, p50, p95, p99, p999 and max (milliseconds) of latencies in
    microseconds; {"n": 0} when there are none."""
    ordered = sorted(vals)
    if not ordered:
        return {"n": 0}
    out = {"n": len(ordered), "max": ordered[-1] / 1000.0}
    for key, pct in (("p50", 50), ("p95", 95), ("p99", 99),
                     ("p999", 99.9)):
        out[key] = percentile(ordered, pct) / 1000.0
    return out


def write_prom(h: Handle, vals):
    if vals is None:
        body = ""
    else:
        tps, lat, sd, failed = vals
        lab = f'{{run="{h.run_label}"}}'
        body = (
            f"# TYPE pgbench_tps gauge\npgbench_tps{lab} {tps}\n"
            f"# TYPE pgbench_latency_ms gauge\npgbench_latency_ms{lab} {lat}\n"
            "# TYPE pgbench_latency_stddev_ms gauge\n"
            f"pgbench_latency_stddev_ms{lab} {sd}\n"
            "# TYPE pgbench_failed_total counter\n"
            f"pgbench_failed_total{lab} {failed}\n"
            f"# TYPE pgbench_clients gauge\npgbench_clients{lab} {h.clients}\n"
        )
        for i, metric in enumerate(("p50", "p95", "p99", "max")):
            if not h.lat_last:
                break
            body += f"# TYPE pgbench_latency_{metric}_ms gauge\n"
            for script, stats in sorted(h.lat_last.items()):
                body += (f'pgbench_latency_{metric}_ms{{run="{h.run_label}"'
                         f',script="{script}"}} {stats[i]:.3f}\n')
    tmp = h.prom.with_suffix(".tmp")
    tmp.write_text(body)
    os.replace(tmp, h.prom)


def poll(h: Handle):
    """Publish the newest progress line since the last call."""
    if not h.log.exists():
        return
    with open(h.log) as f:
        f.seek(h.pos)
        for line in f:
            m = PROGRESS.match(line)
            if not m:
                continue
            tps, lat, sd, failed = m.groups()
            h.failed += int(failed or 0)
            h.last = (tps, lat, "0" if sd == "NaN" else sd, h.failed)
        h.pos = f.tell()
    if h.lat_prefix is not None:
        poll_latencies(h)
    if h.last is not None:
        write_prom(h, h.last)


def follow(h: Handle, stop=None) -> dict:
    """Tail until pgbench exits (or `stop()` says to end it), clear the
    metrics, and return the final report."""
    while h.alive():
        poll(h)
        if stop is not None and stop():
            h.proc.terminate()
        time.sleep(5)
    poll(h)
    write_prom(h, None)
    return final_report(h.log)


def final_report(log: Path) -> dict:
    text = log.read_text(errors="replace")
    # The last run in an appended log: from the last "transaction type"
    # header on.
    idx = text.rfind("transaction type:")
    tail = text[idx:] if idx >= 0 else text
    res = {"rc": None}
    for key, rx in (("tps", FINAL_TPS), ("latency_ms", FINAL_LAT),
                    ("transactions", FINAL_TXNS), ("failed", FINAL_FAILED)):
        m = rx.search(tail)
        res[key] = float(m.group(1)) if m else None
    res["scripts"] = {Path(p).name: int(n) for p, n, _ in SCRIPT.findall(tail)}
    res["progress_lines"] = sum(1 for l in tail.splitlines()
                                if l.startswith("progress"))
    return res


def rows_loaded(run) -> int:
    text = (run.out / "load.log").read_text(errors="replace")
    done = re.findall(r"load done rows=(\d+)", text)
    if not done:
        raise SystemExit(f"no 'load done' in {run.out / 'load.log'}")
    return int(done[-1])


def id_vars(cfg: dict, rows: int, per: int) -> dict:
    """The pgbench variables that say which ids exist. The load fills
    `streams` slices of `slice` ids each, every slice from its first
    id up, so the ids present are the first `per` of each slice (the
    shortest slice's count, so every script's draw exists) and `rows`
    of them in all. A script draws a slice and an offset:
    `:first + :s * :slice + :k` for s in [0, streams) and k in
    [0, per)."""
    total = cfg["last"] - cfg["first"] + 1
    streams = min(cfg["streams"], total)
    return {"rows": rows, "first": cfg["first"], "streams": streams,
            "slice": (total + streams - 1) // streams, "per": per}


def loaded_vars(run) -> dict:
    """`id_vars` for a finished load, from its "load done" line."""
    text = (run.out / "load.log").read_text(errors="replace")
    per = re.findall(r"load done rows=\d+ .*\bper=(\d+)", text)
    cfg = run.env["load"]
    total = cfg["last"] - cfg["first"] + 1
    slice_ids = (total + cfg["streams"] - 1) // cfg["streams"]
    return id_vars(cfg, rows_loaded(run),
                   int(per[-1]) if per else slice_ids)


def workload(run, run_no: int, secs: int) -> dict:
    clients = run.env["clients"]
    name = f"mixed-c{clients}-run{run_no}"
    log = run.results / f"pgbench-{name}.log"
    rows = rows_loaded(run)
    run.event(f"workload {name} rows={rows} secs={secs} clients={clients}")
    lat_prefix = run.results / "latency" / name
    h = start(run, log, "pgbench", name, clients, secs,
              run.env["workload"]["mix"],
              {**loaded_vars(run), "run": run_no, "n": 0}, lat_prefix,
              run.env["latency_sample"])
    # The session leader's pid, for `bench.py stop`.
    (run.out / "workload.pid").write_text(f"{h.pid}\n")
    res = follow(h)
    res["rc"] = h.proc.returncode
    res["latency"] = latency_summary([lat_prefix], h.scripts)
    run.event(f"workload {name} exit={res['rc']} tps={res['tps']} "
              f"latency_ms={res['latency_ms']} failed={res['failed']}")
    # The final report, kept apart from the log as well.
    lines = log.read_text(errors="replace").splitlines()[-40:]
    (run.results / f"pgbench-{name}.summary").write_text(
        "\n".join(lines) + "\n")
    return res
