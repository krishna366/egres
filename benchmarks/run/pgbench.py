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

When pgbench exits the metrics are cleared and its final report parsed
into a dict (tps, latency, transactions, failed, per script).

`workload` is one workload run: the env's mix for `secs` seconds. The
run number reaches the scripts as `:run`, so a workload that inserts
keeps its ids apart from an earlier run's (the orders insert script);
`:n` starts at 0 for such per-client counters; `:rows` is the count the
load reached.
"""

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
                 clients: int):
        self.proc = proc
        self.log = log
        self.prom = prom
        self.run_label = run_label
        self.clients = clients
        self.pos = 0
        self.failed = 0
        self.last = None

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
          secs: int, scripts, extra_vars: dict) -> Handle:
    # Appending, never truncating: a second start with the same name
    # must not erase a finished run's numbers.
    cmd = [str(run.pgbench), run.conn, "-n", "-c", str(clients), "-j",
           str(min(clients, 16)), "-T", str(secs), "-P", "5"]
    cmd += script_args(run, scripts, extra_vars)
    with open(log, "ab") as lf:
        proc = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT,
                                start_new_session=True)
    return Handle(proc, log, run.textfile / f"{prom_name}.prom",
                  run_label, clients)


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


def workload(run, run_no: int, secs: int) -> dict:
    clients = run.env["clients"]
    name = f"mixed-c{clients}-run{run_no}"
    log = run.results / f"pgbench-{name}.log"
    rows = rows_loaded(run)
    run.event(f"workload {name} rows={rows} secs={secs} clients={clients}")
    h = start(run, log, "pgbench", name, clients, secs,
              run.env["workload"]["mix"], {"rows": rows, "run": run_no,
                                           "n": 0})
    # The session leader's pid, for `bench.py stop`.
    (run.out / "workload.pid").write_text(f"{h.pid}\n")
    res = follow(h)
    res["rc"] = h.proc.returncode
    run.event(f"workload {name} exit={res['rc']} tps={res['tps']} "
              f"latency_ms={res['latency_ms']} failed={res['failed']}")
    # The final report, kept apart from the log as well.
    lines = log.read_text(errors="replace").splitlines()[-40:]
    (run.results / f"pgbench-{name}.summary").write_text(
        "\n".join(lines) + "\n")
    return res
