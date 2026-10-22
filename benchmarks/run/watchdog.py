# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

"""The watchdog: the mechanical part of a night operator. `bench.py
<env> watch` starts it detached for the whole run.

Once a minute it checks the server, and on a schedule it logs numbers,
appends progress rows, and moves the run through its phases:

    load      the load driver (pid in load.pid) is running; ends when
              it exits and load.log has "load done rows=N"
    workload  the env's workload runs, one after another
    done      SUMMARY-auto.md written; exit

Incidents, all recorded in INCIDENTS.md with dumps in dumps/:
  - server dead: restart it once; a second death within an hour stops
    the run (STOP file for the load driver, pgbench killed)
  - stall: rows (load) or pgbench progress unchanged for 15 minutes
    takes admin dumps; 30 minutes stops the phase
  - disk: under 150 GiB free where the data dir is stops the load

Everything is by pid from the pid files; nothing is matched by name.
The phase logic reads the load's own metrics file; Prometheus only
feeds the status lines, so the monitoring stack need not be pointed at
this run.
"""

import json
import os
import re
import shutil
import socket
import subprocess
import threading
import time
import urllib.parse
import urllib.request

import bench
import pgbench

GIB = 2**30


def utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def prom(query: str, default=None):
    url = "http://127.0.0.1:9090/api/v1/query?" + urllib.parse.urlencode(
        {"query": query})
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            res = json.load(r)["data"]["result"]
        return float(res[0]["value"][1]) if res else default
    except Exception:
        return default


def admin(command: str, port: int, timeout: float = 10.0) -> str:
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as s:
        s.sendall((command + "\n").encode())
        out = b""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                chunk = s.recv(1 << 16)
            except socket.timeout:
                break
            if not chunk:
                break
            out += chunk
            if out.endswith(b"\n\n"):
                break
    return out.decode(errors="replace")


class Watch:
    def __init__(self, run, runs_done: int):
        self.run = run
        self.out = run.out
        self.dumps = self.out / "dumps"
        self.dumps.mkdir(exist_ok=True)
        self.log = self.out / "watch.log"
        self.incidents = self.out / "INCIDENTS.md"
        self.progress = self.out / "progress.md"
        self.log_pos = 0
        self.phase = "load"
        self.rows_loaded = None
        self.restarts: list[float] = []
        self.last_rows = None
        self.last_rows_change = time.time()
        self.stall_dumped = False
        self.last_status = 0.0
        self.last_progress = 0.0
        self.status_secs = 600
        self.progress_secs = 1800
        self.workload_run = 0
        self.workload_thread = None
        self.workload_result = None
        self.pgbench_log = None
        self.pgbench_pos = 0
        self.pgbench_last_change = time.time()
        self.stopped_reason = None
        (self.out / "watch.pid").write_text(f"{os.getpid()}\n")
        if runs_done > 0:
            # Start in the workload phase with this many runs already
            # done (their logs kept), e.g. after a restart between runs.
            self.rows_loaded = pgbench.rows_loaded(run)
            self.workload_run = runs_done
            self.phase = "workload"
            self.start_workload()

    # -- notes --------------------------------------------------------

    def note(self, msg: str):
        line = f"{utc()} {msg}"
        print(line, flush=True)
        with open(self.log, "a") as f:
            f.write(line + "\n")

    def incident(self, title: str, body: str):
        self.note(f"INCIDENT: {title}")
        with open(self.incidents, "a") as f:
            if f.tell() == 0:
                f.write("# Incidents\n\n")
            f.write(f"## {utc()}: {title}\n\n{body}\n\n")

    def dump(self, tag: str):
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        # Admin dumps are Egres's; a Postgres run has only its log.
        cmds = () if self.run.kind != "egres" else \
            ("zdb", "workers", "locks", "raft")
        port = self.run.env["server"].get("admin_port", 0)
        for cmd in cmds:
            name = f"{stamp}-{tag}-{cmd.replace(' ', '_')}.txt"
            try:
                (self.dumps / name).write_text(admin(cmd, port))
            except OSError as e:
                (self.dumps / name).write_text(f"admin failed: {e}\n")
        try:
            tail = subprocess.run(
                ["tail", "-n", "300", str(self.run.server_log)],
                capture_output=True, text=True).stdout
            (self.dumps / f"{stamp}-{tag}-server.log.tail").write_text(tail)
        except OSError:
            pass

    # -- checks -------------------------------------------------------

    def new_log_lines(self):
        """Counts of panic / error / warn in the server log since the
        last call, and one sample line of each."""
        # An Egres line is `<time> ERROR <scope>: ...`: the level is
        # the second field, so a query or path holding the word does
        # not count. A panic is Zig's own text, anywhere in the line.
        egres = self.run.kind == "egres"
        keys = ("panic", "ERROR", "WARN") if egres \
            else ("PANIC", "ERROR", "FATAL")
        counts = {k: 0 for k in keys}
        sample = {}
        log = self.run.server_log
        try:
            size = log.stat().st_size
            if size < self.log_pos:
                self.log_pos = 0  # rotated
            with open(log, "rb") as f:
                f.seek(self.log_pos)
                chunk = f.read(size - self.log_pos)
                self.log_pos = size
        except OSError:
            return counts, sample
        for line in chunk.decode(errors="replace").splitlines():
            level = line.split(None, 2)[1:2]
            for key in counts:
                hit = key in line if not egres or key == "panic" \
                    else level == [key]
                if hit:
                    counts[key] += 1
                    sample.setdefault(key, line[:300])
        return counts, sample

    def load_rows(self):
        try:
            text = (self.run.textfile / "load.prom").read_text()
        except OSError:
            return None
        m = re.search(r"^egres_load_rows_total (\d+)", text, re.M)
        return float(m.group(1)) if m else None

    def numbers(self) -> dict:
        # The data dir's filesystem: the one a load can fill.
        free = shutil.disk_usage(self.run.data).free
        return {
            "rows": self.load_rows(),
            "rate": prom("sum(rate(egres_load_rows_total[5m]))"),
            "data": prom("egres_data_dir_bytes"),
            "sst": prom("egres_sst_files"),
            "rss": prom('namedprocess_namegroup_memory_bytes'
                        '{groupname="egres",memtype="resident"}'),
            "l0": prom("egres_l0_tables"),
            "l0max": prom("egres_l0_tables_max_tree"),
            "inflight": prom("egres_inflight_jobs"),
            "trees": prom("egres_trees"),
            "free": float(free),
            "tps": prom("sum(pgbench_tps)"),
            "lat": prom("avg(pgbench_latency_ms)"),
            "failed": prom("sum(pgbench_failed_total)"),
        }

    @staticmethod
    def fmt(v, div=1.0, nd=1):
        return "-" if v is None else f"{v / div:.{nd}f}"

    def status(self, n):
        counts, sample = self.new_log_lines()
        f = self.fmt
        self.note(
            f"phase={self.phase} rows={f(n['rows'], 1, 0)} "
            f"rate={f(n['rate'], 1, 0)}/s data={f(n['data'], GIB)}GiB "
            f"sst={f(n['sst'], 1, 0)} rss={f(n['rss'], GIB)}GiB "
            f"l0={f(n['l0'], 1, 0)}/{f(n['l0max'], 1, 0)} "
            f"inflight={f(n['inflight'], 1, 0)} trees={f(n['trees'], 1, 0)} "
            f"tps={f(n['tps'], 1, 0)} lat={f(n['lat'], 1, 2)}ms "
            f"failed={f(n['failed'], 1, 0)} free={f(n['free'], GIB, 0)}GiB "
            "log: " + " ".join(f"{k.rstrip('(').lower()}={v}"
                               for k, v in counts.items()))
        for key, line in sample.items():
            self.note(f"  sample {key}: {line}")
        if counts.get("panic") or counts.get("PANIC"):
            self.incident("panic line in the server log",
                          sample.get("panic") or sample.get("PANIC"))

    def progress_row(self, n):
        f = self.fmt
        with open(self.progress, "a") as p:
            if p.tell() == 0:
                p.write("| time | rows | rate/s | data GiB | sst | rss GiB "
                        "| l0 | tps | lat ms |\n|---|---|---|---|---|---|"
                        "---|---|---|\n")
            p.write(f"| {utc()} | {f(n['rows'], 1, 0)} | "
                    f"{f(n['rate'], 1, 0)} | {f(n['data'], GIB)} | "
                    f"{f(n['sst'], 1, 0)} | {f(n['rss'], GIB)} | "
                    f"{f(n['l0'], 1, 0)} | {f(n['tps'], 1, 0)} | "
                    f"{f(n['lat'], 1, 1)} |\n")

    # -- actions ------------------------------------------------------

    def stop_pgbench(self):
        pid = self.run.pid("workload.pid")
        if bench.alive(pid):
            try:
                os.killpg(pid, 15)
            except OSError:
                pass

    def stop_everything(self, reason: str):
        self.stopped_reason = reason
        (self.out / "STOP").touch()
        self.stop_pgbench()
        self.note(f"stopping: {reason}")

    def restart_server(self, why: str):
        now = time.time()
        self.restarts = [t for t in self.restarts if now - t < 3600]
        body = why + "\n\nLast server log lines are in dumps/."
        if self.restarts:
            self.incident("server died again within an hour", body)
            self.stop_everything("second server death within an hour")
            self.phase = "done"
            return
        self.incident("server died; restarting", body)
        self.restarts.append(now)
        try:
            bench.cmd_start(self.run)
            self.log_pos = 0
        except (SystemExit, subprocess.CalledProcessError) as e:
            self.note(f"restart failed: {e}")
            self.stop_everything("server restart failed")
            self.phase = "done"

    def start_workload(self):
        self.workload_run += 1
        run_no = self.workload_run
        name = f"mixed-c{self.run.env['clients']}-run{run_no}"
        self.pgbench_log = self.run.results / f"pgbench-{name}.log"
        self.pgbench_pos = 0
        self.pgbench_last_change = time.time()
        self.workload_result = None

        def go():
            self.workload_result = pgbench.workload(
                self.run, run_no, self.run.env["secs"])
        self.workload_thread = threading.Thread(target=go, daemon=True)
        self.workload_thread.start()
        self.note(f"workload {name} started rows={self.rows_loaded} "
                  f"secs={self.run.env['secs']}")

    def workload_alive(self) -> bool:
        return self.workload_thread is not None and \
            self.workload_thread.is_alive()

    def record_workload(self):
        res = self.workload_result or {}
        with open(self.progress, "a") as p:
            p.write(f"\nWorkload run {self.workload_run}: "
                    f"tps={res.get('tps')} latency_ms={res.get('latency_ms')} "
                    f"transactions={res.get('transactions')} "
                    f"failed={res.get('failed')} scripts={res.get('scripts')}"
                    "\n")
        return res

    # -- phases -------------------------------------------------------

    def tick_load(self, n):
        load_pid = self.run.pid("load.pid")
        rows = n["rows"]
        if rows is not None and rows != self.last_rows:
            self.last_rows = rows
            self.last_rows_change = time.time()
            self.stall_dumped = False
        stalled = time.time() - self.last_rows_change
        loading = bench.alive(load_pid)
        if loading and stalled > 900 and not self.stall_dumped:
            self.dump("stall")
            self.incident("load stalled 15 min",
                          f"rows stuck at {rows}; dumps taken")
            self.stall_dumped = True
        if loading and stalled > 1800 and self.stopped_reason is None:
            self.stop_everything("load stalled 30 min")
        if n["free"] < 150 * GIB and self.stopped_reason is None:
            self.stop_everything("under 150 GiB free")
        if not loading:
            log = self.out / "load.log"
            text = log.read_text(errors="replace") if log.exists() else ""
            # The last one: a resumed load logs its own, with the total.
            done = re.findall(r"load done rows=(\d+)", text)
            if done:
                self.rows_loaded = int(done[-1])
                self.note(f"load done: rows={self.rows_loaded}")
            else:
                self.rows_loaded = int(rows or 0)
                self.incident("load driver exited without 'load done'",
                              f"rows from metrics: {self.rows_loaded}")
            self.progress_row(n)
            # An env with no workload runs ends with its load.
            if self.rows_loaded > 0 and self.phase == "load" and \
                    self.run.env["runs"] > 0:
                self.phase = "workload"
                self.start_workload()
            else:
                self.phase = "done"

    def tick_workload(self, n):
        if self.pgbench_log and self.pgbench_log.exists():
            size = self.pgbench_log.stat().st_size
            if size != self.pgbench_pos:
                self.pgbench_pos = size
                self.pgbench_last_change = time.time()
                self.stall_dumped = False
        stalled = time.time() - self.pgbench_last_change
        if self.workload_alive() and stalled > 900 and \
                not self.stall_dumped:
            self.dump("pgbench-stall")
            self.incident("pgbench progress stalled 15 min", "dumps taken")
            self.stall_dumped = True
        if self.workload_alive() and stalled > 1800:
            self.incident("pgbench stalled 30 min; run killed", "")
            self.stop_pgbench()
        if not self.workload_alive():
            res = self.record_workload()
            self.note(f"workload run {self.workload_run} ended: "
                      f"tps={res.get('tps')} failed={res.get('failed')}")
            if self.workload_run < self.run.env["runs"] and \
                    self.stopped_reason is None:
                self.start_workload()
            else:
                self.phase = "done"

    def summary(self, n):
        f = self.fmt
        text = [
            "# Auto summary\n",
            f"Written {utc()} by the watchdog. Phase reached: {self.phase}; "
            f"stop reason: {self.stopped_reason or 'none'}.\n",
            f"Rows loaded: {self.rows_loaded}. Data dir {f(n['data'], GIB)} "
            f"GiB, {f(n['sst'], 1, 0)} SST files, RSS {f(n['rss'], GIB)} "
            f"GiB, L0 tables {f(n['l0'], 1, 0)}, trees "
            f"{f(n['trees'], 1, 0)}.\n",
            f"Server restarts: {len(self.restarts)}. Workload runs: "
            f"{self.workload_run}.\n",
            "See load-summary.md (latency and disk bytes of the load), "
            "progress.md, INCIDENTS.md, results/, dumps/, watch.log.\n",
        ]
        (self.out / "SUMMARY-auto.md").write_text("\n".join(text))

    def loop(self):
        self.note(f"watch start phase={self.phase} out={self.out}")
        while self.phase != "done":
            now = time.time()
            if not bench.server_running(self.run):
                self.dump("server-dead")
                self.restart_server(
                    f"pid {self.run.pid('server.pid')} not running")
            n = self.numbers()
            if now - self.last_status >= self.status_secs:
                self.status(n)
                self.last_status = now
            if now - self.last_progress >= self.progress_secs:
                self.progress_row(n)
                self.last_progress = now
            if self.phase == "load":
                self.tick_load(n)
            elif self.phase == "workload":
                self.tick_workload(n)
            time.sleep(60)
        n = self.numbers()
        self.status(n)
        self.summary(n)
        self.note("watch done")
