#!/usr/bin/env python3
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

"""A long run: days of load on one Egres server, with faults, checked
against two oracles. Not a benchmark; the point is to find bugs.

    longrun.py begin [--set load.last=N] [--owners N] [--keys N]
                     the run dir, the server, the bank loaded, the
                     ledger created; then `run`
    longrun.py run   the driver, detached (--foreground to stay)
    longrun.py check the oracles once, by hand, clients or not
    longrun.py status
    longrun.py stop  the driver, the clients and the server

The run is the `egres-bank` env of ../run/bench.py: its paths, server
start and stop, pgbench and monitoring are that harness's. The state
of the workload is held steady, so the data turns over through every
level for as long as the run lasts: the bank's balances move without
the tables growing, history is trimmed, and the ledger deletes and
re-inserts its keys.

The two oracles:

  - The bank. pgbench runs the TPC-B-like transaction (workloads.py,
    `bank`): a delta into one account, its group of 100 accounts, its
    branch and one of the branch's tellers, and a history row.
    Checker threads read one group (its row and its 100 accounts) or
    one branch (its row and its 10 tellers) in one snapshot, all the
    time, and the rows must sum to the row: a difference is a lost or
    torn transaction, or a read that is not one snapshot. Between
    faults the branch rows and the teller rows are summed, which is
    the whole bank.
  - The ledger. Owner threads each update, delete and re-insert keys
    of their own, one transaction per psql process, and remember what
    they committed in a journal. A transaction whose outcome the
    server never answered (the connection died at COMMIT) is pending:
    either state is accepted for that key until the next verify sees
    which one it was. The verify reads an owner's keys back and
    compares every one: sequence, body (a hash of key and sequence),
    presence. Reads inside the transactions check keys as they go.

The driver's cycle: clients on for a random 50 to 70 minutes, with a
status line, a log scan and both checks every hour; then a fault, a
SIGKILL three times in four and a SIGTERM otherwise, at a moment of
the driver's choosing under load; the server restarted; both checks
in the quiet; old history deleted; every third cycle an idle stretch
of half an hour to an hour, checked again at its end. A server that
dies on its own is an incident, with the log tail kept, and the cycle
moves to the restart. Everything is in INCIDENTS.md, longrun.log and
textfile/longrun.prom; a check that fails stops the run with the
server up, for a look.
"""

import argparse
import hashlib
import json
import os
import random
import re
import resource
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "run"))

import bench  # noqa: E402
import pgbench  # noqa: E402
import workloads  # noqa: E402

ENV = "egres-bank"
GIB = 2**30
LEDGER_DDL = """\
CREATE TABLE ledger (
  k BIGINT PRIMARY KEY,
  owner BIGINT,
  seq BIGINT,
  body TEXT
);
CREATE INDEX ledger_owner ON ledger (owner);
"""
ROWS_PER_STMT = 200
# History buckets: `:run * 100000 + :n / 10000` per client. Every
# client passes through every bucket number up to its own count, so a
# run's buckets are a gapless range from its base.
HISTORY_BUCKETS_PER_RUN = 100_000
TRIM_BATCH = 50

# The schedule, in seconds unless named otherwise.
DEFAULTS = {
    "clients": 16,
    "max_tries": 20,
    "owners": 4,
    "keys": 100_000,
    "checkers": 2,
    "active_min": 50 * 60,
    "active_max": 70 * 60,
    "sigterm_ratio": 0.25,
    "idle_every": 3,
    "idle_min": 1800,
    "idle_max": 3600,
    "check_every": 3600,
    "status_every": 600,
}


def utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# ---------------------------------------------------------------------------
# psql
# ---------------------------------------------------------------------------

def psql(run, script: str, timeout=None) -> subprocess.CompletedProcess:
    """One psql over stdin: each statement its own message, which is
    what the server wants; tags (UPDATE 1, COMMIT) on stdout, errors
    on stderr, and the first error ends the script."""
    return subprocess.run(
        [str(run.psql), run.conn, "-X", "-tA", "-v", "ON_ERROR_STOP=1"],
        input=script, capture_output=True, text=True, timeout=timeout)


def insert_rows(run, prefix: str, rows, note: str):
    """Multi-row INSERTs through one psql, `ROWS_PER_STMT` a statement."""
    lines = []
    batch = []

    def flush():
        if batch:
            lines.append(prefix + "\n" + ",\n".join(batch) + ";")
            batch.clear()
    for row in rows:
        batch.append(row)
        if len(batch) == ROWS_PER_STMT:
            flush()
    flush()
    res = psql(run, "\n".join(lines) + "\n")
    if res.returncode != 0:
        sys.exit(f"{note}: psql failed:\n{res.stderr[-2000:]}")


# ---------------------------------------------------------------------------
# The ledger
# ---------------------------------------------------------------------------

def body_of(k: int, seq: int) -> str:
    """What a row must hold for its key and sequence: anything else is
    a row the server made up or mixed up."""
    h = hashlib.blake2b(f"{k}:{seq}".encode(), digest_size=8).hexdigest()
    return f"{k}:{seq}:{h}"


class Owner(threading.Thread):
    """One owner: keys [first, first + keys) of the ledger table, a
    journal of what it did to them, and a loop of transactions.

    Per key the journal holds the committed state (live and seq, or
    deleted) and, while a transaction's outcome is unknown, the state
    it was trying to reach. Lines: `A k seq live` an attempt, `C k seq
    live` committed, `F k` failed before COMMIT (state unchanged), `R k
    seq live` resolved by a verify. A trailing A is a pending key."""

    def __init__(self, driver, idx: int):
        super().__init__(daemon=True, name=f"owner-{idx}")
        self.driver = driver
        self.bench_run = driver.run
        self.idx = idx
        self.first = idx * driver.cfg["keys"]
        self.keys = driver.cfg["keys"]
        self.path = driver.out / "ledger" / f"owner-{idx}.log"
        self.path.parent.mkdir(exist_ok=True)
        # k -> (live, seq); pending: k -> (live, seq) being attempted.
        self.state: dict = {}
        self.pending: dict = {}
        self.counts = {"committed": 0, "failed": 0, "unknown": 0,
                       "mismatch": 0}
        self.lock = threading.Lock()
        # Held through a transaction; a verify takes it, so it never
        # reads while a transaction of this owner is in flight.
        self.busy = threading.Lock()
        self.on = threading.Event()
        self.stop = False
        self.rand = random.Random(idx * 7919 + int(time.time()))
        self.load()
        self.journal = open(self.path, "a")

    def load(self):
        for k in range(self.first, self.first + self.keys):
            self.state[k] = (True, 0)
        if not self.path.exists():
            return
        for line in self.path.read_text().splitlines():
            parts = line.split()
            op, k = parts[0], int(parts[1])
            if op == "A":
                self.pending[k] = (parts[3] == "1", int(parts[2]))
            elif op in ("C", "R"):
                self.state[k] = (parts[3] == "1", int(parts[2]))
                self.pending.pop(k, None)
            elif op == "F":
                self.pending.pop(k, None)

    def note(self, op: str, k: int, live=None, seq=None):
        tail = "" if seq is None else f" {seq} {int(live)}"
        self.journal.write(f"{op} {k}{tail}\n")
        self.journal.flush()

    def expected(self, k: int):
        """The states the server may show for k: one, or two while a
        transaction's outcome is unknown."""
        states = [self.state[k]]
        if k in self.pending:
            states.append(self.pending[k])
        return states

    def check_row(self, k: int, seq, body) -> bool:
        """Whether a row read back (seq and body, or None when absent)
        is one of the states k may be in."""
        for live, s in self.expected(k):
            if not live and seq is None:
                return True
            if live and seq == s and body == body_of(k, s):
                return True
        return False

    def mismatch(self, what: str):
        with self.lock:
            self.counts["mismatch"] += 1
        self.driver.ledger_mismatch(f"owner {self.idx}: {what}")

    def transaction(self):
        """One transaction on a settled key (no pending outcome), with
        a read of another settled key in the same transaction a third
        of the time."""
        settled = [k for k in self.rand.sample(
            range(self.first, self.first + self.keys), 16)
            if k not in self.pending]
        if not settled:
            return
        k = settled[0]
        live, seq = self.state[k]
        nxt = seq + 1
        if live and self.rand.random() < 0.15:
            stmt = f"DELETE FROM ledger WHERE k = {k};"
            tag, target = "DELETE 1", (False, nxt)
        elif live:
            stmt = (f"UPDATE ledger SET seq = {nxt}, "
                    f"body = '{body_of(k, nxt)}' WHERE k = {k};")
            tag, target = "UPDATE 1", (True, nxt)
        else:
            stmt = (f"INSERT INTO ledger (k, owner, seq, body) VALUES "
                    f"({k}, {self.idx}, {nxt}, '{body_of(k, nxt)}');")
            tag, target = "INSERT 0 1", (True, nxt)
        read = settled[1] if len(settled) > 1 and \
            self.rand.random() < 0.33 else None
        script = "BEGIN;\n"
        if read is not None:
            script += f"SELECT seq, body FROM ledger WHERE k = {read};\n"
        script += stmt + "\nCOMMIT;\n"
        self.pending[k] = target
        self.note("A", k, *target)
        try:
            res = psql(self.bench_run, script, timeout=120)
        except subprocess.TimeoutExpired:
            with self.lock:
                self.counts["unknown"] += 1
            self.driver.note(f"owner {self.idx}: psql timed out on k={k}")
            return
        out = res.stdout.splitlines()
        # The read's row (seq|body) between BEGIN and the write's tag,
        # or nothing when the key is deleted. Only judged when the
        # write's tag is there: that proves the read ran, where a
        # connection lost at the read also shows no row.
        if read is not None and tag in out:
            got = [l for l in out[1:] if "|" in l]
            r_seq, r_body = (None, None)
            if got:
                a, _, b = got[0].partition("|")
                r_seq, r_body = int(a), b
            if not self.check_row(read, r_seq, r_body):
                self.mismatch(f"read k={read} got seq={r_seq} "
                              f"body={r_body!r}, expected one of "
                              f"{self.expected(read)}")
        if res.returncode == 0 and "COMMIT" in out:
            self.pending.pop(k, None)
            if tag not in out:
                self.mismatch(f"k={k} {stmt[:40]}... committed with tags "
                              f"{[l for l in out if '|' not in l]}, "
                              f"expected {tag}")
            self.state[k] = target
            self.note("C", k, *target)
            with self.lock:
                self.counts["committed"] += 1
        elif "ERROR:" not in res.stderr and tag in out:
            # The write went through and COMMIT got no answer: the
            # connection died. An ERROR is an answer, and a rollback.
            with self.lock:
                self.counts["unknown"] += 1
        elif not out:
            # Not even BEGIN answered: the connection was refused, as
            # while the server drains after SIGTERM. The key's state is
            # unchanged (F), but it is no failed transaction to count,
            # and the loop waits instead of spinning on refusals.
            self.pending.pop(k, None)
            self.note("F", k)
            time.sleep(1)
        else:
            self.pending.pop(k, None)
            self.note("F", k)
            with self.lock:
                self.counts["failed"] += 1

    def run_loop(self):
        while not self.stop:
            if not self.on.wait(1):
                continue
            if not self.driver.server_up():
                time.sleep(1)
                continue
            with self.busy:
                self.transaction()

    def run(self):
        try:
            self.run_loop()
        except Exception as e:  # noqa: BLE001
            self.driver.incident(f"owner {self.idx} thread died", repr(e))

    def verify(self, resolve: bool = True) -> int:
        """Read every key back and compare; with `resolve`, settle
        pending keys to what the server shows. Returns the mismatch
        count. Waits for a transaction in flight; the owner should be
        paused, or the read lands between its transactions."""
        with self.busy:
            res = psql(self.bench_run, "SELECT k, seq, body FROM ledger "
                       f"WHERE owner = {self.idx};\n", timeout=600)
            if res.returncode != 0:
                raise RuntimeError(
                    f"ledger read failed: {res.stderr[-500:]}")
            return self.compare(res.stdout, resolve)

    def compare(self, text: str, resolve: bool) -> int:
        rows = {}
        for line in text.splitlines():
            k, seq, body = line.split("|", 2)
            rows[int(k)] = (int(seq), body)
        bad = 0
        samples = []
        for k in range(self.first, self.first + self.keys):
            seq, body = rows.pop(k, (None, None))
            if not self.check_row(k, seq, body):
                bad += 1
                if len(samples) < 10:
                    samples.append(f"k={k} server seq={seq} body={body!r} "
                                   f"expected one of {self.expected(k)}")
            elif k in self.pending and resolve:
                # Present: the server's sequence is the truth. Absent:
                # the pending delete went through, or the pending
                # insert did not and the key stays deleted as it was.
                if seq is not None:
                    self.state[k] = (True, seq)
                elif not self.pending[k][0]:
                    self.state[k] = self.pending[k]
                self.pending.pop(k)
                self.note("R", k, *self.state[k])
        for k in rows:
            bad += 1
            if len(samples) < 10:
                samples.append(f"k={k} is not this owner's key")
        if bad:
            self.mismatch(f"verify: {bad} keys wrong; " + "; ".join(samples))
        return bad


# ---------------------------------------------------------------------------
# The bank
# ---------------------------------------------------------------------------

def bank_totals(run) -> dict:
    """The branch rows and the teller rows in one snapshot: per
    branch the tellers must sum to the branch row, and so the whole
    bank agrees. A branch where they do not is listed. The accounts
    are the checkers' job, a group at a time."""
    script = ("BEGIN;\n\\echo @@branches\n"
              "SELECT bid, bbalance FROM pgbench_branches;\n"
              "\\echo @@tellers\nSELECT bid, tbalance FROM pgbench_tellers;\n"
              "COMMIT;\n")
    res = psql(run, script, timeout=600)
    if res.returncode != 0:
        raise RuntimeError(f"bank read failed: {res.stderr[-500:]}")
    sums = {"branches": {}, "tellers": {}}
    section = None
    for line in res.stdout.splitlines():
        if line.startswith("@@"):
            section = line[2:]
        elif section in sums and "|" in line:
            bid, _, val = line.partition("|")
            d = sums[section]
            d[bid] = d.get(bid, 0) + int(val)
    bad = []
    for bid in sorted(sums["branches"], key=int):
        b = sums["branches"][bid]
        t = sums["tellers"].get(bid, 0)
        if t != b:
            bad.append(f"branch {bid}: tellers {t} branch {b}")
    for bid in sorted(sums["tellers"].keys() - sums["branches"].keys()):
        bad.append(f"branch {bid}: tellers without a branch row")
    total = sum(sums["branches"].values())
    return {"ok": not bad, "bad": bad, "total": total,
            "branches": len(sums["branches"])}


class Checker(threading.Thread):
    """One checker: a loop of snapshot reads, each one transaction
    through psql, of one group (its row, then its accounts by id) or,
    one time in ten, one branch (its row, then its tellers). The rows
    must sum to the row and every id must be there once; anything
    else is a bank mismatch. Runs whenever the server is up, pgbench
    or not: the reads are a second or so, well inside the floor."""

    def __init__(self, driver, idx: int, accounts: int):
        super().__init__(daemon=True, name=f"checker-{idx}")
        self.driver = driver
        self.bench_run = driver.run
        self.idx = idx
        self.accounts = accounts
        self.groups = workloads.bank_groups(accounts)
        self.branches = workloads.bank_branches(accounts)
        self.counts = {"groups": 0, "branches": 0, "failed": 0,
                       "mismatch": 0}
        self.lock = threading.Lock()
        self.stop = False
        self.errors_noted = 0
        self.rand = random.Random(idx * 104729 + int(time.time()))

    def count(self, key: str):
        with self.lock:
            self.counts[key] += 1

    def read(self):
        branch = self.rand.random() < 0.1
        if branch:
            per = workloads.BANK_TELLERS_PER_BRANCH
            no = self.rand.randrange(1, self.branches + 1)
            ids = range((no - 1) * per + 1, no * per + 1)
            head = f"SELECT bbalance FROM pgbench_branches WHERE bid = {no};"
            row = "SELECT tid, tbalance FROM pgbench_tellers WHERE tid = "
        else:
            per = workloads.BANK_ACCOUNTS_PER_GROUP
            no = self.rand.randrange(1, self.groups + 1)
            ids = range((no - 1) * per + 1,
                        min(no * per, self.accounts) + 1)
            head = f"SELECT gbalance FROM pgbench_groups WHERE gid = {no};"
            row = "SELECT aid, abalance FROM pgbench_accounts WHERE aid = "
        name = f"{'branch' if branch else 'group'} {no}"
        script = ("BEGIN;\n\\echo @@head\n" + head + "\n\\echo @@rows\n" +
                  "".join(f"{row}{i};\n" for i in ids) + "COMMIT;\n")
        try:
            res = psql(self.bench_run, script, timeout=120)
        except subprocess.TimeoutExpired:
            self.count("failed")
            self.driver.note(f"checker {self.idx}: psql timed out on {name}")
            return
        out = res.stdout.splitlines()
        if res.returncode != 0 or "COMMIT" not in out:
            # No snapshot to judge: the server down or the connection
            # lost at a fault. An ERROR is the server's answer, worth
            # a look; the first few are noted. The pause keeps a
            # restart from counting hundreds of refused connections.
            self.count("failed")
            if "ERROR:" in res.stderr and self.errors_noted < 20:
                self.errors_noted += 1
                self.driver.note(f"checker {self.idx}: {name}: "
                                 f"{res.stderr.strip()[-300:]}")
            time.sleep(1)
            return
        self.count("branches" if branch else "groups")
        head_val = None
        rows: dict = {}
        section = None
        for line in out:
            if line.startswith("@@"):
                section = line[2:]
            elif section == "head" and line.lstrip("-").isdigit():
                head_val = int(line)
            elif section == "rows" and "|" in line:
                i, _, val = line.partition("|")
                rows.setdefault(int(i), []).append(int(val))
        got = [v for vals in rows.values() for v in vals]
        if head_val is not None and sum(got) == head_val and \
                len(got) == len(ids) and set(rows) == set(ids):
            return
        missing = sorted(set(ids) - set(rows))
        extra = sorted(set(rows) - set(ids))
        dup = sorted(i for i, vals in rows.items() if len(vals) > 1)
        self.count("mismatch")
        # One is enough: the driver brings the check forward and the
        # run stops, where reading on would report the same rows again
        # and again.
        self.stop = True
        self.driver.bank_mismatch(
            f"checker {self.idx}: {name} row={head_val} sum={sum(got)} "
            f"rows={len(got)} of {len(ids)} missing={missing[:20]} "
            f"extra={extra[:20]} duplicated={dup[:20]}; rows: " +
            " ".join(f"{i}={vals}" for i, vals in sorted(rows.items())))

    def run_loop(self):
        while not self.stop:
            if not self.driver.server_up():
                time.sleep(1)
                continue
            self.read()

    def run(self):
        try:
            self.run_loop()
        except Exception as e:  # noqa: BLE001
            self.driver.incident(f"checker {self.idx} thread died", repr(e))


# ---------------------------------------------------------------------------
# The driver
# ---------------------------------------------------------------------------

class Driver:
    def __init__(self, run, cfg: dict):
        self.run = run
        self.cfg = cfg
        self.out = run.out
        self.log = self.out / "longrun.log"
        self.incidents = self.out / "INCIDENTS.md"
        self.state_path = self.out / "longrun-state.json"
        self.state = {"cycle": 0, "pg_run": 0, "kills": 0,
                      "checks_ok": 0, "checks_failed": 0,
                      "incidents": 0, "mismatches": 0,
                      "bank_mismatches": 0, "untrimmed": {}}
        if self.state_path.exists():
            self.state.update(json.loads(self.state_path.read_text()))
        self.owners = [Owner(self, i) for i in range(cfg["owners"])]
        accounts = pgbench.rows_loaded(run)
        self.checkers = [Checker(self, i, accounts)
                         for i in range(cfg["checkers"])]
        self.pg = None
        self.pg_thread = None
        self.pg_result = None
        self.pg_started = 0.0
        # A resumed driver counts only what the server logs from now
        # on; start_server rewinds to 0 for a fresh log.
        self.log_pos = 0
        if run.server_log.exists():
            self.log_pos = run.server_log.stat().st_size
        self.stopping = False
        self.rand = random.Random()
        self.mismatches_seen = self.mismatches()
        # Owners and checkers record incidents from their own threads.
        self.state_lock = threading.RLock()

    # -- notes --------------------------------------------------------

    def note(self, msg: str):
        line = f"{utc()} {msg}"
        with open(self.log, "a") as f:
            f.write(line + "\n")
        try:
            print(line, flush=True)
        except OSError:
            pass  # stdout gone (a pipe closed); the log is the record

    def incident(self, title: str, body: str):
        with self.state_lock:
            self.state["incidents"] += 1
            self.note(f"INCIDENT: {title}")
            with open(self.incidents, "a") as f:
                if f.tell() == 0:
                    f.write("# Incidents\n\n")
                f.write(f"## {utc()}: {title}\n\n{body}\n\n")
            self.save()

    def ledger_mismatch(self, what: str):
        with self.state_lock:
            self.state["mismatches"] += 1
            self.incident("ledger mismatch", what)

    def bank_mismatch(self, what: str):
        with self.state_lock:
            self.state["bank_mismatches"] += 1
            self.incident("bank mismatch", what)

    def mismatches(self) -> int:
        return self.state["mismatches"] + self.state["bank_mismatches"]

    def save(self):
        with self.state_lock:
            tmp = self.state_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.state, indent=2) + "\n")
            os.replace(tmp, self.state_path)

    def log_tail(self, tag: str):
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        d = self.out / "dumps"
        d.mkdir(exist_ok=True)
        tail = subprocess.run(["tail", "-n", "300",
                               str(self.run.server_log)],
                              capture_output=True, text=True).stdout
        (d / f"{stamp}-{tag}-server.log.tail").write_text(tail)

    # -- the server ---------------------------------------------------

    def server_up(self) -> bool:
        return bench.server_running(self.run)

    def start_server(self):
        # A crash must leave a core beside the run: the limit is
        # inherited by the server.
        resource.setrlimit(resource.RLIMIT_CORE, (resource.RLIM_INFINITY,
                                                  resource.RLIM_INFINITY))
        bench.cmd_start(self.run)
        self.log_pos = 0

    def kill_server(self, sig: int):
        pid = self.run.pid("server.pid")
        name = "SIGKILL" if sig == signal.SIGKILL else "SIGTERM"
        self.note(f"fault: {name} to server pid {pid}")
        self.run.event(f"longrun fault {name}")
        self.log_tail(f"before-{name.lower()}")
        os.kill(pid, sig)
        for _ in range(120):
            if not bench.alive(pid):
                break
            time.sleep(1)
        else:
            self.incident(f"server ignored {name} for 120 s",
                          f"pid {pid}; sending SIGKILL")
            os.kill(pid, signal.SIGKILL)
            time.sleep(5)
        self.state["kills"] += 1
        self.save()

    # -- the clients --------------------------------------------------

    def start_pgbench(self):
        self.state["pg_run"] += 1
        no = self.state["pg_run"]
        self.save()
        accounts = pgbench.rows_loaded(self.run)
        branches = workloads.bank_branches(accounts)
        extra = {"accounts": accounts, "branches": branches,
                 "tellers": branches * workloads.BANK_TELLERS_PER_BRANCH,
                 "run": no, "n": 0}
        name = f"bank-c{self.cfg['clients']}-run{no}"
        log = self.run.results / f"pgbench-{name}.log"
        lat = self.run.results / "latency" / name
        # Long enough that only a fault or a stop ends it.
        self.pg = pgbench.start(self.run, log, "pgbench", name,
                                self.cfg["clients"], 7 * 86400,
                                self.run.env["workload"]["mix"], extra,
                                lat, 0.01, self.cfg["max_tries"])
        (self.out / "workload.pid").write_text(f"{self.pg.pid}\n")
        self.pg_result = None

        def go():
            self.pg_result = pgbench.follow(self.pg)
        self.pg_thread = threading.Thread(target=go, daemon=True)
        self.pg_thread.start()
        self.pg_started = time.time()
        self.note(f"pgbench {name} started pid={self.pg.pid}")

    def stop_pgbench(self):
        """Wait for pgbench to end on its own (its connections died
        with the server), then make sure."""
        if self.pg is None:
            return
        self.pg_thread.join(60)
        if self.pg.alive():
            try:
                os.killpg(self.pg.pid, signal.SIGTERM)
            except OSError:
                pass
            self.pg_thread.join(30)
        res = self.pg_result or {}
        txns = res.get("transactions") or 0
        self.note(f"pgbench run {self.state['pg_run']} ended: "
                  f"tps={res.get('tps')} transactions={txns} "
                  f"failed={res.get('failed')}")
        # Its history is trimmed once a newer run has ended.
        self.state["untrimmed"][str(self.state["pg_run"])] = True
        self.save()
        self.pg = None

    def pgbench_dead(self) -> bool:
        return self.pg is not None and not self.pg.alive()

    def clients_on(self, on: bool):
        for o in self.owners:
            if on:
                o.on.set()
            else:
                o.on.clear()

    def trim_history(self):
        """Delete the history of pgbench runs two or more back, bucket
        by bucket from each run's base until a whole batch of buckets
        deletes nothing."""
        current = self.state["pg_run"]
        for key in sorted(self.state["untrimmed"], key=int):
            no = int(key)
            if no >= current - 1:
                continue
            base = no * HISTORY_BUCKETS_PER_RUN
            deleted = buckets = 0
            while buckets < HISTORY_BUCKETS_PER_RUN:
                script = "".join(
                    f"DELETE FROM pgbench_history WHERE bucket = {b};\n"
                    for b in range(base + buckets,
                                   base + buckets + TRIM_BATCH))
                res = psql(self.run, script, timeout=3600)
                if res.returncode != 0:
                    self.incident(f"history trim of run {no} failed",
                                  res.stderr[-1000:])
                    return
                got = sum(int(l.split()[1])
                          for l in res.stdout.splitlines()
                          if l.startswith("DELETE "))
                deleted += got
                buckets += TRIM_BATCH
                if got == 0:
                    break
            self.note(f"trimmed history of run {no}: {deleted} rows in "
                      f"{buckets} buckets")
            del self.state["untrimmed"][key]
            self.save()

    # -- checks -------------------------------------------------------

    def new_log_lines(self) -> dict:
        """Counts of panic, error and warning lines in the server log
        since the last call, with one sample of each."""
        counts = {"panic": 0, "error": 0, "warning": 0}
        sample = {}
        log = self.run.server_log
        try:
            size = log.stat().st_size
            if size < self.log_pos:
                self.log_pos = 0
            with open(log, "rb") as f:
                f.seek(self.log_pos)
                chunk = f.read(size - self.log_pos)
                self.log_pos = size
        except OSError:
            return counts
        marks = {"error": "ERROR", "warning": "WARN"}
        for line in chunk.decode(errors="replace").splitlines():
            # `<time> WARN  <scope>: ...`: the level is the second
            # field. A panic is Zig's own text, anywhere in the line.
            level = line.split(None, 2)[1:2]
            for key in counts:
                if (key == "panic" and key in line) or \
                        level == [marks.get(key)]:
                    counts[key] += 1
                    sample.setdefault(key, line[:300])
        for key, line in sample.items():
            self.note(f"  log {key}: {line}")
        if counts["panic"]:
            self.incident("panic line in the server log", sample["panic"])
        return counts

    def numbers(self) -> dict:
        pid = self.run.pid("server.pid")
        rss = fds = None
        try:
            for line in open(f"/proc/{pid}/status"):
                if line.startswith("VmRSS"):
                    rss = int(line.split()[1]) * 1024
            fds = len(os.listdir(f"/proc/{pid}/fd"))
        except OSError:
            pass
        data = wal = ssts = 0
        for root, _dirs, files in os.walk(self.run.data):
            for f in files:
                try:
                    data += os.lstat(os.path.join(root, f)).st_size
                except OSError:
                    continue
                ssts += f.endswith(".sst")
        for root, _dirs, files in os.walk(self.run.wal):
            for f in files:
                try:
                    wal += os.lstat(os.path.join(root, f)).st_size
                except OSError:
                    continue
        tps = self.pg.last[0] if self.pg is not None and self.pg.last \
            else None
        return {"rss": rss, "fds": fds, "data": data, "wal": wal,
                "ssts": ssts, "tps": tps,
                "free": shutil.disk_usage(self.run.data).free}

    def status(self, phase: str):
        n = self.numbers()
        counts = self.new_log_lines()
        ledger = {k: sum(o.counts[k] for o in self.owners)
                  for k in self.owners[0].counts} if self.owners else {}
        bank = {k: sum(c.counts[k] for c in self.checkers)
                for k in self.checkers[0].counts} if self.checkers else {}

        def gib(v):
            return "-" if v is None else f"{v / GIB:.1f}"
        self.note(
            f"phase={phase} cycle={self.state['cycle']} "
            f"kills={self.state['kills']} rss={gib(n['rss'])}GiB "
            f"fds={n['fds']} data={gib(n['data'])}GiB wal={gib(n['wal'])}GiB "
            f"ssts={n['ssts']} free={gib(n['free'])}GiB tps={n['tps']} "
            f"ledger={ledger} bank={bank} log=" +
            " ".join(f"{k}={v}" for k, v in counts.items()))
        self.write_prom(n, ledger, bank)

    def write_prom(self, n: dict, ledger: dict, bank: dict):
        lines = []
        for key in ("cycle", "kills", "checks_ok", "checks_failed",
                    "incidents", "mismatches", "bank_mismatches"):
            lines.append(f"longrun_{key} {self.state[key]}")
        for key, val in ledger.items():
            lines.append(f'longrun_ledger_txns{{outcome="{key}"}} {val}')
        for key, val in bank.items():
            lines.append(f'longrun_bank_reads{{outcome="{key}"}} {val}')
        for key in ("rss", "fds", "data", "wal", "ssts"):
            if n.get(key) is not None:
                lines.append(f"longrun_server_{key} {n[key]}")
        path = self.run.textfile / "longrun.prom"
        tmp = path.with_suffix(".tmp")
        tmp.write_text("\n".join(lines) + "\n")
        os.replace(tmp, path)

    def check(self, when: str, resolve: bool = True) -> bool:
        """Both oracles, the ledger owners paused meanwhile. False
        stops the run."""
        self.clients_on(False)
        ok = True
        t0 = time.time()
        try:
            bank = bank_totals(self.run)
        except RuntimeError as e:
            self.note(f"bank totals failed to read: {e}; retrying once")
            time.sleep(5)
            try:
                bank = bank_totals(self.run)
            except RuntimeError as e2:
                self.incident("bank totals could not be read twice",
                              str(e2))
                bank = {"ok": True, "bad": [], "total": None,
                        "branches": None}
        if not bank["ok"]:
            ok = False
            self.incident(f"bank sums differ ({when})",
                          "\n".join(bank["bad"][:50]))
        wrong = 0
        for o in self.owners:
            try:
                wrong += o.verify(resolve)
            except RuntimeError as e:
                self.incident(f"ledger verify of owner {o.idx} failed to "
                              f"read ({when})", str(e))
        # A read that saw the wrong thing since the last check, by an
        # owner inside a transaction or by a checker, counts even when
        # the verify finds the keys right now: a wrong read is a bug.
        if wrong or self.mismatches() > self.mismatches_seen:
            ok = False
        self.mismatches_seen = self.mismatches()
        self.state["checks_ok" if ok else "checks_failed"] += 1
        self.save()
        self.note(f"check ({when}): {'ok' if ok else 'FAILED'} in "
                  f"{time.time() - t0:.0f}s; bank total={bank['total']} "
                  f"branches={bank['branches']}; ledger wrong={wrong}")
        return ok

    # -- the loop -----------------------------------------------------

    def active(self) -> str:
        """Clients on until the planned fault, a server death, or a
        stop. Returns why it ended."""
        secs = self.rand.uniform(self.cfg["active_min"],
                                 self.cfg["active_max"])
        self.note(f"active for {secs / 3600:.1f} h")
        self.start_pgbench()
        self.clients_on(True)
        t_end = time.time() + secs
        last_status = last_check = time.time()
        while time.time() < t_end:
            if self.stopping:
                return "stop"
            if not self.server_up():
                return "died"
            if self.pgbench_dead():
                self.incident("pgbench ended on its own",
                              f"see {self.pg.log}")
                if time.time() - self.pg_started < 60:
                    return "pgbench-broken"
                return "pgbench"
            now = time.time()
            if now - last_status >= self.cfg["status_every"]:
                self.status("active")
                last_status = now
            # A mismatch seen by an owner or a checker brings the check
            # forward: it fails, and the run stops now rather than at
            # the hour.
            if now - last_check >= self.cfg["check_every"] or \
                    self.mismatches() > self.mismatches_seen:
                if not self.check("under load"):
                    return "check"
                self.clients_on(True)
                last_check = time.time()
            time.sleep(5)
        return "fault"

    def cycle(self) -> bool:
        """One cycle; False when the run must stop."""
        self.state["cycle"] += 1
        self.save()
        why = self.active()
        if why == "stop":
            self.clients_on(False)
            self.stop_pgbench()
            return False
        if why in ("check", "pgbench-broken"):
            self.clients_on(False)
            self.stop_pgbench()
            return False
        if why == "died":
            self.log_tail("server-died")
            self.incident("server died on its own",
                          f"pid {self.run.pid('server.pid')}; log tail in "
                          "dumps/; the log itself is rotated at restart")
            self.clients_on(False)
            self.stop_pgbench()
        elif why == "fault":
            sig = signal.SIGTERM if self.rand.random() < \
                self.cfg["sigterm_ratio"] else signal.SIGKILL
            self.kill_server(sig)
            self.clients_on(False)
            self.stop_pgbench()
        else:
            self.clients_on(False)
            self.stop_pgbench()
        if self.server_up():
            # pgbench died: the server is fine, keep it.
            pass
        else:
            try:
                self.start_server()
            except SystemExit as e:
                self.incident("server restart failed", str(e))
                return False
        if not self.check("after restart"):
            return False
        self.trim_history()
        if self.state["cycle"] % self.cfg["idle_every"] == 0:
            secs = self.rand.uniform(self.cfg["idle_min"],
                                     self.cfg["idle_max"])
            self.note(f"idle for {secs / 60:.0f} min")
            t_end = time.time() + secs
            while time.time() < t_end and not self.stopping and \
                    self.mismatches() == self.mismatches_seen:
                if not self.server_up():
                    self.log_tail("server-died-idle")
                    self.incident("server died while idle", "")
                    self.start_server()
                time.sleep(5)
            self.status("idle")
            if not self.check("after idle"):
                return False
        return not self.stopping

    def loop(self):
        (self.out / "longrun.pid").write_text(f"{os.getpid()}\n")
        def on_term(*_):
            self.stopping = True
            self.note("stop requested; finishing the cycle")
        signal.signal(signal.SIGTERM, on_term)
        for t in self.owners + self.checkers:
            t.start()
        self.note(f"longrun start cycle={self.state['cycle']} cfg={self.cfg}")
        if not self.server_up():
            self.start_server()
        try:
            while self.cycle():
                pass
        finally:
            for t in self.owners + self.checkers:
                t.stop = True
            self.status("end")
            self.note("longrun done; the server is left running")
            (self.out / "longrun.pid").unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def make_run(a) -> bench.Run:
    env = bench.resolve_env(ENV, a.set)
    local = bench.load_local()
    for item in a.local:
        key, _, val = item.partition("=")
        local[key] = val
    run = bench.Run(ENV, a.name or "", env, local)
    run.no_monitor = a.no_monitor
    run.sets, run.locals = a.set, a.local
    # The binary pinned at begin: a rebuild must not change the server
    # under a run that lasts days. Not for begin itself: its run dir
    # does not exist yet, so run.out is the previous run's (through
    # the .latest link), and that run's binary must not carry over.
    pinned = run.out / "egres.bin"
    if a.command != "begin" and pinned.exists():
        run.egres_bin = pinned
    return run


def cfg_path(run) -> Path:
    return run.out / "longrun.json"


def cmd_begin(run, a):
    cfg = dict(DEFAULTS)
    for key in cfg:
        val = getattr(a, key, None)
        if val is not None:
            cfg[key] = val
    bench.cmd_begin(run)
    shutil.copy2(run.egres_bin, run.out / "egres.bin")
    run.egres_bin = run.out / "egres.bin"
    cfg_path(run).write_text(json.dumps(cfg, indent=2) + "\n")
    resource.setrlimit(resource.RLIMIT_CORE, (resource.RLIM_INFINITY,
                                              resource.RLIM_INFINITY))
    bench.cmd_start(run)
    import loader
    print("loading the accounts", flush=True)
    loader.run_load(run, False)
    accounts = pgbench.rows_loaded(run)
    if accounts != run.env["load"]["last"]:
        sys.exit(f"the load stopped at {accounts} of "
                 f"{run.env['load']['last']} accounts; the bank needs "
                 "every id, so pick a load.last that fits")
    groups = workloads.bank_groups(accounts)
    branches = workloads.bank_branches(accounts)
    per = workloads.BANK_TELLERS_PER_BRANCH
    print(f"{accounts} accounts: {groups} groups, {branches} branches, "
          f"{branches * per} tellers", flush=True)
    insert_rows(run, "INSERT INTO pgbench_groups (gid, gbalance) VALUES",
                (f"({g}, 0)" for g in range(1, groups + 1)), "groups")
    insert_rows(run, "INSERT INTO pgbench_branches (bid, bbalance) VALUES",
                (f"({b}, 0)" for b in range(1, branches + 1)), "branches")
    insert_rows(run, "INSERT INTO pgbench_tellers (tid, bid, tbalance) VALUES",
                (f"({t}, {(t - 1) // per + 1}, 0)"
                 for t in range(1, branches * per + 1)), "tellers")
    res = psql(run, LEDGER_DDL)
    if res.returncode != 0:
        sys.exit(f"ledger: {res.stderr[-1000:]}")
    keys = cfg["owners"] * cfg["keys"]
    print(f"ledger: {cfg['owners']} owners, {keys} keys", flush=True)
    insert_rows(run, "INSERT INTO ledger (k, owner, seq, body) VALUES",
                (f"({k}, {k // cfg['keys']}, 0, '{body_of(k, 0)}')"
                 for k in range(keys)), "ledger")
    run.event(f"longrun begin accounts={accounts} ledger_keys={keys}")
    print(f"ready; now: longrun.py run" + (f" --name {a.name}" if a.name
                                            else ""))


def cmd_run(run, a):
    cfg = json.loads(cfg_path(run).read_text())
    if not a.foreground:
        if bench.alive(run.pid("longrun.pid")):
            sys.exit(f"driver already running: pid {run.pid('longrun.pid')}")
        cmd = [sys.executable, str(HERE / "longrun.py"), "run",
               "--foreground"]
        if a.name:
            cmd += ["--name", a.name]
        if a.no_monitor:
            cmd.append("--no-monitor")
        for s in a.set:
            cmd += ["--set", s]
        for s in a.local:
            cmd += ["--local", s]
        with open(run.out / "longrun.out", "w") as out:
            subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT,
                             stdin=subprocess.DEVNULL, start_new_session=True)
        time.sleep(2)
        print(f"driver pid {run.pid('longrun.pid')}; log "
              f"{run.out / 'longrun.log'}")
        return
    Driver(run, cfg).loop()


def cmd_check(run, a):
    cfg = json.loads(cfg_path(run).read_text())
    d = Driver(run, cfg)
    live = bench.alive(run.pid("longrun.pid"))
    if live:
        print("note: the driver is running: its owners go on, and a key "
              "in flight is read between its transactions")
    sys.exit(0 if d.check("by hand", resolve=not live) else 1)


def cmd_status(run, a):
    bench.cmd_status(run)
    for name in ("longrun.pid", "workload.pid"):
        pid = run.pid(name)
        print(f"{name}: {pid} "
              f"{'running' if bench.alive(pid) else 'not running'}")
    if (run.out / "longrun-state.json").exists():
        print((run.out / "longrun-state.json").read_text())
    log = run.out / "longrun.log"
    if log.exists():
        print("--- longrun.log tail")
        print("".join(log.read_text().splitlines(True)[-8:]), end="")


def cmd_stop(run, a):
    pid = run.pid("longrun.pid")
    if bench.alive(pid):
        os.kill(pid, signal.SIGTERM)
        for _ in range(120):
            if not bench.alive(pid):
                break
            time.sleep(1)
        print(f"stopped driver {pid}")
    bench.cmd_stop(run)


def main():
    ap = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["begin", "run", "check", "status",
                                        "stop"])
    ap.add_argument("--set", action="append", default=[],
                    help="a bench.py --set, e.g. load.last=50000000")
    ap.add_argument("--local", action="append", default=[],
                    help="a bench.py --local, e.g. wal_root=/wal")
    ap.add_argument("--name", help="suffix for the run name")
    ap.add_argument("--no-monitor", action="store_true")
    ap.add_argument("--foreground", action="store_true")
    for key, val in DEFAULTS.items():
        ap.add_argument(f"--{key}", type=type(val),
                        help=f"begin: the schedule's {key} (default {val})")
    a = ap.parse_args()
    run = make_run(a)
    {"begin": cmd_begin, "run": cmd_run, "check": cmd_check,
     "status": cmd_status, "stop": cmd_stop}[a.command](run, a)


if __name__ == "__main__":
    main()
