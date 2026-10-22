# A long run

Days of load on one Egres server, with faults, checked against two
oracles. Not a benchmark: the numbers it keeps are for spotting a
leak or a stall, and the point is a crash, a wrong sum, or a row that
is not what was committed.

It is the `egres-bank` env of `../run/bench.py` driven by
`longrun.py`: the paths, server start and stop, pgbench and the
monitoring stack are that harness's, so `../run/README.md` applies.
The workload is `bank` in `../run/workloads.py`.

## Setup

```
cp benchmarks/run/local.example.toml benchmarks/run/local.toml  # once
zig build -Doptimize=ReleaseSafe
ulimit -c unlimited
sudo sysctl -w kernel.core_pattern=/var/crash/core.%e.%p   # cores kept
benchmarks/longrun/longrun.py begin --set load.last=50000000
benchmarks/longrun/longrun.py run
```

ReleaseSafe, not ReleaseFast: a bad index or an overflow then panics
with a trace instead of corrupting something quietly. `begin` copies
the binary into the run dir and runs that copy, so a rebuild during
the run changes nothing.

`load.last` is the scale: the account count, about 600 bytes a row
on disk before compression. Size it at two to three times the box's
RAM so the cache, the chunk fetches and compaction are all in play:
50M accounts is about 30 GiB. The load takes a while at that size
(the orders load runs at 50k to 100k rows/s on a fast NVMe); `begin`
stays in the foreground through it. The ledger is small by design,
`--owners 4 --keys 100000` by default.

The schedule knobs are `begin` options, kept in the run dir's
`longrun.json`: `--active_min`/`--active_max` (seconds of load before
a fault, 50 to 70 minutes), `--sigterm_ratio` (the rest are SIGKILL),
`--idle_every` (every third cycle idles), `--idle_min`/`--idle_max`,
`--check_every` (both oracles, under load), `--status_every`,
`--clients` (pgbench), `--max_tries` (retries of a serialization
failure, which the bank's hot branch rows produce), `--checkers`
(bank readers, 2).

## What runs

- pgbench, `--clients` of them, on the bank's mix: 90% the TPC-B-like
  transaction, 10% a point read. It runs until the fault ends it.
- The ledger owners, threads of the driver, one psql process per
  transaction: an update, a delete or a re-insert of one of their own
  keys, with a read of another key in the same transaction a third
  of the time.
- The bank checkers, threads of the driver too, one transaction after
  another whenever the server is up: a random group of 100 accounts
  read with its group row, or one time in ten a branch with its ten
  tellers, and the rows must sum to the row.
- Every `--check_every`: a status line (RSS, fds, data and WAL bytes,
  SST count, free space, tps, the ledger's counts), a scan of the
  server log for panic, error and warning lines, and both checks.
- The fault: the server killed under load, restarted, both checks in
  the quiet, the history of the pgbench run before last deleted.
- Every third cycle: an idle stretch, then the checks again.

## The checks

**The bank.** The TPC-B-like transaction moves a delta into one
account, the account's group row (one per 100 accounts), its branch
and one of that branch's tellers, so in any one snapshot a group's
accounts sum to its row, a branch's tellers sum to its row, and the
branches and the tellers sum to the same number. The checkers read a
group or a branch in one transaction, about a hundred point reads,
continuously: a wrong sum, a missing or doubled id is a bank
mismatch, with the rows in the incident. The periodic check reads
all the branch and teller rows in one snapshot and compares them per
branch. Nothing scans the accounts: at 543M rows that took hours
under load and outlived the read floor; small reads sample the same
invariant millions of times a day instead.

**The ledger.** Each owner remembers, in `ledger/owner-N.log`, what it
committed per key: live or deleted, and the sequence number. A row's
body is a hash of its key and sequence, so a row the server returned
from the wrong place or the wrong version fails the comparison. A
transaction whose COMMIT got no answer (the server was killed) is
pending: the key may be in either state until the next verify sees
which, and records it. The verify reads an owner's keys back, with
the owner paused, and compares every key: present when it should be,
absent when deleted, the right sequence and body, and no key the
owner never had.

A check that fails stops the run with the server up, so the state can
be looked at; `longrun.py check` runs both by hand at any time.

## Output

In the run dir (`<out_root>/egres-bank-<time>`, `egres-bank.latest`
points at it): `longrun.log` (everything the driver did, status lines
included), `INCIDENTS.md`, `longrun-state.json` (cycle, kills, checks,
ledger and bank mismatches, the pgbench runs whose history is still
to trim),
`ledger/`, `dumps/` (the server log's tail before every fault and
after a death), `results/pgbench-*.log` (one per pgbench start), the
server log (rotated at every restart, so each starts at a process
start), and `textfile/longrun.prom` for Grafana: `longrun_cycle`,
`longrun_kills`, `longrun_checks_ok`, `longrun_checks_failed`,
`longrun_incidents`, `longrun_mismatches`, `longrun_bank_mismatches`,
`longrun_ledger_txns{outcome}`, `longrun_bank_reads{outcome}`,
`longrun_server_{rss,fds,data,wal,ssts}`.

`longrun.py status` is one screen of it; `longrun.py stop` ends the
driver, pgbench and the server. A stopped driver resumes where it was
with `longrun.py run`: the state file and the ledger journals carry
over, and the server is started if it is down.

## A trial

Ten minutes on a laptop, beside anything else on the box:

```
benchmarks/longrun/longrun.py begin --name trial --no-monitor \
  --set load.last=200000 --set load.streams=4 \
  --set server.sql_port=3321 --set server.admin_port=5321 \
  --owners 2 --keys 2000 --clients 4 \
  --active_min 120 --active_max 180 --idle_every 2 \
  --idle_min 30 --idle_max 60 --check_every 60 --status_every 30
benchmarks/longrun/longrun.py run --name trial --no-monitor \
  --set server.sql_port=3321 --set server.admin_port=5321
```

The `--set` values must be repeated on `run`, `check`, `status` and
`stop`: they are how the harness finds the server's port.
