#!/usr/bin/env python3
"""Emit the pgbench schema and data as SQL that Egres can execute.

`pgbench -i` cannot be used against Egres: it needs COPY, NOT NULL,
multi-table DROP/TRUNCATE, VACUUM and ALTER TABLE.  This writes pgbench's
own CREATE TABLE statements (minus NOT NULL) + multi-row INSERT to
stdout instead, to be piped through psql.

Mirrors `pgbench -i -s S`: branches=S, tellers=10*S, accounts=100000*S,
all balances 0, and accounts.filler the empty string, which pgbench's
char(84) pads to 84 spaces on the way out.  Egres stores a char(n)
value stripped, so the accounts rows are smaller on disk than in
Postgres.

Usage: init_egres.py SCALE [ROWS_PER_STMT] [--parallel N]
"""
import sys

# TODO(parallel): --parallel N is accepted but ignored.  When the load
# becomes the bottleneck, shard the accounts range across N streams and
# emit one file per shard so several psql sessions can load at once.
# The single-stream generator is fast enough at the scales we run now.


def parse_args(argv):
    scale = None
    rows_per_stmt = 500
    parallel = 1
    positional = []
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--parallel":
            i += 1
            if i >= len(argv):
                raise SystemExit("--parallel needs a value")
            parallel = int(argv[i])
        elif arg.startswith("--parallel="):
            parallel = int(arg.split("=", 1)[1])
        else:
            positional.append(arg)
        i += 1
    if not positional:
        raise SystemExit(__doc__.strip())
    scale = int(positional[0])
    if len(positional) > 1:
        rows_per_stmt = int(positional[1])
    if scale < 1 or rows_per_stmt < 1 or parallel < 1:
        raise SystemExit("scale, rows-per-stmt and --parallel must be >= 1")
    return scale, rows_per_stmt, parallel


def main():
    scale, rows_per_stmt, _parallel = parse_args(sys.argv[1:])
    w = sys.stdout.write

    tables = (
        "pgbench_accounts",
        "pgbench_branches",
        "pgbench_history",
        "pgbench_tellers",
    )
    # Dropped one at a time: Egres has no multi-table DROP.
    for t in tables:
        w("drop table if exists %s;\n" % t)
    # pgbench's DDL (initCreateTables in pgbench.c), NOT NULL dropped.
    w("create table pgbench_history(tid int, bid int, aid int,"
      " delta int, mtime timestamp, filler char(22));\n")
    w("create table pgbench_tellers(tid int primary key, bid int,"
      " tbalance int, filler char(84));\n")
    w("create table pgbench_accounts(aid int primary key, bid int,"
      " abalance int, filler char(84));\n")
    w("create table pgbench_branches(bid int primary key,"
      " bbalance int, filler char(88));\n")

    def emit(table, cols, n, rowfn):
        for start in range(1, n + 1, rows_per_stmt):
            end = min(start + rows_per_stmt, n + 1)
            rows = ",".join(rowfn(i) for i in range(start, end))
            w("insert into %s(%s) values %s;\n" % (table, cols, rows))

    emit("pgbench_branches", "bid,bbalance", scale,
         lambda i: "(%d,0)" % i)
    emit("pgbench_tellers", "tid,bid,tbalance", 10 * scale,
         lambda i: "(%d,%d,0)" % (i, (i - 1) // 10 + 1))
    emit("pgbench_accounts", "aid,bid,abalance,filler", 100000 * scale,
         lambda i: "(%d,%d,0,'')" % (i, (i - 1) // 100000 + 1))


if __name__ == "__main__":
    try:
        main()
        sys.stdout.flush()
    except BrokenPipeError:
        # The reader stopped early (`| head`).  Point stdout at
        # /dev/null so the interpreter's own flush cannot fail too.
        import os
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        sys.exit(0)
