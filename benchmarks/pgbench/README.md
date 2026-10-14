# pgbench harness for Egres and PostgreSQL

One runner, `run.sh`, produces the launch numbers for both systems.
It runs the same pgbench workloads against Egres and against a stock
PostgreSQL instance and writes one TSV row per measured run, so the
two sides can be compared row by row.

## What it measures

For each combination of workload, query mode and client count the
harness does two pgbench runs: a warmup of `--warmup` seconds that is
thrown away, then a measured run of `--duration` seconds. It records
throughput and latency from pgbench's own summary, and computes
p50/p99/p99.9 latency from the per-transaction log that pgbench
writes with `-l --sampling-rate=0.1`.

Every run passes `-n`, so pgbench never tries to VACUUM. Threads are
`min(clients, 16)`, so a single pgbench thread is never the limit at
high client counts and the harness never uses more than half the 32
cores on the box for the client side.

Workloads:

- `select-only`  pgbench built-in `-b select-only`
- `tpcb-like`    pgbench built-in `-b tpcb-like`
- `hot-update`   `hot-update.sql`, a single UPDATE against a zipfian
                 hot set, which is the contention case

## Binaries

pgbench is not on PATH. It lives in `/home/mrjn/.local/opt/pg18/bin`,
which is the default for `--pgbin` and can be overridden with the
`PGBIN` environment variable. That directory has no `psql`, so the
harness falls back to `psql` on PATH (`/usr/bin/psql`, also v18).

## Running Postgres

Init and full matrix:

    ./run.sh --label postgres \
      --conn "host=127.0.0.1 port=5433 user=mrjn dbname=pgbench" \
      --init postgres --scale 100

`--init postgres` runs stock `pgbench -i -s SCALE`.

Before a real run, append `postgresql.conf.tuned` to the data
directory's `postgresql.conf` and restart:

    cat postgresql.conf.tuned >> /data/pg18/data/postgresql.conf
    /home/mrjn/.local/opt/pg18/bin/pg_ctl -D /data/pg18/data restart

## Running Egres

Build the server in `ReleaseFast` for benchmark numbers (`ReleaseSafe`
for correctness runs, or when a `ReleaseFast` run crashes; see the
top-level README) and start it on a fresh data directory:

    zig build -Doptimize=ReleaseFast egres
    ./zig-out/bin/egres --raft="id=1" \
      --store="dir_path=/data/egres/sst" --wal="dir=/data/egres/wal"

Egres listens on port 3210, the database is `egres`, and TLS is not
supported, so `sslmode=disable` is required. All three query modes
run against it: `simple`, `extended` (Parse/Bind/Execute per command)
and `prepared` (one named Parse per script command, then Bind/Execute).

    ./run.sh --label egres \
      --conn "host=127.0.0.1 port=3210 dbname=egres sslmode=disable" \
      --init egres --scale 100 --modes "simple extended prepared"

`pgbench -i` does not work against Egres: it needs COPY, NOT NULL,
multi-table DROP and TRUNCATE, VACUUM and ALTER TABLE. `--init egres`
instead pipes generated SQL through psql:

    python3 init_egres.py 100 | psql "$CONN" -X -q -v ON_ERROR_STOP=1

The generator emits pgbench's own four CREATE TABLE statements (minus
NOT NULL, so `filler` is `char(84)` on accounts and tellers, `char(88)`
on branches, `char(22)` on history) and multi-row INSERTs of 500 rows
each, matching what `pgbench -i` produces: `branches = scale`,
`tellers = 10 * scale`, `accounts = 100000 * scale`, all balances
zero, `accounts.filler` the empty string. Egres stores `char(n)`
stripped of trailing spaces and pads on output, so the accounts rows
are smaller on disk than in Postgres, where `char(84)` stores 84
spaces. Load time and rows/s go to `<out>/<label>-init.txt`.

`init_egres.py` accepts `--parallel N`, which is currently ignored;
the single-stream generator is fast enough at the scales run today.

## Options

    --label NAME       name for the system under test (required)
    --conn CONNINFO    libpq conninfo string (required)
    --init MODE        egres | postgres | none        (default none)
    --scale N          pgbench scale factor           (default 100)
    --duration S       seconds per measured run       (default 60)
    --warmup S         warmup seconds, 0 skips it     (default 10)
    --clients "LIST"   client counts          (default "1 8 32 64")
    --modes "LIST"     simple | extended | prepared (default simple)
    --workloads "LIST" default "select-only tpcb-like hot-update"
    --out DIR          default benchmarks/pgbench/results
    --pgbin DIR        default $PGBIN or the pg18 bin above

## Output

Rows are appended to `<out>/results.tsv`, which gets a header line
when it is first created. Full pgbench stdout and stderr for each run
goes to `<out>/<label>/<workload>-<mode>-c<C>.log`, and the warmup
output to the matching `.warmup.log`. The aligned table printed at
the end covers only the current invocation.

TSV columns:

    date_utc        UTC timestamp when the run finished
    label           value of --label
    workload        workload name
    mode            pgbench -M value
    clients         pgbench -c value
    jobs            pgbench -j value, min(clients, 16)
    scale           pgbench scale factor
    duration_s      measured run length in seconds
    tps             tps without initial connection time
    lat_avg_ms      pgbench "latency average"
    lat_stddev_ms   pgbench "latency stddev"
    p50_ms          median latency from the -l log
    p99_ms          99th percentile latency from the -l log
    p999_ms         99.9th percentile latency from the -l log
    txns            transactions actually processed
    failed          number of failed transactions
    notes           empty on success, otherwise the failure reason

A run that exits non-zero is recorded with an empty `tps` and the
first error line in `notes`, and the matrix continues.

## Running the built-ins against Egres

pgbench's built-in scripts need two things beyond the workload SQL:
they read the scale with `SELECT count(*) FROM pgbench_branches` at
startup, and `tpcb-like` inserts `CURRENT_TIMESTAMP` into
`pgbench_history.mtime`. Egres supports both, so `select-only` and
`tpcb-like` run unmodified against it.

Without `-n`, pgbench also runs `VACUUM` on the pgbench tables before
the test; Egres rejects VACUUM and pgbench ignores that error, but the
harness always passes `-n` so both sides skip it.
