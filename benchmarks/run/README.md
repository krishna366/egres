# Benchmark runs

One harness runs one server (Egres or PostgreSQL) through one workload:
a load through the SQL port with readers alongside, then pgbench runs
back to back, with Prometheus and Grafana drawing the whole run. The
same commands rerun the same benchmark on any machine.

## Pieces

- `bench.py`: the command, and the tables that define the benchmarks:
  `SERVERS` (ports, database, config) and `ENVS` (a workload, a
  server, a memory cap). Two envs exist: `egres-orders` and
  `postgres-orders`, neither capped.
- `workloads.py`: each workload's schema, row generator, load size,
  and pgbench scripts, with the read-only subset the readers run and
  the full mix. `workloads.py <name> --ddl` prints a schema,
  `workloads.py <name> <first> <last>` the rows.
- `loader.py`: the load driver and the reader groups beside it; times
  every INSERT statement; writes `load-summary.md` when the load ends.
- `pgbench.py`: runs pgbench, publishes its progress lines and
  per-script latency percentiles as metrics, parses its final report.
- `iostat.py`: bytes read and written per disk, and by the server
  process, over a span of a run.
- `watchdog.py`: the night operator. Restarts a dead server once,
  dumps on a stall, starts the workload runs when the load ends,
  records every run and incident, writes the summary.
- `local.toml`: this machine (copy `local.example.toml`; ignored by
  git). Roots for run output, data dirs and WAL dirs, the Postgres
  binaries, the Egres binary.
- `../monitor/`: the docker compose stack. Prometheus (:9090), Grafana
  (:3000, dashboard "Egres run", anonymous viewer), node exporter
  (host CPU, memory, disks, and a textfile collector), process
  exporter, postgres exporter, and `bridge/exporter.py`, which serves
  the Egres admin `zdb metrics` counters, `zdb` gauges, the data dir
  size and the capped server's cgroup memory at :9877.

## A soak

`scripts/soak.sh` runs this harness cut to a CI runner's size, in the
foreground with pass/fail checks: a load with readers for 15 minutes,
two minutes of settle, a row count, a two-minute pgbench workload, and
a clean stop; the `.github/workflows/soak.yml` job runs it twice a
day on Linux and macOS and uploads the run dir. `SOAK_*` variables
set the durations, streams, clients and cache; it needs `local.toml`
and an empty `<data_root>/egres-orders-soak`. Off Linux the summaries
carry no disk or host CPU figures (there is no `/proc`).

## A run

```
cp benchmarks/run/local.example.toml benchmarks/run/local.toml   # once
zig build -Doptimize=ReleaseSafe                                  # Egres
benchmarks/run/bench.py postgres-orders all
benchmarks/run/bench.py postgres-orders status
```

`all` is `begin` (a new output dir, an empty data dir, monitoring up),
`start` (the server; for Postgres, initdb with checksums and pg_wal on
the WAL root, the env's config appended, the database created), `load`
(the load driver, detached, with the readers) and `watch` (the
watchdog, detached). The steps can be run one at a time; `stop` ends
everything by pid.

A run's output dir is `<out_root>/<env>-<start time>`;
`<out_root>/<env>.latest` points at it, so no date is ever typed. In
it: `env.json` (the resolved env the run used), `events.log` (what
happened when), `load.log`, `watch.log`, `progress.md`,
`load-summary.md` and `.json` (the load's rate, insert and reader
latency percentiles, bytes read and written per disk),
`results/pgbench-*.log` and `.summary`, `results/readers-<group>.log`,
`results/latency/` (pgbench's per-transaction logs),
`results/load-<n>.log` (each stream's statement times), `scripts/`
(the pgbench scripts as run), `textfile/*.prom`, `dumps/`,
`INCIDENTS.md`, the server log, and `SUMMARY-auto.md` when the
watchdog is done.

A trial at small scale, beside a real run and without touching its
monitoring:

```
benchmarks/run/bench.py egres-orders all --name trial --no-monitor \
  --set load.last=120000 --set load.streams=4 --set readers.reads=2 \
  --set secs=15 --set clients=4 --set memory_max=none \
  --set server.sql_port=3321 --set server.admin_port=5321
```

`--set runs=0` ends a run with its load: the watchdog writes its
summary and stops, the server stays up. `--set load.stop_secs=600`
ends the load after ten minutes, wherever it has got to.

## What is measured, and why

Three things are measured over the load, and reported in
`load-summary.md`: how long an INSERT statement takes (every one, as
psql times it), how long the readers' queries take (percentiles per
script from pgbench's per-transaction log), and how many bytes each
disk read and wrote, with the server process's own share.

The load is measured with readers on it. A server that defers work to
a later pass (Postgres: hint bits, the visibility map, freezing, set by
vacuum) or does it as it goes (Egres: compaction) pays for it in the
same numbers either way. Postgres runs with autovacuum tuned to keep up
with the load, the setting a DBA would pick for a bulk load; no VACUUM
is run by hand between the load and the workload.

The workload starts the moment the load ends, and runs twice back to
back: the second run shows the settled state each server reaches on
its own. Inserts take ids past the loaded range, in a block per client
and per run, so the runs never collide.

Under a memory cap, the server runs in a systemd user scope with swap
off, page cache included, so a 500 GiB table under 64 GB is mostly
cache misses for both servers.

## Restarts

A planned restart mid-load: `stop`, then `start`, then
`load --resume`, then `watch`. Between workload runs: `stop`, `start`,
then `watch --runs-done N`.

Grafana: http://127.0.0.1:3000/d/egres-run (admin/admin to edit).
