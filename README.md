# egres

## Regenerating Buffers Code

Buffer schemas live in `<module>/buffers.zig` files (e.g. `zbuf/buffers.zig`,
`raft/buffers.zig`, `zdb/buffers.zig`). Each generates its `<module>/buffers.gen.zig`
sibling.

Regenerate every discovered `*/buffers.gen.zig` at once:

```bash
zig build buffers
```

To generate for a specific schema file (writes `<input>.gen.zig` next to it):

```bash
zig build zbuf -- path/to/buffers.zig
```

## Load Testing

A load test spins up one or more `egres` nodes, creates a namespace, and
drives writes through the `zdb-bench` client. It exists to validate correctness
and surface deadlocks / runaway loops under sustained write load.

### 1. Build the binaries

Always build in `ReleaseFast` for load tests. The default `zig build` is
`Debug`, whose safety checks dominate the runtime and make throughput numbers
meaningless.

```bash
zig build --release=fast egres zdb-bench
```

Binaries land in `zig-out/bin/`.

### 2. Run a single-node load test (recommended)

`scripts/loadtest.py` orchestrates the whole flow: wipe the workdir, start the
server, create the namespace, run the load generator, sample `/stats` into a
TSV, then tear everything down.

```bash
python3 scripts/loadtest.py run \
    --nodes 1 \
    --workdir /data/egres \
    --records 10500000 \
    --value-size 1024 \
    --clients 16 \
    --batch-size 256 \
    --duration 30m
```

This writes ~10 GiB of values (≈ `records × value-size`). Size `--records` for
your target: `records = target_bytes / value_size` (10 GiB at 1 KiB values ≈
10,485,760 records). The run ends as soon as the load completes or `--duration`
elapses, whichever comes first.

Output lives under `--workdir` (default: repo root — use a roomy throwaway
filesystem like `/data/egres`, since a 10 GiB load expands further on disk via
the WAL + write amplification before compaction and snapshots catch up):

- `data/node1/` — the LSM forest + raft WAL
- `logs/node1.log` — the server log (grep here first on any problem)
- `logs/stats-node1.txt` — final `/stats` dump, including latency histograms
- `metrics.tsv` — periodic samples (flushes, compactions, bytes, disk usage)

At the end of a run the driver also prints each node's **worker-CPU histogram**
(the server's `workers` admin command): per-worker accumulated CPU plus a
`max/avg` imbalance ratio (1.0 = perfectly balanced, higher = work piled onto a
few workers), so you can see how evenly the fiber runtime spread the load.

Inspect a running cluster or clean up afterwards:

```bash
python3 scripts/loadtest.py stats --node 1   # one-shot Prometheus stats dump
python3 scripts/loadtest.py clean            # wipe data/ logs/ metrics.tsv
```

> The driver applies `offset=i` per node, so node 1 binds admin port `5211`
> and raft port `4211`. `--nodes N` runs a real N-voter cluster (it promotes
> the joiners from learner to voter before loading).

### 3. Manual single-server run (for debugging)

When you want to poke at a running server yourself, start one directly. With
`offset=0` it binds the admin port at `5210` and raft at `4210`:

```bash
rm -rf /data/egres/node1 && mkdir -p /data/egres/node1 /tmp/zdb
./zig-out/bin/egres \
    --raft="id=1; cluster-id=1" \
    --data="dir=/data/egres/node1" \
    --listen="client-port=5210; raft-port=4210" \
    > /tmp/zdb/server.log 2>&1 &
```

It self-bootstraps as a single-node cluster and elects itself leader (look for
`become-leader` in the log). The bench rejects writes to an unknown namespace,
so create one first — the protocol is line-based text, so a bash `/dev/tcp`
one-liner is enough:

```bash
exec 3<>/dev/tcp/127.0.0.1/5210
printf 'ns create 1\n' >&3
head -1 <&3            # expect: OK
exec 3>&-
```

Then drive the load, and stop the server with `SIGTERM` when done:

```bash
./zig-out/bin/zdb-bench --bench="addr=127.0.0.1:5210; clients=16; \
    records=10500000; value-size=1024; batch-size=256; namespaces=1; phase=load"

pkill -TERM -x egres      # -KILL if it won't stop
```

### 4. Health checks

The whole point is to catch deadlocks and runaway loops, so watch the server
log:

```bash
# first thing on any failure — silent error spam is common
grep -iE "error|panic|deadlock|wedge|KeyOrder" /tmp/zdb/server.log

# a healthy idle server ticks only ~1 commit/sec (a handful of debug lines).
# Tens of thousands of lines/sec — typically repeated applyManifestChange or
# split-trigger entries — means a background loop is spinning. Measure it:
a=$(wc -l < /tmp/zdb/server.log); sleep 2; b=$(wc -l < /tmp/zdb/server.log)
echo "log grew $((b-a)) lines in 2s"
```

The text protocol also exposes diagnostic verbs you can query live (each
returns a blank-line-terminated dump): `workers` (per-worker CPU histogram),
`locks` (held tracked mutexes), `vfs` (bandwidth limiter), and `zdb` (stats):

```bash
# worker CPU distribution from a running server on admin port 5210
exec 3<>/dev/tcp/127.0.0.1/5210
printf 'workers\n' >&3
while IFS= read -r line <&3; do [ -z "$line" ] && break; echo "$line"; done
exec 3>&-
```

A successful load reports `0 err` per worker plus a Load Phase summary
(`records`, `throughput`, `errors: 0`). On disk you should see SST files
appear and the WAL stay bounded as snapshots truncate it.
