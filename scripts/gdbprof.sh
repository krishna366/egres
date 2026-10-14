#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Sampling profiler for a running egres command, for boxes where perf is
# not available (perf_event_paranoid > 2 and no root). The server is
# started as gdb's child, so ptrace is allowed; a SIGPROF is sent to it
# every INTERVAL seconds and gdb prints every thread's top frames at each
# stop. A flat profile (self time by frame 0, inclusive over the first 5
# frames) is printed at the end. Overhead: each stop pauses the process
# for a few ms, so numbers under sampling run ~10-20% below unsampled.
#
# usage: scripts/gdbprof.sh [-n SAMPLES] [-i INTERVAL] [-o OUT]
#            -- <egres args...> :: <load command...>
# The load command runs once the server answers on the SQL port; its
# port is taken from `--listen=offset=N` in the egres args (3210 + N).
# Example:
#   scripts/gdbprof.sh -n 200 -- --raft=id=1 \
#     --store=dir_path=/data/egres/sst --wal=dir=/data/egres/wal \
#     --listen=offset=1 :: pgbench "host=127.0.0.1 port=3211 dbname=egres \
#     sslmode=disable" -n -S -c 64 -j 16 -T 40
set -euo pipefail
SAMPLES=150; INTERVAL=0.1; OUT=""
while [ $# -gt 0 ]; do
    case "$1" in
        -n) SAMPLES=$2; shift 2 ;;
        -i) INTERVAL=$2; shift 2 ;;
        -o) OUT=$2; shift 2 ;;
        --) shift; break ;;
        *) echo "unknown option $1" >&2; exit 2 ;;
    esac
done
SERVER_ARGS=()
while [ $# -gt 0 ] && [ "$1" != "::" ]; do SERVER_ARGS+=("$1"); shift; done
[ "${1:-}" = "::" ] || { echo "missing :: <load command>" >&2; exit 2; }
shift; LOAD=("$@")
[ ${#LOAD[@]} -gt 0 ] || { echo "empty load command" >&2; exit 2; }

BIN=${EGRES_BIN:-./zig-out/bin/egres}
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
[ -n "$OUT" ] || OUT=$TMP/gdbprof.out
offset=0
for a in "${SERVER_ARGS[@]}"; do
    case "$a" in *offset=*) offset=${a##*offset=}; offset=${offset%%;*} ;; esac
done
PORT=$((3210 + offset))

{
    echo "set pagination off"; echo "set confirm off"
    echo "set print thread-events off"
    echo "handle SIGPIPE nostop noprint pass"
    echo "handle SIGPROF stop print nopass"
    echo "run"
    for i in $(seq 1 "$SAMPLES"); do
        echo "echo === SAMPLE $i ===\\n"
        echo "thread apply all bt 6"
        echo "continue"
    done
    echo "kill"; echo "quit"
} > "$TMP/cmds"

setsid nohup gdb -q -batch -x "$TMP/cmds" --args "$BIN" "${SERVER_ARGS[@]}" \
    > "$OUT" 2>&1 < /dev/null &
for _ in $(seq 1 1200); do
    pid=$(pgrep -n -x "$(basename "$BIN")" || true)
    [ -n "$pid" ] && grep -q "become-leader" "$OUT" 2>/dev/null && break
    sleep 0.5
done
for _ in $(seq 1 120); do
    psql "host=127.0.0.1 port=$PORT dbname=egres sslmode=disable" -X -tAc \
        "select 1 from pgbench_branches limit 1" >/dev/null 2>&1 && break
    sleep 2
done
pid=$(pgrep -n -x "$(basename "$BIN")")
echo "server pid $pid on port $PORT; sampling $SAMPLES x ${INTERVAL}s"
"${LOAD[@]}" > "$TMP/load.txt" 2>&1 &
LOADPID=$!; sleep 5
for _ in $(seq 1 "$SAMPLES"); do
    kill -PROF "$pid" 2>/dev/null || break
    sleep "$INTERVAL"
done
wait $LOADPID || true
grep -E "^tps|latency average" "$TMP/load.txt" || tail -3 "$TMP/load.txt"
kill -TERM "$pid" 2>/dev/null || true; sleep 3

python3 - "$OUT" <<'PY'
import re, sys, collections
txt = open(sys.argv[1], errors="replace").read()
samples = txt.split("=== SAMPLE ")[1:]
idle_keys = ("io_uring", "IoUring", "errno", "futex", "epoll", "__GI___",
             "syscall", "Syscall.finish", "futexWait")
skip = ("Loop.run", "Loop.tick", "runMain", "callFn", "entryFn", "clone",
        "start_thread", "trampoline", "fiberEntry", "handleConn", "serve__",
        "handleEvent")
frame_re = r"^#(\d+)\s+(?:0x[0-9a-f]+ in )?([^\s(]+)"
top0 = collections.Counter(); incl = collections.Counter(); idle = total = 0
for smp in samples:
    for th in re.split(r"\nThread \d+ \(", smp)[1:]:
        frames = re.findall(frame_re, th, re.M)
        if not frames: continue
        total += 1
        if any(k in frames[0][1] for k in idle_keys): idle += 1; continue
        top0[frames[0][1]] += 1
        seen = set()
        for _, fn in frames[:5]:
            if fn in seen or any(k in fn for k in skip): continue
            seen.add(fn); incl[fn] += 1
busy = max(total - idle, 1)
pct = 100 * idle / max(total, 1)
print(f"samples: {len(samples)}  threads: {total}  idle: {idle} ({pct:.0f}%)")
print("--- self (frame 0), % of busy samples")
for fn, n in top0.most_common(30): print(f"  {100*n/busy:5.1f}%  {fn[:100]}")
print("--- inclusive (frames 0-4), % of busy samples")
for fn, n in incl.most_common(30): print(f"  {100*n/busy:5.1f}%  {fn[:100]}")
PY
echo "raw gdb output: $OUT"
