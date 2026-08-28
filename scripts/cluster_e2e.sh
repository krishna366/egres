#!/usr/bin/env bash
# End-to-end multi-process cluster test: 3 egres nodes distributing SST
# files through the shared object store (the `dir` depot backend), plus an
# optional SeaweedFS leg (SEAWEED=1) that runs the same matrix against a
# real S3 endpoint with SigV4 validated server-side.
#
# Covers: store enforcement (seeded start + join rejection), join +
# replication, flush → store upload (event=store_put) → follower prefetch/
# apply download (event=store_get), follower reads, and a wipe-and-rejoin
# leg where a follower rebuilds purely from raft replay + store downloads.
# Wipes /data/egres-e2e — that dir is throw-away by convention.
set -euo pipefail
cd "$(dirname "$0")/.."

# Override with e.g. ROOT=$HOME/.cache/egres-e2e where /data is absent.
# Must be a real filesystem (SST writes use O_DIRECT — tmpfs won't do).
ROOT=${ROOT:-/data/egres-e2e}
LOGS=${LOGS:-/tmp/zdb/cluster}
E1="host=127.0.0.1 port=3210 dbname=egres sslmode=disable"
E3="host=127.0.0.1 port=3212 dbname=egres sslmode=disable"

pass=0
fail=0
ok() { echo "ok: $1"; pass=$((pass + 1)); }
bad() { echo "FAIL: $1"; fail=$((fail + 1)); }

# expect <desc> <needle> <cmd...>: run the command, assert output contains needle.
expect() {
    local desc=$1 needle=$2
    shift 2
    local out
    out=$("$@" 2>&1) || true
    if grep -q "$needle" <<<"$out"; then ok "$desc"; else bad "$desc — got: $out"; fi
}

# wait_log <file> <needle> [tries]: poll a log for a line, 0.2s apart.
wait_log() {
    local file=$1 needle=$2 tries=${3:-75}
    for _ in $(seq 1 "$tries"); do
        if grep -q "$needle" "$file" 2>/dev/null; then return 0; fi
        sleep 0.2
    done
    return 1
}

declare -a PIDS=()
# start_node <id> <offset> <extra flags...>: background one egres process.
start_node() {
    local id=$1 offset=$2
    shift 2
    ./zig-out/bin/egres \
        --raft="id=$id$([ "$id" != 1 ] && echo ";seeds=127.0.0.1:4210")" \
        --listen="offset=$offset" \
        --data="dir=$ROOT/n$id;memtable_size=262144" \
        "$@" >"$LOGS/n$id.log" 2>&1 &
    PIDS+=($!)
}

stop_all() {
    for pid in "${PIDS[@]:-}"; do kill -TERM "$pid" 2>/dev/null || true; done
    for pid in "${PIDS[@]:-}"; do wait "$pid" 2>/dev/null || true; done
    PIDS=()
}
trap stop_all EXIT

zig build

# run_cluster <store flags for all nodes...>: the shared test matrix.
run_cluster() {
    rm -rf "$ROOT" && mkdir -p "$ROOT" "$LOGS"
    rm -f "$LOGS"/n*.log

    # --- store enforcement ---------------------------------------------------
    # A seeded node refuses to start without a store.
    expect "seeded node without --store fails fast" "store is required" \
        ./zig-out/bin/egres --raft="id=9;seeds=127.0.0.1:4210" --data="dir=$ROOT/n9"
    # A storeless leader rejects joins.
    start_node 1 0
    wait_log "$LOGS/n1.log" "become-leader" || { bad "storeless node1 never led"; return; }
    start_node 2 1 "$@"
    wait_log "$LOGS/n2.log" "cannot join" &&
        ok "join rejected by storeless leader" || bad "join not rejected"
    stop_all
    rm -rf "$ROOT" && mkdir -p "$ROOT"
    rm -f "$LOGS"/n*.log

    # --- bring up the real cluster -------------------------------------------
    start_node 1 0 "$@"
    wait_log "$LOGS/n1.log" "become-leader" || { bad "node1 never became leader"; return; }
    wait_log "$LOGS/n1.log" "sql listener started" || { bad "node1 sql listener"; return; }
    start_node 2 1 "$@"
    start_node 3 2 "$@"
    wait_log "$LOGS/n2.log" "join accepted by" && ok "node2 joined" || bad "node2 join"
    wait_log "$LOGS/n3.log" "join accepted by" && ok "node3 joined" || bad "node3 join"

    # --- writes → flush → store upload → follower download --------------------
    expect "CREATE TABLE" "CREATE TABLE" psql "$E1" -X -c \
        "CREATE TABLE items (id int PRIMARY KEY, body text);"
    # ~600 KiB across 6 statements overflows the 256 KiB memtable at least
    # twice, forcing flushes whose SSTs must travel through the store.
    local pad
    pad=$(printf 'x%.0s' $(seq 1 1000))
    for batch in 0 1 2 3 4 5; do
        vals=""
        for i in $(seq 1 100); do
            id=$((batch * 100 + i))
            vals+="($id,'$pad'),"
        done
        psql "$E1" -X -q -c "INSERT INTO items VALUES ${vals%,};" >/dev/null 2>&1 ||
            bad "insert batch $batch"
    done
    ok "600 rows inserted"

    wait_log "$LOGS/n1.log" "event=store_put" 150 &&
        ok "leader uploaded SSTs to the store" || bad "no store_put on leader"
    wait_log "$LOGS/n2.log" "event=store_get" 150 &&
        ok "node2 downloaded SSTs from the store" || bad "no store_get on node2"
    wait_log "$LOGS/n3.log" "event=store_get" 150 &&
        ok "node3 downloaded SSTs from the store" || bad "no store_get on node3"

    # Followers serve local applied state. No aggregates in egres SQL yet —
    # assert on psql's row-count footer.
    expect "follower sees all rows" "(600 rows)" psql "$E3" -X -c \
        "SELECT id FROM items;"

    # --- wipe a follower; it rebuilds from raft replay + store downloads ------
    local pid3=${PIDS[-1]}
    kill -TERM "$pid3" 2>/dev/null || true
    wait "$pid3" 2>/dev/null || true
    unset 'PIDS[-1]'
    rm -rf "$ROOT/n3"
    rm -f "$LOGS/n3.log"
    start_node 3 2 "$@"
    wait_log "$LOGS/n3.log" "join accepted by" || bad "node3 rejoin"
    wait_log "$LOGS/n3.log" "event=store_get" 300 &&
        ok "wiped node3 re-downloaded SSTs" || bad "no store_get after wipe"
    sleep 1
    expect "rejoined node3 serves reads" "(600 rows)" psql "$E3" -X -c \
        "SELECT id FROM items;"

    stop_all
}

echo "=== dir-store cluster ==="
run_cluster --store="kind=dir;path=$ROOT/depot"
ls "$ROOT/depot"/*.sst >/dev/null 2>&1 && ok "depot holds SST objects" || bad "depot empty"

if [ "${SEAWEED:-0}" = "1" ]; then
    echo "=== seaweedfs (s3) cluster ==="
    command -v weed >/dev/null || { echo "weed not installed"; exit 1; }
    SEAWEED_DIR=$(mktemp -d)
    cat >"$SEAWEED_DIR/s3.json" <<'JSON'
{"identities":[{"name":"egres","credentials":[{"accessKey":"egreskey","secretKey":"egressecret"}],"actions":["Admin","Read","Write","List","Tagging"]}]}
JSON
    weed server -dir="$SEAWEED_DIR" -s3 -s3.port=8333 -s3.config="$SEAWEED_DIR/s3.json" \
        >"$LOGS/weed.log" 2>&1 &
    WEED_PID=$!
    sleep 3
    echo "s3.bucket.create -name egres" | weed shell >/dev/null 2>&1 || true
    export AWS_ACCESS_KEY_ID=egreskey AWS_SECRET_ACCESS_KEY=egressecret
    run_cluster --store="kind=s3;s3_endpoint=http://127.0.0.1:8333;s3_bucket=egres"
    kill -TERM "$WEED_PID" 2>/dev/null || true
    wait "$WEED_PID" 2>/dev/null || true
    rm -rf "$SEAWEED_DIR"
fi

echo
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
