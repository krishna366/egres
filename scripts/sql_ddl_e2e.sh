#!/usr/bin/env bash
# End-to-end smoke test for SQL DDL (CREATE/DROP DATABASE + TABLE) over the
# pgwire endpoint. Builds, starts a fresh single-node egres, drives psql
# through the DDL matrix (including a restart-persistence leg), and greps
# the server log. Wipes /data/egres — that dir is throw-away by convention.
set -euo pipefail
cd "$(dirname "$0")/.."

DATA=/data/egres
LOG=/tmp/zdb/server.log
E="host=127.0.0.1 port=3210 dbname=egres sslmode=disable"
A="host=127.0.0.1 port=3210 dbname=app sslmode=disable"

pass=0
fail=0
ok() { echo "ok: $1"; pass=$((pass + 1)); }
bad() { echo "FAIL: $1"; fail=$((fail + 1)); }

# expect <desc> <needle> <cmd...>: run psql, assert output contains needle.
expect() {
    local desc=$1 needle=$2
    shift 2
    local out
    out=$("$@" 2>&1) || true
    if grep -q "$needle" <<<"$out"; then ok "$desc"; else bad "$desc — got: $out"; fi
}

# Ready = listener up AND election won — the listener opens a few hundred
# ms before become-leader, and proposals before that fail NotCommitted.
start_server() {
    ./zig-out/bin/egres --raft="id=1" --data="dir=$DATA" >"$LOG" 2>&1 &
    SERVER_PID=$!
    for _ in $(seq 1 50); do
        if grep -q "sql listener started" "$LOG" 2>/dev/null &&
            grep -q "become-leader" "$LOG" 2>/dev/null; then return; fi
        sleep 0.2
    done
    echo "server never became ready"; exit 1
}

stop_server() {
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
}

zig build
rm -rf "$DATA" && mkdir -p "$DATA" /tmp/zdb
start_server
trap stop_server EXIT

# --- create / duplicate / IF NOT EXISTS ------------------------------------
expect "CREATE TABLE" "CREATE TABLE" psql "$E" -X -c "CREATE TABLE t (id int PRIMARY KEY, name text);"
expect "duplicate CREATE errors" "already exists" psql "$E" -X -c "CREATE TABLE t (id int);"
expect "IF NOT EXISTS is quiet" "CREATE TABLE" psql "$E" -X -c "CREATE TABLE IF NOT EXISTS t (id int);"

# --- semantic rejections carry reasons + 0A000 ------------------------------
expect "NOT NULL rejected" "PRIMARY KEY column constraints" psql "$E" -X -c "CREATE TABLE u (a int NOT NULL);"
expect "serial rejected by name" "serial" psql "$E" -X -c "CREATE TABLE u (a serial);"
expect "composite pkey rejected" "table-level" psql "$E" -X -c "CREATE TABLE u (a int, b int, PRIMARY KEY (a, b));"

# --- drop / missing / IF EXISTS / id never reused ---------------------------
expect "DROP TABLE" "DROP TABLE" psql "$E" -X -c "DROP TABLE t;"
expect "second DROP errors" "does not exist" psql "$E" -X -c "DROP TABLE t;"
expect "IF EXISTS is quiet" "DROP TABLE" psql "$E" -X -c "DROP TABLE IF EXISTS t;"
expect "re-CREATE after drop" "CREATE TABLE" psql "$E" -X -c "CREATE TABLE t (id int8 PRIMARY KEY);"
grep -q "created table 't' -> ns=16 table_id=17" "$LOG" && ok "dropped id 16 not reused (got 17)" || bad "table_id reuse"

# --- unknown database fails per query ---------------------------------------
expect "unknown dbname errors" "database does not exist" \
    psql "host=127.0.0.1 port=3210 dbname=nope sslmode=disable" -X -c "CREATE TABLE x (a int);"

# --- restart: names + id counter survive WAL replay -------------------------
stop_server
start_server
expect "'t' survives restart" "already exists" psql "$E" -X -c "CREATE TABLE t (id int);"
expect "post-restart CREATE" "CREATE TABLE" psql "$E" -X -c "CREATE TABLE post_restart (a int4 PRIMARY KEY);"
grep -q "created table 'post_restart' -> ns=16 table_id=18" "$LOG" && ok "id counter survived restart (18)" || bad "id counter reset"

# --- per-database name scoping + drop cascades ------------------------------
expect "CREATE DATABASE app" "CREATE DATABASE" psql "$E" -X -c "CREATE DATABASE app;"
expect "same table name in app" "CREATE TABLE" psql "$A" -X -c "CREATE TABLE t (x uint8);"
expect "DROP DATABASE with tables" "DROP DATABASE" psql "$E" -X -c "DROP DATABASE app;"
expect "dropped-db session fails" "database does not exist" psql "$A" -X -c "CREATE TABLE y (a int);"

# --- no unexpected server-side errors ---------------------------------------
if grep -q "^error" "$LOG"; then bad "server log has errors"; else ok "server log clean"; fi

echo
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
