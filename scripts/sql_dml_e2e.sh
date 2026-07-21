#!/usr/bin/env bash
# End-to-end smoke test for SQL INSERT/SELECT/DELETE over the pgwire endpoint.
# Builds, starts a fresh single-node egres, drives psql through
# insert/select/filter, egid visibility, delete (point + scan + full-table), a
# restart-persistence + monotonic-egid leg (incl. no-reuse of a deleted max),
# and greps the server log. Wipes /data/egres — that dir is throw-away.
set -euo pipefail
cd "$(dirname "$0")/.."

DATA=/data/egres
LOG=/tmp/zdb/server.log
E="host=127.0.0.1 port=3210 dbname=egres sslmode=disable"

pass=0
fail=0
ok() { echo "ok: $1"; pass=$((pass + 1)); }
bad() { echo "FAIL: $1"; fail=$((fail + 1)); }

# expect <desc> <needle> <cmd...>: run, assert output contains needle.
expect() {
    local desc=$1 needle=$2
    shift 2
    local out
    out=$("$@" 2>&1) || true
    if grep -q "$needle" <<<"$out"; then ok "$desc"; else bad "$desc — got: $out"; fi
}

q() { psql "$E" -X -c "$1"; }     # full output (row counts, headers)
v() { psql "$E" -X -tA -c "$1"; } # tuples-only, unaligned (bare value)

# Ready = listener up AND election won (see sql_ddl_e2e.sh).
start_server() {
    ./zig-out/bin/egres --raft="id=1" --data="dir=$DATA" >"$LOG" 2>&1 &
    SERVER_PID=$!
    for _ in $(seq 1 50); do
        if grep -q "sql listener started" "$LOG" 2>/dev/null &&
            grep -q "become-leader" "$LOG" 2>/dev/null; then return; fi
        sleep 0.2
    done
    echo "server never became ready"
    exit 1
}

stop_server() {
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
}

zig build
rm -rf "$DATA" && mkdir -p "$DATA" /tmp/zdb
start_server
trap stop_server EXIT

# --- create + insert -------------------------------------------------------
expect "CREATE TABLE" "CREATE TABLE" q "CREATE TABLE t (id int4 PRIMARY KEY, name text);"
expect "INSERT two rows" "INSERT 0 2" q "INSERT INTO t VALUES (1, 'a'), (2, 'b');"

# --- select back -----------------------------------------------------------
expect "SELECT * returns both" "(2 rows)" q "SELECT * FROM t;"
expect "egid 1 = first row" "a" v "SELECT name FROM t WHERE egid = 1;"
expect "egid 2 = second row" "b" v "SELECT name FROM t WHERE egid = 2;"
expect "WHERE on payload column" "b" v "SELECT name FROM t WHERE id = 2;"
expect "SELECT egid, id, name" "(2 rows)" q "SELECT egid, id, name FROM t;"

# egid is a hidden system column: not in SELECT *.
star_out=$(q "SELECT * FROM t;" 2>&1)
if grep -q "egid" <<<"$star_out"; then bad "egid leaked into SELECT *"; else ok "egid excluded from SELECT *"; fi

# --- omitted column defaults NULL; egid auto-allocates ---------------------
expect "INSERT partial cols" "INSERT 0 1" q "INSERT INTO t (name) VALUES ('c');"
expect "auto egid 3 = c" "c" v "SELECT name FROM t WHERE egid = 3;"

# --- egid is read-only at insert time --------------------------------------
expect "cannot insert egid" "not supported" q "INSERT INTO t (egid, name) VALUES (100, 'x');"

# --- error paths -----------------------------------------------------------
expect "unknown table" "does not exist" q "SELECT * FROM nope;"
expect "unknown column" "does not exist" q "SELECT bogus FROM t;"

# --- restart: rows persist, egid stays monotonic (no reuse) ----------------
stop_server
start_server
expect "rows survive restart" "(3 rows)" q "SELECT * FROM t;"
expect "INSERT post-restart" "INSERT 0 1" q "INSERT INTO t (name) VALUES ('d');"
expect "egid 4 post-restart" "d" v "SELECT name FROM t WHERE egid = 4;"
# State here: egids 1=a, 2=b, 3=c, 4=d, all live.

# --- delete by egid (point path) -------------------------------------------
expect "DELETE by egid" "DELETE 1" q "DELETE FROM t WHERE egid = 2;"
expect "deleted row invisible" "(0 rows)" q "SELECT * FROM t WHERE egid = 2;"
expect "DELETE same egid again is 0" "DELETE 0" q "DELETE FROM t WHERE egid = 2;"

# --- delete by payload column (scan path) ----------------------------------
expect "DELETE by column" "DELETE 1" q "DELETE FROM t WHERE name = 'c';"
expect "DELETE no match is 0" "DELETE 0" q "DELETE FROM t WHERE name = 'zzz';"
expect "survivors intact" "(2 rows)" q "SELECT * FROM t;" # a (egid 1), d (egid 4)

# --- delete rejections / errors --------------------------------------------
expect "DELETE USING rejected" "not supported" q "DELETE FROM t USING u WHERE t.id = u.id;"
expect "DELETE unknown table" "does not exist" q "DELETE FROM nope;"

# --- delete the current max, then restart: egid must NOT be reused ----------
# Deletes egid 4 (the highest stored key). maxKey must count the tombstone and
# seed next_egid past it, else the next INSERT resurrects egid 4.
expect "DELETE current max" "DELETE 1" q "DELETE FROM t WHERE egid = 4;"
stop_server
start_server
expect "deletes survive restart" "(1 row)" q "SELECT * FROM t;" # just a (egid 1)
expect "INSERT after delete+restart" "INSERT 0 1" q "INSERT INTO t (name) VALUES ('e');"
expect "egid 4 not reused (lands on 5)" "e" v "SELECT name FROM t WHERE egid = 5;"

# --- delete every row (no WHERE) -------------------------------------------
expect "DELETE all" "DELETE 2" q "DELETE FROM t;" # a (egid 1), e (egid 5)
expect "table empty" "(0 rows)" q "SELECT * FROM t;"
expect "DELETE on empty is 0" "DELETE 0" q "DELETE FROM t;"

# --- no unexpected server-side errors --------------------------------------
if grep -q "^error" "$LOG"; then bad "server log has errors"; else ok "server log clean"; fi

echo
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
