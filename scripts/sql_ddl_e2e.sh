#!/usr/bin/env bash
# End-to-end smoke test for SQL DDL (CREATE/DROP DATABASE + TABLE + INDEX)
# pgwire endpoint. Builds, starts a fresh single-node egres, drives psql
# through the DDL matrix (including a restart-persistence leg), and greps
# the server log. Wipes $DATA (default /data/egres) — throw-away by
# convention; override like cluster_e2e.sh's ROOT (must be a real fs —
# SSTs are O_DIRECT).
set -euo pipefail
cd "$(dirname "$0")/.."

DATA="${DATA:-/data/egres}"
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
# First 't' consumed 16 (table) + 17 (its _pkey index); re-CREATE gets 18.
grep -q "created table 't' -> ns=16 table_id=18" "$LOG" && ok "dropped ids not reused (got 18)" || bad "table_id reuse"

# --- unknown database fails per query ---------------------------------------
expect "unknown dbname errors" "database does not exist" \
    psql "host=127.0.0.1 port=3210 dbname=nope sslmode=disable" -X -c "CREATE TABLE x (a int);"

# --- restart: names + id counter survive WAL replay -------------------------
stop_server
start_server
expect "'t' survives restart" "already exists" psql "$E" -X -c "CREATE TABLE t (id int);"
expect "post-restart CREATE" "CREATE TABLE" psql "$E" -X -c "CREATE TABLE post_restart (a int4 PRIMARY KEY);"
# Ids so far in ns16: t=16,17 then re-t=18,19 → next table is 20.
grep -q "created table 'post_restart' -> ns=16 table_id=20" "$LOG" && ok "id counter survived restart (20)" || bad "id counter reset"

# --- PK auto-index: names share the table namespace, DROP TABLE rejects it --
# post_restart has a PK, so its 'post_restart_pkey' index tree + name exist.
expect "index name reserved in table namespace" "already exists" \
    psql "$E" -X -c "CREATE TABLE post_restart_pkey (a int);"
expect "DROP TABLE on an index is rejected" "cannot DROP TABLE an index" \
    psql "$E" -X -c "DROP TABLE post_restart_pkey;"
# A table named <x>_pkey blocks a later 'x' whose PK index wants that name.
expect "CREATE TABLE mine_pkey" "CREATE TABLE" psql "$E" -X -c "CREATE TABLE mine_pkey (a int);"
expect "PK index name collision fails CREATE" "already exists" \
    psql "$E" -X -c "CREATE TABLE mine (id int PRIMARY KEY);"

# --- per-database name scoping + drop cascades ------------------------------
expect "CREATE DATABASE app" "CREATE DATABASE" psql "$E" -X -c "CREATE DATABASE app;"
expect "same table name in app" "CREATE TABLE" psql "$A" -X -c "CREATE TABLE t (x uint8);"
expect "DROP DATABASE with tables" "DROP DATABASE" psql "$E" -X -c "DROP DATABASE app;"
expect "dropped-db session fails" "database does not exist" psql "$A" -X -c "CREATE TABLE y (a int);"

# --- secondary indexes: create / backfill / DML / drop ----------------------
expect "CREATE TABLE items" "CREATE TABLE" psql "$E" -X -c "CREATE TABLE items (id int PRIMARY KEY, color text);"
expect "INSERT pre-index rows" "INSERT 0 3" psql "$E" -X -c "INSERT INTO items VALUES (1,'blue'),(2,'red'),(3,'blue');"
# Blocking online build: the reply means built + valid (backfill done).
expect "CREATE INDEX backfills" "CREATE INDEX" psql "$E" -X -c "CREATE INDEX items_color ON items (color);"
expect "duplicate index name" "already exists" psql "$E" -X -c "CREATE INDEX items_color ON items (color);"
expect "IF NOT EXISTS is quiet (index)" "CREATE INDEX" psql "$E" -X -c "CREATE INDEX IF NOT EXISTS items_color ON items (color);"
expect "second index on the column" "already indexed" psql "$E" -X -c "CREATE INDEX another ON items (color);"
expect "UNIQUE index rejected" "UNIQUE" psql "$E" -X -c "CREATE UNIQUE INDEX u ON items (color);"
expect "index on missing column" "column does not exist" psql "$E" -X -c "CREATE INDEX i2 ON items (nope);"
expect "index on missing table" "table does not exist" psql "$E" -X -c "CREATE INDEX i3 ON nope (a);"

# DML maintains postings; equality on the indexed column now routes
# through the posting bitmap (getBitmap + own-txn overlay).
expect "SELECT backfilled matches" "(2 rows)" psql "$E" -X -c "SELECT id FROM items WHERE color = 'blue';"
expect "INSERT with index live" "INSERT 0 1" psql "$E" -X -c "INSERT INTO items VALUES (4,'green');"
expect "UPDATE moves the index key" "UPDATE 1" psql "$E" -X -c "UPDATE items SET color = 'blue' WHERE id = 4;"
expect "DELETE with index live" "DELETE 1" psql "$E" -X -c "DELETE FROM items WHERE id = 2;"
expect "SELECT after churn" "(3 rows)" psql "$E" -X -c "SELECT id FROM items WHERE color = 'blue';"

# Bitmap-path semantics: own-txn pending postings, key moves in bulk,
# = NULL, and indexed UPDATE/DELETE (lock-through-bitmap).
expect "own pending INSERT visible via index" "(4 rows)" psql "$E" -X <<'PSQL'
BEGIN;
INSERT INTO items VALUES (6,'blue');
SELECT id FROM items WHERE color = 'blue';
ROLLBACK;
PSQL
expect "rollback leaves postings untouched" "(3 rows)" psql "$E" -X -c "SELECT id FROM items WHERE color = 'blue';"
expect "UPDATE routed via the index" "UPDATE 3" psql "$E" -X -c "UPDATE items SET color = 'cyan' WHERE color = 'blue';"
expect "moved rows found under the new key" "(3 rows)" psql "$E" -X -c "SELECT id FROM items WHERE color = 'cyan';"
expect "old key is empty" "(0 rows)" psql "$E" -X -c "SELECT id FROM items WHERE color = 'blue';"
expect "move the rows back" "UPDATE 3" psql "$E" -X -c "UPDATE items SET color = 'blue' WHERE color = 'cyan';"
expect "NULL cell inserted" "INSERT 0 1" psql "$E" -X -c "INSERT INTO items VALUES (7, NULL);"
expect "= NULL matches via the index" "(1 row)" psql "$E" -X -c "SELECT id FROM items WHERE color = NULL;"
expect "DELETE routed via the index" "DELETE 1" psql "$E" -X -c "DELETE FROM items WHERE color = NULL;"

# DROP INDEX and its rejections.
expect "DROP TABLE of an index name" "use DROP INDEX" psql "$E" -X -c "DROP TABLE items_color;"
expect "DROP INDEX of a base table" "not an index" psql "$E" -X -c "DROP INDEX items;"
expect "cannot drop the pkey index" "PRIMARY KEY index" psql "$E" -X -c "DROP INDEX items_pkey;"
expect "DROP INDEX" "DROP INDEX" psql "$E" -X -c "DROP INDEX items_color;"
expect "DROP INDEX missing errors" "does not exist" psql "$E" -X -c "DROP INDEX items_color;"
expect "DROP INDEX IF EXISTS quiet" "DROP INDEX" psql "$E" -X -c "DROP INDEX IF EXISTS items_color;"
expect "SELECT after drop still answers" "(3 rows)" psql "$E" -X -c "SELECT id FROM items WHERE color = 'blue';"
expect "re-CREATE freed index name" "CREATE INDEX" psql "$E" -X -c "CREATE INDEX items_color ON items (color);"

# --- routing: prove indexed equality does NOT scan --------------------------
# SI discriminator: a statement that RAN A SCAN poisons any later
# read_ts bump in its txn (a scan can't be revalidated → 40001), while
# an index-routed read revalidates by its posting key and the bump
# succeeds. Same interleaving both times — read, concurrent commit on an
# unrelated row, then lock that row; success on the first leg is the
# proof the indexed SELECT never scanned.
expect "CREATE TABLE rt" "CREATE TABLE" psql "$E" -X -c "CREATE TABLE rt (id int PRIMARY KEY, c text, d text);"
expect "CREATE INDEX rt_c" "CREATE INDEX" psql "$E" -X -c "CREATE INDEX rt_c ON rt (c);"
expect "INSERT rt rows" "INSERT 0 2" psql "$E" -X -c "INSERT INTO rt VALUES (1,'x','p'),(2,'y','q');"
expect "indexed read, then bump succeeds" "UPDATE 1" psql "$E" -X <<'PSQL'
BEGIN;
SELECT id FROM rt WHERE c = 'x';
\! psql "host=127.0.0.1 port=3210 dbname=egres sslmode=disable" -X -c "UPDATE rt SET d='q2' WHERE id = 2;" >/dev/null
UPDATE rt SET d = 'r' WHERE id = 2;
COMMIT;
PSQL
expect "scan read, then bump conflicts" "could not serialize" psql "$E" -X <<'PSQL'
BEGIN;
SELECT id FROM rt WHERE d = 'p';
\! psql "host=127.0.0.1 port=3210 dbname=egres sslmode=disable" -X -c "UPDATE rt SET d='s' WHERE id = 1;" >/dev/null
UPDATE rt SET d = 't' WHERE id = 1;
COMMIT;
PSQL
expect "DROP TABLE rt" "DROP TABLE" psql "$E" -X -c "DROP TABLE rt;"

# Restart: index tree, catalog row, and backfilled SSTs survive replay.
stop_server
start_server
expect "index survives restart" "already exists" psql "$E" -X -c "CREATE INDEX items_color ON items (color);"
expect "indexed write after restart" "INSERT 0 1" psql "$E" -X -c "INSERT INTO items VALUES (5,'red');"
expect "DROP TABLE cascades the index" "DROP TABLE" psql "$E" -X -c "DROP TABLE items;"
expect "cascaded index name freed" "CREATE TABLE" psql "$E" -X -c "CREATE TABLE items_color (a int);"

# --- no unexpected server-side errors ---------------------------------------
if grep -q "^error" "$LOG"; then bad "server log has errors"; else ok "server log clean"; fi

echo
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
