#!/usr/bin/env bash
# Concurrency e2e for interactive transactions over pgwire. Two long-lived
# psql sessions (FIFO-fed) drive: BEGIN/COMMIT/ROLLBACK visibility across
# sessions, read-your-own-writes (point + scan), row-lock blocking with
# FIFO hand-off and last-writer-wins, deadlock detection (40P01 for the
# edge-closer), failed-txn state (25P02, COMMIT→ROLLBACK), DDL-in-txn
# rejection (25001), table intent locks (DROP TABLE queues behind open
# DML txns), read_ts bumps (blocked writer restarts and applies with no
# abort; a stale prior read aborts 40001), SELECT FOR UPDATE (blocks
# writers not readers, deadlocks 40P01, a cell-filter scan locks exactly
# the matches, FOR SHARE / NOWAIT reject), PRIMARY KEY enforcement (unique
# index: 23505 duplicates in-statement + sequential + concurrent, 23502 NULL
# keys, DELETE frees the key, UPDATE-of-PK 0A000, and the index-key lock
# serializing racing inserters), and disconnect rollback + lock release.
# Wipes /data/egres.
set -euo pipefail
cd "$(dirname "$0")/.."

DATA="${DATA:-/data/egres}"
LOG=/tmp/zdb/server.log
E="host=127.0.0.1 port=3210 dbname=egres sslmode=disable"
D=$(mktemp -d)

pass=0
fail=0
ok() { echo "ok: $1"; pass=$((pass + 1)); }
bad() { echo "FAIL: $1"; fail=$((fail + 1)); }

q() { psql "$E" -X -c "$1"; }     # full output (row counts, tags)
v() { psql "$E" -X -tA -c "$1"; } # tuples-only, unaligned (bare value)

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
trap 'stop_server; rm -rf "$D"' EXIT

# --- session plumbing ------------------------------------------------------
# Session N: a FIFO (kept open on fd 3+N) feeding a long-lived psql whose
# output lands in $D/outN. `sync_session` sends an \echo marker and waits
# for it — i.e. for every statement queued before it to have finished.
open_session() { # $1 = 1|2
    mkfifo "$D/in$1"
    # 4>&- 5>&-: don't let this psql inherit the OTHER session's FIFO
    # write end, or closing that fd would never EOF the other psql.
    stdbuf -oL -eL psql "$E" -X -tA -f - <"$D/in$1" >"$D/out$1" 2>&1 4>&- 5>&- &
    eval "PID$1=\$!"
    eval "exec $((3 + $1))>\"$D/in$1\""
    send "$1" "\\set VERBOSITY verbose" # errors show their SQLSTATE
}
send() { printf '%s\n' "$2" >&$((3 + $1)); }
MARK=0
sync_session() { # wait (<=10s) for session $1 to drain its queue
    MARK=$((MARK + 1))
    send "$1" "\\echo MARK$MARK"
    for _ in $(seq 1 100); do
        if grep -q "MARK$MARK" "$D/out$1"; then return 0; fi
        sleep 0.1
    done
    bad "session $1 never reached MARK$MARK"
    return 1
}
close_session() {
    eval "exec $((3 + $1))>&-"
    eval "wait \$PID$1" 2>/dev/null || true
}

zig build
rm -rf "$DATA" && mkdir -p "$DATA" /tmp/zdb
start_server

q "CREATE TABLE t (id int4, name text);" >/dev/null
q "INSERT INTO t VALUES (1, 'base');" >/dev/null # egid 1

open_session 1
open_session 2

# --- commit visibility across sessions -------------------------------------
send 1 "BEGIN;"
send 1 "INSERT INTO t VALUES (2, 'pend');" # egid 2
sync_session 1
if [ -z "$(v "SELECT name FROM t WHERE egid = 2;")" ]; then
    ok "uncommitted insert invisible to other sessions"
else bad "uncommitted insert leaked"; fi
send 1 "COMMIT;"
sync_session 1
if [ "$(v "SELECT name FROM t WHERE egid = 2;")" = "pend" ]; then
    ok "committed insert visible"
else bad "committed insert not visible"; fi

# --- read-your-own-writes: point + scan, then rollback ----------------------
send 1 "BEGIN;"
send 1 "INSERT INTO t VALUES (3, 'ryow');" # egid 3 (burned by rollback)
send 1 "SELECT name FROM t WHERE egid = 3;" # point read via pending
send 1 "SELECT name FROM t;"                # scan overlays pending
sync_session 1
if [ "$(grep -c ryow "$D/out1")" = "2" ]; then
    ok "txn reads its own pending insert (point + scan)"
else bad "read-your-own-writes — out1: $(grep ryow "$D/out1" | tr '\n' ' ')"; fi
if [ -z "$(v "SELECT name FROM t WHERE egid = 3;")" ]; then
    ok "pending row invisible to other sessions"
else bad "pending row leaked mid-txn"; fi
send 1 "ROLLBACK;"
sync_session 1
if [ -z "$(v "SELECT name FROM t WHERE egid = 3;")" ]; then
    ok "rolled-back insert never lands"
else bad "rolled-back insert landed"; fi

# --- blocked writer: FIFO hand-off, last writer wins ------------------------
send 1 "BEGIN;"
send 1 "UPDATE t SET name = 's1' WHERE egid = 1;"
sync_session 1
send 2 "UPDATE t SET name = 's2' WHERE egid = 1;" # blocks on the row lock
sleep 0.3
if grep -q "UPDATE 1" "$D/out2"; then
    bad "writer was not blocked by the row lock"
else ok "concurrent writer blocks on the row lock"; fi
send 1 "COMMIT;"
sync_session 2
if grep -q "UPDATE 1" "$D/out2"; then
    ok "blocked writer completes after commit (hand-off)"
else bad "blocked writer never completed"; fi
if [ "$(v "SELECT name FROM t WHERE egid = 1;")" = "s2" ]; then
    ok "last writer wins after hand-off"
else bad "unexpected final value: $(v "SELECT name FROM t WHERE egid = 1;")"; fi

# --- deadlock: closing edge refused with 40P01, victim recovers --------------
# S1 holds row 1 and parks on row 2; S2 holds row 2 and asks for row 1 —
# S2 inserts the cycle's last edge and is refused on the spot (no
# timeout involved; detection runs before anyone parks).
send 1 "BEGIN;"
send 1 "UPDATE t SET name = 'd1' WHERE egid = 1;"
sync_session 1
send 2 "BEGIN;"
send 2 "UPDATE t SET name = 'd2' WHERE egid = 2;"
sync_session 2
send 1 "UPDATE t SET name = 'd1' WHERE egid = 2;" # parks behind S2
sleep 0.3 # S1 is parked mid-statement and cannot answer a sync marker
send 2 "UPDATE t SET name = 'd2' WHERE egid = 1;" # closes the cycle
sync_session 2
if grep -q "40P01" "$D/out2"; then
    ok "deadlock refused at the closing edge with 40P01"
else bad "no 40P01 for the deadlock closer"; fi
send 2 "SELECT name FROM t WHERE egid = 1;"
sync_session 2
if grep -q "25P02" "$D/out2"; then
    ok "failed txn rejects statements with 25P02"
else bad "failed txn accepted a statement"; fi
send 2 "COMMIT;" # of a failed txn -> rolls back
sync_session 2
if grep -q "ROLLBACK" "$D/out2"; then
    ok "COMMIT of a failed txn reports ROLLBACK"
else bad "failed-txn COMMIT did not roll back"; fi
# The victim's abort released row 2 — the survivor's parked UPDATE
# completes (third "UPDATE 1" in out1: hand-off leg + both of S1's).
sync_session 1
if [ "$(grep -c "UPDATE 1" "$D/out1")" = "3" ]; then
    ok "victim's abort unblocked the survivor"
else bad "survivor still blocked after victim aborted"; fi
send 1 "ROLLBACK;"
sync_session 1

# --- DDL inside a txn block is rejected -------------------------------------
send 2 "BEGIN;"
send 2 "CREATE TABLE t2 (a int4);"
sync_session 2
if grep -q "25001" "$D/out2"; then
    ok "DDL in txn block rejected with 25001"
else bad "DDL in txn block was not rejected"; fi
send 2 "ROLLBACK;"
sync_session 2

# --- table intent lock: DROP TABLE queues behind an open DML txn -------------
send 1 "BEGIN;"
send 1 "INSERT INTO t VALUES (7, 'intent');" # shared intent on t
sync_session 1
send 2 "DROP TABLE t;" # exclusive intent — parks behind S1's txn
sleep 0.3
# While the drop is queued, writer preference parks NEW DML on t too,
# so don't touch t from a third connection here.
if grep -q "DROP TABLE" "$D/out2"; then
    bad "DROP TABLE did not wait for the open txn"
else ok "DROP TABLE blocks behind an open txn's intent lock"; fi
send 1 "COMMIT;"
sync_session 2
if grep -q "DROP TABLE" "$D/out2"; then
    ok "DROP TABLE completes once the txn commits"
else bad "DROP TABLE never completed"; fi
# || true: psql exits 1 on the expected error; pipefail would otherwise
# fail the pipeline even when grep matches.
if { q "SELECT name FROM t;" 2>&1 || true; } | grep -q "does not exist"; then
    ok "table is gone after the queued drop"
else bad "table still queryable after drop"; fi

q "CREATE TABLE t2 (id int4, name text);" >/dev/null
q "INSERT INTO t2 VALUES (1, 'base');" >/dev/null # egid 1

# --- read_ts bump: a blocked writer lands with NO abort ----------------------
# S2's implicit UPDATE parks behind S1's lock; when S1 commits, S2's
# lock acquire finds the newer commit, bumps its read_ts, restarts the
# statement against the fresh snapshot, and applies — no 40001, no
# client retry.
send 1 "BEGIN;"
send 1 "UPDATE t2 SET name = 'b1' WHERE egid = 1;"
sync_session 1
send 2 "UPDATE t2 SET name = 'b2' WHERE egid = 1;" # parks on the row lock
sleep 0.3
send 1 "COMMIT;"
sync_session 2
if [ "$(v "SELECT name FROM t2 WHERE egid = 1;")" = "b2" ]; then
    ok "blocked writer bumped past the commit and applied"
else bad "bump+restart lost the blocked write"; fi
if grep -q "40001" "$D/out2"; then
    bad "bump+restart surfaced a serialization abort"
else ok "no serialization abort during bump+restart"; fi

# --- stale interactive read: the bump refuses and aborts (40001) -------------
# S2 point-reads the row in one statement; a concurrent commit changes
# it; S2's next UPDATE needs a bump, but its prior read is now stale —
# unrepairable (the value already reached the client) → 40001.
send 2 "BEGIN;"
send 2 "SELECT name FROM t2 WHERE egid = 1;"
sync_session 2
q "UPDATE t2 SET name = 'x1' WHERE egid = 1;" >/dev/null
send 2 "UPDATE t2 SET name = 'x2' WHERE egid = 1;"
sync_session 2
if grep -q "40001" "$D/out2"; then
    ok "stale prior read turns the bump into 40001"
else bad "conflicting update after a stale read did not abort"; fi
send 2 "ROLLBACK;"
sync_session 2
if [ "$(v "SELECT name FROM t2 WHERE egid = 1;")" = "x1" ]; then
    ok "aborted txn left the concurrent commit in place"
else bad "unexpected value after abort: $(v "SELECT name FROM t2 WHERE egid = 1;")"; fi

# --- SELECT FOR UPDATE: blocks writers, not readers --------------------------
send 1 "BEGIN;"
send 1 "SELECT name FROM t2 WHERE egid = 1 FOR UPDATE;"
sync_session 1
if grep -q "x1" "$D/out1"; then
    ok "FOR UPDATE returns the locked row"
else bad "FOR UPDATE returned nothing"; fi
NU=$(grep -c "UPDATE 1" "$D/out2" || true)
send 2 "UPDATE t2 SET name = 'w2' WHERE egid = 1;" # parks on S1's row lock
sleep 0.3
if [ "$(grep -c "UPDATE 1" "$D/out2" || true)" = "$NU" ]; then
    ok "writer blocks behind FOR UPDATE"
else bad "writer was not blocked by FOR UPDATE"; fi
if [ "$(v "SELECT name FROM t2 WHERE egid = 1;")" = "x1" ]; then
    ok "plain read passes FOR UPDATE without blocking"
else bad "reader blocked or saw uncommitted data"; fi
send 1 "COMMIT;"
sync_session 2
if [ "$(grep -c "UPDATE 1" "$D/out2" || true)" = "$((NU + 1))" ] &&
    [ "$(v "SELECT name FROM t2 WHERE egid = 1;")" = "w2" ]; then
    ok "blocked writer lands after FOR UPDATE holder commits"
else bad "blocked writer never landed after commit"; fi

# --- FOR UPDATE deadlock: closing edge refused with 40P01 --------------------
q "INSERT INTO t2 VALUES (2, 'row2');" >/dev/null # egid 2
send 1 "BEGIN;"
send 1 "SELECT name FROM t2 WHERE egid = 1 FOR UPDATE;"
sync_session 1
send 2 "BEGIN;"
send 2 "SELECT name FROM t2 WHERE egid = 2 FOR UPDATE;"
sync_session 2
N40=$(grep -c "40P01" "$D/out2" || true)
send 1 "SELECT name FROM t2 WHERE egid = 2 FOR UPDATE;" # parks behind S2
sleep 0.3 # S1 is parked mid-statement and cannot answer a sync marker
send 2 "SELECT name FROM t2 WHERE egid = 1 FOR UPDATE;" # closes the cycle
sync_session 2
if [ "$(grep -c "40P01" "$D/out2" || true)" = "$((N40 + 1))" ]; then
    ok "FOR UPDATE deadlock refused at the closing edge with 40P01"
else bad "no 40P01 for the FOR UPDATE deadlock closer"; fi
send 2 "COMMIT;" # of a failed txn -> rolls back; releases row 2
sync_session 2
sync_session 1 # S1's parked FOR UPDATE completes
if grep -q "row2" "$D/out1"; then
    ok "victim's abort unblocked the parked FOR UPDATE"
else bad "parked FOR UPDATE never completed"; fi
send 1 "ROLLBACK;"
sync_session 1

# --- FOR UPDATE scan: the cell filter locks the matches, nothing else --------
send 1 "BEGIN;"
send 1 "SELECT name FROM t2 WHERE name = 'row2' FOR UPDATE;" # locks egid 2
sync_session 1
NU=$(grep -c "UPDATE 1" "$D/out2" || true)
send 2 "UPDATE t2 SET name = 'nb1' WHERE egid = 1;" # unmatched row: no lock
sync_session 2
if [ "$(grep -c "UPDATE 1" "$D/out2" || true)" = "$((NU + 1))" ]; then
    ok "unmatched row stays unlocked under FOR UPDATE scan"
else bad "FOR UPDATE scan locked an unmatched row"; fi
send 2 "UPDATE t2 SET name = 'z2' WHERE egid = 2;" # matched row: parks
sleep 0.3
if [ "$(grep -c "UPDATE 1" "$D/out2" || true)" = "$((NU + 1))" ]; then
    ok "matched row is locked by FOR UPDATE scan"
else bad "matched row was not locked"; fi
send 1 "COMMIT;"
sync_session 2
if [ "$(v "SELECT name FROM t2 WHERE egid = 2;")" = "z2" ]; then
    ok "blocked writer lands after scan FOR UPDATE commits"
else bad "blocked writer lost after scan FOR UPDATE"; fi

# --- FOR UPDATE variants: implicit txn works, the rest reject cleanly --------
if q "SELECT name FROM t2 WHERE egid = 1 FOR UPDATE;" | grep -q "nb1"; then
    ok "implicit-txn FOR UPDATE works"
else bad "implicit-txn FOR UPDATE failed"; fi
if { q "SELECT name FROM t2 FOR SHARE;" 2>&1 || true; } |
    grep -q "only FOR UPDATE"; then
    ok "FOR SHARE rejected"
else bad "FOR SHARE not rejected"; fi
if { q "SELECT name FROM t2 FOR UPDATE NOWAIT;" 2>&1 || true; } |
    grep -q "not supported"; then
    ok "NOWAIT rejected"
else bad "NOWAIT not rejected"; fi

# --- PRIMARY KEY: unique-index enforcement + concurrency --------------------
# A real PK table (the suites above are egid-only). Enforcement is a unique
# index: duplicates 23505, NULL keys 23502, and the index entry is locked so
# concurrent inserters of one key serialize.
q "CREATE TABLE pk (id int4 PRIMARY KEY, name text);" >/dev/null
q "INSERT INTO pk VALUES (1, 'a'), (3, 'c');" >/dev/null
# q() runs a non-verbose psql, so these grep the error message text (the
# SQLSTATE only prints under \set VERBOSITY verbose, which the sessions set).
if { q "INSERT INTO pk VALUES (1, 'dup');" 2>&1 || true; } | grep -q "duplicate key"; then
    ok "sequential duplicate PK rejected"
else bad "sequential duplicate PK not rejected"; fi
if { q "INSERT INTO pk VALUES (8, 'x'), (8, 'y');" 2>&1 || true; } | grep -q "duplicate key"; then
    ok "in-statement duplicate PK rejected"
else bad "in-statement duplicate PK not rejected"; fi
if { q "INSERT INTO pk (name) VALUES ('z');" 2>&1 || true; } | grep -q "not-null constraint"; then
    ok "NULL primary key rejected"
else bad "NULL primary key not rejected"; fi
if q "INSERT INTO pk VALUES (4, 'd'), (5, 'e');" | grep -q "INSERT 0 2"; then
    ok "distinct multi-row INSERT succeeds"
else bad "distinct multi-row INSERT failed"; fi
# DELETE clears the index entry, so the same PK re-inserts (index maintained).
q "DELETE FROM pk WHERE id = 1;" >/dev/null
if q "INSERT INTO pk VALUES (1, 'a2');" | grep -q "INSERT 0 1"; then
    ok "re-INSERT of a deleted PK succeeds"
else bad "re-INSERT after delete rejected — stale index entry"; fi
if { q "UPDATE pk SET id = 99 WHERE id = 3;" 2>&1 || true; } | grep -q "cannot be updated"; then
    ok "UPDATE of the PK column rejected"
else bad "UPDATE of the PK column not rejected"; fi

# Concurrent insert of one key: the loser parks on the index-key lock, then
# 23505 once the winner commits.
DUP0=$(grep -c "23505" "$D/out2" || true)
INS0=$(grep -c "INSERT 0 1" "$D/out2" || true)
send 1 "BEGIN;"
send 1 "INSERT INTO pk VALUES (20, 's1');"
sync_session 1
send 2 "INSERT INTO pk VALUES (20, 's2');" # parks on the index-key lock
sleep 0.3
if [ "$(grep -c "INSERT 0 1" "$D/out2")" = "$INS0" ] &&
    [ "$(grep -c "23505" "$D/out2")" = "$DUP0" ]; then
    ok "concurrent same-PK insert blocks on the index lock"
else bad "second inserter was not blocked"; fi
send 1 "COMMIT;"
sync_session 2
if [ "$(grep -c "23505" "$D/out2")" -gt "$DUP0" ]; then
    ok "loser gets 23505 once the winner commits"
else bad "loser did not get 23505 after winner committed"; fi

# Same race, but the holder ROLLBACKs → the key is free, the waiter lands.
INS1=$(grep -c "INSERT 0 1" "$D/out2" || true)
send 1 "BEGIN;"
send 1 "INSERT INTO pk VALUES (21, 'r1');"
sync_session 1
send 2 "INSERT INTO pk VALUES (21, 'r2');" # parks on the index-key lock
sleep 0.3
send 1 "ROLLBACK;"
sync_session 2
if [ "$(grep -c "INSERT 0 1" "$D/out2")" -gt "$INS1" ]; then
    ok "waiter's insert lands after the holder rolls back"
else bad "waiter blocked or rejected after rollback"; fi

# WHERE <pk> = v routes through the unique index (point lookup, not a scan).
if [ "$(v "SELECT name FROM pk WHERE id = 4;")" = "d" ]; then
    ok "SELECT WHERE pk routes through the index and returns the row"
else bad "index point SELECT wrong: $(v "SELECT name FROM pk WHERE id = 4;")"; fi

# FOR UPDATE on an ABSENT key locks nothing (readers lock rows they return,
# never keys — Postgres's rule): a concurrent INSERT of that key lands at
# once. The reader's own later INSERT of it then finds a commit above its
# snapshot on a key it already read — an unrepairable stale read → 40001,
# the SI outcome; the row is the other session's.
GI0=$(grep -c "INSERT 0 1" "$D/out2" || true)
S1_40001=$(grep -c "40001" "$D/out1" || true)
send 1 "BEGIN;"
send 1 "SELECT name FROM pk WHERE id = 50 FOR UPDATE;" # 0 rows, no lock
sync_session 1
send 2 "INSERT INTO pk VALUES (50, 'ghost');"
sync_session 2
if [ "$(grep -c "INSERT 0 1" "$D/out2")" = "$((GI0 + 1))" ]; then
    ok "FOR UPDATE on an absent PK does not block a concurrent insert"
else bad "insert of an absent key blocked behind a FOR UPDATE reader"; fi
send 1 "INSERT INTO pk VALUES (50, 'late');"
sync_session 1
if [ "$(grep -c "40001" "$D/out1")" = "$((S1_40001 + 1))" ]; then
    ok "the reader's own insert of that key sees its stale read: 40001"
else bad "no 40001 for an insert after a stale absent-key read"; fi
send 1 "ROLLBACK;"
sync_session 1
if [ "$(v "SELECT name FROM pk WHERE id = 50;")" = "ghost" ]; then
    ok "the concurrent insert's row is the one that stands"
else bad "unexpected row 50: $(v "SELECT name FROM pk WHERE id = 50;")"; fi

# --- FOR UPDATE over more than one executor batch (1024 rows) ---------------
# values_list <first id> <count>: "(i, 'ri'), ..." for one INSERT.
values_list() {
    local i=$1 end=$(($1 + $2)) sep=""
    while [ "$i" -lt "$end" ]; do
        printf '%s(%d, %s)' "$sep" "$i" "'r$i'"
        sep=", "
        i=$((i + 1))
    done
}
q "CREATE TABLE wide (id int4, name text);" >/dev/null
for s in $(seq 1 500 2500); do
    q "INSERT INTO wide VALUES $(values_list "$s" 500);" >/dev/null
done
W0=$(grep -cE '^[0-9]+$' "$D/out1" || true)
send 1 "BEGIN;"
send 1 "SELECT id FROM wide FOR UPDATE;" # 2500 rows: drain, lock all, emit
sync_session 1
if [ "$(grep -cE '^[0-9]+$' "$D/out1" || true)" = "$((W0 + 2500))" ]; then
    ok "FOR UPDATE emits every row across batches"
else bad "FOR UPDATE over 2500 rows emitted the wrong count"; fi
NU=$(grep -c "UPDATE 1" "$D/out2" || true)
send 2 "UPDATE wide SET name = 'w' WHERE id = 2000;" # a row past batch 1
sleep 0.3
if [ "$(grep -c "UPDATE 1" "$D/out2" || true)" = "$NU" ]; then
    ok "a row in a later batch is locked by FOR UPDATE"
else bad "FOR UPDATE did not lock a row past the first batch"; fi
send 1 "COMMIT;"
sync_session 2
if [ "$(grep -c "UPDATE 1" "$D/out2" || true)" = "$((NU + 1))" ]; then
    ok "blocked writer lands after the multi-batch FOR UPDATE commits"
else bad "blocked writer never landed"; fi

# --- restart mid-drain: a full-table UPDATE parks, bumps, re-runs once -------
# S2 holds row 1300 (mid-table). S1's UPDATE drains all 2500 rows, then
# its lock pass parks on 1300. S2's commit changes that row's id, so
# S1's lockRow sees a newer commit, bumps read_ts, Restarts, re-drains
# from scratch and applies exactly once: one UPDATE 2500, every row
# rewritten, and the re-read pre-image carries S2's id.
send 2 "BEGIN;"
send 2 "UPDATE wide SET id = 9999 WHERE id = 1300;"
sync_session 2
R0=$(grep -c "UPDATE 2500" "$D/out1" || true)
A0=$(grep -c "40001" "$D/out1" || true) # earlier legs expect some
send 1 "UPDATE wide SET name = 'r';" # drains, then parks on row 1300
sleep 0.5
send 2 "COMMIT;"
sync_session 1
if [ "$(grep -c "UPDATE 2500" "$D/out1" || true)" = "$((R0 + 1))" ]; then
    ok "full-table UPDATE restarted past the held row and applied once"
else bad "full-table UPDATE did not report UPDATE 2500 exactly once"; fi
if [ "$(grep -c "40001" "$D/out1" || true)" = "$A0" ]; then
    ok "no serialization abort during the mid-drain restart"
else bad "the mid-drain restart surfaced a serialization abort"; fi
if [ "$(v "SELECT name FROM wide WHERE id = 9999;")" = "r" ]; then
    ok "the restart re-read the concurrent commit"
else bad "the restarted UPDATE missed the concurrent commit"; fi
if q "SELECT id FROM wide WHERE name = 'r';" | grep -q "(2500 rows)"; then
    ok "every row carries the restarted UPDATE's value"
else bad "rows missing the restarted UPDATE's value"; fi

# --- disconnect mid-txn: rollback + lock release (t is dropped; use t2) ------
send 1 "BEGIN;"
send 1 "INSERT INTO t2 VALUES (9, 'ghost');"
send 1 "UPDATE t2 SET name = 'held' WHERE egid = 1;"
sync_session 1
close_session 1 # Terminate mid-txn
sleep 0.3
if [ -z "$(v "SELECT name FROM t2 WHERE name = 'ghost';")" ]; then
    ok "disconnect rolled the txn back"
else bad "disconnected txn's insert landed"; fi
if q "UPDATE t2 SET name = 'free' WHERE egid = 1;" | grep -q "UPDATE 1"; then
    ok "disconnect released the row lock"
else bad "row lock survived the disconnect"; fi

close_session 2

# --- no unexpected server-side errors --------------------------------------
if grep -q "^error" "$LOG"; then bad "server log has errors"; else ok "server log clean"; fi

echo
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
