# Transactions in Egres

Design notes from the 2026-07 discussion. Not yet implemented — INSERT and
SELECT ship first as implicit single-statement transactions; the machinery
below bolts on without rewiring (see Implementation order at the end).

## Row storage model (the substrate)

- Every row is keyed by an **egid**: a per-table, monotonically increasing
  u64, stored as an 8-byte big-endian key in the table's data tree. BE
  means scans iterate in insertion order and the last key in the tree is
  the highest egid ever allocated.
- Fixed-width u64 row ids make every set of rows — index postings, filter
  results, intermediate query state — representable as a roaring bitmap,
  so query execution composes via bitmap AND/OR instead of merging
  variable-length key streams.
- The user-declared PRIMARY KEY is **not** the physical clustering. It is
  a unique secondary index: a separate tree mapping order-preserving
  encoded pkey bytes -> egid. Cost: pkey point reads take two tree hops
  (index -> egid -> row). Benefit: every index, pkey included, is
  structurally identical, which is what makes bitmap execution uniform.
- **An index is a table_id**: allocated from the same per-namespace
  counter as tables, linked to its base table via the catalog (Postgres
  does the same — indexes are relations). Reuses create_tree / drop /
  splits / snapshot wholesale.
- **egid is exposed, SQLite-style**: reserved column name (CREATE TABLE
  with a column named `egid` errors), synthesized from the key at read
  time (never stored in the row payload), excluded from `SELECT *`,
  available as `SELECT egid, ...` and `WHERE egid = N` (fastest point
  read: one tree get, no index hop). A table with no PRIMARY KEY has no
  pkey index at all — it is keyed by egid and the user uses egid as the
  id. pkey_position all-NULL in the catalog encodes this for free.

### egid allocation (IMPLEMENTED — apply-observed, no persisted counter)

Rejected the manifest-lease design (a persisted per-table high-water via a
new cmd) as over-built: the counter is a pure function of applied state, so
`catchUp` reconstructs it and persisting it separately is redundant. Also
special-casing egid in the manifest is inconsistent with user monotonic ids,
which won't be manifest state either.

- **`Forest.maxKey(ns, table_id)`** — a generic "largest key in a table"
  primitive (rightmost tree, rightmost SST per level via its in-memory
  `biggest`, then a bounded memtable scan above that floor). egid-agnostic;
  zdb never knows what an egid is.
- **`ApplyObserver` hook** (engine → SQL, fired on the apply fiber after each
  committed Mutations entry, on EVERY node): SQL maintains a per-table
  `next_egid` as `max(next_egid, committed_egid + 1)`. Because followers run
  apply too, a node that becomes leader already has `next_egid` current —
  self-healing across leader changes with NO term tracking.
- **Seed** at cache-load via `forest.maxKey` (covers snapshot SSTs the
  observer never saw); **hand-out** on the leader is `egid = next_egid++`;
  `catchUp` gates hand-out so the committed tail is applied (observed) first.
  One counter, on `sql.Server`'s cached `table.Table`. See sql.Server.allocEgid.
- Allocated at INSERT time, not commit. Aborted/uncommitted ids burn; gaps
  are fine (Postgres sequences are non-transactional for the same reason).
- Deletes (IMPLEMENTED as SQL tombstones, `Meta.deleted`): `maxKey` counts
  tombstoned keys (`include_deleted`, consistent with step 1's physical
  `biggest`), so a deleted current-max still seeds `next_egid` past itself in
  the common case. If the horizon later advances and a deleted max is fully
  compacted away, `maxKey` under-reads and an egid can be reused — and that is
  fine. Under-read and stale-leftover are mutually exclusive: the key is missed
  only once every version incl. the tombstone is erased, so there is nothing
  left to resurrect, and a reused egid is always rewritten with a fresh (newest)
  timestamp. This is SQLite-rowid / Postgres-ctid semantics: reuse is a
  client-facing id-stability tradeoff, not a correctness property.
- The real invariant for when reuse matters: once secondary indexes exist,
  DELETE must remove index postings in the SAME Mutations entry as the row
  tombstone. With that atomicity there is no posting that outlives the row, so a
  reused egid can never surface an old index hit. A persisted egid high-water
  (no-reuse guarantee for external references) is then purely optional.

## Why pessimistic

Optimistic (Badger/Dgraph-style oracle + commit-time validation) was
considered and rejected: under contention it degenerates into abort
storms on hot keys — N racing transactions, 1 winner, N-1 redo all their
work, clients see spurious serialization failures. Dgraph fought exactly
this in production. Pessimistic converts contention into queueing:
bounded, predictable waits; every transaction does its work once; errors
are deterministic (a second same-pkey INSERT *waits*, then gets a real
`duplicate key` error instead of a retryable 40001).

The classic objection — holding locks across a consensus round — costs
per *contended key* only: a hot key serializes at ~1 txn per commit
latency; disjoint transactions run fully parallel. The zfiber runtime
makes waiting free (a blocked writer is a parked fiber, FIFO-woken).

Postgres for comparison is MVCC readers + pessimistic writers (row locks,
waiting) — the same half-and-half this design lands on.

## Timestamps come from the raft log

Every raft LogEntry already carries an HLC timestamp assigned by the
leader inside the propose critical section, so **log order == timestamp
order** (raft/buffers.zig). No separate ordering authority:

- `read_ts` = the leader's last-applied entry timestamp at BEGIN
  (raft/node.zig applied-timestamp progress marker).
- `commit_ts` = the commit entry's own HLC timestamp, stamped at propose.
- Reads never block and never take locks: they read the snapshot at
  read_ts, which is by construction already applied.

## Transaction lifecycle

Per pgwire session, in the sql Handler:

1. BEGIN (or implicit for auto-commit): capture read_ts.
2. Reads: pending-map first (read-your-own-writes), then forest.get at
   read_ts.
3. Writes: acquire row locks (see lock classes below), then buffer into
   the session's in-memory pending map. Nothing touches shared storage
   before commit — isolation of uncommitted writes costs zero storage
   support because they never leave the session.
4. COMMIT: propose the full write set as ONE .data raft entry (atomic
   apply across data + index trees), wait for apply, release locks, wake
   waiters, reply. No validation phase, no conflict check — commit
   cannot fail except by leader loss.
5. ROLLBACK: drop the pending map, release locks. Nothing was written.

## Lock table

Leader-local, in-memory, never replicated: followers blindly apply the
ordered log and never make decisions, so they never need locks. A
hashmap `key -> {holder txn, FIFO queue of parked fibers}`; entries
removed when the queue drains.

### Lock classes — lock what conflicts, delta what commutes

- **Unique index keys** (the pkey index and every UNIQUE index —
  one maintenance path, sql/dml.zig's index section): exclusive lock
  + lock-THEN-check. A statement collects every index mutation of
  every row it writes, locks the unique keys in (index table, key)
  order, then probes and writes; the existence check runs while
  holding the very lock any competing inserter of the same value
  queues on — that ordering is what makes `duplicate key`
  deterministic. What the statement does to one key is decided
  from all its rows at once, so `UPDATE t SET pk = pk + 1` moves a
  run of keys in one statement without a false duplicate. The lock
  order for every writer is rows first (the gather), then unique
  keys; INSERT takes keys only. A writer's read_ts is bumped to a
  freshly built unique index's `valid_ts` before it probes, so the
  backfilled entries are visible to the probe.
- **Row data keys** (egid): fresh egids are never contended on insert;
  updates/deletes of an existing row lock its data key — the per-row
  serialization point. Locking readers (FOR UPDATE, and UPDATE/DELETE
  finding their rows) lock exactly the rows they return, after the
  read — never index keys, and nothing for a key with no row behind
  it, as in Postgres. A check-then-insert race therefore ends in the
  SI way: the reader's own later insert of the key sees its stale
  read and gets 40001 to retry.
- **Non-unique index postings: NO lock.** The posting for index key K is
  a set of egids; two transactions inserting different rows with the
  same indexed value both add an egid to K. Appends to a set commute and
  the inserter never reads the posting, so the pending write is a
  **delta** (`add egid E to K`, `remove` for deletes), not a full value.
  Deltas merge at apply — already serialized by raft's single apply
  loop. IMPLEMENTED (LSM-native, the Dgraph posting-list/rollup
  design): delta records are zbuf `RoaringDelta` messages tagged
  `Meta{.delta, .roaring}` (zdb/buffers.zig); `zdb/validate.zig` is
  the write-side gate; reads materialize a zroar bitmap via
  `Forest.getBitmap`; compaction folds chains into a synthetic
  full-bitmap state (`VersionTracker` in zdb/tree.zig owns the fold,
  triggers in config.zig `rollup_*`). The SQL layer emits the deltas
  through the same per-statement index maintenance as unique keys
  and reads them with getBitmap.
  Soundness invariant: everything about a single row is serialized by
  that row's locks; cross-row index deltas only ever commute (add/remove
  of the SAME egid cannot race because the row lock orders them).
- **What a statement reads:** the committed state at read_ts plus the
  pending writes of its own transaction's EARLIER statements — never
  its own. `INSERT 5; SELECT` shows the row and `DELETE pk 5; INSERT 5`
  finds the pending tombstone, so lookups and scans merge the pending
  set in; but every read a statement makes (its row gather, its unique
  probes) precedes its first write, so it can never meet its own.

### Deadlocks

Possible by construction (T1 holds A wants B; T2 holds B wants A).
Policy: minimize aborts — wait freely; after a timeout (Postgres's
deadlock_timeout, 1s), run cycle detection on the wait-for graph and
abort one victim. Wait-die / wound-wait rejected: they abort on
suspicion, reimporting the optimistic-model problem. v1 may ship with
timeout-abort only; real cycle detection when multi-statement
transactions make cycles plausible.

### Eviction — session teardown, not a distributed protocol

Lock lifetime ⊆ session lifetime ⊆ leader lifetime; each level cleans up
the one below. No leases, TTLs, fencing tokens, or intent-cleanup
crawlers (CockroachDB needs those because its intents are PERSISTED
locks that outlive sessions; ours deliberately do not survive the
process — raft handles surviving the process).

- Client disconnect / handler error: serve-loop teardown (defer path)
  rolls back the open transaction — pending map dropped, locks released,
  waiters woken.
- Silent client death (TCP half-open): the one case teardown cannot see.
  **Idle-in-transaction timeout** (Postgres precedent): while in a
  transaction, the connection read is armed with a deadline (zfiber
  Deadlines heap); expiry aborts the transaction and closes the
  connection. The only timer in the design; bounds worst-case hold time.
- Leader death: lock table, pending writes, and every waiting fiber are
  memory in the same dead process. New leader starts empty behind
  catchUp. Open transactions are dead by construction; clients see the
  connection drop and retry — same rule raft already imposes on pending
  proposals.
- Stuck at commit (quorum lost mid proposeAndWaitForApply): committer
  parks holding locks, but no competing writer could commit without
  quorum either. Resolution: entry commits -> normal release; leadership
  lost -> propose fails on step-down -> abort path -> release. Requires
  (already true) that pending proposals fail on step-down.

## Raft payload framing

Frame .data payloads for transactions from day one so v2 is additive,
not a format migration:

    { txn_id, kind: writes | commit | abort }
    write-set entries carry op: put | merge_delta

- **v1 — commit carries everything**: one fused writes+commit entry with
  the full write set. Simple, atomic. Cost: commit latency includes
  replicating the whole write set; a large transaction is a large entry.
- **v2 — provisional streaming**: pending writes replicate through the
  log as they are made (kind=writes, staged invisibly at apply, keyed by
  txn_id); a tiny commit record flips them visible; abort (or term
  change) discards staging. Commit latency collapses to one small-entry
  round. Cost: the state machine becomes transaction-aware and recovery
  must replay staging deterministically.

## Deferred

- **Version garbage collection.** Versioned storage itself is
  implemented: every key carries its commit timestamp, a read at
  read_ts returns the newest version at or below it, a tombstone
  hides the key from later readers, and compaction folds version
  chains (`VersionTracker`, zdb/tree.zig). What is deferred is
  discarding versions nobody can read any more: the read horizon
  (`Forest.setSnapshotTs`, the oldest read_ts any open
  transaction may still use) has no production caller, so no
  version is ever dropped, and the catalog rows of dropped tables
  stay behind as unreachable garbage. Advancing it needs the
  minimum read_ts across every session's open transaction, and a
  reader below the horizon is refused (`ReadBelowMinRead`), so an
  idle interactive transaction pins it.
- Follower / lease reads (this design is leader-only).
- Lock timeouts: a DDL barrier (CREATE INDEX, DROP TABLE) waits
  behind an idle interactive transaction holding the table intent
  for as long as that transaction stays open.
- Deferrable unique constraints; `NULLS NOT DISTINCT`.

## Implementation order

1. INSERT + SELECT first, but structured as **implicit single-statement
   transactions** from the start: a thin transaction context (read_ts,
   pending writes, held locks) that auto-commit statements create,
   execute against, and commit immediately. The .data payload uses the
   txn framing above with kind=writes+commit fused.
2. Interactive transactions (BEGIN/COMMIT/ROLLBACK, ReadyForQuery I/T/E
   status) then only change the *lifetime* of that context, not the
   plumbing: the lock table, deadline, and deadlock policy arrive here.
3. Provisional streaming (v2) and versioned storage when SELECT
   consistency and commit latency demand them.
