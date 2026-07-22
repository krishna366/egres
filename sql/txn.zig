//! Transaction context for DML. Every statement runs inside a Txn —
//! implicit (one statement, auto-commit) today; interactive
//! (BEGIN..COMMIT) changes only its lifetime. Pending writes live in a
//! private skiplist — the memtable structure — holding full marshaled
//! keys at ts = read_ts. Point read-your-own-writes is a skiplist get;
//! scans can merge the skiplist in as one more iterator source; commit
//! walks it in key order into the flat wire kv list. Nothing touches
//! shared storage before commit; rollback just drops the skiplist.

const std = @import("std");
const fiber = @import("zfiber");
const zdb = @import("zdb");

const zen = zdb.buffers;
const Item = zdb.kv.Item;
const Key = zdb.kv.Key;
const Meta = zdb.kv.Meta;
const Oracle = @import("oracle.zig").Oracle;

/// One statement-generation's read tracking: the point-read identity
/// hashes it accumulated and whether it ran a scan. A Txn holds two —
/// `prev` (settled prior statements) and `cur` (the one in flight) — so
/// a restart just clears `cur`, and finishing a statement merges `cur`
/// into `prev`.
const ReadSet = struct {
    /// Identity hashes of point reads (Key.identityHash).
    reads: std.ArrayList(u64) = .empty,
    /// A scan ran: its result set can't be revalidated by key hashes (a
    /// fresh matching row would be a phantom), so a later bump must abort
    /// rather than revalidate.
    ran_scan: bool = false,

    fn deinit(self: *ReadSet, alloc: std.mem.Allocator) void {
        self.reads.deinit(alloc);
    }

    /// Discard everything — on restart, or after merging into `prev`.
    fn clear(self: *ReadSet) void {
        self.reads.clearRetainingCapacity();
        self.ran_scan = false;
    }

    /// Fold `other` in: its reads join ours and its scan flag ORs into
    /// ours. Promotes a finished statement's `cur` into `prev`.
    fn merge(self: *ReadSet, alloc: std.mem.Allocator, other: ReadSet) !void {
        try self.reads.appendSlice(alloc, other.reads.items);
        self.ran_scan = self.ran_scan or other.ran_scan;
    }

    /// Would advancing to `read_ts` invalidate any read here? A scan
    /// can't be revalidated by hash (phantoms) so it always conflicts;
    /// a point read conflicts iff its key committed above the snapshot
    /// (oracle floor > read_ts). Conservative: a commit even above the
    /// new snapshot still aborts.
    fn aboveReadTs(self: ReadSet, oracle: *Oracle, read_ts: u64) bool {
        if (self.ran_scan) return true;
        return oracle.maxCommitTs(self.reads.items) > read_ts;
    }
};

pub const Txn = struct {
    allocator: std.mem.Allocator,
    /// Owner token for the lock table; Server-issued, process-local.
    id: u64,
    ns: u64,
    /// Implicit (auto-commit) txn — created for and committed by a single
    /// statement — vs an interactive BEGIN..COMMIT block. The txn
    /// machinery ignores it; the SQL handler sets it at creation and
    /// reads it to decide whether the statement commits at its end.
    implicit: bool = false,
    /// Raft term at begin. Checked before commit: a demote/re-elect
    /// between our reads and the propose means another leader may have
    /// committed writes our locked reads never saw — abort instead.
    term: u64,
    /// The database's lock table (leader-local keyed semaphores).
    locks: *fiber.SemaphoreMap,
    /// The database's commit oracle: newest commit_ts per key identity,
    /// consulted after every fresh row lock (the bump check) and when a
    /// bump revalidates prior reads.
    oracle: *Oracle,
    /// Committed storage, the fallback half of `lookup`.
    forest: *zdb.forest.Forest,
    /// Lock keys (row + table identity hashes) this txn newly acquired
    /// (the map handles re-entrant acquires); released in deinit — lock
    /// lifetime IS txn lifetime.
    held: std.ArrayList(u64) = .empty,
    /// A statement in this txn errored: locks are already released
    /// (Postgres aborts and releases at error time), pending writes are
    /// doomed, and only COMMIT/ROLLBACK — both of which roll back — are
    /// accepted until the client ends the block.
    failed: bool = false,
    /// The snapshot every read in this txn sees. Starts at the initial
    /// read_ts and only moves FORWARD, via the bump in `lockRow`: locking
    /// a key another txn committed above our snapshot advances read_ts to
    /// that commit — after revalidating every prior read (see bump
    /// notes there) and forcing the current statement to restart at the
    /// new snapshot. Pending entries are inserted at the read_ts in
    /// effect at their put: at-or-above every committed version the txn
    /// can see, and visible to its own scans. The commit timestamp is
    /// raft's, not ours — apply rewrites every wire key's ts to the
    /// commit entry's HLC.
    read_ts: u64,
    /// Reads settled by prior statements. Revalidated on a bump (see
    /// lockRow). INVARIANT: all are valid as of the current read_ts —
    /// bump revalidates `prev`, restart re-runs `cur` — so COMMIT never
    /// validates anything.
    prev: ReadSet = .{},
    /// The in-flight statement's reads: discarded and re-run on a
    /// restart, merged into `prev` when the statement finishes.
    cur: ReadSet = .{},
    /// Pending puts and tombstones, created by the first `put` — a
    /// read-only statement never allocates it. Same-key re-put replaces
    /// value+meta in place (putItem's exact-match path) — last write
    /// wins within the txn. Owns its arena; released in deinit.
    writes: ?*zdb.skl.SkipList = null,

    pub fn init(
        allocator: std.mem.Allocator,
        id: u64,
        ns: u64,
        read_ts: u64,
        term: u64,
        locks: *fiber.SemaphoreMap,
        oracle: *Oracle,
        forest: *zdb.forest.Forest,
    ) Txn {
        return .{
            .allocator = allocator,
            .id = id,
            .ns = ns,
            .read_ts = read_ts,
            .term = term,
            .locks = locks,
            .oracle = oracle,
            .forest = forest,
        };
    }

    pub fn deinit(self: *Txn) void {
        self.releaseLocks();
        self.held.deinit(self.allocator);
        self.prev.deinit(self.allocator);
        self.cur.deinit(self.allocator);
        if (self.writes) |w| w.release();
    }

    /// A new statement begins: merge the finished statement's reads and
    /// scan flag into `prev` (they become "prior" — revalidated on a
    /// later bump) and clear `cur` for the new one. The statement driver
    /// calls this once per DML statement of an interactive txn; an
    /// implicit txn is born at its only statement's start, so its empty
    /// defaults already say the same.
    pub fn beginStatement(self: *Txn) !void {
        try self.prev.merge(self.allocator, self.cur);
        self.cur.clear();
    }

    /// Rewind the current statement's tracking before re-running it at
    /// the bumped read_ts: its point reads and scan are about to be
    /// re-executed against the new snapshot. `prev` is untouched.
    pub fn restartStatement(self: *Txn) void {
        self.cur.clear();
    }

    /// The current statement ran (or is about to run) a scan at the
    /// current read_ts.
    pub fn recordScan(self: *Txn) void {
        self.cur.ran_scan = true;
    }

    /// Release every held row lock NOW — the abort path. Idempotent;
    /// deinit calls it too. Failing txns release immediately (a deadlock
    /// victim holding its locks until the client's ROLLBACK would keep
    /// the cycle alive); the txn object itself stays for the failed-state
    /// bookkeeping until the block ends.
    pub fn releaseLocks(self: *Txn) void {
        for (self.held.items) |key| self.locks.release(key, self.id);
        self.held.clearRetainingCapacity();
    }

    pub const LockError = error{ Deadlock, Upgrade, OutOfMemory };
    /// lockRow's additions: `Conflict` = a prior read is invalid at the
    /// snapshot the bump needs (40001, the txn aborts); `Restart` = the
    /// bump succeeded and the CURRENT statement must re-run at the new
    /// read_ts (the statement driver retries; never client-visible).
    pub const RowLockError = LockError || error{ Conflict, Restart };

    /// Block until this txn holds the row lock for (table_id, primary) —
    /// FIFO, re-entrant, released at txn end. The lock key is the row's
    /// identity hash (Key.identityHash — ts excluded): one identity per
    /// row, every version, no allocation.
    ///
    /// A FRESH lock also runs the bump check: if the row committed
    /// above our snapshot (oracle floor > read_ts), advance read_ts to
    /// the floor so the statement can see that version — legal only if
    /// everything already read stays valid at the new snapshot:
    ///   - prior statements' scans: unverifiable by hash → Conflict;
    ///   - prior statements' point reads: one batch oracle probe; any
    ///     of them re-committed above the old snapshot → Conflict
    ///     (conservative: a commit even above the new floor aborts);
    ///   - the current statement's reads/scans: discarded and re-run —
    ///     Restart.
    /// A re-entrant lock skips the check: we have held the key since
    /// its first lock, so its floor cannot have moved.
    pub fn lockRow(
        self: *Txn,
        table_id: u32,
        primary: []const u8,
    ) RowLockError!void {
        const h = (Key{
            .namespace = self.ns,
            .table_id = table_id,
            .primary = primary,
        }).identityHash();
        if (!try self.lockKey(h, .exclusive)) return;

        const commit_ts = self.oracle.commitTs(h);
        if (commit_ts <= self.read_ts) return;

        if (self.prev.aboveReadTs(self.oracle, self.read_ts))
            return error.Conflict;
        self.read_ts = commit_ts;
        return error.Restart;
    }

    /// Table intent lock: shared for DML statements (they only need the
    /// table to keep existing), exclusive for DROP TABLE. Re-entrant per
    /// txn like row locks; released at txn end. No bump check: table
    /// keys are never committed to, so their oracle floor is only ever
    /// the horizon — pure noise.
    pub fn lockTable(
        self: *Txn,
        table_id: u32,
        mode: fiber.SemaphoreMap.Mode,
    ) LockError!void {
        _ = try self.lockKey((zdb.kv.TableKey{
            .namespace = self.ns,
            .table_id = table_id,
        }).identityHash(), mode);
    }

    /// Namespace intent lock, one level above `lockTable`: shared for
    /// every DML statement and DROP TABLE, exclusive for DROP DATABASE —
    /// which thus queues behind every open txn and blocks new ones
    /// (writer preference). Acquired BEFORE any table lock (the global
    /// order is ns → table → row). Re-entrant per txn; released at txn
    /// end. No bump check, like lockTable.
    pub fn lockNamespace(
        self: *Txn,
        mode: fiber.SemaphoreMap.Mode,
    ) LockError!void {
        _ = try self.lockKey(zdb.kv.namespaceHash(self.ns), mode);
    }

    /// True = newly acquired (recorded in `held`); false = re-entrant.
    fn lockKey(
        self: *Txn,
        key: u64,
        mode: fiber.SemaphoreMap.Mode,
    ) LockError!bool {
        if (!try self.locks.acquire(key, self.id, mode)) return false;
        self.held.append(self.allocator, key) catch |err| {
            self.locks.release(key, self.id);
            return err;
        };
        return true;
    }

    /// Buffer a put — or a tombstone, via `meta` — of `primary` in
    /// `table_id`. Key and value bytes are copied into the txn's arena.
    pub fn put(
        self: *Txn,
        table_id: u32,
        primary: []const u8,
        val: []const u8,
        meta: Meta,
    ) !void {
        const w = self.writes orelse blk: {
            // Capacity 0: unlike the memtable, a txn's write set is
            // usually tiny — let the arena grow on demand.
            const w = try zdb.skl.SkipList.initCapacity(self.allocator, 0);
            self.writes = w;
            break :blk w;
        };
        try w.putItem(.{
            .key = .{
                .namespace = self.ns,
                .table_id = table_id,
                .primary = primary,
                .timestamp = self.read_ts,
            },
            .value = val,
            .meta = meta,
        });
    }

    /// A found row and how to let it go: committed rows are owned
    /// (release frees them), pending rows borrow from the txn arena
    /// (release is a no-op).
    pub const Found = struct {
        item: Item,
        owned: bool,
        alloc: std.mem.Allocator,

        pub fn release(self: Found) void {
            if (self.owned) self.item.free(self.alloc);
        }
    };

    /// The row `key` names as this txn sees it: the txn's own pending
    /// write first — a pending tombstone means absent with NO forest
    /// fallback (that would resurrect the shadowed version) — else the
    /// forest at key.timestamp. Callers read at read_ts, never latest:
    /// after a lock's bump check, read_ts already covers the newest
    /// committed version of any key this txn writes. Pending entries
    /// sit at-or-below read_ts, so lookups see them. null = absent.
    /// Scans get the same overlay by merging `writes` in as a source.
    ///
    /// Every lookup is recorded in `reads` — absent keys included
    /// (their absence shaped results, and a later commit of one is a
    /// conflict like any other). Reads of keys this txn write-locks are
    /// recorded too: their floor is frozen at-or-below read_ts by our
    /// own lock, so revalidation can never falsely abort on them.
    pub fn lookup(self: *Txn, key: Key) !?Found {
        std.debug.assert(key.namespace == self.ns);
        try self.cur.reads.append(self.allocator, key.identityHash());
        if (self.writes) |w| {
            if (w.get(key)) |item| {
                if (item.meta.isDeleted()) return null;
                return .{
                    .item = item,
                    .owned = false,
                    .alloc = self.allocator,
                };
            }
        }
        const item = (try self.forest.get(self.allocator, key, .p0)) orelse
            return null;
        return .{ .item = item, .owned = true, .alloc = self.allocator };
    }

    /// The pending write set as wire kvs in key order — the commit
    /// payload. Key/value slices point into the txn arena (valid until
    /// deinit); the array itself comes from `a`. A key re-put across a
    /// read_ts bump leaves versions at both stamps in the skiplist;
    /// within one key the iteration order is ts-DESCENDING, so keeping
    /// only the first version per (table_id, primary) emits exactly the
    /// txn's final state.
    pub fn wireKvs(self: *Txn, a: std.mem.Allocator) ![]zen.KeyValueStruct {
        var kvs: std.ArrayList(zen.KeyValueStruct) = .empty;
        const w = self.writes orelse return kvs.toOwnedSlice(a);
        var it = w.iterator();
        defer it.deinit();
        var last: ?Key = null;
        while (it.itemRef()) |item| : (try it.next()) {
            if (last) |lk| {
                if (item.key.orderWithoutTs(lk) == .eq) continue;
            }
            last = item.key;
            try kvs.append(a, .{
                .key = try item.key.marshal(a),
                .value = item.value,
                .meta = item.meta.toByte(),
            });
        }
        return kvs.toOwnedSlice(a);
    }
};
