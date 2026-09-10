const std = @import("std");
const log = std.log.scoped(.forest);
const fiber = @import("zfiber");
const utils = @import("utils");
const _config = @import("config.zig");
const _manifest = @import("manifest.zig");
const _read = @import("read.zig");
const _table = @import("table.zig");
const vfs = @import("vfs");
const _tree = @import("tree.zig");
const _skl = @import("skl/skiplist.zig");
const _metrics = @import("metrics.zig");
const _fi = @import("file_ids.zig");
const zroar = @import("zroar");

const Key = @import("kv.zig").Key;
const TreeKey = @import("kv.zig").TreeKey;
const TableKey = @import("kv.zig").TableKey;
const makeTreeId = @import("kv.zig").makeTreeId;
const treeTableId = @import("kv.zig").treeTableId;
const treeSplitId = @import("kv.zig").treeSplitId;
const Tree = _tree.Tree;
const TableCache = _tree.TableCache;
const Table = _table.Table;
const Item = _table.table.Item;
const SkipList = _skl.SkipList;
const Builder = _table.builder.Builder;
const merge_iterator = _table.merge_iterator;

// ---------------------------------------------------------------------------
// TableTrees
// ---------------------------------------------------------------------------

/// TableTrees holds the LSM trees of a single `{namespace, table_id}` table
/// and maps a key to the tree that owns it. A table is one tree until it
/// grows past `tree_split_size`, then splits at the primary-key median into
/// more trees; entries are kept sorted by range.left so we can binary-search.
pub const TableTrees = struct {
    pub const Lock = fiber.RWMutex(.untracked);

    allocator: std.mem.Allocator,
    /// Guards `entries`: shared to find a tree or walk the list, exclusive
    /// to add or remove one (`addTree` can reallocate the list under a
    /// walker). A tree found here is handed out with a ref taken while
    /// this is still held — the window in which a split or drop could
    /// otherwise retire it between the find and the take — and the caller
    /// releases the ref when done. Writers never wait for readers: they
    /// swap the list and drop their own ref, and the last holder closes
    /// the tree.
    mu: Lock,
    /// The table's trees, sorted by key_range.left for binary search. A tree's
    /// id and range live on the Tree itself (tree.tree_id / tree.key_range), so
    /// TableTrees owns the *Tree directly — no separate entry record.
    entries: std.ArrayList(*Tree),

    pub fn init(allocator: std.mem.Allocator) TableTrees {
        return .{
            .allocator = allocator,
            .mu = .{},
            .entries = .empty,
        };
    }

    pub fn deinit(self: *TableTrees) void {
        self.entries.deinit(self.allocator);
    }

    fn lessByLeft(_: void, a: *Tree, b: *Tree) bool {
        return a.key_range.left.order(b.key_range.left) == .lt;
    }

    /// Add a tree and keep entries sorted by key_range.left. Takes over the
    /// caller's ref on it (the one `Tree.open` starts with).
    pub fn addTree(self: *TableTrees, held: Lock.Exclusive, tree: *Tree) !void {
        self.mu.assertHeld(held);
        try self.entries.append(self.allocator, tree);
        std.mem.sort(*Tree, self.entries.items, {}, lessByLeft);
    }

    /// Remove the tree with `tree_id`, returning it (or null if absent) —
    /// with the list's ref, which the caller releases once outside `mu`.
    pub fn removeTree(
        self: *TableTrees,
        held: Lock.Exclusive,
        tree_id: u64,
    ) ?*Tree {
        self.mu.assertHeld(held);
        var i: usize = self.entries.items.len;
        while (i > 0) {
            i -= 1;
            if (self.entries.items[i].tree_id == tree_id) {
                return self.entries.orderedRemove(i);
            }
        }
        return null;
    }

    /// The tree with `tree_id`, or null. One table's trees (one until it
    /// splits), so effectively O(1).
    pub fn byId(
        self: *const TableTrees,
        held: Lock.Shared,
        tree_id: u64,
    ) ?*Tree {
        self.mu.assertHeld(held);
        for (self.entries.items) |tree| {
            if (tree.tree_id == tree_id) return tree;
        }
        return null;
    }

    /// True iff the table still has trees. A dropped table's TableTrees
    /// stays in the forest's map, empty — table ids are never reused, and a
    /// reader may still hold the pointer — so this, not map presence, is
    /// what "the table exists" means.
    pub fn hasTrees(self: *TableTrees) bool {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        return self.entries.items.len > 0;
    }

    /// The tree owning `key`, with a ref taken for the caller — release it
    /// with `tree.release()` once done reading through the tree. Null if
    /// no tree owns the key.
    pub fn getTreeByKey(self: *TableTrees, key: Key) ?*Tree {
        const held = self.mu.hold(.shared, @src());
        defer held.release();
        return retain(self.lookup(held, key));
    }

    /// `getTree` by tree id.
    pub fn getTreeById(self: *TableTrees, tree_id: u64) ?*Tree {
        const held = self.mu.hold(.shared, @src());
        defer held.release();
        return retain(self.byId(held, tree_id));
    }

    /// The list itself, borrowed for as long as `held` is.
    pub fn list(self: *const TableTrees, held: Lock.Shared) []const *Tree {
        self.mu.assertHeld(held);
        return self.entries.items;
    }

    /// `getTree` for the rightmost tree — where an id-monotonic table's
    /// newest keys land. Null when the table has no trees.
    pub fn getRightmostTree(self: *TableTrees) ?*Tree {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        if (self.entries.items.len == 0) return null;
        return retain(self.entries.items[self.entries.items.len - 1]);
    }

    /// The ref is taken while `mu` is still held: there is no instant at
    /// which a caller has found a tree but doesn't hold it.
    fn retain(found: ?*Tree) ?*Tree {
        const tree = found orelse return null;
        tree.acquire();
        return tree;
    }

    /// Find the tree that owns this key, or null if none.
    pub fn lookup(
        self: *const TableTrees,
        held: Lock.Shared,
        search_key: Key,
    ) ?*Tree {
        self.mu.assertHeld(held);
        if (self.entries.items.len == 1) return self.entries.items[0];
        if (self.entries.items.len == 0) return null;

        var lo: usize = 0;
        var hi: usize = self.entries.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.entries.items[mid].key_range.left.order(
                search_key,
            ) != .gt) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo == 0) return null;
        const candidate = self.entries.items[lo - 1];
        if (candidate.key_range.inf) return candidate;
        if (candidate.key_range.right.primary.len > 0 and
            search_key.order(candidate.key_range.right) == .gt)
        {
            return null;
        }
        return candidate;
    }
};

// ---------------------------------------------------------------------------

pub const Memtables = struct {
    allocator: std.mem.Allocator,
    mu: fiber.RWMutex(.untracked),
    current: *SkipList,
    /// Newest at index 0, oldest last — the order every reader wants
    /// (getIterators hands sources out newest-first as-is), and the
    /// flush path pops the oldest off the END for free.
    immutables: std.ArrayList(*SkipList),

    pub fn init(allocator: std.mem.Allocator, current: *SkipList) Memtables {
        return .{
            .allocator = allocator,
            .mu = .{},
            .current = current,
            .immutables = .empty,
        };
    }

    pub fn deinit(self: *Memtables) void {
        self.current.release();
        for (self.immutables.items) |imm| imm.release();
        self.immutables.deinit(self.allocator);
    }

    pub fn putItem(self: *Memtables, item: Item) !void {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        try self.current.putItem(item);
    }

    pub fn memoryUsage(self: *Memtables) usize {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        return self.current.memoryUsage();
    }

    /// Get iterators for a scan merge: `pending` (a txn's private
    /// write skiplist, when non-null) FIRST — src 0, the merge's
    /// equal-key tie-break, so the txn sees its own writes — then the
    /// active memtable, then the immutables newest-first (their
    /// storage order). Each `skl.Iterator.init`
    /// acquires the underlying skiplist — release happens via `deinit`
    /// on the returned iterator (transitively from
    /// `MergeIterator.deinit`). Doing the acquires under our shared
    /// lock guarantees that even if a concurrent flush +
    /// `popOldestIfMatches` releases the original ref a moment later,
    /// the skiplist stays alive until the iterator drops its ref.
    /// Caller frees the slice itself.
    pub fn getIterators(
        self: *Memtables,
        allocator: std.mem.Allocator,
        pending: ?*_skl.SkipList,
    ) ![]merge_iterator.Iterator {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        const extra: usize = @intFromBool(pending != null);
        const total = extra + 1 + self.immutables.items.len;
        const out = try allocator.alloc(merge_iterator.Iterator, total);
        if (pending) |p| out[0] = .{ .skiplist = _skl.Iterator.init(p) };
        out[extra] = .{ .skiplist = _skl.Iterator.init(self.current) };
        for (self.immutables.items, 0..) |imm, i| {
            out[extra + 1 + i] = .{ .skiplist = _skl.Iterator.init(imm) };
        }
        return out;
    }

    /// id of the current memtable. 0 means the initial placeholder is still
    /// in place — no `newMemtable` has been applied yet, and any writes that
    /// land in `current` will be discarded by the first `swapCurrentOut`.
    pub fn currentId(self: *Memtables) u64 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        return self.current.id;
    }

    /// Aggregate sizes for the active + immutable memtables, taken under
    /// one lock. Used by the stats dump so we don't have to chain three
    /// separate lookups (each of which would re-acquire the lock).
    pub const Stats = struct {
        active_bytes: usize,
        immutable_bytes: usize,
        immutable_count: usize,
    };

    pub fn stats(self: *Memtables) Stats {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        var imm_bytes: usize = 0;
        for (self.immutables.items) |imm| imm_bytes += imm.memoryUsage();
        return .{
            .active_bytes = self.current.memoryUsage(),
            .immutable_bytes = imm_bytes,
            .immutable_count = self.immutables.items.len,
        };
    }

    /// Look up a key across current and immutable memtables.
    /// Returns a duped Item owned by the caller.
    pub fn get(
        self: *Memtables,
        allocator: std.mem.Allocator,
        search_key: Key,
    ) !?Item {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();

        if (self.current.get(search_key)) |item| {
            return try item.dupe(allocator);
        }
        // Immutables are stored newest-first.
        for (self.immutables.items) |imm| {
            if (imm.get(search_key)) |item| {
                return try item.dupe(allocator);
            }
        }
        return null;
    }

    /// Freeze the current memtable: move it to immutables, create a new
    /// current. `log_index` is the apply index of the Raft `newMemtable` entry;
    /// it pins the snapshot horizon (`snapshot.index <=
    /// oldest.created_at_log_index - 1`).
    ///
    /// Special case: the id=0 placeholder (created by `Forest.open` before any
    /// real `newMemtable` has been applied) is *discarded*, not frozen — it
    /// would otherwise sit in immutables forever, since no flush ever targets
    /// id=0.
    pub fn swapCurrentOut(self: *Memtables, new_id: u64, log_index: u64) !void {
        self.mu.lock(@src());
        defer self.mu.unlock();

        if (self.current.id == 0) {
            self.current.release();
        } else {
            // Newest-first: the just-frozen memtable goes in front.
            try self.immutables.insert(self.allocator, 0, self.current);
        }
        const new_mt = try SkipList.init(self.allocator);
        new_mt.id = new_id;
        new_mt.created_at_log_index = log_index;
        self.current = new_mt;
    }

    /// Log index of the oldest in-memory memtable. Returns 0 when the only
    /// thing around is the initial placeholder (no real `newMemtable` entry
    /// has been applied yet), which the caller treats as "no snapshot yet."
    pub fn oldestInMemoryLogIndex(self: *Memtables) u64 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        const imms = self.immutables.items;
        if (imms.len > 0) return imms[imms.len - 1].created_at_log_index;
        return self.current.created_at_log_index;
    }

    /// Drop any phantom immutables whose id is strictly less than
    /// `expected_id`, then pop+release the oldest IFF its id equals
    /// `expected_id`. No-op when the matched memtable isn't found.
    ///
    /// Used by `applyFlush`. Three cases reflect leader/follower divergence
    /// during recovery replay:
    ///   - oldest.id < expected_id  → phantom: flush already happened
    ///     pre-snap on leader, SST is in snap.data, but we still have the
    ///     local memtable (e.g., its `newMemtable` was post-snap and
    ///     replayed, but its `flush` was pre-snap and compacted). Release
    ///     and continue checking the next oldest.
    ///   - oldest.id == expected_id → normal: pop and release.
    ///   - oldest.id > expected_id  → flush's target memtable was popped on
    ///     the leader pre-snap and never re-created here (its `newMemtable`
    ///     proposal was below snap.index). Nothing local to remove; the SST
    ///     install above handles the data side.
    /// Steady-state: FIFO ordering means oldest always equals expected.
    pub fn popOldestIfMatches(self: *Memtables, expected_id: u64) void {
        self.mu.lock(@src());
        defer self.mu.unlock();

        // Newest-first storage: the oldest sits at the END, so both
        // the phantom sweep and the pop are cheap tail removals.
        while (self.immutables.items.len > 0 and
            self.immutables.items[self.immutables.items.len - 1].id <
                expected_id)
        {
            const phantom = self.immutables.pop().?;
            log.info(
                "popOldestIfMatches: releasing phantom immutable id={d} " ++
                    "(expected_id={d})",
                .{ phantom.id, expected_id },
            );
            phantom.release();
        }

        if (self.immutables.items.len == 0) {
            log.info(
                "popOldestIfMatches: skipping pop, immutables empty " ++
                    "(expected_id={d})",
                .{expected_id},
            );
            return;
        }
        const oldest_id =
            self.immutables.items[self.immutables.items.len - 1].id;
        if (oldest_id != expected_id) {
            log.info(
                "popOldestIfMatches: skipping pop, oldest.id={d} > " ++
                    "expected_id={d} (recovery: pre-snap flush, memtable " ++
                    "not re-created locally)",
                .{ oldest_id, expected_id },
            );
            return;
        }
        const imm = self.immutables.pop().?;
        imm.release();
    }

    /// The oldest immutable memtable, with a ref for the caller (release
    /// when done). The ref is taken under the lock, as `getIterators`
    /// does: the apply loop's `popOldestIfMatches` may drop the list's ref
    /// — the phantom sweep drops exactly the oldest ones — at any moment
    /// after this returns. Null when there is none.
    pub fn peekOldest(self: *Memtables) ?*SkipList {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();

        if (self.immutables.items.len == 0) return null;
        const oldest = self.immutables.items[self.immutables.items.len - 1];
        oldest.acquire();
        return oldest;
    }
};

// ---------------------------------------------------------------------------
// Forest
// ---------------------------------------------------------------------------

pub const Forest = struct {
    allocator: std.mem.Allocator,
    opts: _config.Config,
    ms: _manifest.ForestState,
    /// Per-node file_id allocator. Wired at startup via `bindWorker`.
    /// All callers go through `file_ids.allocate()`; raw `fetchAdd` is
    /// never correct because it skips the HardState lease.
    file_ids: _fi.FileIds,
    /// One TableTrees per table, keyed by its `{namespace, table_id}` identity.
    /// The TableTreess own the live `*Tree` objects (in their sorted entries);
    /// there is no separate tree map. Look a tree up by id via `getTree`.
    /// The map guards itself, and a TableTrees once inserted is never freed
    /// (a dropped table's stays, empty — see `TableTrees.hasTrees`), so
    /// `getTable` needs no lock and its result never dangles.
    tables: utils.SafeHashMap(TableKey, *TableTrees),
    table_cache: TableCache,
    memtables: Memtables,
    /// The oldest timestamp any reader is still allowed to ask for. Reads
    /// below it are rejected (`error.ReadBelowMinRead`), which is exactly what
    /// makes data below it unobservable and therefore safe to discard: bottom-
    /// level compaction drops a tombstone older than this along with every
    /// version it shadows (see `Tree.buildCompaction`).
    ///
    /// ZDB does not derive this itself — it has no reader registry. The
    /// embedder (egres) owns the value and pushes it in via
    /// `setGlobalMinRead`. It only ever moves forward.
    ///
    /// 0 means "nothing is droppable", the safe default: no timestamp is
    /// below it, so no read is rejected and no row is ever dropped.
    global_min_read: std.atomic.Value(u64),

    // Read paths, from read.zig.
    pub const get = _read.get;
    pub const getRaw = _read.getRaw;
    pub const getBitmap = _read.getBitmap;
    pub const ScanOptions = _read.ScanOptions;
    pub const scan = _read.scan;
    pub const openCursor = _read.openCursor;
    pub const Cursor = _read.Cursor;
    pub const maxKey = _read.maxKey;
    pub const StatsScope = _read.StatsScope;
    pub const StatsOpts = _read.StatsOpts;
    pub const writeStats = _read.writeStats;
    pub const writeForestStats = _read.writeForestStats;
    pub const writeTreeStats = _read.writeTreeStats;
    pub const writeNamespaceStats = _read.writeNamespaceStats;
    pub const writeBytes = _read.writeBytes;

    // Building and applying manifest changes, from manifest.zig.
    pub const installTree = _manifest.installTree;
    pub const collectMissingFileIds = _manifest.collectMissingFileIds;
    pub const openTablesFromManifest = _manifest.openTablesFromManifest;
    pub const createTree = _manifest.createTree;
    pub const buildNamespaceCreate = _manifest.buildNamespaceCreate;
    pub const CreateNamespaceResult = _manifest.CreateNamespaceResult;
    pub const buildCreateNamespace = _manifest.buildCreateNamespace;
    pub const lookupNamespace = _manifest.lookupNamespace;
    pub const lookupTable = _manifest.lookupTable;
    pub const applyNamespaceCreate = _manifest.applyNamespaceCreate;
    pub const buildCreateTree = _manifest.buildCreateTree;
    pub const CreateTablesResult = _manifest.CreateTablesResult;
    pub const buildCreateTables = _manifest.buildCreateTables;
    pub const applyCreateTree = _manifest.applyCreateTree;
    pub const buildDropTables = _manifest.buildDropTables;
    pub const applyDropTree = _manifest.applyDropTree;
    pub const applyTreeDeletes = _manifest.applyTreeDeletes;
    pub const buildDropNamespace = _manifest.buildDropNamespace;
    pub const applyDropNamespace = _manifest.applyDropNamespace;
    pub const buildSplit = _manifest.buildSplit;
    pub const applySplit = _manifest.applySplit;
    pub const splitTreeForTests = _manifest.splitTreeForTests;

    /// Open a forest with empty in-memory state. Persistent state is restored
    /// later via `installManifest` (called by recovery if a Raft snapshot is
    /// present); fresh starts go straight to `createTree` / Raft replay.
    pub fn open(allocator: std.mem.Allocator, opts: _config.Config) !*Forest {
        var ms = _manifest.ForestState.init(allocator);
        errdefer ms.deinit();

        const self = try allocator.create(Forest);
        errdefer allocator.destroy(self);

        // Initial placeholder memtable. id and created_at_log_index both 0
        // are the sentinel values that mark "no real memtable yet" — the
        // first replayed `newMemtable` swaps it out (Phase 5).
        const mt = try SkipList.init(allocator);
        errdefer mt.release();

        self.* = .{
            .allocator = allocator,
            .opts = opts,
            .ms = ms,
            .file_ids = .{},
            .tables = utils.SafeHashMap(TableKey, *TableTrees).init(allocator),
            .table_cache = TableCache.init(allocator, opts.dir),
            .memtables = Memtables.init(allocator, mt),
            .global_min_read = std.atomic.Value(u64).init(0),
        };
        return self;
    }

    /// Raise the read horizon. Monotonic: a lower value is ignored rather
    /// than rejected, so a caller racing with a newer update can't walk the
    /// horizon backwards and un-discard data that a compaction already
    /// dropped. Returns the value in force afterward.
    pub fn setGlobalMinRead(self: *Forest, ts: u64) u64 {
        var cur = self.global_min_read.load(.acquire);
        while (ts > cur) {
            const prev = self.global_min_read.cmpxchgWeak(
                cur,
                ts,
                .acq_rel,
                .acquire,
            ) orelse return ts;
            cur = prev;
        }
        return cur;
    }

    pub fn globalMinRead(self: *Forest) u64 {
        return self.global_min_read.load(.acquire);
    }

    pub fn close(self: *Forest) void {
        self.memtables.deinit();

        // TableTreess own the trees: drop each list's ref (the last one at
        // shutdown, so the tree closes here), then the table.
        var tables_it = self.tables.valueIterator();
        while (tables_it.next()) |trees| {
            for (trees.*.entries.items) |tree| tree.release();
            trees.*.deinit();
            self.allocator.destroy(trees.*);
        }
        self.tables.deinit();

        self.ms.deinit();
        self.file_ids.mu.deinit();
        self.table_cache.deinit();
        self.allocator.destroy(self);
    }

    /// Put a batch of key-value pairs into the current memtable.
    /// Callers can use shouldFreeze() afterward to decide whether to propose
    /// a memtable freeze through Raft.
    pub fn put(self: *Forest, items: []const Item) !void {
        for (items) |item| {
            try self.memtables.putItem(item);
        }
    }

    /// Create a new memtable, freezing the current one.
    /// Called when the newMemtable entry is committed via Raft.
    /// The new_id is assigned by the leader and included in the proposal so
    /// all nodes use the same ID. `log_index` is the apply index of that
    /// Raft entry — pins the snapshot horizon for the new memtable.
    pub fn newMemtable(self: *Forest, new_id: u64, log_index: u64) !void {
        try self.memtables.swapCurrentOut(new_id, log_index);
    }

    /// buildFlush is Phase A of the memtable flush: iterates the oldest frozen
    /// memtable, routes each key to its tree, builds one L0 SST per tree, and
    /// returns the manifest changes. The caller proposes these through Raft.
    pub const FlushResult = struct {
        changes: std.ArrayList(_manifest.SSTableChange),
        memtable_id: u64,
    };

    pub fn buildFlush(self: *Forest) !?FlushResult {
        const mt = self.memtables.peekOldest() orelse return null;
        defer mt.release();
        var iter = mt.iterator();
        defer iter.deinit();

        // Per-tree builders, keyed by the tree's global identity. Keys within
        // each tree are already in sorted order because the skiplist is
        // globally sorted and each tree covers a contiguous key range within
        // its (namespace, table_id) table.
        var builders = std.AutoHashMap(TreeKey, *Builder).init(self.allocator);
        defer {
            var bit = builders.valueIterator();
            while (bit.next()) |bld| {
                bld.*.deinit();
                self.allocator.destroy(bld.*);
            }
            builders.deinit();
        }

        const cfg = _table.builder.Config{
            .target_block_size = self.opts.block_size,
            .codec = self.opts.compression,
            .priority = .p0,
        };

        // Route each key to a tree and add to the appropriate builder.
        while (iter.itemRef()) |item| {
            const tk = blk: {
                const trees = self.getTable(
                    item.key.namespace,
                    item.key.table_id,
                ) orelse {
                    try iter.next();
                    continue;
                };
                const tree = trees.getTreeByKey(item.key) orelse {
                    try iter.next();
                    continue;
                };
                defer tree.release();
                break :blk TreeKey{
                    .namespace = item.key.namespace,
                    .tree_id = tree.tree_id,
                };
            };

            const bld = if (builders.get(tk)) |b| b else blk: {
                const b = try self.allocator.create(Builder);
                b.* = Builder.init(self.allocator, cfg);
                try builders.put(tk, b);
                break :blk b;
            };
            try bld.addItem(item);
            try iter.next();
        }

        // Flush all builders to SST files. writeAndSyncTable handles the full
        // write path (finish → create file → write → sync → close) and returns
        // the manifest SSTableChange directly.
        var flush_err: utils.SafeError = .{};
        var g = fiber.Group(?_manifest.SSTableChange).init(self.allocator);
        defer {
            _ = g.wait();
            g.deinit();
        }

        var bit = builders.iterator();
        while (bit.next()) |kv| {
            const tk = kv.key_ptr.*;
            const bld = kv.value_ptr.*;

            var tb = try bld.finish();
            if (tb.bytes.len == 0) {
                tb.deinit();
                continue;
            }

            const file_id = try self.file_ids.allocate();
            const path = try _fi.sstPath(
                self.allocator,
                self.opts.dir,
                file_id,
            );

            try g.spawn(.{}, writeAndSyncTable, .{
                self.allocator, tb, path, file_id, tk, 0, &flush_err,
            });
        }

        // Join fibers and collect manifest changes.
        const tcs = g.wait();
        var changes: std.ArrayList(_manifest.SSTableChange) = .empty;
        for (tcs) |tc_opt| {
            const tc = tc_opt orelse continue;
            try changes.append(self.allocator, tc);
        }
        if (flush_err.get()) |err| return err;

        // Best-effort directory sync.
        if (self.opts.dir.len > 0) {
            var dir_sync = try fiber.spawn(.{}, vfs.syncDir, .{self.opts.dir});
            defer dir_sync.join();
        }

        return .{ .changes = changes, .memtable_id = mt.id };
    }

    /// Writes table bytes to a new SST file, syncs, and closes.
    /// Returns a manifest.SSTableChange (CREATE at `level`) on success.
    fn writeAndSyncTable(
        allocator: std.mem.Allocator,
        tb: _table.builder.TableBuffer,
        path: []const u8,
        file_id: u64,
        tk: TreeKey,
        lvl: u32,
        err_out: *utils.SafeError,
    ) ?_manifest.SSTableChange {
        defer allocator.free(path);
        var mtb = tb;
        defer mtb.deinit();

        vfs.writeAndSync(
            path,
            mtb.bytes,
            .{ .mode = .direct },
            .p0,
        ) catch |err| {
            err_out.set(err);
            return null;
        };
        return _manifest.createSSTableChange(
            file_id,
            tk.namespace,
            tk.tree_id,
            lvl,
        );
    }

    /// Build BOTTOM-level SSTs for (`ns`, `table_id`) from
    /// caller-supplied items — sorted ascending, one version per key —
    /// write + sync them, and return the manifest changes to propose
    /// (CREATEs, cmd .index_backfill). CREATE INDEX's online backfill
    /// pours its per-key roaring states through here: those states
    /// are by construction the OLDEST data the tree will ever hold
    /// (every posting delta commits after the backfill snapshot), so
    /// the bottom level is the one placement that keeps the
    /// level-ordering invariant against everything above.
    /// Tree.applyBackfill enforces the matching sorted-run condition
    /// (the bottom level must still be empty at install).
    ///
    /// Output splits at the bottom level's target size, so a huge
    /// backfill lands as several legit tables rather than one giant
    /// one; items are single-version keys, so any split point is
    /// safe, and the files are disjoint + sorted — a valid sorted
    /// run. Writes+syncs run in a fiber group, one per output file,
    /// like buildFlush; files reach followers exactly like
    /// flush/compaction outputs (uploaded to the shared file store
    /// before propose; apply downloads what's missing). The caller
    /// guarantees the table is un-split —
    /// one tree — which holds for a freshly created index table
    /// under ddl_mu (splits never touch an empty tree). `changes` is
    /// allocated from `allocator` and caller-owned.
    pub fn buildBottomTable(
        self: *Forest,
        allocator: std.mem.Allocator,
        ns: u64,
        table_id: u32,
        items: []const Item,
    ) !std.ArrayList(_manifest.SSTableChange) {
        var changes: std.ArrayList(_manifest.SSTableChange) = .empty;
        errdefer changes.deinit(allocator);
        if (items.len == 0) return changes;

        const bottom: u32 = @intCast(self.opts.max_levels - 1);
        const tk, const max_bytes = blk: {
            const tree = try self.singleTree(ns, table_id);
            defer tree.release();
            var targets = try tree.levelTargets();
            defer targets.deinit();
            break :blk .{
                TreeKey{ .namespace = ns, .tree_id = tree.tree_id },
                @as(usize, @intCast(targets.file_sz[bottom])),
            };
        };

        var build_err: utils.SafeError = .{};
        var g = fiber.Group(?_manifest.SSTableChange).init(self.allocator);
        defer {
            _ = g.wait();
            g.deinit();
        }

        var bld = Builder.init(self.allocator, .{
            .target_block_size = self.opts.block_size,
            .codec = self.opts.compression,
            .priority = .p2,
        });
        defer bld.deinit();
        for (items) |item| {
            if (bld.table_bytes.items.len >= max_bytes) {
                try self.spawnTableWrite(&g, &bld, tk, bottom, &build_err);
                bld.reset();
            }
            try bld.addItem(item);
        }
        try self.spawnTableWrite(&g, &bld, tk, bottom, &build_err);

        const tcs = g.wait();
        for (tcs) |tc_opt| {
            const tc = tc_opt orelse continue;
            try changes.append(allocator, tc);
        }
        if (build_err.get()) |err| return err;

        if (self.opts.dir.len > 0) {
            var dir_sync = try fiber.spawn(.{}, vfs.syncDir, .{self.opts.dir});
            defer dir_sync.join();
        }
        return changes;
    }

    /// One buildBottomTable output file: finish the builder and hand
    /// the write+sync to the fiber group. Empty builders spawn
    /// nothing.
    fn spawnTableWrite(
        self: *Forest,
        g: *fiber.Group(?_manifest.SSTableChange),
        bld: *Builder,
        tk: TreeKey,
        lvl: u32,
        err_out: *utils.SafeError,
    ) !void {
        var tb = try bld.finish();
        if (tb.bytes.len == 0) {
            tb.deinit();
            return;
        }
        const file_id = try self.file_ids.allocate();
        const path = try _fi.sstPath(self.allocator, self.opts.dir, file_id);
        try g.spawn(.{}, writeAndSyncTable, .{
            self.allocator, tb, path, file_id, tk, lvl, err_out,
        });
    }

    /// Whether every CREATE in `changes` is now a table of `table_id`'s
    /// bottom level — CREATE INDEX's post-install verification.
    /// applyBackfill skips, deterministically on every node, when the
    /// bottom level was not empty; a table count cannot tell a skip
    /// from an install (either way the level is non-empty after), so
    /// the proposed sst ids are looked for by name.
    pub fn bottomLevelHas(
        self: *Forest,
        ns: u64,
        table_id: u32,
        changes: []const _manifest.SSTableChange,
    ) !bool {
        // The proposed ids as a set (a few files), built before the
        // tree lock; then one pass over the level counting the ones it
        // holds. Ids are unique on both sides, so a full count means
        // every file is present.
        var wanted = try zroar.Bitmap.init(self.allocator);
        defer wanted.deinit();
        for (changes) |c| _ = try wanted.set(c.sst_id);

        const tree = try self.singleTree(ns, table_id);
        defer tree.release();
        const held = tree.mu.hold(.shared, @src());
        defer held.release();
        var found: usize = 0;
        for (tree.levels[tree.levels.len - 1].entries(held)) |e| {
            if (wanted.contains(e.id)) found += 1;
        }
        return found == changes.len;
    }

    /// The ONE tree of an un-split table — backfill's addressing mode:
    /// a freshly created index table cannot have split (splits never
    /// touch an empty tree, and ddl_mu serializes DDL against drops).
    /// Returned with a ref; the caller releases it.
    fn singleTree(self: *Forest, ns: u64, table_id: u32) !*Tree {
        const trees = self.getTable(ns, table_id) orelse
            return error.TreeNotFound;
        const held = trees.mu.hold(.shared, @src());
        defer held.release();
        const listed = trees.list(held);
        std.debug.assert(listed.len == 1);
        return TableTrees.retain(listed[0]).?;
    }

    /// applyFlush is Phase B: commits the manifest changes from a memtable
    /// flush and updates in-memory levels. Called on all nodes after Raft
    /// commit. Also removes the flushed memtable from the immutable list.
    pub fn applyFlush(
        self: *Forest,
        changes: []const _manifest.SSTableChange,
        memtable_id: u64,
    ) !void {
        if (changes.len > 0) {
            try self.ms.commit(changes, &.{});
        }

        // Load all new L0 tables concurrently via the cache. writeFile may
        // have pre-opened these (follower path) → cache hit. On the leader,
        // flush writes bytes directly, so these will typically be misses.
        var needed: std.ArrayList(_tree.TableNeed) = .empty;
        defer needed.deinit(self.allocator);
        for (changes) |c| {
            std.debug.assert(c.op == .create); // flush only creates
            try needed.append(self.allocator, .{
                .id = c.sst_id,
                .level = c.level,
            });
        }
        try self.table_cache.loadTables(needed.items);

        // Install each loaded table into its tree's level. We zip changes and
        // needed in the same order they were built, so indices align.
        for (changes, needed.items) |c, n| {
            std.debug.assert(c.sst_id == n.id);
            const tree = self.getTree(
                c.namespace_id,
                c.tree_id,
            ) orelse continue;
            defer tree.release();
            {
                const held = tree.mu.hold(.exclusive, @src());
                defer held.release();
                try tree.levels[c.level].addEntry(held, n.id, n.table.?);
            }

            // Metrics: one per-tree flush output + bytes written to L0.
            try _metrics.g().flushes.incr(.{
                .namespace_id = c.namespace_id,
                .tree_id = c.tree_id,
            });
            try _metrics.g().bytes_written.incrBy(.{
                .source = "flush",
                .namespace_id = c.namespace_id,
                .tree_id = c.tree_id,
                .level = @intCast(c.level),
            }, n.table.?.fileSize());
        }

        // Remove the flushed memtable from local immutables IF it's there.
        // During recovery replay, the memtable's `newMemtable` proposal may
        // have been below snap.index and compacted; in that case immutables
        // either is empty or contains a *later* memtable. The SST install
        // above handles the data side; nothing local to remove.
        self.memtables.popOldestIfMatches(memtable_id);

        // Update compaction candidates for affected trees.
        var seen_trees = std.AutoHashMap(TreeKey, void).init(self.allocator);
        defer seen_trees.deinit();
        for (changes) |c| {
            const tk = TreeKey{
                .namespace = c.namespace_id,
                .tree_id = c.tree_id,
            };
            if (!seen_trees.contains(tk)) {
                try seen_trees.put(tk, {});
                if (self.getTree(tk.namespace, tk.tree_id)) |tree| {
                    defer tree.release();
                    try tree.updateCompactCandidate();
                }
            }
        }
    }

    /// Compact finds the tree with the highest compaction score and runs one
    /// compaction on it. Returns true if a compaction was executed, false if
    /// no tree needs compaction.
    /// An identity, not an object: which tree, and what its candidate said.
    /// The proposer resolves the tree by id (`getTree`) when it builds —
    /// by then a split or drop may have retired the object the score came
    /// from, in which case there is nothing to build and the candidate was
    /// stale anyway. A key can also travel to a peer; a pointer cannot.
    pub const CompactionTarget = struct {
        key: TreeKey,
        candidate: _tree.CompactCandidate,
    };

    /// Find the best tree/level to compact without performing compaction.
    pub fn bestCompactionCandidate(self: *Forest) ?CompactionTarget {
        return self.bestCompactionCandidateExcluding(null);
    }

    /// Same as `bestCompactionCandidate` but skips any tree whose key is
    /// in `skip` (used by parallel compactors so two fibers don't pick
    /// the same tree). `skip == null` means "no exclusions".
    pub fn bestCompactionCandidateExcluding(
        self: *Forest,
        skip: ?*const std.AutoHashMap(TreeKey, void),
    ) ?CompactionTarget {
        var best: ?CompactionTarget = null;
        var tables_it = self.tables.iterator();
        defer tables_it.deinit();
        while (tables_it.next()) |kv| {
            const trees = kv.value_ptr.*;
            const held = trees.mu.hold(.shared, @src());
            defer held.release();
            for (trees.list(held)) |tree| {
                const tk = TreeKey{
                    .namespace = tree.namespace,
                    .tree_id = tree.tree_id,
                };
                if (skip) |s| if (s.contains(tk)) continue;
                const th = tree.mu.hold(.shared, @src());
                defer th.release();
                const candidate = tree.compact_candidate orelse continue;
                if (best == null or candidate.score > best.?.candidate.score) {
                    best = .{ .key = tk, .candidate = candidate };
                }
            }
        }
        return best;
    }

    /// Check if the current memtable should be frozen.
    /// Largest L0 table count among the trees that own data for this
    /// namespace. Used by the write-path backpressure in the engine so a
    /// hot namespace's bloated L0 throttles only its own writers, not
    /// every namespace on the node. After a tree split, a namespace can
    /// own multiple trees; we take the worst of them. Returns 0 if the
    /// namespace doesn't exist (caller already rejected the write).
    /// TODO: We should ideally do this based on the tree the keys are going to
    /// hit, instead of any tree in the namespace — which would also drop the
    /// scan below (the flat tables map has no per-namespace index).
    pub fn maxL0TableCountForNs(self: *Forest, ns: u64) u32 {
        var max_count: u32 = 0;
        var it = self.tables.iterator();
        defer it.deinit();
        while (it.next()) |kv| {
            if (kv.key_ptr.namespace != ns) continue;
            const trees = kv.value_ptr.*;
            const held = trees.mu.hold(.shared, @src());
            defer held.release();
            for (trees.list(held)) |tree| {
                const n: u32 = @intCast(tree.numTablesAt(0));
                if (n > max_count) max_count = n;
            }
        }
        return max_count;
    }

    pub fn shouldFreeze(self: *Forest) bool {
        return self.memtables.memoryUsage() >= self.opts.memtable_size;
    }

    /// Check if there are frozen memtables waiting to be flushed.
    pub fn hasFrozenMemtables(self: *Forest) bool {
        const oldest = self.memtables.peekOldest() orelse return false;
        oldest.release();
        return true;
    }

    /// Check if an SST file exists on disk.
    pub fn hasFile(self: *Forest, file_id: u64) bool {
        const path = _fi.sstPath(
            self.allocator,
            self.opts.dir,
            file_id,
        ) catch return false;
        defer self.allocator.free(path);
        vfs.access(path) catch return false;
        return true;
    }

    /// Write an SST file to disk and pre-open it into the table cache.
    /// Skips the write when the file is already cached, or already on disk with
    /// a matching size — file_id → content is deterministic in this system, and
    /// a size match is a strong signal the bytes are correct (no cross-ID
    /// collisions possible). If a size mismatch is detected, we log and
    /// rewrite.
    pub fn writeFile(self: *Forest, file_id: u64, data: []const u8) void {
        const fe = std.log.scoped(.file_event);
        if (self.table_cache.contains(file_id)) {
            fe.info("event=write_file file_id={d} path=cache_hit", .{file_id});
            return;
        }

        const path = _fi.sstPath(
            self.allocator,
            self.opts.dir,
            file_id,
        ) catch return;
        defer self.allocator.free(path);

        // Probe existence + size via vfs (no allocation, just open +
        // getEndPos).
        const existing_size: ?u64 = if (vfs.g().openFile(
            self.allocator,
            path,
            .{},
        )) |fh| blk: {
            defer fh.close();
            break :blk fh.file_size;
        } else |_| null;

        const size_matches = existing_size != null and
            existing_size.? == data.len;
        if (existing_size != null and !size_matches) {
            log.warn(
                "writeFile: size mismatch for {d} ({d} on disk vs {d}), " ++
                    "rewriting",
                .{ file_id, existing_size.?, data.len },
            );
        }

        if (!size_matches) {
            const fh = vfs.g().createFile(path, .{
                .mode = .direct,
                .flags = .{ .truncate = true },
            }) catch |err| {
                log.err(
                    "writeFile: createFile failed for {d}: {s}",
                    .{ file_id, @errorName(err) },
                );
                return;
            };
            // `data` is a slice into a network frame (see node.zig p1_recv), so
            // its ADDRESS isn't page-aligned — copy it into a page-aligned
            // buffer for the O_DIRECT write. Its LENGTH must already be page
            // aligned: every SST the builder produces is page-padded, so a
            // non-aligned length means a corrupt/unexpected file — log and bail
            // rather than reshape it.
            if (data.len % vfs.page_size != 0) {
                log.err(
                    "writeFile: file {d} length {d} not page-aligned; " ++
                        "refusing O_DIRECT write",
                    .{ file_id, data.len },
                );
                fh.close();
                return;
            }
            const buf = self.allocator.alignedAlloc(
                u8,
                vfs.page_alignment,
                data.len,
            ) catch |err| {
                log.err(
                    "writeFile: bounce alloc failed for {d}: {s}",
                    .{ file_id, @errorName(err) },
                );
                fh.close();
                return;
            };
            defer self.allocator.free(buf);
            @memcpy(buf, data);
            fh.write(buf, 0, .p1) catch |err| {
                log.err(
                    "writeFile: write failed for {d}: {s}",
                    .{ file_id, @errorName(err) },
                );
                fh.close();
                return;
            };
            fh.sync() catch {};
            fh.close();
            fe.info(
                "event=write_file file_id={d} path=wrote bytes={d}",
                .{ file_id, data.len },
            );
        } else {
            fe.info(
                "event=write_file file_id={d} path=disk_hit bytes={d}",
                .{ file_id, data.len },
            );
        }

        // Pre-open the table into the cache so it's ready when compaction needs
        // it.
        const t = Table.open(self.allocator, path, .p1) catch |err| {
            log.err(
                "writeFile: Table.open failed for {d}: {s}",
                .{ file_id, @errorName(err) },
            );
            return;
        };
        const adopted = self.table_cache.own(file_id, t) catch |err| blk: {
            log.err(
                "writeFile: table_cache.own failed for {d}: {s}",
                .{ file_id, @errorName(err) },
            );
            break :blk false;
        };
        // The open-time ref is ours to drop: the cache holds its own once it
        // adopted the table. Unadopted (the apply loop cached the same id
        // first, or the insert failed), the struct is ours to free as well.
        t.decrRef();
        if (!adopted) self.allocator.destroy(t);
    }

    /// Read an SST file from disk. Caller owns the returned slice.
    pub fn readFile(
        self: *Forest,
        allocator: std.mem.Allocator,
        file_id: u64,
    ) ?[]const u8 {
        const path = _fi.sstPath(
            self.allocator,
            self.opts.dir,
            file_id,
        ) catch return null;
        defer self.allocator.free(path);
        const handle = vfs.g().openFile(allocator, path, .{}) catch return null;
        defer handle.close();
        const result = handle.readAligned(
            allocator,
            0,
            @intCast(handle.file_size),
            .p1,
        ) catch return null;
        return result.data;
    }

    // -----------------------------------------
    // All the funcs using tables
    // -----------------------------------------

    /// True iff the namespace has been created (its default table exists).
    /// Used by the leader to reject mutations destined for unknown
    /// namespaces — those would otherwise pile up in immutable memtables
    /// since `buildFlush` silently drops items whose namespace has no
    /// route.
    pub fn hasNamespace(self: *Forest, namespace_id: u64) bool {
        // A namespace exists iff its default table (table_id 0) has trees —
        // ns_create always installs it, drop_ns removes them.
        return self.hasTable(namespace_id, 0);
    }

    /// Get a tree by its global identity (namespace + namespace-local
    /// tree_id): find the table (high 32 bits of tree_id), scan its trees.
    /// Returned with a ref for the caller — `tree.release()` when done.
    pub fn getTree(self: *Forest, namespace_id: u64, tree_id: u64) ?*Tree {
        const trees = self.getTable(
            namespace_id,
            treeTableId(tree_id),
        ) orelse return null;
        return trees.getTreeById(tree_id);
    }

    /// True iff the tree is currently listed (idempotency checks).
    pub fn hasTree(self: *Forest, namespace_id: u64, tree_id: u64) bool {
        const trees = self.getTable(
            namespace_id,
            treeTableId(tree_id),
        ) orelse return false;
        const held = trees.mu.hold(.shared, @src());
        defer held.release();
        return trees.byId(held, tree_id) != null;
    }

    /// Derive the next free tree_id for a (namespace, table_id) table:
    /// (table_id << 32) | (max_existing_split_id + 1), or split 0 when the
    /// table has no trees yet. Replaces the old global next_tree_id counter —
    /// the id is a pure function of the table's current trees, so every node
    /// derives the same value and there is nothing to recover on leader change.
    /// Reads the table's list under its own lock. A proposer's derivation
    /// can still race a publish already in flight — they are a raft round
    /// trip apart, which no local lock covers — and the apply-side
    /// `hasTree` check absorbs the duplicate.
    pub fn nextTreeId(self: *Forest, namespace_id: u64, table_id: u32) u64 {
        var max_tree_id: u64 = makeTreeId(table_id, 0); // table's first tree_id
        if (self.getTable(namespace_id, table_id)) |trees| {
            const held = trees.mu.hold(.shared, @src());
            defer held.release();
            for (trees.list(held)) |e| {
                max_tree_id = @max(max_tree_id, e.tree_id + 1);
            }
        }
        return max_tree_id;
    }

    /// Route to a table's trees by its `{namespace, table_id}` identity.
    /// Returns null if the table was never created. No lock: the map guards
    /// itself and a TableTrees is never freed, so the pointer stays good —
    /// though a dropped table's is empty (`hasTrees`).
    pub fn getTable(
        self: *Forest,
        namespace_id: u64,
        table_id: u32,
    ) ?*TableTrees {
        return self.tables.get(.{
            .namespace = namespace_id,
            .table_id = table_id,
        });
    }

    /// True iff a table exists for (namespace, table_id), i.e. has trees.
    /// Used by catalog bootstrap to make create_tree idempotent across
    /// restarts.
    pub fn hasTable(self: *Forest, namespace_id: u64, table_id: u32) bool {
        const trees = self.getTable(namespace_id, table_id) orelse return false;
        return trees.hasTrees();
    }

    pub fn getOrCreateTable(
        self: *Forest,
        namespace_id: u64,
        table_id: u32,
    ) !*TableTrees {
        const key = TableKey{ .namespace = namespace_id, .table_id = table_id };
        if (self.tables.get(key)) |trees| return trees;

        const fresh = try self.allocator.create(TableTrees);
        errdefer self.allocator.destroy(fresh);
        fresh.* = TableTrees.init(self.allocator);
        const stored = try self.tables.getOrPut(key, fresh);
        if (stored != fresh) {
            fresh.deinit();
            self.allocator.destroy(fresh);
        }
        return stored;
    }
};
