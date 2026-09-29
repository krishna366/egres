const std = @import("std");
const log = std.log.scoped(.forest);
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;
const _config = @import("config.zig");
const _manifest = @import("manifest.zig");
const _ddl = @import("forest_ddl.zig");
const _read = @import("read.zig");
const _stats = @import("stats.zig");
const _table = @import("table.zig");
const _cache = @import("cache.zig");
const vfs = @import("vfs");
const _tree = @import("tree.zig");
const _skl = @import("skl/skiplist.zig");
const _metrics = @import("metrics.zig");
const zroar = @import("zroar");
const flags = @import("flags");

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
const Emitter = _tree.Tree.Emitter;
const _mitr = _table.merge_iterator;

/// Build the SST path for `file_id`. Filename layout is `<id:016x>.sst` —
/// fixed-width hex, so directory listings sort by id lexicographically.
/// Pass `dir = ""` to get the bare filename without any prefix; otherwise
/// the result is `<dir>/<filename>`. Caller owns the returned slice.
pub fn sstPath(allocator: std.mem.Allocator, dir: String, file_id: u64) ![]u8 {
    if (dir.len == 0) return std.fmt.allocPrint(
        allocator,
        "{x:0>16}.sst",
        .{file_id},
    );
    return std.fmt.allocPrint(
        allocator,
        "{s}/{x:0>16}.sst",
        .{ dir, file_id },
    );
}

// ---------------------------------------------------------------------------
// TableTrees
// ---------------------------------------------------------------------------

/// TableTrees holds the LSM trees of a single `{namespace, table_id}` table
/// and maps a key to the tree that owns it. A table is one tree until it
/// grows past `Config.splitBytes`, then splits at the primary-key median into
/// more trees; entries are kept sorted by range.left so we can binary-search.
pub const TableTrees = struct {
    pub const Lock = fiber.RWMutex(.tracked);

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
        self.mu.deinit();
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
            // The first tree's left is unbounded: below every key.
            const left = self.entries.items[mid].key_range.left;
            if (left.isInf() or left.order(search_key) != .gt) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo == 0) return null;
        const candidate = self.entries.items[lo - 1];
        const right = candidate.key_range.right;
        if (!right.isInf() and search_key.order(right) == .gt) return null;
        return candidate;
    }
};

// ---------------------------------------------------------------------------

pub const Memtables = struct {
    allocator: std.mem.Allocator,
    mu: fiber.RWMutex(.tracked),
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
        self.mu.deinit();
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

    /// Which skiplists `getIterators` covers.
    pub const Select = struct {
        /// A txn's private write skiplist. It goes FIRST, as src 0, the
        /// merge's equal-key tie-break, so the txn sees its own writes.
        pending: ?*SkipList = null,
        /// The active memtable, which writes still land in. A read wants
        /// it; a flush must leave it out, so a flush's inputs are the
        /// frozen memtables alone.
        current: bool = true,
        /// Frozen memtables to include: the oldest until their writes
        /// exceed this, and at least one. A read takes them all; a
        /// flush sets its byte budget, and always covers an oldest-first
        /// prefix, since its apply releases the newest flushed memtable
        /// and every older one.
        max_bytes: u64 = std.math.maxInt(u64),
    };

    /// Iterators for a merge: `pending` when given, then the active
    /// memtable when `current`, then the selected immutables newest-first
    /// (their storage order). Each `skl.Iterator.init` acquires the
    /// underlying skiplist — release happens via `deinit` on the returned
    /// iterator (transitively from `MergeIterator.deinit`). Doing the
    /// acquires under our shared lock guarantees that even if a
    /// concurrent flush + `popOldestIfMatches` releases the original ref
    /// a moment later, the skiplist stays alive until the iterator drops
    /// its ref. Caller frees the slice itself.
    pub fn getIterators(
        self: *Memtables,
        allocator: std.mem.Allocator,
        select: Select,
        limit: _skl.Iterator.Limit,
    ) ![]_mitr.Iterator {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();

        // A budget leaves out the newest frozen memtables, so a read that
        // includes `current` would skip writes between them: only a
        // flush budgets, and a flush leaves `current` out.
        std.debug.assert(!select.current or
            select.max_bytes == std.math.maxInt(u64));

        // `immutables` is newest first, so the oldest are at the end:
        // count how many fit the budget from the end, then take that
        // tail as is.
        const all = self.immutables.items;
        var n: usize = 0;
        var bytes: u64 = 0;
        while (n < all.len) {
            bytes += all[all.len - 1 - n].memoryUsage();
            if (n > 0 and bytes > select.max_bytes) break;
            n += 1;
        }
        const imms = all[all.len - n ..];

        var out: std.ArrayList(_mitr.Iterator) = .empty;
        errdefer {
            for (out.items) |*it| it.deinit();
            out.deinit(allocator);
        }
        // Widen before adding: two `u1`s overflow at 2.
        const extra: usize = @as(usize, @intFromBool(select.pending != null)) +
            @intFromBool(select.current);
        try out.ensureTotalCapacityPrecise(allocator, extra + imms.len);
        if (select.pending) |p| out.appendAssumeCapacity(.{
            .skiplist = try _skl.Iterator.init(allocator, p, limit),
        });
        if (select.current) out.appendAssumeCapacity(.{
            .skiplist = try _skl.Iterator.init(allocator, self.current, limit),
        });
        for (imms) |imm| out.appendAssumeCapacity(.{
            .skiplist = try _skl.Iterator.init(allocator, imm, limit),
        });
        return out.toOwnedSlice(allocator);
    }

    /// id of the current memtable. 0 means the initial placeholder is still
    /// in place — no `newMemtable` has been applied yet, and any writes that
    /// land in `current` will be discarded by the first `swapCurrentOut`.
    pub fn currentId(self: *Memtables) u64 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        return self.current.raft_index;
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

    /// Freeze the current memtable: move it to immutables, create a new
    /// current. `log_index` is the apply index of the Raft `newMemtable`
    /// entry: it becomes the new memtable's `raft_index`, which is its
    /// identity and pins the snapshot horizon (`snapshot.index <=
    /// oldest.raft_index - 1`). A log index is deterministic on every
    /// node and monotonic by construction, which is what
    /// `popOldestIfMatches`'s `<` / `>` comparisons need.
    ///
    /// Special case: the raft_index=0 placeholder (created by `Forest.open`
    /// before any real `newMemtable` has been applied) is *discarded*, not
    /// frozen — it would otherwise sit in immutables forever, since no
    /// flush ever targets it. A real entry's log index is >= 1, so it
    /// never collides with the placeholder.
    pub fn swapCurrentOut(self: *Memtables, log_index: u64) !void {
        self.mu.lock(@src());
        defer self.mu.unlock();

        if (self.current.raft_index == 0) {
            self.current.release();
        } else {
            // Newest-first: the just-frozen memtable goes in front.
            try self.immutables.insert(self.allocator, 0, self.current);
        }
        const new_mt = try SkipList.init(self.allocator);
        new_mt.raft_index = log_index;
        self.current = new_mt;
    }

    /// Log index of the oldest in-memory memtable. Returns 0 when the only
    /// thing around is the initial placeholder (no real `newMemtable` entry
    /// has been applied yet), which the caller treats as "no snapshot yet."
    pub fn oldestInMemoryLogIndex(self: *Memtables) u64 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        const imms = self.immutables.items;
        if (imms.len > 0) return imms[imms.len - 1].raft_index;
        return self.current.raft_index;
    }

    /// Release every immutable whose id is strictly less than
    /// `expected_id`, then pop+release the oldest IFF its id equals
    /// `expected_id`. No-op when the matched memtable isn't found.
    ///
    /// Used by `applyFlush`. `expected_id` is the NEWEST memtable the
    /// flush covered (`buildFlush` merges several), so:
    ///   - oldest.raft_index < expected_id  → covered by the same flush,
    ///     its data is in the SSTs just installed. Also the recovery
    ///     phantom: the flush happened pre-snap on the leader, the SST is
    ///     in snap.data, but we still have the local memtable (its
    ///     `newMemtable` was post-snap and replayed, its `flush` was
    ///     pre-snap and compacted). Release and check the next oldest.
    ///   - oldest.raft_index == expected_id → normal: pop and release.
    ///   - oldest.raft_index > expected_id  → flush's target memtable was popped on
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
            self.immutables.items[self.immutables.items.len - 1].raft_index <
                expected_id)
        {
            const last = self.immutables.pop().?;
            log.debug(
                "popOldestIfMatches: releasing immutable id={d} below " ++
                    "expected_id={d}",
                .{ last.raft_index, expected_id },
            );
            last.release();
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
            self.immutables.items[self.immutables.items.len - 1].raft_index;
        if (oldest_id != expected_id) {
            log.info(
                "popOldestIfMatches: skipping pop, oldest.raft_index={d} > " ++
                    "expected_id={d} (recovery: pre-snap flush, memtable " ++
                    "not re-created locally)",
                .{ oldest_id, expected_id },
            );
            return;
        }
        const imm = self.immutables.pop().?;
        imm.release();
    }

    /// How many frozen memtables are waiting for a flush.
    pub fn immutableCount(self: *Memtables) usize {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        return self.immutables.items.len;
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
    /// The options: the registered `--zdb` flag on a server. Read with
    /// `cur()` once per flush, job or check, so an admin update reaches
    /// the next one; the fields in `Config.fixed` never change. The
    /// caller keeps it alive past `close`.
    live: *flags.Live(_config.Config),
    manifest: _manifest.Manifest,
    /// One TableTrees per table, keyed by its `{namespace, table_id}` identity.
    /// The TableTreess own the live `*Tree` objects (in their sorted entries);
    /// there is no separate tree map. Look a tree up by id via `getTree`.
    /// The map guards itself, and a TableTrees once inserted is never freed
    /// (a dropped table's stays, empty — see `TableTrees.hasTrees`), so
    /// `getTable` needs no lock and its result never dangles.
    tables: fiber.SafeHashMap(TableKey, *TableTrees),
    table_cache: TableCache,
    memtables: Memtables,
    /// The read floor: the oldest timestamp a reader may ask for. Every
    /// version below it that a newer version shadows may already be gone,
    /// since a bottom-level compaction discards under it (`VersionTracker`,
    /// fed the floor by `Engine.proposeCompaction`), and the read entry
    /// points refuse a `read_ts` below it.
    ///
    /// It is the timestamp of the installed raft snapshot
    /// (`Snapshot.timestamp`), raised through `setGlobalMinRead` when the
    /// snapshot entry applies and at recovery, so every node raises it at
    /// the same point in the log: a compaction the leader builds after
    /// that entry discards below it, and a follower applies that
    /// compaction after the same entry. It never moves back. No new read
    /// starts below it — a new read_ts is the applied timestamp, and the
    /// snapshot entry applied earlier. A read open across the install is
    /// refused from then on rather than answered from a table missing
    /// its versions.
    ///
    /// 0 means "nothing is droppable", the safe default: no timestamp is
    /// below it, so no read is rejected and no row is ever dropped.
    global_min_read: std.atomic.Value(u64),

    // Read paths, from read.zig.
    pub const get = _read.get;
    pub const getRaw = _read.getRaw;
    pub const getBitmap = _read.getBitmap;
    pub const ScanOptions = _read.ScanOptions;
    pub const Lookup = _read.Lookup;
    pub const scan = _read.scan;
    pub const openCursor = _read.openCursor;
    pub const Cursor = _read.Cursor;
    pub const maxKey = _read.maxKey;
    pub const MultiGet = _read.MultiGet;
    pub const multiGet = _read.multiGet;

    // The stats dump, from stats.zig.
    pub const StatsScope = _stats.StatsScope;
    pub const StatsOpts = _stats.StatsOpts;
    pub const writeStats = _stats.writeStats;
    pub const writeForestStats = _stats.writeForestStats;
    pub const writeTreeStats = _stats.writeTreeStats;
    pub const writeNamespaceStats = _stats.writeNamespaceStats;
    pub const writeBytes = _stats.writeBytes;

    // Building and applying manifest changes, from forest_ddl.zig.
    pub const installTree = _ddl.installTree;
    pub const collectMissingFileIds = _ddl.collectMissingFileIds;
    pub const openTablesFromManifest = _ddl.openTablesFromManifest;
    pub const buildNamespaceCreate = _ddl.buildNamespaceCreate;
    pub const CreateNamespaceResult = _ddl.CreateNamespaceResult;
    pub const buildCreateNamespace = _ddl.buildCreateNamespace;
    pub const applyNamespaceCreate = _ddl.applyNamespaceCreate;
    pub const CreateTablesResult = _ddl.CreateTablesResult;
    pub const buildCreateTables = _ddl.buildCreateTables;
    pub const applyCreateTree = _ddl.applyCreateTree;
    pub const buildDropTables = _ddl.buildDropTables;
    pub const applyDropTree = _ddl.applyDropTree;
    pub const buildDropNamespace = _ddl.buildDropNamespace;
    pub const applyNamespaceDrop = _ddl.applyNamespaceDrop;
    pub const buildSplit = _ddl.buildSplit;
    pub const applySplit = _ddl.applySplit;
    pub const splitTreeForTests = _ddl.splitTreeForTests;

    /// Open a forest with empty in-memory state. Persistent state is restored
    /// later via `installManifest` (called by recovery if a Raft snapshot is
    /// present); fresh starts go straight to `createTree` / Raft replay.
    pub fn open(
        allocator: std.mem.Allocator,
        live: *flags.Live(_config.Config),
    ) !*Forest {
        const opts = live.cur();
        var manifest = _manifest.Manifest.init(allocator);
        errdefer manifest.deinit();

        const self = try allocator.create(Forest);
        errdefer allocator.destroy(self);

        // Initial placeholder memtable. raft_index 0 is the sentinel that
        // marks "no real memtable yet" — the first replayed `newMemtable`
        // swaps it out (Phase 5).
        const mt = try SkipList.init(allocator);
        errdefer mt.release();

        self.* = .{
            .allocator = allocator,
            .live = live,
            .manifest = manifest,
            .tables = fiber.SafeHashMap(TableKey, *TableTrees).init(allocator),
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

        self.manifest.deinit();
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
    /// `log_index` is the apply index of that Raft entry: it pins the
    /// snapshot horizon for the new memtable and IS the new memtable's id,
    /// so every node derives the same id without the leader proposing one,
    /// and ids stay monotonic across freezes.
    pub fn newMemtable(self: *Forest, log_index: u64) !void {
        try self.memtables.swapCurrentOut(log_index);
    }

    /// buildFlush is Phase A of the memtable flush: merges the oldest
    /// frozen memtables up to `job_max_bytes`, routes each key to its
    /// tree, builds one L0 SST per tree, and returns the manifest
    /// changes. The caller proposes these through Raft. Null when
    /// nothing is waiting.
    ///
    /// `memtable_id` names the NEWEST memtable the flush covers. The apply
    /// (`applyFlush` → `popOldestIfMatches`) releases that one and every
    /// older one, which is exactly the set flushed here: memtables freeze
    /// and flush in log order.
    ///
    /// Several memtables in one flush give each tree one larger table in
    /// place of several small ones, and a key written in more than one of
    /// them lands in that table once, with all its versions.
    pub const FlushResult = struct {
        changes: std.ArrayList(_manifest.SSTableChange),
        newest_memtable_id: u64,
    };

    pub fn buildFlush(self: *Forest) !?FlushResult {
        const opts = self.live.cur();
        // One merge over the oldest frozen memtables up to the flush's
        // byte budget, newest first: an item is one key with every
        // version of it across all of them, newest first. Each iterator
        // holds its own ref on its skiplist; the merge takes them over.
        const srcs = try self.memtables.getIterators(
            self.allocator,
            .{ .current = false, .max_bytes = opts.job_max_bytes },
            .{},
        );
        defer self.allocator.free(srcs);
        if (srcs.len == 0) return null;

        const newest_id = srcs[0].skiplist.skiplist.raft_index;
        var iter = try _mitr.MergeIterator.init(self.allocator, srcs);
        defer iter.deinit();
        try iter.seekRange(Key.inf, Key.inf);

        // One Emitter per tree, in the order the trees are met. The
        // merge is globally sorted and a tree covers a contiguous key
        // range of one table, so a tree's keys arrive as one run: the
        // current emitter serves until a key falls outside its tree,
        // and a tree is never met twice. Once a run ends its table
        // goes out on a fiber of its own, overlapping the walk of the
        // trees after it. Each emitter holds its tree's ref for as
        // long as it lives. An item is one key with every version of
        // it, newest first, which is the order the emitter wants. At
        // L0 an emitter writes one table, whatever it weighs, and
        // folds only the roaring delta chains the flushed memtables
        // hold whole.
        var emitters: std.ArrayList(*Emitter) = .empty;
        defer {
            for (emitters.items) |em| {
                em.deinit();
                em.tree.release();
                self.allocator.destroy(em);
            }
            emitters.deinit(self.allocator);
        }
        const min_read = self.globalMinRead();
        var flush_err: utils.SafeError = .{};
        var flushes = fiber.Group(void).init(self.allocator);
        defer {
            _ = flushes.wait();
            flushes.deinit();
        }

        var cur: ?*Emitter = null;
        while (iter.itemRef()) |item| {
            if (cur == null or !cur.?.tree.covers(item.key)) {
                // We're on a new tree. Flush the previous one; a table
                // that failed fails the flush, so no point walking on.
                if (cur) |em| {
                    try flushes.spawn(.{}, flushTable, .{ em, &flush_err });
                }
                if (flush_err.get()) |err| return err;

                cur = try self.openFlushEmitter(&emitters, item.key, min_read);
            }
            // No tree for the key (its namespace is gone): dropped.
            if (cur) |em| try em.push(item);
            try iter.next();
        }
        if (cur) |em| try flushes.spawn(.{}, flushTable, .{ em, &flush_err });

        // Every table written, then each emitter's syncs joined for
        // its CREATEs.
        _ = flushes.wait();
        if (flush_err.get()) |err| return err;

        var changes: std.ArrayList(_manifest.SSTableChange) = .empty;
        errdefer changes.deinit(self.allocator);

        for (emitters.items) |em| {
            var r = try em.finish();
            defer r.changes.deinit(self.allocator);
            try changes.appendSlice(self.allocator, r.changes.items);
        }

        // Best-effort directory sync.
        if (self.live.cur().dir.len > 0) {
            var dir_sync = try fiber.spawn(
                .{},
                vfs.syncDir,
                .{self.live.cur().dir},
            );
            defer dir_sync.join();
        }

        if (srcs.len > 1) log.info(
            "buildFlush: {d} memtables up to id={d} into {d} table(s)",
            .{ srcs.len, newest_id, changes.items.len },
        );
        return .{ .changes = changes, .newest_memtable_id = newest_id };
    }

    /// A flush emitter's table out, on its own fiber: the emitter is
    /// done being pushed to, and nothing touches it until the fiber
    /// is joined. A failure is logged here and set in `err_out`.
    fn flushTable(em: *Emitter, err_out: *utils.SafeError) void {
        em.flush() catch |err| {
            log.err("flush tree={d} table failed: {s}", .{
                em.tree.tree_id, @errorName(err),
            });
            err_out.set(err);
        };
    }

    /// An L0 emitter for the tree owning `key`, added to `emitters`,
    /// or null when no tree does. The emitter takes over the tree ref.
    fn openFlushEmitter(
        self: *Forest,
        emitters: *std.ArrayList(*Emitter),
        key: Key,
        min_read: u64,
    ) !?*Emitter {
        const trees = self.getTable(key.tableKey()) orelse return null;
        const tree = trees.getTreeByKey(key) orelse return null;
        errdefer tree.release();

        const em = try self.allocator.create(Emitter);
        errdefer self.allocator.destroy(em);

        em.* = Emitter.init(tree, 0, .p0, min_read);
        errdefer em.deinit();

        try emitters.append(self.allocator, em);
        return em;
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
        table_key: TableKey,
        items: []const Item,
    ) !std.ArrayList(_manifest.SSTableChange) {
        var changes: std.ArrayList(_manifest.SSTableChange) = .empty;
        errdefer changes.deinit(allocator);
        if (items.len == 0) return changes;

        // One emitter at the bottom level: tables cut at `table_bytes`,
        // and every item kept, a single state above the horizon.
        const bottom: usize = self.live.cur().max_levels - 1;
        const tree = try self.singleTree(table_key);
        defer tree.release();

        var em = Emitter.init(tree, bottom, .p2, self.globalMinRead());
        defer em.deinit();

        for (items) |item| try em.push(item);

        var r = try em.finish();
        defer r.changes.deinit(self.allocator);
        try changes.appendSlice(allocator, r.changes.items);

        if (self.live.cur().dir.len > 0) {
            var dir_sync = try fiber.spawn(.{}, vfs.syncDir, .{self.live.cur().dir});
            defer dir_sync.join();
        }
        return changes;
    }

    /// Whether every CREATE in `changes` is now a table of `table_key`'s
    /// bottom level — CREATE INDEX's post-install verification.
    /// applyBackfill skips, deterministically on every node, when the
    /// bottom level was not empty; a table count cannot tell a skip
    /// from an install (either way the level is non-empty after), so
    /// the proposed sst ids are looked for by name.
    pub fn bottomLevelHas(
        self: *Forest,
        table_key: TableKey,
        changes: []const _manifest.SSTableChange,
    ) !bool {
        // The proposed ids as a set (a few files), built before the
        // tree lock; then one pass over the level counting the ones it
        // holds. Ids are unique on both sides, so a full count means
        // every file is present.
        var wanted = try zroar.Bitmap.init(self.allocator);
        defer wanted.deinit();
        for (changes) |c| _ = try wanted.set(c.sst_id);

        const tree = try self.singleTree(table_key);
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
    fn singleTree(self: *Forest, table_key: TableKey) !*Tree {
        const trees = self.getTable(table_key) orelse
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
        index: u64,
        changes: []const _manifest.SSTableChange,
        memtable_id: u64,
    ) !void {
        // Load all new L0 tables concurrently via the cache. writeFile may
        // have pre-opened these (follower path) → cache hit. On the leader,
        // flush writes bytes directly, so these will typically be misses.
        // This is the step that can fail (I/O), so it runs before any
        // state changes: a retry of the entry then starts from a clean
        // manifest and clean levels.
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
        defer TableCache.decrRefs(needed.items);

        // Manifest and levels change together, both in memory; the entry
        // itself is already durable in the raft log.
        if (changes.len > 0) {
            try self.manifest.commit(index, changes, &.{});
        }

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
                // A flush output is a new file, so the applying entry's
                // index is exactly what the manifest just stored for it.
                try tree.levels[c.level].addEntry(held, n.id, index, n.table.?);
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
    /// The most L0 tables in any tree these marshaled keys land in: what
    /// a write of them waits on (`Engine.applyWriteBackpressure`). Only
    /// those trees: a backlog in one table's tail tree must not slow
    /// writes to another table, or to the same table's other trees. A
    /// key with no tree (a table not yet created) counts as 0; the
    /// apply will drop it.
    pub fn maxL0TableCountForKeys(self: *Forest, keys: []const String) u32 {
        var max_count: u32 = 0;
        // Consecutive keys of one batch mostly share a tree (rows are
        // keyed by increasing egids); skip the lookup when they do.
        var last: ?*Tree = null;
        defer if (last) |t| t.release();

        for (keys) |raw| {
            const key = Key.init(raw);
            if (last) |ltree| {
                if (ltree.key_range.contains(key)) continue;
                ltree.release();
                last = null;
            }
            const trees = self.getTable(key.tableKey()) orelse continue;
            const tree = trees.getTreeByKey(key) orelse continue;
            last = tree;
            const n: u32 = @intCast(tree.numTablesAt(0));
            if (n > max_count) max_count = n;
        }
        return max_count;
    }

    pub fn shouldFreeze(self: *Forest) bool {
        return self.memtables.memoryUsage() >= self.live.cur().table_bytes;
    }

    /// Check if there are frozen memtables waiting to be flushed.
    pub fn hasFrozenMemtables(self: *Forest) bool {
        const oldest = self.memtables.peekOldest() orelse return false;
        oldest.release();
        return true;
    }

    /// Check if an SST file exists on disk.
    pub fn hasFile(self: *Forest, file_id: u64) bool {
        const path = sstPath(
            self.allocator,
            self.live.cur().dir,
            file_id,
        ) catch return false;
        defer self.allocator.free(path);
        vfs.access(path) catch return false;
        return true;
    }

    /// Let go of every file the manifest unlisted since the last call:
    /// the table cache drops its entry and its ref. Run after each
    /// manifest apply: a compaction's inputs are dead for readers the
    /// moment the apply removes them from the level, so this is usually
    /// the last ref, and the table frees itself, blocks in the block
    /// cache included. A reader or compaction that pinned such a table
    /// before the apply holds its own ref and keeps the open `Table`
    /// until it releases. The files themselves wait for `deleteFiles`.
    pub fn evictTables(self: *Forest) !void {
        const ids = try self.manifest.takeToEvict();
        defer self.allocator.free(ids);

        for (ids) |id| self.table_cache.evict(id);
        if (ids.len > 0) log.debug(
            "evictTables: evicted {d} unlisted file(s)",
            .{ids.len},
        );
    }

    /// Unlink the SST files this node no longer needs: every file retired
    /// at a log index at or below `snapshot_index`. A restart restores the
    /// manifest of the durable snapshot at index I and replays the entries
    /// above I, opening every file that manifest lists and every file an
    /// entry above I creates — so a file whose last listing went away at
    /// index K is needed by no future restart once I >= K. Local rule,
    /// local inputs: every node sweeps its own copies off its own snapshot
    /// index.
    pub fn deleteFiles(self: *Forest, snapshot_index: u64) !void {
        const to_delete = try self.manifest.takeToDelete(
            self.allocator,
            snapshot_index,
        );
        defer self.allocator.free(to_delete);
        if (to_delete.len == 0) return;

        for (to_delete) |r| {
            // The table cache let go of the file when the manifest
            // unlisted it (`evictTables`); only the file is left.
            const path = try sstPath(self.allocator, self.live.cur().dir, r.sst_id);
            defer self.allocator.free(path);
            vfs.deleteFile(path) catch |err| switch (err) {
                // Already gone — removeOrphans took it, or a previous
                // sweep did and the process restarted mid-way.
                error.FileNotFound => {},
                else => {
                    log.warn(
                        "deleteFiles: unlink {s}: {s}",
                        .{ path, @errorName(err) },
                    );
                    continue;
                },
            };
        }
        log.info(
            "deleteFiles: unlinked {d} SST file(s) retired at or below " ++
                "snapshot index {d}",
            .{ to_delete.len, snapshot_index },
        );
    }

    /// Unlink every SST-shaped file in the data dir that the manifest does
    /// not know and that was last written at or before `cutoff_s`, whole
    /// seconds on the realtime clock.
    ///
    /// The engine derives `cutoff_s` as the earlier of two moments
    /// (`Engine.scanOrphans`):
    ///   - the wall-clock time of the log entry at this node's snapshot
    ///     index (`Snapshot.timestamp`), 0 before the first snapshot, and
    ///     then the scan removes nothing. A file written after it may
    ///     belong to an entry the snapshot does not cover.
    ///   - now minus `SnapshotCfg.orphan_min_age_seconds`. A file this
    ///     process is still writing, or one a leader is pushing to this
    ///     follower, is in no manifest yet, so anything younger is
    ///     assumed to be in flight. This has to exceed one compaction's
    ///     window from its first output write to its proposal; the
    ///     snapshot entry can be much more recent than that.
    ///
    /// It catches three kinds of leftovers: files retired at or below the
    /// snapshot index before a crash (the `to_delete` map is not persisted, so
    /// `deleteFiles` will never hear of them again), partial files from a
    /// crash between writing an SST and proposing its manifest change, and
    /// orphans from before file GC existed. The second kind is newer than
    /// the cutoff when the node comes back, which is why the scan runs
    /// periodically and not just at startup.
    ///
    /// The directory is listed by name only, and only the files the
    /// manifest does not know are stat'ed: normally a handful out of
    /// hundreds of thousands.
    pub fn removeOrphans(self: *Forest, cutoff_s: i64) !void {
        // TODO: Switch to using DirStore.

        const unknowns = blk: {
            var arena = std.heap.ArenaAllocator.init(self.allocator);
            defer arena.deinit();
            const arloc = arena.allocator();

            const entries = try vfs.listFiles(arloc, self.live.cur().dir, .names);
            break :blk try self.manifest.unknownFiles(self.allocator, entries);
        };
        defer self.allocator.free(unknowns);

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arloc = arena.allocator();

        var removed: usize = 0;
        for (unknowns) |id| {
            const path = try sstPath(arloc, self.live.cur().dir, id);
            // Gone between the listing and here: someone else's unlink.
            const st = vfs.fileStat(path) catch continue;
            if (st.mtime_s > cutoff_s) continue;

            // A pre-opened table for a create that never applied sits in
            // the cache with an open fd; unlinked without this, the file
            // would keep its blocks until the process exits.
            self.table_cache.evict(id);
            vfs.deleteFile(path) catch |err| switch (err) {
                error.FileNotFound => continue,
                else => {
                    log.warn(
                        "removeOrphans: unlink {s}: {s}",
                        .{ path, @errorName(err) },
                    );
                    continue;
                },
            };
            removed += 1;
        }
        if (removed > 0) log.info(
            "removeOrphans: unlinked {d} of {d} unknown SST file(s) last " ++
                "written at or before the cutoff",
            .{ removed, unknowns.len },
        );
    }

    /// Write an SST file to disk and pre-open it into the table cache.
    /// Skips the write when the file is already cached, or already on disk with
    /// a matching size — file_id → content is deterministic in this system, and
    /// a size match is a strong signal the bytes are correct (no cross-ID
    /// collisions possible). If a size mismatch is detected, we log and
    /// rewrite.
    pub fn writeFile(self: *Forest, file_id: u64, data: String) void {
        const fe = std.log.scoped(.file_event);
        if (self.table_cache.contains(file_id)) {
            fe.info("event=write_file file_id={d} path=cache_hit", .{file_id});
            return;
        }

        const path = sstPath(
            self.allocator,
            self.live.cur().dir,
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
        const t = Table.open(self.allocator, path, file_id, .p1) catch |err| {
            log.err(
                "writeFile: Table.open failed for {d}: {s}",
                .{ file_id, @errorName(err) },
            );
            return;
        };
        // `own` takes the cache's own ref when it adopts the table; when it
        // does not (the apply loop cached the same id first, or the insert
        // failed) the open-time ref below is the only one and frees it.
        _ = self.table_cache.own(file_id, t) catch |err| log.err(
            "writeFile: table_cache.own failed for {d}: {s}",
            .{ file_id, @errorName(err) },
        );
        t.decrRef();
    }

    /// Read an SST file from disk. The bytes are `result.data`; the caller
    /// releases them with `result.free()`, which knows the page-aligned
    /// buffer behind the slice (a plain `allocator.free` on the slice frees
    /// with the wrong alignment).
    pub fn readFile(
        self: *Forest,
        allocator: std.mem.Allocator,
        file_id: u64,
    ) ?vfs.ReadResult {
        const path = sstPath(
            self.allocator,
            self.live.cur().dir,
            file_id,
        ) catch return null;
        defer self.allocator.free(path);
        const handle = vfs.g().openFile(allocator, path, .{}) catch return null;
        defer handle.close();
        return handle.readAligned(
            allocator,
            0,
            @intCast(handle.file_size),
            .p1,
        ) catch return null;
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
        return self.hasTable(.{ .namespace = namespace_id, .table_id = 0 });
    }

    /// Get a tree by its global identity (namespace + namespace-local
    /// tree_id): find the table (high 32 bits of tree_id), scan its trees.
    /// Returned with a ref for the caller — `tree.release()` when done.
    pub fn getTree(self: *Forest, namespace_id: u64, tree_id: u64) ?*Tree {
        const trees = self.getTable(.{
            .namespace = namespace_id,
            .table_id = treeTableId(tree_id),
        }) orelse return null;
        return trees.getTreeById(tree_id);
    }

    /// True iff the tree is currently listed (idempotency checks).
    pub fn hasTree(self: *Forest, namespace_id: u64, tree_id: u64) bool {
        const trees = self.getTable(.{
            .namespace = namespace_id,
            .table_id = treeTableId(tree_id),
        }) orelse return false;
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
    pub fn nextTreeId(self: *Forest, table_key: TableKey) u64 {
        // The table's first tree_id.
        var max_tree_id: u64 = makeTreeId(table_key.table_id, 0);
        if (self.getTable(table_key)) |trees| {
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
    pub fn getTable(self: *Forest, table_key: TableKey) ?*TableTrees {
        return self.tables.get(table_key);
    }

    /// True iff a table exists for (namespace, table_id), i.e. has trees.
    /// Used by catalog bootstrap to make create_tree idempotent across
    /// restarts.
    pub fn hasTable(self: *Forest, table_key: TableKey) bool {
        const trees = self.getTable(table_key) orelse return false;
        return trees.hasTrees();
    }

    pub fn getOrCreateTable(
        self: *Forest,
        table_key: TableKey,
    ) !*TableTrees {
        if (self.tables.get(table_key)) |trees| return trees;

        const fresh = try self.allocator.create(TableTrees);
        errdefer self.allocator.destroy(fresh);
        fresh.* = TableTrees.init(self.allocator);
        const stored = try self.tables.getOrPut(table_key, fresh);
        if (stored != fresh) {
            fresh.deinit();
            self.allocator.destroy(fresh);
        }
        return stored;
    }
};
