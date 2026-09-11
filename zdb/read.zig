//! Forest's read paths: point lookups (`get`, `getRaw`, `getBitmap`),
//! the batch they are one-key calls of (`MultiGet`, `multiGet`), and
//! the range scan with its `Cursor` and `maxKey`. Every function here
//! takes a `*Forest` and leaves it unchanged; `Forest` aliases each
//! declaration back in, so callers keep using method syntax.

const std = @import("std");
const vfs = @import("vfs");
const zroar = @import("zroar");
const utils = @import("utils");
const String = utils.String;
const _level = @import("level.zig");
const _table = @import("table.zig");
const _tree = @import("tree.zig");
const _skl = @import("skl/skiplist.zig");
const DeltaReader = @import("buffers.gen.zig").RoaringDeltaReader;

const Forest = @import("forest.zig").Forest;
const Key = @import("kv.zig").Key;
const Meta = @import("kv.zig").Meta;
const KeyRange = _level.KeyRange;
const Level = _level.Level;
const Item = _table.table.Item;
const Table = _table.Table;
const TableIterator = _table.TableIterator;
const Tree = _tree.Tree;
const merge_iterator = _table.merge_iterator;
const Iterator = merge_iterator.Iterator;
const MergeIterator = merge_iterator.MergeIterator;
const Pin = merge_iterator.Pin;

/// Many keys of one table at one read_ts, resolved together (see
/// `MultiGet`). The result owns its pins: read it through `item` /
/// `bitmap`, then `deinit`. `keys` need not be sorted; they are
/// borrowed until then. Rejects a read_ts below `global_min_read`,
/// whose versions may already have been compacted away.
pub fn multiGet(
    self: *Forest,
    allocator: std.mem.Allocator,
    ns: u64,
    table_id: u32,
    keys: []const String,
    opts: MultiGet.Options,
) !MultiGet {
    if (opts.read_ts < self.globalMinRead()) {
        return error.ReadBelowMinRead;
    }
    var mg = try MultiGet.init(allocator, ns, table_id, keys, opts);
    errdefer mg.deinit();
    try fillMultiGet(self, &mg);
    return mg;
}

/// The walk: the memtables (with the txn's pending writes, when
/// given) through one merge, then each tree's levels for the keys
/// still pending. Memtable refs are taken BEFORE the tree lookup:
/// data flushed in between is then visible in both, and the node's
/// maxTs skips the L0 duplicate. The reverse order could miss it in
/// both.
fn fillMultiGet(self: *Forest, mg: *MultiGet) !void {
    const alloc = mg.allocator;
    {
        const srcs = try self.memtables.getIterators(alloc, mg.opts.pending);
        var merge: Iterator = .{ .merge = MergeIterator.init(alloc, srcs) };
        defer {
            merge.deinit(); // releases each source's skiplist ref
            alloc.free(srcs);
        }
        try mg.drainMerge(&merge);
    }

    // Keys ascend, so a tree covers a contiguous run of the pending
    // nodes; the run ends at the first node another tree owns.
    const trees = self.getTable(mg.ns, mg.table_id) orelse return;
    const pend = mg.pending.items;
    var i: usize = 0;
    while (i < pend.len) {
        const first = mg.keyAt(pend[i], mg.opts.read_ts);
        const tree = trees.getTreeByKey(first) orelse {
            i += 1;
            continue;
        };
        defer tree.release();

        var j = i + 1;
        while (j < pend.len) : (j += 1) {
            const key = mg.keyAt(pend[j], mg.opts.read_ts);
            const owner = trees.getTreeByKey(key) orelse break;
            defer owner.release();

            if (owner.tree_id != tree.tree_id) break;
        }
        try mg.walkTree(tree, pend[i..j], 0);
        i = j;
    }
    MultiGet.compactPending(&mg.pending);
}

/// Get a value by key: the newest version at or below
/// `search_key.timestamp`, from the memtables (current, then
/// immutables newest-to-oldest) or the owning tree. Returned Item is
/// owned by the caller; call item.free(allocator) when done.
///
/// A key deleted at or below `search_key.timestamp` reads as absent: the
/// tombstone wins the lookup and is reported as null. Rejects reads below
/// `global_min_read`, whose versions may already have been compacted away.
pub fn get(
    self: *Forest,
    allocator: std.mem.Allocator,
    search_key: Key,
    priority: vfs.Priority,
) !?Item {
    if (search_key.timestamp < self.globalMinRead()) {
        return error.ReadBelowMinRead;
    }
    return getOne(self, allocator, search_key, priority, false);
}

/// get without the tombstone filter or the horizon check: returns the
/// winning version verbatim, tombstone or not. For callers that need to
/// see deletions as data (compaction diagnostics, tests, repair tools)
/// rather than as absence.
pub fn getRaw(
    self: *Forest,
    allocator: std.mem.Allocator,
    search_key: Key,
    priority: vfs.Priority,
) !?Item {
    return getOne(self, allocator, search_key, priority, true);
}

/// A one-key row MultiGet, its item duped out with `allocator`.
fn getOne(
    self: *Forest,
    allocator: std.mem.Allocator,
    search_key: Key,
    priority: vfs.Priority,
    include_deleted: bool,
) !?Item {
    const keys = [_]String{search_key.primary};
    var mg = try MultiGet.init(
        allocator,
        search_key.namespace,
        search_key.table_id,
        &keys,
        .{
            .read_ts = search_key.timestamp,
            .priority = priority,
            .include_deleted = include_deleted,
        },
    );
    defer mg.deinit();
    try fillMultiGet(self, &mg);
    const found = mg.item(search_key.primary) orelse return null;
    return try found.dupe(allocator);
}

/// The posting bitmap for `search_key`'s primary as of
/// `search_key.timestamp` (the read_ts — same convention as `get`).
/// A one-key posting MultiGet: the memtables, then the owning tree's
/// levels, stopping at the first terminal (a state or a tombstone).
/// After compaction rollup the newest version is usually already a
/// state, so the walk rarely descends past the level that holds it.
///
/// An absent key, a tombstoned key, and an empty posting all return
/// an EMPTY bitmap — an index reader cannot distinguish them and
/// should not try. The bitmap is owned by `allocator`; deinit it.
///
/// Fails with CorruptPosting on undecodable delta bytes and
/// NotRoaring if the chain contains a non-roaring version —
/// getBitmap is only meaningful on keys written as roaring postings.
pub fn getBitmap(
    self: *Forest,
    allocator: std.mem.Allocator,
    search_key: Key,
    priority: vfs.Priority,
) !zroar.Bitmap {
    if (search_key.timestamp < self.globalMinRead()) {
        return error.ReadBelowMinRead;
    }
    const keys = [_]String{search_key.primary};
    var mg = try MultiGet.init(
        allocator,
        search_key.namespace,
        search_key.table_id,
        &keys,
        .{ .read_ts = search_key.timestamp, .priority = priority },
    );
    defer mg.deinit();
    try fillMultiGet(self, &mg);
    return mg.bitmap(allocator, search_key.primary);
}

/// Many keys of one table, resolved together: for each, its versions
/// at or below read_ts, newest first, down to the first that is not
/// a delta (a state or a tombstone), which closes the chain. A row is
/// one version; a posting is its deltas over a state, folded into a
/// bitmap by `bitmap`. The keys are kept as nodes sorted by key (duplicates
/// collapse), so a table's share of them is one contiguous slice
/// found by binary search and consecutive keys land in the same
/// block. The sources are walked newest first — the txn's pending
/// writes and the memtables through one merge (`drainMerge`), then
/// each tree's levels from the top (`walkTree`) — and at every level
/// only the tables whose range and bloom filter admit a key that is
/// still pending are probed. A key leaves `pending` as soon as its
/// versions settle it, so lower levels cost only what is still open.
/// Versions are borrowed under pins, never copied. The key slices are
/// borrowed from the caller for the MultiGet's lifetime; the nodes
/// and versions come from one arena, and `deinit` releases every pin.
pub const MultiGet = struct {
    /// The caller's allocator: iterators and the per-level table
    /// slices, which come and go during the walk.
    allocator: std.mem.Allocator,
    /// Nodes, versions and the pending list: freed together.
    arena: std.heap.ArenaAllocator,
    opts: Options,
    ns: u64,
    table_id: u32,
    nodes: []Node,
    /// The nodes not yet done, in key order. Rebuilt from itself
    /// after every source, so each source sees only what the ones
    /// above it left open; empty ends the walk.
    pending: std.ArrayList(*Node),
    /// Scratch for `probeTable`: the nodes one table is seeked for,
    /// and the blocks those seeks land in.
    accepted: std.ArrayList(*Node) = .empty,
    blocks: std.ArrayList(usize) = .empty,

    pub const Options = struct {
        /// Versions above this are not visible. Must be at or above
        /// `Forest.globalMinRead` for the entry points that check it.
        read_ts: u64 = std.math.maxInt(u64),
        priority: vfs.Priority = .p0,
        /// Rows only: a txn's private writes, the first source (src 0
        /// of the memtable merge, as in ScanOptions.pending), so a
        /// pending write wins over every committed version and a
        /// pending tombstone reads as absent with no fallback. Not
        /// for postings: their pending ops are per-egid entries the
        /// sql layer overlays itself (Txn.overlayPostings).
        pending: ?*_skl.SkipList = null,
        /// Rows only: hand back a tombstone version as an item
        /// (getRaw).
        include_deleted: bool = false,
    };

    /// One version of a key as found in a source, borrowed: `value`
    /// points into the block or arena `pin` holds. Prepended as
    /// found, and sources are walked newest first, so a node's list
    /// reads OLDEST first: the head is the state or tombstone when
    /// one was found, the rest are the deltas in the order a fold
    /// applies them.
    pub const Version = struct {
        ts: u64,
        meta: Meta,
        value: []const u8,
        pin: Pin,
        next: ?*Version,
    };

    /// One key of the batch and the versions found for it so far.
    pub const Node = struct {
        key: []const u8,
        /// bloomHash(key), once per key: every table probe reuses it.
        hash: u64,
        /// Oldest taken first (see Version). Null until a source has
        /// one.
        head: ?*Version = null,

        /// Versions above this are not wanted from the next source:
        /// the head's ts - 1 once something was taken, so the next
        /// source down starts strictly below what was consumed (a
        /// version that re-surfaces at a lower source after a flush
        /// or compaction arrives above it and is skipped); read_ts
        /// before that.
        pub fn maxTs(self: *const Node, read_ts: u64) u64 {
            const head = self.head orelse return read_ts;
            // Real HLC timestamps are never 0: no wrap.
            std.debug.assert(head.ts > 0);
            return head.ts - 1;
        }

        /// Nothing more is wanted: the head is a state or a tombstone,
        /// which closes the chain. A delta keeps it open.
        pub fn done(self: *const Node) bool {
            const head = self.head orelse return false;
            return !head.meta.isDelta();
        }

        fn lessByKey(_: void, a: Node, b: Node) bool {
            return std.mem.order(u8, a.key, b.key) == .lt;
        }
    };

    pub fn init(
        allocator: std.mem.Allocator,
        ns: u64,
        table_id: u32,
        keys: []const String,
        opts: Options,
    ) !MultiGet {
        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        const arloc = arena.allocator();

        const all = try arloc.alloc(Node, keys.len);
        for (keys, all) |key, *node| {
            node.* = .{
                .key = key,
                .hash = (Key{ .primary = key }).bloomHash(),
            };
        }
        std.mem.sort(Node, all, {}, Node.lessByKey);
        // Collapse duplicates: the sort made them adjacent.
        var kept: usize = 0;
        for (all) |node| {
            if (kept > 0 and std.mem.eql(u8, all[kept - 1].key, node.key)) {
                continue;
            }
            all[kept] = node;
            kept += 1;
        }
        const nodes = all[0..kept];

        var pending: std.ArrayList(*Node) = .empty;
        try pending.ensureTotalCapacityPrecise(arloc, nodes.len);
        for (nodes) |*node| pending.appendAssumeCapacity(node);

        return .{
            .allocator = allocator,
            .arena = arena,
            .opts = opts,
            .ns = ns,
            .table_id = table_id,
            .nodes = nodes,
            .pending = pending,
        };
    }

    /// Drops every pin and every borrow handed out.
    pub fn deinit(self: *MultiGet) void {
        for (self.nodes) |node| {
            var cur = node.head;
            while (cur) |ver| : (cur = ver.next) ver.pin.release();
        }
        self.arena.deinit();
    }

    /// Row result: the newest version of `key` at or below read_ts,
    /// null if absent or deleted (unless include_deleted). Its key is
    /// `key` at that version's timestamp, its value a borrow valid
    /// until deinit. `key` must be one of the batch's keys. A row's
    /// chain is one version; for a chain of deltas this is the newest
    /// delta, the version `get` has always returned.
    pub fn item(self: *const MultiGet, key: []const u8) ?Item {
        const node = self.find(key);
        var ver = node.head orelse return null;
        while (ver.next) |next| ver = next;
        if (ver.meta.isDeleted() and !self.opts.include_deleted) return null;
        return .{
            .key = self.keyAt(node, ver.ts),
            .value = ver.value,
            .meta = ver.meta,
        };
    }

    /// Posting result: the chain of `key` folded into a bitmap owned
    /// by `alloc`. Absent, tombstoned and empty all fold to an empty
    /// bitmap. `key` must be one of the batch's keys. Fails with
    /// NotRoaring when a version of the chain is not roaring (a
    /// tombstone's encoding is not looked at, as in compaction), and
    /// CorruptPosting on undecodable delta bytes.
    pub fn bitmap(
        self: *const MultiGet,
        alloc: std.mem.Allocator,
        key: []const u8,
    ) !zroar.Bitmap {
        // Pending posting ops are the sql layer's to overlay.
        std.debug.assert(self.opts.pending == null);
        const node = self.find(key);
        var cur = node.head;
        var terminal: ?Item = null;
        if (cur) |head| {
            // A tombstone's encoding is not looked at, as in compaction.
            if (!head.meta.isDeleted() and !head.meta.isRoaring()) {
                return error.NotRoaring;
            }
            // A state or a tombstone is the base the deltas fold onto.
            // A delta at the head means the chain has no base here and
            // folds from empty.
            if (!head.meta.isDelta()) {
                terminal = .{
                    .key = self.keyAt(node, head.ts),
                    .value = head.value,
                    .meta = head.meta,
                };
                cur = head.next;
            }
        }
        var bm = try baseBitmap(alloc, terminal, null);
        errdefer bm.deinit();

        while (cur) |ver| : (cur = ver.next) {
            if (!ver.meta.isRoaring()) return error.NotRoaring;
            try applyDelta(&bm, ver.value);
        }
        return bm;
    }

    /// Whether `key`'s chain has any version at all (rollupBitmap's
    /// null result).
    pub fn hasVersions(self: *const MultiGet, key: []const u8) bool {
        return self.find(key).head != null;
    }

    /// The node of a key that was passed to init.
    fn find(self: *const MultiGet, key: []const u8) *Node {
        const idx = utils.search(Node, self.nodes, key, struct {
            fn f(k: []const u8, node: Node) bool {
                return std.mem.order(u8, node.key, k) != .lt;
            }
        }.f);
        std.debug.assert(idx < self.nodes.len);
        std.debug.assert(std.mem.eql(u8, self.nodes[idx].key, key));
        return &self.nodes[idx];
    }

    /// `node`'s key as a full Key at `ts`.
    pub fn keyAt(self: *const MultiGet, node: *const Node, ts: u64) Key {
        return .{
            .namespace = self.ns,
            .table_id = self.table_id,
            .primary = node.key,
            .timestamp = ts,
        };
    }

    /// Drop the nodes that are done from `pending`.
    pub fn compactPending(pending: *std.ArrayList(*Node)) void {
        var kept: usize = 0;
        for (pending.items) |node| {
            if (node.done()) continue;
            pending.items[kept] = node;
            kept += 1;
        }
        pending.items.len = kept;
    }

    /// One merge over the sources above the trees (the txn's pending
    /// writes and the memtables), seeked once per pending node.
    /// Memtable seeks cost no I/O, and the merge settles the order
    /// across memtables.
    pub fn drainMerge(self: *MultiGet, merge: *Iterator) !void {
        for (self.pending.items) |node| try self.takeVersions(node, merge);
        compactPending(&self.pending);
    }

    /// The level loop over one tree for `run`, pending nodes in key
    /// order that this tree covers. Per level from `from_level`,
    /// under one shared hold of `tree.mu`, pin the tables that can
    /// hold the nodes still open: on L0 every table, since their
    /// ranges overlap, walked newest to oldest; deeper, the tables
    /// overlapping [first open, last open]. Then per table, with the
    /// hold released, probe its share (see `probeTable`). A level
    /// with nothing open ends the walk.
    pub fn walkTree(
        self: *MultiGet,
        tree: *Tree,
        run: []const *Node,
        from_level: usize,
    ) !void {
        const arloc = self.arena.allocator();
        var pending: std.ArrayList(*Node) = .empty;
        try pending.appendSlice(arloc, run);

        var lvl = from_level;
        while (lvl < tree.opts.max_levels) : (lvl += 1) {
            compactPending(&pending);
            const open = pending.items;
            if (open.len == 0) return;

            const range = KeyRange{
                .left = self.keyAt(open[0], std.math.maxInt(u64)),
                .right = self.keyAt(open[open.len - 1], 0),
            };
            const tables = blk: {
                const held = tree.mu.hold(.shared, @src());
                defer held.release();
                break :blk try tree.levels[lvl].overlappingTables(
                    held,
                    self.allocator,
                    range,
                );
            };
            defer Level.releaseTables(self.allocator, tables);

            if (lvl == 0) {
                // L0 sorts by id ascending (newest last): walk in
                // reverse so the newest table settles a key first.
                var ri = tables.len;
                while (ri > 0) {
                    ri -= 1;
                    try self.probeTable(tables[ri].table, open);
                }
            } else {
                for (tables) |e| try self.probeTable(e.table, open);
            }
        }
    }

    /// One table's share of the open nodes: those inside
    /// [smallest, biggest] (a binary search on the list) whose hash
    /// the table's filter accepts, skipping any a newer L0 table
    /// settled this level. None → the table is not opened. Else the
    /// blocks their seeks land in are prefetched, and one iterator is
    /// seeked per node in key order, so a run of keys in one block is
    /// one in-block search each.
    fn probeTable(
        self: *MultiGet,
        tbl: *Table,
        nodes: []const *Node,
    ) !void {
        const lo = utils.search(*Node, nodes, tbl.smallest.primary, struct {
            fn f(smallest: []const u8, node: *Node) bool {
                return std.mem.order(u8, node.key, smallest) != .lt;
            }
        }.f);
        const hi = utils.search(*Node, nodes, tbl.biggest.primary, struct {
            fn f(biggest: []const u8, node: *Node) bool {
                return std.mem.order(u8, node.key, biggest) == .gt;
            }
        }.f);

        const arloc = self.arena.allocator();
        self.accepted.clearRetainingCapacity();
        for (nodes[lo..hi]) |node| {
            if (node.done()) continue;
            if (!tbl.containsHash(node.hash)) continue;
            try self.accepted.append(arloc, node);
        }
        if (self.accepted.items.len == 0) return;
        try self.prefetch(tbl);

        var it: Iterator = .{ .table = TableIterator.init(
            self.allocator,
            tbl,
            self.opts.priority,
        ) };
        defer it.deinit();
        for (self.accepted.items) |node| {
            try self.takeVersions(node, &it);
        }
    }

    /// Before a table's seeks: the blocks they will land in, read in
    /// one batch into the block cache when there is one, so each seek
    /// then hits. Keys ascend, so the block indices do too and a repeat
    /// is the previous entry.
    fn prefetch(self: *MultiGet, tbl: *Table) !void {
        const cache = _table.table.block_cache orelse return;
        const arloc = self.arena.allocator();
        self.blocks.clearRetainingCapacity();
        for (self.accepted.items) |node| {
            const at = self.keyAt(node, node.maxTs(self.opts.read_ts));
            const idx = tbl.blockFor(at) orelse return;
            if (self.blocks.items.len > 0 and self.blocks.getLast() == idx) {
                continue;
            }
            try self.blocks.append(arloc, idx);
        }
        _ = try tbl.prefetchBlocks(
            cache,
            self.allocator,
            self.blocks.items,
            self.opts.priority,
        );
    }

    /// Take `node`'s versions from one positioned source, newest
    /// first, until the node is done or the source has no more of
    /// them: the seek is bounded to the key's own run of versions
    /// (`Key.end`), which is load-bearing for the memtable merge, whose
    /// sources span the namespace.
    fn takeVersions(self: *MultiGet, node: *Node, it: *Iterator) !void {
        const read_ts = self.opts.read_ts;
        const start = self.keyAt(node, node.maxTs(read_ts));

        try it.seekRange(start, start.end());
        while (it.itemRef()) |found| {
            // The seek starts at the key at maxTs and versions sort
            // newest first, so nothing newer can be yielded.
            std.debug.assert(found.key.equalPrimary(start));
            std.debug.assert(found.key.timestamp <= node.maxTs(read_ts));
            try self.take(node, found, it.currentPin().?);
            if (node.done()) return;
            try it.next();
        }
    }

    /// Prepend one version, pinned.
    fn take(self: *MultiGet, node: *Node, found: Item, pin: Pin) !void {
        const ver = try self.arena.allocator().create(Version);
        ver.* = .{
            .ts = found.key.timestamp,
            .meta = found.meta,
            .value = found.value,
            .pin = pin.acquire(),
            .next = node.head,
        };
        node.head = ver;
    }
};

/// The bitmap a roaring chain folds from: empty for a tombstone
/// terminal, the state's bitmap for a state terminal (copied: SST
/// values sit at arbitrary offsets, so zroar's aligned zero-copy path
/// is not usable), else `external` cloned (compaction's read-below),
/// else empty. Reads (`MultiGet.bitmap`) and compaction
/// (`VersionTracker.foldBufferedToBitmap`) both fold through this
/// and `applyDelta`, which is what keeps rollup read-invariant.
pub fn baseBitmap(
    alloc: std.mem.Allocator,
    terminal: ?Item,
    external: ?*const zroar.Bitmap,
) !zroar.Bitmap {
    if (terminal) |term| {
        if (term.meta.isDeleted()) return zroar.Bitmap.init(alloc);
        return zroar.Bitmap.fromBufferCopy(alloc, term.value);
    }
    if (external) |eb| return eb.clone();
    return zroar.Bitmap.init(alloc);
}

/// Apply one roaring delta: removes, then adds. The bytes get an O(1)
/// structural check — without it a truncated element region decodes
/// as an empty list — but content contracts were enforced at write
/// time (validateDelta). Deltas are applied oldest to newest.
pub fn applyDelta(bm: *zroar.Bitmap, delta: []const u8) !void {
    const r = DeltaReader.init(delta, 0) catch return error.CorruptPosting;
    r.verify() catch return error.CorruptPosting;
    var j: u32 = 0;
    while (j < r.removesLen()) : (j += 1)
        _ = bm.remove(r.removesAt(j) catch unreachable);
    j = 0;
    while (j < r.addsLen()) : (j += 1)
        _ = try bm.set(r.addsAt(j) catch unreachable);
}

/// How much of a key's version history a scan should surface.
///
/// The merge iterator hands us every version of every primary, newest
/// first; these decide which of them reach the callback. The defaults
/// make a scan agree with `get`: the current value of each live key, and
/// nothing for a deleted one.
pub const ScanOptions = struct {
    /// Stop after handing this many items to the callback. Bounds what the
    /// caller RECEIVES, not what the scan walks past: versions skipped by
    /// the filters below are free. 0 returns nothing. The default is
    /// unbounded — the whole range.
    max_rows: u64 = std.math.maxInt(u64),
    /// Max versions to emit per primary, newest first. 1 (the default) is
    /// "just the current value". Clamped up to 1 — there's no use for a
    /// scan that returns zero versions of every key, and `max_rows = 0`
    /// already says "return nothing".
    num_versions: u32 = 1,
    /// When false, a primary stops emitting at its first tombstone: the
    /// tombstone is not emitted, and neither is anything older than it,
    /// regardless of `num_versions`. That is what makes a deleted key look
    /// absent.
    ///
    /// When true, tombstones are emitted like any other version and older
    /// versions behind them keep coming, up to `num_versions`. For callers
    /// that need deletions as data — replication, repair, debugging.
    include_deleted: bool = false,
    /// Report the world as of this timestamp: versions above it are not
    /// visible and are skipped as though they had not been written. The
    /// default, maxInt, means "latest".
    ///
    /// This is the same view `get` gives for one key, so a scan at
    /// `read_ts` and a `get` at `read_ts` agree — including on deletion: a
    /// tombstone ABOVE read_ts hasn't happened yet from here, so it does
    /// not hide the key, and the live version under it is what surfaces.
    ///
    /// Must be at or above `global_min_read`, like any other read.
    read_ts: u64 = std.math.maxInt(u64),
    /// Only what changed after this timestamp: versions at or below it
    /// are skipped, so a key is emitted only when its newest visible
    /// version is above it, and an SST whose every version is at or
    /// below it (`TableIndex.max_version`) is not opened at all. The
    /// default, 0, skips nothing. With `include_deleted`, a key deleted
    /// after `min_ts` is emitted as its tombstone — the shape of a
    /// "what changed since" pass (CREATE INDEX's merge of the rows
    /// written during its backfill). Memtables are always opened.
    min_ts: u64 = 0,
    /// I/O priority for the SST reads this scan issues (see vfs). `.p0` is
    /// the foreground/interactive class every current caller wants;
    /// background sweeps should pick a lower one so they queue behind
    /// user-facing reads.
    priority: vfs.Priority = .p0,
    /// A transaction's private pending-write skiplist, merged in as
    /// the highest-priority source (src 0) so the txn sees its own
    /// uncommitted writes. Its entries carry ts = read_ts, which the
    /// visibility filter admits and which wins equal-key ties against
    /// committed versions (lower src wins in the merge). null = none.
    pending: ?*_skl.SkipList = null,
    /// Cursor only. Off: an item's value is a borrow valid until the
    /// following `next`. On: the cursor holds the storage behind
    /// every value it hands out until `Cursor.releasePins`, so a
    /// caller can keep a whole batch of borrows instead of copying
    /// them. Costs one refcount pair per block or memtable arena
    /// walked, and keeps those blocks resident until released.
    pin_values: bool = false,
};

/// Forward scan over a table-limited `scan_range` — a KeyRange run through
/// `KeyRange.limitToTable`, so its bounds are full keys confined to one
/// `(namespace, table_id)` (which ride the `left` bound). That confinement
/// keeps the scan from spilling across the namespace-wide memtable into an
/// adjacent table.
///
/// `opts` carries everything else: how many items to return, the view
/// (as-of timestamp, versions per key, deletions as data or absence), and
/// the read priority — see `ScanOptions`. The defaults emit every live
/// key's current value once and hide deleted keys, matching `get`.
///
/// `scan_range`'s own timestamps only position the seek — `opts.read_ts`
/// is what decides visibility.
///
/// A loop over a `Cursor`: the callback sees each item the cursor hands
/// out, in order, and stops the scan by returning false. Returns how many
/// items reached the callback (the one it said false to included). The
/// item is a borrow — see `Cursor.next`. Instantiated per ctx type, but
/// the walk itself lives in `Cursor` and compiles once.
pub fn scan(
    self: *Forest,
    allocator: std.mem.Allocator,
    scan_range: KeyRange,
    opts: ScanOptions,
    ctx: anytype,
    comptime cb: fn (@TypeOf(ctx), Item) anyerror!bool,
) !u64 {
    var cur = try self.openCursor(allocator, scan_range, opts);
    defer cur.deinit();
    while (try cur.next()) |item| {
        if (!try cb(ctx, item)) break;
    }
    return cur.emitted;
}

/// Open a `Cursor` over `scan_range` — the walk `scan` does, handed out
/// one item per `next` so the caller sets the pace. Same arguments and
/// guards as `scan`: a `read_ts` below `globalMinRead` is refused here,
/// except when `max_rows` is 0, which opens an already-exhausted cursor
/// without looking. Nothing is read or pinned until the first `next`.
pub fn openCursor(
    self: *Forest,
    allocator: std.mem.Allocator,
    scan_range: KeyRange,
    opts: ScanOptions,
) !Cursor {
    if (opts.max_rows != 0 and opts.read_ts < self.globalMinRead()) {
        return error.ReadBelowMinRead;
    }
    return .{
        .forest = self,
        .allocator = allocator,
        .arena = std.heap.ArenaAllocator.init(allocator),
        .key_range = scan_range,
        .opts = opts,
        .want_versions = @max(opts.num_versions, 1),
        // The range is already table-limited; ns + table_id ride its left
        // bound.
        .ns = scan_range.left.namespace,
        .table_id = scan_range.left.table_id,
    };
}

/// A scan as a pull handle: `next` returns the next visible item, null
/// once the range is exhausted. Same walk, filters, and options as
/// `scan` — that function is a loop over one of these. What the cursor
/// adds is pacing: a caller can take some items, return to whoever
/// called it, and come back later, with the open tree's merge iterator
/// and its refs (SST tables, memtable arenas, the pending skiplist)
/// held in between. Holding across a wait is safe — the refs keep that
/// storage alive even if a flush or compaction has since replaced it in
/// the manifest — so the cost is residency, not correctness.
///
/// One tree at a time: look up the tree owning `key_range.left`,
/// snapshot its range, open a merge over the intersection (with a fresh
/// memtable iter snapshot), drain it, then move `key_range.left` past
/// the snapshotted right boundary and re-look-up. Re-looking-up per hop
/// keeps the scan safe against splits firing mid-scan. Per-hop memtable
/// snapshots also pick up newly-flushed data.
///
/// One fiber drives a cursor. `deinit` on every path — an abandoned
/// cursor pins its tables until then.
pub const Cursor = struct {
    forest: *Forest,
    allocator: std.mem.Allocator,
    /// Per-cursor arena: memtable iterator slices and each visited
    /// tree's duped range live exactly as long as the cursor (the tree
    /// ref is released once the hop's merge is open, and a split
    /// retires the tree, so the range cannot borrow from it).
    arena: std.heap.ArenaAllocator,
    /// The window still to visit: the scan's range at open, its left
    /// bound advanced past each drained tree's snapshotted right edge.
    key_range: KeyRange,
    opts: ScanOptions,
    /// `opts.num_versions` clamped up to 1 (see ScanOptions).
    want_versions: u32,
    ns: u64,
    table_id: u32,
    /// The open tree's merge; null between trees (a hop is pending)
    /// and once `done`.
    merge: ?merge_iterator.MergeIterator = null,
    /// The open tree's range as snapshotted at its hop (tree.key_range
    /// stays in sync with the manifest via applySplit).
    tree_range: KeyRange = .{},
    /// The merge is parked on an item the caller already has. Advancing
    /// right after the hand-out would overwrite the key buffer and could
    /// drop the block the value lives in, so the step past it is owed to
    /// the following `next`. False only before the first hand-out from a
    /// freshly opened tree.
    next_pending: bool = false,
    /// With `opts.pin_values`: a hold on the storage behind every
    /// value handed out since the last `releasePins` — one entry per
    /// block or memtable arena, since consecutive items share one —
    /// so those borrows stay valid past the following `next`.
    pins: std.ArrayList(Pin) = .empty,
    /// No tree left to visit.
    done: bool = false,
    /// Per-primary emit state. A primary's versions arrive contiguously,
    /// newest first, so this only ever tracks one run at a time.
    ///
    /// The state deliberately spans tree hops rather than resetting per
    /// tree: a primary can't straddle two trees (findSplitKey pins the
    /// split above every version of its key), so a hop always lands on
    /// a new primary anyway — and if that ever stops holding, carrying
    /// the state is the behaviour that stays correct.
    ///
    /// `cur_primary` owns its bytes: item.key.primary points into the
    /// block iterator's key_buf, which the next merge.next() overwrites.
    /// Empty doubles as "no run yet" — a stored primary is never empty
    /// (an empty primary is the open-bound sentinel, and Key.marshalTo
    /// encodes it as zero bytes), so the first item can never compare
    /// equal to it.
    cur_primary: std.ArrayList(u8) = .empty,
    versions_emitted: u32 = 0,
    skip_versions: bool = false,
    /// Items handed out so far — what `scan` returns. Bounded by
    /// `opts.max_rows`; versions the filters skip are free.
    emitted: u64 = 0,

    /// Releases the open merge (table + skiplist refs) and the cursor's
    /// memory. Safe after exhaustion, and after an error from `next`.
    pub fn deinit(self: *Cursor) void {
        for (self.pins.items) |pin| pin.release();
        self.pins.deinit(self.allocator);
        if (self.merge) |*m| m.deinit();
        self.merge = null;
        self.cur_primary.deinit(self.allocator);
        self.arena.deinit();
    }

    /// The next item the options admit, or null when the scan is done
    /// (`max_rows` reached, or the range walked to its end). Null is
    /// sticky.
    ///
    /// The returned Item is a BORROW valid until the following `next`
    /// or `deinit`: its key points into a source's key buffer, its
    /// value into a block or arena the source holds a ref on, and the
    /// advance overwrites the one and may release the other. Copy what
    /// must outlive the call — except the value under
    /// `opts.pin_values`, which stays valid until `releasePins`.
    pub fn next(self: *Cursor) !?Item {
        while (self.emitted < self.opts.max_rows) {
            if (self.merge == null) {
                if (self.done) return null;

                try self.openNextTree();
                // nothing covers key_range.left
                if (self.merge == null) return null;
            }
            const merge = &self.merge.?;
            if (self.next_pending) {
                self.next_pending = false;
                try merge.next();
            }
            if (try self.nextVisible(merge)) |item| {
                // The merge stays parked on `item` so the borrow holds;
                // the step past it is owed to the next call.
                self.next_pending = true;
                if (self.opts.pin_values) try self.pinCurrent(merge);
                return item;
            }

            // This tree is drained: close it and move the window past
            // its snapshotted right edge. Empty primary = unbounded
            // right (rightmost tree) → done.
            merge.deinit();
            self.merge = null;
            if (self.tree_range.right.primary.len == 0) {
                self.done = true;
                return null;
            }
            self.key_range.left = self.tree_range.right;
        }
        return null;
    }

    /// Hold the storage behind the parked item's value, unless the
    /// last pin already covers it (Pin's doc has the stability
    /// guarantee a hold gives).
    fn pinCurrent(
        self: *Cursor,
        merge: *const merge_iterator.MergeIterator,
    ) !void {
        const pin = merge.currentPin().?;
        const covered = self.pins.items.len > 0 and
            std.meta.eql(self.pins.getLast(), pin);
        if (!covered) try self.pins.append(self.allocator, pin.acquire());
    }

    /// The caller is done with the values handed out so far: drop
    /// their holds — except the item most recently handed out, which
    /// the merge is still parked on and the caller may still be
    /// holding; it stays pinned. No-op unless `opts.pin_values`.
    pub fn releasePins(self: *Cursor) void {
        for (self.pins.items) |pin| pin.release();
        self.pins.clearRetainingCapacity();
        if (self.opts.pin_values and self.next_pending) {
            // That item's own pin sat in the list, so the capacity
            // retained covers this one.
            const pin = self.merge.?.currentPin().?;
            self.pins.appendAssumeCapacity(pin.acquire());
        }
    }

    /// Look up the tree owning `key_range.left`, snapshot its range, and
    /// open a merge over its intersection with the window. Redoing the
    /// lookup per hop keeps the scan safe against splits firing between
    /// hops — we always take whichever tree currently covers the next
    /// key. Leaves `merge` null and sets `done` when no tree does. An
    /// empty intersection just opens an empty merge; `next` then hops
    /// or finishes as usual.
    ///
    /// The tree is held by ref (`TableTrees.getTree`) across the range
    /// snapshot and the table selection. A split never mutates a live
    /// tree — it installs fresh objects and retires this one, which
    /// stays whole for us — so the range copied and the tables pinned
    /// describe the same instant, and a later split just moves tables
    /// we already hold. The ref is released before the seek, the part
    /// that reads blocks.
    fn openNextTree(self: *Cursor) !void {
        const a = self.arena.allocator();

        const visit_range = blk: {
            const trees = self.forest.getTable(self.ns, self.table_id) orelse {
                self.done = true;
                return;
            };
            const tree = trees.getTreeByKey(self.key_range.left) orelse {
                self.done = true;
                return;
            };
            defer tree.release();
            self.tree_range = try tree.key_range.dupe(a);
            const visit_range = self.key_range.intersect(self.tree_range);

            // Memtable iters come from the cursor arena so they live
            // exactly as long as the merge. (Memtables.getIterators
            // returns a fresh slice; freeing it through the arena at
            // deinit is fine.) The txn's pending skiplist rides in as
            // src 0 — the merge's equal-key tie-break.
            const srcs = try self.forest.memtables.getIterators(
                a,
                self.opts.pending,
            );
            self.merge = try tree.openMerge(
                a,
                visit_range,
                self.opts.priority,
                self.opts.min_ts,
                srcs,
            );
            break :blk visit_range;
        };

        // Position the sources. On error the merge stays set, so
        // `deinit` still releases everything it pinned.
        try self.merge.?.seekRange(visit_range.left, visit_range.right);
        // A hop only happens after the previous merge was drained, and
        // draining begins by paying the owed advance — so none is owed.
        std.debug.assert(!self.next_pending);
    }

    /// Walk the open merge to the next item the options admit and
    /// return it, leaving the merge positioned ON it. Null when this
    /// tree is drained.
    fn nextVisible(self: *Cursor, merge: *merge_iterator.MergeIterator) !?Item {
        while (merge.itemRef()) |item| {
            // Not written yet, as far as this scan is concerned.
            // Filtered before everything below on purpose: before the
            // tombstone rule, so a delete above read_ts can't hide a
            // key that is still live at read_ts; and before the
            // version cap, so invisible versions don't consume it.
            if (item.key.timestamp > self.opts.read_ts) {
                try merge.next();
                continue;
            }
            // Unchanged since min_ts, as far as this scan is concerned:
            // versions arrive newest first, so a key whose newest
            // visible version fails this never starts, and one that
            // changed emits only what changed.
            if (item.key.timestamp <= self.opts.min_ts) {
                try merge.next();
                continue;
            }

            if (!item.key.equalPrimary(.{
                .primary = self.cur_primary.items,
            })) {
                self.cur_primary.clearRetainingCapacity();
                try self.cur_primary.appendSlice(
                    self.allocator,
                    item.key.primary,
                );
                self.versions_emitted = 0;
                self.skip_versions = false;
            }

            // Skipped versions don't count against max_rows — that
            // bounds what the caller receives, not what we walk past.
            if (self.skip_versions or
                self.versions_emitted >= self.want_versions)
            {
                // TODO: Add a nextKey in underlying iterators.
                try merge.next();
                continue;
            }

            if (item.meta.isDeleted() and !self.opts.include_deleted) {
                // Deleted as of this version: suppress the tombstone
                // and everything older behind it.
                self.skip_versions = true;
                try merge.next();
                continue;
            }

            self.emitted += 1;
            self.versions_emitted += 1;
            return item;
        }
        return null;
    }
};

/// The largest stored key for (namespace, table_id), or null if the table
/// has no keys. Assumes a monotonically-increasing row id (SQL's egid), so
/// the max lives in the rightmost tree and, within it, the rightmost SST of
/// each level: last-by-id in L0 (newest flush = highest ids) or last-by-
/// smallest in L≥1 (highest range). Step 1 takes the max of those SSTs'
/// in-memory `biggest` keys — O(#levels), no I/O. Step 2 scans from that
/// floor and keeps the highest key: the SSTs top out at floor, so anything
/// beyond it is an unflushed memtable row. If the scan surfaces nothing
/// higher, the floor stands. The returned Key's `primary` is owned by
/// `allocator` (free it). Callers derive a table's next id from it without
/// persisting a counter — so the max is the largest STORED key, deleted
/// or not: the memtable scan counts tombstones (include_deleted) and step
/// 1's SST `biggest` is physical, so a deleted current-max still seeds the
/// next id past itself and an id is never re-minted onto a tombstoned row.
pub fn maxKey(
    self: *Forest,
    allocator: std.mem.Allocator,
    ns: u64,
    table_id: u32,
) !?Key {
    // Step 1: floor = max `biggest` over the rightmost SST of each level in
    // the rightmost tree. Owned by `allocator`, or null if no SSTs.
    var floor: ?Key = null;
    errdefer if (floor) |f| f.free(allocator);
    {
        const trees = self.getTable(ns, table_id) orelse return null;
        const tree = trees.getRightmostTree() orelse return null;
        defer tree.release();
        var best_sst: Key = .{};
        const held = tree.mu.hold(.shared, @src());
        defer held.release();
        for (tree.levels) |*lv| {
            const items = lv.entries(held);
            if (items.len == 0) continue;

            const b = items[items.len - 1].table.biggest; // rightmost
            if (b.primary.len == 0) continue;

            if (best_sst.primary.len == 0 or
                b.orderWithoutTs(best_sst) == .gt) best_sst = b;
        }
        // Dupe out from under the tables — they may close once locks drop.
        if (best_sst.primary.len > 0) floor = try best_sst.dupe(allocator);
    }

    // Step 2: scan from floor (or the table start if there's no floor) and
    // keep the highest key. `best` is a single reused buffer holding the
    // latest (thus, at end, the largest) key's marshaled bytes — no
    // per-row alloc churn. `floor` backs the left bound, so it outlives the
    // scan.
    const Ctx = struct {
        allocator: std.mem.Allocator,
        best: std.ArrayList(u8) = .empty,
        fn emit(c: *@This(), item: Item) anyerror!bool {
            c.best.clearRetainingCapacity();
            _ = try item.key.marshalTo(c.allocator, &c.best);
            return true;
        }
    };
    var ctx = Ctx{ .allocator = allocator };
    defer ctx.best.deinit(allocator);
    // [floor, table end): floor (if any) is an in-table key we keep; no
    // floor limits to the table start. limitToTable confines the memtable.
    // include_deleted: a tombstoned max is still a stored key, so it must
    // count — else its id gets re-minted onto a deleted row (see doc).
    const scan_range = (KeyRange{
        .left = floor orelse Key{},
    }).limitToTable(ns, table_id);
    _ = try self.scan(
        allocator,
        scan_range,
        .{ .include_deleted = true },
        &ctx,
        Ctx.emit,
    );

    // A memtable row above floor wins; otherwise floor stands (or null if
    // the table had no SSTs and no rows).
    if (ctx.best.items.len > 0) {
        const k = try Key.initAlloc(allocator, ctx.best.items);
        if (floor) |f| f.free(allocator);
        return k;
    }
    return floor;
}
