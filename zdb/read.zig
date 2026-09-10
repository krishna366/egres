//! Forest's read paths: point lookups (`get`, `getRaw`, `getBitmap`), the
//! range scan and its `Cursor`, `maxKey`, and the human-readable stats
//! dump. Every function here takes a `*Forest` and leaves it unchanged;
//! `Forest` aliases each declaration back in, so callers keep using
//! method syntax.

const std = @import("std");
const vfs = @import("vfs");
const zroar = @import("zroar");
const _level = @import("level.zig");
const _table = @import("table.zig");
const _tree = @import("tree.zig");
const _skl = @import("skl/skiplist.zig");

const Forest = @import("forest.zig").Forest;
const Key = @import("kv.zig").Key;
const TreeKey = @import("kv.zig").TreeKey;
const KeyRange = _level.KeyRange;
const Item = _table.table.Item;
const merge_iterator = _table.merge_iterator;
const Pin = merge_iterator.Pin;

/// Get a value by key. Checks memtables first (current, then immutables
/// newest-to-oldest), then routes to the correct tree.
/// Returned Item is owned by the caller; call item.free(allocator) when done.
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

    const found = try self.getRaw(allocator, search_key, priority);
    // Every layer stops at its first hit, and versions sort newest-first,
    // so `found` is the newest version at or below the read timestamp. If
    // that version is a tombstone, the key is deleted as of this read —
    // older versions behind it are shadowed and must not surface.
    if (found) |item| {
        if (item.meta.isDeleted()) {
            item.free(allocator);
            return null;
        }
    }
    return found;
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
    if (try self.memtables.get(allocator, search_key)) |item| {
        return item;
    }

    const trees = self.getTable(
        search_key.namespace,
        search_key.table_id,
    ) orelse return null;
    const tree = trees.getTreeByKey(search_key) orelse return null;
    defer tree.release();
    return tree.get(search_key, priority);
}

/// The posting bitmap for `search_key`'s primary as of
/// `search_key.timestamp` (the read_ts — same convention as `get`).
/// A lazy top-down walk via Tree.rollupBitmap: one merge over the
/// memtable sources, then the owning tree's levels, stopping at
/// the first terminal (a state or a tombstone). After compaction
/// rollup the newest version is usually already a state, so the
/// walk rarely descends past the level that holds it.
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

    // Memtable sources BEFORE the tree lookup: data flushed in
    // between is then visible in both our held snapshot and L0,
    // and rollupBitmap's descending bound skips the L0 duplicate.
    // The reverse order could miss it in both. getIterators
    // acquires refs, so the arenas outlive a concurrent flush.
    // One sorted merge over them (rather than sequential drains):
    // memtable seeks cost no I/O, and the merge is indifferent to
    // source order and cross-memtable ordering subtleties.
    const srcs = try self.memtables.getIterators(allocator, null);
    var mems_itr = merge_iterator.MergeIterator.init(allocator, srcs);
    defer {
        mems_itr.deinit(); // releases each source's skiplist ref
        allocator.free(srcs);
    }

    const trees = self.getTable(
        search_key.namespace,
        search_key.table_id,
    ) orelse return zroar.Bitmap.init(allocator);
    const t = trees.getTreeByKey(search_key) orelse
        return zroar.Bitmap.init(allocator);
    defer t.release();

    const bm = try t.rollupBitmap(
        allocator,
        search_key,
        priority,
        &mems_itr,
        0,
    );
    return bm orelse zroar.Bitmap.init(allocator);
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

// -----------------------------------------------------------------------
// Stats dump
// -----------------------------------------------------------------------

/// What slice of the forest the caller wants to see. Default is a
/// top-level summary that aggregates across all trees.
pub const StatsScope = union(enum) {
    forest,
    tree: TreeKey,
    namespace: u64,
};

pub const StatsOpts = struct {
    scope: StatsScope = .forest,
};

/// writeStats produces a hierarchical, human-readable text dump of
/// current LSM state to `w`. Unlike the Prometheus metrics endpoint,
/// this is for ops drill-down: per-tree shape, per-level sizes,
/// pending compactions. Snapshot is best-effort — locks are taken
/// briefly per section, so a concurrent flush/compaction may show
/// itself half-applied. Acceptable for a debug endpoint.
pub fn writeStats(self: *Forest, w: *std.Io.Writer, opts: StatsOpts) !void {
    switch (opts.scope) {
        .forest => try self.writeForestStats(w),
        .tree => |id| try self.writeTreeStats(w, id),
        .namespace => |ns| try self.writeNamespaceStats(w, ns),
    }
}

pub fn writeForestStats(self: *Forest, w: *std.Io.Writer) !void {
    const mt = self.memtables.stats();
    try w.print("forest:\n", .{});
    try w.print("  memtables: active=", .{});
    try writeBytes(w, mt.active_bytes);
    try w.print(" immutable={d} (", .{mt.immutable_count});
    try writeBytes(w, mt.immutable_bytes);
    try w.print(" pending flush)\n", .{});
    try w.print("  flush_pending: {d}\n", .{mt.immutable_count});

    // Per-level aggregate (sum + max-with-tree).
    const max_levels = self.opts.max_levels;
    const Agg = struct {
        tables_total: u64 = 0,
        tables_max: u64 = 0,
        tables_max_tree: u64 = 0,
        bytes_total: u64 = 0,
        bytes_max: u64 = 0,
        bytes_max_tree: u64 = 0,
    };
    const aggs = try self.allocator.alloc(Agg, max_levels);
    defer self.allocator.free(aggs);
    for (aggs) |*a| a.* = .{};

    // Per-level "compactions wanted" rollup.
    const Wanted = struct {
        count: u64 = 0,
        score_max: f64 = 0,
        score_max_tree: u64 = 0,
    };
    const wanted = try self.allocator.alloc(Wanted, max_levels);
    defer self.allocator.free(wanted);
    for (wanted) |*x| x.* = .{};

    // Largest tree (sum across levels).
    var largest_tree_id: u64 = 0;
    var largest_tree_size: i64 = 0;
    var tree_count: usize = 0;

    // Walk under the map lock, each table's lock shared while its list
    // is read, but only briefly: collect the numbers we need, release.
    // Each level access takes its own lock inside numTables/getTotalSize.
    {
        var tables_it = self.tables.iterator();
        defer tables_it.deinit();
        while (tables_it.next()) |kv| {
            const trees = kv.value_ptr.*;
            const held = trees.mu.hold(.shared, @src());
            defer held.release();
            for (trees.list(held)) |tree| {
                tree_count += 1;
                const tree_id = tree.tree_id;

                const th = tree.mu.hold(.shared, @src());
                defer th.release();
                var tree_total: i64 = 0;
                for (tree.levels, 0..) |*lv, idx| {
                    if (idx >= max_levels) break;
                    const tcount: u64 = @intCast(lv.numTables(th));
                    const tbytes: u64 = @intCast(lv.getTotalSize(th));
                    tree_total += @intCast(tbytes);

                    aggs[idx].tables_total += tcount;
                    if (tcount > aggs[idx].tables_max) {
                        aggs[idx].tables_max = tcount;
                        aggs[idx].tables_max_tree = tree_id;
                    }
                    aggs[idx].bytes_total += tbytes;
                    if (tbytes > aggs[idx].bytes_max) {
                        aggs[idx].bytes_max = tbytes;
                        aggs[idx].bytes_max_tree = tree_id;
                    }
                }
                if (tree_total > largest_tree_size) {
                    largest_tree_size = tree_total;
                    largest_tree_id = tree_id;
                }

                if (tree.compact_candidate) |c| {
                    if (c.level < max_levels) {
                        wanted[c.level].count += 1;
                        if (c.score > wanted[c.level].score_max) {
                            wanted[c.level].score_max = c.score;
                            wanted[c.level].score_max_tree = tree_id;
                        }
                    }
                }
            }
        }
    }

    try w.print("  trees: {d}\n", .{tree_count});
    try w.print("  largest_tree: id={d} size=", .{largest_tree_id});
    try writeBytes(w, @intCast(largest_tree_size));
    try w.print("\n\n", .{});

    try w.print("  level summary (across all trees):\n", .{});
    for (aggs, 0..) |a, idx| {
        // Skip levels that have nothing — shows max_levels=7 dumps cleanly.
        if (a.tables_total == 0 and a.bytes_total == 0) continue;
        try w.print("    L{d}: tables={d} (max={d} in tree {d})  bytes=", .{
            idx, a.tables_total, a.tables_max, a.tables_max_tree,
        });
        try writeBytes(w, a.bytes_total);
        try w.print(" (max=", .{});
        try writeBytes(w, a.bytes_max);
        try w.print(" in tree {d})\n", .{a.bytes_max_tree});
    }

    var any_wanted = false;
    for (wanted) |x| if (x.count > 0) {
        any_wanted = true;
        break;
    };
    if (any_wanted) {
        try w.print("\n  compactions wanted:\n", .{});
        for (wanted, 0..) |x, idx| {
            if (x.count == 0) continue;
            try w.print("    L{d}: trees={d} score_max={d:.2} (tree {d})\n", .{
                idx, x.count, x.score_max, x.score_max_tree,
            });
        }
    }
}

pub fn writeTreeStats(self: *Forest, w: *std.Io.Writer, tk: TreeKey) !void {
    const tree = self.getTree(tk.namespace, tk.tree_id) orelse {
        try w.print("tree id={d}: not found\n", .{tk.tree_id});
        return;
    };
    defer tree.release();

    // Numbers first, under the lock; printing after — the writer may be
    // a socket, and a slow client must not hold up installs into this
    // tree.
    const LevelStat = struct { tables: usize, bytes: u64 };
    const per_level = try self.allocator.alloc(LevelStat, tree.levels.len);
    defer self.allocator.free(per_level);
    const total: i64, const candidate: ?_tree.CompactCandidate = blk: {
        const held = tree.mu.hold(.shared, @src());
        defer held.release();
        for (tree.levels, per_level) |*lv, *st| {
            st.* = .{
                .tables = lv.numTables(held),
                .bytes = @intCast(lv.getTotalSize(held)),
            };
        }
        break :blk .{ tree.totalSize(held), tree.compact_candidate };
    };

    try w.print(
        "tree id={d} namespace={d} size=",
        .{ tk.tree_id, tk.namespace },
    );
    try writeBytes(w, @intCast(total));
    try w.print("\n", .{});

    for (per_level, 0..) |st, idx| {
        if (st.tables == 0 and st.bytes == 0) continue;
        try w.print("  L{d}: {d} tables, ", .{ idx, st.tables });
        try writeBytes(w, st.bytes);
        try w.print("\n", .{});
    }
    if (candidate) |c| {
        try w.print(
            "  compact_candidate: L{d} score={d:.2}\n",
            .{ c.level, c.score },
        );
    }
}

pub fn writeNamespaceStats(self: *Forest, w: *std.Io.Writer, ns_id: u64) !void {
    try w.print("namespace={d}:\n", .{ns_id});
    var any = false;
    // Tree keys come out under the manifest lock (the map they live in
    // is the apply loop's to rehash); writeTreeStats then takes each
    // tree's own lock.
    const keys = try self.ms.treeKeys(self.allocator, ns_id);
    defer self.allocator.free(keys);
    for (keys) |tk| {
        any = true;
        try writeTreeStats(self, w, tk);
    }
    if (!any) try w.print("  (no trees)\n", .{});
}

/// Pretty-print a byte count: KiB / MiB / GiB / TiB. Two decimal places
/// of precision past the smallest meaningful unit.
pub fn writeBytes(w: *std.Io.Writer, b: u64) !void {
    const kib: u64 = 1024;
    const mib: u64 = 1024 * kib;
    const gib: u64 = 1024 * mib;
    const tib: u64 = 1024 * gib;
    if (b >= tib) {
        try w.print(
            "{d:.2}TiB",
            .{@as(f64, @floatFromInt(b)) / @as(f64, @floatFromInt(tib))},
        );
    } else if (b >= gib) {
        try w.print(
            "{d:.2}GiB",
            .{@as(f64, @floatFromInt(b)) / @as(f64, @floatFromInt(gib))},
        );
    } else if (b >= mib) {
        try w.print(
            "{d:.2}MiB",
            .{@as(f64, @floatFromInt(b)) / @as(f64, @floatFromInt(mib))},
        );
    } else if (b >= kib) {
        try w.print(
            "{d:.2}KiB",
            .{@as(f64, @floatFromInt(b)) / @as(f64, @floatFromInt(kib))},
        );
    } else {
        try w.print("{d}B", .{b});
    }
}
