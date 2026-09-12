//! Compaction for a zdb Tree: the per-level size targets, the scoring
//! that picks which level to compact next, and Phase A of the pipeline,
//! which merges a level into the one below it and writes the output SST
//! files. It also holds VersionTracker, the per-primary state machine
//! that decides which versions are retained and folds roaring delta
//! chains, and the Emitter that writes out what the tracker decides.
//! Every function here takes `self: *Tree` and is aliased into `Tree`
//! in tree.zig, so callers keep using method syntax on a tree.

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;
const _manifest = @import("manifest.zig");
const _level_handler = @import("level.zig");
const _table = @import("table.zig");
const vfs = @import("vfs");
const _metrics = @import("metrics.zig");
const _fi = @import("file_ids.zig");
const _read = @import("read.zig");
const zroar = @import("zroar");
const _tree = @import("tree.zig");

const Tree = _tree.Tree;
const min_lmax_tables_for_split = Tree.min_lmax_tables_for_split;
const TableIterator = _table.TableIterator;
const Item = _table.table.Item;
const Level = _level_handler.Level;
const Lock = _level_handler.Lock;
const TableEntry = _level_handler.TableEntry;
const Key = @import("kv.zig").Key;
const Meta = @import("kv.zig").Meta;
const KeyRange = _level_handler.KeyRange;
const MergeIterator = _table.merge_iterator.MergeIterator;
const Pin = _table.merge_iterator.Pin;
const ConcatIterator = _table.merge_iterator.ConcatIterator;
const Iterator = _table.merge_iterator.Iterator;
const Builder = _table.builder.Builder;

const log = std.log.scoped(.zdb_tree);

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

pub const Targets = struct {
    allocator: std.mem.Allocator,
    target_sz: []i64,
    file_sz: []i64,

    pub fn deinit(self: *Targets) void {
        self.allocator.free(self.target_sz);
        self.allocator.free(self.file_sz);
    }
};

const LevelPriority = struct {
    level: usize,
    score: f64,
    adjusted: f64,
};

/// Tracks the version run for one primary during compaction — both
/// jobs that need run awareness live here, so there is exactly ONE
/// per-primary state machine:
///
/// 1. Retention (`update`): a terminal is a complete state or
///    tombstone. Versions after the newest terminal are shadowed and
///    count as stale. At the bottom level, the first terminal at or
///    below the read horizon establishes a discard boundary. A settled
///    tombstone is itself unreachable and is discarded.
///
/// 2. Roaring rollup (`track`): a roaring delta chain is buffered
///    (pinned, never copied) until its terminal arrives, the primary
///    changes, or the stream ends — then `rollupRoaring` folds it into a
///    synthetic state (config triggers permitting) or replays it
///    verbatim. Once anything for the primary is emitted unbuffered,
///    the rest streams too (`no_buffer`): everything older is
///    shadowed, and re-rolling shadowed history would regenerate a
///    synthetic state under the live terminal on every compaction
///    until GC collapses the chain.
///
/// `track` consumes and decides; the Emitter (passed per call) only
/// knows how to emit. Emission feeds back through `update`, so the
/// retention cursor (`primary`) trails the consumption cursor (`cur`)
/// while a chain is buffered.
pub const VersionTracker = struct {
    /// Retention verdict for one emitted item. `.stale` means kept
    /// but shadowed — the caller charges the item's own size to the
    /// stale accounting.
    const Update = enum { retain, stale, discard };

    /// One buffered version: a borrowed value slice plus the pin
    /// holding its backing storage alive — never a copy.
    const Buffered = struct {
        ts: u64,
        meta: Meta,
        value: String,
        pin: Pin,
    };

    allocator: std.mem.Allocator,
    can_discard_versions: bool,
    global_min_read_ts: u64,

    // Retention state (emitted-side cursor).
    primary: std.ArrayList(u8) = .empty,
    stale_below: u64 = 0,
    discard_below: u64 = 0,

    // Rollup state (consumed-side cursor).
    cur: std.ArrayList(u8) = .empty,
    ns: u64 = 0,
    tid: u32 = 0,
    buffered: std.ArrayList(Buffered) = .empty,
    have_state: bool = false,
    no_buffer: bool = false,

    pub fn init(
        allocator: std.mem.Allocator,
        global_min_read_ts: u64,
        can_discard_versions: bool,
    ) VersionTracker {
        return .{
            .allocator = allocator,
            .global_min_read_ts = global_min_read_ts,
            .can_discard_versions = can_discard_versions,
        };
    }

    pub fn deinit(self: *VersionTracker) void {
        self.clearBufferAndUnpin();
        self.buffered.deinit(self.allocator);
        self.cur.deinit(self.allocator);
        self.primary.deinit(self.allocator);
    }

    // shouldRetain tracks if an Item should be retained, should be garbage
    // collected (discard), or if it's stale and would be eligible for discard
    // later.
    fn shouldRetain(self: *VersionTracker, item: Item) !Update {
        if (!item.key.equalPrimary(.{ .primary = self.primary.items })) {
            self.primary.clearRetainingCapacity();
            try self.primary.appendSlice(self.allocator, item.key.primary);

            // stale_below is only used for tracking stale bytes in the table.
            // discard_below is for discarding versions.
            self.stale_below = 0;
            self.discard_below = 0;
        }

        if (item.key.timestamp < self.discard_below) {
            return .discard;
        }
        const stale = item.key.timestamp < self.stale_below;

        if (item.meta.isState() or item.meta.isDeleted()) {
            if (self.stale_below == 0) {
                self.stale_below = item.key.timestamp;
            }

            if (self.can_discard_versions and
                item.key.timestamp <= self.global_min_read_ts)
            {
                self.discard_below = item.key.timestamp;
                if (item.meta.isDeleted()) return .discard;
            }
        }

        return if (stale) .stale else .retain;
    }

    /// Every consumed item enters here, in merge order, with the pin
    /// backing its value (merge.currentPin()). `item.value` is only
    /// valid during this call unless pinned.
    fn track(
        t: *VersionTracker,
        e: *Tree.Emitter,
        item: Item,
        pin: ?Pin,
    ) !void {
        if (!item.key.equalPrimary(.{ .primary = t.cur.items })) {
            // Previous primary is over; resolve whatever it buffered.
            try t.rollupRoaring(e);
            try t.setPrimary(e, item);
        }
        if (t.shouldBuffer(item)) {
            switch (try t.roaringBuffer(item, pin)) {
                .more => return,
                .done => try t.rollupRoaring(e),
                .foreign => try t.emitBuffered(e),
            }
            t.no_buffer = true;
        } else {
            try e.emitOne(item);
            // Everything below an unbuffered emission is shadowed by
            // it — never buffer (or fold) shadowed history.
            t.no_buffer = true;
        }
    }

    /// End of stream: resolve a chain still buffered.
    fn finishTracking(t: *VersionTracker, e: *Tree.Emitter) !void {
        try t.rollupRoaring(e);
    }

    fn setPrimary(
        t: *VersionTracker,
        e: *const Tree.Emitter,
        item: Item,
    ) !void {
        std.debug.assert(t.buffered.items.len == 0);
        t.cur.clearRetainingCapacity();
        try t.cur.appendSlice(t.allocator, item.key.primary);
        t.ns = item.key.namespace;
        t.tid = item.key.table_id;
        t.no_buffer = !e.tree.opts.rollup_enabled;
        t.have_state = false;
    }

    fn shouldBuffer(t: *const VersionTracker, item: Item) bool {
        if (t.no_buffer) return false;
        // An active chain classifies everything through roaringBuffer
        // — the buffered deltas must reach the output before whatever
        // ends the chain. A chain only OPENS on a roaring delta: a
        // lone state is already rolled, and raw rows are not ours.
        if (t.buffered.items.len > 0) return true;
        return item.meta.isRoaring() and item.meta.isDelta();
    }

    pub const RoaringChain = enum {
        /// A delta: the chain stays open.
        more,
        /// A terminal — state or tombstone, whose encoding nibble is
        /// irrelevant — completed the chain.
        done,
        /// Non-roaring mixed into a posting chain (never expected in
        /// practice): not the fold's to touch.
        foreign,
    };

    /// The collection core shared by compaction (`track`) and the
    /// read walk (Tree.rollupBitmap): buffer one pinned version of the
    /// current chain, newest first, and classify it off the meta byte.
    /// A positioned iterator always has a pin.
    pub fn roaringBuffer(
        t: *VersionTracker,
        item: Item,
        pin_opt: ?Pin,
    ) !RoaringChain {
        const pin = pin_opt.?.acquire();
        errdefer pin.release();

        try t.buffered.append(t.allocator, .{
            .ts = item.key.timestamp,
            .meta = item.meta,
            .value = item.value,
            .pin = pin,
        });

        if (item.meta.isRoaring() and item.meta.isDelta()) {
            return .more;
        }
        if (item.meta.isDeleted() or
            (item.meta.isRoaring() and item.meta.isState()))
        {
            t.have_state = true;
            return .done;
        }
        return .foreign;
    }

    /// The buffered chain is deltas newest-first with at most one
    /// trailing terminal — everything before that terminal is a delta.
    fn nDeltas(t: *const VersionTracker) usize {
        return t.buffered.items.len - @intFromBool(t.have_state);
    }

    /// Resolve the buffered chain: fold it into a synthetic state when
    /// policy allows, else replay it verbatim. The synthetic state
    /// carries the newest delta's timestamp, REPLACING that delta:
    /// readers at read_ts >= t stop there (the point of rollup);
    /// readers below t skip it and fold the retained chain, emitted
    /// unchanged beneath — MVCC and GC semantics are untouched, the
    /// state is just a new terminal for `update`. An empty fold emits
    /// a tombstone instead: 0 bytes vs zroar's 64-byte empty bitmap,
    /// and bottom-level GC can then drop the key outright.
    fn rollupRoaring(t: *VersionTracker, e: *Tree.Emitter) !void {
        const items = t.buffered.items;
        if (items.len == 0) return;

        const opts = &e.tree.opts;
        const alloc = e.tree.allocator;

        // Policy: fold when the chain is long enough, and — when its
        // base is not among the inputs (no terminal buffered) — when a
        // trigger justifies peeking below the destination for it (see
        // config.zig's rollup_* knobs).
        const roll = t.nDeltas() >= opts.rollup_min_deltas and
            (t.have_state or
                e.dest >= opts.rollup_at_level or
                t.nDeltas() >= opts.rollup_max_deltas);
        if (!roll) return t.emitBuffered(e);

        var external: ?zroar.Bitmap = null;
        defer if (external) |*eb| eb.deinit();

        if (!t.have_state) {
            // The oldest buffered ts bounds the peek: the base must
            // sit strictly below the chain we hold. HLC timestamps
            // are never 0, so the -1 cannot wrap.
            const oldest = items[items.len - 1].ts;
            std.debug.assert(oldest > 0);

            external = e.tree.rollupBitmap(
                alloc,
                t.bufferedKey(oldest - 1),
                e.prio,
                e.dest + 1,
            ) catch |err| switch (err) {
                // The chain below can't be folded (mixed or corrupt):
                // keep everything verbatim, reads keep folding.
                error.NotRoaring, error.CorruptPosting => {
                    log.warn(
                        "rollup: unfoldable chain below L{d} tree={d}; run " ++
                            "kept verbatim",
                        .{ e.dest, e.tree.tree_id },
                    );
                    return t.emitBuffered(e);
                },
                else => return err,
            };
        }

        var bm = t.foldBufferedToBitmap(
            alloc,
            if (external) |*eb| eb else null,
        ) catch |err| switch (err) {
            // Undecodable delta bytes (structure only — content was
            // validated at write time): keep the chain verbatim.
            error.CorruptPosting => {
                log.warn(
                    "rollup: undecodable roaring delta tree={d}; run kept " ++
                        "verbatim",
                    .{e.tree.tree_id},
                );
                return t.emitBuffered(e);
            },
            else => return err,
        };
        defer bm.deinit();
        // Canonical minimal buffer for persistence.
        try bm.compact();

        const newest = items[0];
        if (bm.isEmpty()) {
            try e.emitOne(.{
                .key = t.bufferedKey(newest.ts),
                .value = "",
                .meta = .{ .kind = .deleted, .encoding = .roaring },
            });
        } else {
            try e.emitOne(.{
                .key = t.bufferedKey(newest.ts),
                .value = bm.toBuffer(),
                .meta = .{ .kind = .state, .encoding = .roaring },
            });
        }
        e.stats.rollups += 1;
        for (items[1..]) |b| try e.emitOne(t.bufferedItem(b));
        t.clearBufferAndUnpin();
    }

    /// Fold the buffered chain into a bitmap — THE fold: compaction
    /// (`rollupRoaring`) and reads (Tree.rollupBitmap) both resolve
    /// through here, which is what keeps rollup read-invariant.
    /// Applies deltas oldest→newest (reverse buffer order), removes
    /// before adds within each. Base priority: a tombstone terminal
    /// folds from empty; a state terminal via fromBufferCopy (SST
    /// values sit at arbitrary offsets, so zroar's aligned zero-copy
    /// path is not usable); else `external` (compaction read-below)
    /// or the empty set. Delta bytes get an O(1) structural check —
    /// without it a truncated element region decodes as an empty
    /// list — but content contracts were enforced at write time
    /// (validateDelta). Leaves the buffer intact — compaction
    /// still replays the retained chain after the synthetic state.
    pub fn foldBufferedToBitmap(
        t: *VersionTracker,
        alloc: std.mem.Allocator,
        external: ?*const zroar.Bitmap,
    ) !zroar.Bitmap {
        const items = t.buffered.items;

        const terminal: ?Item = if (t.have_state)
            t.bufferedItem(items[items.len - 1])
        else
            null;
        var bm = try _read.baseBitmap(alloc, terminal, external);
        errdefer bm.deinit();

        // The deltas are everything but a trailing terminal, newest
        // first — apply in reverse.
        var i = t.nDeltas();
        while (i > 0) {
            i -= 1;
            try _read.applyDelta(&bm, items[i].value);
        }
        return bm;
    }

    fn emitBuffered(t: *VersionTracker, e: *Tree.Emitter) !void {
        for (t.buffered.items) |b| try e.emitOne(t.bufferedItem(b));
        t.clearBufferAndUnpin();
    }

    fn bufferedKey(t: *const VersionTracker, ts: u64) Key {
        return .{
            .namespace = t.ns,
            .table_id = t.tid,
            .primary = t.cur.items,
            .timestamp = ts,
        };
    }

    fn bufferedItem(t: *const VersionTracker, b: Buffered) Item {
        return .{
            .key = t.bufferedKey(b.ts),
            .value = b.value,
            .meta = b.meta,
        };
    }

    fn clearBufferAndUnpin(t: *VersionTracker) void {
        for (t.buffered.items) |b| b.pin.release();
        t.buffered.clearRetainingCapacity();
        t.have_state = false;
    }
};

/// A tree's pending work item, picked by `Forest.bestCompactionCandidate`.
/// `needs_split=true` means the tree has outgrown `tree_split_size` and
/// should be split instead of compacted; the `level` field then describes
/// the lowest non-empty level (informational, since split operates across
/// all levels at the median key, not on a single level). `score` is bumped
/// above the natural compaction range so a split preempts any pending
/// compaction in the forest.
pub const CompactCandidate = struct {
    level: u8,
    score: f64,
    needs_split: bool = false,
};

/// Score assigned to a tree that needs a split. Comfortably above any
/// natural compaction adjusted-score so splits always rank first.
pub const split_priority_score: f64 = 100.0;

// -----------------------------------------------------------------------
// Level sizing
// -----------------------------------------------------------------------

/// Targets are a function of the options alone; no lock needed.
pub fn levelTargets(self: *Tree) !Targets {
    const n = self.opts.max_levels;
    const target_sz = try self.allocator.alloc(i64, n);
    errdefer self.allocator.free(target_sz);
    const file_sz = try self.allocator.alloc(i64, n);
    errdefer self.allocator.free(file_sz);

    @memset(target_sz, 0);
    @memset(file_sz, 0);

    // Target sizes are FIXED per tree, derived from `tree_split_size`:
    // each tree splits when its bottom level outgrows that cap, so
    // the bottom level's target should be slightly under it (we use
    // 4/5 = 0.8x to leave headroom; once the actual bottom-level size
    // hits ~1.25x target it triggers a split, well within the cap).
    // Levels above cascade up by dividing by `level_size_multiplier`,
    // floored at 1 byte so the size/target score is well-defined.
    //
    // Why FIXED instead of derived from current data: dynamic targets
    // (RocksDB-style) have a chicken-and-egg problem — early in a
    // tree's life, Lmax is empty, so cascading from current sizes
    // produces big upper-level targets. Stepwise compaction then
    // can't drain those intermediates. Fixed targets sized to the
    // tree's eventual capacity sidestep this entirely. If Lmax
    // briefly exceeds its target before the split kicks in, that's
    // fine — we don't crash, the split will eventually fire.
    const lmax_idx: usize = n - 1;
    target_sz[lmax_idx] = @divTrunc(self.opts.tree_split_size * 4, 5);
    var t: i64 = target_sz[lmax_idx];
    {
        var i: usize = lmax_idx;
        while (i > 1) {
            i -= 1;
            t = @divTrunc(t, self.opts.level_size_multiplier);
            if (t < 1) t = 1;
            target_sz[i] = t;
        }
    }

    // Per-level file sizes: L0 inherits the memtable size; L1 uses
    // `l1_table_size`; each level below ramps by
    // `table_size_multiplier` up to `max_table_size`. Compaction is
    // strictly stepwise (L0→L1, L1→L2, …) so there's no separate
    // base-level concept — L1 is always the destination of L0.
    var tsz: i64 = self.opts.l1_table_size;
    file_sz[0] = @intCast(self.opts.memtable_size);
    for (1..n) |j| {
        if (j == 1) {
            file_sz[j] = tsz;
        } else {
            tsz = @min(
                tsz * self.opts.table_size_multiplier,
                self.opts.max_table_size,
            );
            file_sz[j] = tsz;
        }
    }

    return .{
        .allocator = self.allocator,
        .target_sz = target_sz,
        .file_sz = file_sz,
    };
}

// -----------------------------------------------------------------------
// Compaction selection
// -----------------------------------------------------------------------

/// updateCompactCandidate recomputes which level most needs compaction and
/// stores the result in self.compact_candidate. Called after open and after
/// every doCompact so the candidate is always fresh.
pub fn updateCompactCandidate(self: *Tree) !void {
    // Reads every level's size and writes the verdict, under one
    // exclusive hold. Installs call this after releasing theirs.
    const held = self.mu.hold(.exclusive, @src());
    defer held.release();
    try self.computeCompactCandidate(held);
}

/// The verdict itself. Caller holds `mu` exclusively — `held` is the
/// proof; the level reads below use it as shared.
pub fn computeCompactCandidate(self: *Tree, held: Lock.Exclusive) !void {
    self.mu.assertHeld(held);
    const rd = held.shared();
    // Split check overrides compaction. We compare TOTAL tree size
    // (L0+L1+…+Lmax) against `tree_split_size`, not just Lmax. If
    // compactions can't keep up the data piles up across the upper
    // levels and Lmax stays under threshold even when the tree as a
    // whole is several times oversized — exactly the wedged shape
    // we just observed. Splitting on total size lets each tree get
    // its own future compactor and divides the catch-up work.
    // The score is bumped to `split_priority_score` so this tree
    // wins over normal compaction candidates in the forest. Set
    // `level` to the lowest non-empty level for diagnostic
    // legibility (`stats forest`).
    const linfo = self.lowestLevelSize(rd);
    const total = self.totalSize(rd);
    // Gate the split on Lmax feasibility: findSplitKey needs Lmax to hold
    // at least min_lmax_tables_for_split tables to pick a boundary key. A
    // tree can be oversized in total while its data is still stacked in the
    // upper levels (Lmax near-empty) — triggering then spins forever:
    // needs_split stays true, buildSplit returns null, proposeSplit skips.
    // When infeasible, fall through to compaction, which drains data into
    // Lmax until a split becomes possible.
    const lmax_tables = self.levels[self.opts.max_levels - 1].numTables(rd);
    if (total > self.opts.tree_split_size and
        lmax_tables >= min_lmax_tables_for_split)
    {
        log.debug(
            "split-trigger tree={d} total={d} deepest_level={d} " ++
                "lmax_size={d} lmax_tables={d} > threshold={d}",
            .{
                self.tree_id,
                total,
                linfo.level,
                linfo.size,
                lmax_tables,
                self.opts.tree_split_size,
            },
        );
        self.compact_candidate = .{
            .level = @intCast(linfo.level),
            .score = split_priority_score,
            .needs_split = true,
        };
        return;
    }

    var t = try self.levelTargets();
    defer t.deinit();

    const n = self.opts.max_levels;
    const prios = try self.allocator.alloc(LevelPriority, n);
    defer self.allocator.free(prios);

    // L0: count-based score, scaled so it competes on the same axis
    // as L1+ size scores. Score reaches 1.0 at HALF the backpressure
    // start count — early enough that compaction usually catches up
    // before backpressure kicks in. Without this scaling (or with
    // the old "if score >= 2.5, compact L0 immediately" shortcut),
    // L0 starves L1→L2/L3 work and lower levels go unmaintained.
    // Backpressure (`l0_backpressure_start_count`) is what protects
    // writes from a temporarily-out-of-shape L0; the scoring loop
    // is for steady-state level shape.
    const l0_tables = self.levels[0].numTables(rd);
    const l0_score = if (self.opts.l0_backpressure_start_count == 0)
        @as(f64, @floatFromInt(l0_tables))
    else
        @as(f64, @floatFromInt(l0_tables * 2)) /
            @as(f64, @floatFromInt(self.opts.l0_backpressure_start_count));
    prios[0] = .{ .level = 0, .score = l0_score, .adjusted = l0_score };

    // L≥1: size-based score.
    for (1..n) |j| {
        const target = t.target_sz[j];
        const score = if (target > 0)
            @as(f64, @floatFromInt(self.levels[j].getTotalSize(rd))) /
                @as(f64, @floatFromInt(target))
        else
            0.0;
        prios[j] = .{ .level = j, .score = score, .adjusted = score };
    }

    // Adjusted scores: mirrors Badger/PebbleDB's priority dampening.
    // upperLevel starts at 0; for each level from L1 downward, if
    // prios[upperLevel].adjusted >= 1.0, divide it by the current level's
    // adjusted score (floored at 0.01).
    var upper_level: usize = 0;
    for (1..n) |cur_level| {
        if (prios[upper_level].adjusted >= 1.0) {
            // If current level's adjusted score (= score, initially) is < 1.0,
            // then it would boost the uppper level's adjusted score AS. If it's
            // > 1.0, then it would dampen the upper level's AS. This helps
            // because there's no point compacting something at upper level when
            // the level below it is already too big. This basically helps make
            // space in lower levels first before compacting upper levels.
            const denom = @max(prios[cur_level].adjusted, 0.01);
            prios[upper_level].adjusted /= denom;
        } else {
            // If adjusted score is < 1.0, it won't be compacted anyways,
            // so we can safely skip calculating the adjusted score.
        }
        upper_level = cur_level;
    }

    // Collect levels with score >= 1.0 (excluding the last level).
    var filtered = try std.ArrayList(LevelPriority).initCapacity(
        self.allocator,
        n,
    );
    defer filtered.deinit(self.allocator);
    for (prios[0 .. n - 1]) |p| {
        if (p.score >= 1.0) try filtered.append(self.allocator, p);
    }

    // Sort descending by adjusted score; pick the top.
    std.mem.sort(LevelPriority, filtered.items, {}, struct {
        fn gt(_: void, a: LevelPriority, b: LevelPriority) bool {
            return a.adjusted > b.adjusted;
        }
    }.gt);
    const winner: ?LevelPriority = if (filtered.items.len > 0 and
        filtered.items[0].adjusted > 1.0)
        filtered.items[0]
    else
        null;

    if (winner) |w| {
        self.compact_candidate = .{
            .level = @intCast(w.level),
            .score = w.adjusted,
        };
        return;
    }

    // Idle fallback: nothing scored above 1.0, but if L0 has any tables,
    // drain it. Without this, a few stragglers (e.g. 2 L0 tables, score
    // 0.5) would sit forever, paying read amplification until enough
    // flushes pushed L0 over the start_count threshold. Lower levels by
    // definition need no work when nothing scored ≥ 1.0, so L0 is always
    // the right idle pick.
    if (self.levels[0].numTables(rd) > 0) {
        self.compact_candidate = .{ .level = 0, .score = prios[0].score };
    } else {
        self.compact_candidate = null;
    }
}

// -----------------------------------------------------------------------
// Compaction execution
// -----------------------------------------------------------------------

/// doCompact runs the full compaction pipeline: buildCompaction (Phase A)
/// then applyCompaction (Phase B). For Raft integration, callers can use
/// buildCompaction and applyCompaction separately.
pub fn doCompact(
    self: *Tree,
    src_level: u8,
    file_ids: *_fi.FileIds,
    global_min_read: u64,
) !void {
    var changes = try self.buildCompaction(
        src_level,
        file_ids,
        global_min_read,
    );
    defer changes.deinit(self.allocator);
    try self.applyCompaction(changes.items);
}

/// Diagnostics. `keys_in - keys_out` is what this compaction discarded on
/// purpose (settled history below the horizon); 0 above the bottom level.
/// `stale_bytes` is kept-but-shadowed data, recorded per output table in
/// stale_data_size. A nonzero `expected_in - keys_in` is never expected —
/// it means the merge iterator lost rows. `bytes_*` count user data only
/// (key.primary + value), not framing. `rollups` counts roaring delta
/// chains folded into a synthetic state (so bytes_out > bytes_in is
/// expected when it's nonzero — the state is bigger than the delta it
/// replaces).
const Metrics = struct {
    keys_in: u64 = 0,
    keys_out: u64 = 0,
    bytes_in: u64 = 0,
    bytes_out: u64 = 0,
    dropped_bytes: u64 = 0,
    stale_bytes: u64 = 0,
    rollups: u64 = 0,
    src_keys: u64 = 0,
    src_bytes_disk: u64 = 0,
    bot_keys: u64 = 0,
    bot_bytes_disk: u64 = 0,
};

/// The compaction output stage — emission mechanics only. Every
/// consumed item goes through `push` in merge order; `finish`
/// closes the stream. Run awareness (retention boundaries AND the
/// roaring rollup) lives in the VersionTracker, which calls back
/// into `emitOne`: tracker update → discard → file-split check →
/// addItem → stale/stats accounting. Consumption accounting
/// (keys_in/bytes_in) happens in `push`.
pub const Emitter = struct {
    tree: *Tree,
    bld: *Builder,
    group: *fiber.Group(?FlushResult),
    file_ids: *_fi.FileIds,
    stats: *Metrics,
    file_sz: usize,
    dest: usize,
    prio: vfs.Priority,
    tracker: VersionTracker,

    fn init(
        tree: *Tree,
        bld: *Builder,
        group: *fiber.Group(?FlushResult),
        file_ids: *_fi.FileIds,
        stats: *Metrics,
        file_sz: usize,
        dest: usize,
        prio: vfs.Priority,
        global_min_read: u64,
    ) Emitter {
        return .{
            .tree = tree,
            .bld = bld,
            .group = group,
            .file_ids = file_ids,
            .stats = stats,
            .file_sz = file_sz,
            .dest = dest,
            .prio = prio,
            // Only the bottom level may discard history: lower
            // levels could still contain a version that an early
            // tombstone would otherwise resurrect.
            .tracker = VersionTracker.init(
                tree.allocator,
                global_min_read,
                dest == @as(usize, tree.opts.max_levels) - 1,
            ),
        };
    }

    /// Error paths land here with a chain still buffered; dropping
    /// it is fine — the whole compaction output is being
    /// abandoned. The happy path must call `finish` first: a
    /// forgotten trailing chain surfaces loudly as lost keys in
    /// the compact log's expected_in reconciliation.
    fn deinit(e: *Emitter) void {
        e.tracker.deinit();
    }

    /// Feed one item in merge (consumption) order, with the pin
    /// backing its value (merge.currentPin()).
    fn push(e: *Emitter, item: Item, pin: ?Pin) !void {
        e.stats.keys_in += 1;
        e.stats.bytes_in += item.key.primary.len + item.value.len;
        try e.tracker.track(e, item, pin);
    }

    /// End of stream: resolve a chain still being buffered.
    fn finish(e: *Emitter) !void {
        try e.tracker.finishTracking(e);
    }

    fn emitOne(e: *Emitter, item: Item) !void {
        const nbytes = item.key.primary.len + item.value.len;
        const ret = try e.tracker.shouldRetain(item);
        if (ret == .discard) {
            e.stats.dropped_bytes += nbytes;
            return;
        }

        if (e.bld.table_bytes.items.len >= e.file_sz) {
            // We are not going to break up a primary key across tables. All the
            // versions for this key should live in the same table. This avoids
            // a scenario where a table with a higher version key ends up
            // getting compacted and living in a lower level. We expect that the
            // flow of keys is from top to bottom levels, so we're expecting to
            // find higher version keys at higher levels. This expectation is
            // used by get fn to stop searching for key as soon as we're able to
            // find a version >= search key.
            if (!item.key.equalPrimary(e.bld.lastKey())) {
                try e.tree.flushBuilderOutput(e.bld, e.group, e.file_ids);
                e.bld.reset();
            }
        }
        try e.bld.addItem(item);
        if (ret == .stale) {
            e.bld.addStaleBytes(nbytes);
            e.stats.stale_bytes += nbytes;
        }
        e.stats.keys_out += 1;
        e.stats.bytes_out += nbytes;
    }
};

/// buildCompaction is Phase A: merges src tables with overlapping dest tables,
/// writes new SST files, and returns the manifest SSTableChange list describing
/// the result. The caller is responsible for proposing these changes (e.g.
/// through Raft) and then calling applyCompaction.
///
/// `global_min_read` is the oldest timestamp a reader may still ask for
/// (`Forest.global_min_read`); anything strictly below it is unobservable
/// and eligible to be discarded. Pass 0 to discard nothing.
pub fn buildCompaction(
    self: *Tree,
    src_level: u8,
    file_ids: *_fi.FileIds,
    global_min_read: u64,
) !std.ArrayList(_manifest.SSTableChange) {
    const src: usize = src_level;
    const prio: vfs.Priority = if (src_level == 0) .p1 else .p2;

    var t = try self.levelTargets();
    defer t.deinit();

    // Stepwise compaction: every level pushes to the level immediately
    // below it, including L0 → L1. Routing L0 directly to Lbase used to
    // produce huge cross-level compactions and made intermediate
    // levels effectively unused; routing one step at a time keeps
    // each compaction's input set bounded by the destination level's
    // overlapping range.
    const dest: usize = @as(usize, src_level) + 1;
    if (dest >= self.opts.max_levels) return .empty;

    // --- collect src entries ---
    // L≥1: pick a RANDOM single table. Always picking items[0] (the
    // smallest-key table) starves higher key ranges and biases write
    // amplification toward the left edge of the keyspace.
    // L0: take the oldest-first prefix that brings total compaction
    // input bytes (selected L0 + their L1 overlap) up to
    // `l0_compaction_max_bytes`. The table that crosses the budget
    // is included; we stop afterwards. Newer L0 tables stay put and
    // continue to shadow the compacted versions on read.
    const src_entries_slice = blk: {
        // Snapshot the src table list under a short shared hold so
        // the per-iteration dest-overlap queries take their own.
        // Pinned under that hold: a remote-built applyCompaction
        // landing mid-build (leadership flip) must not free tables
        // we're about to read. decrRef at zero frees memory only —
        // the file stays — so a pinned table remains readable.
        const src_snapshot = ss: {
            const held = self.mu.hold(.shared, @src());
            defer held.release();
            const items = self.levels[src].entries(held);
            if (items.len == 0) return .empty;
            const snap = try self.allocator.dupe(TableEntry, items);
            for (snap) |e| e.table.incrRef();
            break :ss snap;
        };
        defer Level.releaseTables(self.allocator, src_snapshot);

        if (src_level != 0) {
            // Pick any table at random.
            const rng_impl: std.Random.IoSource = .{
                .io = utils.context.g().io,
            };
            const rng = rng_impl.interface();
            const idx = rng.uintLessThan(usize, src_snapshot.len);
            const pick = try self.allocator.dupe(
                TableEntry,
                src_snapshot[idx .. idx + 1],
            );
            for (pick) |e| e.table.incrRef();
            break :blk pick;
        }

        const budget = self.opts.l0_compaction_max_bytes;
        var union_kr = KeyRange{};
        var l0_bytes: u64 = 0;
        var n: usize = 0;
        while (n < src_snapshot.len) {
            const e = src_snapshot[n];
            union_kr.extend(.{
                .left = e.table.smallest,
                .right = e.table.biggest,
            });
            l0_bytes += e.table.fileSize();
            n += 1;

            const overlap = ov: {
                const held = self.mu.hold(.shared, @src());
                defer held.release();
                break :ov try self.levels[dest].overlappingTables(
                    held,
                    self.allocator,
                    union_kr,
                );
            };
            defer Level.releaseTables(self.allocator, overlap);
            var bot_bytes: u64 = 0;
            for (overlap) |oe| bot_bytes += oe.table.fileSize();
            if (l0_bytes + bot_bytes >= budget) break;
        }
        const pick = try self.allocator.dupe(TableEntry, src_snapshot[0..n]);
        for (pick) |e| e.table.incrRef();
        break :blk pick;
    };
    defer Level.releaseTables(self.allocator, src_entries_slice);

    // --- compute union key range of all src tables ---
    var kr = KeyRange{};
    for (src_entries_slice) |e| {
        const tkr = KeyRange{
            .left = e.table.smallest,
            .right = e.table.biggest,
        };
        kr.extend(tkr);
    }

    // --- collect overlapping dest (bot) entries ---
    const bot_entries_slice = blk: {
        const held = self.mu.hold(.shared, @src());
        defer held.release();
        break :blk try self.levels[dest].overlappingTables(
            held,
            self.allocator,
            kr,
        );
    };
    defer Level.releaseTables(self.allocator, bot_entries_slice);

    // --- build single MergeIterator over src tables + one ConcatIterator for
    // bot ---
    const bot_iters = try self.allocator.alloc(
        TableIterator,
        bot_entries_slice.len,
    );
    defer self.allocator.free(bot_iters);

    const all_iters = try self.allocator.alloc(
        Iterator,
        src_entries_slice.len + 1,
    );
    defer self.allocator.free(all_iters);

    for (src_entries_slice, 0..) |e, i|
        all_iters[i] = .{
            .table = TableIterator.init(self.allocator, e.table, prio),
        };
    for (bot_entries_slice, 0..) |e, i|
        bot_iters[i] = TableIterator.init(self.allocator, e.table, prio);
    all_iters[src_entries_slice.len] = .{
        .concat = ConcatIterator.init(bot_iters),
    };

    var merge = MergeIterator.init(self.allocator, all_iters);
    defer merge.deinit();
    try merge.seekRange(self.key_range.left, self.key_range.right);

    // --- output file IDs produced by this compaction ---
    var output_file_ids: std.ArrayList(u64) = .empty;
    defer output_file_ids.deinit(self.allocator);

    // --- merge loop ---
    const cfg = _table.builder.Config{
        .target_block_size = self.opts.block_size,
        .codec = self.opts.compression,
        .priority = prio,
    };
    const file_sz: usize = @intCast(t.file_sz[dest]);

    var bld = Builder.init(self.allocator, cfg);
    defer bld.deinit();

    var flush_group = fiber.Group(?FlushResult).init(self.allocator);
    defer {
        _ = flush_group.wait();
        flush_group.deinit();
    }

    var stats: Metrics = .{};
    var emitter = Emitter.init(
        self,
        &bld,
        &flush_group,
        file_ids,
        &stats,
        file_sz,
        dest,
        prio,
        global_min_read,
    );
    defer emitter.deinit();

    while (merge.itemRef()) |item| {
        try emitter.push(item, merge.currentPin());
        try merge.next();
    }
    // Resolve a roaring run still buffered at end of stream.
    try emitter.finish();

    // Flush any remaining data.
    try self.flushBuilderOutput(&bld, &flush_group, file_ids);

    // Sum up input key counts + on-disk byte sizes from every src + bot
    // SST's index for the "expected" baseline. If keys_in != expected, the
    // merge iterator is losing items and something is wrong. keys_out <
    // keys_in is normal now: it's what this compaction discarded.
    for (src_entries_slice) |e| {
        stats.src_keys += @intCast(e.table.index.key_count);
        stats.src_bytes_disk += e.table.fileSize();
    }
    for (bot_entries_slice) |e| {
        stats.bot_keys += @intCast(e.table.index.key_count);
        stats.bot_bytes_disk += e.table.fileSize();
    }
    const expected_in = stats.src_keys + stats.bot_keys;
    const expected_bytes_disk =
        stats.src_bytes_disk + stats.bot_bytes_disk;
    const lost_in_iter = expected_in -| stats.keys_in;
    const discarded = stats.keys_in -| stats.keys_out;
    log.info(
        "compact tree={d} L{d}->L{d} src_tables={d} src_keys={d} " ++
            "src_bytes_disk={d} bot_tables={d} bot_keys={d} " ++
            "bot_bytes_disk={d} expected_in={d} expected_bytes_disk={d} " ++
            "keys_in={d} keys_out={d} bytes_in={d} bytes_out={d} " ++
            "lost_in_iter={d} discarded={d} discarded_bytes={d} " ++
            "stale_bytes={d} rollups={d} min_read={d}",
        .{
            self.tree_id,          src,                 dest,
            src_entries_slice.len, stats.src_keys,      stats.src_bytes_disk,
            bot_entries_slice.len, stats.bot_keys,      stats.bot_bytes_disk,
            expected_in,           expected_bytes_disk, stats.keys_in,
            stats.keys_out,        stats.bytes_in,      stats.bytes_out,
            lost_in_iter,          discarded,           stats.dropped_bytes,
            stats.stale_bytes,     stats.rollups,       global_min_read,
        },
    );
    if (stats.rollups > 0) {
        try _metrics.g().rollups.incrBy(.{
            .namespace_id = self.namespace,
            .tree_id = self.tree_id,
            .level = @intCast(dest),
        }, stats.rollups);
    }

    // Start dir sync while we're waiting for file syncs to be done. We can do
    // dir sync here because files have already been created and written. So,
    // dir should have everything it needs.
    var dir_sync = try fiber.spawn(.{}, vfs.syncDir, .{self.opts.dir});
    defer dir_sync.join();

    // Join all sync fibers and collect output file IDs.
    const results = flush_group.wait();
    for (results) |r_opt| {
        const result = r_opt orelse return error.FlushFailed;
        try output_file_ids.append(self.allocator, result.file_id);
    }

    // --- build manifest changes ---
    var changes = try std.ArrayList(_manifest.SSTableChange).initCapacity(
        self.allocator,
        src_entries_slice.len + bot_entries_slice.len +
            output_file_ids.items.len,
    );

    for (src_entries_slice) |e| try changes.append(
        self.allocator,
        _manifest.deleteSSTableChange(
            e.id,
            self.namespace,
            self.tree_id,
            @intCast(src),
        ),
    );
    for (bot_entries_slice) |e| try changes.append(
        self.allocator,
        _manifest.deleteSSTableChange(
            e.id,
            self.namespace,
            self.tree_id,
            @intCast(dest),
        ),
    );
    for (output_file_ids.items) |fid| {
        // usize → u32
        try changes.append(
            self.allocator,
            _manifest.createSSTableChange(
                fid,
                self.namespace,
                self.tree_id,
                @intCast(dest),
            ),
        );
    }

    return changes;
}

const FlushResult = struct { file_id: u64 };

/// flushBuilderOutput finishes the builder, writes bytes to a new SST file,
/// then spawns a sync fiber registered with the caller's Group.
pub fn flushBuilderOutput(
    self: *Tree,
    bld: *Builder,
    group: *fiber.Group(?FlushResult),
    file_ids: *_fi.FileIds,
) !void {
    var tb = try bld.finish();
    defer tb.deinit();
    if (tb.bytes.len == 0) return;

    const id = try file_ids.allocate();
    const path = try _fi.sstPath(self.allocator, self.opts.dir, id);
    errdefer self.allocator.free(path);

    const wh = try vfs.g().createFile(path, .{ .mode = .direct });
    try wh.write(tb.bytes, 0, bld.cfg.priority);

    // Sync in a fiber.
    try group.spawn(.{}, syncFile, .{ self.allocator, wh, path, id });
}

fn syncFile(
    allocator: std.mem.Allocator,
    wh: vfs.FileHandle,
    path: String,
    file_id: u64,
) ?FlushResult {
    defer allocator.free(path);
    wh.sync() catch return null;
    wh.close();
    return .{ .file_id = file_id };
}
