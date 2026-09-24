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
const _forest = @import("forest.zig");
const _read = @import("read.zig");
const zroar = @import("zroar");
const _tree = @import("tree.zig");
const _config = @import("config.zig");

const Tree = _tree.Tree;
const min_lmax_tables_for_split = Tree.min_lmax_tables_for_split;
const TableIterator = _table.TableIterator;
const Item = _table.table.Item;
const Level = _level_handler.Level;
const Lock = _level_handler.Lock;
pub const TableEntry = _level_handler.TableEntry;
const Key = @import("kv.zig").Key;
const TableKey = @import("kv.zig").TableKey;
const Version = @import("kv.zig").Version;
const KeyRange = @import("kv.zig").KeyRange;
const MergeIterator = _table.merge_iterator.MergeIterator;
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

/// Decides what of one primary's versions compaction writes. The merge
/// hands over one item per primary with every version of it, newest
/// first, so both jobs run over a slice within one call:
///
/// 1. Retention (`retain`): a terminal is a complete state or
///    tombstone. Versions after the newest terminal are shadowed and
///    count as stale. At the bottom level, the first terminal at or
///    below the read horizon establishes a discard boundary. A settled
///    tombstone is itself unreachable and is discarded.
///
/// 2. Roaring rollup (`track`): when the item's newest version is a
///    roaring delta, the chain down to its terminal (`Item.chain`) is
///    folded by `rollupRoaring` into a synthetic state (config triggers
///    permitting) or emitted verbatim. Only that head chain is ever
///    folded: everything below its terminal is shadowed history, and
///    re-rolling it would regenerate a synthetic state under the live
///    terminal on every compaction until GC collapses the chain.
///
/// `track` consumes and decides; the Emitter (passed per call) only
/// knows how to emit, and runs `retain` over what it is handed.
pub const VersionTracker = struct {
    /// Retention verdict for one version. `.stale` means kept but
    /// shadowed — the caller charges the version's own size to the
    /// stale accounting.
    const Update = enum { retain, stale, discard };

    allocator: std.mem.Allocator,
    can_discard_versions: bool,
    global_min_read_ts: u64,

    // Retention state, reset by `startItem`: one item is one primary.
    stale_below: u64 = 0,
    discard_below: u64 = 0,
    /// The versions of the item being emitted that retention keeps.
    kept: std.ArrayList(Version) = .empty,
    /// What a rollup emits: the synthetic state, then the item's
    /// versions below the delta it replaced.
    rolled: std.ArrayList(Version) = .empty,

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
        self.kept.deinit(self.allocator);
        self.rolled.deinit(self.allocator);
    }

    /// Open retention for a new item (a new primary).
    fn startItem(self: *VersionTracker) void {
        // stale_below is only used for tracking stale bytes in the
        // table. discard_below is for discarding versions.
        self.stale_below = 0;
        self.discard_below = 0;
    }

    /// Whether one version of the open item is retained, discarded, or
    /// retained but stale (eligible for discard later). Versions arrive
    /// newest first.
    fn retain(self: *VersionTracker, ver: Version) Update {
        if (ver.ts < self.discard_below) {
            return .discard;
        }
        const stale = ver.ts < self.stale_below;

        if (ver.meta.isState() or ver.meta.isDeleted()) {
            if (self.stale_below == 0) {
                self.stale_below = ver.ts;
            }

            if (self.can_discard_versions and
                ver.ts <= self.global_min_read_ts)
            {
                self.discard_below = ver.ts;
                if (ver.meta.isDeleted()) return .discard;
            }
        }

        return if (stale) .stale else .retain;
    }

    /// Every consumed item enters here, in merge order. Its values are
    /// valid for the whole call: the merge holds their storage until
    /// its own `next`, which is what lets the rollup's peek below yield
    /// for I/O with the chain still borrowed.
    fn track(t: *VersionTracker, e: *Tree.Emitter, item: Item) !void {
        if (foldable(e, item)) |chain| return t.rollupRoaring(e, item, chain);
        try e.emitOne(item);
    }

    /// The chain a rollup may fold: the item's versions down to and
    /// including the first terminal, when rollup is on, the newest
    /// version is a roaring delta, and nothing foreign sits inside the
    /// chain — every version is a roaring delta, or the terminal is a
    /// tombstone (its encoding is not looked at) or a roaring state.
    /// Null otherwise: the item is emitted verbatim. A lone state is
    /// already rolled, raw rows are not ours, and everything below the
    /// terminal is shadowed history that is never folded.
    fn foldable(e: *const Tree.Emitter, item: Item) ?Item {
        if (!e.opts.rollup_enabled) return null;

        const newest = item.newest();
        if (!(newest.meta.isRoaring() and newest.meta.isDelta())) return null;

        const chain = item.chain();
        for (chain.versions.slice()) |ver| {
            if (ver.meta.isRoaring() and ver.meta.isDelta()) continue;
            if (ver.meta.isDeleted()) continue;
            if (ver.meta.isRoaring() and ver.meta.isState()) continue;
            return null;
        }
        return chain;
    }

    /// Whether `chain` ends in a terminal (a state or a tombstone), as
    /// against running out of versions on a delta.
    fn hasTerminal(chain: Item) bool {
        const vers = chain.versions.slice();
        return !vers[vers.len - 1].meta.isDelta();
    }

    /// Resolve `chain`, the foldable head of `item`: fold it into a
    /// synthetic state when policy allows, else emit the item verbatim.
    /// The synthetic state carries the newest delta's timestamp,
    /// REPLACING that delta: readers at read_ts >= t stop there (the
    /// point of rollup); readers below t skip it and fold the retained
    /// chain, emitted unchanged beneath — MVCC and GC semantics are
    /// untouched, the state is just a new terminal for `retain`. An
    /// empty fold emits a tombstone instead: 0 bytes vs zroar's 64-byte
    /// empty bitmap, and bottom-level GC can then drop the key outright.
    fn rollupRoaring(
        t: *VersionTracker,
        e: *Tree.Emitter,
        item: Item,
        chain: Item,
    ) !void {
        const opts = &e.opts;
        const alloc = e.tree.allocator;
        const have_state = hasTerminal(chain);
        const n_deltas = chain.versions.len() - @intFromBool(have_state);

        // Policy: fold when the chain is long enough, and — when its
        // base is not among the inputs (no terminal in the chain) —
        // when a trigger justifies peeking below the destination for
        // it (see config.zig's rollup_* knobs).
        const roll = n_deltas >= opts.rollup_min_deltas and
            (have_state or
                e.dest >= opts.rollup_at_level or
                n_deltas >= opts.rollup_max_deltas);
        if (!roll) return e.emitOne(item);

        var external: ?zroar.Bitmap = null;
        defer if (external) |*eb| eb.deinit();

        if (!have_state) {
            // The oldest ts in the chain bounds the peek: the base must
            // sit strictly below the chain we hold. HLC timestamps are
            // never 0, so the -1 cannot wrap.
            const chain_vers = chain.versions.slice();
            const oldest = chain_vers[chain_vers.len - 1].ts;
            std.debug.assert(oldest > 0);

            external = e.tree.rollupBitmap(alloc, .{
                .key = item.key,
                .read_ts = oldest - 1,
                .priority = e.prio,
            }, e.dest + 1) catch |err| switch (err) {
                // The chain below can't be folded (mixed or corrupt):
                // keep everything verbatim, reads keep folding.
                error.NotRoaring, error.CorruptPosting => {
                    log.warn(
                        "rollup: unfoldable chain below L{d} tree={d}; run " ++
                            "kept verbatim",
                        .{ e.dest, e.tree.tree_id },
                    );
                    return e.emitOne(item);
                },
                else => return err,
            };
        }

        var bm = foldChain(
            alloc,
            chain,
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
                return e.emitOne(item);
            },
            else => return err,
        };
        defer bm.deinit();
        // Canonical minimal buffer for persistence.
        try bm.compact();

        // The bitmap's buffer lives until this returns; emitOne copies
        // it into the builder during the call.
        const newest = chain.newest();
        const synthetic: Version = if (bm.isEmpty()) .{
            .ts = newest.ts,
            .meta = .{ .kind = .deleted, .encoding = .roaring },
            .value = "",
        } else .{
            .ts = newest.ts,
            .meta = .{ .kind = .state, .encoding = .roaring },
            .value = bm.toBuffer(),
        };
        utils.clearRetainCap(&t.rolled);
        try t.rolled.append(t.allocator, synthetic);
        try t.rolled.appendSlice(t.allocator, item.versions.slice()[1..]);
        try e.emitOne(.{
            .key = item.key,
            .versions = .{ .many = t.rolled.items },
        });
        e.stats.rollups += 1;
    }

    /// Fold `chain` into a bitmap — THE fold: compaction
    /// (`rollupRoaring`) and reads (MultiGet.bitmap through
    /// Tree.rollupBitmap) both resolve through `_read.baseBitmap` and
    /// `_read.applyDelta`, which is what keeps rollup read-invariant.
    /// Applies deltas oldest→newest (the chain is newest first),
    /// removes before adds within each. Base priority: a tombstone
    /// terminal folds from empty; a state terminal via fromBufferCopy
    /// (SST values sit at arbitrary offsets, so zroar's aligned
    /// zero-copy path is not usable); else `external` (compaction
    /// read-below) or the empty set. Delta bytes get an O(1) structural
    /// check — without it a truncated element region decodes as an
    /// empty list — but content contracts were enforced at write time
    /// (validateDelta).
    pub fn foldChain(
        alloc: std.mem.Allocator,
        chain: Item,
        external: ?*const zroar.Bitmap,
    ) !zroar.Bitmap {
        const vers = chain.versions.slice();
        const have_state = hasTerminal(chain);
        const terminal: ?Version = if (have_state) vers[vers.len - 1] else null;
        var bm = try _read.baseBitmap(alloc, terminal, external);
        errdefer bm.deinit();

        var i = vers.len - @intFromBool(have_state);
        while (i > 0) {
            i -= 1;
            try _read.applyDelta(&bm, vers[i].value);
        }
        return bm;
    }
};

/// A tree's pending work item, picked by `Forest.bestCompactionCandidate`.
/// `needs_split=true` means the tree has outgrown `Config.splitBytes` and
/// should be split instead of compacted; the `level` field then describes
/// the lowest non-empty level (informational, since split operates across
/// all levels at the median key, not on a single level). `score` is bumped
/// above the natural compaction range so a split preempts any pending
/// compaction in the forest.
pub const CompactCandidate = struct {
    /// The source level. L1 and below write to the level beneath; L0
    /// decides at build time (`compactL0`) whether it merges within
    /// itself or pushes into L1.
    level: u8,
    score: f64,
    needs_split: bool = false,
};

// -----------------------------------------------------------------------
// L0 job selection
// -----------------------------------------------------------------------

pub const L0Job = union(enum) {
    /// Merge these tables, a contiguous run of L0's entries, into one L0
    /// table. A sub-slice of the level's own array: valid under the
    /// hold it was picked under.
    merge: []const TableEntry,
    /// Compact L0 into L1.
    push,
};

/// The job L0 needs now, null when it needs none (see `Config`).
/// `tables` is L0 in its own order, oldest first. A sub-slice of it
/// comes back in `.merge`, so the caller holds the level for as long
/// as it uses one.
///
///   1. L0 holds as many bytes as L1's target (`levelTarget`): push
///      L0 into L1. A push of that size costs about two bytes written
///      per byte drained.
///   2. A merge run is ready: merge it into one L0 table. The run
///      starts after the newest finished table (one that has reached
///      the L0 file size, `opts.memtable_bytes`), takes the tables that
///      follow it in order, and stops once it holds twice that size or
///      runs out of tables. It is ready once it has at least
///      `opts.l0_merge_min_tables` tables. The output is finished if the
///      run reached the size.
///   3. Otherwise L0 is left to fill.
///
/// A run is contiguous and its output is listed at the newest input's
/// raft index, so it stands where its inputs stood: a point read walks
/// L0 newest first and settles a key at the first table holding it,
/// which is only right while newer tables hold newer versions. A
/// finished table is never an input again, so a byte is rewritten
/// within L0 only until it sits in a finished table.
pub fn pickL0Job(opts: _config.Config, tables: []const TableEntry) ?L0Job {
    if (tables.len == 0) return null;

    // A tree with no L1 has nowhere to push.
    if (opts.max_levels > 1) {
        var total: u64 = 0;
        for (tables) |t| total += t.size;
        const push_bytes: u64 = @intCast(@max(levelTarget(opts, 1), 1));
        if (total >= push_bytes) return .push;
    }

    const min_tables: usize = opts.l0_merge_min_tables;
    if (min_tables == 0) return null;
    const full: u64 = opts.memtable_bytes;
    var lo: usize = 0;
    for (tables, 0..) |t, i| {
        if (t.size >= full) lo = i + 1;
    }
    var hi: usize = lo;
    var total: u64 = 0;
    while (hi < tables.len and total < 2 * full) : (hi += 1) {
        total += tables[hi].size;
    }
    if (hi - lo < min_tables) return null;
    return .{ .merge = tables[lo..hi] };
}

/// Score assigned to a tree that needs a split. Comfortably above any
/// natural compaction adjusted-score so splits always rank first.
pub const split_priority_score: f64 = 100.0;

// -----------------------------------------------------------------------
// Level sizing
// -----------------------------------------------------------------------

/// The byte target of `level`, a function of the options alone. Fixed
/// per tree: the bottom level's is `lmax_target_bytes`, and the levels
/// above cascade up by dividing by `level_size_multiplier`, floored at
/// 1 byte so the size/target score is well-defined. L0 has none. A tree
/// splits once its total passes `Config.splitBytes`, five fourths of
/// the Lmax target: what the whole tree weighs with every level at
/// target.
///
/// Why FIXED instead of derived from current data: dynamic targets
/// (RocksDB-style) have a chicken-and-egg problem — early in a tree's
/// life, Lmax is empty, so cascading from current sizes produces big
/// upper-level targets. Stepwise compaction then can't drain those
/// intermediates. Fixed targets sized to the tree's eventual capacity
/// sidestep this entirely. If Lmax briefly exceeds its target before
/// the split kicks in, that's fine — the split will eventually fire.
pub fn levelTarget(opts: _config.Config, level: usize) i64 {
    if (level == 0 or level >= opts.max_levels) return 0;
    var target: i64 = opts.lmax_target_bytes;
    var i = opts.max_levels - 1;
    while (i > level) : (i -= 1) {
        target = @divTrunc(target, opts.level_size_multiplier);
        if (target < 1) return 1;
    }
    return target;
}

/// Targets are a function of the options alone; no lock needed.
pub fn levelTargets(self: *Tree) !Targets {
    const opts = self.live.cur();
    const n = self.live.cur().max_levels;
    const target_sz = try self.allocator.alloc(i64, n);
    errdefer self.allocator.free(target_sz);
    const file_sz = try self.allocator.alloc(i64, n);
    errdefer self.allocator.free(file_sz);

    @memset(target_sz, 0);
    @memset(file_sz, 0);

    for (1..n) |i| target_sz[i] = levelTarget(opts, i);

    // Per-level file sizes: L0 inherits the memtable size; L1 uses
    // `l1_table_bytes`; each level below ramps by
    // `table_size_multiplier` up to `max_table_bytes`. Compaction is
    // strictly stepwise (L0→L1, L1→L2, …) so there's no separate
    // base-level concept — L1 is always the destination of L0.
    var tsz: i64 = opts.l1_table_bytes;
    file_sz[0] = @intCast(opts.memtable_bytes);
    for (1..n) |j| {
        if (j == 1) {
            file_sz[j] = tsz;
        } else {
            tsz = @min(
                tsz * opts.table_size_multiplier,
                opts.max_table_bytes,
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
    // (L0+L1+…+Lmax) against `Config.splitBytes`, not just Lmax. If
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
    const lmax_tables = self.levels[self.live.cur().max_levels - 1].numTables(rd);
    const split_bytes = self.live.cur().splitBytes();
    if (total > split_bytes and
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
                split_bytes,
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

    const opts = self.live.cur();
    const n = self.live.cur().max_levels;
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
    //
    // L0 scores only when it has a job to run (`pickL0Job`): an L0 that
    // is within its bounds and has nothing to merge is left alone, by
    // design, and scoring it would hand the workers a tree with nothing
    // to build.
    const l0_job = pickL0Job(opts, self.levels[0].entries(rd));
    const l0_tables = self.levels[0].numTables(rd);
    const l0_score = if (l0_job == null)
        0.0
    else
        @as(f64, @floatFromInt(l0_tables * 2)) /
            @as(f64, @floatFromInt(opts.l0_backpressure_start_count));
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
    const l0_merges = l0_job != null and l0_job.? == .merge;

    // Adjusted scores: mirrors Badger/PebbleDB's priority dampening.
    // upperLevel starts at 0; for each level from L1 downward, if
    // prios[upperLevel].adjusted >= 1.0, divide it by the current level's
    // adjusted score (floored at 0.01).
    //
    // An L0→L0 merge does not write into L1, so L1's fullness says
    // nothing about it: its score is left as it is.
    var upper_level: usize = 0;
    for (1..n) |cur_level| {
        if (upper_level == 0 and l0_merges) {
            // L0 to L0 compaction. No score changes. Keep as is.
        } else if (prios[upper_level].adjusted >= 1.0) {
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

    // Idle fallback: nothing scored above 1.0, but L0 has a job. Run it
    // while the workers have nothing better: a merge of a few small
    // tables scores far below 1.0 and would otherwise wait for L0 to
    // fill up. Lower levels by definition need no work when nothing
    // scored ≥ 1.0, so L0 is always the right idle pick.
    if (l0_job != null) {
        self.compact_candidate = .{
            .level = 0,
            .score = prios[0].score,
        };
    } else {
        self.compact_candidate = null;
    }
}

// -----------------------------------------------------------------------
// Compaction execution
// -----------------------------------------------------------------------

/// doCompact runs the full compaction pipeline: buildCompaction (Phase A)
/// then applyCompaction (Phase B). For Raft integration, callers can use
/// buildCompaction and applyCompaction separately. `index` is the log
/// index the outputs are listed under, as for `applyCompaction`.
pub fn doCompact(
    self: *Tree,
    index: u64,
    src_level: u8,
    global_min_read: u64,
) !void {
    var changes = try self.buildCompaction(
        src_level,
        global_min_read,
    );
    defer changes.deinit(self.allocator);
    try self.applyCompaction(index, changes.items);
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
    /// The tunables when the job started: one snapshot for the whole
    /// merge, read per key by the rollup policy.
    opts: _config.Config,
    bld: *Builder,
    group: *fiber.Group(?FlushResult),
    stats: *Metrics,
    file_sz: usize,
    dest: usize,
    prio: vfs.Priority,
    tracker: VersionTracker,

    fn init(
        tree: *Tree,
        bld: *Builder,
        group: *fiber.Group(?FlushResult),
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
            .stats = stats,
            .file_sz = file_sz,
            .dest = dest,
            .prio = prio,
            .opts = tree.live.cur(),
            // Only the bottom level may discard history: lower
            // levels could still contain a version that an early
            // tombstone would otherwise resurrect.
            .tracker = VersionTracker.init(
                tree.allocator,
                global_min_read,
                dest == @as(usize, tree.live.cur().max_levels) - 1,
            ),
        };
    }

    fn deinit(e: *Emitter) void {
        e.tracker.deinit();
    }

    /// Feed one item — one primary with its versions — in merge
    /// (consumption) order. Nothing is held across calls: the merge
    /// keeps the item's storage pinned until its own `next`.
    fn push(e: *Emitter, item: Item) !void {
        e.stats.keys_in += item.versions.len();
        for (item.versions.slice()) |ver| {
            e.stats.bytes_in += item.key.primary.len + ver.value.len;
        }
        try e.tracker.track(e, item);
    }

    /// Write the versions of `item` that retention keeps as one entry
    /// of the output.
    fn emitOne(e: *Emitter, item: Item) !void {
        const t = &e.tracker;
        t.startItem();
        utils.clearRetainCap(&t.kept);
        var stale_bytes: usize = 0;
        for (item.versions.slice()) |ver| {
            const nbytes = item.key.primary.len + ver.value.len;
            switch (t.retain(ver)) {
                .discard => {
                    e.stats.dropped_bytes += nbytes;
                    continue;
                },
                .stale => stale_bytes += nbytes,
                .retain => {},
            }
            try t.kept.append(t.allocator, ver);
            e.stats.keys_out += 1;
            e.stats.bytes_out += nbytes;
        }
        if (t.kept.items.len == 0) return;

        if (e.bld.table_bytes.items.len >= e.file_sz) {
            // An item is a whole primary, so splitting the output here
            // never separates a primary's versions across tables. That
            // matters: a table holding a newer version of a key could
            // otherwise be compacted below a table holding an older
            // one, and the read path expects newer versions at higher
            // levels so it can stop at the first hit.
            try e.tree.flushBuilderOutput(e.bld, e.group);
            e.bld.reset();
        }
        try e.bld.addItem(.{
            .key = item.key,
            .versions = .{ .many = t.kept.items },
        });
        if (stale_bytes > 0) {
            e.bld.addStaleBytes(stale_bytes);
            e.stats.stale_bytes += stale_bytes;
        }
    }
};

/// The source tables of an L`src`→L`dest` compaction, pinned (a ref
/// each) and caller-owned: release with `Level.releaseTables`. Null
/// when the level is empty.
///
/// L≥1: pick a RANDOM single table. Always picking items[0] (the
/// smallest-key table) starves higher key ranges and biases write
/// amplification toward the left edge of the keyspace.
/// L0: take the oldest-first prefix that brings total compaction
/// input bytes (selected L0 + their L1 overlap) up to
/// `l0_compaction_max_bytes`. The table that crosses the budget
/// is included; we stop afterwards. Newer L0 tables stay put and
/// continue to shadow the compacted versions on read.
fn pickSourceTables(
    self: *Tree,
    src: usize,
    dest: usize,
) !?[]TableEntry {
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
        if (items.len == 0) return null;
        const snap = try self.allocator.dupe(TableEntry, items);
        for (snap) |e| e.table.incrRef();
        break :ss snap;
    };
    defer Level.releaseTables(self.allocator, src_snapshot);

    if (src != 0) {
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
        return pick;
    }

    const budget = self.live.cur().l0_compaction_max_bytes;
    // The union of the tables taken so far; null until the first.
    var union_kr: ?KeyRange = null;
    var l0_bytes: u64 = 0;
    var n: usize = 0;
    while (n < src_snapshot.len) {
        const e = src_snapshot[n];
        const tkr = KeyRange{
            .left = e.table.smallest,
            .right = e.table.biggest,
        };
        if (union_kr) |*u| u.extend(tkr) else union_kr = tkr;
        l0_bytes += e.table.fileSize();
        n += 1;

        const overlap = ov: {
            const held = self.mu.hold(.shared, @src());
            defer held.release();
            break :ov try self.levels[dest].overlappingTables(
                held,
                self.allocator,
                union_kr.?,
            );
        };
        defer Level.releaseTables(self.allocator, overlap);
        var bot_bytes: u64 = 0;
        for (overlap) |oe| bot_bytes += oe.table.fileSize();
        if (l0_bytes + bot_bytes >= budget) break;
    }
    const pick = try self.allocator.dupe(TableEntry, src_snapshot[0..n]);
    for (pick) |e| e.table.incrRef();
    return pick;
}

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
    if (dest >= self.live.cur().max_levels) return .empty;

    const src_entries_slice = (try pickSourceTables(self, src, dest)) orelse
        return .empty;
    defer Level.releaseTables(self.allocator, src_entries_slice);

    // --- compute union key range of all src tables ---
    var kr: ?KeyRange = null;
    for (src_entries_slice) |e| {
        const tkr = KeyRange{
            .left = e.table.smallest,
            .right = e.table.biggest,
        };
        if (kr) |*r| r.extend(tkr) else kr = tkr;
    }

    // --- collect overlapping dest (bot) entries ---
    const bot_entries_slice = blk: {
        const held = self.mu.hold(.shared, @src());
        defer held.release();
        break :blk try self.levels[dest].overlappingTables(
            held,
            self.allocator,
            kr orelse return .empty,
        );
    };
    defer Level.releaseTables(self.allocator, bot_entries_slice);

    return self.mergeAndWrite(.{
        .src_level = src,
        .dest_level = dest,
        .src = src_entries_slice,
        .bot = bot_entries_slice,
        .file_sz = @intCast(t.file_sz[dest]),
        .prio = prio,
    }, global_min_read);
}

/// Build the job L0 needs now (`pickL0Job`), whichever it is: a push
/// into L1 is `buildCompaction` from L0; a merge rewrites one run of
/// small L0 tables as ONE L0 table. Decided here, at build time, not at
/// the pick that scored the tree: a flush in between may have changed
/// which one L0 wants. Returns no changes when it wants neither.
///
/// A merge's output is listed under the newest input's `raft_index`,
/// not the applying entry's: it holds what its inputs held, so it stands
/// where they stood in L0's order. A flush that lands while this builds
/// is newer than every input and keeps sorting after the output.
pub fn compactL0(
    self: *Tree,
    global_min_read: u64,
) !std.ArrayList(_manifest.SSTableChange) {
    // Pinned under the hold, as in `buildCompaction`.
    const run = ss: {
        const held = self.mu.hold(.shared, @src());
        defer held.release();
        const job = pickL0Job(self.live.cur(), self.levels[0].entries(held)) orelse
            return .empty;
        const picked = switch (job) {
            .merge => |r| r,
            .push => return self.buildCompaction(0, global_min_read),
        };
        const pick = try self.allocator.dupe(TableEntry, picked);
        for (pick) |e| e.table.incrRef();
        break :ss pick;
    };
    defer Level.releaseTables(self.allocator, run);

    return self.mergeAndWrite(.{
        .src_level = 0,
        .dest_level = 0,
        .src = run,
        .bot = &.{},
        // One output, whatever it weighs: an L0 table has no size
        // limit, and a second output would share its raft_index with
        // the first.
        .file_sz = std.math.maxInt(usize),
        .prio = .p1,
        .out_raft_index = run[run.len - 1].raft_index,
    }, global_min_read);
}

/// The inputs and the shape of one compaction's output.
pub const MergeJob = struct {
    src_level: usize,
    dest_level: usize,
    /// Pinned by the caller for the duration of the build.
    src: []const TableEntry,
    /// The destination level's tables the sources overlap; rewritten
    /// with them. Empty for an L0→L0 merge.
    bot: []const TableEntry,
    /// An output table is cut once it reaches this size.
    file_sz: usize,
    prio: vfs.Priority,
    /// The `raft_index` the outputs are listed under; 0 = the index of
    /// the entry that applies the job (every job but an L0→L0 merge).
    out_raft_index: u64 = 0,
};

/// Merge `job.src` with `job.bot`, write the output SSTs, and return the
/// manifest changes that swap the inputs for the outputs.
pub fn mergeAndWrite(
    self: *Tree,
    job: MergeJob,
    global_min_read: u64,
) !std.ArrayList(_manifest.SSTableChange) {
    const src = job.src_level;
    const dest = job.dest_level;
    const prio = job.prio;
    const src_entries_slice = job.src;
    const bot_entries_slice = job.bot;

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

    const table_key: TableKey = .{
        .namespace = self.namespace,
        .table_id = self.tableId(),
    };
    for (src_entries_slice, 0..) |e, i|
        all_iters[i] = .{
            .table = TableIterator.init(
                self.allocator,
                e.table,
                prio,
                table_key,
            ),
        };
    for (bot_entries_slice, 0..) |e, i|
        bot_iters[i] = TableIterator.init(
            self.allocator,
            e.table,
            prio,
            table_key,
        );
    all_iters[src_entries_slice.len] = .{
        .concat = ConcatIterator.init(bot_iters),
    };

    var merge = MergeIterator.init(self.allocator, all_iters) catch |err| {
        for (all_iters) |*it| it.deinit();
        return err;
    };
    defer merge.deinit();
    try merge.seekRange(self.key_range.left, self.key_range.right);

    // --- output file IDs produced by this compaction ---
    var output_file_ids: std.ArrayList(u64) = .empty;
    defer output_file_ids.deinit(self.allocator);

    // --- merge loop ---
    const opts = self.live.cur();
    const cfg = _table.builder.Config{
        .target_block_size = opts.block_bytes,
        .codec = opts.codecFor(dest),
        .priority = prio,
        .key_kind = self.key_kind,
    };
    const file_sz = job.file_sz;

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
        &stats,
        file_sz,
        dest,
        prio,
        global_min_read,
    );
    defer emitter.deinit();

    while (merge.itemRef()) |item| {
        try emitter.push(item);
        try merge.next();
    }

    // Flush any remaining data.
    try self.flushBuilderOutput(&bld, &flush_group);

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
    var dir_sync = try fiber.spawn(.{}, vfs.syncDir, .{self.live.cur().dir});
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
        changes.items[changes.items.len - 1].raft_index = job.out_raft_index;
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
) !void {
    var tb = try bld.finish();
    defer tb.deinit();
    if (tb.bytes.len == 0) return;

    const id = self.manifest.newFileId();
    const path = try _forest.sstPath(self.allocator, self.live.cur().dir, id);
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
