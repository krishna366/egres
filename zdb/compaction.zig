// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Compaction for a zdb Tree: the per-level size targets, the scoring
//! that picks which level to compact next, and Phase A of the pipeline,
//! which merges a level into the one below it and writes the output SST
//! files through the Emitter (emitter.zig, with the VersionTracker
//! that decides what of each primary is written). Every function here
//! takes `self: *Tree` and is aliased into `Tree` in tree.zig, so
//! callers keep using method syntax on a tree.

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const _manifest = @import("manifest.zig");
const _level_handler = @import("level.zig");
const _table = @import("table.zig");
const vfs = @import("vfs");
const _metrics = @import("metrics.zig");
const _tree = @import("tree.zig");
const _config = @import("config.zig");
const _emitter = @import("emitter.zig");

const Tree = _tree.Tree;
const min_lmax_tables_for_split = Tree.min_lmax_tables_for_split;
const TableIterator = _table.TableIterator;
const Level = _level_handler.Level;
const Lock = _level_handler.Lock;
pub const TableEntry = _level_handler.TableEntry;
const Key = @import("kv.zig").Key;
const TableKey = @import("kv.zig").TableKey;
const KeyRange = @import("kv.zig").KeyRange;
const MergeIterator = _table.merge_iterator.MergeIterator;
const ConcatIterator = _table.merge_iterator.ConcatIterator;
const Iterator = _table.merge_iterator.Iterator;
pub const Emitter = _emitter.Emitter;
pub const VersionTracker = _emitter.VersionTracker;
const Metrics = _emitter.Metrics;

const log = std.log.scoped(.zdb_tree);

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

pub const Targets = struct {
    allocator: std.mem.Allocator,
    target_sz: []i64,

    pub fn deinit(self: *Targets) void {
        self.allocator.free(self.target_sz);
    }
};

const LevelPriority = struct {
    level: usize,
    score: f64,
};

/// A tree's pending work item, picked by `Forest.bestCompactionCandidate`.
/// What a tree wants done next, decided by `updateCompactCandidate_`
/// from the levels as they are and kept fresh by every install. The
/// forest ranks trees by `score`; the builder runs `job` on `tables`
/// without picking again.
///
/// A `split` means the tree has outgrown `Config.splitBytes` and should
/// be split instead of compacted; `level` then describes the lowest
/// non-empty level (informational, since split operates across all
/// levels at the median key, not on a single level), and `score` is
/// bumped above the natural compaction range so a split preempts any
/// pending compaction in the forest.
pub const CompactCandidate = struct {
    /// The source level. L1 and below write to the level beneath; L0
    /// merges within itself or pushes into L1, as `job` says.
    level: u8,
    score: f64,
    /// Names the tables, nothing more: they are not pinned, so a table
    /// unlisted since the refresh may be freed. The builder looks each
    /// one up by id in its level and gives up if one is gone. Under the
    /// tree's lock, freed at the next refresh (`clearCandidate`).
    job: Job,
};

/// What a level wants done (`pickJob`), or a split of the whole tree.
/// Picked as views into the level's own array, valid under the hold it
/// was picked under; the candidate holds a copy.
pub const Job = union(enum) {
    /// One table that goes into the level beneath as it is.
    move_down: TableEntry,
    /// Sources to rewrite with what they overlap in the level beneath.
    merge_down: []const TableEntry,
    /// An L0 run to rewrite as one L0 table.
    merge_l0: []const TableEntry,
    /// The tree has outgrown `Config.splitBytes`: split it instead of
    /// compacting. Never picked for a level.
    split,

    fn tables(self: *const Job) []const TableEntry {
        return switch (self.*) {
            .move_down => |*e| e[0..1],
            .merge_down, .merge_l0 => |ts| ts,
            .split => &.{},
        };
    }
};

// -----------------------------------------------------------------------
// L0 job selection
// -----------------------------------------------------------------------

/// The job L0 needs now, null when it needs none (see `Config`), from
/// L0 alone: `tables` is L0 in its own order, oldest first, and a
/// sub-slice of it comes back, so the caller holds the level for as
/// long as it uses one. A push comes back as `merge_down` of all of
/// L0; `pickJob` narrows it against L1 to the oldest table's move or
/// the prefix a merge takes.
///
///   1. L0 holds as many bytes as L1's target (`levelTarget`): push
///      L0 into L1. A push of that size costs about two bytes written
///      per byte drained.
///   2. A merge run is ready: merge it into one L0 table. The run
///      starts after the newest finished table (one that has reached
///      the table size, `opts.table_bytes`), takes the tables that
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
pub fn pickL0Job(opts: _config.Config, tables: []const TableEntry) ?Job {
    if (tables.len == 0) return null;

    // A tree with no L1 has nowhere to push.
    if (opts.max_levels > 1) {
        var total: u64 = 0;
        for (tables) |t| total += t.size;
        const push_bytes: u64 = @intCast(@max(levelTarget(opts, 1), 1));
        if (total >= push_bytes) return .{ .merge_down = tables };
    }

    const min_tables: usize = opts.l0_merge_min_tables;
    if (min_tables == 0) return null;
    const full: u64 = opts.table_bytes;
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
    return .{ .merge_l0 = tables[lo..hi] };
}

/// Whether the tree's tables may ever move down without a rewrite:
/// rows and unique entries, whose rewrite above the bottom would only
/// recount stale bytes. A posting index's chains need the rewrite to
/// roll up; an unknown kind gets no assumption.
fn canMove(self: *const Tree) bool {
    return switch (self.kind) {
        .rows, .unique_index => true,
        .posting_index, .unknown, _ => false,
    };
}

/// Whether table `e` of the level above `dest` can go into `dest` as
/// it is (`moveDown`) instead of being rewritten. Sequential keys
/// (egids, timestamps) make this the common case: every flush covers
/// a new key range, so a push and every step down after it would
/// otherwise copy each byte into an identical file.
///
/// Three conditions, so that the rewrite a move skips would have
/// changed nothing:
///   - `e` meets no table of `dest`. A level below L0 is a sorted run.
///   - `dest` is not the bottom level. Only the bottom rewrite discards
///     versions below the read floor and drops tombstones (the tracker
///     is told so), so a moved table still gets both, later. It also
///     gives the bulk of the data the bottom codec; moved tables above
///     it keep their own (each records it).
///   - The tree's kind allows it (`canMove`).
///
/// One table moves at a time, and from L0 only the oldest: a version
/// in a lower level must be older than every version of its key above
/// it, and L0 tables can share keys, so a younger L0 table moved past
/// an older one would be shadowed by it. Below L0 a table shares no key
/// with its level, and every level above holds only newer versions.
fn movable(
    self: *Tree,
    held: Lock.Shared,
    dest: usize,
    e: TableEntry,
) bool {
    if (dest + 1 >= self.live.cur().max_levels) return false;
    if (!canMove(self)) return false;
    return !self.levels[dest].overlapsAny(held, tableRange(e));
}

fn tableRange(e: TableEntry) KeyRange {
    return .{ .left = e.table.smallest, .right = e.table.biggest };
}

/// Score of a level with a movable table: above any size score, below
/// a split's.
pub const move_priority_score: f64 = 50.0;

/// Score assigned to a tree that needs a split. Comfortably above any
/// natural compaction score so splits always rank first.
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
    @memset(target_sz, 0);
    for (1..n) |i| target_sz[i] = levelTarget(opts, i);

    // Every level's files are cut at `opts.table_bytes`; only the byte
    // targets differ by level. Compaction is strictly stepwise (L0→L1,
    // L1→L2, …) so there's no separate base-level concept — L1 is
    // always the destination of L0.
    return .{ .allocator = self.allocator, .target_sz = target_sz };
}

// -----------------------------------------------------------------------
// Compaction selection
// -----------------------------------------------------------------------

/// updateCompactCandidate recomputes which level most needs compaction and
/// stores the result in self.compact_candidate. Called after open and after
/// every compaction so the candidate is always fresh.
pub fn updateCompactCandidate(self: *Tree) !void {
    // Reads every level's size and writes the verdict, under one
    // exclusive hold. Installs call this after releasing theirs.
    const held = self.mu.hold(.exclusive, @src());
    defer held.release();
    try self.updateCompactCandidate_(held);
}

/// The verdict itself. Caller holds `mu` exclusively — `held` is the
/// proof; the level reads below use it as shared.
pub fn updateCompactCandidate_(self: *Tree, held: Lock.Exclusive) !void {
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
    // the split verdict stays, buildSplit returns null, proposeSplit skips.
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
        const level: u8 = @intCast(linfo.level);
        try setCandidate(self, level, split_priority_score, .split);
        return;
    }

    var t = try self.levelTargets();
    defer t.deinit();

    const opts = self.live.cur();
    const n = self.live.cur().max_levels;
    const prios = try self.allocator.alloc(LevelPriority, n);
    defer self.allocator.free(prios);

    // What each level would do (`pickJob`), decided once here: the
    // winner's job is stored with the verdict and the builder runs it
    // as it is.
    const jobs = try self.allocator.alloc(?Job, n);
    defer self.allocator.free(jobs);
    for (jobs, 0..) |*job, level| job.* = pickJob(self, rd, level);

    // A level with a table that can move down (`Job.move_down`: it meets
    // nothing in the level below) outranks every rewrite. A move costs
    // one manifest entry, so there is nothing to wait for, and it
    // empties the upper levels of sequential data at no write cost.
    // Measured at 32 load streams: 165k rows/s with this rule against
    // 153k on raw scores alone, whose L0 picks were 1 GiB rewrites.
    for (jobs, 0..) |j, level| {
        const job = j orelse continue;
        if (job != .move_down) continue;
        try setCandidate(self, @intCast(level), move_priority_score, job);
        return;
    }

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
    const l0_job = jobs[0];

    const l0_tables = self.levels[0].numTables(rd);
    const l0_score = if (l0_job == null)
        0.0
    else
        @as(f64, @floatFromInt(l0_tables * 2)) /
            @as(f64, @floatFromInt(opts.l0_backpressure_start_count));
    prios[0] = .{ .level = 0, .score = l0_score };

    // L≥1: size-based score.
    for (1..n) |j| {
        const target = t.target_sz[j];
        const score = if (target > 0)
            @as(f64, @floatFromInt(self.levels[j].getTotalSize(rd))) /
                @as(f64, @floatFromInt(target))
        else
            0.0;
        prios[j] = .{ .level = j, .score = score };
    }
    // The highest raw score above 1.0 wins, the bottom level aside:
    // the fullest level by ratio is drained first, as RocksDB does.
    // Badger's adjustment, dividing a level's score by the one below
    // it, was here once; it boosts a level over an under-full one, and
    // under a sustained load that let L1 win every pick over an L0 of
    // 65 tables, with the write brake throttling the load meanwhile.
    var winner: ?LevelPriority = null;
    for (prios[0 .. n - 1]) |p| {
        if (p.score <= 1.0) continue;
        if (winner == null or p.score > winner.?.score) winner = p;
    }

    if (winner) |w| {
        // A level that scored holds tables and is not the bottom, so
        // it has a job.
        try setCandidate(self, @intCast(w.level), w.score, jobs[w.level].?);
        return;
    }

    // Idle fallback: nothing scored above 1.0, but L0 has a job. Run it
    // while the workers have nothing better: a merge of a few small
    // tables scores far below 1.0 and would otherwise wait for L0 to
    // fill up. Lower levels by definition need no work when nothing
    // scored ≥ 1.0, so L0 is always the right idle pick.
    if (l0_job) |job| {
        try setCandidate(self, 0, prios[0].score, job);
    } else {
        self.clearCandidate();
    }
}

/// The verdict, with a copy of `job`'s tables in place of the level
/// views it was picked as.
fn setCandidate(self: *Tree, level: u8, score: f64, job: Job) !void {
    const owned: Job = switch (job) {
        .move_down => |e| .{ .move_down = e },
        .merge_down => |ts| .{
            .merge_down = try self.allocator.dupe(TableEntry, ts),
        },
        .merge_l0 => |ts| .{
            .merge_l0 = try self.allocator.dupe(TableEntry, ts),
        },
        .split => .split,
    };
    self.clearCandidate();
    self.compact_candidate = .{ .level = level, .score = score, .job = owned };
}

pub fn clearCandidate(self: *Tree) void {
    const c = self.compact_candidate orelse return;
    switch (c.job) {
        .merge_down, .merge_l0 => |ts| self.allocator.free(ts),
        .move_down, .split => {},
    }
    self.compact_candidate = null;
}

/// The job level `src` wants now, null when it wants none. The one
/// place that decides what compacts: `updateCompactCandidate_` stores
/// the answer and `buildCompaction` runs it.
///
/// L0 asks `pickL0Job`: a merge run is returned as it is; a push moves
/// the oldest table alone when it can (`movable`), else pushes the
/// oldest-first prefix that brings total compaction input bytes
/// (selected L0 + their L1 overlap) up to `job_max_bytes`.
/// The table that crosses the budget is included; after it, only
/// tables that add no L1 overlap (`l0MergeDownOldest`).
/// Newer L0 tables stay put and continue to shadow the compacted
/// versions on read.
///
/// L≥1 hands over ONE table, at random among those that can move down
/// as they are when there are any, else among all. Random rather than
/// the first: always taking the smallest-key table starves higher key
/// ranges and biases write amplification to the keyspace's left edge.
fn pickJob(self: *Tree, held: Lock.Shared, src: usize) ?Job {
    const entries = self.levels[src].entries(held);
    if (entries.len == 0) return null;

    const dest = src + 1;
    if (src == 0) {
        const job = pickL0Job(self.live.cur(), entries) orelse return null;
        if (job != .merge_down) return job;
        if (movable(self, held, dest, entries[0]))
            return .{ .move_down = entries[0] };
        return .{ .merge_down = l0MergeDownOldest(self, held, entries) };
    }

    if (dest >= self.live.cur().max_levels) return null;

    var movables: std.ArrayList(TableEntry) = .empty;
    defer movables.deinit(self.allocator);
    for (entries) |e| {
        if (movable(self, held, dest, e))
            movables.append(self.allocator, e) catch unreachable;
    }

    const rng_impl: std.Random.IoSource = .{ .io = utils.context.g().io };
    const rng = rng_impl.interface();
    if (movables.items.len > 0) {
        const i = rng.uintLessThan(usize, movables.items.len);
        return .{ .move_down = movables.items[i] };
    }
    const i = rng.uintLessThan(usize, entries.len);
    return .{ .merge_down = entries[i..][0..1] };
}

/// The oldest L0 tables a `merge_down` takes (`pickJob`): `entries` is
/// L0 oldest first. Past the budget a table is still taken while it
/// widens no L1 overlap and L0's own bytes are under the budget: the
/// job rewrites the same L1 tables either way. Without that, an L1
/// larger than the budget let each push drain a single table.
fn l0MergeDownOldest(
    self: *Tree,
    held: Lock.Shared,
    entries: []const TableEntry,
) []const TableEntry {
    const budget = self.live.cur().job_max_bytes;
    // The union of the tables taken so far; null until the first.
    var union_kr: ?KeyRange = null;
    var l0_bytes: u64 = 0;
    var bot_bytes: u64 = 0;
    var n: usize = 0;
    while (n < entries.len) : (n += 1) {
        var kr = tableRange(entries[n]);
        if (union_kr) |u| kr.extend(u);

        var next_bot: u64 = 0;
        for (self.levels[1].overlapping(held, kr)) |oe|
            next_bot += oe.table.fileSize();

        // The first table is always taken, so a job makes progress.
        if (n > 0 and l0_bytes + bot_bytes >= budget and
            (next_bot > bot_bytes or l0_bytes >= budget)) break;

        union_kr = kr;
        bot_bytes = next_bot;
        l0_bytes += entries[n].table.fileSize();
    }
    return entries[0..n];
}

/// buildCompaction is Phase A: the candidate's job (`CompactCandidate`),
/// built into new SST files, as the manifest SSTableChange list the
/// caller proposes (through Raft) and then applies (applyCompaction).
/// Nothing is picked here: the candidate was computed from the levels
/// as they are, and every install recomputes it. The job's tables are
/// looked up by id under one hold; one that is gone means an install
/// landed between its refresh and this read, and there is nothing to
/// build until the next refresh.
///
/// A merge's output is listed under the newest input's `raft_index`,
/// not the applying entry's: it holds what its inputs held, so it stands
/// where they stood in L0's order. A flush that lands while this builds
/// is newer than every input and keeps sorting after the output.
///
/// `snapshot_ts` is the oldest timestamp a reader may still ask for
/// (`Forest.snapshot_ts`); anything strictly below it is unobservable
/// and eligible to be discarded. Pass 0 to discard nothing.
pub fn buildCompaction(
    self: *Tree,
    snapshot_ts: u64,
) !std.ArrayList(_manifest.SSTableChange) {
    // Pinned under the hold: a remote-built applyCompaction landing
    // mid-build (leadership flip) must not free tables we're about to
    // read. decrRef at zero frees memory only — the file stays — so a
    // pinned table remains readable. The hold covers the lookup only:
    // a build takes seconds to minutes, and the tree's exclusive holders
    // (a flush's apply, on the raft apply loop) must not wait on it.
    const cand, const srcs = blk: {
        const held = self.mu.hold(.shared, @src());
        defer held.release();

        const cand = self.compact_candidate orelse return .empty;
        if (cand.job == .split) return .empty;

        // The listed entries, by id: the candidate's own may name a
        // table that has since been unlisted and freed.
        const entries = self.levels[cand.level].entries(held);
        const named = cand.job.tables();

        const srcs = try self.allocator.alloc(TableEntry, named.len);
        errdefer self.allocator.free(srcs);

        for (named, srcs) |n, *e| {
            e.* = findEntry(entries, n.id) orelse {
                log.info("compact tree={d} L{d} stale: sst={d} gone", .{
                    self.tree_id, cand.level, n.id,
                });
                self.allocator.free(srcs);
                return .empty;
            };
        }
        for (srcs) |e| e.table.incrRef();
        break :blk .{ cand, srcs };
    };
    defer Level.releaseTables(self.allocator, srcs);

    const src: usize = cand.level;
    switch (cand.job) {
        .move_down => return moveDown(self, src, src + 1, srcs[0]),
        .merge_down => return mergeDown(self, src, srcs, snapshot_ts),
        .merge_l0 => return self.mergeAndWrite(.{
            .src_level = 0,
            .dest_level = 0,
            .src = srcs,
            .bot = &.{},
            .prio = .p1,
            .out_raft_index = srcs[srcs.len - 1].raft_index,
        }, snapshot_ts),
        .split => unreachable,
    }
}

fn findEntry(entries: []const TableEntry, id: u64) ?TableEntry {
    for (entries) |e| {
        if (e.id == id) return e;
    }
    return null;
}

/// `srcs` of L`src` rewritten with the tables they overlap in the level
/// beneath. Stepwise: every level pushes to the level immediately below
/// it, including L0 → L1. Routing L0 directly to Lbase used to produce
/// huge cross-level compactions and made intermediate levels
/// effectively unused; routing one step at a time keeps each
/// compaction's input set bounded by the destination level's
/// overlapping range.
fn mergeDown(
    self: *Tree,
    src: usize,
    srcs: []const TableEntry,
    snapshot_ts: u64,
) !std.ArrayList(_manifest.SSTableChange) {
    const dest = src + 1;
    const prio: vfs.Priority = if (src == 0) .p1 else .p2;

    var kr: ?KeyRange = null;
    for (srcs) |e| {
        const tkr = tableRange(e);
        if (kr) |*r| r.extend(tkr) else kr = tkr;
    }

    const bot = blk: {
        const held = self.mu.hold(.shared, @src());
        defer held.release();
        break :blk try self.levels[dest].overlappingTables(
            held,
            self.allocator,
            kr orelse return .empty,
        );
    };
    defer Level.releaseTables(self.allocator, bot);

    return self.mergeAndWrite(.{
        .src_level = src,
        .dest_level = dest,
        .src = srcs,
        .bot = bot,
        .prio = prio,
    }, snapshot_ts);
}

/// A trivial move: list `e` in `dest` and unlist it from `src`, in one
/// entry, the file untouched. The manifest applies creates before
/// deletes, so a moved file's listing count goes 1, 2, 1 and it is never
/// retired. The create keeps the table's `raft_index`: the file's place
/// in L0's order is where it was first listed, and the manifest keeps
/// that record for a file it already knows.
fn moveDown(
    self: *Tree,
    src: usize,
    dest: usize,
    e: TableEntry,
) !std.ArrayList(_manifest.SSTableChange) {
    // The candidate said so when it was picked; nothing has changed
    // `dest` since without refreshing it.
    {
        const held = self.mu.hold(.shared, @src());
        defer held.release();
        std.debug.assert(movable(self, held, dest, e));
    }

    var changes = try std.ArrayList(_manifest.SSTableChange).initCapacity(
        self.allocator,
        2,
    );
    errdefer changes.deinit(self.allocator);
    changes.appendAssumeCapacity(_manifest.deleteSSTableChange(
        e.id,
        self.namespace,
        self.tree_id,
        @intCast(src),
    ));

    var create = _manifest.createSSTableChange(
        e.id,
        self.namespace,
        self.tree_id,
        @intCast(dest),
    );
    create.raft_index = e.raft_index;
    changes.appendAssumeCapacity(create);

    log.info("move tree={d} L{d}->L{d} sst={d} bytes={d}", .{
        self.tree_id, src, dest, e.id, e.table.fileSize(),
    });
    return changes;
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
    prio: vfs.Priority,
    /// The `raft_index` the outputs are listed under; 0 = the index of
    /// the entry that applies the job (every job but an L0→L0 merge).
    out_raft_index: u64 = 0,
};

/// One key-range slice of a job: the half-open range every source is
/// read in, and the bottom tables it rewrites, already opened as one
/// concat. A boundary is a bottom table's smallest key, so no version
/// of a primary is split across two slices and each slice's outputs
/// sit between its neighbours' in key order. The range bounds the L0
/// sources, which span the whole tree; the concat bounds only the L1
/// side.
const Partition = struct {
    range: KeyRange,
    bot: ConcatIterator,
};

/// Cut `job.bot` into up to `max` runs of consecutive tables, one
/// partition each. The first starts at the tree's left edge and the
/// last ends at its right, so the partitions cover the tree exactly;
/// with no bottom tables, or `max` of 1, there is one partition.
/// `bot_iters` is one iterator per bottom table, the caller's to free
/// once every partition's merge has released them.
fn partitions(
    self: *Tree,
    job: MergeJob,
    max: usize,
    bot_iters: []TableIterator,
    out: []Partition,
) []Partition {
    const bot = job.bot;
    std.debug.assert(bot_iters.len == bot.len);
    const table_key: TableKey = .{
        .namespace = self.namespace,
        .table_id = self.tableId(),
    };
    for (bot, bot_iters) |e, *it| {
        it.* = TableIterator.init(self.allocator, e.table, job.prio, table_key);
    }
    const n = @min(@max(max, 1), @max(bot.len, 1), out.len);
    for (out[0..n], 0..) |*part, i| {
        const lo = bot.len * i / n;
        const hi = bot.len * (i + 1) / n;
        part.* = .{
            .range = .{
                .left = if (i == 0)
                    self.key_range.left
                else
                    bot[lo].table.smallest,
                .right = if (i == n - 1)
                    self.key_range.right
                else
                    bot[hi].table.smallest,
            },
            .bot = ConcatIterator.init(bot_iters[lo..hi]),
        };
    }
    return out[0..n];
}

/// Merge `job.src` with `job.bot`, write the output SSTs, and return the
/// manifest changes that swap the inputs for the outputs. An L0 push is
/// split into `l0_push_fibers` partitions along L1 table boundaries
/// (`partitions`), each merged and written by its own fiber: the
/// sources are read once over, each fiber taking its own range, and
/// the outputs are installed by one manifest entry as before.
pub fn mergeAndWrite(
    self: *Tree,
    job: MergeJob,
    snapshot_ts: u64,
) !std.ArrayList(_manifest.SSTableChange) {
    const src = job.src_level;
    const dest = job.dest_level;
    const opts = self.live.cur();

    // The bottom iterators outlive the fibers: each partition's merge
    // releases its own at deinit, the slice is freed here after.
    const bot_iters = try self.allocator.alloc(TableIterator, job.bot.len);
    defer self.allocator.free(bot_iters);

    var part_buf: [max_push_fibers]Partition = undefined;
    const num_fibers: usize = if (src == 0 and dest == 1) opts.l0_push_fibers else 1;
    const parts = partitions(self, job, num_fibers, bot_iters, &part_buf);

    var group = fiber.Group(?Emitter.Result).init(self.allocator);
    defer group.deinit();
    try group.ensureTotalCapacity(parts.len);

    // A partition not handed to a fiber still owns its concat.
    var spawned: usize = 0;
    errdefer for (parts[spawned..]) |*part| part.bot.deinit();

    for (parts) |part| {
        try group.spawn(.{}, runPartition, .{ self, job, part, snapshot_ts });
        spawned += 1;
    }
    const results = group.wait();

    // Start dir sync while we're waiting for file syncs to be done. We can do
    // dir sync here because files have already been created and written. So,
    // dir should have everything it needs.
    var dir_sync = try fiber.spawn(.{}, vfs.syncDir, .{self.live.cur().dir});
    defer dir_sync.join();

    // The manifest changes: every partition's CREATEs, in partition
    // order and listed at the job's raft index, then the DELETEs of
    // the inputs (the manifest applies creates first whatever the
    // order). The stats are summed.
    var changes: std.ArrayList(_manifest.SSTableChange) = .empty;
    errdefer changes.deinit(self.allocator);

    var stats: Metrics = .{};
    var failed = false;
    for (results) |r_opt| {
        var r = r_opt orelse {
            failed = true;
            continue;
        };
        defer r.changes.deinit(self.allocator);
        for (r.changes.items) |create| {
            var tc = create;
            tc.raft_index = job.out_raft_index;
            try changes.append(self.allocator, tc);
        }
        stats.merge(r.stats);
    }
    if (failed) return error.FlushFailed;
    try reportJob(self, job, parts.len, &stats, snapshot_ts);

    for (job.src) |e| try changes.append(
        self.allocator,
        _manifest.deleteSSTableChange(
            e.id,
            self.namespace,
            self.tree_id,
            @intCast(src),
        ),
    );
    for (job.bot) |e| try changes.append(
        self.allocator,
        _manifest.deleteSSTableChange(
            e.id,
            self.namespace,
            self.tree_id,
            @intCast(dest),
        ),
    );
    return changes;
}

/// A finished job's accounting: the input tables' own key counts and
/// on-disk sizes are added to `stats` as the "expected" baseline, the
/// job is logged, and its rollups counted in the metrics. If keys_in
/// != expected, the merge iterator is losing items and something is
/// wrong. keys_out < keys_in is normal: it's what this compaction
/// discarded.
fn reportJob(
    self: *Tree,
    job: MergeJob,
    num_fibers: usize,
    stats: *Metrics,
    snapshot_ts: u64,
) !void {
    const src = job.src_level;
    const dest = job.dest_level;
    for (job.src) |e| {
        stats.src_keys += @intCast(e.table.index.key_count);
        stats.src_bytes_disk += e.table.fileSize();
    }
    for (job.bot) |e| {
        stats.bot_keys += @intCast(e.table.index.key_count);
        stats.bot_bytes_disk += e.table.fileSize();
    }
    const expected_in = stats.src_keys + stats.bot_keys;
    const expected_bytes_disk =
        stats.src_bytes_disk + stats.bot_bytes_disk;
    const lost_in_iter = expected_in -| stats.keys_in;
    const discarded = stats.keys_in -| stats.keys_out;
    log.info(
        "compact tree={d} L{d}->L{d} fibers={d} src_tables={d} src_keys={d} " ++
            "src_bytes_disk={d} bot_tables={d} bot_keys={d} " ++
            "bot_bytes_disk={d} expected_in={d} expected_bytes_disk={d} " ++
            "keys_in={d} keys_out={d} bytes_in={d} bytes_out={d} " ++
            "lost_in_iter={d} discarded={d} discarded_bytes={d} " ++
            "stale_bytes={d} rollups={d} snapshot_ts={d}",
        .{
            self.tree_id,         src,               dest,
            num_fibers,           job.src.len,       stats.src_keys,
            stats.src_bytes_disk, job.bot.len,       stats.bot_keys,
            stats.bot_bytes_disk, expected_in,       expected_bytes_disk,
            stats.keys_in,        stats.keys_out,    stats.bytes_in,
            stats.bytes_out,      lost_in_iter,      discarded,
            stats.dropped_bytes,  stats.stale_bytes, stats.rollups,
            snapshot_ts,
        },
    );
    if (stats.rollups > 0) {
        try _metrics.g().rollups.incrBy(.{
            .namespace_id = self.namespace,
            .tree_id = self.tree_id,
            .level = @intCast(dest),
        }, stats.rollups);
    }
}

/// The bound on `l0_push_fibers`: partitions live on the stack.
pub const max_push_fibers = 16;

/// One partition of a job on its own fiber: merge the sources over the
/// partition's range with its bottom tables, write the outputs, wait
/// for their syncs. Null on failure, logged here.
fn runPartition(
    self: *Tree,
    job: MergeJob,
    part: Partition,
    snapshot_ts: u64,
) ?Emitter.Result {
    return runPartition_(self, job, part, snapshot_ts) catch |err| {
        log.err("compact tree={d} L{d}->L{d} partition failed: {s}", .{
            self.tree_id, job.src_level, job.dest_level, @errorName(err),
        });
        return null;
    };
}

fn runPartition_(
    self: *Tree,
    job: MergeJob,
    part: Partition,
    snapshot_ts: u64,
) !Emitter.Result {
    const dest = job.dest_level;
    const prio = job.prio;

    // --- one MergeIterator over the src tables + the partition's
    // concat over its bottom tables. The concat is this fiber's to
    // release until the merge takes it over. ---
    var part_bot = part.bot;
    const n_iters = job.src.len + 1;
    const all_iters = self.allocator.alloc(Iterator, n_iters) catch |err| {
        part_bot.deinit();
        return err;
    };
    defer self.allocator.free(all_iters);

    const table_key: TableKey = .{
        .namespace = self.namespace,
        .table_id = self.tableId(),
    };
    for (job.src, 0..) |e, i|
        all_iters[i] = .{
            .table = TableIterator.init(
                self.allocator,
                e.table,
                prio,
                table_key,
            ),
        };
    all_iters[job.src.len] = .{ .concat = part_bot };

    // The merge takes every iterator over, the concat included.
    var merge = MergeIterator.init(self.allocator, all_iters) catch |err| {
        for (all_iters) |*it| it.deinit();
        return err;
    };
    defer merge.deinit();
    try merge.seekRange(part.range.left, part.range.right);

    // --- merge loop ---
    var emitter = Emitter.init(self, dest, prio, snapshot_ts);
    defer emitter.deinit();
    while (merge.itemRef()) |item| {
        try emitter.push(item);
        try merge.next();
    }
    return emitter.finish();
}
