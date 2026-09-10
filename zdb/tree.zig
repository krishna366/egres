const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const _config = @import("config.zig");
const _manifest = @import("manifest.zig");
const _level_handler = @import("level.zig");
const _table = @import("table.zig");
const vfs = @import("vfs");
const _metrics = @import("metrics.zig");
const _fi = @import("file_ids.zig");
const zroar = @import("zroar");
const _compaction = @import("compaction.zig");

const Table = _table.Table;
const TableIterator = _table.TableIterator;
const Item = _table.table.Item;
const Level = _level_handler.Level;
const Lock = _level_handler.Lock;
const TableEntry = _level_handler.TableEntry;
const Key = @import("kv.zig").Key;
const KeyRange = _level_handler.KeyRange;
const MergeIterator = _table.merge_iterator.MergeIterator;
const ConcatIterator = _table.merge_iterator.ConcatIterator;
const Iterator = _table.merge_iterator.Iterator;

const log = std.log.scoped(.zdb_tree);

// Compaction lives in compaction.zig; these keep `_tree.X` and this
// file's own references to the moved declarations resolving.
pub const Targets = _compaction.Targets;
pub const CompactCandidate = _compaction.CompactCandidate;
pub const split_priority_score = _compaction.split_priority_score;
const VersionTracker = _compaction.VersionTracker;

// ---------------------------------------------------------------------------
// TableCache
// ---------------------------------------------------------------------------

pub const TableNeed = struct { id: u64, level: usize, table: ?*Table = null };

/// TableCache is a Forest-owned registry of open *Table objects keyed by
/// table_id. Tree.open() calls loadTables() instead of opening SST files
/// itself, so a table opened for one tree is reused when the same file is
/// needed by another tree (e.g. shared tables after a split).
pub const TableCache = struct {
    allocator: std.mem.Allocator,
    dir: []const u8,
    tables: std.AutoHashMap(u64, *Table),
    mu: fiber.RWMutex(.tracked),

    pub fn init(allocator: std.mem.Allocator, dir: []const u8) TableCache {
        return .{
            .allocator = allocator,
            .dir = dir,
            .tables = std.AutoHashMap(u64, *Table).init(allocator),
            .mu = .{},
        };
    }

    pub fn deinit(self: *TableCache) void {
        var it = self.tables.iterator();
        while (it.next()) |kv| {
            const table = kv.value_ptr.*;
            // detach + decrRef mirrors Level.deinit: the path is cleared so
            // the file isn't deleted, and decrRef at zero frees the index /
            // binary_fuse / file handle (via Table.deinit). Without this,
            // those allocations leak — `destroy(table)` only frees the
            // *Table struct memory, not its owned fields.
            table.detach();
            table.decrRef();
            self.allocator.destroy(table);
        }
        self.tables.deinit();
        self.mu.deinit();
    }

    /// contains reports whether a table with this id is currently cached.
    pub fn contains(self: *TableCache, id: u64) bool {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        return self.tables.contains(id);
    }

    /// own hands a *Table to the cache, which takes its own ref; the caller
    /// keeps its ref and drops it when done with `t`. Returns false when
    /// the id is already cached — the apply loop's `loadTables` can open
    /// the same file between a caller's `contains` check and this call,
    /// and Forest.writeFile does exactly that across seconds of I/O — in
    /// which case the cached one stays, nothing is taken, and the struct's
    /// disposal is the caller's too (a cache entry is what would otherwise
    /// own it).
    pub fn own(self: *TableCache, id: u64, t: *Table) !bool {
        self.mu.lock(@src());
        defer self.mu.unlock();
        const gop = try self.tables.getOrPut(id);
        if (gop.found_existing) return false;
        t.incrRef();
        gop.value_ptr.* = t;
        return true;
    }

    /// Let go of a table nobody published: the open-time ref goes (index,
    /// filter, file handle freed) and so does the struct, which only a
    /// cache entry would otherwise own.
    fn dropUnowned(allocator: std.mem.Allocator, t: *Table) void {
        std.debug.assert(t.ref.load(.acquire) == 1);
        t.decrRef();
        allocator.destroy(t);
    }

    /// Evict tables whose ref has reached zero. Since own() holds a cache ref,
    /// a table only becomes evictable after its cache ref is explicitly
    /// released (e.g. after a snapshot confirms no follower needs the SST
    /// file).
    pub fn clearStale(self: *TableCache) void {
        self.mu.lock(@src());
        defer self.mu.unlock();

        var to_evict: std.ArrayList(u64) = .empty;
        defer to_evict.deinit(self.allocator);

        var it = self.tables.iterator();
        while (it.next()) |entry| {
            const table = entry.value_ptr.*;
            if (table.ref.load(.acquire) == 0) {
                to_evict.append(self.allocator, entry.key_ptr.*) catch continue;
            }
        }

        for (to_evict.items) |id| {
            if (self.tables.fetchRemove(id)) |entry| {
                self.allocator.destroy(entry.value);
            }
        }
    }

    /// loadTables serves each TableNeed from cache (read lock) or opens it
    /// concurrently via fibers (write lock per miss). On return, each
    /// need's .table field points to an open *Table owned by the cache.
    /// The caller is responsible for installing these tables into levels
    /// (e.g. via Level.addEntry) — this keeps loadTables free of any
    /// level-layout assumptions.
    pub fn loadTables(self: *TableCache, needed: []TableNeed) !void {
        // Phase 1: serve cache hits under read lock; collect pointers for
        // misses.
        var misses: std.ArrayList(*TableNeed) = .empty;
        defer misses.deinit(self.allocator);
        {
            self.mu.lockShared(@src());
            defer self.mu.unlockShared();
            for (needed) |*n| {
                if (self.tables.get(n.id)) |t| {
                    std.debug.assert(t.ref.load(.acquire) > 0);
                    n.table = t;
                } else {
                    try misses.append(self.allocator, n);
                }
            }
        }
        if (misses.items.len == 0) return;

        // Phase 2: open misses concurrently. Each fiber writes to need.table
        // and bumps `completed` on return; the watchdog uses that to log
        // periodic progress if the group hangs.
        var tse: utils.SafeError = .{};
        var completed = std.atomic.Value(usize).init(0);
        const total = misses.items.len;
        var g = fiber.Group(void).init(self.allocator);
        defer g.deinit();
        try g.ensureTotalCapacity(1 + misses.items.len);
        g.spawn(.{}, loadTablesWatchdog, .{ total, &completed }) catch {};
        for (misses.items) |n| {
            try g.spawn(.{}, openFiber, .{ self, n, &tse, &completed });
        }
        g.wait();
        if (tse.get()) |err| return err;
    }

    /// Sibling of the `openFiber` spawns: poll the completed counter
    /// every 500 ms and log every 15 s while we're still short. Exits
    /// as soon as all opens are accounted for, so it doesn't hold the
    /// group's `wait()` open.
    fn loadTablesWatchdog(
        total: usize,
        completed: *std.atomic.Value(usize),
    ) void {
        const start = fiber.coarseMs();
        var last_warn = start;
        while (completed.load(.acquire) < total) {
            fiber.sleepMs(500);
            const now = fiber.coarseMs();
            if (now - last_warn >= 15_000) {
                log.warn(
                    "loadTables watchdog: {d}/{d} opens completed after {d}ms",
                    .{ completed.load(.acquire), total, now - start },
                );
                last_warn = now;
            }
        }
    }

    fn openFiber(
        self: *TableCache,
        need: *TableNeed,
        tse: *utils.SafeError,
        completed: *std.atomic.Value(usize),
    ) void {
        defer _ = completed.fetchAdd(1, .release);
        // Phase A: check under write lock (brief — no I/O).
        self.mu.lock(@src());
        if (self.tables.get(need.id)) |t| {
            self.mu.unlock();
            need.table = t;
            return;
        }
        self.mu.unlock();

        // Phase B: open the SST file WITHOUT holding the lock.
        // Table.open may yield the fiber (async I/O); holding the lock here
        // would deadlock cooperative fibers on the same OS thread (EDEADLK).
        const path = _fi.sstPath(
            self.allocator,
            self.dir,
            need.id,
        ) catch |err| {
            tse.set(err);
            return;
        };
        defer self.allocator.free(path);
        const t = Table.open(self.allocator, path, .p0) catch |err| {
            tse.set(err);
            return;
        };

        // Phase C: re-acquire write lock to insert; handle race with another
        // fiber. The open-time ref becomes the cache's on success; on the
        // two other paths nothing owns `t`, so it is dropped whole.
        self.mu.lock(@src());
        if (self.tables.get(need.id)) |existing| {
            // Another fiber opened the same table while we were in phase B.
            self.mu.unlock();
            need.table = existing;
            dropUnowned(self.allocator, t);
            return;
        }
        self.tables.put(need.id, t) catch |err| {
            self.mu.unlock();
            tse.set(err);
            dropUnowned(self.allocator, t);
            return;
        };
        self.mu.unlock();
        need.table = t;
    }
};

// ---------------------------------------------------------------------------
// Tree
// ---------------------------------------------------------------------------
//
// The terminology used here is that data starts flowing in Level 0 (top most)
// and goes to lower levels (Level 1, Level 2, onwards).
// Sometimes, people would call Level 3 as an "upper level", but in my
// terminology, it would be a "lower" level.
// I think of this as a pyramid, where flowing happens at top (L0) and then
// settles to lower levels (L1..n). If it helps, we can think of L1..n as
// basement levels, where i is the ith floor below ground. DB Infrastructure
// works way under the API surface, so this is apt.

pub const Tree = struct {
    allocator: std.mem.Allocator,
    opts: _config.Config,
    namespace: u64,
    tree_id: u64,
    ms: *_manifest.ForestState,
    cache: *TableCache,
    levels: []Level,
    compact_candidate: ?CompactCandidate = null,
    key_range: KeyRange,
    /// Holders of this object. One is the `TableTrees` entry that lists it;
    /// `TableTrees.getTree` hands out another per reader, released with
    /// `release` when the reader is done. The last release closes and frees
    /// the tree. This is what lets a split or a drop retire a tree without
    /// waiting: a split never mutates a live tree — it opens fresh objects
    /// for both halves from the manifest and swaps them in — so a reader
    /// that still holds the old one sees its full range and full table
    /// set until it lets go. Flush and compaction do install into a live
    /// tree's levels, under the level locks, with readers pinning the
    /// tables they select there.
    refs: std.atomic.Value(u32) = .init(1),
    /// The one lock over this tree's mutable state: every level's table
    /// list and `compact_candidate`. Shared to read — a point get, a merge
    /// open, the split-key and compaction pickers, stats — and exclusive to
    /// install: flush, compaction, backfill, and the candidate refresh.
    /// Levels have no lock of their own: every `Level` method takes the
    /// token this lock mints (`hold`), so a level cannot be touched without
    /// it, and so do the tree methods that read levels on a caller's behalf
    /// (`computeCompactCandidate`, `totalSize`, `lowestLevelSize`). Never
    /// held across I/O: readers pin the tables they select while they hold
    /// it and read after releasing (`Level.acquireTables`,
    /// `Level.overlappingTables`), so an install only ever waits on a
    /// selection, never on a block read. Not reentrant — nothing that runs
    /// under an exclusive hold may call something that takes it
    /// (`updateCompactCandidate` takes its own, so installs call it after
    /// releasing theirs). Order: `TableTrees.mu`, then this.
    mu: Lock = .{},

    // Compaction, from compaction.zig.
    pub const levelTargets = _compaction.levelTargets;
    pub const updateCompactCandidate = _compaction.updateCompactCandidate;
    pub const computeCompactCandidate = _compaction.computeCompactCandidate;
    pub const doCompact = _compaction.doCompact;
    pub const Emitter = _compaction.Emitter;
    pub const buildCompaction = _compaction.buildCompaction;
    pub const flushBuilderOutput = _compaction.flushBuilderOutput;

    pub fn acquire(self: *Tree) void {
        const prev = self.refs.fetchAdd(1, .acquire);
        std.debug.assert(prev > 0);
    }

    /// Drop a ref; the last one closes the tree and frees it (it must
    /// have been created with `allocator`, as installTree/applySplit do).
    pub fn release(self: *Tree) void {
        const prev = self.refs.fetchSub(1, .release);
        std.debug.assert(prev > 0);
        if (prev == 1) {
            _ = self.refs.load(.acquire);
            const allocator = self.allocator;
            self.close();
            allocator.destroy(self);
        }
    }

    /// open builds a Tree from an existing manifest state, on the heap with
    /// one ref — `release` is its teardown, and a tree lives at one address
    /// for its whole life. Tables are loaded via cache, which opens SST
    /// files concurrently for misses and reuses already-open *Table objects
    /// for cache hits. Runs on the apply loop (or at startup), the only
    /// place the manifest map it reads is ever written.
    ///
    /// No lock here: nobody else can see this tree yet, and the caller may
    /// publish it (`TableTrees.addTree`) before the first lock is taken.
    pub fn open(
        allocator: std.mem.Allocator,
        opts: _config.Config,
        namespace: u64,
        tree_id: u64,
        ms: *_manifest.ForestState,
        cache: *TableCache,
    ) !*Tree {
        const self = try allocator.create(Tree);
        errdefer allocator.destroy(self);

        const levels = try allocator.alloc(Level, opts.max_levels);
        errdefer allocator.free(levels);
        for (levels, 0..) |*l, i| {
            l.* = Level.init(allocator, @intCast(i), &self.mu);
        }
        errdefer for (levels) |*l| l.deinit();

        var kr: KeyRange = .{};
        errdefer {
            kr.left.free(allocator);
            kr.right.free(allocator);
        }

        // The manifest record is read under the manifest lock and copied
        // out — range and table ids — so the loads below, file I/O, run
        // with it released.
        var needed: std.ArrayList(TableNeed) = .empty;
        defer needed.deinit(allocator);
        {
            const held = ms.mu.hold(.shared, @src());
            defer held.release();
            if (ms.treeState(held, .{
                .namespace = namespace,
                .tree_id = tree_id,
            })) |tree| {
                kr.left = try tree.left.dupe(allocator);
                kr.right = try tree.right.dupe(allocator);
                if (kr.left.len() == 0 and kr.right.len() == 0) {
                    kr.inf = true;
                }
                for (tree.levels.items, 0..) |lm, level_idx| {
                    if (level_idx >= opts.max_levels) continue;
                    var tit = lm.tables.keyIterator();
                    while (tit.next()) |tid| {
                        try needed.append(allocator, .{
                            .id = tid.*,
                            .level = level_idx,
                        });
                    }
                }
            }
        }
        try cache.loadTables(needed.items);

        self.* = .{
            .allocator = allocator,
            .opts = opts,
            .namespace = namespace,
            .tree_id = tree_id,
            .ms = ms,
            .cache = cache,
            .levels = levels,
            .key_range = kr,
        };
        // Unpublished, so uncontended — but the levels take the token, and
        // the tree is at its final address, so the lock is simply taken.
        {
            const held = self.mu.hold(.exclusive, @src());
            defer held.release();
            for (needed.items) |n| {
                try levels[n.level].addEntry(held, n.id, n.table.?);
            }
        }
        try self.updateCompactCandidate();
        return self;
    }

    /// close tears down all levels, freeing each table's memory without
    /// deleting SST files (which are managed by the manifest). The
    /// internal half of `release`, which is the teardown callers use.
    fn close(self: *Tree) void {
        for (self.levels) |*h| h.deinit();
        self.allocator.free(self.levels);
        self.key_range.left.free(self.allocator);
        self.key_range.right.free(self.allocator);
        self.mu.deinit();
    }

    /// get checks each level from L0 upward and returns the first matching
    /// item, or null if the key is absent. The returned item's key and value
    /// are duplicated with self.allocator; the caller must free them.
    ///
    /// Two phases: under one shared hold of `mu`, pin every table that
    /// could hold the key, across all levels, in read order; then release
    /// and read them in that order, first hit wins. The pins keep the
    /// tables readable through any install that lands in between, and the
    /// lock is never held across a block read.
    pub fn get(self: *Tree, search_key: Key, priority: vfs.Priority) !?Item {
        var list: std.ArrayList(TableEntry) = .empty;
        errdefer {
            for (list.items) |e| e.table.decrRef();
            list.deinit(self.allocator);
        }
        {
            const held = self.mu.hold(.shared, @src());
            defer held.release();
            for (self.levels) |*lv| try lv.acquireTables(
                held,
                self.allocator,
                search_key,
                &list,
            );
        }
        const tables = try list.toOwnedSlice(self.allocator);
        defer Level.releaseTables(self.allocator, tables);
        for (tables) |e| {
            if (try Level.readKey(
                self.allocator,
                e.table,
                search_key,
                priority,
            )) |item| return item;
        }
        return null;
    }

    /// Build a merge iterator over this tree's data overlapping `range`,
    /// plus the caller-supplied `extras` (typically memtable iterators).
    /// The walk loop lives in `Forest.Cursor`, which opens one of these
    /// per tree it visits.
    ///
    /// The caller holds a ref on this tree (`TableTrees.getTree`) and read
    /// `key_range` from this same object. That is enough for the range and
    /// the tables selected here to describe one instant: a split never
    /// mutates a live tree — it installs fresh objects for both halves
    /// and retires this one, which stays whole for whoever still holds it
    /// — so there is no gap in which the range says 1..100 while the
    /// levels hold only 1..50. Flush and compaction do install into the
    /// levels concurrently, under the level locks; the tables selected
    /// are pinned, so what was chosen stays readable.
    ///
    /// NOT positioned: `range` only picks the tables (and pins them —
    /// every iterator wired in holds its own table ref). The selection
    /// runs under one shared hold of `mu` here; the caller seeks with
    /// `merge.seekRange(range.left, range.right)` afterwards, so it can
    /// release the tree first and let the block reads a seek issues run
    /// holding nothing but table refs.
    ///
    /// Sources fed into the merge:
    ///   - `extras` — passed through verbatim (typically memtable iters)
    ///   - L0    — one `TableIterator` per overlapping table (overlapping
    ///             ranges, so each needs its own merge slot)
    ///   - L≥1  — one `ConcatIterator` per level (tables are disjoint so
    ///             they can be walked sequentially)
    ///
    /// The cursor takes ownership of every iterator wired in (extras
    /// included). On success: `cursor.deinit()` releases everything.
    /// On error (before merge is built): we deinit `extras` ourselves
    /// so refs don't leak. All wiring memory (slices, sub-arrays) lives
    /// in the cursor's arena.
    /// `allocator` is the caller's per-call arena. Every allocation made
    /// here — overlapping-table slices, iter slots, the merge's internal
    /// heap, plus the iters' own scratch (key_buf etc.) — comes from it.
    /// The caller keeps the arena alive for the merge's lifetime and frees
    /// it at end-of-scope; `merge.deinit()` only releases table refs.
    /// `min_ts`: a table whose newest version (`TableIndex.max_version`)
    /// is at or below it holds nothing the scan wants and is not opened
    /// (Forest.ScanOptions.min_ts); 0 opens every overlapping table.
    pub fn openMerge(
        self: *Tree,
        allocator: std.mem.Allocator,
        range: KeyRange,
        priority: vfs.Priority,
        min_ts: u64,
        extras: []const Iterator,
    ) !MergeIterator {
        // Build iters into a list. Until we hand them off to merge,
        // each iter we created here owns a Table/SkipList ref — on
        // failure we must deinit them all to release those refs.
        var iters: std.ArrayList(Iterator) = .empty;
        var transferred = false;
        errdefer {
            if (!transferred) for (iters.items) |*it| it.deinit();
        }

        try iters.appendSlice(allocator, extras);

        const held = self.mu.hold(.shared, @src());
        defer held.release();
        for (self.levels, 0..) |*lv, level_idx| {
            const overlapping = try lv.overlappingTables(
                held,
                allocator,
                range,
            );
            // The iterators take their own table refs; the selection
            // pins are only needed to bridge the gap between the
            // level lock releasing and the iterators existing.
            defer Level.releaseTables(allocator, overlapping);
            if (overlapping.len == 0) continue;

            if (level_idx == 0) {
                for (overlapping) |e| {
                    if (e.table.index.max_version <= min_ts) continue;
                    try iters.append(allocator, .{
                        .table = TableIterator.init(
                            allocator,
                            e.table,
                            priority,
                        ),
                    });
                }
            } else {
                var subs: std.ArrayList(TableIterator) = .empty;
                for (overlapping) |e| {
                    if (e.table.index.max_version <= min_ts) continue;
                    const tbl = e.table;
                    const sub = TableIterator.init(allocator, tbl, priority);
                    try subs.append(allocator, sub);
                }
                if (subs.items.len == 0) continue;
                const concat = ConcatIterator.init(subs.items);
                try iters.append(allocator, .{ .concat = concat });
            }
        }

        // merge.deinit now owns iter cleanup.
        transferred = true;
        return MergeIterator.init(allocator, iters.items);
    }

    /// One level's table count, under a hold of its own — for stats and
    /// tests, where nothing else is read alongside it.
    pub fn numTablesAt(self: *Tree, level: usize) usize {
        const held = self.mu.hold(.shared, @src());
        defer held.release();
        return self.levels[level].numTables(held);
    }

    pub fn totalSize(self: *Tree, held: Lock.Shared) i64 {
        self.mu.assertHeld(held);
        var total: i64 = 0;
        for (self.levels) |*l| total += l.getTotalSize(held);
        return total;
    }

    pub fn lowestLevelSize(self: *Tree, held: Lock.Shared) struct {
        level: u32,
        size: i64,
    } {
        self.mu.assertHeld(held);
        var idx: usize = self.levels.len;
        while (idx > 0) {
            idx -= 1;
            const sz = self.levels[idx].getTotalSize(held);
            if (sz > 0) return .{ .level = @intCast(idx), .size = sz };
        }
        return .{ .level = 0, .size = 0 };
    }

    /// Minimum number of Lmax tables required before we'll consider a
    /// split. Below this, the keyspace coverage in Lmax is too coarse
    /// for an honest balance — the split would either leave one side
    /// nearly empty or land on a key that puts most of the upper-level
    /// data on one side. Wait for Lmax to fill out via compactions.
    pub const min_lmax_tables_for_split: usize = 10;

    /// Pick a split key that minimizes `|left_bytes − right_bytes|`
    /// across all L≥1 tables, drawn ONLY from the smallest-key
    /// boundaries of Lmax tables. This guarantees no L3 table gets
    /// split in half (which would force an L3→L3 compaction); the
    /// chosen key always cleanly partitions Lmax into "tables[0..i]"
    /// left and "tables[i..]" right.
    ///
    /// Candidates are only valid when Lmax has ≥ `min_lmax_tables_for_split`
    /// tables. Below that, return empty (no split this round; the
    /// caller refreshes the candidate and tries again later).
    ///
    /// For the balance score, we ignore tables that STRADDLE the
    /// candidate K — they go to BOTH children in `buildSplit`, so
    /// they cost the same on each side and don't differentiate
    /// candidates. Only strict-left and strict-right bytes count.
    pub fn findSplitKey(self: *Tree, allocator: std.mem.Allocator) !Key {
        // Use a per-call arena for keys we collect under each level's
        // read lock — once the lock drops, the source Table could be
        // freed by a concurrent delete, so we must not keep raw
        // pointers into table memory.
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const a = arena.allocator();

        // Step 1: snapshot Lmax candidate keys (smallest-of-table for
        // each table, skipping items[0] whose smallest is the level's
        // left boundary — using it would put nothing on the old side).
        // Bail if Lmax doesn't have enough tables for a meaningful split.
        const lmax_idx = self.opts.max_levels - 1;
        var cand_keys: std.ArrayList(Key) = .empty;
        // Steps 1-2 read the level lists (keys are duped into the arena);
        // step 3 is arithmetic on the copies. No I/O anywhere, so one
        // shared hold covers the whole function.
        const held = self.mu.hold(.shared, @src());
        defer held.release();
        {
            const lv = &self.levels[lmax_idx];
            const lmax = lv.entries(held);
            if (lmax.len < min_lmax_tables_for_split) return .{};
            for (lmax[1..]) |tbe| {
                try cand_keys.append(a, try tbe.table.smallest.dupe(a));
            }
        }

        // Step 2: snapshot all L≥1 tables (smallest, biggest, size)
        // across every level so we can score each candidate by its
        // L≥1-wide balance, not just within Lmax.
        const Entry = struct { smallest: Key, biggest: Key, size: u64 };
        var all_entries: std.ArrayList(Entry) = .empty;
        for (self.levels[1..]) |*lv| {
            for (lv.entries(held)) |tbe| {
                try all_entries.append(a, .{
                    .smallest = try tbe.table.smallest.dupe(a),
                    .biggest = try tbe.table.biggest.dupe(a),
                    .size = tbe.table.fileSize(),
                });
            }
        }

        // Step 3: for each candidate K, count strict-left and
        // strict-right bytes; pick the K with smallest difference.
        var best_key: ?Key = null;
        var best_score: u64 = std.math.maxInt(u64);
        for (cand_keys.items) |k| {
            var left: u64 = 0;
            var right: u64 = 0;
            for (all_entries.items) |e| {
                if (e.biggest.order(k) == .lt) {
                    left += e.size;
                } else if (e.smallest.order(k) != .lt) {
                    right += e.size;
                }
                // else: straddles K → goes to BOTH children → skip.
            }
            const score = if (left > right) left - right else right - left;
            if (score < best_score) {
                best_score = score;
                best_key = k;
            }
        }

        const chosen = best_key orelse return .{};
        // Re-dupe into the caller's allocator (arena dies on return).
        // Bump timestamp so every (primary, ts) version of the chosen
        // key sorts AFTER the split key — they all go to the right
        // tree, no primary gets split across trees.
        var key = try chosen.dupe(allocator);
        std.debug.assert(key.len() > 0);
        key.timestamp = std.math.maxInt(u64);
        return key;
    }

    /// Drain one source's versions of `seek`'s primary into `rollup`,
    /// newest first, admitting only timestamps at or below `bound`.
    /// Each admitted version lowers `bound` beneath itself, so the
    /// NEXT source starts strictly below everything consumed so far —
    /// that is what keeps the sequential walk correct if a concurrent
    /// flush/compaction re-surfaces already-consumed versions at a
    /// lower source mid-walk (they arrive above the bound, skipped).
    /// `it` is anything with the iterator surface (seekRange/itemRef/
    /// next/currentPin): a single `Iterator` or a MergeIterator.
    ///
    /// Kept versions land in the tracker's buffer — pinned, never
    /// copied; the tracker owns the pin lifecycle.
    fn drainSource(
        vt: *VersionTracker,
        it: anytype,
        seek: Key,
        bound: *u64,
    ) !VersionTracker.RoaringChain {
        var start = seek;
        start.timestamp = bound.*;
        // Both bounds carry ns/table_id/primary, so the window can
        // only yield this primary's versions — load-bearing for the
        // namespace-wide memtable sources.
        try it.seekRange(start, KeyRange.versionsAt(seek).right);
        while (it.itemRef()) |item| {
            std.debug.assert(item.key.equalPrimary(seek));
            if (item.key.timestamp > bound.*) {
                try it.next();
                continue;
            }
            const ts = item.key.timestamp;
            switch (try vt.roaringBuffer(item, it.currentPin())) {
                .more => {
                    // Real HLC timestamps are never 0: no wrap.
                    std.debug.assert(ts > 0);
                    bound.* = ts - 1;
                    try it.next();
                },
                .done => return .done,
                .foreign => return .foreign,
            }
        }
        // Source exhausted with the chain still open: more input
        // wanted from the next source down.
        return .more;
    }

    /// Fold one primary's roaring version chain into a bitmap by
    /// walking sources top-down, LAZILY: the optional `extra` source
    /// first (Forest.getBitmap passes one MergeIterator over all
    /// memtable sources — merged rather than sequential because
    /// memtable seeks are free of I/O and a sorted merge is immune to
    /// their ordering subtleties), then this tree's levels from
    /// `from_level` down, stopping at the first terminal without
    /// touching anything beneath it. L0 tables are probed via their
    /// binary-fuse filter before any I/O.
    ///
    /// `seek.timestamp` is the read timestamp: only versions at or
    /// below it participate. Returns null when no version exists in
    /// the walked window (the posting has no history here — folding
    /// from empty is the caller's call). The bitmap and the fold
    /// allocate from `allocator`.
    ///
    /// Both readers use this: Forest.getBitmap (memtable extra,
    /// from_level=0, the reader's ts) and compaction read-below (no
    /// extra, from_level=dest+1, ts = oldest buffered ts − 1 so the
    /// walk starts strictly beneath the chain the compaction already
    /// buffered). Soundness of walking sources sequentially rests on
    /// the level-ordering invariant — for any primary, every version
    /// at level i is newer than every version at level j > i
    /// (compaction merges src ∪ overlapping-dest into dest and never
    /// splits a primary's run across output files) — plus
    /// `drainSource`'s descending-bound guard for mid-walk shuffles.
    ///
    /// CorruptPosting / NotRoaring mean the chain cannot be folded;
    /// compaction degrades to verbatim emission, reads surface it.
    pub fn rollupBitmap(
        self: *Tree,
        allocator: std.mem.Allocator,
        seek: Key,
        priority: vfs.Priority,
        extra: ?*MergeIterator,
        from_level: usize,
    ) !?zroar.Bitmap {
        // Read-mode tracker: pure collection, no retention state and
        // no emitter — resolution here is just the fold.
        var vt = VersionTracker.init(allocator, 0, false);
        defer vt.deinit();

        var bound = seek.timestamp;
        var status: VersionTracker.RoaringChain = .more;

        if (extra) |ex| {
            status = try drainSource(&vt, ex, seek, &bound);
        }

        var lvl = from_level;
        while (status == .more and lvl < self.opts.max_levels) : (lvl += 1) {
            var probe = seek;
            probe.timestamp = bound;
            const tables = blk: {
                const held = self.mu.hold(.shared, @src());
                defer held.release();
                break :blk try self.levels[lvl].overlappingTables(
                    held,
                    self.allocator,
                    KeyRange.versionsAt(probe),
                );
            };
            defer Level.releaseTables(self.allocator, tables);

            // Newest first within a level: L0 sorts by id ascending
            // (newest last), so walk in reverse. Point ranges select
            // at most one table on L≥1.
            var ri = tables.len;
            while (status == .more and ri > 0) {
                ri -= 1;
                const e = tables[ri];
                if (!e.table.containsKey(probe)) continue;

                var it: Iterator = .{
                    .table = TableIterator.init(
                        self.allocator,
                        e.table,
                        priority,
                    ),
                };
                defer it.deinit();
                status = try drainSource(&vt, &it, seek, &bound);
            }
        }

        switch (status) {
            .foreign => return error.NotRoaring,
            .done => return try vt.foldBufferedToBitmap(allocator, null),
            // Deltas (or nothing at all) through the whole window:
            // no terminal exists for this posting here.
            .more => {
                if (vt.buffered.items.len == 0) return null;
                return try vt.foldBufferedToBitmap(allocator, null);
            },
        }
    }

    /// applyCompaction is Phase B: commits the manifest changes and updates the
    /// in-memory levels by opening tables from the SST directory.
    /// Called after Raft commit on all nodes (leader and followers).
    pub fn applyCompaction(
        self: *Tree,
        changes: []const _manifest.SSTableChange,
    ) !void {
        if (changes.len == 0) return;

        // Commit to manifest (durable) before touching the levels.
        try self.ms.commit(changes, &.{});

        // Batch-load all CREATE tables via the cache in a single concurrent
        // pass. writeFile may have pre-opened them (follower path) → hit;
        // otherwise loadTables opens them.
        var needed: std.ArrayList(TableNeed) = .empty;
        defer needed.deinit(self.allocator);
        for (changes) |c| {
            if (c.op == .create) {
                try needed.append(self.allocator, .{
                    .id = c.sst_id,
                    .level = c.level,
                });
            }
        }
        try self.cache.loadTables(needed.items);

        // Apply level changes in original order: delete old entries, add new.
        // Track total bytes created and the destination level for metrics.
        // One exclusive hold for the whole batch, so no reader sees the
        // source gone before the output is in.
        var need_idx: usize = 0;
        var bytes_out: u64 = 0;
        var dest_level: ?u8 = null;
        {
            const held = self.mu.hold(.exclusive, @src());
            defer held.release();
            for (changes) |c| {
                if (c.op == .delete) { // DELETE
                    self.levels[c.level].deleteEntry(held, c.sst_id);
                } else if (c.op == .create) { // CREATE
                    const n = needed.items[need_idx];
                    need_idx += 1;
                    std.debug.assert(c.sst_id == n.id);
                    try self.levels[c.level].addEntry(held, n.id, n.table.?);
                    bytes_out += n.table.?.fileSize();
                    dest_level = @intCast(c.level);
                }
            }
        }

        // Metrics: one compaction run, plus bytes written to the dest level.
        const ns_id = self.namespace;
        if (dest_level) |lvl| {
            try _metrics.g().compactions.incr(.{
                .namespace_id = ns_id,
                .tree_id = self.tree_id,
                .level = lvl,
            });
            try _metrics.g().bytes_written.incrBy(.{
                .source = "compact",
                .namespace_id = ns_id,
                .tree_id = self.tree_id,
                .level = lvl,
            }, bytes_out);
        }

        try self.updateCompactCandidate();
    }

    /// Install a CREATE INDEX backfill: bottom-level CREATEs only
    /// (Forest.buildBottomTable). Valid solely while the bottom level
    /// is still EMPTY — the backfill states are older than every
    /// delta, so anything already at the bottom (a compaction that
    /// raced past the engine's tree reservation) would sit ABOVE them
    /// in version order while sharing the level, breaking the sorted
    /// run — and a rolled-up terminal there would already be missing
    /// the backfilled rows. The check reads applied state, so every
    /// node skips (or installs) identically; the DDL session verifies
    /// via Forest.bottomLevelHas after the apply and fails CREATE
    /// INDEX loudly on a skip.
    pub fn applyBackfill(
        self: *Tree,
        changes: []const _manifest.SSTableChange,
    ) !void {
        if (changes.len == 0) return;
        const bottom: u32 = @intCast(self.opts.max_levels - 1);
        for (changes) |c| {
            std.debug.assert(c.op == .create);
            std.debug.assert(c.level == bottom);
        }
        const bottom_tables = blk: {
            const held = self.mu.hold(.shared, @src());
            defer held.release();
            break :blk self.levels[bottom].tableCount(held);
        };
        if (bottom_tables > 0) {
            log.err(
                "backfill install skipped: bottom level not empty tree={d}",
                .{self.tree_id},
            );
            return;
        }
        try self.applyCompaction(changes);
    }
};
