const std = @import("std");
const zio = @import("zio");
const utils = @import("utils");
const options = @import("options.zig");
const manifest = @import("manifest.zig");
const level_handler = @import("level.zig");
const table_mod = @import("table.zig");
const vfs = @import("vfs.zig");

const Table = table_mod.Table;
const TableIterator = table_mod.TableIterator;
const Item = table_mod.table.Item;
const Level = level_handler.Level;
const TableEntry = level_handler.TableEntry;
const Key = @import("key.zig").Key;
const KeyRange = level_handler.KeyRange;
const MergeIterator = table_mod.merge_iterator.MergeIterator;
const ConcatIterator = table_mod.merge_iterator.ConcatIterator;
const Iterator = table_mod.merge_iterator.Iterator;
const Builder = table_mod.builder.Builder;

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

const Targets = struct {
    allocator: std.mem.Allocator,
    base_level: u8,
    target_sz: []i64,
    file_sz: []i64,

    fn deinit(self: *Targets) void {
        self.allocator.free(self.target_sz);
        self.allocator.free(self.file_sz);
    }
};

const LevelPriority = struct {
    level: usize,
    score: f64,
    adjusted: f64,
};

// ---------------------------------------------------------------------------
// Tree
// ---------------------------------------------------------------------------
//
// The terminology used here is that data starts flowing in Level 0 (top most)
// and goes to lower levels (Level 1, Level 2, onwards).
// Sometimes, people would call Level 3 as an "upper level", but in my
// terminology, it would be a "lower" level.
// I think of this as a pyramid, where flowing happens at top (L0) and then settles
// to lower levels (L1..n).
// If it helps, we can think of L1..n as basement levels, where i is the ith floor
// below ground. DB Infrastructure works way under the API surface, so this is apt.

pub const CompactCandidate = struct { level: u8, score: f64 };

pub const Tree = struct {
    allocator: std.mem.Allocator,
    opts: options.Options,
    tree_id: u64,
    ms: *manifest.ForestState,
    levels: []Level,
    compact_candidate: ?CompactCandidate = null,

    /// open builds an Tree from an existing manifest state.
    /// It reads the tree's own levels and opens only tables belonging to this tree.
    /// Table opens are spread across zio fibers for parallelism.
    pub fn open(allocator: std.mem.Allocator, opts: options.Options, tree_id: u64, ms: *manifest.ForestState) !Tree {
        const levels = try allocator.alloc(Level, opts.max_levels);
        errdefer allocator.free(levels);
        for (levels, 0..) |*l, i| {
            l.* = Level.init(allocator, @intCast(i));
        }
        errdefer for (levels) |*l| l.deinit();

        var tse: utils.ThreadSafeError = .{};
        var group: zio.Group = .init;
        defer group.cancel();

        // Iterate the tree's own levels to find which tables belong to this tree.
        if (ms.trees.getPtr(tree_id)) |tree| {
            for (tree.levels.items, 0..) |lm, level_idx| {
                if (level_idx >= opts.max_levels) continue;
                var tit = lm.tables.iterator();
                while (tit.next()) |tkv| {
                    const table_id = tkv.key_ptr.*;

                    try group.spawn(openTableFiber, .{
                        allocator, opts.dir, table_id, level_idx, levels, &tse,
                    });
                }
            }
        }
        group.wait() catch {};
        if (tse.get()) |err| return err;

        var tree: Tree = .{
            .allocator = allocator,
            .opts = opts,
            .tree_id = tree_id,
            .ms = ms,
            .levels = levels,
        };
        try tree.updateCompactCandidate();
        return tree;
    }

    /// Opens a single SST and adds it to the appropriate handler.
    fn openTableFiber(
        allocator: std.mem.Allocator,
        dir: []const u8,
        id: u64,
        level: usize,
        levels: []Level,
        tse: *utils.ThreadSafeError,
    ) void {
        const path = std.fmt.allocPrint(allocator, "{s}/{d:0>6}.sst", .{ dir, id }) catch |err| {
            tse.set(err);
            return;
        };
        defer allocator.free(path);

        const t = Table.open(allocator, path, .p0) catch |err| {
            tse.set(err);
            return;
        };
        defer t.decrRef();

        levels[level].addEntry(id, t) catch |err| {
            tse.set(err);
        };
    }

    /// close tears down all level levels, freeing each table's memory without
    /// deleting SST files (which are managed by the manifest).
    pub fn close(self: *Tree) void {
        for (self.levels) |*h| h.deinit();
        self.allocator.free(self.levels);
    }

    /// get checks each level from L0 upward and returns the first matching item,
    /// or null if the key is absent. The returned item's key and value are
    /// duplicated with self.allocator; the caller must free them.
    pub fn get(self: *Tree, search_key: Key, priority: vfs.Priority) !?Item {
        for (self.levels) |*h| {
            if (try h.get(search_key, priority)) |item| return item;
        }
        return null;
    }

    // -----------------------------------------------------------------------
    // Level sizing
    // -----------------------------------------------------------------------

    fn levelTargets(self: *Tree) !Targets {
        const n = self.opts.max_levels;
        const target_sz = try self.allocator.alloc(i64, n);
        errdefer self.allocator.free(target_sz);
        const file_sz = try self.allocator.alloc(i64, n);
        errdefer self.allocator.free(file_sz);

        @memset(target_sz, 0);
        @memset(file_sz, 0);

        var base_level: u8 = 0;
        var db_size: i64 = self.levels[n - 1].getTotalSize();

        // Compute target sizes from the last level down to L0.
        var i: usize = n - 1;
        while (i > 0) : (i -= 1) {
            const ltarget = @max(db_size, self.opts.base_level_size);
            target_sz[i] = ltarget;
            // Set base_level on the first iteration where the level is at or
            // below the base size (mirrors Badger's sentinel-0 trick).
            if (base_level == 0 and ltarget <= self.opts.base_level_size) {
                base_level = @intCast(i);
            }
            db_size = @divTrunc(db_size, self.opts.level_size_multiplier);
        }

        // If any level between L1 and base_level-1 already has data, we must
        // set base_level to that level — L0 compaction can't skip over a non-empty level.
        if (base_level > 1) {
            for (1..base_level) |j| {
                if (self.levels[j].getTotalSize() > 0) {
                    base_level = @intCast(j);
                    break;
                }
            }
        }

        // Compute file sizes.
        var tsz: i64 = self.opts.base_table_size;
        file_sz[0] = @intCast(self.opts.memtable_size);
        for (1..n) |j| {
            if (j <= base_level) {
                file_sz[j] = tsz;
            } else {
                tsz = @min(tsz * self.opts.table_size_multiplier, self.opts.max_table_size);
                file_sz[j] = tsz;
            }
        }

        return .{
            .allocator = self.allocator,
            .base_level = base_level,
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
        var t = try self.levelTargets();
        defer t.deinit();

        const n = self.opts.max_levels;
        const prios = try self.allocator.alloc(LevelPriority, n);
        defer self.allocator.free(prios);

        // L0: count-based score.
        const l0_score = @as(f64, @floatFromInt(self.levels[0].numTables())) /
            @as(f64, @floatFromInt(self.opts.num_level_zero_tables));
        prios[0] = .{ .level = 0, .score = l0_score, .adjusted = l0_score };

        // If L0 needs compaction, execute it. We don't want to block writes because lower
        // levels are out of shape. Lower levels being out of shape is not as bad as blocking
        // user facing writes.
        if (l0_score >= 2.5) {
            self.compact_candidate = .{ .level = 0, .score = l0_score };
            return;
        }

        // L≥1: size-based score.
        for (1..n) |j| {
            const sz = self.levels[j].getTotalSize();
            const target = t.target_sz[j];
            const score = if (target > 0)
                @as(f64, @floatFromInt(sz)) / @as(f64, @floatFromInt(target))
            else
                0.0;
            prios[j] = .{ .level = j, .score = score, .adjusted = score };
        }

        // Adjusted scores: mirrors Badger/PebbleDB's priority dampening.
        // upperLevel starts at 0; for each level from base_level downward, if
        // prios[upperLevel].adjusted >= 1.0, divide it by the current level's
        // adjusted score (floored at 0.01).
        var upper_level: usize = 0;
        for (@as(usize, t.base_level)..n) |cur_level| {
            if (prios[upper_level].adjusted >= 1.0) {
                // If current level's adjusted score (= score, initially) is < 1.0, then it would
                // boost the uppper level's adjusted score AS. If it's > 1.0, then it would dampen
                // the upper level's AS. This helps because there's no point compacting something
                // at upper level when the level below it is already too big. This basically helps
                // make space in lower levels first before compacting upper levels.
                const denom = @max(prios[cur_level].adjusted, 0.01);
                prios[upper_level].adjusted /= denom;
            } else {
                // If adjusted score is < 1.0, it won't be compacted anyways,
                // so we can safely skip calculating the adjusted score.
            }
            upper_level = cur_level;
        }

        // Collect levels with score >= 1.0 (excluding the last level).
        var filtered = try std.ArrayList(LevelPriority).initCapacity(self.allocator, n);
        defer filtered.deinit(self.allocator);
        for (prios[0 .. n - 1]) |p| {
            if (p.score >= 1.0) try filtered.append(self.allocator, p);
        }
        if (filtered.items.len == 0) {
            self.compact_candidate = null;
            return;
        }

        // Sort descending by adjusted score.
        std.mem.sort(LevelPriority, filtered.items, {}, struct {
            fn gt(_: void, a: LevelPriority, b: LevelPriority) bool {
                return a.adjusted > b.adjusted;
            }
        }.gt);

        const winner = filtered.items[0];
        if (winner.adjusted <= 1.0) {
            self.compact_candidate = null;
        } else {
            self.compact_candidate = .{ .level = @intCast(winner.level), .score = winner.adjusted };
        }
    }

    // -----------------------------------------------------------------------
    // Compaction execution
    // -----------------------------------------------------------------------

    /// doCompact compacts src_level into dest (src+1, or Lbase when src==0).
    /// It merges src tables with any overlapping dest tables, writes new SSTs,
    /// commits the manifest, and updates the level levels atomically.
    pub fn doCompact(self: *Tree, src_level: u8, next_file_id: *std.atomic.Value(u64)) !void {
        const src: usize = src_level;
        const prio: vfs.Priority = if (src_level == 0) .p1 else .p2;

        var t = try self.levelTargets();
        defer t.deinit();

        const dest: usize = if (src_level == 0) t.base_level else @as(usize, src_level) + 1;
        if (dest >= self.opts.max_levels) return;

        // --- collect src entries (all of L0; first entry of L≥1) ---
        const src_entries_slice = blk: {
            self.levels[src].mu.lockShared();
            defer self.levels[src].mu.unlockShared();

            const items = self.levels[src].tables.items;
            if (items.len == 0) return;

            // TODO: This can be improved to consider which table to compact.
            const take = if (src_level == 0) items else items[0..1];
            break :blk try self.allocator.dupe(TableEntry, take);
        };
        defer self.allocator.free(src_entries_slice);

        // --- compute union key range of all src tables ---
        var kr = KeyRange{};
        for (src_entries_slice) |e| {
            const tkr = KeyRange{ .left = e.table.smallest, .right = e.table.biggest };
            kr.extend(tkr);
        }

        // --- collect overlapping dest (bot) entries ---
        const bot_entries_slice = try self.levels[dest].overlappingTables(self.allocator, kr);
        defer self.allocator.free(bot_entries_slice);

        // --- build single MergeIterator over src tables + one ConcatIterator for bot ---
        const bot_iters = try self.allocator.alloc(TableIterator, bot_entries_slice.len);
        defer self.allocator.free(bot_iters);

        const all_iters = try self.allocator.alloc(Iterator, src_entries_slice.len + 1);
        defer self.allocator.free(all_iters);

        for (src_entries_slice, 0..) |e, i|
            all_iters[i] = .{ .table = TableIterator.init(self.allocator, e.table, prio) };
        for (bot_entries_slice, 0..) |e, i|
            bot_iters[i] = TableIterator.init(self.allocator, e.table, prio);
        all_iters[src_entries_slice.len] = .{ .concat = ConcatIterator.init(bot_iters) };

        var merge = MergeIterator.init(self.allocator, all_iters);
        defer merge.deinit();
        try merge.seekToFirst();

        // --- output table entries produced by this compaction ---
        var output_tables: std.ArrayList(TableEntry) = .{};
        defer output_tables.deinit(self.allocator);
        // On error before manifest commit, decrRef to 0 → deletes the partial output files.
        var committed = false;
        errdefer if (!committed) for (output_tables.items) |e| e.table.decrRef();

        // --- merge loop ---
        const cfg = table_mod.builder.Config{
            .target_block_size = self.opts.block_size,
            .codec = self.opts.compression,
            .priority = prio,
        };
        const file_sz: usize = @intCast(t.file_sz[dest]);

        var bld = Builder.init(self.allocator, cfg);
        defer bld.deinit();

        const FlushHandle = zio.JoinHandle(?*Table);
        var flush_handles: std.ArrayList(FlushHandle) = .empty;
        defer {
            for (flush_handles.items) |*h| {
                if (h.join()) |tp| tp.decrRef();
            }
            flush_handles.deinit(self.allocator);
        }

        while (merge.itemRef()) |item| {
            if (bld.table_bytes.items.len >= file_sz) {
                // We are not going to break up a primary key across tables. All the versions for
                // this key should live in the same table. This avoids a scenario where a table with
                // a higher version key ends up getting compacted and living in a lower level. We
                // expect that the flow of keys is from top to bottom levels, so we're expecting to
                // find higher version keys at higher levels. This expectation is used by get fn to
                // stop searching for key as soon as we're able to find a version >= search key.
                if (item.key.orderPrimary(bld.lastKey()) != .eq) {
                    try self.flushBuilderOutput(&bld, &flush_handles, next_file_id);
                    bld.reset();
                }
            }
            try bld.add(item.key, item.value);
            try merge.next();
        }

        // Flush any remaining data.
        try self.flushBuilderOutput(&bld, &flush_handles, next_file_id);

        // Join all sync+open fibers and collect output tables.
        for (flush_handles.items) |*h| {
            const table_ptr = h.join() orelse return error.FlushFailed;
            errdefer table_ptr.decrRef();
            try output_tables.append(self.allocator, .{ .id = table_ptr.file_id, .table = table_ptr });
        }
        flush_handles.clearAndFree(self.allocator);

        // Best-effort directory sync in a zio fiber, overlapping with manifest build.
        var dir_sync = try zio.spawn(vfs.VFS.syncDir, .{self.opts.dir});
        defer dir_sync.join();

        // --- build manifest changes ---
        var changes = try std.ArrayList(manifest.TableChange).initCapacity(
            self.allocator,
            src_entries_slice.len + bot_entries_slice.len + output_tables.items.len,
        );
        defer changes.deinit(self.allocator);

        for (src_entries_slice) |e| try changes.append(self.allocator, manifest.deleteTableChange(e.id, self.tree_id));
        for (bot_entries_slice) |e| try changes.append(self.allocator, manifest.deleteTableChange(e.id, self.tree_id));
        for (output_tables.items) |e| {
            try changes.append(self.allocator, manifest.createTableChange(e.id, self.tree_id, @intCast(dest))); // usize → u32
        }

        // Commit to manifest (durable) before touching the levels.
        try self.ms.commit(changes.items, &.{});
        committed = true;

        // --- update levels ---
        self.levels[src].deleteEntries(src_entries_slice);
        try self.levels[dest].replaceEntries(bot_entries_slice, output_tables.items);

        // Release our refs on output tables; levels have taken their own refs via replaceEntries.
        for (output_tables.items) |e| e.table.decrRef();

        try self.updateCompactCandidate();
    }

    /// flushBuilderOutput finishes the builder, writes bytes to a new SST file,
    /// then spawns a fiber to sync and open it. The JoinHandle is appended to handles.
    fn flushBuilderOutput(
        self: *Tree,
        bld: *Builder,
        handles: *std.ArrayList(zio.JoinHandle(?*Table)),
        next_file_id: *std.atomic.Value(u64),
    ) !void {
        var tb = try bld.finish();
        defer tb.deinit();
        if (tb.bytes.len == 0) return;

        const id = next_file_id.fetchAdd(1, .monotonic);
        const path = try std.fmt.allocPrint(
            self.allocator,
            "{s}/{d:0>6}.sst",
            .{ self.opts.dir, id },
        );
        errdefer self.allocator.free(path);

        const wh = try vfs.instance.createFile(path, .{ .mode = .direct });
        try wh.write(tb.bytes, 0, bld.cfg.priority);

        // Sync + open in a fiber; data is in page cache after write.
        try handles.append(self.allocator, try zio.spawn(syncAndOpen, .{
            self.allocator, wh, path, bld.cfg.priority,
        }));
    }

    fn syncAndOpen(
        allocator: std.mem.Allocator,
        wh: vfs.FileHandle,
        path: []const u8,
        priority: vfs.Priority,
    ) ?*Table {
        wh.sync() catch {};
        wh.close();
        defer allocator.free(path);
        return Table.open(allocator, path, priority) catch null;
    }
};
