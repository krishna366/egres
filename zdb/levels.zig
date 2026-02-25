const std = @import("std");
const options = @import("options.zig");
const manifest = @import("manifest.zig");
const level_handler = @import("level_handler.zig");
const table_mod = @import("table.zig");

const Table = table_mod.Table;
const TableIterator = table_mod.TableIterator;
const Item = table_mod.table.Item;
const LevelHandler = level_handler.LevelHandler;
const TableEntry = level_handler.TableEntry;
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
// LevelsController
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

pub const LevelsController = struct {
    allocator: std.mem.Allocator,
    opts: options.Options,
    ms: *manifest.ManifestState,
    handlers: []LevelHandler,
    next_file_id: std.atomic.Value(u64),

    /// open builds a LevelsController from an existing manifest state.
    /// It reads each SST file listed in ms.tables, opens the table in memory,
    /// and distributes tables across their corresponding level handlers.
    pub fn open(
        allocator: std.mem.Allocator,
        opts: options.Options,
        ms: *manifest.ManifestState,
    ) !LevelsController {
        const handlers = try allocator.alloc(LevelHandler, opts.max_levels);
        errdefer allocator.free(handlers);
        for (handlers, 0..) |*h, i| {
            h.* = LevelHandler.init(allocator, @intCast(i));
        }
        errdefer for (handlers) |*h| h.deinit();

        var max_id: u64 = 0;

        var it = ms.tables.iterator();
        while (it.next()) |kv| {
            const id = kv.key_ptr.*;
            const tm = kv.value_ptr.*;
            if (id > max_id) max_id = id;

            if (tm.level >= opts.max_levels) continue;

            const path = try std.fmt.allocPrint(allocator, "{s}/{d:0>6}.sst", .{ opts.dir, id });
            defer allocator.free(path);

            const t = try Table.open(allocator, path); // ref = 1
            defer t.decrRef(); // on return, ref -= 1, so handlers is the only owner.

            try handlers[tm.level].addEntry(id, t); // incrRef → ref = 2
        }

        return .{
            .allocator = allocator,
            .opts = opts,
            .ms = ms,
            .handlers = handlers,
            .next_file_id = std.atomic.Value(u64).init(max_id + 1),
        };
    }

    /// close tears down all level handlers, freeing each table's memory without
    /// deleting SST files (which are managed by the manifest).
    pub fn close(self: *LevelsController) void {
        for (self.handlers) |*h| h.deinit();
        self.allocator.free(self.handlers);
    }

    /// get checks each level from L0 upward and returns the first matching item,
    /// or null if the key is absent. The returned item's key and value are
    /// duplicated with self.allocator; the caller must free them.
    pub fn get(self: *LevelsController, search_key: []const u8) !?Item {
        for (self.handlers) |*h| {
            if (try h.get(search_key)) |item| return item;
        }
        return null;
    }

    // -----------------------------------------------------------------------
    // Level sizing
    // -----------------------------------------------------------------------

    fn levelTargets(self: *LevelsController) !Targets {
        const n = self.opts.max_levels;
        const target_sz = try self.allocator.alloc(i64, n);
        errdefer self.allocator.free(target_sz);
        const file_sz = try self.allocator.alloc(i64, n);
        errdefer self.allocator.free(file_sz);

        @memset(target_sz, 0);
        @memset(file_sz, 0);

        var base_level: u8 = 0;
        var db_size: i64 = self.handlers[n - 1].getTotalSize();

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
                if (self.handlers[j].getTotalSize() > 0) {
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

    /// pickCompactLevel returns the level that most needs compaction, or null
    /// if no level has exceeded its target (score < 1.0 for all levels).
    pub fn pickCompactLevel(self: *LevelsController) !?u8 {
        var t = try self.levelTargets();
        defer t.deinit();

        const n = self.opts.max_levels;
        const prios = try self.allocator.alloc(LevelPriority, n);
        defer self.allocator.free(prios);

        // L0: count-based score.
        {
            const l0_score = @as(f64, @floatFromInt(self.handlers[0].numTables())) /
                @as(f64, @floatFromInt(self.opts.num_level_zero_tables));
            prios[0] = .{ .level = 0, .score = l0_score, .adjusted = l0_score };

            // If L0 needs compaction, execute it. We don't want to block writes because lower
            // levels are out of shape. Lower levels being out of shape is not as bad as blocking
            // user facing writes.
            if (l0_score >= 2.5) {
                return 0;
            }
        }

        // L≥1: size-based score.
        for (1..n) |j| {
            const sz = self.handlers[j].getTotalSize();
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
        if (filtered.items.len == 0) return null;

        // Sort descending by adjusted score.
        std.mem.sort(LevelPriority, filtered.items, {}, struct {
            fn gt(_: void, a: LevelPriority, b: LevelPriority) bool {
                return a.adjusted > b.adjusted;
            }
        }.gt);

        return @intCast(filtered.items[0].level);
    }

    // -----------------------------------------------------------------------
    // Compaction execution
    // -----------------------------------------------------------------------

    /// doCompact compacts src_level into dest (src+1, or Lbase when src==0).
    /// It merges src tables with any overlapping dest tables, writes new SSTs,
    /// commits the manifest, and updates the level handlers atomically.
    pub fn doCompact(self: *LevelsController, src_level: u8) !void {
        const src: usize = src_level;

        var t = try self.levelTargets();
        defer t.deinit();

        const dest: usize = if (src_level == 0) t.base_level else @as(usize, src_level) + 1;
        if (dest >= self.opts.max_levels) return;

        // --- collect src entries (all of L0; first entry of L≥1) ---
        const src_entries_slice = blk: {
            self.handlers[src].mu.lockShared();
            defer self.handlers[src].mu.unlockShared();

            const items = self.handlers[src].entries.items;
            if (items.len == 0) return;

            // TODO: This can be improved to consider which table to compact.
            const take = if (src_level == 0) items else items[0..1];
            break :blk try self.allocator.dupe(TableEntry, take);
        };
        defer self.allocator.free(src_entries_slice);

        // --- compute union key range of all src tables ---
        var kr = KeyRange{};
        for (src_entries_slice) |e| {
            const tkr = KeyRange{ .left = e.table.smallest(), .right = e.table.biggest() };
            kr.extend(tkr);
        }

        // --- collect overlapping dest (bot) entries ---
        const bot_entries_slice = try self.handlers[dest].overlappingTables(self.allocator, kr);
        defer self.allocator.free(bot_entries_slice);

        // --- build single MergeIterator over src tables + one ConcatIterator for bot ---
        const bot_iters = try self.allocator.alloc(TableIterator, bot_entries_slice.len);
        defer self.allocator.free(bot_iters);

        const all_iters = try self.allocator.alloc(Iterator, src_entries_slice.len + 1);
        defer self.allocator.free(all_iters);

        for (src_entries_slice, 0..) |e, i|
            all_iters[i] = .{ .table = TableIterator.init(self.allocator, e.table) };
        for (bot_entries_slice, 0..) |e, i|
            bot_iters[i] = TableIterator.init(self.allocator, e.table);
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
        };
        const file_sz: usize = @intCast(t.file_sz[dest]);

        var bld = Builder.init(self.allocator, cfg);
        defer bld.deinit();

        while (merge.itemRef()) |item| {
            try bld.add(item.key, item.value);

            try merge.next();
            if (bld.table_bytes.items.len >= file_sz) {
                try self.flushBuilderOutput(&bld, &output_tables);
                bld.reset();
            }
        }

        // Flush any remaining data.
        try self.flushBuilderOutput(&bld, &output_tables);

        // Best-effort directory sync so new SST filenames are visible after a crash.
        // Some filesystems (tmpfs, certain network mounts) return EINVAL or EBADF for
        // directory fsync — that's fine, we just skip it on those.
        {
            var dir = try std.fs.openDirAbsolute(self.opts.dir, .{});
            defer dir.close();
            _ = std.posix.system.fsync(dir.fd);
        }

        // --- build manifest changes ---
        var changes = try std.ArrayList(manifest.Change).initCapacity(
            self.allocator,
            src_entries_slice.len + bot_entries_slice.len + output_tables.items.len,
        );
        defer changes.deinit(self.allocator);

        for (src_entries_slice) |e| try changes.append(self.allocator, manifest.Change.delete(e.id));
        for (bot_entries_slice) |e| try changes.append(self.allocator, manifest.Change.delete(e.id));
        for (output_tables.items) |e| {
            try changes.append(self.allocator, manifest.Change.create(e.id, @intCast(dest)));
        }

        // Commit to manifest (durable) before touching the handlers.
        try self.ms.commit(changes.items);
        committed = true;

        // --- update handlers ---
        self.handlers[src].deleteEntries(src_entries_slice);
        try self.handlers[dest].replaceEntries(bot_entries_slice, output_tables.items);

        // Release our refs on output tables; handlers have taken their own refs via replaceEntries.
        for (output_tables.items) |e| e.table.decrRef();
    }

    /// flushBuilderOutput finishes the builder, writes the bytes to a new SST
    /// file on disk, mmaps it as a Table, and appends the entry to output.
    fn flushBuilderOutput(
        self: *LevelsController,
        bld: *Builder,
        output: *std.ArrayList(TableEntry),
    ) !void {
        var tb = try bld.finish();
        defer tb.deinit(); // always free the in-memory bytes; the file on disk is the source of truth
        if (tb.bytes.len == 0) return;

        const id = self.next_file_id.fetchAdd(1, .monotonic);
        const path = try std.fmt.allocPrint(
            self.allocator,
            "{s}/{d:0>6}.sst",
            .{ self.opts.dir, id },
        );
        defer self.allocator.free(path);

        {
            var buf: [4096]u8 = undefined;
            const file = try std.fs.createFileAbsolute(path, .{});
            defer file.close();

            var w = file.writer(&buf);
            try w.interface.writeAll(tb.bytes);
            try w.interface.flush();
            try file.sync();
        }
        // tb.bytes are now on disk; open the file via mmap for random access.
        const table_ptr = try Table.open(self.allocator, path); // ref = 1
        errdefer table_ptr.decrRef(); // on error: ref → 0 → deletes the file
        try output.append(self.allocator, .{ .id = id, .table = table_ptr });
    }
};
