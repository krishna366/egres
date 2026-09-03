const std = @import("std");
const utils = @import("utils");
const zdb = @import("zdb");

fn noopPersist(_: *anyopaque, _: u64) anyerror!void {}
const vfs = zdb.vfs;

const Key = zdb.kv.Key;
const _table = zdb.table;
const Builder = _table.builder.Builder;
const Table = _table.Table;
const TableIterator = _table.TableIterator;

const manifest = zdb.manifest;
const createSSTableChange = manifest.createSSTableChange;
const createTreeChange = manifest.createTreeChange;

const _level = zdb.level;
const Level = _level.Level;
const TableEntry = _level.TableEntry;
const KeyRange = _level.KeyRange;

const Tree = zdb.tree.Tree;
const TableCache = zdb.tree.TableCache;

const config = zdb.config;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns the absolute path of a TmpDir (caller frees). Sentinel slice
/// because realPathFileAlloc allocates len+1; declaring []u8 here would
/// make the caller's free undercount by 1 (DebugAllocator catches it).
fn tmpPath(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir) ![:0]u8 {
    return tmp.dir.realPathFileAlloc(utils.context.g().io, ".", alloc);
}

/// Builds a TableBuffer containing the given key/value pairs.
/// Keys must be in sorted order (caller's responsibility).
/// Each entry is (key_no_ts, ts, value).
fn buildTable(
    alloc: std.mem.Allocator,
    entries: []const struct { key: []const u8, ts: u64, val: []const u8 },
) !_table.builder.TableBuffer {
    var b = Builder.init(alloc, .{ .target_block_size = 4 * 1024 });
    defer b.deinit();
    for (entries) |e| {
        try b.add(.{ .primary = e.key, .timestamp = e.ts }, e.val);
    }
    return b.finish();
}

/// One row for buildTableMeta. Named rather than anonymous so helpers can
/// pass the same slice along — Zig gives each anonymous struct literal its
/// own distinct type.
const MetaEntry = struct {
    key: []const u8,
    ts: u64,
    val: []const u8,
    meta: zdb.kv.Meta = .{},
};

/// buildTable, but each entry carries a Meta. Entries must already be in
/// sort order: primary ascending, then timestamp DESCENDING.
fn buildTableMeta(
    alloc: std.mem.Allocator,
    entries: []const MetaEntry,
) !_table.builder.TableBuffer {
    var b = Builder.init(alloc, .{ .target_block_size = 4 * 1024 });
    defer b.deinit();
    for (entries) |e| {
        try b.addItem(.{
            .key = .{ .primary = e.key, .timestamp = e.ts },
            .value = e.val,
            .meta = e.meta,
        });
    }
    return b.finish();
}

/// Install `tb` as SST `id` at `level` for `tree_id`, on disk + in the manifest.
fn installTable(
    alloc: std.mem.Allocator,
    ms: *manifest.ForestState,
    dir_path: []const u8,
    tb: *_table.builder.TableBuffer,
    id: u64,
    tree_id: u64,
    level: u32,
) !void {
    const path = try zdb.file_ids.sstPath(alloc, dir_path, id);
    defer alloc.free(path);
    try vfs.writeFile(path, tb.bytes);
    try ms.commit(&.{createSSTableChange(id, 0, tree_id, level)}, &.{});
}

/// Sum of stale_data_size across a level's tables. buildCompaction records it
/// per output SST; nothing in production reads it back yet, so the test reaches
/// into the index directly rather than through a helper on Level.
fn levelStaleBytes(level: *Level) u64 {
    var total: u64 = 0;
    for (level.tables.items) |e| total += e.table.index.stale_data_size;
    return total;
}

/// Test helper: create an in-memory FileHandle from a buffer (dupes the data).
fn fromBuf(allocator: std.mem.Allocator, buf: []const u8) !vfs.FileHandle {
    const owned = try allocator.dupe(u8, buf);
    return .{
        .file_size = @intCast(owned.len),
        .owned_buf = owned,
        .buf_allocator = allocator,
        .vfs = vfs.g(),
    };
}

/// Opens a heap-allocated Table from a TableBuffer via VFS FileHandle.
/// Duplicates tb.bytes internally; caller retains ownership of tb.
fn openTableOwned(alloc: std.mem.Allocator, tb: *_table.builder.TableBuffer) !*Table {
    return Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
}

fn destroyTable(t: *Table) void {
    const allocator = t.allocator;
    t.decrRef();
    allocator.destroy(t);
}

// ---------------------------------------------------------------------------
// 1. level_handler L0 sort
// ---------------------------------------------------------------------------

test "levels: handler L0 sort" {
    const alloc = std.testing.allocator;

    var tb3 = try buildTable(alloc, &.{.{ .key = "c", .ts = 0, .val = "v" }});
    defer tb3.deinit();
    var tb1 = try buildTable(alloc, &.{.{ .key = "a", .ts = 0, .val = "v" }});
    defer tb1.deinit();
    var tb2 = try buildTable(alloc, &.{.{ .key = "b", .ts = 0, .val = "v" }});
    defer tb2.deinit();

    const t3 = try openTableOwned(alloc, &tb3);
    defer destroyTable(t3);
    const t1 = try openTableOwned(alloc, &tb1);
    defer destroyTable(t1);
    const t2 = try openTableOwned(alloc, &tb2);
    defer destroyTable(t2);

    var lock: zdb.level.Lock = .{};

    var level = Level.init(alloc, 0, &lock);

    const held = lock.hold(.exclusive, @src());

    defer held.release();
    defer level.deinit();

    try level.addEntry(held, 3, t3);
    try level.addEntry(held, 1, t1);
    try level.addEntry(held, 2, t2);

    // L0 sorts ascending by ID.
    try std.testing.expectEqual(@as(u64, 1), level.tables.items[0].id);
    try std.testing.expectEqual(@as(u64, 2), level.tables.items[1].id);
    try std.testing.expectEqual(@as(u64, 3), level.tables.items[2].id);
}

// ---------------------------------------------------------------------------
// 2. level_handler L1 sort
// ---------------------------------------------------------------------------

test "levels: handler L1 sort" {
    const alloc = std.testing.allocator;

    // Three tables at L1 with non-overlapping key ranges (added out of order).
    var tbc = try buildTable(alloc, &.{.{ .key = "c", .ts = 0, .val = "vc" }});
    defer tbc.deinit();
    var tba = try buildTable(alloc, &.{.{ .key = "a", .ts = 0, .val = "va" }});
    defer tba.deinit();
    var tbb = try buildTable(alloc, &.{.{ .key = "b", .ts = 0, .val = "vb" }});
    defer tbb.deinit();

    const tc = try openTableOwned(alloc, &tbc);
    defer destroyTable(tc);
    const ta = try openTableOwned(alloc, &tba);
    defer destroyTable(ta);
    const tb = try openTableOwned(alloc, &tbb);
    defer destroyTable(tb);

    var lock: zdb.level.Lock = .{};

    var level = Level.init(alloc, 1, &lock);

    const held = lock.hold(.exclusive, @src());

    defer held.release();
    defer level.deinit();

    try level.addEntry(held, 3, tc); // "c"
    try level.addEntry(held, 1, ta); // "a"
    try level.addEntry(held, 2, tb); // "b"

    // L1 sorts by smallest key ascending.
    const sk0 = level.tables.items[0].table.smallest.primary;
    const sk1 = level.tables.items[1].table.smallest.primary;
    const sk2 = level.tables.items[2].table.smallest.primary;
    try std.testing.expectEqualStrings("a", sk0);
    try std.testing.expectEqualStrings("b", sk1);
    try std.testing.expectEqualStrings("c", sk2);
}

// ---------------------------------------------------------------------------
// 3. overlapping tables L1
// ---------------------------------------------------------------------------

test "levels: overlapping tables L1" {
    const alloc = std.testing.allocator;

    // 5 tables with ranges: a-b, c-d, e-f, g-h, i-j
    const ranges = [_][2][]const u8{
        .{ "a", "b" },
        .{ "c", "d" },
        .{ "e", "f" },
        .{ "g", "h" },
        .{ "i", "j" },
    };

    var tables: [5]*Table = undefined;
    var tbs: [5]_table.builder.TableBuffer = undefined;
    for (ranges, 0..) |r, i| {
        tbs[i] = try buildTable(alloc, &.{
            .{ .key = r[0], .ts = 0, .val = "v" },
            .{ .key = r[1], .ts = 0, .val = "v" },
        });
        tables[i] = try openTableOwned(alloc, &tbs[i]);
    }
    defer for (&tbs) |*tb| tb.deinit();
    defer for (tables, 0..) |t, i| {
        destroyTable(t);
        _ = i;
    };

    var lock: zdb.level.Lock = .{};

    var level = Level.init(alloc, 1, &lock);

    const held = lock.hold(.exclusive, @src());

    defer held.release();
    defer level.deinit();
    for (tables, 0..) |t, i| {
        try level.addEntry(held, @intCast(i + 1), t);
    }

    // Query range c-g should overlap tables c-d, e-f, g-h → indices 1..4.
    const ck = try (Key{ .primary = "c", .timestamp = 0 }).marshal(alloc);
    defer alloc.free(ck);
    const gk = try (Key{ .primary = "g", .timestamp = 0 }).marshal(alloc);
    defer alloc.free(gk);

    const result = try level.overlappingTables(held.shared(), alloc, .{ .left = Key.init(ck), .right = Key.init(gk) });
    defer Level.releaseTables(alloc, result);
    try std.testing.expectEqual(@as(usize, 3), result.len);
}

// ---------------------------------------------------------------------------
// 4. overlapping tables L0
// ---------------------------------------------------------------------------

test "levels: overlapping tables L0" {
    const alloc = std.testing.allocator;

    var tb1 = try buildTable(alloc, &.{.{ .key = "a", .ts = 0, .val = "v" }});
    defer tb1.deinit();
    var tb2 = try buildTable(alloc, &.{.{ .key = "b", .ts = 0, .val = "v" }});
    defer tb2.deinit();
    var tb3 = try buildTable(alloc, &.{.{ .key = "c", .ts = 0, .val = "v" }});
    defer tb3.deinit();

    const t1 = try openTableOwned(alloc, &tb1);
    defer destroyTable(t1);
    const t2 = try openTableOwned(alloc, &tb2);
    defer destroyTable(t2);
    const t3 = try openTableOwned(alloc, &tb3);
    defer destroyTable(t3);

    var lock: zdb.level.Lock = .{};

    var level = Level.init(alloc, 0, &lock);

    const held = lock.hold(.exclusive, @src());

    defer held.release();
    defer level.deinit();
    try level.addEntry(held, 1, t1);
    try level.addEntry(held, 2, t2);
    try level.addEntry(held, 3, t3);

    const ck = try (Key{ .primary = "x", .timestamp = 0 }).marshal(alloc);
    defer alloc.free(ck);

    // L0 always returns all tables regardless of the query.
    const result = try level.overlappingTables(held.shared(), alloc, .{ .left = Key.init(ck), .right = Key.init(ck) });
    defer Level.releaseTables(alloc, result);
    try std.testing.expectEqual(@as(usize, 3), result.len);
}

// ---------------------------------------------------------------------------
// 5. get L0 newest wins
// ---------------------------------------------------------------------------

test "levels: get L0 newest wins" {
    const alloc = std.testing.allocator;

    // Table ID=1: key "k" at ts=3 → "old"
    // Table ID=2: key "k" at ts=5 → "new"
    var tb1 = try buildTable(alloc, &.{.{ .key = "k", .ts = 3, .val = "old" }});
    defer tb1.deinit();
    var tb2 = try buildTable(alloc, &.{.{ .key = "k", .ts = 5, .val = "new" }});
    defer tb2.deinit();

    const t1 = try openTableOwned(alloc, &tb1);
    defer destroyTable(t1);
    const t2 = try openTableOwned(alloc, &tb2);
    defer destroyTable(t2);

    var lock: zdb.level.Lock = .{};

    var level = Level.init(alloc, 0, &lock);

    const held = lock.hold(.exclusive, @src());

    defer held.release();
    defer level.deinit();
    try level.addEntry(held, 1, t1);
    try level.addEntry(held, 2, t2);

    // Search with max ts to find the newest version.
    const item = try level.get(held.shared(), .{ .primary = "k", .timestamp = std.math.maxInt(u64) }, .p0);
    try std.testing.expect(item != null);
    defer {
        alloc.free(item.?.key.primary);
        alloc.free(item.?.value);
    }
    try std.testing.expectEqualStrings("new", item.?.value);
}

// ---------------------------------------------------------------------------
// 6. get L1 hit
// ---------------------------------------------------------------------------

test "levels: get L1 hit" {
    const alloc = std.testing.allocator;

    var tba = try buildTable(alloc, &.{.{ .key = "a", .ts = 0, .val = "va" }});
    defer tba.deinit();
    var tbb = try buildTable(alloc, &.{.{ .key = "b", .ts = 0, .val = "vb" }});
    defer tbb.deinit();
    var tbc = try buildTable(alloc, &.{.{ .key = "c", .ts = 0, .val = "vc" }});
    defer tbc.deinit();

    const ta = try openTableOwned(alloc, &tba);
    defer destroyTable(ta);
    const tb = try openTableOwned(alloc, &tbb);
    defer destroyTable(tb);
    const tc = try openTableOwned(alloc, &tbc);
    defer destroyTable(tc);

    var lock: zdb.level.Lock = .{};

    var level = Level.init(alloc, 1, &lock);

    const held = lock.hold(.exclusive, @src());

    defer held.release();
    defer level.deinit();
    try level.addEntry(held, 1, ta);
    try level.addEntry(held, 2, tb);
    try level.addEntry(held, 3, tc);

    const item = try level.get(held.shared(), .{ .primary = "b", .timestamp = 0 }, .p0);
    try std.testing.expect(item != null);
    defer {
        alloc.free(item.?.key.primary);
        alloc.free(item.?.value);
    }
    try std.testing.expectEqualStrings("vb", item.?.value);
}

// ---------------------------------------------------------------------------
// 7. get miss
// ---------------------------------------------------------------------------

test "levels: get miss" {
    const alloc = std.testing.allocator;

    var tba = try buildTable(alloc, &.{.{ .key = "a", .ts = 0, .val = "va" }});
    defer tba.deinit();
    const ta = try openTableOwned(alloc, &tba);
    defer destroyTable(ta);

    var lock: zdb.level.Lock = .{};

    var level = Level.init(alloc, 1, &lock);

    const held = lock.hold(.exclusive, @src());

    defer held.release();
    defer level.deinit();
    try level.addEntry(held, 1, ta);

    const item = try level.get(held.shared(), .{ .primary = "z", .timestamp = 0 }, .p0);
    try std.testing.expect(item == null);
}

// ---------------------------------------------------------------------------
// 8. levels open
// ---------------------------------------------------------------------------

test "levels: open" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // Write SST 1 at L0, SST 2 at L1.
    {
        var tb1 = try buildTable(alloc, &.{.{ .key = "a", .ts = 0, .val = "va" }});
        defer tb1.deinit();
        var tb2 = try buildTable(alloc, &.{.{ .key = "b", .ts = 0, .val = "vb" }});
        defer tb2.deinit();

        const p1 = try zdb.file_ids.sstPath(alloc, dir_path, 1);
        defer alloc.free(p1);
        const p2 = try zdb.file_ids.sstPath(alloc, dir_path, 2);
        defer alloc.free(p2);

        try vfs.writeFile(p1, tb1.bytes);
        try vfs.writeFile(p2, tb2.bytes);
    }

    // Create manifest with table 1 at L0, table 2 at L1.
    const test_tree_id: u64 = 1;
    var ms = manifest.ForestState.init(alloc);
    defer ms.deinit();
    try ms.commit(&.{}, &.{createTreeChange(test_tree_id, 0, &.{}, &.{})});
    try ms.commit(&.{
        createSSTableChange(1, 0, test_tree_id, 0),
        createSSTableChange(2, 0, test_tree_id, 1),
    }, &.{});

    const opts = config.Config{ .dir = dir_path };
    var cache = TableCache.init(alloc, opts.dir);
    defer cache.deinit();
    const tree = try Tree.open(alloc, opts, 0, test_tree_id, &ms, &cache);
    defer tree.release();

    try std.testing.expectEqual(@as(usize, 1), tree.numTablesAt(0));
    try std.testing.expectEqual(@as(usize, 1), tree.numTablesAt(1));
    // All other levels empty.
    for (2..tree.levels.len) |i| {
        try std.testing.expectEqual(@as(usize, 0), tree.numTablesAt(i));
    }
}

// ---------------------------------------------------------------------------
// 9. pick compact level
// ---------------------------------------------------------------------------

test "levels: pick compact level" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // Create an empty manifest (no SST files needed for this test).
    const test_tree_id: u64 = 1;
    var ms = manifest.ForestState.init(alloc);
    defer ms.deinit();

    // Targets cascade from `tree_split_size`; setting it to a single byte
    // makes every level's target tiny so one table in L1 exceeds it and
    // becomes the compaction candidate.
    const opts = config.Config{
        .dir = dir_path,
        .tree_split_size = 1,
    };

    var cache = TableCache.init(alloc, opts.dir);
    defer cache.deinit();
    const tree = try Tree.open(alloc, opts, 0, test_tree_id, &ms, &cache);
    defer tree.release();

    // Manually plant a large table in L1.
    var tb = try buildTable(alloc, &.{
        .{ .key = "a", .ts = 0, .val = "va" },
        .{ .key = "b", .ts = 0, .val = "vb" },
    });
    defer tb.deinit();
    const t = try openTableOwned(alloc, &tb);
    _ = try cache.own(999, t);
    {
        const h = tree.mu.hold(.exclusive, @src());
        defer h.release();
        try tree.levels[1].addEntry(h, 999, t);
    }
    t.decrRef(); // now levels owns the table.

    try tree.updateCompactCandidate();
    try std.testing.expect(tree.compact_candidate != null);
    try std.testing.expectEqual(@as(u8, 1), tree.compact_candidate.?.level);
}

// ---------------------------------------------------------------------------
// 10. do compact L0→L1
// ---------------------------------------------------------------------------

test "levels: doCompact L0 L1" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // Write three L0 SST files with non-overlapping keys.
    const l0_data = [_]struct { id: u64, key: []const u8, val: []const u8 }{
        .{ .id = 1, .key = "aa", .val = "vaa" },
        .{ .id = 2, .key = "bb", .val = "vbb" },
        .{ .id = 3, .key = "cc", .val = "vcc" },
    };

    const test_tree_id: u64 = 1;
    var ms = manifest.ForestState.init(alloc);
    defer ms.deinit();

    for (l0_data) |d| {
        var tb = try buildTable(alloc, &.{.{ .key = d.key, .ts = 0, .val = d.val }});
        defer tb.deinit();
        const path = try zdb.file_ids.sstPath(alloc, dir_path, d.id);
        defer alloc.free(path);
        try vfs.writeFile(path, tb.bytes);
    }
    try ms.commit(&.{}, &.{createTreeChange(test_tree_id, 0, &.{}, &.{})});
    try ms.commit(&.{
        createSSTableChange(1, 0, test_tree_id, 0),
        createSSTableChange(2, 0, test_tree_id, 0),
        createSSTableChange(3, 0, test_tree_id, 0),
    }, &.{});

    // Use max_levels=2 so base_level=1 and doCompact(0) goes L0→L1.
    const opts = config.Config{ .dir = dir_path, .max_levels = 2 };
    var cache = TableCache.init(alloc, opts.dir);
    defer cache.deinit();
    const tree = try Tree.open(alloc, opts, 0, test_tree_id, &ms, &cache);
    defer tree.release();

    // All three tables should be in L0 before compaction.
    try std.testing.expectEqual(@as(usize, 3), tree.numTablesAt(0));
    try std.testing.expectEqual(@as(usize, 0), tree.numTablesAt(1));

    var file_ids: zdb.file_ids.FileIds = .{};
    file_ids.bind(1, 100, @ptrCast(&file_ids), noopPersist);
    try tree.doCompact(0, &file_ids, 0);

    // After compaction L0 must be empty and keys must live in L1.
    try std.testing.expectEqual(@as(usize, 0), tree.numTablesAt(0));
    try std.testing.expect(tree.numTablesAt(1) > 0);

    // All three keys must be retrievable via get.
    for (l0_data) |d| {
        const item = try tree.get(.{ .primary = d.key, .timestamp = std.math.maxInt(u64) }, .p0);
        try std.testing.expect(item != null);
        defer {
            alloc.free(item.?.key.primary);
            alloc.free(item.?.value);
        }
        try std.testing.expectEqualStrings(d.val, item.?.value);
    }
}

// ---------------------------------------------------------------------------
// 11. do compact L0→L1 with dest overlap
// ---------------------------------------------------------------------------

test "levels: doCompact L0 L1 with dest overlap" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const test_tree_id: u64 = 1;
    var ms = manifest.ForestState.init(alloc);
    defer ms.deinit();

    // L0 table 1: {aa→vaa}
    {
        var tb = try buildTable(alloc, &.{.{ .key = "aa", .ts = 0, .val = "vaa" }});
        defer tb.deinit();
        const p = try zdb.file_ids.sstPath(alloc, dir_path, 1);
        defer alloc.free(p);
        try vfs.writeFile(p, tb.bytes);
    }
    // L0 table 2: {cc→vcc}
    {
        var tb = try buildTable(alloc, &.{.{ .key = "cc", .ts = 0, .val = "vcc" }});
        defer tb.deinit();
        const p = try zdb.file_ids.sstPath(alloc, dir_path, 2);
        defer alloc.free(p);
        try vfs.writeFile(p, tb.bytes);
    }
    // L1 table 3: {bb→vbb, dd→vdd} — key range [bb..dd] overlaps [aa..cc].
    {
        var tb = try buildTable(alloc, &.{
            .{ .key = "bb", .ts = 0, .val = "vbb" },
            .{ .key = "dd", .ts = 0, .val = "vdd" },
        });
        defer tb.deinit();
        const p = try zdb.file_ids.sstPath(alloc, dir_path, 3);
        defer alloc.free(p);
        try vfs.writeFile(p, tb.bytes);
    }
    try ms.commit(&.{}, &.{createTreeChange(test_tree_id, 0, &.{}, &.{})});
    try ms.commit(&.{
        createSSTableChange(1, 0, test_tree_id, 0),
        createSSTableChange(2, 0, test_tree_id, 0),
        createSSTableChange(3, 0, test_tree_id, 1),
    }, &.{});

    // max_levels=2: L0 compacts directly to L1 (base_level=1).
    const opts = config.Config{ .dir = dir_path, .max_levels = 2 };
    var cache = TableCache.init(alloc, opts.dir);
    defer cache.deinit();
    const tree = try Tree.open(alloc, opts, 0, test_tree_id, &ms, &cache);
    defer tree.release();

    try std.testing.expectEqual(@as(usize, 2), tree.numTablesAt(0));
    try std.testing.expectEqual(@as(usize, 1), tree.numTablesAt(1));

    var file_ids: zdb.file_ids.FileIds = .{};
    file_ids.bind(1, 100, @ptrCast(&file_ids), noopPersist);
    try tree.doCompact(0, &file_ids, 0);

    // L0 must be empty; L1 holds the merged result in a single table.
    try std.testing.expectEqual(@as(usize, 0), tree.numTablesAt(0));
    try std.testing.expectEqual(@as(usize, 1), tree.numTablesAt(1));

    // All four keys from both levels must be present after compaction.
    const expected = [_]struct { key: []const u8, val: []const u8 }{
        .{ .key = "aa", .val = "vaa" },
        .{ .key = "bb", .val = "vbb" },
        .{ .key = "cc", .val = "vcc" },
        .{ .key = "dd", .val = "vdd" },
    };
    for (expected) |e| {
        const item = try tree.get(.{ .primary = e.key, .timestamp = std.math.maxInt(u64) }, .p0);
        try std.testing.expect(item != null);
        defer {
            alloc.free(item.?.key.primary);
            alloc.free(item.?.value);
        }
        try std.testing.expectEqualStrings(e.val, item.?.value);
    }
}

// ---------------------------------------------------------------------------
// 12. do compact L1→L2
// ---------------------------------------------------------------------------

test "levels: doCompact L1 L2" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // L1: {aa, bb}; L2: {cc} — non-overlapping, so compacting L1 doesn't
    // touch the L2 table; both survive in L2 after compaction.
    const test_tree_id: u64 = 1;
    var ms = manifest.ForestState.init(alloc);
    defer ms.deinit();

    {
        var tb1 = try buildTable(alloc, &.{
            .{ .key = "aa", .ts = 0, .val = "vaa" },
            .{ .key = "bb", .ts = 0, .val = "vbb" },
        });
        defer tb1.deinit();
        const p1 = try zdb.file_ids.sstPath(alloc, dir_path, 1);
        defer alloc.free(p1);
        try vfs.writeFile(p1, tb1.bytes);

        var tb2 = try buildTable(alloc, &.{.{ .key = "cc", .ts = 0, .val = "vcc" }});
        defer tb2.deinit();
        const p2 = try zdb.file_ids.sstPath(alloc, dir_path, 2);
        defer alloc.free(p2);
        try vfs.writeFile(p2, tb2.bytes);
    }
    try ms.commit(&.{}, &.{createTreeChange(test_tree_id, 0, &.{}, &.{})});
    try ms.commit(&.{
        createSSTableChange(1, 0, test_tree_id, 1),
        createSSTableChange(2, 0, test_tree_id, 2),
    }, &.{});

    // max_levels=3: doCompact(1) goes L1→L2 (dest = src + 1 = 2).
    const opts = config.Config{ .dir = dir_path, .max_levels = 3 };
    var cache = TableCache.init(alloc, opts.dir);
    defer cache.deinit();
    const tree = try Tree.open(alloc, opts, 0, test_tree_id, &ms, &cache);
    defer tree.release();

    try std.testing.expectEqual(@as(usize, 1), tree.numTablesAt(1));
    try std.testing.expectEqual(@as(usize, 1), tree.numTablesAt(2));

    var file_ids: zdb.file_ids.FileIds = .{};
    file_ids.bind(1, 100, @ptrCast(&file_ids), noopPersist);
    try tree.doCompact(1, &file_ids, 0);

    // L1 must be empty; L2 holds the original {cc} table plus new {aa, bb}.
    try std.testing.expectEqual(@as(usize, 0), tree.numTablesAt(1));
    try std.testing.expectEqual(@as(usize, 2), tree.numTablesAt(2));

    const expected = [_]struct { key: []const u8, val: []const u8 }{
        .{ .key = "aa", .val = "vaa" },
        .{ .key = "bb", .val = "vbb" },
        .{ .key = "cc", .val = "vcc" },
    };
    for (expected) |e| {
        const item = try tree.get(.{ .primary = e.key, .timestamp = std.math.maxInt(u64) }, .p2);
        try std.testing.expect(item != null);
        defer {
            alloc.free(item.?.key.primary);
            alloc.free(item.?.value);
        }
        try std.testing.expectEqualStrings(e.val, item.?.value);
    }
}

// ---------------------------------------------------------------------------
// 13. levels get: L0 shadows L1
// ---------------------------------------------------------------------------

test "levels: get L0 shadows L1" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const test_tree_id: u64 = 1;
    var ms = manifest.ForestState.init(alloc);
    defer ms.deinit();

    const opts = config.Config{ .dir = dir_path };
    var cache = TableCache.init(alloc, opts.dir);
    defer cache.deinit();
    const tree = try Tree.open(alloc, opts, 0, test_tree_id, &ms, &cache);
    defer tree.release();

    // Plant in-memory tables directly into the level handlers.
    // L1: foo at ts=1 (old version).
    var tb_l1 = try buildTable(alloc, &.{.{ .key = "foo", .ts = 1, .val = "l1_val" }});
    defer tb_l1.deinit();
    const t_l1 = try openTableOwned(alloc, &tb_l1);
    _ = try cache.own(1, t_l1);
    {
        const h = tree.mu.hold(.exclusive, @src());
        defer h.release();
        try tree.levels[1].addEntry(h, 1, t_l1);
    }
    t_l1.decrRef();

    // L0: foo at ts=5 (newer version; higher ts sorts earlier in byte order).
    var tb_l0 = try buildTable(alloc, &.{.{ .key = "foo", .ts = 5, .val = "l0_val" }});
    defer tb_l0.deinit();
    const t_l0 = try openTableOwned(alloc, &tb_l0);
    _ = try cache.own(2, t_l0);
    {
        const h = tree.mu.hold(.exclusive, @src());
        defer h.release();
        try tree.levels[0].addEntry(h, 2, t_l0);
    }
    t_l0.decrRef();

    // Searching with maxInt ts seeks to the very beginning of "foo" entries;
    // L0 is checked first → the ts=5 version wins.
    {
        const item = try tree.get(.{ .primary = "foo", .timestamp = std.math.maxInt(u64) }, .p1);
        try std.testing.expect(item != null);
        defer {
            alloc.free(item.?.key.primary);
            alloc.free(item.?.value);
        }
        try std.testing.expectEqualStrings("l0_val", item.?.value);
    }

    // Searching with ts=3: L0's ts=5 entry sorts *before* ts=3 in byte order
    // (higher ts ↔ smaller suffix), so the L0 seek lands past it → L0 miss.
    // L1's ts=1 entry sorts *after* ts=3, so the L1 seek lands on it → hit.
    {
        const item = try tree.get(.{ .primary = "foo", .timestamp = 3 }, .p2);
        try std.testing.expect(item != null);
        defer {
            alloc.free(item.?.key.primary);
            alloc.free(item.?.value);
        }
        try std.testing.expectEqualStrings("l1_val", item.?.value);
    }
}

test "levels: overlappingTables finds a dest table sharing the boundary primary" {
    const alloc = std.testing.allocator;

    // Dest table holding an OLDER version of "k", plus a later primary.
    // Sort order is primary asc, then ts DESC, so this is ascending.
    var tb = try buildTable(alloc, &.{
        .{ .key = "k", .ts = 50, .val = "old" },
        .{ .key = "m", .ts = 20, .val = "v" },
    });
    defer tb.deinit();
    const t = try openTableOwned(alloc, &tb);
    defer destroyTable(t);

    var lock: zdb.level.Lock = .{};

    var level = Level.init(alloc, 1, &lock);

    const held = lock.hold(.exclusive, @src());

    defer held.release();
    defer level.deinit();
    try level.addEntry(held, 1, t);

    // A src table {(a,10), (k,100), (k,90)} has smallest=(a,10) and
    // biggest=(k,90) — biggest is the LOWEST-ts version of its largest
    // primary, because ts sorts descending. That's the range a compaction
    // probes dest with.
    const left = try (Key{ .primary = "a", .timestamp = 10 }).marshal(alloc);
    defer alloc.free(left);
    const right = try (Key{ .primary = "k", .timestamp = 90 }).marshal(alloc);
    defer alloc.free(right);

    const result = try level.overlappingTables(held.shared(), alloc, .{
        .left = Key.init(left),
        .right = Key.init(right),
    });
    defer Level.releaseTables(alloc, result);

    // The dest table holds "k" and MUST participate: otherwise the compaction
    // writes k@100/k@90 into dest while k@50 stays in a separate dest table,
    // putting two tables at the same level in charge of one primary.
    try std.testing.expectEqual(@as(usize, 1), result.len);
}

// ---------------------------------------------------------------------------
// Tombstone dropping in compaction
// ---------------------------------------------------------------------------

/// Open a one-tree forest-less Tree over `dir_path` with `max_levels`, holding
/// a single L0 SST built from `entries`. Returns the tree; caller closes it.
const TombstoneFixture = struct {
    ms: manifest.ForestState,
    cache: TableCache,
    tree: *Tree,
    file_ids: zdb.file_ids.FileIds,

    fn deinit(self: *TombstoneFixture) void {
        self.tree.release();
        self.cache.deinit();
        self.ms.deinit();
    }
};

fn setupTombstoneTree(
    alloc: std.mem.Allocator,
    fx: *TombstoneFixture,
    dir_path: []const u8,
    max_levels: u8,
    entries: []const MetaEntry,
) !void {
    const tree_id: u64 = 1;
    fx.ms = manifest.ForestState.init(alloc);
    errdefer fx.ms.deinit();
    try fx.ms.commit(&.{}, &.{createTreeChange(tree_id, 0, &.{}, &.{})});

    var tb = try buildTableMeta(alloc, entries);
    defer tb.deinit();
    try installTable(alloc, &fx.ms, dir_path, &tb, 1, tree_id, 0);

    const opts = config.Config{ .dir = dir_path, .max_levels = max_levels };
    fx.cache = TableCache.init(alloc, opts.dir);
    errdefer fx.cache.deinit();
    fx.tree = try Tree.open(alloc, opts, 0, tree_id, &fx.ms, &fx.cache);
    fx.file_ids = .{};
    fx.file_ids.bind(1, 100, @ptrCast(&fx.file_ids), noopPersist);
}

test "compaction: bottom level drops a tombstone below the horizon and what it shadows" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // max_levels=2 → L0's dest is L1, which IS the bottom level.
    var fx: TombstoneFixture = undefined;
    try setupTombstoneTree(alloc, &fx, dir_path, 2, &.{
        .{ .key = "k", .ts = 100, .val = "", .meta = .{ .kind = .deleted } },
        .{ .key = "k", .ts = 50, .val = "old" },
    });
    defer fx.deinit();

    // Horizon above the tombstone: nothing can read at or below ts=100 any
    // more, so the tombstone and the version it shadows are both unreachable.
    try fx.tree.doCompact(0, &fx.file_ids, 200);

    try std.testing.expect((try fx.tree.get(
        .{ .primary = "k", .timestamp = std.math.maxInt(u64) },
        .p0,
    )) == null);
}

test "compaction: a tombstone above the horizon is retained with the state it shadows" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    var fx: TombstoneFixture = undefined;
    try setupTombstoneTree(alloc, &fx, dir_path, 2, &.{
        .{ .key = "k", .ts = 100, .val = "", .meta = .{ .kind = .deleted } },
        .{ .key = "k", .ts = 50, .val = "old" },
    });
    defer fx.deinit();

    // Horizon below the tombstone: the deletion is not settled. The tombstone
    // is the newest version and stays — and so does the state it shadows,
    // because the state@50 is the first terminal at or below the horizon (50),
    // i.e. the oldest version an allowed read can still resolve to.
    try fx.tree.doCompact(0, &fx.file_ids, 50);

    const got = try fx.tree.get(
        .{ .primary = "k", .timestamp = std.math.maxInt(u64) },
        .p0,
    ) orelse return error.TestExpectedItem;
    defer got.free(alloc);
    try std.testing.expect(got.meta.isDeleted());
    try std.testing.expectEqual(@as(u64, 100), got.key.timestamp);

    // A read at 60 (>= horizon, so allowed) sees the pre-delete value: the
    // tombstone at 100 hasn't happened yet from there, so the shadowed state
    // is exactly what a correct MVCC read returns — and thus must survive GC.
    const mid = try fx.tree.get(.{ .primary = "k", .timestamp = 60 }, .p0) orelse
        return error.TestExpectedItem;
    defer mid.free(alloc);
    try std.testing.expectEqualStrings("old", mid.value);
}

test "compaction: a non-bottom level never drops tombstones" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // max_levels=4 → L0's dest is L1, and L2/L3 sit below it. An older
    // version of "k" could be down there, so dropping here would resurrect it.
    var fx: TombstoneFixture = undefined;
    try setupTombstoneTree(alloc, &fx, dir_path, 4, &.{
        .{ .key = "k", .ts = 100, .val = "", .meta = .{ .kind = .deleted } },
        .{ .key = "k", .ts = 50, .val = "old" },
    });
    defer fx.deinit();

    // Horizon is way above the tombstone — eligible on every count EXCEPT
    // that this compaction isn't writing into the bottom level.
    try fx.tree.doCompact(0, &fx.file_ids, 200);

    const got = try fx.tree.get(
        .{ .primary = "k", .timestamp = std.math.maxInt(u64) },
        .p0,
    ) orelse return error.TestExpectedItem;
    defer got.free(alloc);
    try std.testing.expect(got.meta.isDeleted());
}

test "compaction: a put above the horizon survives a lower tombstone's drop" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // The key is re-created after being deleted. Sort order is ts descending.
    var fx: TombstoneFixture = undefined;
    try setupTombstoneTree(alloc, &fx, dir_path, 2, &.{
        .{ .key = "k", .ts = 300, .val = "new" },
        .{ .key = "k", .ts = 100, .val = "", .meta = .{ .kind = .deleted } },
        .{ .key = "k", .ts = 50, .val = "old" },
    });
    defer fx.deinit();

    try fx.tree.doCompact(0, &fx.file_ids, 200);

    // k@300 is above the horizon: still readable, untouched.
    const got = try fx.tree.get(
        .{ .primary = "k", .timestamp = std.math.maxInt(u64) },
        .p0,
    ) orelse return error.TestExpectedItem;
    defer got.free(alloc);
    try std.testing.expectEqualStrings("new", got.value);
    try std.testing.expect(!got.meta.isDeleted());

    // k@100 and k@50 are gone. A read at 250 used to find the tombstone and
    // report "deleted"; now it finds nothing — the same answer, which is
    // exactly why the drop was legal.
    try std.testing.expect((try fx.tree.get(
        .{ .primary = "k", .timestamp = 250 },
        .p0,
    )) == null);
}

test "compaction: dropping one primary does not disturb its neighbours" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // "j" and "m" bracket the dropped primary: the skip must end when the
    // primary changes, not run on and eat them.
    var fx: TombstoneFixture = undefined;
    try setupTombstoneTree(alloc, &fx, dir_path, 2, &.{
        .{ .key = "j", .ts = 10, .val = "jv" },
        .{ .key = "k", .ts = 100, .val = "", .meta = .{ .kind = .deleted } },
        .{ .key = "k", .ts = 50, .val = "old" },
        .{ .key = "m", .ts = 10, .val = "mv" },
    });
    defer fx.deinit();

    try fx.tree.doCompact(0, &fx.file_ids, 200);

    const j = try fx.tree.get(.{ .primary = "j", .timestamp = std.math.maxInt(u64) }, .p0) orelse
        return error.TestExpectedItem;
    defer j.free(alloc);
    try std.testing.expectEqualStrings("jv", j.value);
    const m = try fx.tree.get(.{ .primary = "m", .timestamp = std.math.maxInt(u64) }, .p0) orelse
        return error.TestExpectedItem;
    defer m.free(alloc);
    try std.testing.expectEqualStrings("mv", m.value);
    try std.testing.expect((try fx.tree.get(
        .{ .primary = "k", .timestamp = std.math.maxInt(u64) },
        .p0,
    )) == null);
}

// ---------------------------------------------------------------------------
// Version GC: shadowed updates + stale accounting
// ---------------------------------------------------------------------------

test "compaction: bottom level drops shadowed versions once they reach the horizon" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // Three plain (no tombstone) versions of "k".
    var fx: TombstoneFixture = undefined;
    try setupTombstoneTree(alloc, &fx, dir_path, 2, &.{
        .{ .key = "k", .ts = 500, .val = "v500" },
        .{ .key = "k", .ts = 300, .val = "v300" },
        .{ .key = "k", .ts = 100, .val = "v100" },
    });
    defer fx.deinit();

    // Horizon 600: all three are at or below it, so the two older versions are
    // collectable. The newest value (k@500) is the current value and stays.
    try fx.tree.doCompact(0, &fx.file_ids, 600);

    const got = try fx.tree.get(.{ .primary = "k", .timestamp = std.math.maxInt(u64) }, .p0) orelse
        return error.TestExpectedItem;
    defer got.free(alloc);
    try std.testing.expectEqualStrings("v500", got.value);
    // A read that would have landed on k@300 finds nothing — it's collected.
    try std.testing.expect((try fx.tree.get(.{ .primary = "k", .timestamp = 400 }, .p0)) == null);
    // Only the newest survived → nothing shadowed remains → no stale data.
    try std.testing.expectEqual(@as(u64, 0), levelStaleBytes(&fx.tree.levels[1]));
}

test "compaction: bottom level keeps a shadowed version above the horizon and counts it stale" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    var fx: TombstoneFixture = undefined;
    try setupTombstoneTree(alloc, &fx, dir_path, 2, &.{
        .{ .key = "k", .ts = 500, .val = "v500" },
        .{ .key = "k", .ts = 300, .val = "v300" },
    });
    defer fx.deinit();

    // Horizon 200: both versions are above it, so nothing is collected yet — a
    // read at 400 legitimately sees k@300, and it stays until the horizon
    // rises past 300.
    try fx.tree.doCompact(0, &fx.file_ids, 200);

    const newest = try fx.tree.get(.{ .primary = "k", .timestamp = std.math.maxInt(u64) }, .p0) orelse
        return error.TestExpectedItem;
    defer newest.free(alloc);
    try std.testing.expectEqualStrings("v500", newest.value);
    const mid = try fx.tree.get(.{ .primary = "k", .timestamp = 350 }, .p0) orelse
        return error.TestExpectedItem;
    defer mid.free(alloc);
    try std.testing.expectEqualStrings("v300", mid.value);
    // k@300 is shadowed by the newer full row k@500 → counted stale: primary
    // "k" (1) + value "v300" (4). Reclaimed once the horizon rises past 500.
    try std.testing.expectEqual(@as(u64, 5), levelStaleBytes(&fx.tree.levels[1]));
}

test "compaction: a non-bottom level keeps everything but still counts stale" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // max_levels=4 → L0's dest is L1, well above the bottom (L3). Nothing is
    // dropped no matter how far the horizon has advanced, but the shadowed
    // version is still accounted so a future trigger can see the garbage.
    var fx: TombstoneFixture = undefined;
    try setupTombstoneTree(alloc, &fx, dir_path, 4, &.{
        .{ .key = "k", .ts = 500, .val = "v500" },
        .{ .key = "k", .ts = 300, .val = "v300" },
    });
    defer fx.deinit();

    try fx.tree.doCompact(0, &fx.file_ids, 600); // horizon irrelevant: not bottom

    // Both versions survive — a non-bottom compaction never drops.
    const newest = try fx.tree.get(.{ .primary = "k", .timestamp = std.math.maxInt(u64) }, .p0) orelse
        return error.TestExpectedItem;
    defer newest.free(alloc);
    try std.testing.expectEqualStrings("v500", newest.value);
    const mid = try fx.tree.get(.{ .primary = "k", .timestamp = 350 }, .p0) orelse
        return error.TestExpectedItem;
    defer mid.free(alloc);
    try std.testing.expectEqualStrings("v300", mid.value);
    // The shadowed k@300 is counted stale even though nothing was dropped.
    try std.testing.expectEqual(@as(u64, 5), levelStaleBytes(&fx.tree.levels[1]));
}

test "compaction: a delta above the newest state is live, not stale" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // Newest-first: a delta on top of a full state, with an older state beneath
    // it. Only the version BELOW the newest terminal (state@200) is stale — the
    // delta and the state it rebuilds on are both live. This is the case the old
    // "every non-newest version is stale" rule got wrong. Non-bottom keeps the
    // drop path out of it, isolating the stale rule.
    var fx: TombstoneFixture = undefined;
    try setupTombstoneTree(alloc, &fx, dir_path, 4, &.{
        .{ .key = "k", .ts = 300, .val = "d300", .meta = .{ .kind = .delta } },
        .{ .key = "k", .ts = 200, .val = "s200", .meta = .{ .kind = .state } },
        .{ .key = "k", .ts = 100, .val = "s100", .meta = .{ .kind = .state } },
    });
    defer fx.deinit();

    try fx.tree.doCompact(0, &fx.file_ids, 0);

    // All three survive (a non-bottom compaction drops nothing).
    const top = try fx.tree.get(.{ .primary = "k", .timestamp = std.math.maxInt(u64) }, .p0) orelse
        return error.TestExpectedItem;
    defer top.free(alloc);
    try std.testing.expect(top.meta.isDelta());
    const base = try fx.tree.get(.{ .primary = "k", .timestamp = 250 }, .p0) orelse
        return error.TestExpectedItem;
    defer base.free(alloc);
    try std.testing.expectEqualStrings("s200", base.value);

    // Only k@100 sits below the newest terminal (state@200) → stale: "k" (1) +
    // "s100" (4). The delta and its base state are NOT counted.
    try std.testing.expectEqual(@as(u64, 5), levelStaleBytes(&fx.tree.levels[1]));
}

// ---------------------------------------------------------------------------
// Roaring rollup in compaction
// ---------------------------------------------------------------------------

const zroar = zdb.zroar;

const roaring_delta: zdb.kv.Meta = .{ .kind = .delta, .encoding = .roaring };
const roaring_state: zdb.kv.Meta = .{ .kind = .state, .encoding = .roaring };

/// setupTombstoneTree with a caller-supplied Config (dir is filled in
/// here). Rollup tests need to vary the rollup knobs.
fn setupTreeWithCfg(
    alloc: std.mem.Allocator,
    fx: *TombstoneFixture,
    dir_path: []const u8,
    cfg: config.Config,
    entries: []const MetaEntry,
) !void {
    const tree_id: u64 = 1;
    fx.ms = manifest.ForestState.init(alloc);
    errdefer fx.ms.deinit();
    try fx.ms.commit(&.{}, &.{createTreeChange(tree_id, 0, &.{}, &.{})});

    var tb = try buildTableMeta(alloc, entries);
    defer tb.deinit();
    try installTable(alloc, &fx.ms, dir_path, &tb, 1, tree_id, 0);

    var opts = cfg;
    opts.dir = dir_path;
    fx.cache = TableCache.init(alloc, opts.dir);
    errdefer fx.cache.deinit();
    fx.tree = try Tree.open(alloc, opts, 0, tree_id, &fx.ms, &fx.cache);
    fx.file_ids = .{};
    fx.file_ids.bind(1, 100, @ptrCast(&fx.file_ids), noopPersist);
}

fn encodeDelta(
    alloc: std.mem.Allocator,
    adds: []const u64,
    rems: []const u64,
) ![]u8 {
    var list: std.ArrayList(u8) = .empty;
    errdefer list.deinit(alloc);
    var w = try zdb.buffers.RoaringDeltaWriter.initWithBuf(alloc, &list);
    if (adds.len > 0) try w.setAdds(adds);
    if (rems.len > 0) try w.setRemoves(rems);
    _ = try w.finish();
    return list.toOwnedSlice(alloc);
}

// The aligned return type matters: toBufferCopy allocates align(8),
// and freeing through a plain []u8 trips the DebugAllocator's
// alignment bookkeeping.
fn encodeState(
    alloc: std.mem.Allocator,
    vals: []const u64,
) ![]align(8) u8 {
    var bm = try zroar.Bitmap.fromSortedList(alloc, vals);
    defer bm.deinit();
    return bm.toBufferCopy(alloc);
}

fn expectBitmapBytes(
    alloc: std.mem.Allocator,
    bytes: []const u8,
    expected: []const u64,
) !void {
    var bm = try zroar.Bitmap.fromBufferCopy(alloc, bytes);
    defer bm.deinit();
    try std.testing.expectEqual(expected.len, bm.getCardinality());
    for (expected) |v| try std.testing.expect(bm.contains(v));
}

/// The posting visible at `read_ts`, via the real read walk
/// (Tree.rollupBitmap): after a rollup, an old-snapshot reader must
/// land on the retained chain below the too-new synthetic state and
/// fold to the same answer it got before the compaction.
fn foldAt(
    alloc: std.mem.Allocator,
    tree: *Tree,
    primary: []const u8,
    read_ts: u64,
) !zroar.Bitmap {
    const bm = try tree.rollupBitmap(
        alloc,
        .{ .primary = primary, .timestamp = read_ts },
        .p0,
        null,
        0,
    );
    return bm orelse zroar.Bitmap.init(alloc);
}

test "compaction: rollup folds a delta chain onto its in-inputs state" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const d300 = try encodeDelta(alloc, &.{40}, &.{2});
    defer alloc.free(d300);
    const d200 = try encodeDelta(alloc, &.{ 2, 3 }, &.{});
    defer alloc.free(d200);
    const s100 = try encodeState(alloc, &.{ 1, 2 });
    defer alloc.free(s100);

    // Non-bottom (4 levels): the retained chain must survive.
    var fx: TombstoneFixture = undefined;
    try setupTreeWithCfg(alloc, &fx, dir_path, .{ .max_levels = 4 }, &.{
        .{ .key = "k", .ts = 300, .val = d300, .meta = roaring_delta },
        .{ .key = "k", .ts = 200, .val = d200, .meta = roaring_delta },
        .{ .key = "k", .ts = 100, .val = s100, .meta = roaring_state },
    });
    defer fx.deinit();

    try fx.tree.doCompact(0, &fx.file_ids, 0);

    // Newest version is now a synthetic state at the newest delta's
    // timestamp: {1,2} +{2,3} → {1,2,3}, then +{40}-{2} → {1,3,40}.
    const top = try fx.tree.get(
        .{ .primary = "k", .timestamp = std.math.maxInt(u64) },
        .p0,
    ) orelse return error.TestExpectedItem;
    defer top.free(alloc);
    try std.testing.expect(top.meta.isState());
    try std.testing.expect(top.meta.isRoaring());
    try std.testing.expectEqual(@as(u64, 300), top.key.timestamp);
    try expectBitmapBytes(alloc, top.value, &.{ 1, 3, 40 });

    // The replaced delta is gone; the older chain is retained verbatim
    // beneath the synthetic state.
    const mid = try fx.tree.get(.{ .primary = "k", .timestamp = 250 }, .p0) orelse
        return error.TestExpectedItem;
    defer mid.free(alloc);
    try std.testing.expect(mid.meta.isDelta());
    try std.testing.expectEqual(@as(u64, 200), mid.key.timestamp);

    // MVCC: an old-snapshot reader folding the retained chain gets the
    // historical bitmaps, unchanged by the rollup.
    var at250 = try foldAt(alloc, fx.tree, "k", 250);
    defer at250.deinit();
    try std.testing.expectEqual(@as(u64, 3), at250.getCardinality());
    for ([_]u64{ 1, 2, 3 }) |v| try std.testing.expect(at250.contains(v));

    var at150 = try foldAt(alloc, fx.tree, "k", 150);
    defer at150.deinit();
    try std.testing.expectEqual(@as(u64, 2), at150.getCardinality());
}

test "compaction: rollup leaves lone states and baseless chains alone" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const s100 = try encodeState(alloc, &.{ 7, 8 });
    defer alloc.free(s100);
    const d200 = try encodeDelta(alloc, &.{5}, &.{});
    defer alloc.free(d200);

    // "a": already rolled (lone state). "b": delta with no base among
    // the inputs — read-below isn't wired yet, so it stays a delta.
    var fx: TombstoneFixture = undefined;
    try setupTreeWithCfg(alloc, &fx, dir_path, .{ .max_levels = 4 }, &.{
        .{ .key = "a", .ts = 100, .val = s100, .meta = roaring_state },
        .{ .key = "b", .ts = 200, .val = d200, .meta = roaring_delta },
    });
    defer fx.deinit();

    try fx.tree.doCompact(0, &fx.file_ids, 0);

    const a = try fx.tree.get(
        .{ .primary = "a", .timestamp = std.math.maxInt(u64) },
        .p0,
    ) orelse return error.TestExpectedItem;
    defer a.free(alloc);
    try std.testing.expect(a.meta.isState());
    try std.testing.expectEqualSlices(u8, s100, a.value);

    const b = try fx.tree.get(
        .{ .primary = "b", .timestamp = std.math.maxInt(u64) },
        .p0,
    ) orelse return error.TestExpectedItem;
    defer b.free(alloc);
    try std.testing.expect(b.meta.isDelta());
    try std.testing.expectEqualSlices(u8, d200, b.value);
}

test "compaction: a mixed chain aborts the rollup and stays verbatim" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const d300 = try encodeDelta(alloc, &.{5}, &.{});
    defer alloc.free(d300);

    // A raw state under a roaring delta: not ours to fold. Both
    // versions must come out byte-identical.
    var fx: TombstoneFixture = undefined;
    try setupTreeWithCfg(alloc, &fx, dir_path, .{ .max_levels = 4 }, &.{
        .{ .key = "k", .ts = 300, .val = d300, .meta = roaring_delta },
        .{ .key = "k", .ts = 200, .val = "raw-state", .meta = .{} },
    });
    defer fx.deinit();

    try fx.tree.doCompact(0, &fx.file_ids, 0);

    const top = try fx.tree.get(
        .{ .primary = "k", .timestamp = std.math.maxInt(u64) },
        .p0,
    ) orelse return error.TestExpectedItem;
    defer top.free(alloc);
    try std.testing.expect(top.meta.isDelta());
    try std.testing.expectEqualSlices(u8, d300, top.value);

    const under = try fx.tree.get(.{ .primary = "k", .timestamp = 250 }, .p0) orelse
        return error.TestExpectedItem;
    defer under.free(alloc);
    try std.testing.expectEqualStrings("raw-state", under.value);
}

test "compaction: an empty fold emits a roaring tombstone" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const d200 = try encodeDelta(alloc, &.{}, &.{1});
    defer alloc.free(d200);
    const s100 = try encodeState(alloc, &.{1});
    defer alloc.free(s100);

    var fx: TombstoneFixture = undefined;
    try setupTreeWithCfg(alloc, &fx, dir_path, .{ .max_levels = 4 }, &.{
        .{ .key = "k", .ts = 200, .val = d200, .meta = roaring_delta },
        .{ .key = "k", .ts = 100, .val = s100, .meta = roaring_state },
    });
    defer fx.deinit();

    try fx.tree.doCompact(0, &fx.file_ids, 0);

    const top = try fx.tree.get(
        .{ .primary = "k", .timestamp = std.math.maxInt(u64) },
        .p0,
    ) orelse return error.TestExpectedItem;
    defer top.free(alloc);
    try std.testing.expect(top.meta.isDeleted());
    try std.testing.expectEqual(@as(usize, 0), top.value.len);
    try std.testing.expectEqual(@as(u64, 200), top.key.timestamp);

    // Retained history below: the old state is still readable.
    const old = try fx.tree.get(.{ .primary = "k", .timestamp = 150 }, .p0) orelse
        return error.TestExpectedItem;
    defer old.free(alloc);
    try std.testing.expect(old.meta.isState());
}

test "compaction: bottom level collapses a rolled chain below the horizon" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const d200 = try encodeDelta(alloc, &.{5}, &.{});
    defer alloc.free(d200);
    const s100 = try encodeState(alloc, &.{1});
    defer alloc.free(s100);

    // max_levels=2 → dest L1 IS the bottom; horizon above the whole
    // chain lets the synthetic state's discard boundary drop the rest.
    var fx: TombstoneFixture = undefined;
    try setupTreeWithCfg(alloc, &fx, dir_path, .{ .max_levels = 2 }, &.{
        .{ .key = "k", .ts = 200, .val = d200, .meta = roaring_delta },
        .{ .key = "k", .ts = 100, .val = s100, .meta = roaring_state },
    });
    defer fx.deinit();

    try fx.tree.doCompact(0, &fx.file_ids, 300);

    const top = try fx.tree.get(
        .{ .primary = "k", .timestamp = std.math.maxInt(u64) },
        .p0,
    ) orelse return error.TestExpectedItem;
    defer top.free(alloc);
    try std.testing.expect(top.meta.isState());
    try expectBitmapBytes(alloc, top.value, &.{ 1, 5 });

    // The chain beneath the synthetic state settled below the horizon
    // and is gone: nothing older than the state remains.
    try std.testing.expect(
        (try fx.tree.get(.{ .primary = "k", .timestamp = 150 }, .p0)) == null,
    );
}

test "compaction: rollup gates — min_deltas and rollup_enabled" {
    const alloc = std.testing.allocator;

    const d200 = try encodeDelta(alloc, &.{5}, &.{});
    defer alloc.free(d200);
    const s100 = try encodeState(alloc, &.{1});
    defer alloc.free(s100);
    const entries = [_]MetaEntry{
        .{ .key = "k", .ts = 200, .val = d200, .meta = roaring_delta },
        .{ .key = "k", .ts = 100, .val = s100, .meta = roaring_state },
    };

    // min_deltas=2 with a 1-delta chain: not worth the rewrite.
    {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const dir_path = try tmpPath(alloc, &tmp);
        defer alloc.free(dir_path);
        var fx: TombstoneFixture = undefined;
        try setupTreeWithCfg(
            alloc,
            &fx,
            dir_path,
            .{ .max_levels = 4, .rollup_min_deltas = 2 },
            &entries,
        );
        defer fx.deinit();
        try fx.tree.doCompact(0, &fx.file_ids, 0);
        const top = try fx.tree.get(
            .{ .primary = "k", .timestamp = std.math.maxInt(u64) },
            .p0,
        ) orelse return error.TestExpectedItem;
        defer top.free(alloc);
        try std.testing.expect(top.meta.isDelta());
    }

    // rollup_enabled=false: today's behavior, everything verbatim.
    {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const dir_path = try tmpPath(alloc, &tmp);
        defer alloc.free(dir_path);
        var fx: TombstoneFixture = undefined;
        try setupTreeWithCfg(
            alloc,
            &fx,
            dir_path,
            .{ .max_levels = 4, .rollup_enabled = false },
            &entries,
        );
        defer fx.deinit();
        try fx.tree.doCompact(0, &fx.file_ids, 0);
        const top = try fx.tree.get(
            .{ .primary = "k", .timestamp = std.math.maxInt(u64) },
            .p0,
        ) orelse return error.TestExpectedItem;
        defer top.free(alloc);
        try std.testing.expect(top.meta.isDelta());
        try std.testing.expectEqualSlices(u8, d200, top.value);
    }
}

/// One SST per (level, entries) pair, ids assigned 1..n. For the
/// read-below tests, which need history spread across levels.
const LevelTable = struct { level: u32, entries: []const MetaEntry };

fn setupTreeMulti(
    alloc: std.mem.Allocator,
    fx: *TombstoneFixture,
    dir_path: []const u8,
    cfg: config.Config,
    tables: []const LevelTable,
) !void {
    const tree_id: u64 = 1;
    fx.ms = manifest.ForestState.init(alloc);
    errdefer fx.ms.deinit();
    try fx.ms.commit(&.{}, &.{createTreeChange(tree_id, 0, &.{}, &.{})});

    for (tables, 1..) |t, id| {
        var tb = try buildTableMeta(alloc, t.entries);
        defer tb.deinit();
        try installTable(alloc, &fx.ms, dir_path, &tb, id, tree_id, t.level);
    }

    var opts = cfg;
    opts.dir = dir_path;
    fx.cache = TableCache.init(alloc, opts.dir);
    errdefer fx.cache.deinit();
    fx.tree = try Tree.open(alloc, opts, 0, tree_id, &fx.ms, &fx.cache);
    fx.file_ids = .{};
    fx.file_ids.bind(1, 100, @ptrCast(&fx.file_ids), noopPersist);
}

test "compaction: shadowed delta chains are never re-rolled" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const s400 = try encodeState(alloc, &.{ 1, 5 });
    defer alloc.free(s400);
    const d300 = try encodeDelta(alloc, &.{5}, &.{});
    defer alloc.free(d300);
    const s100 = try encodeState(alloc, &.{1});
    defer alloc.free(s100);

    // The shape a chain has right after a rollup: synthetic state on
    // top, retained delta + old base beneath it. A later compaction
    // must NOT regenerate a synthetic state for the shadowed delta —
    // that would repeat the fold + write amp on every compaction
    // until GC collapses the chain.
    var fx: TombstoneFixture = undefined;
    try setupTreeWithCfg(alloc, &fx, dir_path, .{ .max_levels = 4 }, &.{
        .{ .key = "k", .ts = 400, .val = s400, .meta = roaring_state },
        .{ .key = "k", .ts = 300, .val = d300, .meta = roaring_delta },
        .{ .key = "k", .ts = 100, .val = s100, .meta = roaring_state },
    });
    defer fx.deinit();

    try fx.tree.doCompact(0, &fx.file_ids, 0);

    // The shadowed delta came through verbatim, not as a state.
    const mid = try fx.tree.get(.{ .primary = "k", .timestamp = 350 }, .p0) orelse
        return error.TestExpectedItem;
    defer mid.free(alloc);
    try std.testing.expect(mid.meta.isDelta());
    try std.testing.expectEqualSlices(u8, d300, mid.value);
}

test "compaction: read-below rolls a baseless chain onto a deep state" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const d300 = try encodeDelta(alloc, &.{5}, &.{1});
    defer alloc.free(d300);
    const s100 = try encodeState(alloc, &.{ 1, 2 });
    defer alloc.free(s100);

    // Delta at L1, its base two levels down at L3. Compacting L1→L2
    // (dest=2 >= rollup_at_level=2) peeks below and regenerates.
    var fx: TombstoneFixture = undefined;
    try setupTreeMulti(alloc, &fx, dir_path, .{ .max_levels = 4 }, &.{
        .{ .level = 1, .entries = &.{
            .{ .key = "k", .ts = 300, .val = d300, .meta = roaring_delta },
        } },
        .{ .level = 3, .entries = &.{
            .{ .key = "k", .ts = 100, .val = s100, .meta = roaring_state },
        } },
    });
    defer fx.deinit();

    try fx.tree.doCompact(1, &fx.file_ids, 0);

    const top = try fx.tree.get(
        .{ .primary = "k", .timestamp = std.math.maxInt(u64) },
        .p0,
    ) orelse return error.TestExpectedItem;
    defer top.free(alloc);
    try std.testing.expect(top.meta.isState());
    try std.testing.expectEqual(@as(u64, 300), top.key.timestamp);
    try expectBitmapBytes(alloc, top.value, &.{ 2, 5 });
}

test "compaction: max_deltas forces read-below even at L1" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const d300 = try encodeDelta(alloc, &.{5}, &.{});
    defer alloc.free(d300);
    const d200 = try encodeDelta(alloc, &.{3}, &.{});
    defer alloc.free(d200);
    const s100 = try encodeState(alloc, &.{1});
    defer alloc.free(s100);

    // Level trigger disabled (rollup_at_level way above max_levels);
    // the 2-delta chain hits rollup_max_deltas=2 and forces the peek
    // from an L0→L1 compaction.
    var fx: TombstoneFixture = undefined;
    try setupTreeMulti(alloc, &fx, dir_path, .{
        .max_levels = 4,
        .rollup_at_level = 200,
        .rollup_max_deltas = 2,
    }, &.{
        .{ .level = 0, .entries = &.{
            .{ .key = "k", .ts = 300, .val = d300, .meta = roaring_delta },
            .{ .key = "k", .ts = 200, .val = d200, .meta = roaring_delta },
        } },
        .{ .level = 2, .entries = &.{
            .{ .key = "k", .ts = 100, .val = s100, .meta = roaring_state },
        } },
    });
    defer fx.deinit();

    try fx.tree.doCompact(0, &fx.file_ids, 0);

    const top = try fx.tree.get(
        .{ .primary = "k", .timestamp = std.math.maxInt(u64) },
        .p0,
    ) orelse return error.TestExpectedItem;
    defer top.free(alloc);
    try std.testing.expect(top.meta.isState());
    try std.testing.expectEqual(@as(u64, 300), top.key.timestamp);
    try expectBitmapBytes(alloc, top.value, &.{ 1, 3, 5 });
}

test "compaction: read-below with no state anywhere folds from empty" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const d300 = try encodeDelta(alloc, &.{5}, &.{});
    defer alloc.free(d300);
    const d100 = try encodeDelta(alloc, &.{3}, &.{});
    defer alloc.free(d100);

    var fx: TombstoneFixture = undefined;
    try setupTreeMulti(alloc, &fx, dir_path, .{ .max_levels = 4 }, &.{
        .{ .level = 1, .entries = &.{
            .{ .key = "k", .ts = 300, .val = d300, .meta = roaring_delta },
        } },
        .{ .level = 3, .entries = &.{
            .{ .key = "k", .ts = 100, .val = d100, .meta = roaring_delta },
        } },
    });
    defer fx.deinit();

    try fx.tree.doCompact(1, &fx.file_ids, 0);

    const top = try fx.tree.get(
        .{ .primary = "k", .timestamp = std.math.maxInt(u64) },
        .p0,
    ) orelse return error.TestExpectedItem;
    defer top.free(alloc);
    try std.testing.expect(top.meta.isState());
    try expectBitmapBytes(alloc, top.value, &.{ 3, 5 });
}

test "compaction: read-below declined by triggers or mixed chains" {
    const alloc = std.testing.allocator;

    const d300 = try encodeDelta(alloc, &.{5}, &.{});
    defer alloc.free(d300);
    const s100 = try encodeState(alloc, &.{1});
    defer alloc.free(s100);

    // Neither trigger fires: the chain stays a delta.
    {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const dir_path = try tmpPath(alloc, &tmp);
        defer alloc.free(dir_path);
        var fx: TombstoneFixture = undefined;
        try setupTreeMulti(alloc, &fx, dir_path, .{
            .max_levels = 4,
            .rollup_at_level = 200,
            .rollup_max_deltas = 200,
        }, &.{
            .{ .level = 1, .entries = &.{
                .{ .key = "k", .ts = 300, .val = d300, .meta = roaring_delta },
            } },
            .{ .level = 3, .entries = &.{
                .{ .key = "k", .ts = 100, .val = s100, .meta = roaring_state },
            } },
        });
        defer fx.deinit();
        try fx.tree.doCompact(1, &fx.file_ids, 0);
        const top = try fx.tree.get(
            .{ .primary = "k", .timestamp = std.math.maxInt(u64) },
            .p0,
        ) orelse return error.TestExpectedItem;
        defer top.free(alloc);
        try std.testing.expect(top.meta.isDelta());
        try std.testing.expectEqualSlices(u8, d300, top.value);
    }

    // A raw (non-roaring) version below: the peek reports the chain
    // unfoldable and the run stays verbatim.
    {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const dir_path = try tmpPath(alloc, &tmp);
        defer alloc.free(dir_path);
        var fx: TombstoneFixture = undefined;
        try setupTreeMulti(alloc, &fx, dir_path, .{ .max_levels = 4 }, &.{
            .{ .level = 1, .entries = &.{
                .{ .key = "k", .ts = 300, .val = d300, .meta = roaring_delta },
            } },
            .{ .level = 3, .entries = &.{
                .{ .key = "k", .ts = 100, .val = "raw", .meta = .{} },
            } },
        });
        defer fx.deinit();
        try fx.tree.doCompact(1, &fx.file_ids, 0);
        const top = try fx.tree.get(
            .{ .primary = "k", .timestamp = std.math.maxInt(u64) },
            .p0,
        ) orelse return error.TestExpectedItem;
        defer top.free(alloc);
        try std.testing.expect(top.meta.isDelta());
        try std.testing.expectEqualSlices(u8, d300, top.value);
    }
}

test "compaction: rollup folds a chain terminated by a tombstone" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const d300 = try encodeDelta(alloc, &.{9}, &.{});
    defer alloc.free(d300);

    // A raw tombstone terminates a roaring chain (the index key was
    // cleared, then a new delta arrived): fold = {} + {9}.
    var fx: TombstoneFixture = undefined;
    try setupTreeWithCfg(alloc, &fx, dir_path, .{ .max_levels = 4 }, &.{
        .{ .key = "k", .ts = 300, .val = d300, .meta = roaring_delta },
        .{ .key = "k", .ts = 200, .val = "", .meta = .{ .kind = .deleted } },
    });
    defer fx.deinit();

    try fx.tree.doCompact(0, &fx.file_ids, 0);

    const top = try fx.tree.get(
        .{ .primary = "k", .timestamp = std.math.maxInt(u64) },
        .p0,
    ) orelse return error.TestExpectedItem;
    defer top.free(alloc);
    try std.testing.expect(top.meta.isState());
    try expectBitmapBytes(alloc, top.value, &.{9});
}
