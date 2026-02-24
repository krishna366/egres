const std = @import("std");
const zdb = @import("zdb");

const key_mod = zdb.key;
const table_mod = zdb.table;
const Builder = table_mod.builder.Builder;
const Table = table_mod.Table;
const TableIterator = table_mod.TableIterator;

const manifest = zdb.manifest;
const Change = manifest.Change;

const level_handler = zdb.level_handler;
const LevelHandler = level_handler.LevelHandler;
const TableEntry = level_handler.TableEntry;
const KeyRange = level_handler.KeyRange;

const levels = zdb.levels;
const LevelsController = levels.LevelsController;

const options_mod = zdb.options;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns the absolute path of a TmpDir (caller frees).
fn tmpPath(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir) ![]u8 {
    return tmp.dir.realpathAlloc(alloc, ".");
}

/// Builds a TableBuffer containing the given key/value pairs.
/// Keys must be in sorted order (caller's responsibility).
/// Each entry is (key_no_ts, ts, value).
fn buildTable(
    alloc: std.mem.Allocator,
    entries: []const struct { key: []const u8, ts: u64, val: []const u8 },
) !table_mod.builder.TableBuffer {
    var b = Builder.init(alloc, .{ .target_block_size = 4 * 1024 });
    defer b.deinit();
    for (entries) |e| {
        const k = try key_mod.withTs(alloc, e.key, e.ts);
        defer alloc.free(k);
        try b.add(k, e.val);
    }
    return b.finish();
}

/// Opens a heap-allocated Table from a TableBuffer. Duplicates tb.bytes
/// internally; caller retains ownership of tb.
fn openTableOwned(alloc: std.mem.Allocator, tb: *table_mod.builder.TableBuffer) !*Table {
    return Table.openMem(alloc, tb.bytes);
}

fn freeTable(_: std.mem.Allocator, t: *Table) void {
    t.decrRef();
}

// ---------------------------------------------------------------------------
// 1. level_handler L0 sort
// ---------------------------------------------------------------------------

test "level_handler L0 sort" {
    const alloc = std.testing.allocator;

    var tb3 = try buildTable(alloc, &.{.{ .key = "c", .ts = 0, .val = "v" }});
    defer tb3.deinit();
    var tb1 = try buildTable(alloc, &.{.{ .key = "a", .ts = 0, .val = "v" }});
    defer tb1.deinit();
    var tb2 = try buildTable(alloc, &.{.{ .key = "b", .ts = 0, .val = "v" }});
    defer tb2.deinit();

    const t3 = try openTableOwned(alloc, &tb3);
    defer freeTable(alloc, t3);
    const t1 = try openTableOwned(alloc, &tb1);
    defer freeTable(alloc, t1);
    const t2 = try openTableOwned(alloc, &tb2);
    defer freeTable(alloc, t2);

    var lh = LevelHandler.init(alloc, 0);
    defer lh.deinit();

    try lh.addEntry(3, t3);
    try lh.addEntry(1, t1);
    try lh.addEntry(2, t2);

    // L0 sorts ascending by ID.
    try std.testing.expectEqual(@as(u64, 1), lh.entries.items[0].id);
    try std.testing.expectEqual(@as(u64, 2), lh.entries.items[1].id);
    try std.testing.expectEqual(@as(u64, 3), lh.entries.items[2].id);
}

// ---------------------------------------------------------------------------
// 2. level_handler L1 sort
// ---------------------------------------------------------------------------

test "level_handler L1 sort" {
    const alloc = std.testing.allocator;

    // Three tables at L1 with non-overlapping key ranges (added out of order).
    var tbc = try buildTable(alloc, &.{.{ .key = "c", .ts = 0, .val = "vc" }});
    defer tbc.deinit();
    var tba = try buildTable(alloc, &.{.{ .key = "a", .ts = 0, .val = "va" }});
    defer tba.deinit();
    var tbb = try buildTable(alloc, &.{.{ .key = "b", .ts = 0, .val = "vb" }});
    defer tbb.deinit();

    const tc = try openTableOwned(alloc, &tbc);
    defer freeTable(alloc, tc);
    const ta = try openTableOwned(alloc, &tba);
    defer freeTable(alloc, ta);
    const tb = try openTableOwned(alloc, &tbb);
    defer freeTable(alloc, tb);

    var lh = LevelHandler.init(alloc, 1);
    defer lh.deinit();

    try lh.addEntry(3, tc); // "c"
    try lh.addEntry(1, ta); // "a"
    try lh.addEntry(2, tb); // "b"

    // L1 sorts by smallest key ascending.
    const sk0 = key_mod.parseKey(lh.entries.items[0].table.smallest);
    const sk1 = key_mod.parseKey(lh.entries.items[1].table.smallest);
    const sk2 = key_mod.parseKey(lh.entries.items[2].table.smallest);
    try std.testing.expectEqualStrings("a", sk0);
    try std.testing.expectEqualStrings("b", sk1);
    try std.testing.expectEqualStrings("c", sk2);
}

// ---------------------------------------------------------------------------
// 3. overlapping tables L1
// ---------------------------------------------------------------------------

test "overlapping tables L1" {
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
    var tbs: [5]table_mod.builder.TableBuffer = undefined;
    for (ranges, 0..) |r, i| {
        tbs[i] = try buildTable(alloc, &.{
            .{ .key = r[0], .ts = 0, .val = "v" },
            .{ .key = r[1], .ts = 0, .val = "v" },
        });
        tables[i] = try openTableOwned(alloc, &tbs[i]);
    }
    defer for (&tbs) |*tb| tb.deinit();
    defer for (tables, 0..) |t, i| {
        freeTable(alloc, t);
        _ = i;
    };

    var lh = LevelHandler.init(alloc, 1);
    defer lh.deinit();
    for (tables, 0..) |t, i| {
        try lh.addEntry(@intCast(i + 1), t);
    }

    // Query range c-g should overlap tables c-d, e-f, g-h → indices 1..4.
    const ck = try key_mod.withTs(alloc, "c", 0);
    defer alloc.free(ck);
    const gk = try key_mod.withTs(alloc, "g", 0);
    defer alloc.free(gk);

    const result = try lh.overlappingTables(alloc, .{ .left = ck, .right = gk });
    defer alloc.free(result);
    try std.testing.expectEqual(@as(usize, 3), result.len);
}

// ---------------------------------------------------------------------------
// 4. overlapping tables L0
// ---------------------------------------------------------------------------

test "overlapping tables L0" {
    const alloc = std.testing.allocator;

    var tb1 = try buildTable(alloc, &.{.{ .key = "a", .ts = 0, .val = "v" }});
    defer tb1.deinit();
    var tb2 = try buildTable(alloc, &.{.{ .key = "b", .ts = 0, .val = "v" }});
    defer tb2.deinit();
    var tb3 = try buildTable(alloc, &.{.{ .key = "c", .ts = 0, .val = "v" }});
    defer tb3.deinit();

    const t1 = try openTableOwned(alloc, &tb1);
    defer freeTable(alloc, t1);
    const t2 = try openTableOwned(alloc, &tb2);
    defer freeTable(alloc, t2);
    const t3 = try openTableOwned(alloc, &tb3);
    defer freeTable(alloc, t3);

    var lh = LevelHandler.init(alloc, 0);
    defer lh.deinit();
    try lh.addEntry(1, t1);
    try lh.addEntry(2, t2);
    try lh.addEntry(3, t3);

    const ck = try key_mod.withTs(alloc, "x", 0);
    defer alloc.free(ck);

    // L0 always returns all tables regardless of the query.
    const result = try lh.overlappingTables(alloc, .{ .left = ck, .right = ck });
    defer alloc.free(result);
    try std.testing.expectEqual(@as(usize, 3), result.len);
}

// ---------------------------------------------------------------------------
// 5. get L0 newest wins
// ---------------------------------------------------------------------------

test "get L0 newest wins" {
    const alloc = std.testing.allocator;

    // Table ID=1: key "k" at ts=3 → "old"
    // Table ID=2: key "k" at ts=5 → "new"
    var tb1 = try buildTable(alloc, &.{.{ .key = "k", .ts = 3, .val = "old" }});
    defer tb1.deinit();
    var tb2 = try buildTable(alloc, &.{.{ .key = "k", .ts = 5, .val = "new" }});
    defer tb2.deinit();

    const t1 = try openTableOwned(alloc, &tb1);
    defer freeTable(alloc, t1);
    const t2 = try openTableOwned(alloc, &tb2);
    defer freeTable(alloc, t2);

    var lh = LevelHandler.init(alloc, 0);
    defer lh.deinit();
    try lh.addEntry(1, t1);
    try lh.addEntry(2, t2);

    // Search with max ts to find the newest version.
    const search = try key_mod.withTs(alloc, "k", std.math.maxInt(u64));
    defer alloc.free(search);

    const item = try lh.get(search);
    try std.testing.expect(item != null);
    defer {
        alloc.free(item.?.key);
        alloc.free(item.?.value);
    }
    try std.testing.expectEqualStrings("new", item.?.value);
}

// ---------------------------------------------------------------------------
// 6. get L1 hit
// ---------------------------------------------------------------------------

test "get L1 hit" {
    const alloc = std.testing.allocator;

    var tba = try buildTable(alloc, &.{.{ .key = "a", .ts = 0, .val = "va" }});
    defer tba.deinit();
    var tbb = try buildTable(alloc, &.{.{ .key = "b", .ts = 0, .val = "vb" }});
    defer tbb.deinit();
    var tbc = try buildTable(alloc, &.{.{ .key = "c", .ts = 0, .val = "vc" }});
    defer tbc.deinit();

    const ta = try openTableOwned(alloc, &tba);
    defer freeTable(alloc, ta);
    const tb = try openTableOwned(alloc, &tbb);
    defer freeTable(alloc, tb);
    const tc = try openTableOwned(alloc, &tbc);
    defer freeTable(alloc, tc);

    var lh = LevelHandler.init(alloc, 1);
    defer lh.deinit();
    try lh.addEntry(1, ta);
    try lh.addEntry(2, tb);
    try lh.addEntry(3, tc);

    const search = try key_mod.withTs(alloc, "b", 0);
    defer alloc.free(search);

    const item = try lh.get(search);
    try std.testing.expect(item != null);
    defer {
        alloc.free(item.?.key);
        alloc.free(item.?.value);
    }
    try std.testing.expectEqualStrings("vb", item.?.value);
}

// ---------------------------------------------------------------------------
// 7. get miss
// ---------------------------------------------------------------------------

test "get miss" {
    const alloc = std.testing.allocator;

    var tba = try buildTable(alloc, &.{.{ .key = "a", .ts = 0, .val = "va" }});
    defer tba.deinit();
    const ta = try openTableOwned(alloc, &tba);
    defer freeTable(alloc, ta);

    var lh = LevelHandler.init(alloc, 1);
    defer lh.deinit();
    try lh.addEntry(1, ta);

    const search = try key_mod.withTs(alloc, "z", 0);
    defer alloc.free(search);

    const item = try lh.get(search);
    try std.testing.expect(item == null);
}

// ---------------------------------------------------------------------------
// 8. levels open
// ---------------------------------------------------------------------------

test "levels open" {
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

        const p1 = try std.fmt.allocPrint(alloc, "{s}/000001.sst", .{dir_path});
        defer alloc.free(p1);
        const p2 = try std.fmt.allocPrint(alloc, "{s}/000002.sst", .{dir_path});
        defer alloc.free(p2);

        const f1 = try std.fs.createFileAbsolute(p1, .{});
        defer f1.close();
        try f1.writeAll(tb1.bytes);

        const f2 = try std.fs.createFileAbsolute(p2, .{});
        defer f2.close();
        try f2.writeAll(tb2.bytes);
    }

    // Create manifest with table 1 at L0, table 2 at L1.
    var ms = try manifest.openOrCreate(alloc, dir_path);
    defer ms.close();
    try ms.commit(&.{
        Change.create(1, 0),
        Change.create(2, 1),
    });

    const opts = options_mod.Options{ .dir = dir_path };
    var lc = try LevelsController.open(alloc, opts, &ms);
    defer lc.close();

    try std.testing.expectEqual(@as(usize, 1), lc.handlers[0].numTables());
    try std.testing.expectEqual(@as(usize, 1), lc.handlers[1].numTables());
    // All other levels empty.
    for (lc.handlers[2..]) |*h| {
        try std.testing.expectEqual(@as(usize, 0), h.numTables());
    }
}

// ---------------------------------------------------------------------------
// 9. pick compact level
// ---------------------------------------------------------------------------

test "pick compact level" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // Create an empty manifest (no SST files needed for this test).
    var ms = try manifest.openOrCreate(alloc, dir_path);
    defer ms.close();

    // Use a very small base_level_size so one table in L1 exceeds the target.
    const opts = options_mod.Options{
        .dir = dir_path,
        .base_level_size = 1, // 1 byte → any data exceeds it
        .num_level_zero_tables = 5,
    };

    var lc = try LevelsController.open(alloc, opts, &ms);
    defer lc.close();

    // Manually plant a large table in L1.
    var tb = try buildTable(alloc, &.{
        .{ .key = "a", .ts = 0, .val = "va" },
        .{ .key = "b", .ts = 0, .val = "vb" },
    });
    defer tb.deinit();
    const t = try openTableOwned(alloc, &tb);
    defer freeTable(alloc, t);
    try lc.handlers[1].addEntry(999, t);

    const level = try lc.pickCompactLevel();
    try std.testing.expect(level != null);
    try std.testing.expectEqual(@as(u8, 1), level.?);
}

// ---------------------------------------------------------------------------
// 10. do compact L0→L1
// ---------------------------------------------------------------------------

test "do compact L0->L1" {
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

    var ms = try manifest.openOrCreate(alloc, dir_path);
    defer ms.close();

    for (l0_data) |d| {
        var tb = try buildTable(alloc, &.{.{ .key = d.key, .ts = 0, .val = d.val }});
        defer tb.deinit();
        const path = try std.fmt.allocPrint(alloc, "{s}/{d:0>6}.sst", .{ dir_path, d.id });
        defer alloc.free(path);
        const f = try std.fs.createFileAbsolute(path, .{});
        defer f.close();
        try f.writeAll(tb.bytes);
    }
    try ms.commit(&.{
        Change.create(1, 0),
        Change.create(2, 0),
        Change.create(3, 0),
    });

    // Use max_levels=2 so base_level=1 and doCompact(0) goes L0→L1.
    const opts = options_mod.Options{ .dir = dir_path, .max_levels = 2 };
    var lc = try LevelsController.open(alloc, opts, &ms);
    defer lc.close();

    // All three tables should be in L0 before compaction.
    try std.testing.expectEqual(@as(usize, 3), lc.handlers[0].numTables());
    try std.testing.expectEqual(@as(usize, 0), lc.handlers[1].numTables());

    try lc.doCompact(0);

    // After compaction L0 must be empty and keys must live in L1.
    try std.testing.expectEqual(@as(usize, 0), lc.handlers[0].numTables());
    try std.testing.expect(lc.handlers[1].numTables() > 0);

    // All three keys must be retrievable via get.
    for (l0_data) |d| {
        const sk = try key_mod.withTs(alloc, d.key, std.math.maxInt(u64));
        defer alloc.free(sk);
        const item = try lc.get(sk);
        try std.testing.expect(item != null);
        defer {
            alloc.free(item.?.key);
            alloc.free(item.?.value);
        }
        try std.testing.expectEqualStrings(d.val, item.?.value);
    }
}

// ---------------------------------------------------------------------------
// 11. do compact L0→L1 with dest overlap
// ---------------------------------------------------------------------------

test "do compact L0->L1 with dest overlap" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    var ms = try manifest.openOrCreate(alloc, dir_path);
    defer ms.close();

    // L0 table 1: {aa→vaa}
    {
        var tb = try buildTable(alloc, &.{.{ .key = "aa", .ts = 0, .val = "vaa" }});
        defer tb.deinit();
        const p = try std.fmt.allocPrint(alloc, "{s}/000001.sst", .{dir_path});
        defer alloc.free(p);
        const f = try std.fs.createFileAbsolute(p, .{});
        defer f.close();
        try f.writeAll(tb.bytes);
    }
    // L0 table 2: {cc→vcc}
    {
        var tb = try buildTable(alloc, &.{.{ .key = "cc", .ts = 0, .val = "vcc" }});
        defer tb.deinit();
        const p = try std.fmt.allocPrint(alloc, "{s}/000002.sst", .{dir_path});
        defer alloc.free(p);
        const f = try std.fs.createFileAbsolute(p, .{});
        defer f.close();
        try f.writeAll(tb.bytes);
    }
    // L1 table 3: {bb→vbb, dd→vdd} — key range [bb..dd] overlaps [aa..cc].
    {
        var tb = try buildTable(alloc, &.{
            .{ .key = "bb", .ts = 0, .val = "vbb" },
            .{ .key = "dd", .ts = 0, .val = "vdd" },
        });
        defer tb.deinit();
        const p = try std.fmt.allocPrint(alloc, "{s}/000003.sst", .{dir_path});
        defer alloc.free(p);
        const f = try std.fs.createFileAbsolute(p, .{});
        defer f.close();
        try f.writeAll(tb.bytes);
    }
    try ms.commit(&.{
        Change.create(1, 0),
        Change.create(2, 0),
        Change.create(3, 1),
    });

    // max_levels=2: L0 compacts directly to L1 (base_level=1).
    const opts = options_mod.Options{ .dir = dir_path, .max_levels = 2 };
    var lc = try LevelsController.open(alloc, opts, &ms);
    defer lc.close();

    try std.testing.expectEqual(@as(usize, 2), lc.handlers[0].numTables());
    try std.testing.expectEqual(@as(usize, 1), lc.handlers[1].numTables());

    try lc.doCompact(0);

    // L0 must be empty; L1 holds the merged result in a single table.
    try std.testing.expectEqual(@as(usize, 0), lc.handlers[0].numTables());
    try std.testing.expectEqual(@as(usize, 1), lc.handlers[1].numTables());

    // All four keys from both levels must be present after compaction.
    const expected = [_]struct { key: []const u8, val: []const u8 }{
        .{ .key = "aa", .val = "vaa" },
        .{ .key = "bb", .val = "vbb" },
        .{ .key = "cc", .val = "vcc" },
        .{ .key = "dd", .val = "vdd" },
    };
    for (expected) |e| {
        const sk = try key_mod.withTs(alloc, e.key, std.math.maxInt(u64));
        defer alloc.free(sk);
        const item = try lc.get(sk);
        try std.testing.expect(item != null);
        defer {
            alloc.free(item.?.key);
            alloc.free(item.?.value);
        }
        try std.testing.expectEqualStrings(e.val, item.?.value);
    }
}

// ---------------------------------------------------------------------------
// 12. do compact L1→L2
// ---------------------------------------------------------------------------

test "do compact L1->L2" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // L1: {aa, bb}; L2: {cc} — non-overlapping, so compacting L1 doesn't
    // touch the L2 table; both survive in L2 after compaction.
    var ms = try manifest.openOrCreate(alloc, dir_path);
    defer ms.close();

    {
        var tb1 = try buildTable(alloc, &.{
            .{ .key = "aa", .ts = 0, .val = "vaa" },
            .{ .key = "bb", .ts = 0, .val = "vbb" },
        });
        defer tb1.deinit();
        const p1 = try std.fmt.allocPrint(alloc, "{s}/000001.sst", .{dir_path});
        defer alloc.free(p1);
        const f1 = try std.fs.createFileAbsolute(p1, .{});
        defer f1.close();
        try f1.writeAll(tb1.bytes);

        var tb2 = try buildTable(alloc, &.{.{ .key = "cc", .ts = 0, .val = "vcc" }});
        defer tb2.deinit();
        const p2 = try std.fmt.allocPrint(alloc, "{s}/000002.sst", .{dir_path});
        defer alloc.free(p2);
        const f2 = try std.fs.createFileAbsolute(p2, .{});
        defer f2.close();
        try f2.writeAll(tb2.bytes);
    }
    try ms.commit(&.{ Change.create(1, 1), Change.create(2, 2) });

    // max_levels=3: doCompact(1) goes L1→L2 (dest = src + 1 = 2).
    const opts = options_mod.Options{ .dir = dir_path, .max_levels = 3 };
    var lc = try LevelsController.open(alloc, opts, &ms);
    defer lc.close();

    try std.testing.expectEqual(@as(usize, 1), lc.handlers[1].numTables());
    try std.testing.expectEqual(@as(usize, 1), lc.handlers[2].numTables());

    try lc.doCompact(1);

    // L1 must be empty; L2 holds the original {cc} table plus new {aa, bb}.
    try std.testing.expectEqual(@as(usize, 0), lc.handlers[1].numTables());
    try std.testing.expectEqual(@as(usize, 2), lc.handlers[2].numTables());

    const expected = [_]struct { key: []const u8, val: []const u8 }{
        .{ .key = "aa", .val = "vaa" },
        .{ .key = "bb", .val = "vbb" },
        .{ .key = "cc", .val = "vcc" },
    };
    for (expected) |e| {
        const sk = try key_mod.withTs(alloc, e.key, std.math.maxInt(u64));
        defer alloc.free(sk);
        const item = try lc.get(sk);
        try std.testing.expect(item != null);
        defer {
            alloc.free(item.?.key);
            alloc.free(item.?.value);
        }
        try std.testing.expectEqualStrings(e.val, item.?.value);
    }
}

// ---------------------------------------------------------------------------
// 13. levels get: L0 shadows L1
// ---------------------------------------------------------------------------

test "levels get: L0 shadows L1" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    var ms = try manifest.openOrCreate(alloc, dir_path);
    defer ms.close();

    const opts = options_mod.Options{ .dir = dir_path };
    var lc = try LevelsController.open(alloc, opts, &ms);
    defer lc.close();

    // Plant in-memory tables directly into the level handlers.
    // L1: foo at ts=1 (old version).
    var tb_l1 = try buildTable(alloc, &.{.{ .key = "foo", .ts = 1, .val = "l1_val" }});
    defer tb_l1.deinit();
    const t_l1 = try openTableOwned(alloc, &tb_l1);
    defer freeTable(alloc, t_l1);
    try lc.handlers[1].addEntry(1, t_l1);

    // L0: foo at ts=5 (newer version; higher ts sorts earlier in byte order).
    var tb_l0 = try buildTable(alloc, &.{.{ .key = "foo", .ts = 5, .val = "l0_val" }});
    defer tb_l0.deinit();
    const t_l0 = try openTableOwned(alloc, &tb_l0);
    defer freeTable(alloc, t_l0);
    try lc.handlers[0].addEntry(2, t_l0);

    // Searching with maxInt ts seeks to the very beginning of "foo" entries;
    // L0 is checked first → the ts=5 version wins.
    {
        const sk = try key_mod.withTs(alloc, "foo", std.math.maxInt(u64));
        defer alloc.free(sk);
        const item = try lc.get(sk);
        try std.testing.expect(item != null);
        defer {
            alloc.free(item.?.key);
            alloc.free(item.?.value);
        }
        try std.testing.expectEqualStrings("l0_val", item.?.value);
    }

    // Searching with ts=3: L0's ts=5 entry sorts *before* ts=3 in byte order
    // (higher ts ↔ smaller suffix), so the L0 seek lands past it → L0 miss.
    // L1's ts=1 entry sorts *after* ts=3, so the L1 seek lands on it → hit.
    {
        const sk = try key_mod.withTs(alloc, "foo", 3);
        defer alloc.free(sk);
        const item = try lc.get(sk);
        try std.testing.expect(item != null);
        defer {
            alloc.free(item.?.key);
            alloc.free(item.?.value);
        }
        try std.testing.expectEqualStrings("l1_val", item.?.value);
    }
}
