const std = @import("std");
const zdb = @import("zdb");

const Builder = zdb.table.builder.Builder;
const Table = zdb.table.Table;
const TableIterator = zdb.table.TableIterator;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a TableBuffer with n entries keyed as "{prefix}{i:0>4}" at ts=0,
/// with value "{i}". block_size controls how many entries land per block.
fn buildTestTable(
    allocator: std.mem.Allocator,
    prefix: []const u8,
    n: usize,
    block_size: u32,
) !zdb.table.builder.TableBuffer {
    var b = Builder.init(allocator, .{ .target_block_size = block_size });
    defer b.deinit();

    var i: usize = 0;
    while (i < n) : (i += 1) {
        var key_buf: [32]u8 = undefined;
        const key_no_ts = try std.fmt.bufPrint(&key_buf, "{s}{d:0>4}", .{ prefix, i });
        var val_buf: [20]u8 = undefined;
        const val = try std.fmt.bufPrint(&val_buf, "{d}", .{i});

        const k = try zdb.key.keyWithTs(allocator, key_no_ts, 0);
        defer allocator.free(k);
        try b.add(k, val);
    }
    return b.finish();
}

// ---------------------------------------------------------------------------
// Existing tests (updated for itemRef / blocksAt)
// ---------------------------------------------------------------------------

test "table builder/reader roundtrip and seek" {
    var b = Builder.init(std.testing.allocator, .{ .target_block_size = 256 });
    defer b.deinit();

    const addKV = struct {
        fn f(builder: *Builder, alloc: std.mem.Allocator, key_no_ts: []const u8, ts: u64, value: []const u8) !void {
            const k = try zdb.key.keyWithTs(alloc, key_no_ts, ts);
            defer alloc.free(k);
            try builder.add(k, value);
        }
    }.f;

    try addKV(&b, std.testing.allocator, "a", 9, "va9");
    try addKV(&b, std.testing.allocator, "a", 7, "va7");
    try addKV(&b, std.testing.allocator, "b", 3, "vb3");
    try addKV(&b, std.testing.allocator, "c", 1, "vc1");

    var tb = try b.finish();
    defer tb.deinit();

    var r = try Table.open(std.testing.allocator, tb.bytes);
    defer r.deinit();

    try std.testing.expectEqual(@as(u32, 4), r.key_count);
    try std.testing.expectEqual(@as(u64, 9), r.max_version);
    try std.testing.expect(r.blockCount() >= 1);

    var it = TableIterator.init(std.testing.allocator, &r);
    defer it.deinit();
    try it.seekToFirst();
    try std.testing.expectEqualStrings("va9", it.itemRef().?.value);

    const seek_key = try zdb.key.keyWithTs(std.testing.allocator, "b", 3);
    defer std.testing.allocator.free(seek_key);
    try it.seek(seek_key);
    try std.testing.expectEqualStrings("vb3", it.itemRef().?.value);
}

test "table checksum mismatch is detected" {
    var b = Builder.init(std.testing.allocator, .{ .target_block_size = 256 });
    defer b.deinit();

    const k1 = try zdb.key.keyWithTs(std.testing.allocator, "k1", 1);
    defer std.testing.allocator.free(k1);
    try b.add(k1, "v1");
    const k2 = try zdb.key.keyWithTs(std.testing.allocator, "k2", 1);
    defer std.testing.allocator.free(k2);
    try b.add(k2, "v2");

    var tb = try b.finish();
    defer tb.deinit();

    var r = try Table.open(std.testing.allocator, tb.bytes);
    defer r.deinit();

    const first_meta = try r.index.blocksAt(0);
    const block_start: usize = first_meta.offset();
    const block_end = block_start + first_meta.len();
    tb.bytes[block_end - 5] ^= 0x01;

    var block_buf = std.ArrayList(u8).empty;
    defer block_buf.deinit(std.testing.allocator);
    try std.testing.expectError(error.ChecksumMismatch, r.readBlock(std.testing.allocator, 0, &block_buf));
}

test "merge iterator merges sorted streams" {
    var b1 = Builder.init(std.testing.allocator, .{ .target_block_size = 256 });
    defer b1.deinit();
    const a5k = try zdb.key.keyWithTs(std.testing.allocator, "a", 5);
    defer std.testing.allocator.free(a5k);
    try b1.add(a5k, "a5");
    const c1k = try zdb.key.keyWithTs(std.testing.allocator, "c", 1);
    defer std.testing.allocator.free(c1k);
    try b1.add(c1k, "c1");
    var t1 = try b1.finish();
    defer t1.deinit();

    var b2 = Builder.init(std.testing.allocator, .{ .target_block_size = 256 });
    defer b2.deinit();
    const b4k = try zdb.key.keyWithTs(std.testing.allocator, "b", 4);
    defer std.testing.allocator.free(b4k);
    try b2.add(b4k, "b4");
    const d1k = try zdb.key.keyWithTs(std.testing.allocator, "d", 1);
    defer std.testing.allocator.free(d1k);
    try b2.add(d1k, "d1");
    var t2 = try b2.finish();
    defer t2.deinit();

    var r1 = try Table.open(std.testing.allocator, t1.bytes);
    defer r1.deinit();
    var r2 = try Table.open(std.testing.allocator, t2.bytes);
    defer r2.deinit();

    var iters = [_]TableIterator{
        TableIterator.init(std.testing.allocator, &r1),
        TableIterator.init(std.testing.allocator, &r2),
    };
    var m = zdb.table.merge_iterator.MergeIterator.init(iters[0..]);
    defer m.deinit();

    try m.seekToFirst();
    try std.testing.expectEqualStrings("a5", m.item().?.value);
    try m.next();
    try std.testing.expectEqualStrings("b4", m.item().?.value);
    try m.next();
    try std.testing.expectEqualStrings("c1", m.item().?.value);
    try m.next();
    try std.testing.expectEqualStrings("d1", m.item().?.value);
    try m.next();
    try std.testing.expect(m.item() == null);
}

// ---------------------------------------------------------------------------
// TestTableIterator: iterate forward over n entries, verify each key+value
// ---------------------------------------------------------------------------

test "table iterator: forward scan" {
    const alloc = std.testing.allocator;
    for ([_]usize{ 99, 100, 101 }) |n| {
        var tb = try buildTestTable(alloc, "key", n, 4096);
        defer tb.deinit();
        var tbl = try Table.open(alloc, tb.bytes);
        defer tbl.deinit();

        var it = TableIterator.init(alloc, &tbl);
        defer it.deinit();

        var count: usize = 0;
        try it.seekToFirst();
        while (it.itemRef()) |entry| : (count += 1) {
            var key_buf: [32]u8 = undefined;
            const expected_key_no_ts = try std.fmt.bufPrint(&key_buf, "key{d:0>4}", .{count});
            try std.testing.expectEqualStrings(expected_key_no_ts, zdb.key.parseKey(entry.key));

            var val_buf: [20]u8 = undefined;
            const expected_val = try std.fmt.bufPrint(&val_buf, "{d}", .{count});
            try std.testing.expectEqualStrings(expected_val, entry.value);

            try it.next();
        }
        try std.testing.expectEqual(n, count);
    }
}

// ---------------------------------------------------------------------------
// TestSeekToFirst: seekToFirst lands on the first entry
// ---------------------------------------------------------------------------

test "seek to first" {
    const alloc = std.testing.allocator;
    for ([_]usize{ 99, 100, 101, 199, 200, 250, 9999, 10000 }) |n| {
        var tb = try buildTestTable(alloc, "key", n, 4096);
        defer tb.deinit();
        var tbl = try Table.open(alloc, tb.bytes);
        defer tbl.deinit();

        var it = TableIterator.init(alloc, &tbl);
        defer it.deinit();

        try it.seekToFirst();
        const entry = it.itemRef() orelse return error.UnexpectedNull;
        try std.testing.expectEqualStrings("0", entry.value);
        try std.testing.expectEqualStrings("key0000", zdb.key.parseKey(entry.key));
    }
}

// ---------------------------------------------------------------------------
// TestSeek: various seek targets on a 10 000-entry table
// ---------------------------------------------------------------------------

test "seek: various targets" {
    const alloc = std.testing.allocator;
    var tb = try buildTestTable(alloc, "k", 10000, 4096);
    defer tb.deinit();
    var tbl = try Table.open(alloc, tb.bytes);
    defer tbl.deinit();

    var it = TableIterator.init(alloc, &tbl);
    defer it.deinit();

    const Case = struct { in: []const u8, valid: bool, out: []const u8 };
    const cases = [_]Case{
        .{ .in = "abc", .valid = true, .out = "k0000" }, // before all keys
        .{ .in = "k0100", .valid = true, .out = "k0100" }, // exact hit
        .{ .in = "k0100b", .valid = true, .out = "k0101" }, // falls between blocks
        .{ .in = "k1234", .valid = true, .out = "k1234" },
        .{ .in = "k1234b", .valid = true, .out = "k1235" },
        .{ .in = "k9999", .valid = true, .out = "k9999" },
        .{ .in = "z", .valid = false, .out = "" }, // past end
    };

    for (cases) |c| {
        const sk = try zdb.key.keyWithTs(alloc, c.in, 0);
        defer alloc.free(sk);
        try it.seek(sk);
        if (!c.valid) {
            try std.testing.expect(it.itemRef() == null);
            continue;
        }
        const entry = it.itemRef() orelse return error.UnexpectedNull;
        try std.testing.expectEqualStrings(c.out, zdb.key.parseKey(entry.key));
    }
}

// ---------------------------------------------------------------------------
// TestIterateFromStart: full forward scan for various n
// ---------------------------------------------------------------------------

test "iterate from start: full scan" {
    const alloc = std.testing.allocator;
    for ([_]usize{ 99, 100, 101, 199, 200, 250, 9999, 10000 }) |n| {
        var tb = try buildTestTable(alloc, "key", n, 4096);
        defer tb.deinit();
        var tbl = try Table.open(alloc, tb.bytes);
        defer tbl.deinit();

        var it = TableIterator.init(alloc, &tbl);
        defer it.deinit();

        var count: usize = 0;
        try it.seekToFirst();
        while (it.itemRef() != null) : (count += 1) {
            try it.next();
        }
        try std.testing.expectEqual(n, count);
    }
}

// ---------------------------------------------------------------------------
// TestTable: seek to a mid-point, iterate to end; seek past end; seek before start
// ---------------------------------------------------------------------------

test "table: seek and iterate to end" {
    const alloc = std.testing.allocator;
    var tb = try buildTestTable(alloc, "key", 10000, 4096);
    defer tb.deinit();
    var tbl = try Table.open(alloc, tb.bytes);
    defer tbl.deinit();

    var it = TableIterator.init(alloc, &tbl);
    defer it.deinit();

    // Seek to key1010, iterate to end — should visit keys 1010..9999.
    const sk1010 = try zdb.key.keyWithTs(alloc, "key1010", 0);
    defer alloc.free(sk1010);
    try it.seek(sk1010);
    var kid: usize = 1010;
    while (it.itemRef()) |entry| : (kid += 1) {
        var buf: [32]u8 = undefined;
        const expected = try std.fmt.bufPrint(&buf, "key{d:0>4}", .{kid});
        try std.testing.expectEqualStrings(expected, zdb.key.parseKey(entry.key));
        try it.next();
    }
    try std.testing.expectEqual(@as(usize, 10000), kid);

    // Seek past end → invalid.
    const sk_end = try zdb.key.keyWithTs(alloc, "key99999", 0);
    defer alloc.free(sk_end);
    try it.seek(sk_end);
    try std.testing.expect(it.itemRef() == null);

    // Seek before start → first key.
    const sk_before = try zdb.key.keyWithTs(alloc, "aaa", 0);
    defer alloc.free(sk_before);
    try it.seek(sk_before);
    const first = it.itemRef() orelse return error.UnexpectedNull;
    try std.testing.expectEqualStrings("key0000", zdb.key.parseKey(first.key));
}

// ---------------------------------------------------------------------------
// TestMaxVersion: max_version is the highest timestamp seen
// ---------------------------------------------------------------------------

test "table: max_version" {
    const alloc = std.testing.allocator;
    var b = Builder.init(alloc, .{ .target_block_size = 4096 });
    defer b.deinit();

    const n = 1000;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        var buf: [32]u8 = undefined;
        const key_str = try std.fmt.bufPrint(&buf, "foo:{d:0>4}", .{i});
        const k = try zdb.key.keyWithTs(alloc, key_str, i + 1);
        defer alloc.free(k);
        try b.add(k, "");
    }

    var tb = try b.finish();
    defer tb.deinit();
    var tbl = try Table.open(alloc, tb.bytes);
    defer tbl.deinit();

    try std.testing.expectEqual(@as(u64, n), tbl.max_version);
}

// ---------------------------------------------------------------------------
// Smallest/biggest: Table.open() fills these from the index
// ---------------------------------------------------------------------------

test "table: smallest and biggest keys" {
    const alloc = std.testing.allocator;
    var tb = try buildTestTable(alloc, "key", 200, 256);
    defer tb.deinit();
    var tbl = try Table.open(alloc, tb.bytes);
    defer tbl.deinit();

    try std.testing.expectEqualStrings("key0000", zdb.key.parseKey(tbl.smallest));
    try std.testing.expectEqualStrings("key0199", zdb.key.parseKey(tbl.biggest));
}

// ---------------------------------------------------------------------------
// TestTableBigValues: values larger than the block size
// ---------------------------------------------------------------------------

test "table: big values" {
    const alloc = std.testing.allocator;
    const n = 20;
    const value_size = 64 * 1024; // 64 KB

    var b = Builder.init(alloc, .{ .target_block_size = 4096 });
    defer b.deinit();

    const big_val = try alloc.alloc(u8, value_size);
    defer alloc.free(big_val);

    var i: usize = 0;
    while (i < n) : (i += 1) {
        var buf: [16]u8 = undefined;
        const key_str = try std.fmt.bufPrint(&buf, "{d:0>4}", .{i});
        const k = try zdb.key.keyWithTs(alloc, key_str, @intCast(i + 1));
        defer alloc.free(k);
        @memset(big_val, @intCast(i % 256));
        try b.add(k, big_val);
    }

    var tb = try b.finish();
    defer tb.deinit();
    var tbl = try Table.open(alloc, tb.bytes);
    defer tbl.deinit();

    try std.testing.expectEqual(@as(u32, n), tbl.key_count);

    var it = TableIterator.init(alloc, &tbl);
    defer it.deinit();
    try it.seekToFirst();

    var count: usize = 0;
    while (it.itemRef()) |entry| : (count += 1) {
        try std.testing.expectEqual(value_size, entry.value.len);
        // Verify fill byte matches the key index.
        try std.testing.expectEqual(@as(u8, @intCast(count % 256)), entry.value[0]);
        try it.next();
    }
    try std.testing.expectEqual(n, count);
}
