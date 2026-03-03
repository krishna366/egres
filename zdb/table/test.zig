const std = @import("std");
const zdb = @import("zdb");
const vfs = zdb.vfs;

const Builder = zdb.table.builder.Builder;
const Table = zdb.table.Table;
const TableIterator = zdb.table.TableIterator;

/// Test helper: create an in-memory FileHandle from a buffer (dupes the data).
fn fromBuf(allocator: std.mem.Allocator, buf: []const u8) !vfs.FileHandle {
    const owned = try allocator.dupe(u8, buf);
    return .{
        .file_size = @intCast(owned.len),
        .owned_buf = owned,
        .buf_allocator = allocator,
        .vfs = &vfs.instance,
    };
}

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

        const k = try zdb.key.withTs(allocator, key_no_ts, 0);
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
            const k = try zdb.key.withTs(alloc, key_no_ts, ts);
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

    const r = try Table.openHandle(std.testing.allocator, try fromBuf(std.testing.allocator, tb.bytes), .p0);
    defer r.decrRef();

    try std.testing.expectEqual(@as(u32, 4), r.index.key_count);
    try std.testing.expectEqual(@as(u64, 9), r.index.max_version);
    try std.testing.expect(r.blockCount() >= 1);

    var it = TableIterator.init(std.testing.allocator, r, .p0);
    defer it.deinit();
    try it.seekToFirst();
    try std.testing.expectEqualStrings("va9", it.itemRef().?.value);

    const seek_key = try zdb.key.withTs(std.testing.allocator, "b", 3);
    defer std.testing.allocator.free(seek_key);
    try it.seek(seek_key);
    try std.testing.expectEqualStrings("vb3", it.itemRef().?.value);
}

test "table binary fuse contains keys" {
    const alloc = std.testing.allocator;

    var b = Builder.init(alloc, .{ .target_block_size = 4 * 1024 });
    defer b.deinit();

    const entries = [_]struct { key: []const u8, ts: u64, val: []const u8 }{
        .{ .key = "apple", .ts = 1, .val = "v1" },
        .{ .key = "banana", .ts = 2, .val = "v2" },
        .{ .key = "carrot", .ts = 3, .val = "v3" },
    };

    for (entries) |e| {
        const k = try zdb.key.withTs(alloc, e.key, e.ts);
        defer alloc.free(k);
        try b.add(k, e.val);
    }

    var tb = try b.finish();
    defer tb.deinit();

    const t = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
    defer t.decrRef();

    try std.testing.expect(t.containsKey("apple"));
    try std.testing.expect(t.containsKey("banana"));
    try std.testing.expect(t.containsKey("carrot"));

    try std.testing.expect(!t.containsKey("date"));
    try std.testing.expect(!t.containsKey("eggplant"));
    try std.testing.expect(!t.containsKey("fig"));
}

test "table binary fuse large keyset contains sampled keys" {
    const alloc = std.testing.allocator;
    const n: usize = 1_000_000;
    const sample: usize = 10_000;

    var b = Builder.init(alloc, .{ .target_block_size = 32 * 1024 });
    defer b.deinit();

    var i: usize = 0;
    while (i < n) : (i += 1) {
        var key_buf: [32]u8 = undefined;
        const key_no_ts = try std.fmt.bufPrint(&key_buf, "k{d:0>6}", .{i});
        try zdb.key.setTs(key_buf[key_no_ts.len..], 0);

        const k = key_buf[0 .. key_no_ts.len + 8];
        try b.add(k, "v");
    }

    var tb = try b.finish();
    defer tb.deinit();

    const t = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
    defer t.decrRef();

    const seed: u64 = @intCast(std.time.nanoTimestamp());
    var prng = std.Random.DefaultPrng.init(seed);
    const rnd = prng.random();

    i = 0;
    while (i < sample) : (i += 1) {
        const idx = rnd.uintAtMost(u64, n - 1);
        var key_buf: [16]u8 = undefined;
        const key_no_ts = try std.fmt.bufPrint(&key_buf, "k{d:0>6}", .{idx});
        try std.testing.expect(t.containsKey(key_no_ts));
    }

    var non_added_matches: usize = 0;
    i = 0;
    while (i < sample) : (i += 1) {
        const idx = @as(u64, n) + rnd.uintAtMost(u64, n - 1);
        var key_buf: [16]u8 = undefined;
        const key_no_ts = try std.fmt.bufPrint(&key_buf, "k{d:0>6}", .{idx});
        if (t.containsKey(key_no_ts)) {
            non_added_matches += 1;
        }
    }
    // Some false positives are possible.
    try std.testing.expect(non_added_matches < 10);
}

test "table checksum mismatch is detected" {
    var b = Builder.init(std.testing.allocator, .{ .target_block_size = 256 });
    defer b.deinit();

    const k1 = try zdb.key.withTs(std.testing.allocator, "k1", 1);
    defer std.testing.allocator.free(k1);
    try b.add(k1, "v1");
    const k2 = try zdb.key.withTs(std.testing.allocator, "k2", 1);
    defer std.testing.allocator.free(k2);
    try b.add(k2, "v2");

    var tb = try b.finish();
    defer tb.deinit();

    const r = try Table.openHandle(std.testing.allocator, try fromBuf(std.testing.allocator, tb.bytes), .p0);
    defer r.decrRef();

    const first_meta = r.index.blocks[0];
    const block_start: usize = first_meta.offset;
    const block_end = block_start + first_meta.len;
    r.handle.owned_buf.?[block_end - 5] ^= 0x01;

    try std.testing.expectError(error.ChecksumMismatch, r.readBlock(0, .p0));
}

test "merge iterator merges sorted streams" {
    var b1 = Builder.init(std.testing.allocator, .{ .target_block_size = 256 });
    defer b1.deinit();
    const a5k = try zdb.key.withTs(std.testing.allocator, "a", 5);
    defer std.testing.allocator.free(a5k);
    try b1.add(a5k, "a5");
    const c1k = try zdb.key.withTs(std.testing.allocator, "c", 1);
    defer std.testing.allocator.free(c1k);
    try b1.add(c1k, "c1");
    var t1 = try b1.finish();
    defer t1.deinit();

    var b2 = Builder.init(std.testing.allocator, .{ .target_block_size = 256 });
    defer b2.deinit();
    const b4k = try zdb.key.withTs(std.testing.allocator, "b", 4);
    defer std.testing.allocator.free(b4k);
    try b2.add(b4k, "b4");
    const d1k = try zdb.key.withTs(std.testing.allocator, "d", 1);
    defer std.testing.allocator.free(d1k);
    try b2.add(d1k, "d1");
    var t2 = try b2.finish();
    defer t2.deinit();

    const r1 = try Table.openHandle(std.testing.allocator, try fromBuf(std.testing.allocator, t1.bytes), .p0);
    defer r1.decrRef();
    const r2 = try Table.openHandle(std.testing.allocator, try fromBuf(std.testing.allocator, t2.bytes), .p0);
    defer r2.decrRef();

    var iters = [_]zdb.table.merge_iterator.Iterator{
        .{ .table = TableIterator.init(std.testing.allocator, r1, .p0) },
        .{ .table = TableIterator.init(std.testing.allocator, r2, .p0) },
    };
    var m = zdb.table.merge_iterator.MergeIterator.init(std.testing.allocator, iters[0..]);
    defer m.deinit();

    try m.seekToFirst();
    try std.testing.expectEqualStrings("a5", m.itemRef().?.value);
    try m.next();
    try std.testing.expectEqualStrings("b4", m.itemRef().?.value);
    try m.next();
    try std.testing.expectEqualStrings("c1", m.itemRef().?.value);
    try m.next();
    try std.testing.expectEqualStrings("d1", m.itemRef().?.value);
    try m.next();
    try std.testing.expect(m.itemRef() == null);
}

// ---------------------------------------------------------------------------
// TestTableIterator: iterate forward over n entries, verify each key+value
// ---------------------------------------------------------------------------

test "table iterator: forward scan" {
    const alloc = std.testing.allocator;
    for ([_]usize{ 99, 100, 101 }) |n| {
        var tb = try buildTestTable(alloc, "key", n, 4096);
        defer tb.deinit();
        const tbl = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
        defer tbl.decrRef();

        var it = TableIterator.init(alloc, tbl, .p0);
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
        const tbl = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
        defer tbl.decrRef();

        var it = TableIterator.init(alloc, tbl, .p0);
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
    const tbl = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
    defer tbl.decrRef();

    var it = TableIterator.init(alloc, tbl, .p0);
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
        const sk = try zdb.key.withTs(alloc, c.in, 0);
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
        const tbl = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
        defer tbl.decrRef();

        var it = TableIterator.init(alloc, tbl, .p0);
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
    const tbl = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
    defer tbl.decrRef();

    var it = TableIterator.init(alloc, tbl, .p0);
    defer it.deinit();

    // Seek to key1010, iterate to end — should visit keys 1010..9999.
    const sk1010 = try zdb.key.withTs(alloc, "key1010", 0);
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
    const sk_end = try zdb.key.withTs(alloc, "key99999", 0);
    defer alloc.free(sk_end);
    try it.seek(sk_end);
    try std.testing.expect(it.itemRef() == null);

    // Seek before start → first key.
    const sk_before = try zdb.key.withTs(alloc, "aaa", 0);
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
        const k = try zdb.key.withTs(alloc, key_str, i + 1);
        defer alloc.free(k);
        try b.add(k, "");
    }

    var tb = try b.finish();
    defer tb.deinit();
    const tbl = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
    defer tbl.decrRef();

    try std.testing.expectEqual(@as(u64, n), tbl.index.max_version);
}

// ---------------------------------------------------------------------------
// Smallest/biggest: Table open fills these from the index
// ---------------------------------------------------------------------------

test "table: smallest and biggest keys" {
    const alloc = std.testing.allocator;
    var tb = try buildTestTable(alloc, "key", 200, 256);
    defer tb.deinit();
    const tbl = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
    defer tbl.decrRef();

    try std.testing.expectEqualStrings("key0000", zdb.key.parseKey(tbl.smallest()));
    try std.testing.expectEqualStrings("key0199", zdb.key.parseKey(tbl.biggest()));
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
        const k = try zdb.key.withTs(alloc, key_str, @intCast(i + 1));
        defer alloc.free(k);
        @memset(big_val, @intCast(i % 256));
        try b.add(k, big_val);
    }

    var tb = try b.finish();
    defer tb.deinit();
    const tbl = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
    defer tbl.decrRef();

    try std.testing.expectEqual(@as(u32, n), tbl.index.key_count);

    var it = TableIterator.init(alloc, tbl, .p0);
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
