const std = @import("std");
const utils = @import("utils");
const String = utils.String;
const zdb = @import("zdb");
const zbuf = @import("zbuf").zbuf;
const Key = zdb.kv.Key;
const vfs = zdb.vfs;

const Builder = zdb.table.builder.Builder;
const Table = zdb.table.Table;
const TableIterator = zdb.table.TableIterator;

/// Test helper: decrRef + destroy the Table allocation.
fn destroyTable(t: *Table) void {
    const allocator = t.allocator;
    t.decrRef();
    allocator.destroy(t);
}

/// Test helper: create an in-memory FileHandle from a buffer (dupes the data).
fn fromBuf(allocator: std.mem.Allocator, buf: String) !vfs.FileHandle {
    const owned = try allocator.dupe(u8, buf);
    return .{
        .file_size = @intCast(owned.len),
        .owned_buf = owned,
        .buf_allocator = allocator,
        .vfs = vfs.g(),
    };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a TableBuffer with entries keyed as "{prefix}{i:0>4}" for i in [start, end),
/// at ts=0, with value "{i}". block_size controls how many entries land per block.
fn buildRangeTable(
    allocator: std.mem.Allocator,
    prefix: String,
    start: usize,
    end: usize,
    block_size: u32,
) !zdb.table.builder.TableBuffer {
    var b = Builder.init(allocator, .{ .target_block_size = block_size });
    defer b.deinit();

    var i: usize = start;
    while (i < end) : (i += 1) {
        var key_buf: [32]u8 = undefined;
        const key_no_ts = try std.fmt.bufPrint(&key_buf, "{s}{d:0>4}", .{ prefix, i });
        var val_buf: [20]u8 = undefined;
        const val = try std.fmt.bufPrint(&val_buf, "{d}", .{i});

        try b.add(.{ .primary = key_no_ts }, val);
    }
    return b.finish();
}

/// Build a TableBuffer with n entries keyed as "{prefix}{i:0>4}" at ts=0,
/// with value "{i}". block_size controls how many entries land per block.
fn buildTestTable(
    allocator: std.mem.Allocator,
    prefix: String,
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

        try b.add(.{ .primary = key_no_ts }, val);
    }
    return b.finish();
}

// ---------------------------------------------------------------------------
// Existing tests (updated for itemRef / blocksAt)
// ---------------------------------------------------------------------------

// Builder.finish() pads the bytes so the total file size is a multiple of
// vfs.page_size. Verifies the math across several payload sizes that span
// page-boundary wraparound.
test "builder.finish output is page-aligned for any payload size" {
    const allocator = std.testing.allocator;
    const sizes = [_]usize{ 1, 32, 100, 1000, 4095, 4096, 4097, 9000 };
    for (sizes) |n| {
        var b = Builder.init(allocator, .{ .target_block_size = 4 * 1024 });
        defer b.deinit();
        var i: usize = 0;
        while (i < n) : (i += 1) {
            var key_buf: [32]u8 = undefined;
            const k = try std.fmt.bufPrint(&key_buf, "k{d:0>10}", .{i});
            try b.add(.{ .primary = k }, "v");
        }
        var tb = try b.finish();
        defer tb.deinit();
        const remainder = tb.bytes.len % zdb.vfs.page_size;
        std.testing.expectEqual(@as(usize, 0), remainder) catch |err| {
            std.log.warn("payload n={d}: bytes.len={d}, page_size={d}, remainder={d}", .{
                n, tb.bytes.len, zdb.vfs.page_size, remainder,
            });
            return err;
        };
    }
}

test "table builder/reader roundtrip and seek" {
    var b = Builder.init(std.testing.allocator, .{ .target_block_size = 256 });
    defer b.deinit();

    try b.add(.{ .primary = "a", .timestamp = 9 }, "va9");
    try b.add(.{ .primary = "a", .timestamp = 7 }, "va7");
    try b.add(.{ .primary = "b", .timestamp = 3 }, "vb3");
    try b.add(.{ .primary = "c", .timestamp = 1 }, "vc1");

    var tb = try b.finish();
    defer tb.deinit();

    const r = try Table.openHandle(std.testing.allocator, try fromBuf(std.testing.allocator, tb.bytes), .p0);
    defer destroyTable(r);

    try std.testing.expectEqual(@as(u32, 4), r.index.key_count);
    try std.testing.expectEqual(@as(u64, 9), r.index.max_version);
    try std.testing.expect(r.blockCount() >= 1);

    var it = TableIterator.init(std.testing.allocator, r, .p0);
    defer it.deinit();
    try it.seekRange(.{}, .{});
    try std.testing.expectEqualStrings("va9", it.itemRef().?.value);

    try it.seekRange(.{ .primary = "b", .timestamp = 3 }, .{});
    try std.testing.expectEqualStrings("vb3", it.itemRef().?.value);
}

// seekGe compares stored keys in place, against the base key plus a diff,
// never materializing them. Two rules are easy to get wrong that way and
// unexercised by the fixed-width keys the other tests use: a primary that
// is a strict PREFIX of another sorts first (a < ab < abc < b), and among
// versions of one primary the NEWEST sorts first.
test "seekGe: prefix-length and version ordering" {
    const alloc = std.testing.allocator;

    var b = Builder.init(alloc, .{ .target_block_size = 4 * 1024 });
    defer b.deinit();
    try b.add(.{ .primary = "a", .timestamp = 5 }, "a5");
    try b.add(.{ .primary = "ab", .timestamp = 9 }, "ab9");
    try b.add(.{ .primary = "ab", .timestamp = 3 }, "ab3");
    try b.add(.{ .primary = "abc", .timestamp = 1 }, "abc1");
    try b.add(.{ .primary = "b", .timestamp = 7 }, "b7");

    var tb = try b.finish();
    defer tb.deinit();
    const t = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
    defer destroyTable(t);

    // One block, so every seek exercises the in-block search.
    try std.testing.expectEqual(@as(usize, 1), t.blockCount());

    var it = TableIterator.init(alloc, t, .p0);
    defer it.deinit();

    // Stored order is the seek order.
    const want = [_]String{ "a5", "ab9", "ab3", "abc1", "b7" };
    try it.seekRange(.{}, .{});
    for (want) |v| {
        try std.testing.expectEqualStrings(v, it.itemRef().?.value);
        try it.next();
    }
    try std.testing.expect(it.itemRef() == null);

    const max_ts = std.math.maxInt(u64);
    const cases = [_]struct { k: Key, want: String }{
        // Newest version of a primary comes first.
        .{ .k = .{ .primary = "ab", .timestamp = max_ts }, .want = "ab9" },
        // A read below the newest lands on the next version down.
        .{ .k = .{ .primary = "ab", .timestamp = 5 }, .want = "ab3" },
        .{ .k = .{ .primary = "ab", .timestamp = 3 }, .want = "ab3" },
        // Shorter primary sorts before its own extensions.
        .{ .k = .{ .primary = "a", .timestamp = max_ts }, .want = "a5" },
        .{ .k = .{ .primary = "abc", .timestamp = max_ts }, .want = "abc1" },
        // Absent keys land on the next one that exists.
        .{ .k = .{ .primary = "aa", .timestamp = max_ts }, .want = "ab9" },
        .{ .k = .{ .primary = "az", .timestamp = max_ts }, .want = "b7" },
    };
    for (cases) |c| {
        try it.seekRange(c.k, .{});
        try std.testing.expectEqualStrings(c.want, it.itemRef().?.value);
    }

    // Past every key: nothing left.
    try it.seekRange(.{ .primary = "c", .timestamp = max_ts }, .{});
    try std.testing.expect(it.itemRef() == null);
}

test "table binary fuse contains keys" {
    const alloc = std.testing.allocator;

    var b = Builder.init(alloc, .{ .target_block_size = 4 * 1024 });
    defer b.deinit();

    const n: usize = 1024;
    var key_buf: [16]u8 = undefined;
    for (0..n) |i| {
        const key = try std.fmt.bufPrint(&key_buf, "k{d:0>6}", .{i});
        try b.add(.{ .primary = key, .timestamp = @intCast(i + 1) }, "v");
    }

    var tb = try b.finish();
    defer tb.deinit();

    const t = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
    defer destroyTable(t);

    // Sample present keys.
    try std.testing.expect(t.containsKey(.{ .primary = "k000000" }));
    try std.testing.expect(t.containsKey(.{ .primary = "k000500" }));
    try std.testing.expect(t.containsKey(.{ .primary = "k001023" }));

    // Absent keys — BF should reject (containsKey returns false).
    try std.testing.expect(!t.containsKey(.{ .primary = "missing-1" }));
    try std.testing.expect(!t.containsKey(.{ .primary = "missing-2" }));
    try std.testing.expect(!t.containsKey(.{ .primary = "missing-3" }));
}

test "table binary fuse large keyset contains sampled keys" {
    const alloc = std.testing.allocator;
    const n: usize = 1_000_000;
    const sample: usize = 10_000;

    var b = Builder.init(alloc, .{ .target_block_size = 32 * 1024 });
    defer b.deinit();

    var i: usize = 0;
    while (i < n) : (i += 1) {
        var key_buf: [16]u8 = undefined;
        const primary = try std.fmt.bufPrint(&key_buf, "k{d:0>6}", .{i});
        try b.add(.{ .primary = primary }, "v");
    }

    var tb = try b.finish();
    defer tb.deinit();

    const t = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
    defer destroyTable(t);

    const seed: u64 = @intCast(std.Io.Timestamp.now(utils.context.g().io, .real).nanoseconds);
    var prng = std.Random.DefaultPrng.init(seed);
    const rnd = prng.random();

    i = 0;
    while (i < sample) : (i += 1) {
        const idx = rnd.uintAtMost(u64, n - 1);
        var key_buf: [16]u8 = undefined;
        const key_no_ts = try std.fmt.bufPrint(&key_buf, "k{d:0>6}", .{idx});
        try std.testing.expect(t.containsKey(.{ .primary = key_no_ts }));
    }

    var non_added_matches: usize = 0;
    i = 0;
    while (i < sample) : (i += 1) {
        const idx = @as(u64, n) + rnd.uintAtMost(u64, n - 1);
        var key_buf: [16]u8 = undefined;
        const key_no_ts = try std.fmt.bufPrint(&key_buf, "k{d:0>6}", .{idx});
        if (t.containsKey(.{ .primary = key_no_ts })) {
            non_added_matches += 1;
        }
    }
    // Some false positives are possible.
    try std.testing.expect(non_added_matches < 10);
}

test "table checksum mismatch is detected" {
    var b = Builder.init(std.testing.allocator, .{ .target_block_size = 256 });
    defer b.deinit();

    try b.add(.{ .primary = "k1", .timestamp = 1 }, "v1");
    try b.add(.{ .primary = "k2", .timestamp = 1 }, "v2");

    var tb = try b.finish();
    defer tb.deinit();

    const r = try Table.openHandle(std.testing.allocator, try fromBuf(std.testing.allocator, tb.bytes), .p0);
    defer destroyTable(r);

    const first_meta = r.index.blocks[0];
    const block_start: usize = first_meta.offset;
    const block_end = block_start + first_meta.len;
    r.handle.owned_buf.?[block_end - 5] ^= 0x01;

    try std.testing.expectError(error.ChecksumMismatch, r.readBlock(0, .p0));
}

test "merge iterator merges sorted streams" {
    var b1 = Builder.init(std.testing.allocator, .{ .target_block_size = 256 });
    defer b1.deinit();
    try b1.add(.{ .primary = "a", .timestamp = 5 }, "a5");
    try b1.add(.{ .primary = "c", .timestamp = 1 }, "c1");
    var t1 = try b1.finish();
    defer t1.deinit();

    var b2 = Builder.init(std.testing.allocator, .{ .target_block_size = 256 });
    defer b2.deinit();
    try b2.add(.{ .primary = "b", .timestamp = 4 }, "b4");
    try b2.add(.{ .primary = "d", .timestamp = 1 }, "d1");
    var t2 = try b2.finish();
    defer t2.deinit();

    const r1 = try Table.openHandle(std.testing.allocator, try fromBuf(std.testing.allocator, t1.bytes), .p0);
    defer destroyTable(r1);
    const r2 = try Table.openHandle(std.testing.allocator, try fromBuf(std.testing.allocator, t2.bytes), .p0);
    defer destroyTable(r2);

    var iters = [_]zdb.table.merge_iterator.Iterator{
        .{ .table = TableIterator.init(std.testing.allocator, r1, .p0) },
        .{ .table = TableIterator.init(std.testing.allocator, r2, .p0) },
    };
    var m = zdb.table.merge_iterator.MergeIterator.init(std.testing.allocator, iters[0..]);
    defer m.deinit();

    try m.seekRange(.{}, .{});
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
        defer destroyTable(tbl);

        var it = TableIterator.init(alloc, tbl, .p0);
        defer it.deinit();

        var count: usize = 0;
        try it.seekRange(.{}, .{});
        while (it.itemRef()) |entry| : (count += 1) {
            var key_buf: [32]u8 = undefined;
            const expected_key_no_ts = try std.fmt.bufPrint(&key_buf, "key{d:0>4}", .{count});
            try std.testing.expectEqualStrings(expected_key_no_ts, entry.key.primary);

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
        defer destroyTable(tbl);

        var it = TableIterator.init(alloc, tbl, .p0);
        defer it.deinit();

        try it.seekRange(.{}, .{});
        const entry = it.itemRef() orelse return error.UnexpectedNull;
        try std.testing.expectEqualStrings("0", entry.value);
        try std.testing.expectEqualStrings("key0000", entry.key.primary);
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
    defer destroyTable(tbl);

    var it = TableIterator.init(alloc, tbl, .p0);
    defer it.deinit();

    const Case = struct { in: String, valid: bool, out: String };
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
        try it.seekRange(.{ .primary = c.in, .timestamp = 0 }, .{});
        if (!c.valid) {
            try std.testing.expect(it.itemRef() == null);
            continue;
        }
        const entry = it.itemRef() orelse return error.UnexpectedNull;
        try std.testing.expectEqualStrings(c.out, entry.key.primary);
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
        defer destroyTable(tbl);

        var it = TableIterator.init(alloc, tbl, .p0);
        defer it.deinit();

        var count: usize = 0;
        try it.seekRange(.{}, .{});
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
    defer destroyTable(tbl);

    var it = TableIterator.init(alloc, tbl, .p0);
    defer it.deinit();

    // Seek to key1010, iterate to end — should visit keys 1010..9999.
    try it.seekRange(.{ .primary = "key1010", .timestamp = 0 }, .{});
    var kid: usize = 1010;
    while (it.itemRef()) |entry| : (kid += 1) {
        var buf: [32]u8 = undefined;
        const expected = try std.fmt.bufPrint(&buf, "key{d:0>4}", .{kid});
        try std.testing.expectEqualStrings(expected, entry.key.primary);
        try it.next();
    }
    try std.testing.expectEqual(@as(usize, 10000), kid);

    // Seek past end → invalid.
    try it.seekRange(.{ .primary = "key99999", .timestamp = 0 }, .{});
    try std.testing.expect(it.itemRef() == null);

    // Seek before start → first key.
    try it.seekRange(.{ .primary = "aaa", .timestamp = 0 }, .{});
    const first = it.itemRef() orelse return error.UnexpectedNull;
    try std.testing.expectEqualStrings("key0000", first.key.primary);
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
        try b.add(.{ .primary = key_str, .timestamp = i + 1 }, "");
    }

    var tb = try b.finish();
    defer tb.deinit();
    const tbl = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
    defer destroyTable(tbl);

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
    defer destroyTable(tbl);

    try std.testing.expectEqualStrings("key0000", tbl.smallest.primary);
    try std.testing.expectEqualStrings("key0199", tbl.biggest.primary);
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
        @memset(big_val, @intCast(i % 256));
        try b.add(.{ .primary = key_str, .timestamp = @as(u64, @intCast(i + 1)) }, big_val);
    }

    var tb = try b.finish();
    defer tb.deinit();
    const tbl = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
    defer destroyTable(tbl);

    try std.testing.expectEqual(@as(u32, n), tbl.index.key_count);

    var it = TableIterator.init(alloc, tbl, .p0);
    defer it.deinit();
    try it.seekRange(.{}, .{});

    var count: usize = 0;
    while (it.itemRef()) |entry| : (count += 1) {
        try std.testing.expectEqual(value_size, entry.value.len);
        // Verify fill byte matches the key index.
        try std.testing.expectEqual(@as(u8, @intCast(count % 256)), entry.value[0]);
        try it.next();
    }
    try std.testing.expectEqual(n, count);
}

// ---------------------------------------------------------------------------
// endBefore tests
// ---------------------------------------------------------------------------

test "seekRange: end truncates iteration" {
    const alloc = std.testing.allocator;
    // 100 keys: k0000..k0099
    var tb = try buildTestTable(alloc, "k", 100, 4096);
    defer tb.deinit();
    const tbl = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
    defer destroyTable(tbl);

    var it = TableIterator.init(alloc, tbl, .p0);
    defer it.deinit();

    // Stop before k0050.
    try it.seekRange(.{}, .{ .primary = "k0050" });

    var count: usize = 0;
    while (it.itemRef()) |_| : (count += 1) {
        try it.next();
    }
    try std.testing.expectEqual(@as(usize, 50), count);
}

test "seekRange: end after all keys returns everything" {
    const alloc = std.testing.allocator;
    var tb = try buildTestTable(alloc, "k", 100, 4096);
    defer tb.deinit();
    const tbl = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
    defer destroyTable(tbl);

    var it = TableIterator.init(alloc, tbl, .p0);
    defer it.deinit();

    try it.seekRange(.{}, .{ .primary = "z" });

    var count: usize = 0;
    while (it.itemRef()) |_| : (count += 1) {
        try it.next();
    }
    try std.testing.expectEqual(@as(usize, 100), count);
}

test "seekRange: end before all keys returns nothing" {
    const alloc = std.testing.allocator;
    var tb = try buildTestTable(alloc, "k", 100, 4096);
    defer tb.deinit();
    const tbl = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
    defer destroyTable(tbl);

    var it = TableIterator.init(alloc, tbl, .p0);
    defer it.deinit();

    try it.seekRange(.{}, .{ .primary = "a" });

    try std.testing.expect(it.itemRef() == null);
}

test "seekRange: no end restores full iteration" {
    const alloc = std.testing.allocator;
    var tb = try buildTestTable(alloc, "k", 100, 4096);
    defer tb.deinit();
    const tbl = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
    defer destroyTable(tbl);

    var it = TableIterator.init(alloc, tbl, .p0);
    defer it.deinit();

    // Set a bound, then clear it with a new seekRange.
    try it.seekRange(.{}, .{ .primary = "k0010" });
    try it.seekRange(.{}, .{});

    var count: usize = 0;
    while (it.itemRef()) |_| : (count += 1) {
        try it.next();
    }
    try std.testing.expectEqual(@as(usize, 100), count);
}

test "seekRange: start + end combined" {
    const alloc = std.testing.allocator;
    var tb = try buildTestTable(alloc, "k", 100, 4096);
    defer tb.deinit();
    const tbl = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
    defer destroyTable(tbl);

    var it = TableIterator.init(alloc, tbl, .p0);
    defer it.deinit();

    // Iterate only k0020..k0029.
    try it.seekRange(.{ .primary = "k0020" }, .{ .primary = "k0030" });

    var count: usize = 0;
    while (it.itemRef()) |entry| : (count += 1) {
        var buf: [32]u8 = undefined;
        const expected = try std.fmt.bufPrint(&buf, "k{d:0>4}", .{count + 20});
        try std.testing.expectEqualStrings(expected, entry.key.primary);
        try it.next();
    }
    try std.testing.expectEqual(@as(usize, 10), count);
}

// ---------------------------------------------------------------------------
// seekRange across multiple blocks and multiple tables (ConcatIterator)
// ---------------------------------------------------------------------------

test "seekRange: multiple blocks within a single table" {
    const alloc = std.testing.allocator;
    // 200 keys with tiny block size → many blocks.
    var tb = try buildTestTable(alloc, "k", 200, 64);
    defer tb.deinit();
    const tbl = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
    defer destroyTable(tbl);

    // Sanity: we have more than one block.
    try std.testing.expect(tbl.blockCount() > 1);

    var it = TableIterator.init(alloc, tbl, .p0);
    defer it.deinit();

    // Range: k0050..k0150 (exclusive), spanning multiple blocks.
    try it.seekRange(.{ .primary = "k0050" }, .{ .primary = "k0150" });

    var count: usize = 0;
    while (it.itemRef()) |entry| : (count += 1) {
        var buf: [32]u8 = undefined;
        const expected = try std.fmt.bufPrint(&buf, "k{d:0>4}", .{count + 50});
        try std.testing.expectEqualStrings(expected, entry.key.primary);
        try it.next();
    }
    try std.testing.expectEqual(@as(usize, 100), count);
}

test "seekRange: ConcatIterator across multiple tables" {
    const alloc = std.testing.allocator;
    const merge_iterator = zdb.table.merge_iterator;

    // Build 3 non-overlapping tables with small blocks:
    //   t0: k0000..k0099
    //   t1: k0100..k0199
    //   t2: k0200..k0299
    var tb0 = try buildTestTable(alloc, "k", 100, 64);
    defer tb0.deinit();
    var tb1 = try buildRangeTable(alloc, "k", 100, 200, 64);
    defer tb1.deinit();
    var tb2 = try buildRangeTable(alloc, "k", 200, 300, 64);
    defer tb2.deinit();

    const tbl0 = try Table.openHandle(alloc, try fromBuf(alloc, tb0.bytes), .p0);
    defer destroyTable(tbl0);
    const tbl1 = try Table.openHandle(alloc, try fromBuf(alloc, tb1.bytes), .p0);
    defer destroyTable(tbl1);
    const tbl2 = try Table.openHandle(alloc, try fromBuf(alloc, tb2.bytes), .p0);
    defer destroyTable(tbl2);

    var table_iters = [_]TableIterator{
        TableIterator.init(alloc, tbl0, .p0),
        TableIterator.init(alloc, tbl1, .p0),
        TableIterator.init(alloc, tbl2, .p0),
    };

    var concat = merge_iterator.ConcatIterator.init(&table_iters);
    defer concat.deinit();

    // Range spanning all 3 tables: k0050..k0250 (exclusive).
    try concat.seekRange(.{ .primary = "k0050" }, .{ .primary = "k0250" });

    var count: usize = 0;
    while (concat.itemRef()) |entry| : (count += 1) {
        var buf: [32]u8 = undefined;
        const expected = try std.fmt.bufPrint(&buf, "k{d:0>4}", .{count + 50});
        try std.testing.expectEqualStrings(expected, entry.key.primary);
        try concat.next();
    }
    try std.testing.expectEqual(@as(usize, 200), count);

    // Range within a single middle table: k0120..k0180.
    try concat.seekRange(.{ .primary = "k0120" }, .{ .primary = "k0180" });

    count = 0;
    while (concat.itemRef()) |entry| : (count += 1) {
        var buf: [32]u8 = undefined;
        const expected = try std.fmt.bufPrint(&buf, "k{d:0>4}", .{count + 120});
        try std.testing.expectEqualStrings(expected, entry.key.primary);
        try concat.next();
    }
    try std.testing.expectEqual(@as(usize, 60), count);

    // Range past all tables.
    try concat.seekRange(.{ .primary = "z" }, .{});
    try std.testing.expect(concat.itemRef() == null);

    // Range before all tables with end inside first table.
    try concat.seekRange(.{}, .{ .primary = "k0010" });
    count = 0;
    while (concat.itemRef()) |_| : (count += 1) {
        try concat.next();
    }
    try std.testing.expectEqual(@as(usize, 10), count);

    // Regression: empty start AND empty end must walk every key across
    // every sub-iter. The previous implementation only seeked iters[0]
    // up-front; once `next()` exhausted iters[0] and advanced to
    // iters[1], `itemRef()` returned null (never seeked) and the
    // ConcatIterator declared itself done after just the first table.
    try concat.seekRange(.{}, .{});
    count = 0;
    while (concat.itemRef()) |entry| : (count += 1) {
        var buf: [32]u8 = undefined;
        const expected = try std.fmt.bufPrint(&buf, "k{d:0>4}", .{count});
        try std.testing.expectEqualStrings(expected, entry.key.primary);
        try concat.next();
    }
    try std.testing.expectEqual(@as(usize, 300), count);
}

// ---------------------------------------------------------------------------
// Meta byte: survives the SST entry encoding
// ---------------------------------------------------------------------------

/// Distinct-but-deterministic meta for entry i, mixing both nibbles so a
/// mix-up between kind and encoding shows up as a mismatch.
fn metaFor(i: usize) zdb.kv.Meta {
    return .{
        .kind = switch (i % 3) {
            0 => .deleted,
            1 => .delta,
            else => .state,
        },
        .encoding = if ((i % 2) == 1) .roaring else .raw,
    };
}

test "table: meta round-trips through builder and iterator" {
    const alloc = std.testing.allocator;
    // Small blocks so the table spans many blocks: meta has to survive each
    // block re-anchoring on a new base key.
    var b = Builder.init(alloc, .{ .target_block_size = 4096 });
    defer b.deinit();

    const n = 500;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        var key_buf: [32]u8 = undefined;
        const key_str = try std.fmt.bufPrint(&key_buf, "k:{d:0>4}", .{i});
        var val_buf: [20]u8 = undefined;
        // Every third entry has an EMPTY value with a non-zero meta. This is
        // the case that would break if meta were a value prefix instead of a
        // header field: the value region's length is implicit, so an empty
        // value must stay a zero-byte region.
        const val = if (i % 3 == 0)
            ""
        else
            try std.fmt.bufPrint(&val_buf, "v{d}", .{i});

        try b.addItem(.{
            .key = .{ .primary = key_str, .timestamp = 1 },
            .value = val,
            .meta = metaFor(i),
        });
    }

    var tb = try b.finish();
    defer tb.deinit();
    const tbl = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
    defer destroyTable(tbl);

    var it = TableIterator.init(alloc, tbl, .p0);
    defer it.deinit();
    try it.seekRange(.{}, .{});

    var count: usize = 0;
    while (it.itemRef()) |item| {
        try std.testing.expectEqual(metaFor(count), item.meta);
        if (count % 3 == 0) try std.testing.expectEqual(@as(usize, 0), item.value.len);
        count += 1;
        try it.next();
    }
    try std.testing.expectEqual(@as(usize, n), count);
}

test "table: meta survives random access via seekGe" {
    const alloc = std.testing.allocator;
    var b = Builder.init(alloc, .{ .target_block_size = 4096 });
    defer b.deinit();

    const n = 500;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        var key_buf: [32]u8 = undefined;
        const key_str = try std.fmt.bufPrint(&key_buf, "k:{d:0>4}", .{i});
        try b.addItem(.{
            .key = .{ .primary = key_str, .timestamp = 1 },
            .value = "x",
            .meta = metaFor(i),
        });
    }

    var tb = try b.finish();
    defer tb.deinit();
    const tbl = try Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
    defer destroyTable(tbl);

    // setIdx is reached by binary search too, not just forward scans, so seek
    // straight into the middle of blocks rather than walking there.
    for ([_]usize{ 0, 1, 7, 128, 199, 256, 499 }) |target| {
        var key_buf: [32]u8 = undefined;
        const key_str = try std.fmt.bufPrint(&key_buf, "k:{d:0>4}", .{target});

        var it = TableIterator.init(alloc, tbl, .p0);
        defer it.deinit();
        try it.seekRange(.{ .primary = key_str, .timestamp = 1 }, .{});

        const item = it.itemRef() orelse return error.TestExpectedItem;
        try std.testing.expectEqualStrings(key_str, item.key.primary);
        try std.testing.expectEqual(metaFor(target), item.meta);
    }
}

test "table: SST written before format_version is rejected" {
    const alloc = std.testing.allocator;
    var tb = try buildTestTable(alloc, "foo", 10, 4096);
    defer tb.deinit();

    // Rewind the index to look like one written before field id 10 existed:
    // shrink its size_bytes so formatVersion() reads back 0 from a slot past
    // the header. Then repair the index checksum, because the whole point is
    // that checksums do NOT catch a stale entry format — only this gate does.
    const bytes = tb.bytes;
    const trailer_at = bytes.len - 12;
    const index_len = std.mem.readInt(u32, bytes[trailer_at..][0..4], .little);
    const index_at = trailer_at - index_len;
    const index_bytes = bytes[index_at..trailer_at];

    const cur_size = std.mem.readInt(u16, index_bytes[0..2], .little);
    try std.testing.expectEqual(@as(u16, 51), cur_size);
    std.mem.writeInt(u16, index_bytes[0..2], 47, .little);
    std.mem.writeInt(u64, bytes[trailer_at + 4 ..][0..8], zbuf.hash(index_bytes), .little);

    try std.testing.expectError(
        error.UnsupportedTableFormat,
        Table.openHandle(alloc, try fromBuf(alloc, bytes), .p0),
    );
}
