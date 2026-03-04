const std = @import("std");
const testing = std.testing;
const skiplist = @import("zdb").skl;
const Key = @import("zdb").key.Key;
const u = @import("utils");

fn writeKey(buf: []u8, idx: usize) ![]const u8 {
    return std.fmt.bufPrint(buf, "k{d:0>10}", .{idx});
}

fn fillValueForIndex(allocator: std.mem.Allocator, rand: *std.Random) ![]u8 {
    const min_len = 4 * 1024;
    const max_len = 1024 * 1024;
    const len = rand.intRangeAtMost(usize, min_len, max_len);
    const value = try allocator.alloc(u8, len);
    rand.bytes(value);
    return value;
}

test "skiplist basic operations and updates" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    const k1 = Key{ .primary = "1" };
    const k2 = Key{ .primary = "2" };

    try testing.expect(sl.get(k1) == null);
    try testing.expect(!sl.contains(k1));
    try testing.expectEqual(@as(usize, 0), sl.numNodes());

    try sl.put(.{ .primary = "1" }, "100");
    try testing.expectEqualStrings("100", sl.get(k1).?);
    try testing.expect(sl.contains(k1));
    try testing.expectEqual(@as(usize, 1), sl.numNodes());

    try sl.put(.{ .primary = "1" }, "200");
    try testing.expectEqualStrings("200", sl.get(k1).?);

    try sl.put(.{ .primary = "2" }, "300");
    try testing.expectEqualStrings("300", sl.get(k2).?);
    try testing.expectEqual(@as(usize, 2), sl.numNodes());
}

test "skiplist iterator ordering and seek" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    const data = [_]struct { key: []const u8, value: []const u8 }{
        .{ .key = "5", .value = "50" },
        .{ .key = "2", .value = "20" },
        .{ .key = "8", .value = "80" },
        .{ .key = "1", .value = "10" },
        .{ .key = "9", .value = "90" },
        .{ .key = "3", .value = "30" },
    };
    for (data) |item| try sl.put(.{ .primary = item.key }, item.value);

    var it = sl.iterator();
    defer it.deinit();

    const expected = [_][]const u8{ "1", "2", "3", "5", "8", "9" };
    for (expected) |k| {
        const e = it.next().?;
        try testing.expectEqualStrings(k, e.key.primary);
    }
    try testing.expect(it.next() == null);

    var seek_it = sl.iterator();
    defer seek_it.deinit();
    const seek_entry = seek_it.seek(.{ .primary = "4" }).?;
    try testing.expectEqualStrings("5", seek_entry.key.primary);
    try testing.expectEqualStrings("8", seek_it.next().?.key.primary);
}

test "skiplist refcount usage with explicit acquire/release" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    try sl.put(.{ .primary = "1" }, "10");
    try sl.put(.{ .primary = "2" }, "20");

    sl.acquire();
    sl.acquire();

    const k1 = Key{ .primary = "1" };
    try testing.expectEqualStrings("10", sl.get(k1).?);

    sl.release();
    sl.release();

    try testing.expectEqualStrings("10", sl.get(k1).?);
}

test "skiplist grows beyond 64 MiB up to 256 MiB and validates full iteration" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    const target_capacity = 256 * 1024 * 1024;
    try testing.expect(sl.memoryCapacity() >= 64 * 1024 * 1024);

    var expected_values = std.ArrayList([]u8).empty;
    defer expected_values.deinit(testing.allocator);

    var tmp_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    _ = try tmp_arena.allocator().alloc(u8, target_capacity);
    _ = tmp_arena.reset(.{ .retain_with_limit = target_capacity });
    defer tmp_arena.deinit();

    var value_prng = std.Random.DefaultPrng.init(@intCast(std.time.nanoTimestamp()));
    var value_rand = value_prng.random();

    const insert_start_ns = std.time.nanoTimestamp();
    var inserted: usize = 0;
    while (sl.memoryCapacity() < target_capacity) : (inserted += 1) {
        var key_buf: [32]u8 = undefined;
        const key = try writeKey(&key_buf, inserted);
        const value = try fillValueForIndex(tmp_arena.allocator(), &value_rand);

        try sl.put(.{ .primary = key }, value);
        try expected_values.append(testing.allocator, value);

        if ((inserted + 1) % 100 == 0) {
            u.testPrint(
                "[zdb.skl] inserted keys={d} capacity={d} usage={d}\n",
                .{ inserted + 1, sl.memoryCapacity(), sl.memoryUsage() },
            );
        }
    }
    const insert_elapsed_ns: i128 = std.time.nanoTimestamp() - insert_start_ns;

    try testing.expect(sl.memoryCapacity() >= target_capacity);
    try testing.expectEqual(inserted, sl.numNodes());
    try testing.expectEqual(inserted, expected_values.items.len);
    u.testPrint(
        "[zdb.skl] final inserted keys={d} insert_elapsed_ms={d}\n",
        .{ inserted, @divTrunc(insert_elapsed_ns, std.time.ns_per_ms) },
    );

    const verify_start_ns = std.time.nanoTimestamp();
    var it = sl.iterator();
    defer it.deinit();

    var idx: usize = 0;
    while (idx < inserted) : (idx += 1) {
        const entry = it.next() orelse return error.TestUnexpectedResult;

        var expected_key_buf: [32]u8 = undefined;
        const expected_key = try writeKey(&expected_key_buf, idx);
        try testing.expectEqualStrings(expected_key, entry.key.primary);

        const expected_value = expected_values.items[idx];
        try testing.expectEqualSlices(u8, expected_value, entry.value);
    }
    try testing.expect(it.next() == null);
    const verify_elapsed_ns: i128 = std.time.nanoTimestamp() - verify_start_ns;
    u.testPrint(
        "[zdb.skl] verification complete keys={d} verify_elapsed_ms={d}\n",
        .{ inserted, @divTrunc(verify_elapsed_ns, std.time.ns_per_ms) },
    );
}
