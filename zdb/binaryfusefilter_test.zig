const std = @import("std");
const zdb = @import("zdb");
const BinaryFuse = zdb.binaryfusefilter.BinaryFuse;

// The 11,504 block hashes of a compaction output that made `populate`
// give up with KeysLikelyNotUnique on 2026-09-20 (W2 run). They are all
// distinct; the filter must accept them.
const fuse_fail_dump = @embedFile("testdata/fuse_fail_11504.txt");

fn parseDump(alloc: std.mem.Allocator, text: []const u8) ![]u64 {
    var keys: std.ArrayList(u64) = .empty;
    errdefer keys.deinit(alloc);
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        try keys.append(alloc, try std.fmt.parseInt(u64, line, 16));
    }
    return keys.toOwnedSlice(alloc);
}

test "binary fuse populates the W2 crash key set" {
    const alloc = std.testing.allocator;
    const keys = try parseDump(alloc, fuse_fail_dump);
    defer alloc.free(keys);
    try std.testing.expectEqual(@as(usize, 11504), keys.len);

    // `init` + `populate` alone give up on this set: the array is too
    // tight at this size. `build` grows it by a segment and succeeds.
    var tight = try BinaryFuse(u16).init(alloc, keys.len);
    defer tight.deinit(alloc);
    try std.testing.expectError(error.KeysLikelyNotUnique, tight.populate(alloc, keys));

    var filter = try BinaryFuse(u16).build(alloc, keys);
    defer filter.deinit(alloc);
    try std.testing.expect(filter.segment_count > tight.segment_count);
    for (keys) |k| try std.testing.expect(filter.contain(k));
}

test "binary fuse populates random key sets around the segment-length boundary" {
    // n=11504 sits where the segment length steps from 512 to 1024, the
    // worst ratio of segment to array. Every size in the band must build.
    const alloc = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0x5eed);
    const rng = prng.random();
    var n: usize = 11_000;
    while (n <= 12_200) : (n += 50) {
        const keys = try alloc.alloc(u64, n);
        defer alloc.free(keys);
        for (keys) |*k| k.* = rng.int(u64);
        var filter = BinaryFuse(u16).build(alloc, keys) catch |err| {
            std.debug.print("build failed at n={d}: {s}\n", .{ n, @errorName(err) });
            return err;
        };
        defer filter.deinit(alloc);
        for (keys) |k| try std.testing.expect(filter.contain(k));
    }
}
