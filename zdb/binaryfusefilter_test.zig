// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

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

    // Before the segment-count fix this set exhausted `populate`'s seeds
    // (12 segments of 1,024). Now the length halves twice, to 256 (512
    // would still give only 28 segments), and it builds on the first
    // attempt.
    var filter = try BinaryFuse(u16).build(alloc, keys);
    defer filter.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 256), filter.segment_length);
    try std.testing.expect(filter.segment_count >= 32);
    for (keys) |k| try std.testing.expect(filter.contain(k));
}

test "binary fuse populates random key sets around the segment-length boundary" {
    // The segment length doubles at several sizes below ~40k keys, and
    // each doubling used to leave too few segments. Every size across
    // those bands must build on the first attempt, for several seeds.
    const alloc = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0x5eed);
    const rng = prng.random();
    var n: usize = 3_000;
    while (n <= 60_000) : (n += if (n < 13_000) 25 else 500) {
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
