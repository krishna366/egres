const std = @import("std");
const GuardAllocator = @import("utils").GuardAllocator;

test "GuardAllocator: alloc/write/free across sizes + stats" {
    var ga: GuardAllocator = .init();
    defer ga.deinit();
    const a = ga.allocator();

    const sizes = [_]usize{ 1, 7, 64, 4095, 4096, 4097, 100_000 };
    var bufs: [sizes.len][]u8 = undefined;
    for (sizes, 0..) |n, i| {
        const buf = try a.alloc(u8, n);
        // Touch every byte: must all be inside the RW data pages (no fault).
        @memset(buf, 0xAB);
        try std.testing.expectEqual(@as(u8, 0xAB), buf[0]);
        try std.testing.expectEqual(@as(u8, 0xAB), buf[n - 1]);
        bufs[i] = buf;
    }
    try std.testing.expectEqual(@as(usize, sizes.len), ga.alloc_count);
    for (bufs) |buf| a.free(buf);
    try std.testing.expectEqual(@as(usize, sizes.len), ga.freed_count);
    try std.testing.expectEqual(@as(usize, 0), ga.live_bytes);
}

test "GuardAllocator: honors alignment" {
    var ga: GuardAllocator = .init();
    defer ga.deinit();
    const a = ga.allocator();

    inline for (.{ 1, 8, 16, 64, 256 }) |alignment| {
        const p = try a.alignedAlloc(u8, comptime std.mem.Alignment.fromByteUnits(alignment), 17);
        try std.testing.expect(std.mem.isAligned(@intFromPtr(p.ptr), alignment));
        @memset(p, 1);
        a.free(p);
    }
}

test "GuardAllocator: realloc copies into a fresh mapping" {
    var ga: GuardAllocator = .init();
    defer ga.deinit();
    const a = ga.allocator();

    var buf = try a.alloc(u8, 32);
    for (buf, 0..) |*b, i| b.* = @truncate(i);
    // resize() returns false, so realloc must move + copy.
    buf = try a.realloc(buf, 9000);
    for (0..32) |i| try std.testing.expectEqual(@as(u8, @truncate(i)), buf[i]);
    a.free(buf);
}
