const std = @import("std");
const jemalloc = @import("root.zig");

/// Real usable size of a live allocation, straight from the extent map.
extern fn sallocx(ptr: *const anyopaque, flags: c_int) usize;
/// Usable size a fresh allocation of `size` would get (pure size-class math).
extern fn nallocx(size: usize, flags: c_int) usize;

test "jemalloc accounts for Zig allocator allocations" {
    const allocator = jemalloc.allocator();
    const before = jemalloc.threadActivity() orelse
        return error.JemallocStatsUnavailable;

    // Thread-local cumulative counters make this deterministic even when the
    // project runner executes allocation-heavy tests on other worker threads.
    const requested = 1024 * 1024 + 17;
    var during: jemalloc.ThreadActivity = undefined;
    {
        const memory = try allocator.alignedAlloc(
            u8,
            .@"64",
            requested,
        );
        defer allocator.free(memory);

        try std.testing.expectEqual(0, @intFromPtr(memory.ptr) % 64);
        @memset(memory, 0xa5);
        during = jemalloc.threadActivity() orelse
            return error.JemallocStatsUnavailable;
        try std.testing.expect(
            during.allocated >= before.allocated + requested,
        );
    }

    const after = jemalloc.threadActivity() orelse
        return error.JemallocStatsUnavailable;
    try std.testing.expect(
        after.deallocated >= before.deallocated + requested,
    );

    // Counters are cumulative, so they are monotone non-decreasing across
    // the whole test on this thread.
    try std.testing.expect(after.allocated >= during.allocated);
    try std.testing.expect(after.deallocated >= during.deallocated);

    // Cached-pointer fast path: with no allocator activity between two
    // consecutive reads, the counters must be bit-identical.
    const again = jemalloc.threadActivity() orelse
        return error.JemallocStatsUnavailable;
    try std.testing.expectEqual(after.allocated, again.allocated);
    try std.testing.expectEqual(after.deallocated, again.deallocated);
}

test "verifyInUse accepts the jemalloc allocator" {
    try jemalloc.verifyInUse(jemalloc.allocator());
}

test "verifyInUse rejects a non-jemalloc allocator" {
    // Zig's testing allocator draws from the debug allocator, not jemalloc,
    // so the thread's jemalloc counters must not move.
    try std.testing.expectError(
        error.JemallocNotInUse,
        jemalloc.verifyInUse(std.testing.allocator),
    );
}

/// A successful `resize` promises the caller may later free with `new_len`.
/// `free` uses sized deallocation (`sdallocx`), which requires `new_len` to
/// round to the extent's REAL size class. So whenever `resize` reports
/// success, `sallocx(ptr) == nallocx(new_len)` must hold; reporting success
/// without it means the eventual free deallocates into the wrong bin.
fn expectResizeKeepsSizedFreeSafe(
    memory: []u8,
    new_len: usize,
) !?[]u8 {
    const allocator = jemalloc.allocator();
    if (!allocator.resize(memory, new_len)) return null;
    try std.testing.expectEqual(
        sallocx(memory.ptr, 0),
        nallocx(new_len, 0),
    );
    return memory.ptr[0..new_len];
}

test "shrink across small size classes must not report success" {
    const allocator = jemalloc.allocator();
    // 128 and 64 are distinct jemalloc small size classes, so an in-place
    // shrink is impossible: jemalloc cannot move a slab region between bins.
    const memory = try allocator.alloc(u8, 128);
    try std.testing.expectEqual(128, sallocx(memory.ptr, 0));
    // On the buggy adapter this returns a slice and the inner expectEqual
    // fails (real class 128, sized free would use 64). On a correct adapter
    // resize declines and we free at the original length. The allocation is
    // deliberately leaked on failure: freeing it at the wrong length is the
    // corruption this test exists to prevent.
    const shrunk = try expectResizeKeepsSizedFreeSafe(memory, 64);
    try std.testing.expectEqual(null, shrunk);
    allocator.free(memory);
}

test "shrink within one small size class stays in place" {
    const allocator = jemalloc.allocator();
    // Small classes in this range are 96/112/128: 116 and 120 both round
    // to 128, so resize must succeed as a no-op and sized free at the new
    // length stays in class 128.
    const memory = try allocator.alloc(u8, 120);
    try std.testing.expectEqual(nallocx(120, 0), nallocx(116, 0));
    const shrunk = (try expectResizeKeepsSizedFreeSafe(memory, 116)) orelse
        return error.SameClassShrinkRefused;
    allocator.free(shrunk);
}

test "large in-place shrink adjusts the extent before sized free" {
    const allocator = jemalloc.allocator();
    // Both sizes are large classes; jemalloc shrinks by trimming the
    // extent, so success requires the recorded class to become 1 MiB.
    const memory = try allocator.alloc(u8, 2 * 1024 * 1024);
    const shrunk = (try expectResizeKeepsSizedFreeSafe(
        memory,
        1024 * 1024,
    )) orelse {
        allocator.free(memory);
        return;
    };
    allocator.free(shrunk);
}
