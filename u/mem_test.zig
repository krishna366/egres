// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const utils = @import("utils");
const CountingAlloc = utils.mem.CountingAlloc;

test "counting allocator: live bytes follow alloc, resize and free" {
    var counting = CountingAlloc.init(std.testing.allocator);
    const alloc = counting.allocator();

    var a = try alloc.alloc(u8, 1000);
    const b = try alloc.alloc(u64, 10);
    try std.testing.expectEqual(@as(u64, 1080), live(&counting));

    // A resize counts only when the allocator does it in place; the
    // testing allocator may refuse either direction.
    var want: u64 = 1080;
    if (alloc.resize(a, 500)) {
        a.len = 500;
        want -= 500;
    }
    try std.testing.expectEqual(want, live(&counting));
    if (alloc.resize(a, 4096)) {
        want += 4096 - a.len;
        a.len = 4096;
    }
    try std.testing.expectEqual(want, live(&counting));

    alloc.free(a);
    alloc.free(b);
    try std.testing.expectEqual(@as(u64, 0), live(&counting));
}

fn live(c: *CountingAlloc) u64 {
    return c.live_bytes.load(.monotonic);
}

test "counting allocator: allocations past the limit are counted" {
    var counting = CountingAlloc.init(std.testing.allocator);
    const alloc = counting.allocator();
    counting.limit.store(1500, .monotonic);

    const a = try alloc.alloc(u8, 1000);
    defer alloc.free(a);
    try std.testing.expectEqual(@as(u64, 0), over(&counting));
    const b = try alloc.alloc(u8, 1000);
    defer alloc.free(b);
    try std.testing.expectEqual(@as(u64, 1), over(&counting));
}

fn over(c: *CountingAlloc) u64 {
    return c.over_limit.load(.monotonic);
}
