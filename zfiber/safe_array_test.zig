// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");

test "SafeArray: append, appendSlice, pop order, len" {
    var arr = fiber.SafeArray(u64).init(std.testing.allocator);
    defer arr.deinit();
    try std.testing.expectEqual(@as(usize, 0), arr.len());
    try std.testing.expect(arr.pop() == null);

    try arr.append(1);
    try arr.appendSlice(&.{ 2, 3 });
    try std.testing.expectEqual(@as(usize, 3), arr.len());

    // Last in, first out.
    try std.testing.expectEqual(@as(?u64, 3), arr.pop());
    try std.testing.expectEqual(@as(?u64, 2), arr.pop());
    try std.testing.expectEqual(@as(?u64, 1), arr.pop());
    try std.testing.expect(arr.pop() == null);
    try std.testing.expectEqual(@as(usize, 0), arr.len());
}
