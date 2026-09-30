// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");

test "SafeHashMap: countValue tallies matching entries" {
    const Status = enum(u8) { sent, ok, fail };
    var m = fiber.SafeHashMap(u64, Status).init(std.testing.allocator);
    defer m.deinit();

    try std.testing.expectEqual(@as(u32, 0), m.countValue(.ok));

    try m.put(1, .sent);
    try m.put(2, .ok);
    try m.put(3, .fail);
    try m.put(4, .ok);
    try std.testing.expectEqual(@as(u32, 2), m.countValue(.ok));
    try std.testing.expectEqual(@as(u32, 1), m.countValue(.fail));
    try std.testing.expectEqual(@as(u32, 1), m.countValue(.sent));

    // Overwrite flips the tally, mirroring an announce response arriving.
    try m.put(1, .ok);
    try std.testing.expectEqual(@as(u32, 3), m.countValue(.ok));
    try std.testing.expectEqual(@as(u32, 0), m.countValue(.sent));
}
