const std = @import("std");
const utils = @import("utils");

test "SafeHashMap: countValue tallies matching entries" {
    const Status = enum(u8) { sent, ok, fail };
    var m = utils.SafeHashMap(u64, Status).init(std.testing.allocator);
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

test "SafeArray: append, appendSlice, pop order, len" {
    var arr = utils.SafeArray(u64).init(std.testing.allocator);
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
