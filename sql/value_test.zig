const std = @import("std");
const testing = std.testing;
const sql = @import("sql");
const Type = sql.Type;

test "sql.value: fixedWidth and isVarlen per type" {
    try testing.expectEqual(@as(?u8, 1), Type.bool_.fixedWidth());
    try testing.expectEqual(@as(?u8, 2), Type.int2.fixedWidth());
    try testing.expectEqual(@as(?u8, 4), Type.int4.fixedWidth());
    try testing.expectEqual(@as(?u8, 8), Type.int8.fixedWidth());
    try testing.expectEqual(@as(?u8, 2), Type.uint2.fixedWidth());
    try testing.expectEqual(@as(?u8, 4), Type.uint4.fixedWidth());
    try testing.expectEqual(@as(?u8, 8), Type.uint8.fixedWidth());
    try testing.expectEqual(@as(?u8, 4), Type.float4.fixedWidth());
    try testing.expectEqual(@as(?u8, 8), Type.float8.fixedWidth());
    try testing.expectEqual(@as(?u8, 4), Type.date.fixedWidth());
    try testing.expectEqual(@as(?u8, 8), Type.time.fixedWidth());
    try testing.expectEqual(@as(?u8, 8), Type.timestamp.fixedWidth());
    try testing.expectEqual(@as(?u8, 8), Type.timestamptz.fixedWidth());
    try testing.expectEqual(@as(?u8, 16), Type.interval.fixedWidth());
    try testing.expectEqual(@as(?u8, 16), Type.uuid.fixedWidth());
    try testing.expectEqual(@as(?u8, null), Type.text.fixedWidth());
    try testing.expectEqual(@as(?u8, null), Type.bytea.fixedWidth());

    try testing.expect(!Type.int4.isVarlen());
    try testing.expect(!Type.timestamptz.isVarlen());
    try testing.expect(!Type.interval.isVarlen());
    try testing.expect(!Type.uuid.isVarlen());
    try testing.expect(Type.text.isVarlen());
    try testing.expect(Type.bytea.isVarlen());
}
