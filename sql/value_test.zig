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

test "sql.value: Type.fromName resolves DDL type names" {
    try testing.expectEqual(@as(?Type, .int2), Type.fromName("int2"));
    try testing.expectEqual(@as(?Type, .int4), Type.fromName("int4"));
    try testing.expectEqual(@as(?Type, .int8), Type.fromName("int8"));
    try testing.expectEqual(@as(?Type, .uint2), Type.fromName("uint2"));
    try testing.expectEqual(@as(?Type, .uint4), Type.fromName("uint4"));
    try testing.expectEqual(@as(?Type, .uint8), Type.fromName("uint8"));
    try testing.expectEqual(@as(?Type, .float4), Type.fromName("float4"));
    try testing.expectEqual(@as(?Type, .float8), Type.fromName("float8"));
    try testing.expectEqual(@as(?Type, .bool_), Type.fromName("bool"));
    // varchar folds into text (length modifier dropped).
    try testing.expectEqual(@as(?Type, .text), Type.fromName("text"));
    try testing.expectEqual(@as(?Type, .text), Type.fromName("varchar"));
    try testing.expectEqual(@as(?Type, .bytea), Type.fromName("bytea"));
    try testing.expectEqual(@as(?Type, .date), Type.fromName("date"));
    try testing.expectEqual(@as(?Type, .timestamptz), Type.fromName("timestamptz"));
    try testing.expectEqual(@as(?Type, .uuid), Type.fromName("uuid"));
    // Unsupported names miss — the DDL layer turns these into errors.
    try testing.expect(Type.fromName("serial") == null);
    try testing.expect(Type.fromName("numeric") == null);
    try testing.expect(Type.fromName("") == null);
}
