const std = @import("std");
const testing = std.testing;
const sql = @import("sql");
const pgwire = @import("pgwire");
const Type = sql.Type;
const value = sql.value;

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

test "sql.value: parseValue coerces literals and rejects unsupported types" {
    try testing.expectEqual(@as(i32, -7), (try value.parseValue(.int4, "-7")).int4);
    try testing.expectEqual(@as(u64, 42), (try value.parseValue(.uint8, "42")).uint8);
    try testing.expectEqual(true, (try value.parseValue(.bool_, "t")).bool_);
    try testing.expectEqual(false, (try value.parseValue(.bool_, "FALSE")).bool_);
    try testing.expectEqual(@as(f64, 1.5), (try value.parseValue(.float8, "1.5")).float8);
    try testing.expectEqualStrings("hi", (try value.parseValue(.text, "hi")).text);
    // Bad numeric / bool → error (executor maps to 22P02).
    try testing.expectError(error.InvalidCharacter, value.parseValue(.int4, "nope"));
    try testing.expectError(error.InvalidBool, value.parseValue(.bool_, "maybe"));
    // Types this slice can't parse.
    try testing.expectError(error.UnsupportedType, value.parseValue(.date, "2026-01-01"));
    try testing.expectError(error.UnsupportedType, value.parseValue(.uuid, "x"));
}

test "sql.value: formatValue renders text-format output" {
    var buf: [64]u8 = undefined;
    const cases = [_]struct { v: value.Value, want: []const u8 }{
        .{ .v = .{ .int4 = -7 }, .want = "-7" },
        .{ .v = .{ .uint8 = 42 }, .want = "42" },
        .{ .v = .{ .bool_ = true }, .want = "t" },
        .{ .v = .{ .bool_ = false }, .want = "f" },
        .{ .v = .{ .text = "hello" }, .want = "hello" },
    };
    for (cases) |c| {
        var w = std.Io.Writer.fixed(&buf);
        try value.formatValue(&w, c.v);
        try testing.expectEqualStrings(c.want, w.buffered());
    }
}

test "sql.value: pgTypeOid widens unsigned ints" {
    try testing.expectEqual(pgwire.oid.bool_, value.pgTypeOid(.bool_));
    try testing.expectEqual(pgwire.oid.int4, value.pgTypeOid(.int4));
    try testing.expectEqual(pgwire.oid.int4, value.pgTypeOid(.uint2)); // widen
    try testing.expectEqual(pgwire.oid.int8, value.pgTypeOid(.uint4)); // widen
    try testing.expectEqual(pgwire.oid.numeric, value.pgTypeOid(.uint8)); // widen
    try testing.expectEqual(pgwire.oid.text, value.pgTypeOid(.text));
    try testing.expectEqual(pgwire.oid.text, value.pgTypeOid(.date)); // no dedicated oid yet
}
