const std = @import("std");
const testing = std.testing;
const sql = @import("sql");

const key = sql.key;
const Value = sql.Value;

test "sql.key: egid encodes big-endian and round-trips in key order" {
    const a = key.encodeEgid(1);
    const b = key.encodeEgid(256);
    // Big-endian ⇒ byte order matches numeric order.
    try testing.expect(std.mem.order(u8, &a, &b) == .lt);
    try testing.expectEqual(@as(?u64, 1), key.decodeEgid(&a));
    try testing.expectEqual(@as(?u64, 256), key.decodeEgid(&b));
    // Non-8-byte primaries aren't egid-shaped.
    try testing.expectEqual(@as(?u64, null), key.decodeEgid("abc"));
    try testing.expectEqual(@as(?u64, null), key.decodeEgid(""));
}

// Encode one value into an owned buffer.
fn enc(v: Value) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(testing.allocator);
    try key.encodeIndexKey(testing.allocator, v, &buf);
    return buf.toOwnedSlice(testing.allocator);
}

// Assert each value encodes strictly above its predecessor.
fn expectAscending(vals: []const Value) !void {
    var prev: ?[]u8 = null;
    defer if (prev) |p| testing.allocator.free(p);
    for (vals) |v| {
        const cur = try enc(v);
        if (prev) |p| {
            try testing.expect(std.mem.order(u8, p, cur) == .lt);
            testing.allocator.free(p);
        }
        prev = cur;
    }
}

fn expectSameKey(x: Value, y: Value) !void {
    const kx = try enc(x);
    defer testing.allocator.free(kx);
    const ky = try enc(y);
    defer testing.allocator.free(ky);
    try testing.expectEqualSlices(u8, kx, ky);
}

test "sql.key: U64 sorts upper-major and round-trips" {
    const a = (key.U64{ .upper = 7, .lower = 2 }).encode();
    const b = (key.U64{ .upper = 7, .lower = 10 }).encode();
    const c = (key.U64{ .upper = 8, .lower = 1 }).encode();
    // Byte order == (upper, lower) order: lower 2 < 10 within upper 7,
    // and upper 7 < 8 regardless of lower.
    try testing.expect(std.mem.order(u8, &a, &b) == .lt);
    try testing.expect(std.mem.order(u8, &b, &c) == .lt);

    const d = key.U64.decode(&b).?;
    try testing.expectEqual(@as(u32, 7), d.upper);
    try testing.expectEqual(@as(u32, 10), d.lower);
    try testing.expectEqual(null, key.U64.decode(b[0..7]));
}

test "sql.key: signed integers sort numerically" {
    try expectAscending(&.{
        .{ .int8 = std.math.minInt(i64) },
        .{ .int8 = -1_000_000 },
        .{ .int8 = -1 },
        .{ .int8 = 0 },
        .{ .int8 = 1 },
        .{ .int8 = 1_000_000 },
        .{ .int8 = std.math.maxInt(i64) },
    });
    // Narrow widths widen losslessly — same ordering.
    try expectAscending(&.{
        .{ .int2 = std.math.minInt(i16) },
        .{ .int2 = -1 },
        .{ .int2 = 0 },
        .{ .int2 = std.math.maxInt(i16) },
    });
    // Temporal payloads are signed counts.
    try expectAscending(&.{
        .{ .date = -30_000 },
        .{ .date = 0 },
        .{ .date = 30_000 },
    });
}

test "sql.key: unsigned integers sort numerically" {
    try expectAscending(&.{
        .{ .uint8 = 0 },
        .{ .uint8 = 1 },
        .{ .uint8 = 256 },
        .{ .uint8 = std.math.maxInt(u64) },
    });
}

test "sql.key: floats sort numerically, NaN last, zeros unify" {
    const inf = std.math.inf(f64);
    try expectAscending(&.{
        .{ .float8 = -inf },
        .{ .float8 = -1.5e300 },
        .{ .float8 = -2.5 },
        .{ .float8 = -1e-300 },
        .{ .float8 = 0 },
        .{ .float8 = 1e-300 },
        .{ .float8 = 2.5 },
        .{ .float8 = 1.5e300 },
        .{ .float8 = inf },
        .{ .float8 = std.math.nan(f64) },
    });
    try expectSameKey(.{ .float8 = 0.0 }, .{ .float8 = -0.0 });
    try expectSameKey(
        .{ .float8 = std.math.nan(f64) },
        .{ .float8 = -std.math.nan(f64) },
    );
    try expectAscending(&.{
        .{ .float4 = -1.5 },
        .{ .float4 = 0 },
        .{ .float4 = 1.5 },
    });
}

test "sql.key: text and bool sort in value order" {
    try expectAscending(&.{
        .{ .text = "" },
        .{ .text = "a" },
        .{ .text = "a\x00" },
        .{ .text = "ab" },
        .{ .text = "b" },
    });
    try expectAscending(&.{ .{ .bool_ = false }, .{ .bool_ = true } });
}

test "sql.key: NULL and interval are unencodable" {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(testing.allocator);
    try testing.expectError(
        error.Unencodable,
        key.encodeIndexKey(testing.allocator, .null_value, &buf),
    );
    try testing.expectError(
        error.Unencodable,
        key.encodeIndexKey(testing.allocator, .{ .interval = .{} }, &buf),
    );
}
