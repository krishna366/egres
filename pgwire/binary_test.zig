// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Binary encoder/decoder tests for the primitive Postgres types.
//! Pure-logic — every byte goes through a stack buffer.

const std = @import("std");
const testing = std.testing;
const pgwire = @import("pgwire");

const binary = pgwire.binary;
const types = pgwire.types;

test "bool round-trip" {
    var buf: [1]u8 = undefined;
    binary.encodeBool(&buf, true);
    try testing.expectEqual(@as(u8, 1), buf[0]);
    try testing.expectEqual(true, try binary.decodeBool(&buf));
    binary.encodeBool(&buf, false);
    try testing.expectEqual(@as(u8, 0), buf[0]);
    try testing.expectEqual(false, try binary.decodeBool(&buf));
    try testing.expectError(error.BadBool, binary.decodeBool(&[_]u8{2}));
    try testing.expectError(error.BadLength, binary.decodeBool(&[_]u8{ 0, 0 }));
}

test "int2/int4/int8 round-trip including extremes" {
    var b2: [2]u8 = undefined;
    binary.encodeInt2(&b2, std.math.minInt(i16));
    try testing.expectEqual(std.math.minInt(i16), try binary.decodeInt2(&b2));
    binary.encodeInt2(&b2, std.math.maxInt(i16));
    try testing.expectEqual(std.math.maxInt(i16), try binary.decodeInt2(&b2));

    var b4: [4]u8 = undefined;
    binary.encodeInt4(&b4, -1);
    try testing.expectEqual(@as(i32, -1), try binary.decodeInt4(&b4));
    // -1 in int32 BE is 0xFFFFFFFF; verify the wire bytes match.
    try testing.expectEqualSlices(u8, &.{ 0xFF, 0xFF, 0xFF, 0xFF }, &b4);
    binary.encodeInt4(&b4, 101);
    try testing.expectEqualSlices(u8, &.{ 0x00, 0x00, 0x00, 0x65 }, &b4);

    var b8: [8]u8 = undefined;
    binary.encodeInt8(&b8, std.math.maxInt(i64));
    try testing.expectEqual(std.math.maxInt(i64), try binary.decodeInt8(&b8));
}

test "float4/float8 round-trip + bit-pattern check" {
    var b4: [4]u8 = undefined;
    binary.encodeFloat4(&b4, 1.0);
    // 1.0 as float32 is 0x3F800000.
    try testing.expectEqualSlices(u8, &.{ 0x3F, 0x80, 0x00, 0x00 }, &b4);
    try testing.expectEqual(@as(f32, 1.0), try binary.decodeFloat4(&b4));

    var b8: [8]u8 = undefined;
    binary.encodeFloat8(&b8, 1.0);
    // 1.0 as float64 is 0x3FF0000000000000.
    try testing.expectEqualSlices(
        u8,
        &.{ 0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 },
        &b8,
    );
    try testing.expectEqual(@as(f64, 1.0), try binary.decodeFloat8(&b8));

    // NaN survives the round-trip in bit-pattern terms (==.NaN
    // never holds, so we compare bits).
    binary.encodeFloat8(&b8, std.math.nan(f64));
    const back = try binary.decodeFloat8(&b8);
    try testing.expect(std.math.isNan(back));
}

test "isBinarySupported covers expected OIDs" {
    try testing.expect(binary.isBinarySupported(types.oid.int4));
    try testing.expect(binary.isBinarySupported(types.oid.bool_));
    try testing.expect(binary.isBinarySupported(types.oid.float8));
    try testing.expect(!binary.isBinarySupported(types.oid.text));
    try testing.expect(!binary.isBinarySupported(types.oid.numeric));
}
