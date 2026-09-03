//! Binary encoders/decoders for the primitive Postgres types most
//! clients will negotiate by default. Pure-logic — operates on byte
//! buffers, no I/O. The 6 primitives covered here unblock pgx's and
//! asyncpg's "I'd rather send binary" defaults for the everyday
//! types (counters, IDs, flags, doubles). Anything else still goes
//! over text — the dispatch is per-OID and per-column, so adding
//! more types is incremental.
//!
//! Wire formats follow the PostgreSQL source of truth (`src/backend/
//! utils/adt/`); each function below cites the C function it
//! mirrors.

const std = @import("std");
const _types = @import("types.zig");

pub const DecodeError = error{
    /// Wrong number of bytes for the expected type — e.g. an int4
    /// payload that wasn't 4 bytes.
    BadLength,
    /// `bool` payload was a byte other than 0 or 1.
    BadBool,
};

// ---------- bool (OID 16) ----------
// Mirrors `boolsend` / `boolrecv` in pg_proc. One byte, 0 or 1.

pub fn encodeBool(out: []u8, v: bool) void {
    std.debug.assert(out.len == 1);
    out[0] = if (v) 1 else 0;
}

pub fn decodeBool(bytes: []const u8) DecodeError!bool {
    if (bytes.len != 1) return error.BadLength;
    return switch (bytes[0]) {
        0 => false,
        1 => true,
        else => error.BadBool,
    };
}

// ---------- int2 / int4 / int8 (OIDs 21, 23, 20) ----------
// Big-endian, two's complement. Mirrors `int2send` / `int4send` /
// `int8send` (which are just byte-swapped copies of the host int).

pub fn encodeInt2(out: []u8, v: i16) void {
    std.debug.assert(out.len == 2);
    std.mem.writeInt(i16, out[0..2], v, .big);
}

pub fn decodeInt2(bytes: []const u8) DecodeError!i16 {
    if (bytes.len != 2) return error.BadLength;
    return std.mem.readInt(i16, bytes[0..2], .big);
}

pub fn encodeInt4(out: []u8, v: i32) void {
    std.debug.assert(out.len == 4);
    std.mem.writeInt(i32, out[0..4], v, .big);
}

pub fn decodeInt4(bytes: []const u8) DecodeError!i32 {
    if (bytes.len != 4) return error.BadLength;
    return std.mem.readInt(i32, bytes[0..4], .big);
}

pub fn encodeInt8(out: []u8, v: i64) void {
    std.debug.assert(out.len == 8);
    std.mem.writeInt(i64, out[0..8], v, .big);
}

pub fn decodeInt8(bytes: []const u8) DecodeError!i64 {
    if (bytes.len != 8) return error.BadLength;
    return std.mem.readInt(i64, bytes[0..8], .big);
}

// ---------- float4 / float8 (OIDs 700, 701) ----------
// IEEE 754 binary representation, big-endian. Mirrors `float4send` /
// `float8send`, which `pq_sendfloat4`/`8` over a host-order bitcast.
// We bitcast the IEEE pattern to int and write big-endian — same
// observable bytes as Postgres on every supported platform.

pub fn encodeFloat4(out: []u8, v: f32) void {
    std.debug.assert(out.len == 4);
    const bits: u32 = @bitCast(v);
    std.mem.writeInt(u32, out[0..4], bits, .big);
}

pub fn decodeFloat4(bytes: []const u8) DecodeError!f32 {
    if (bytes.len != 4) return error.BadLength;
    const bits = std.mem.readInt(u32, bytes[0..4], .big);
    return @bitCast(bits);
}

pub fn encodeFloat8(out: []u8, v: f64) void {
    std.debug.assert(out.len == 8);
    const bits: u64 = @bitCast(v);
    std.mem.writeInt(u64, out[0..8], bits, .big);
}

pub fn decodeFloat8(bytes: []const u8) DecodeError!f64 {
    if (bytes.len != 8) return error.BadLength;
    const bits = std.mem.readInt(u64, bytes[0..8], .big);
    return @bitCast(bits);
}

/// True if we have a binary encoder/decoder for this OID. Lets
/// callers dispatch ahead of attempting a decode.
pub fn isBinarySupported(oid: _types.Oid) bool {
    return switch (oid) {
        _types.oid.bool_,
        _types.oid.int2,
        _types.oid.int4,
        _types.oid.int8,
        _types.oid.float4,
        _types.oid.float8,
        => true,
        else => false,
    };
}
