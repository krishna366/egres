//! Runtime SQL types and values for the physical row codec.
//!
//! A table's columns are known only at runtime, so — unlike zbuf's
//! comptime schemas — a row carries no per-field tags. The PhysicalSchema
//! (schema.zig) drives encode/decode. `Type` classifies a column as fixed-
//! or variable-width; `Value` is a concrete typed cell, with `null_value`
//! representing SQL NULL in band so a decoded `Row` is self-contained.

const std = @import("std");

/// SQL column types this codec stores. Fixed-width types live in the row's
/// fixed region at constant offsets; varlen types live in the trailing data
/// region, sliced via the row's offset directory.
///
/// Storage is native little-endian (see row.zig). Temporal types use the
/// Postgres integer-datetime shapes (epoch 2000-01-01): `date` = int4 days,
/// `time` = int8 µs since midnight, `timestamp`/`timestamptz` = int8 µs,
/// `interval` = int8 µs + int4 days + int4 months (16 bytes). `uuid` is 16
/// opaque bytes — not byte-swapped, since it isn't an integer.
///
/// Postgres has no unsigned integers, so `uint2`/`uint4`/`uint8` are ours,
/// stored as native-LE u16/u32/u64. On the wire each widens to the smallest
/// signed pg type that holds it — uint2→int4, uint4→int8 (both lossless,
/// native integers to the client) — except uint8, which no signed pg type
/// holds, so it is advertised as `numeric` (decimal text). The type NAME is
/// resolved by our DDL binder (libpg_query parses `uint8` as a generic type
/// name); Postgres itself never needs to know it.
pub const Type = enum {
    bool_,
    int2,
    int4,
    int8,
    uint2,
    uint4,
    uint8,
    float4,
    float8,
    date,
    time,
    timestamp,
    timestamptz,
    interval,
    uuid,
    text,
    bytea,

    /// Byte width of a fixed-width type, or null for a varlen type.
    pub inline fn fixedWidth(self: Type) ?u8 {
        return switch (self) {
            .bool_ => 1,
            .int2, .uint2 => 2,
            .int4, .uint4, .float4, .date => 4,
            .int8, .uint8, .float8, .time, .timestamp, .timestamptz => 8,
            .interval, .uuid => 16,
            .text, .bytea => null,
        };
    }

    pub inline fn isVarlen(self: Type) bool {
        return self.fixedWidth() == null;
    }
};

/// A Postgres interval: months + days + microseconds kept separate (a
/// calendar month isn't a fixed span of days). Stored as int8 µs + int4
/// days + int4 months — pg's binary field order.
pub const Interval = struct {
    micros: i64 = 0,
    days: i32 = 0,
    months: i32 = 0,
};

/// A concrete cell value, tagged by type, plus `null_value` for SQL NULL.
/// Used both as encode input and decode output. `text`/`bytea` payloads are
/// borrowed slices: on decode they point into the source row buffer
/// (zero-copy), so they stay valid only as long as that buffer does.
pub const Value = union(enum) {
    null_value,
    bool_: bool,
    int2: i16,
    int4: i32,
    int8: i64,
    uint2: u16,
    uint4: u32,
    uint8: u64,
    float4: f32,
    float8: f64,
    date: i32,
    time: i64,
    timestamp: i64,
    timestamptz: i64,
    interval: Interval,
    uuid: [16]u8,
    text: []const u8,
    bytea: []const u8,
};

/// A decoded (possibly projected) row: one Value per requested column, in
/// the caller's projection order. Reusable — pass the same Row to repeated
/// `unmarshalRow` calls and its backing store is refilled instead
/// of reallocated per row. A struct (not a bare slice) so row operations
/// (iteration, formatting, re-projection) can hang off it later.
pub const Row = struct {
    /// Backing store, reused across decodes. External code reads via the
    /// accessors below; the codec resizes and fills it.
    cells: std.ArrayList(Value) = .empty,

    pub inline fn len(self: Row) usize {
        return self.cells.items.len;
    }

    pub inline fn at(self: Row, index: usize) Value {
        return self.cells.items[index];
    }

    pub inline fn items(self: Row) []const Value {
        return self.cells.items;
    }

    /// Free the backing store. Borrowed text/bytea payloads point into the
    /// source row buffer and are not owned, so only the cell array is freed.
    pub fn deinit(self: *Row, alloc: std.mem.Allocator) void {
        self.cells.deinit(alloc);
    }
};

test {
    std.testing.refAllDecls(@This());
}
