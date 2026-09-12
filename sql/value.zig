//! Runtime SQL types and values for the physical row codec.
//!
//! A table's columns are known only at runtime, so — unlike zbuf's
//! comptime schemas — a row carries no per-field tags. The PhysicalSchema
//! (schema.zig) drives encode/decode. `Type` classifies a column as fixed-
//! or variable-width; `Value` is a concrete typed cell, with `null_value`
//! representing SQL NULL in band so a decoded `Row` is self-contained.

const std = @import("std");
const pgwire = @import("pgwire");
const utils = @import("utils");

/// A borrowed byte string (names, row bytes, encoded keys); see
/// `utils.String`.
pub const String = utils.String;

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

    /// Every integer width, signed or not.
    pub inline fn isInteger(self: Type) bool {
        return switch (self) {
            .int2, .int4, .int8, .uint2, .uint4, .uint8 => true,
            else => false,
        };
    }

    pub inline fn isFloat(self: Type) bool {
        return self == .float4 or self == .float8;
    }

    /// The types SET arithmetic computes over (expr.zig).
    pub inline fn isNumeric(self: Type) bool {
        return self.isInteger() or self.isFloat();
    }

    /// Whether a value of this type may be assigned to a column of
    /// `to` by SET: the same type; any integer into any number (the
    /// width is range-checked on assignment); a float into a float.
    /// A float into an integer column is refused (PG would round).
    pub fn assignableTo(self: Type, to: Type) bool {
        if (self == to) return true;
        if (self.isInteger()) return to.isNumeric();
        if (self.isFloat()) return to.isFloat();
        return false;
    }

    /// Resolve a DDL type name to a storage Type, or null if unsupported.
    /// libpg_query normalizes pg_catalog spellings to internal names before
    /// we see them (int/integer→int4, bigint→int8, real→float4, boolean→
    /// bool, character varying→varchar), and those internal names ARE our
    /// variant names — so the enum itself is the lookup table. Only two
    /// spellings can't come from it: `bool` (the variant is bool_; `bool`
    /// is a Zig primitive) and `varchar` (folds into text; any length
    /// modifier is dropped).
    pub fn fromName(name: String) ?Type {
        if (std.mem.eql(u8, name, "bool")) return .bool_;
        if (std.mem.eql(u8, name, "varchar")) return .text;
        return std.meta.stringToEnum(Type, name);
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
    text: String,
    bytea: String,

    /// The cell's type, or null for SQL NULL. Every non-null tag is
    /// named exactly as its Type.
    pub fn ty(self: Value) ?Type {
        return switch (self) {
            .null_value => null,
            inline else => |_, tag| @field(Type, @tagName(tag)),
        };
    }
};

/// A decoded (possibly projected) row: one Value per requested column, in
/// the caller's projection order. Reusable — pass the same Row to repeated
/// `unmarshalRow` calls and its backing store is refilled instead
/// of reallocated per row. A struct (not a bare slice) so row operations
/// (iteration, formatting, re-projection) can hang off it later.
pub const Row = struct {
    /// The cell values, reused across decodes. External code reads via
    /// the accessors below; the codec resizes and fills it.
    values: std.ArrayList(Value) = .empty,

    pub inline fn len(self: Row) usize {
        return self.values.items.len;
    }

    pub inline fn at(self: Row, index: usize) Value {
        return self.values.items[index];
    }

    /// The cells as a slice, in projection order.
    pub inline fn cells(self: Row) []const Value {
        return self.values.items;
    }

    /// Overwrite one cell — an UPDATE's SET on a decoded pre-image.
    /// The next unmarshalRow refills every cell, so the reused list is
    /// free to take it.
    pub inline fn set(self: *Row, index: usize, value: Value) void {
        self.values.items[index] = value;
    }

    /// Free the backing list. Borrowed text/bytea payloads point into the
    /// source row buffer and are not owned, so only the cell array is freed.
    pub fn deinit(self: *Row, alloc: std.mem.Allocator) void {
        self.values.deinit(alloc);
    }
};

// -----------------------------------------------------------------------------
// Text ↔ Value + wire type mapping (INSERT literals, SELECT output).
// -----------------------------------------------------------------------------

/// Parse a SQL literal into a typed cell of column type `ty`. The returned
/// `text`/`bytea` Value BORROWS `text` (the caller keeps it alive until the row
/// is marshaled). NULL is handled by the caller, not here. Numeric/bool parse
/// failures surface as errors (the executor maps them to 22P02); types this
/// slice can't parse (temporal, uuid, interval) return `error.UnsupportedType`
/// (→ 0A000).
pub fn parseValue(ty: Type, text: String) !Value {
    return switch (ty) {
        .bool_ => .{ .bool_ = try parseBool(text) },
        .int2 => .{ .int2 = try std.fmt.parseInt(i16, text, 10) },
        .int4 => .{ .int4 = try std.fmt.parseInt(i32, text, 10) },
        .int8 => .{ .int8 = try std.fmt.parseInt(i64, text, 10) },
        .uint2 => .{ .uint2 = try std.fmt.parseInt(u16, text, 10) },
        .uint4 => .{ .uint4 = try std.fmt.parseInt(u32, text, 10) },
        .uint8 => .{ .uint8 = try std.fmt.parseInt(u64, text, 10) },
        .float4 => .{ .float4 = try std.fmt.parseFloat(f32, text) },
        .float8 => .{ .float8 = try std.fmt.parseFloat(f64, text) },
        .text => .{ .text = text },
        .bytea => .{ .bytea = text },
        .date, .time, .timestamp, .timestamptz, .interval, .uuid => error.UnsupportedType,
    };
}

fn parseBool(text: String) !bool {
    // Postgres accepts several spellings; cover the common ones.
    if (eqlAny(text, &.{ "t", "true", "1", "yes", "on" })) return true;
    if (eqlAny(text, &.{ "f", "false", "0", "no", "off" })) return false;
    return error.InvalidBool;
}

fn eqlAny(text: String, options: []const String) bool {
    for (options) |o| if (std.ascii.eqlIgnoreCase(text, o)) return true;
    return false;
}

/// Render a non-null cell as its text-format wire representation. The caller
/// emits SQL NULL as `proto.DataValue.null_value` and never passes it here.
pub fn formatValue(w: *std.Io.Writer, v: Value) !void {
    switch (v) {
        .null_value => {}, // caller handles null out-of-band
        .bool_ => |b| try w.writeAll(if (b) "t" else "f"),
        .text, .bytea => |s| try w.writeAll(s),
        // Every numeric + temporal payload renders via {d}; `inline` binds the
        // capture to each tag's concrete type. Temporal/uuid can't be inserted
        // this slice, so a live value never actually reaches those tags — they
        // degrade to the raw fields rather than crash. TODO: proper Postgres
        // date/time text formats.
        inline .int2, .int4, .int8, .uint2, .uint4, .uint8, .float4, .float8, .date, .time, .timestamp, .timestamptz => |x| try w.print("{d}", .{x}),
        .interval => |iv| try w.print("{d}mon {d}d {d}us", .{ iv.months, iv.days, iv.micros }),
        .uuid => |u| try w.print("{x}", .{u}),
    }
}

/// Equality of two cells (same type expected — the caller coerces the filter
/// literal to the column's type first). NULL equals only NULL.
pub fn valuesEqual(x: Value, y: Value) bool {
    if (std.meta.activeTag(x) != std.meta.activeTag(y)) return false;
    return switch (x) {
        .null_value => true,
        .text => std.mem.eql(u8, x.text, y.text),
        .bytea => std.mem.eql(u8, x.bytea, y.bytea),
        .uuid => std.mem.eql(u8, &x.uuid, &y.uuid),
        .interval => std.meta.eql(x.interval, y.interval),
        inline else => |v, tag| v == @field(y, @tagName(tag)),
    };
}

/// The Postgres OID advertised for a column of this type. Unsigned integers
/// widen to the smallest signed pg type that holds them (uint2→int4,
/// uint4→int8, uint8→numeric); types without a dedicated OID here are sent as
/// text (formatValue renders them).
pub fn pgTypeOid(ty: Type) pgwire.Oid {
    return switch (ty) {
        .bool_ => pgwire.oid.bool_,
        .int2 => pgwire.oid.int2,
        .int4, .uint2 => pgwire.oid.int4,
        .int8, .uint4 => pgwire.oid.int8,
        .uint8 => pgwire.oid.numeric,
        .float4 => pgwire.oid.float4,
        .float8 => pgwire.oid.float8,
        .text, .bytea, .date, .time, .timestamp, .timestamptz, .interval, .uuid => pgwire.oid.text,
    };
}

test {
    std.testing.refAllDecls(@This());
}
