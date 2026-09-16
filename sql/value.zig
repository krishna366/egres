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
/// The runtime column (table.zig imports this file too; Zig allows the
/// cycle). formatValue takes it because a char(n) cell is a plain text
/// value and only the column knows to pad it.
const Column = @import("table.zig").Column;

/// A borrowed byte string (names, row bytes, encoded keys); see
/// `utils.String`.
pub const String = utils.String;

/// SQL column types this codec stores. Fixed-width types live in the row's
/// fixed region at constant offsets; varlen types live in the trailing data
/// region, sliced via the row's offset directory.
///
/// Storage is native little-endian (see row.zig). Temporal types use the
/// Postgres integer-datetime widths but the Unix epoch: `date` = int4 days
/// and `timestamp`/`timestamptz` = int8 µs, both since 1970-01-01 UTC;
/// `time` = int8 µs since midnight; `interval` = int8 µs + int4 days +
/// int4 months (16 bytes). Postgres counts from 2000-01-01 in its binary
/// wire format only — that shift belongs to the wire encoder
/// (pgwire.pg_epoch_unix_us), never to storage. `uuid` is 16 opaque
/// bytes — not byte-swapped, since it isn't an integer.
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
    /// `char(n)`: text stored stripped of trailing spaces, padded back
    /// to n codepoints on output. Cells are `Value.text`; the column's
    /// `type_mod` (n) drives the boundary behavior, never the value tag.
    bpchar,
    /// `varchar(n)`: text with a length check on the way in. Bare
    /// `varchar` is `.text`.
    varchar,

    /// Byte width of a fixed-width type, or null for a varlen type.
    pub inline fn fixedWidth(self: Type) ?u8 {
        return switch (self) {
            .bool_ => 1,
            .int2, .uint2 => 2,
            .int4, .uint4, .float4, .date => 4,
            .int8, .uint8, .float8, .time, .timestamp, .timestamptz => 8,
            .interval, .uuid => 16,
            .text, .bytea, .bpchar, .varchar => null,
        };
    }

    /// The three string types. Their cells are all `Value.text` and
    /// they assign to and concatenate with each other freely.
    pub inline fn isString(self: Type) bool {
        return self == .text or self == .bpchar or self == .varchar;
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
        if (self.isString()) return to.isString();
        return false;
    }

    /// Resolve a DDL type name to a storage Type, or null if unsupported.
    /// libpg_query normalizes pg_catalog spellings to internal names before
    /// we see them (int/integer→int4, bigint→int8, real→float4, boolean→
    /// bool, character varying→varchar, char/character→bpchar), and those
    /// internal names ARE our variant names — so the enum itself is the
    /// lookup table. Two spellings can't come from it: `bool` (the variant
    /// is bool_; `bool` is a Zig primitive) and `varchar`, which is `.text`
    /// here; the DDL parser turns it into `.varchar` when a length is
    /// given (the length lives in the column's `type_mod`).
    pub fn fromName(name: String) ?Type {
        if (std.mem.eql(u8, name, "bool")) return .bool_;
        if (std.mem.eql(u8, name, "varchar")) return .text;
        if (std.mem.eql(u8, name, "char") or std.mem.eql(u8, name, "character")) return .bpchar;
        return std.meta.stringToEnum(Type, name);
    }

    /// The largest `char(n)` / `varchar(n)` Postgres accepts.
    pub const max_type_mod: u32 = 10_485_760;
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
/// is marshaled). NULL is handled by the caller, not here. Numeric/bool/
/// date/timestamp parse failures surface as errors (the executor maps them
/// to 22P02); types this slice can't parse (time, interval, uuid) return
/// `error.UnsupportedType` (→ 0A000).
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
        .text, .bpchar, .varchar => .{ .text = text },
        .bytea => .{ .bytea = text },
        .timestamp => .{ .timestamp = try parseTimestamp(text, false) },
        .timestamptz => .{ .timestamptz = try parseTimestamp(text, true) },
        .date => .{ .date = try parseDate(text) },
        .time, .interval, .uuid => error.UnsupportedType,
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

/// Check and normalize a string on its way into a column of string type
/// `ty` with length `type_mod` (n; null = no length check). Every string
/// type must be valid UTF-8, as in Postgres. `bpchar` strips trailing
/// spaces (only U+0020, which is never a continuation byte, so the strip
/// needs no decoding) and, like `varchar`, refuses more than n codepoints.
/// A `bpchar` value whose excess is only spaces is accepted. The result
/// is a subslice of `s`.
pub fn coerceString(s: String, ty: Type, type_mod: ?u32) error{ InvalidUtf8, StringTooLong }!String {
    std.debug.assert(ty.isString());
    if (!std.unicode.utf8ValidateSlice(s)) return error.InvalidUtf8;

    // varchar maintains its trailing spaces. bpchar drops it.
    const out = if (ty == .bpchar) std.mem.trimEnd(u8, s, " ") else s;
    if (ty == .text) return out;

    const n = type_mod orelse return out;
    if (out.len > n and countCodepoints(out) > n) return error.StringTooLong;
    return out;
}

/// Codepoints in valid UTF-8 (`coerceString` validated it).
fn countCodepoints(s: String) usize {
    return std.unicode.utf8CountCodepoints(s) catch unreachable;
}

/// Write a `bpchar` cell the way Postgres sends it: the stored (stripped)
/// text padded with spaces to `n` codepoints.
fn writePadded(w: *std.Io.Writer, s: String, n: u32) !void {
    try w.writeAll(s);
    const have = countCodepoints(s);
    if (have < n) try w.splatByteAll(' ', n - have);
}

/// Render a non-null cell as its text-format wire representation. `col` is
/// the cell's column when there is one: it decides the rendering where the
/// value alone cannot, since a `char(n)` cell is a plain `.text` stored
/// stripped and comes back padded to the column's length. null (a computed
/// cell such as count(*)) renders the value as is. The caller emits SQL
/// NULL as `proto.DataValue.null_value` and never passes it here.
pub fn formatValue(w: *std.Io.Writer, col: ?*const Column, v: Value) !void {
    if (col) |c| if (c.ty == .bpchar) {
        return writePadded(w, v.text, c.type_mod orelse 1);
    };

    switch (v) {
        .null_value => {}, // caller handles null out-of-band
        .bool_ => |b| try w.writeAll(if (b) "t" else "f"),
        .text, .bytea => |s| try w.writeAll(s),
        // Every numeric payload renders via {d}; `inline` binds the capture
        // to each tag's concrete type. time/interval/uuid can't be inserted
        // this slice, so a live value never actually reaches those tags —
        // they degrade to the raw fields rather than crash.
        // TODO: proper Postgres text formats for them.
        inline .int2,
        .int4,
        .int8,
        .uint2,
        .uint4,
        .uint8,
        .float4,
        .float8,
        .time,
        => |x| try w.print("{d}", .{x}),
        .date => |days| try formatDate(w, days),
        .timestamp => |us| try formatTimestamp(w, us, false),
        .timestamptz => |us| try formatTimestamp(w, us, true),
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
        .timestamp => pgwire.oid.timestamp,
        .timestamptz => pgwire.oid.timestamptz,
        .date => pgwire.oid.date,
        .bpchar => pgwire.oid.bpchar,
        .varchar => pgwire.oid.varchar,
        .text, .bytea, .time, .interval, .uuid => pgwire.oid.text,
    };
}

/// The RowDescription type modifier for a column: Postgres reports a
/// `char(n)` / `varchar(n)` length as n plus its 4-byte varlena header
/// (VARHDRSZ), and -1 for every type without a modifier.
pub fn pgTypeModifier(type_mod: ?u32) i32 {
    const n = type_mod orelse return -1;
    return @as(i32, @intCast(n)) + 4;
}

// -----------------------------------------------------------------------------
// Dates and timestamps: Postgres text form ↔ int4 days / int8 Unix µs, UTC.
// -----------------------------------------------------------------------------

const us_per_sec: i64 = 1_000_000;
const us_per_day: i64 = 86_400 * us_per_sec;

/// Parse a date literal into days since 1970-01-01. Postgres's date
/// input takes anything its timestamp input does and keeps the day, so
/// this is parseTimestamp with the time of day dropped: `YYYY-MM-DD`,
/// or a full timestamp truncated to its date (a zone offset is ignored,
/// as for a plain timestamp). Anything else is `error.InvalidTimestamp`
/// (→ 22P02).
pub fn parseDate(text: String) !i32 {
    const us = try parseTimestamp(text, false);
    return @intCast(@divFloor(us, us_per_day));
}

/// Render days since 1970-01-01 as `YYYY-MM-DD`, exactly what Postgres
/// prints in the default (ISO) DateStyle.
pub fn formatDate(w: *std.Io.Writer, days: i32) !void {
    const c = civilFromDays(days);
    // Unsigned on purpose (see formatTimestamp).
    try w.print("{d:0>4}-{d:0>2}-{d:0>2}", .{
        @as(u64, @intCast(c.year)),
        @as(u64, @intCast(c.month)),
        @as(u64, @intCast(c.day)),
    });
}

/// Parse `YYYY-MM-DD[ HH:MM[:SS[.ffffff]]][Z|±HH[:MM]]` (a `T` may
/// separate date and time) into Unix µs. Only four-digit years.
/// A trailing zone offset moves a timestamptz to UTC and is ignored
/// for a plain timestamp (Postgres does the same). Fraction digits
/// past six are dropped. Anything else is `error.InvalidTimestamp`
/// (→ 22P02).
pub fn parseTimestamp(text: String, with_tz: bool) !i64 {
    var p = TsParser{ .s = std.mem.trim(u8, text, " ") };
    const year = try p.digits(4);
    try p.expect('-');
    const month = try p.digits(2);
    try p.expect('-');
    const day = try p.digits(2);
    if (month < 1 or month > 12 or day < 1 or day > daysInMonth(year, month))
        return error.InvalidTimestamp;

    var hour: i64 = 0;
    var minute: i64 = 0;
    var second: i64 = 0;
    var frac: i64 = 0;
    if (p.peekIs(' ') or p.peekIs('T') or p.peekIs('t')) {
        p.i += 1;
        hour = try p.digits(2);
        try p.expect(':');
        minute = try p.digits(2);
        if (p.peekIs(':')) {
            p.i += 1;
            second = try p.digits(2);
        }
        if (p.peekIs('.')) {
            p.i += 1;
            frac = try p.fraction();
        }
    }
    if (hour > 23 or minute > 59 or second > 59) return error.InvalidTimestamp;

    // The zone: `Z`, or a signed hour with an optional `:MM` / `MM`.
    var offset_us: i64 = 0;
    while (p.peekIs(' ')) p.i += 1;
    if (p.peekIs('Z') or p.peekIs('z')) {
        p.i += 1;
    } else if (p.peekIs('+') or p.peekIs('-')) {
        const negative = p.peekIs('-');
        p.i += 1;
        var minutes = 60 * try p.digits(2);
        if (p.peekIs(':')) p.i += 1;
        if (p.peekIsDigit()) minutes += try p.digits(2);
        offset_us = minutes * 60 * us_per_sec;
        if (negative) offset_us = -offset_us;
    }
    if (p.i != p.s.len) return error.InvalidTimestamp;

    const days = daysFromCivil(year, month, day);
    const secs = hour * 3600 + minute * 60 + second;
    var us = days * us_per_day + secs * us_per_sec + frac;
    // `12:00+02` is 10:00 UTC: local time minus its offset.
    if (with_tz) us -= offset_us;
    return us;
}

/// Render Unix µs as `YYYY-MM-DD HH:MM:SS[.ffffff]`, the fraction
/// without trailing zeros, plus `+00` for a timestamptz — exactly what
/// Postgres prints in a UTC session.
pub fn formatTimestamp(w: *std.Io.Writer, unix_us: i64, with_tz: bool) !void {
    const c = civilFromDays(@divFloor(unix_us, us_per_day));
    const day_us = @mod(unix_us, us_per_day);
    const secs = @divFloor(day_us, us_per_sec);
    const frac = @mod(day_us, us_per_sec);
    // Unsigned on purpose: a `0` fill on a signed integer prints an
    // explicit `+` sign (`0000+5`). Every field here is non-negative.
    try w.print("{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{
        @as(u64, @intCast(c.year)),
        @as(u64, @intCast(c.month)),
        @as(u64, @intCast(c.day)),
        @as(u64, @intCast(@divFloor(secs, 3600))),
        @as(u64, @intCast(@mod(@divFloor(secs, 60), 60))),
        @as(u64, @intCast(@mod(secs, 60))),
    });
    if (frac != 0) {
        var buf: [6]u8 = undefined;
        const digits = try std.fmt.bufPrint(&buf, "{d:0>6}", .{@as(u64, @intCast(frac))});
        try w.writeByte('.');
        try w.writeAll(std.mem.trimEnd(u8, digits, "0"));
    }
    if (with_tz) try w.writeAll("+00");
}

/// A cursor over the timestamp text: fixed-width digit fields and
/// one-byte separators.
const TsParser = struct {
    s: String,
    i: usize = 0,

    fn peekIs(self: *const TsParser, c: u8) bool {
        return self.i < self.s.len and self.s[self.i] == c;
    }

    fn peekIsDigit(self: *const TsParser) bool {
        return self.i < self.s.len and std.ascii.isDigit(self.s[self.i]);
    }

    fn expect(self: *TsParser, c: u8) !void {
        if (!self.peekIs(c)) return error.InvalidTimestamp;
        self.i += 1;
    }

    /// Exactly `n` decimal digits, as a number.
    fn digits(self: *TsParser, n: usize) !i64 {
        if (self.i + n > self.s.len) return error.InvalidTimestamp;
        var v: i64 = 0;
        for (self.s[self.i..][0..n]) |c| {
            if (!std.ascii.isDigit(c)) return error.InvalidTimestamp;
            v = v * 10 + (c - '0');
        }
        self.i += n;
        return v;
    }

    /// The digits after the point, scaled to µs: `.5` is 500000,
    /// digits past the sixth are consumed and dropped.
    fn fraction(self: *TsParser) !i64 {
        var us: i64 = 0;
        var n: usize = 0;
        while (self.peekIsDigit()) : (self.i += 1) {
            if (n < 6) us = us * 10 + (self.s[self.i] - '0');
            n += 1;
        }
        if (n == 0) return error.InvalidTimestamp;
        while (n < 6) : (n += 1) us *= 10;
        return us;
    }
};

fn isLeapYear(y: i64) bool {
    return @rem(y, 4) == 0 and (@rem(y, 100) != 0 or @rem(y, 400) == 0);
}

fn daysInMonth(y: i64, m: i64) i64 {
    return switch (m) {
        1, 3, 5, 7, 8, 10, 12 => 31,
        4, 6, 9, 11 => 30,
        else => if (isLeapYear(y)) 29 else 28,
    };
}

/// Proleptic-Gregorian date → days since 1970-01-01, Howard Hinnant's
/// `days_from_civil`. Closed form, no loops, no tables; it and its
/// inverse below are the whole calendar (std.time.epoch goes only from
/// epoch to civil, and only for unsigned years from 1970 on).
///
/// Two ideas make it a formula:
///
/// 1. The year is counted from March 1, not January 1, so February is
///    the LAST month and the leap day is the last day of the year. Then
///    every month's day-of-year is a fixed number that does not depend
///    on whether the year is leap. `y` is the March-based year: a date
///    in January or February belongs to the previous one.
/// 2. The Gregorian calendar repeats every 400 years exactly: 400 * 365
///    + 97 leap days = 146,097 days. An `era` is one such block; every
///    era is the same length, so the day count is `era * 146097` plus
///    the day within the era.
///
/// Symbols (all integers, days unless said otherwise):
///   y     March-based year: `year`, or `year - 1` for January/February.
///   era   Which 400-year block `y` is in, numbered from 0000-03-01:
///         `y / 400`, rounding DOWN also for negative years (hence the
///         `- 399` before a truncating divide). 2026 → era 5.
///   yoe   Year of era, `y - era * 400`, in 0..399. 2026 → 26.
///   mp    Month index in the March-based year, in 0..11: March is 0,
///         February is 11. `month - 3`, or `month + 9` for Jan/Feb.
///   doy   Day of the March-based year, in 0..365. `(153 * mp + 2) / 5`
///         is the day month `mp` starts on. March..July run 31,30,31,
///         30,31 = 153 days and August..December 31,31,30,31,30 = 153,
///         so month starts advance by 30.6 days on average, and
///         `floor(30.6 * mp + 0.4)` lands on every start exactly (0, 31,
///         61, 92, ... 306 for January, 337 for February); the integer
///         form is that expression scaled by 5. Then `+ day - 1` walks
///         into the month.
///   doe   Day of era, in 0..146096: `yoe * 365` plus one leap day per
///         four years, minus one per hundred (the 100-year rule); the
///         400-year leap day needs no term because `yoe` never reaches
///         400, and the era boundary itself accounts for it.
///   719468  Days from 0000-03-01 (day 0 of era 0) to 1970-01-01, the
///         shift onto the Unix epoch.
fn daysFromCivil(year: i64, month: i64, day: i64) i64 {
    const y = if (month <= 2) year - 1 else year;
    const era = @divTrunc(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400;
    const mp = if (month > 2) month - 3 else month + 9;
    const doy = @divTrunc(153 * mp + 2, 5) + day - 1;
    const doe = yoe * 365 + @divTrunc(yoe, 4) - @divTrunc(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}

const Civil = struct { year: i64, month: i64, day: i64 };

/// The inverse of daysFromCivil, Hinnant's `civil_from_days`: the same
/// symbols, recovered in the opposite order. Undo the epoch shift, split
/// off the era, then the year within it, then the month, then the day.
///
///   z     Days since 0000-03-01: `days + 719468`.
///   era   `z / 146097`, rounding down for negative `z` (the `- 146096`
///         before the truncating divide does that).
///   doe   Day of era, `z - era * 146097`, in 0..146096.
///   leaps The leap days that fall before `doe` within the era, so they
///         can be removed before dividing by 365: one per 1460 days
///         (four years), take back one per 36524 days (a century), add
///         back the one at day 146096 (the era's final, 400th-year leap
///         day, which the century rule wrongly removed).
///   yoe   Year of era: `(doe - leaps) / 365`, in 0..399.
///   doy   Day of the March-based year: `doe` minus the days of the
///         `yoe` complete years before it (the same `365 * yoe + yoe/4
///         - yoe/100` as in the forward direction).
///   mp    Month index from the day of year, `(5 * doy + 2) / 153`: the
///         forward formula `(153 * mp + 2) / 5` inverted.
///   day   `doy` minus the start day of month `mp`, plus 1 to be 1-based.
///   month mp back to a calendar month: `mp + 3` for March..December,
///         `mp - 9` for January and February.
///   year  `yoe + era * 400`, plus 1 when the month is January or
///         February, since those belong to the NEXT calendar year.
fn civilFromDays(days: i64) Civil {
    const z = days + 719_468;
    const era = @divTrunc(if (z >= 0) z else z - 146_096, 146_097);
    const doe = z - era * 146_097;
    const leaps = @divTrunc(doe, 1460) - @divTrunc(doe, 36_524) +
        @divTrunc(doe, 146_096);
    const yoe = @divTrunc(doe - leaps, 365);
    const doy = doe - (365 * yoe + @divTrunc(yoe, 4) - @divTrunc(yoe, 100));
    const mp = @divTrunc(5 * doy + 2, 153);
    const day = doy - @divTrunc(153 * mp + 2, 5) + 1;
    const month = if (mp < 10) mp + 3 else mp - 9;
    const year = yoe + era * 400 + @as(i64, if (month <= 2) 1 else 0);
    return .{ .year = year, .month = month, .day = day };
}

test {
    std.testing.refAllDecls(@This());
}
