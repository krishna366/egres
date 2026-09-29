//! The schema DSL: a `buffers.zig` describes its records with the
//! constructors here, and the generator (gen.zig) reads the result as
//! plain values. A record type is a `Type` holding its fields; a field
//! names its `Type`; the wire layout is decided by the record's kind,
//! `Table` or `Packed`, not by the field types, which are shared.
//!
//! Everything is data: the generator walks slices and switches on
//! enums, and a schema mistake is reported with the record and field it
//! was found in. Only the enum list is gathered from the schema file's
//! declarations, so an `EnumDef` needs no registry entry.
const std = @import("std");

pub const Kind = enum {
    u8,
    u16,
    u32,
    u64,
    bytes,
    array,
    table,
    bool,
    enum_,
    /// A record with no header (see `Packed`).
    packed_,
};

/// A field's `.len`: the width of the length a `Bytes` or the count an
/// `Array` is prefixed with in a `Packed` record, or `.rest` for bytes
/// with no prefix that run to the record's end. Ignored in a `Table`,
/// whose slots are always u32.
pub const LenWidth = enum {
    u8,
    u16,
    u32,
    rest,

    pub fn bytes(self: LenWidth) u32 {
        return switch (self) {
            .u8 => 1,
            .u16 => 2,
            .u32 => 4,
            .rest => 0,
        };
    }

    pub fn max(self: LenWidth) u64 {
        return switch (self) {
            .u8 => std.math.maxInt(u8),
            .u16 => std.math.maxInt(u16),
            .u32 => std.math.maxInt(u32),
            .rest => std.math.maxInt(u64),
        };
    }

    pub fn typeName(self: LenWidth) []const u8 {
        return switch (self) {
            .u8 => "u8",
            .u16 => "u16",
            .u32 => "u32",
            .rest => unreachable,
        };
    }
};

pub const Variant = struct {
    name: []const u8,
    tag: u16,
};

/// A field's type. The scalar kinds carry nothing; an array its
/// element, a record its fields, an enum its variants, a bool the
/// scalar field whose bit it is. Two `Type`s name the same record or
/// enum when they share the same `fields` or `variants` slice, which
/// is how the generator finds a record's name.
pub const Type = struct {
    kind: Kind,
    fields: []const Field = &.{},
    child: ?*const Type = null,
    variants: []const Variant = &.{},
    bool_field_id: u32 = 0,
    bool_bit: u16 = 0,

    pub fn isScalar(self: Type) bool {
        return switch (self.kind) {
            .u8, .u16, .u32, .u64 => true,
            else => false,
        };
    }

    pub fn elem(self: Type) Type {
        return self.child.?.*;
    }

    pub fn sameRecord(self: Type, other: Type) bool {
        return self.fields.ptr == other.fields.ptr;
    }

    pub fn sameEnum(self: Type, other: Type) bool {
        return self.variants.ptr == other.variants.ptr;
    }
};

pub const Field = struct {
    /// The stable wire id, the slot order in a table and the field
    /// order in a packed record. A bool has none: it addresses a bit of
    /// another field.
    id: u32 = 0,
    name: []const u8,
    ty: Type,
    len: LenWidth = .u32,
};

/// An entry of a schema's `tables` or `packed_records` list.
pub const Named = struct {
    name: []const u8,
    ty: Type,
    /// A table's message id, in its tail. Packed records have none.
    id: u16 = 0,
};

pub const U8: Type = .{ .kind = .u8 };
pub const U16: Type = .{ .kind = .u16 };
pub const U32: Type = .{ .kind = .u32 };
pub const U64: Type = .{ .kind = .u64 };
pub const Bytes: Type = .{ .kind = .bytes };

pub fn Bool(comptime cfg: struct { field_id: u32, bit: u16 }) Type {
    return .{ .kind = .bool, .bool_field_id = cfg.field_id, .bool_bit = cfg.bit };
}

pub fn EnumDef(comptime variants: []const Variant) Type {
    return .{ .kind = .enum_, .variants = variants };
}

/// In a `Table`, a slot pointing at the elements. In a `Packed`, the
/// elements inline after a count: fixed-size elements (scalars, enums,
/// a fixed `Packed`) back to back, so element `i` is at `i × size`;
/// variable-size records behind a table of u32 end offsets, one per
/// record, the last being the region's length, so record `i` lies
/// between ends `i - 1` and `i`.
pub fn Array(comptime child: Type) Type {
    return .{ .kind = .array, .child = &child };
}

/// A record with a header: `u16 size_bytes` then one fixed slot per
/// field in id order, bytes and arrays pointing elsewhere in the
/// payload, so a reader treats a slot past the header as absent and
/// fields can be added over time. See the README for the layout.
pub fn Table(comptime fields: []const Field) Type {
    return .{ .kind = .table, .fields = fields };
}

/// A record with no header and no padding: its fields lie one after
/// another in field-id order, each exactly as wide as its value. Made
/// for the formats where every byte counts and random access is by
/// computed offset — an SST block's entries, say. The field types are
/// the `Table` ones, encoded inline: a `Bytes` is `[len][bytes]`, an
/// `Array` is `[count][elements]`, each prefix as wide as the field's
/// `.len` says (u32 unless given; `.rest` on the last `Bytes` field
/// means no prefix, the bytes run to the record's end), and a nested
/// `Packed` sits inline. Array elements and nested records must be
/// fixed size. Bools and `Table` fields are not allowed. Unlike a
/// `Table` a packed record has no field-level evolution: nothing marks
/// a field absent, so its layout changes only behind a version gate
/// the container provides (a table index's format_version).
pub fn Packed(comptime fields: []const Field) Type {
    return .{ .kind = .packed_, .fields = fields };
}

/// Bytes a field takes in a table's fixed header.
pub fn slotSize(ty: Type) u16 {
    return switch (ty.kind) {
        .u8 => 1,
        .u16 => 2,
        .u32 => 4,
        .u64 => 8,
        .bytes => 8,
        .array => 8,
        .table => 4,
        .bool => 0,
        .enum_ => 1,
        .packed_ => unreachable, // refused before layout
    };
}

/// The width in bytes of a packed field whose size is fixed: a scalar,
/// an enum, or a `Packed` made only of such fields. Null for `Bytes`,
/// `Array`, and a `Packed` holding either; also null for the kinds a
/// `Packed` refuses, which its validation names.
pub fn packedFixedSize(ty: Type) ?u32 {
    return switch (ty.kind) {
        .u8 => 1,
        .u16 => 2,
        .u32 => 4,
        .u64 => 8,
        .enum_ => 1,
        .packed_ => blk: {
            var total: u32 = 0;
            for (ty.fields) |f| {
                total += packedFixedSize(f.ty) orelse break :blk null;
            }
            break :blk total;
        },
        .bytes, .array, .table, .bool => null,
    };
}

/// The fields in wire order (ascending id, bools where they were
/// declared), as an owned slice. Refuses a duplicate id.
pub fn sortFieldsById(alloc: std.mem.Allocator, fields: []const Field) ![]Field {
    const out = try alloc.dupe(Field, fields);
    errdefer alloc.free(out);
    std.mem.sort(Field, out, {}, fieldLessThan);
    var i: usize = 1;
    while (i < out.len) : (i += 1) {
        if (out[i - 1].id > 0 and out[i].id > 0 and out[i - 1].id == out[i].id) {
            std.debug.print("duplicate field id {d} ({s})\n", .{ out[i].id, out[i].name });
            return error.InvalidSchema;
        }
    }
    return out;
}

fn fieldLessThan(_: void, a: Field, b: Field) bool {
    return a.id < b.id;
}

pub fn camelName(alloc: std.mem.Allocator, name: []const u8) ![]const u8 {
    var out_len: usize = 0;
    var upper_next = false;
    for (name) |c| {
        if (c == '_') {
            upper_next = true;
            continue;
        }
        out_len += 1;
        upper_next = false;
    }

    const out = try alloc.alloc(u8, out_len);
    var i: usize = 0;
    upper_next = false;
    for (name) |c| {
        if (c == '_') {
            upper_next = true;
            continue;
        }
        out[i] = if (upper_next) std.ascii.toUpper(c) else c;
        i += 1;
        upper_next = false;
    }
    return out;
}

pub fn camelNameCapitalized(alloc: std.mem.Allocator, name: []const u8) ![]const u8 {
    const base = try camelName(alloc, name);
    defer alloc.free(base);
    if (base.len == 0) return try alloc.alloc(u8, 0);
    var out = try alloc.alloc(u8, base.len);
    std.mem.copyForwards(u8, out, base);
    out[0] = std.ascii.toUpper(out[0]);
    return out;
}
