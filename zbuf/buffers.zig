// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const schema = @import("schema");

pub const Cell = schema.Table(&.{
    .{ .id = 1, .name = "tag", .ty = schema.U32 },
    .{ .id = 2, .name = "data", .ty = schema.Bytes },
    .{ .id = 3, .name = "cell_flags", .ty = schema.U8 },
    .{ .name = "cell_hot", .ty = schema.Bool(.{ .field_id = 3, .bit = 0 }) },
});

// Mutually-exclusive lifecycle state — encoded as a single u8 enum slot.
pub const Lifecycle = schema.EnumDef(&.{
    .{ .name = "active", .tag = 0 },
    .{ .name = "draining", .tag = 1 },
    .{ .name = "bootstrapping", .tag = 2 },
    .{ .name = "retiring", .tag = 3 },
});

pub const Row = schema.Table(&.{
    .{ .id = 1, .name = "name", .ty = schema.Bytes },
    .{ .id = 2, .name = "value", .ty = schema.U64 },
    .{ .id = 3, .name = "cells", .ty = schema.Array(Cell) },
    .{ .id = 4, .name = "row_cell", .ty = Cell },
    // Independent flag bits — can co-exist (e.g. row may be both pinned AND deleted).
    .{ .id = 5, .name = "flags", .ty = schema.U64 },
    .{ .name = "is_pinned", .ty = schema.Bool(.{ .field_id = 5, .bit = 0 }) },
    .{ .name = "is_deleted", .ty = schema.Bool(.{ .field_id = 5, .bit = 1 }) },
    .{ .id = 6, .name = "lifecycle", .ty = Lifecycle },
    .{ .id = 7, .name = "u32_values", .ty = schema.Array(schema.U32) },
    .{ .id = 8, .name = "u64_values", .ty = schema.Array(schema.U64) },
});

pub const Table = schema.Table(&.{
    .{ .id = 1, .name = "rows", .ty = schema.Array(Row) },
    .{ .id = 2, .name = "payload", .ty = schema.Bytes },
    .{ .id = 3, .name = "count", .ty = schema.U32 },
    .{ .id = 4, .name = "table_row", .ty = Row },
});

pub const U32ArrayOnly = schema.Table(&.{
    .{ .id = 1, .name = "values", .ty = schema.Array(schema.U32) },
});

pub const U64ArrayOnly = schema.Table(&.{
    .{ .id = 1, .name = "values", .ty = schema.Array(schema.U64) },
});

pub const U8ArrayOnly = schema.Table(&.{
    .{ .id = 1, .name = "values", .ty = schema.Array(schema.U8) },
});

pub const U16ArrayOnly = schema.Table(&.{
    .{ .id = 1, .name = "values", .ty = schema.Array(schema.U16) },
});

// Packed records (schema.Packed), shaped like an SST block: a fixed
// version header, an entry of a prefix-compressed key with its
// versions and their bytes running to the entry's end, and a block of
// entry offsets followed by the entries region.
pub const VersionHdr = schema.Packed(&.{
    .{ .id = 1, .name = "ts", .ty = schema.U64 },
    .{ .id = 2, .name = "meta", .ty = schema.U8 },
    .{ .id = 3, .name = "off", .ty = schema.U32 },
});

pub const Entry = schema.Packed(&.{
    .{ .id = 1, .name = "key_overlap", .ty = schema.U16 },
    .{ .id = 2, .name = "key_diff", .ty = schema.Bytes, .len = .u16 },
    .{ .id = 3, .name = "versions", .ty = schema.Array(VersionHdr) },
    .{ .id = 4, .name = "values", .ty = schema.Bytes, .len = .rest },
});

// Entries of varying size behind their end-offset table, with a field
// after them to show the table also bounds the region.
pub const Block = schema.Packed(&.{
    .{ .id = 1, .name = "entries", .ty = schema.Array(Entry) },
    .{ .id = 2, .name = "crc", .ty = schema.U64 },
});

// A fixed record nested in another, an enum, and every prefix width.
pub const Point = schema.Packed(&.{
    .{ .id = 1, .name = "x", .ty = schema.U32 },
    .{ .id = 2, .name = "y", .ty = schema.U32 },
});

pub const Shape = schema.Packed(&.{
    .{ .id = 1, .name = "lifecycle", .ty = Lifecycle },
    .{ .id = 2, .name = "origin", .ty = Point },
    .{ .id = 3, .name = "name", .ty = schema.Bytes, .len = .u8 },
    .{ .id = 4, .name = "corners", .ty = schema.Array(Point), .len = .u8 },
    .{ .id = 5, .name = "weights", .ty = schema.Array(schema.U16) },
    .{ .id = 6, .name = "note", .ty = schema.Bytes },
});

pub const records: []const schema.Named = &.{
    .{ .name = "Cell", .ty = Cell, .id = 1 },
    .{ .name = "Row", .ty = Row, .id = 2 },
    .{ .name = "Table", .ty = Table, .id = 3 },
    .{ .name = "U32ArrayOnly", .ty = U32ArrayOnly, .id = 4 },
    .{ .name = "U64ArrayOnly", .ty = U64ArrayOnly, .id = 5 },
    .{ .name = "U16ArrayOnly", .ty = U16ArrayOnly, .id = 6 },
    .{ .name = "U8ArrayOnly", .ty = U8ArrayOnly, .id = 7 },

    // Packed records: no message id.
    .{ .name = "VersionHdr", .ty = VersionHdr },
    .{ .name = "Entry", .ty = Entry },
    .{ .name = "Block", .ty = Block },
    .{ .name = "Point", .ty = Point },
    .{ .name = "Shape", .ty = Shape },
};
