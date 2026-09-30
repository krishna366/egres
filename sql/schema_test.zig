// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const sql = @import("sql");
const Type = sql.Type;
const LogicalColumn = sql.LogicalColumn;
const LogicalSchema = sql.LogicalSchema;
const PhysicalSchema = sql.PhysicalSchema;

fn col(id: u32, ty: Type) LogicalColumn {
    return .{ .column_id = id, .name = "c", .ty = ty };
}

fn expectSameLayout(a: PhysicalSchema, b: PhysicalSchema) !void {
    try testing.expectEqual(a.var_offset_array_start, b.var_offset_array_start);
    try testing.expectEqual(a.var_data_start, b.var_data_start);
    try testing.expectEqual(a.n_varlen, b.n_varlen);
    try testing.expectEqual(a.columns.len, b.columns.len);
    for (a.columns, b.columns) |ca, cb| {
        try testing.expectEqual(ca.column_id, cb.column_id);
        try testing.expectEqual(ca.ty, cb.ty);
        try testing.expectEqual(ca.null_byte_index, cb.null_byte_index);
        try testing.expectEqual(ca.null_bit_index, cb.null_bit_index);
        try testing.expectEqual(ca.physical_offset, cb.physical_offset);
        try testing.expectEqual(ca.var_offset_index, cb.var_offset_index);
    }
}

test "sql.schema: compile orders by column_id, packs fixed, indexes varlen" {
    const alloc = testing.allocator;
    // Defined out of column_id order, interleaving fixed and varlen.
    const cols = [_]LogicalColumn{
        col(3, .text), col(1, .int4), col(4, .int8), col(2, .bytea),
    };
    var ps = try sql.compile(alloc, .{ .columns = &cols, .current_version = 1 }, 1);
    defer ps.deinit(alloc);

    // Array is in ascending column_id order.
    try testing.expectEqual(@as(usize, 4), ps.columns.len);
    try testing.expectEqual(@as(u32, 1), ps.columns[0].column_id);
    try testing.expectEqual(@as(u32, 2), ps.columns[1].column_id);
    try testing.expectEqual(@as(u32, 3), ps.columns[2].column_id);
    try testing.expectEqual(@as(u32, 4), ps.columns[3].column_id);

    // n=4 → 1 bitmap byte → fixed region starts at offset 3. Fixed columns
    // packed in column_id order: int4(id1)@3, int8(id4)@7.
    try testing.expectEqual(@as(u32, 3), ps.columns[0].physical_offset); // int4
    try testing.expectEqual(@as(u32, 7), ps.columns[3].physical_offset); // int8
    // Varlen columns get directory indices in column_id order: bytea(id2)=0,
    // text(id3)=1.
    try testing.expectEqual(@as(u32, 0), ps.columns[1].var_offset_index); // bytea
    try testing.expectEqual(@as(u32, 1), ps.columns[2].var_offset_index); // text
    try testing.expectEqual(@as(u16, 2), ps.n_varlen);

    // null bits run 0..3, all in byte 2 (just past the version stamp).
    for (ps.columns, 0..) |c, i| {
        try testing.expectEqual(@as(u32, 2), c.null_byte_index);
        try testing.expectEqual(@as(u3, @intCast(i)), c.null_bit_index);
    }

    // fixed region [3,15) size 12; directory at 15 (2 entries), data at 23.
    try testing.expectEqual(@as(u32, 15), ps.var_offset_array_start);
    try testing.expectEqual(@as(u32, 23), ps.var_data_start);
}

test "sql.schema: old version layout reconstructs from current catalog" {
    const alloc = testing.allocator;
    const a = col(1, .int4);
    const b = col(2, .text);
    const c = LogicalColumn{ .column_id = 3, .name = "c", .ty = .int8, .added_in_version = 2 };
    const current = [_]LogicalColumn{ a, b, c };

    // v1 layout reconstructed from the v2 catalog == v1 layout from a catalog
    // that only ever had A,B (C is invisible at v1).
    var v1_from_current = try sql.compile(alloc, .{ .columns = &current, .current_version = 2 }, 1);
    defer v1_from_current.deinit(alloc);
    const only_ab = [_]LogicalColumn{ a, b };
    var v1_orig = try sql.compile(alloc, .{ .columns = &only_ab, .current_version = 1 }, 1);
    defer v1_orig.deinit(alloc);
    try expectSameLayout(v1_from_current, v1_orig);

    // At v2, C is appended without moving A or B.
    var v2 = try sql.compile(alloc, .{ .columns = &current, .current_version = 2 }, 2);
    defer v2.deinit(alloc);
    try testing.expectEqual(v1_from_current.find(1).?.physical_offset, v2.find(1).?.physical_offset);
    try testing.expectEqual(@as(u32, 0), v2.find(2).?.var_offset_index);
    try testing.expectEqual(@as(u32, 7), v2.find(3).?.physical_offset); // int8 after int4
}

test "sql.schema: dropped column retained before, reclaimed at drop version" {
    const alloc = testing.allocator;
    const a = col(1, .int4);
    const b = LogicalColumn{ .column_id = 2, .name = "b", .ty = .int4, .dropped_in_version = 3 };
    const c = LogicalColumn{ .column_id = 3, .name = "c", .ty = .int4, .added_in_version = 2 };
    const cols = [_]LogicalColumn{ a, b, c };
    const cat = LogicalSchema{ .columns = &cols, .current_version = 3 };

    // v2: A,B,C all live and packed in order.
    var v2 = try sql.compile(alloc, cat, 2);
    defer v2.deinit(alloc);
    try testing.expectEqual(@as(usize, 3), v2.columns.len);
    try testing.expectEqual(@as(u32, 3), v2.find(1).?.physical_offset);
    try testing.expectEqual(@as(u32, 7), v2.find(2).?.physical_offset);
    try testing.expectEqual(@as(u32, 11), v2.find(3).?.physical_offset);

    // v3: B dropped. C reclaims B's old slot (offset 7) — which is exactly
    // why a v2 row must be read with the v2 schema, never v3.
    var v3 = try sql.compile(alloc, cat, 3);
    defer v3.deinit(alloc);
    try testing.expectEqual(@as(usize, 2), v3.columns.len);
    try testing.expect(v3.find(2) == null);
    try testing.expectEqual(@as(u32, 3), v3.find(1).?.physical_offset);
    try testing.expectEqual(@as(u32, 7), v3.find(3).?.physical_offset);
}
