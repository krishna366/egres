const std = @import("std");
const testing = std.testing;
const sql = @import("sql");
const catalog = sql.catalog;
const Type = sql.Type;
const LogicalColumn = sql.LogicalColumn;

fn col(id: u32, name: []const u8, ty: Type, added: u16, dropped: ?u16) LogicalColumn {
    return .{
        .column_id = id,
        .name = name,
        .ty = ty,
        .added_in_version = added,
        .dropped_in_version = dropped,
    };
}

test "sql.catalog: the fixed catalog schema has the six expected columns" {
    const alloc = testing.allocator;
    var cps = try sql.compile(alloc, catalog.logical_schema, catalog.logical_schema.current_version);
    defer cps.deinit(alloc);
    try testing.expectEqual(@as(usize, 6), cps.columns.len);
    try testing.expectEqual(@as(u16, 1), cps.n_varlen); // only `name` is varlen
}

test "sql.catalog: a table's logical schema round-trips through catalog rows" {
    const alloc = testing.allocator;

    // A user table that has evolved across versions: id+name from v1, a legacy
    // 'age' int4 dropped at v3, and 'email' added at v2.
    const user_table_id: u32 = 7;
    const original = [_]LogicalColumn{
        col(1, "id", .int8, 1, null),
        col(2, "name", .text, 1, null),
        col(3, "age", .int4, 1, 3), // dropped at v3
        col(4, "email", .text, 2, null), // added at v2
    };

    // The catalog's own (fixed) physical schema, compiled once.
    var cps = try sql.compile(alloc, catalog.logical_schema, 1);
    defer cps.deinit(alloc);

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);
    var r: sql.Row = .{};
    defer r.deinit(alloc);

    // Decoded columns' names borrow the reused row buffer, so dupe them into a
    // side list that outlives it.
    var recovered: std.ArrayList(LogicalColumn) = .empty;
    defer recovered.deinit(alloc);
    var names: std.ArrayList([]u8) = .empty;
    defer {
        for (names.items) |n| alloc.free(n);
        names.deinit(alloc);
    }

    for (original) |c| {
        const vals = catalog.encodeColumn(user_table_id, c);
        const bytes = try sql.marshalRow(alloc, &cps, &vals, &buf);
        try sql.unmarshalRow(alloc, &cps, bytes, &[_]u32{}, &r);

        const tc = try catalog.decodeColumn(&r);
        try testing.expectEqual(user_table_id, tc.table_id);

        const name_copy = try alloc.dupe(u8, tc.column.name);
        try names.append(alloc, name_copy);
        var restored = tc.column;
        restored.name = name_copy;
        try recovered.append(alloc, restored);
    }

    // Every column comes back field-for-field.
    try testing.expectEqual(original.len, recovered.items.len);
    for (original, recovered.items) |want, got| {
        try testing.expectEqual(want.column_id, got.column_id);
        try testing.expectEqualStrings(want.name, got.name);
        try testing.expectEqual(want.ty, got.ty);
        try testing.expectEqual(want.added_in_version, got.added_in_version);
        try testing.expectEqual(want.dropped_in_version, got.dropped_in_version);
    }

    // Reconstructing the schema derives current_version = highest add/drop = 3.
    const restored_schema = catalog.reconstruct(recovered.items);
    try testing.expectEqual(@as(u16, 3), restored_schema.current_version);
}

test "sql.catalog: current_version derives from the highest add/drop" {
    try testing.expectEqual(@as(u16, 1), catalog.deriveCurrentVersion(&.{}));
    const cols = [_]LogicalColumn{
        col(1, "a", .int4, 1, null),
        col(2, "b", .int4, 2, 5), // dropped at v5 → highest
        col(3, "c", .int4, 4, null),
    };
    try testing.expectEqual(@as(u16, 5), catalog.deriveCurrentVersion(&cols));
}

test "sql.catalog: row keys sort by (table_id, column_id) and share a table prefix" {
    const k_7_2 = catalog.encodeKey(7, 2);
    const k_7_10 = catalog.encodeKey(7, 10);
    const k_8_1 = catalog.encodeKey(8, 1);

    // Big-endian keys ⇒ byte order matches numeric order: column 2 < 10,
    // and table 7 < 8 regardless of column.
    try testing.expect(std.mem.order(u8, &k_7_2, &k_7_10) == .lt);
    try testing.expect(std.mem.order(u8, &k_7_10, &k_8_1) == .lt);

    // A table prefix selects exactly that table's rows.
    const p7 = catalog.tablePrefix(7);
    try testing.expect(std.mem.startsWith(u8, &k_7_2, &p7));
    try testing.expect(std.mem.startsWith(u8, &k_7_10, &p7));
    try testing.expect(!std.mem.startsWith(u8, &k_8_1, &p7));

    // Keys decode back to their components.
    const d = try catalog.decodeKey(&k_7_10);
    try testing.expectEqual(@as(u32, 7), d.table_id);
    try testing.expectEqual(@as(u32, 10), d.column_id);
}
