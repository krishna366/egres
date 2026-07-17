const std = @import("std");
const testing = std.testing;
const zdb = @import("zdb");
const sql = @import("sql");
const utils = @import("utils");

const Forest = zdb.forest.Forest;
const Key = zdb.key.Key;
const Item = zdb.key.Item;

const catalog = sql.catalog;
const schema = sql.schema;
const row = sql.row;
const Type = sql.Type;

fn tmpPath(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir) ![:0]u8 {
    return tmp.dir.realPathFileAlloc(utils.context.g().io, ".", alloc);
}

// Write one catalog row per column into namespace `ns`'s catalog tree, keyed
// (table_id, column_id) BE, then reconstruct the schema via loadTableSchema.
test "sql.table: load reconstructs a user table's schema from catalog rows" {
    const alloc = testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f = try Forest.open(alloc, .{ .dir = dir_path });
    defer f.close();

    const ns: u64 = 16;
    const user_table_id: u32 = 16;
    _ = try f.createTree(ns, catalog.table_id, .{ .inf = true });

    var catalog_ps = try schema.compile(alloc, catalog.logical_schema, catalog.logical_schema.current_version);
    defer catalog_ps.deinit(alloc);

    // Synthetic table: (id int4 PRIMARY KEY, name text).
    const cols = [_]schema.LogicalColumn{
        .{ .column_id = 1, .name = "id", .ty = .int4, .pkey_position = 1 },
        .{ .column_id = 2, .name = "name", .ty = .text },
    };

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);
    var items: std.ArrayList(Item) = .empty;
    defer items.deinit(alloc);
    for (cols) |c| {
        const cells = catalog.encodeColumn(user_table_id, c);
        const bytes = try row.marshalRow(alloc, &catalog_ps, &cells, &buf);
        const key = catalog.encodeKey(user_table_id, c.column_id);
        try items.append(alloc, .{
            .key = .{
                .namespace = ns,
                .table_id = catalog.table_id,
                .primary = try a.dupe(u8, &key),
                .timestamp = 1,
            },
            .value = try a.dupe(u8, bytes),
        });
    }
    try f.put(items.items);

    var t = try sql.table.loadTableSchema(alloc, f, &catalog_ps, ns, user_table_id);
    defer t.deinit();

    try testing.expectEqual(@as(usize, 2), t.columns.len);
    try testing.expectEqual(@as(u16, 1), t.version);

    try testing.expectEqualStrings("id", t.columns[0].name);
    try testing.expectEqual(Type.int4, t.columns[0].ty);
    try testing.expectEqual(@as(?u16, 1), t.columns[0].pkey_position);

    try testing.expectEqualStrings("name", t.columns[1].name);
    try testing.expectEqual(Type.text, t.columns[1].ty);
    try testing.expectEqual(@as(?u16, null), t.columns[1].pkey_position);

    // Compiled layout: two live columns, one varlen (name).
    try testing.expectEqual(@as(usize, 2), t.ps.columns.len);
    try testing.expectEqual(@as(u16, 1), t.ps.n_varlen);

    // Name resolution helper.
    try testing.expect(t.column("id") != null);
    try testing.expect(t.column("missing") == null);
}

test "sql.table: load returns UndefinedTable when no catalog rows exist" {
    const alloc = testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f = try Forest.open(alloc, .{ .dir = dir_path });
    defer f.close();

    const ns: u64 = 16;
    _ = try f.createTree(ns, catalog.table_id, .{ .inf = true });

    var catalog_ps = try schema.compile(alloc, catalog.logical_schema, catalog.logical_schema.current_version);
    defer catalog_ps.deinit(alloc);

    try testing.expectError(error.UndefinedTable, sql.table.loadTableSchema(alloc, f, &catalog_ps, ns, 999));
}
