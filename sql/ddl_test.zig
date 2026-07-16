const std = @import("std");
const testing = std.testing;
const sql = @import("sql");
const ddl = sql.ddl;

test "sql.ddl: CREATE/DROP DATABASE parse to typed statements" {
    const alloc = testing.allocator;

    var create = try ddl.parseDdl(alloc, "CREATE DATABASE app;");
    defer create.deinit(alloc);
    try testing.expectEqualStrings("app", create.create_database);

    var drop = try ddl.parseDdl(alloc, "drop database app");
    defer drop.deinit(alloc);
    try testing.expectEqualStrings("app", drop.drop_database);
}

test "sql.ddl: everything else is unsupported; bad SQL is a syntax error" {
    const alloc = testing.allocator;

    var sel = try ddl.parseDdl(alloc, "SELECT 1;");
    defer sel.deinit(alloc);
    try testing.expect(sel == .unsupported);

    try testing.expectError(error.SyntaxError, ddl.parseDdl(alloc, "CREATE DATABASE;"));
}

test "sql.ddl: CREATE TABLE parses columns, types, and the primary key" {
    const alloc = testing.allocator;

    var ct = try ddl.parseDdl(alloc, "CREATE TABLE t (id int PRIMARY KEY, name text);");
    defer ct.deinit(alloc);
    try testing.expectEqualStrings("t", ct.create_table.name);
    try testing.expect(!ct.create_table.if_not_exists);
    const cols = ct.create_table.columns;
    try testing.expectEqual(@as(usize, 2), cols.len);
    try testing.expectEqualStrings("id", cols[0].name);
    try testing.expectEqual(sql.Type.int4, cols[0].ty); // `int` → pg_catalog.int4
    try testing.expect(cols[0].pkey);
    try testing.expectEqualStrings("name", cols[1].name);
    try testing.expectEqual(sql.Type.text, cols[1].ty);
    try testing.expect(!cols[1].pkey);

    // IF NOT EXISTS + schema-qualified name + our unsigned custom type.
    var ine = try ddl.parseDdl(alloc, "CREATE TABLE IF NOT EXISTS public.u (a uint4);");
    defer ine.deinit(alloc);
    try testing.expectEqualStrings("u", ine.create_table.name);
    try testing.expect(ine.create_table.if_not_exists);
    try testing.expectEqual(sql.Type.uint4, ine.create_table.columns[0].ty);

    // Zero columns is legal Postgres.
    var empty = try ddl.parseDdl(alloc, "CREATE TABLE nothing ();");
    defer empty.deinit(alloc);
    try testing.expectEqual(@as(usize, 0), empty.create_table.columns.len);
}

test "sql.ddl: CREATE TABLE semantic rejections carry a reason" {
    const alloc = testing.allocator;
    const cases = [_]struct { sql: []const u8, needle: []const u8 }{
        .{ .sql = "CREATE TABLE t (a int NOT NULL);", .needle = "PRIMARY KEY" },
        .{ .sql = "CREATE TABLE t (a serial);", .needle = "serial" },
        .{ .sql = "CREATE TABLE t (a int, b int, PRIMARY KEY (a, b));", .needle = "table-level" },
        .{ .sql = "CREATE TABLE t (a int primary key, b int primary key);", .needle = "multiple PRIMARY KEY" },
        .{ .sql = "CREATE TABLE t (a int, a text);", .needle = "duplicate" },
        .{ .sql = "DROP TABLE a, b;", .needle = "multiple tables" },
    };
    for (cases) |case| {
        var stmt = try ddl.parseDdl(alloc, case.sql);
        defer stmt.deinit(alloc);
        try testing.expect(stmt == .invalid);
        try testing.expect(std.mem.indexOf(u8, stmt.invalid, case.needle) != null);
    }
}

test "sql.ddl: DROP TABLE variants" {
    const alloc = testing.allocator;

    var dt = try ddl.parseDdl(alloc, "DROP TABLE t;");
    defer dt.deinit(alloc);
    try testing.expectEqualStrings("t", dt.drop_table.name);
    try testing.expect(!dt.drop_table.missing_ok);

    // IF EXISTS + schema-qualified: the last name part is the table.
    var ie = try ddl.parseDdl(alloc, "DROP TABLE IF EXISTS public.t;");
    defer ie.deinit(alloc);
    try testing.expectEqualStrings("t", ie.drop_table.name);
    try testing.expect(ie.drop_table.missing_ok);

    // Non-table DROPs are a different feature, not a rejection.
    var di = try ddl.parseDdl(alloc, "DROP INDEX i;");
    defer di.deinit(alloc);
    try testing.expect(di == .unsupported);
}
