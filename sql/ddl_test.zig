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

    // A statement kind we don't handle at all (UPDATE) is unsupported.
    var upd = try ddl.parseDdl(alloc, "UPDATE t SET x = 1;");
    defer upd.deinit(alloc);
    try testing.expect(upd == .unsupported);

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

test "sql.ddl: CREATE TABLE reserves the egid column name" {
    const alloc = testing.allocator;
    var stmt = try ddl.parseDdl(alloc, "CREATE TABLE t (egid int8, name text);");
    defer stmt.deinit(alloc);
    try testing.expect(stmt == .invalid);
    try testing.expect(std.mem.indexOf(u8, stmt.invalid, "egid") != null);
}

test "sql.ddl: INSERT ... VALUES parses rows, columns, and literals" {
    const alloc = testing.allocator;

    // Positional, multi-row.
    var pos = try ddl.parseDdl(alloc, "INSERT INTO t VALUES (1, 'a'), (2, 'b');");
    defer pos.deinit(alloc);
    try testing.expectEqualStrings("t", pos.insert.table);
    try testing.expect(pos.insert.columns == null);
    try testing.expectEqual(@as(usize, 2), pos.insert.rows.len);
    try testing.expectEqual(@as(i64, 1), pos.insert.rows[0][0].int);
    try testing.expectEqualStrings("a", pos.insert.rows[0][1].str);
    try testing.expectEqual(@as(i64, 2), pos.insert.rows[1][0].int);

    // Explicit column list + null + bool literals.
    var cols = try ddl.parseDdl(alloc, "INSERT INTO t (id, flag, note) VALUES (0, true, NULL);");
    defer cols.deinit(alloc);
    try testing.expect(cols.insert.columns != null);
    try testing.expectEqual(@as(usize, 3), cols.insert.columns.?.len);
    try testing.expectEqualStrings("id", cols.insert.columns.?[0]);
    try testing.expectEqual(@as(i64, 0), cols.insert.rows[0][0].int); // zero-valued ival
    try testing.expectEqual(true, cols.insert.rows[0][1].bool_);
    try testing.expect(cols.insert.rows[0][2] == .null_value);
}

test "sql.ddl: INSERT rejections carry a reason" {
    const alloc = testing.allocator;
    const cases = [_]struct { sql: []const u8, needle: []const u8 }{
        .{ .sql = "INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING;", .needle = "ON CONFLICT" },
        .{ .sql = "INSERT INTO t VALUES (1) RETURNING id;", .needle = "RETURNING" },
        .{ .sql = "INSERT INTO t SELECT * FROM u;", .needle = "VALUES" },
    };
    for (cases) |case| {
        var stmt = try ddl.parseDdl(alloc, case.sql);
        defer stmt.deinit(alloc);
        try testing.expect(stmt == .invalid);
        try testing.expect(std.mem.indexOf(u8, stmt.invalid, case.needle) != null);
    }
}

test "sql.ddl: SELECT parses projection, table, and an equality filter" {
    const alloc = testing.allocator;

    // Star, no filter.
    var s1 = try ddl.parseDdl(alloc, "SELECT * FROM t;");
    defer s1.deinit(alloc);
    try testing.expectEqualStrings("t", s1.select.table);
    try testing.expect(s1.select.projection == .star);
    try testing.expect(s1.select.filter == null);

    // Named columns incl. the egid pseudo-column + a WHERE filter.
    var s2 = try ddl.parseDdl(alloc, "SELECT egid, name FROM t WHERE id = 5;");
    defer s2.deinit(alloc);
    try testing.expect(s2.select.projection == .columns);
    try testing.expectEqual(@as(usize, 2), s2.select.projection.columns.len);
    try testing.expectEqualStrings("egid", s2.select.projection.columns[0]);
    try testing.expectEqualStrings("name", s2.select.projection.columns[1]);
    const f = s2.select.filter.?;
    try testing.expectEqualStrings("id", f.column);
    try testing.expectEqual(@as(i64, 5), f.literal.int);
}

test "sql.ddl: SELECT rejections carry a reason" {
    const alloc = testing.allocator;
    const cases = [_]struct { sql: []const u8, needle: []const u8 }{
        .{ .sql = "SELECT * FROM t ORDER BY a;", .needle = "ORDER BY" },
        .{ .sql = "SELECT * FROM t LIMIT 3;", .needle = "LIMIT" },
        .{ .sql = "SELECT * FROM a, b;", .needle = "one table" },
        .{ .sql = "SELECT count(*) FROM t;", .needle = "column references" },
        .{ .sql = "SELECT * FROM t WHERE a > 5;", .needle = "column = value" },
    };
    for (cases) |case| {
        var stmt = try ddl.parseDdl(alloc, case.sql);
        defer stmt.deinit(alloc);
        try testing.expect(stmt == .invalid);
        try testing.expect(std.mem.indexOf(u8, stmt.invalid, case.needle) != null);
    }
}

test "sql.ddl: DELETE parses table and the optional equality filter" {
    const alloc = testing.allocator;

    // No WHERE: delete every row.
    var d1 = try ddl.parseDdl(alloc, "DELETE FROM t;");
    defer d1.deinit(alloc);
    try testing.expectEqualStrings("t", d1.delete.table);
    try testing.expect(d1.delete.filter == null);

    // WHERE on a payload column.
    var d2 = try ddl.parseDdl(alloc, "DELETE FROM t WHERE id = 5;");
    defer d2.deinit(alloc);
    const f2 = d2.delete.filter.?;
    try testing.expectEqualStrings("id", f2.column);
    try testing.expectEqual(@as(i64, 5), f2.literal.int);

    // WHERE on the egid pseudo-column parses as a plain column filter.
    var d3 = try ddl.parseDdl(alloc, "DELETE FROM t WHERE egid = 3;");
    defer d3.deinit(alloc);
    const f3 = d3.delete.filter.?;
    try testing.expectEqualStrings("egid", f3.column);
    try testing.expectEqual(@as(i64, 3), f3.literal.int);

    // Qualified name keeps the last part; string literal.
    var d4 = try ddl.parseDdl(alloc, "DELETE FROM public.t WHERE name = 'x';");
    defer d4.deinit(alloc);
    try testing.expectEqualStrings("t", d4.delete.table);
    const f4 = d4.delete.filter.?;
    try testing.expectEqualStrings("name", f4.column);
    try testing.expectEqualStrings("x", f4.literal.str);
}

test "sql.ddl: DELETE rejections carry a reason" {
    const alloc = testing.allocator;
    const cases = [_]struct { sql: []const u8, needle: []const u8 }{
        .{ .sql = "DELETE FROM t USING u WHERE t.a = u.a;", .needle = "USING" },
        .{ .sql = "DELETE FROM t RETURNING id;", .needle = "RETURNING" },
        .{ .sql = "WITH x AS (SELECT 1) DELETE FROM t;", .needle = "WITH" },
        .{ .sql = "DELETE FROM t WHERE a > 5;", .needle = "column = value" },
        .{ .sql = "DELETE FROM t WHERE a = 1 AND b = 2;", .needle = "WHERE" },
        .{
            .sql = "DELETE FROM t WHERE id IN (SELECT id FROM u);",
            .needle = "WHERE",
        },
        .{ .sql = "DELETE FROM t WHERE a = b;", .needle = "literal" },
    };
    for (cases) |case| {
        var stmt = try ddl.parseDdl(alloc, case.sql);
        defer stmt.deinit(alloc);
        try testing.expect(stmt == .invalid);
        const found = std.mem.indexOf(u8, stmt.invalid, case.needle);
        try testing.expect(found != null);
    }
}
