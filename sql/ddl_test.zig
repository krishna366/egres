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

    // CREATE TABLE parses fine but is out of scope for this slice.
    var ct = try ddl.parseDdl(alloc, "CREATE TABLE t (a int);");
    defer ct.deinit(alloc);
    try testing.expect(ct == .unsupported);

    try testing.expectError(error.SyntaxError, ddl.parseDdl(alloc, "CREATE DATABASE;"));
}
