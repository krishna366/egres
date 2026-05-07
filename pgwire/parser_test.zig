//! libpg_query wrapper tests. The parser layer is technically Layer 1
//! (no I/O), but it pulls in the C library so we test it separately.

const std = @import("std");
const testing = std.testing;
const pgwire = @import("pgwire");

const parser = pgwire.parser_mod;

/// Walk into the first statement's "stmt" wrapper and return its key
/// (the statement-kind name libpg_query emits, e.g. "SelectStmt").
fn firstStmtKind(tree: *const parser.ParseTree) ?[]const u8 {
    const stmts = tree.statements() orelse return null;
    if (stmts.len == 0) return null;
    const first = stmts[0];
    const obj = switch (first) {
        .object => |o| o,
        else => return null,
    };
    const wrapper = obj.get("stmt") orelse return null;
    const wrapper_obj = switch (wrapper) {
        .object => |o| o,
        else => return null,
    };
    var it = wrapper_obj.iterator();
    if (it.next()) |entry| return entry.key_ptr.*;
    return null;
}

test "parse: SELECT 1 → SelectStmt" {
    const a = std.testing.allocator;
    var tree = try parser.parse(a, "SELECT 1", null);
    defer tree.deinit(a);
    try testing.expectEqualStrings("SelectStmt", firstStmtKind(&tree).?);
}

test "parse: SELECT a, b FROM t WHERE c = 1 → 2 target entries" {
    const a = std.testing.allocator;
    var tree = try parser.parse(
        a,
        "SELECT a, b FROM t WHERE c = 1",
        null,
    );
    defer tree.deinit(a);

    const stmts = tree.statements().?;
    const first = stmts[0].object;
    const wrapper = first.get("stmt").?.object;
    const select = wrapper.get("SelectStmt").?.object;
    const targets = select.get("targetList").?.array;
    try testing.expectEqual(@as(usize, 2), targets.items.len);
}

test "parse: CREATE TABLE → CreateStmt" {
    const a = std.testing.allocator;
    var tree = try parser.parse(
        a,
        "CREATE TABLE t (id int primary key, name text)",
        null,
    );
    defer tree.deinit(a);
    try testing.expectEqualStrings("CreateStmt", firstStmtKind(&tree).?);
}

test "parse: INSERT INTO t VALUES (1) → InsertStmt" {
    const a = std.testing.allocator;
    var tree = try parser.parse(
        a,
        "INSERT INTO t VALUES (1)",
        null,
    );
    defer tree.deinit(a);
    try testing.expectEqualStrings("InsertStmt", firstStmtKind(&tree).?);
}

test "parse: syntax error returns SyntaxError + cursor info" {
    const a = std.testing.allocator;
    var info: ?parser.ErrorInfo = null;
    // Unmatched paren — guaranteed syntax error in any Postgres
    // grammar. `FROMM x` would just be parsed as a column reference.
    const result = parser.parse(a, "SELECT 1 FROM (", &info);
    if (result) |*tree| {
        var t = tree.*;
        t.deinit(a);
        try testing.expect(false); // unreachable: must error
    } else |err| {
        try testing.expectEqual(error.SyntaxError, err);
    }
    try testing.expect(info != null);
    var info_unwrap = info.?;
    defer info_unwrap.deinit(a);
    // libpg_query reports cursor positions 1-based; we don't assert
    // an exact number (it's tied to libpg internals) but it must be
    // strictly positive.
    try testing.expect(info_unwrap.cursor_pos > 0);
    try testing.expect(info_unwrap.message.len > 0);
}

test "parse: 1000 iterations under testing allocator (no leaks)" {
    const a = std.testing.allocator;
    var i: usize = 0;
    while (i < 1000) : (i += 1) {
        var tree = try parser.parse(a, "SELECT 1", null);
        tree.deinit(a);
    }
    // libpg_query lazily creates a per-thread top memory context; we
    // tear it down explicitly so testing.allocator can detect any
    // leak in our wrapper code (libpg's own state is tracked by the
    // C lib and isn't allocated through Zig's allocator).
    parser.shutdown();
}
