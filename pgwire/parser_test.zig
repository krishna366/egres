//! libpg_query wrapper tests. The parser layer is technically Layer 1
//! (no I/O), but it pulls in the C library so we test it separately.

const std = @import("std");
const testing = std.testing;
const fiber = @import("zfiber");
const pgwire = @import("pgwire");
const utils = @import("utils");
const String = utils.String;

const parser = pgwire.parser;
const c = parser.c;

/// The first statement's node tag.
fn firstStmtTag(tree: parser.Tree) ?c.NodeTag {
    const node = tree.statement(0) orelse return null;
    return node.type;
}

test "parse: SELECT 1 → SelectStmt" {
    const a = std.testing.allocator;
    const tree = try parser.parse(a, "SELECT 1", null);
    defer tree.deinit();
    const want: c.NodeTag = c.T_SelectStmt;
    try testing.expectEqual(want, firstStmtTag(tree).?);
}

test "parse: SELECT a, b FROM t WHERE c = 1 → 2 target entries" {
    const a = std.testing.allocator;
    const tree = try parser.parse(a, "SELECT a, b FROM t WHERE c = 1", null);
    defer tree.deinit();

    const sel = parser.cast(c.SelectStmt, tree.statement(0)).?;
    try testing.expectEqual(@as(usize, 2), parser.items(sel.targetList).len);
}

test "parse: CREATE TABLE → CreateStmt" {
    const a = std.testing.allocator;
    const tree = try parser.parse(
        a,
        "CREATE TABLE t (id int primary key, name text)",
        null,
    );
    defer tree.deinit();
    const want: c.NodeTag = c.T_CreateStmt;
    try testing.expectEqual(want, firstStmtTag(tree).?);
}

test "parse: INSERT INTO t VALUES (1) → InsertStmt" {
    const a = std.testing.allocator;
    const tree = try parser.parse(a, "INSERT INTO t VALUES (1)", null);
    defer tree.deinit();
    const want: c.NodeTag = c.T_InsertStmt;
    try testing.expectEqual(want, firstStmtTag(tree).?);
}

test "parse: syntax error returns SyntaxError + cursor info" {
    const a = std.testing.allocator;
    var info: ?parser.ErrorInfo = null;
    // Unmatched paren — guaranteed syntax error in any Postgres
    // grammar. `FROMM x` would just be parsed as a column reference.
    const result = parser.parse(a, "SELECT 1 FROM (", &info);
    if (result) |tree| {
        tree.deinit();
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

test "parse: 1000 concurrent fibers parse distinct queries (no leaks, no races)" {
    // Each fiber N parses `SELECT a, b, c FROM t_N WHERE id = N`, then
    // walks the parse tree to confirm relname == `t_N` and the WHERE
    // clause's integer constant == N. With parallel=16 workers running
    // 1000 fibers, this exercises libpg_query under genuine cross-thread
    // concurrency. If any per-thread libpg state ever leaked across
    // threads, fibers would see each other's parses and either the
    // relname or the int wouldn't match — the failure counter catches it.
    //
    // testing.allocator is GPA with thread_safe = !single_threaded, so
    // the leak check stays valid under concurrent fibers. libpg_query's
    // plain-malloc per-thread state is released on thread exit by its
    // own pthread destructor — no manual shutdown needed.
    const a = std.testing.allocator;
    var failures: std.atomic.Value(u32) = .{ .raw = 0 };

    const Worker = struct {
        fn run(alloc: std.mem.Allocator, n: u32, fail_count: *std.atomic.Value(u32)) void {
            const sql = std.fmt.allocPrint(
                alloc,
                "SELECT a, b, c FROM t_{d} WHERE id = {d}",
                .{ n, n },
            ) catch {
                _ = fail_count.fetchAdd(1, .seq_cst);
                return;
            };
            defer alloc.free(sql);

            const tree = parser.parse(alloc, sql, null) catch {
                _ = fail_count.fetchAdd(1, .seq_cst);
                return;
            };
            defer tree.deinit();

            if (!verifyParse(tree, n)) {
                _ = fail_count.fetchAdd(1, .seq_cst);
            }
        }

        fn verifyParse(tree: parser.Tree, n: u32) bool {
            if (tree.count() != 1) return false;
            const sel = parser.cast(c.SelectStmt, tree.statement(0)) orelse
                return false;

            // FROM: relname must be `t_<n>` for THIS fiber's n.
            if (parser.items(sel.fromClause).len != 1) return false;
            const from = parser.nodeAt(sel.fromClause, 0);
            const rv = parser.cast(c.RangeVar, from) orelse return false;
            const relname = parser.str(rv.relname);
            if (!std.mem.startsWith(u8, relname, "t_")) return false;
            const got_n = std.fmt.parseInt(u32, relname[2..], 10) catch return false;
            if (got_n != n) return false;

            // WHERE: integer constant must equal n. Independent witness
            // from the relname — both being right rules out a cross-fiber
            // bleed that happens to align on one of them.
            const ae = parser.cast(c.A_Expr, sel.whereClause) orelse
                return false;
            const ac = parser.cast(c.A_Const, ae.rexpr) orelse return false;
            if (ac.val.node.type != c.T_Integer) return false;
            return ac.val.ival.ival == @as(c_int, @intCast(n));
        }
    };

    var group = fiber.Group(void).init(a);
    defer group.deinit();
    try group.ensureTotalCapacity(1000);
    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        try group.spawn(.{}, Worker.run, .{ a, i, &failures });
    }
    group.wait();
    try testing.expectEqual(@as(u32, 0), failures.load(.seq_cst));
}
