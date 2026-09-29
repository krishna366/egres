//! libpg_query wrapper: the raw parse tree, read in place.
//!
//! `pg_query_raw_parse` builds Postgres's own node tree in a memory
//! context. The JSON path had libpg_query serialize that tree and
//! std.json parse it back, which cost more than the parse itself:
//! 3 ms of a 200-row INSERT's 4. The tree is now read straight through
//! the translated Postgres headers and freed with its context; what a
//! caller keeps, it copies out (sql.parse dupes into its arena).
//!
//! The memory context is per OS thread and `parse` nests one under it,
//! so a `Tree` must be released before the fiber parks: another fiber
//! on the same thread would nest a second context and the two would
//! be deleted out of order. sql.parse walks the whole tree without
//! yielding.

const std = @import("std");
const utils = @import("utils");
const String = utils.String;

pub const c = @cImport({
    // `__thread` on extern globals does not translate; nothing here
    // reads them, and the C side is compiled separately.
    @cDefine("__thread", "");
    @cInclude("pg_query.h");
    @cInclude("pg_query_internal.h");
    @cInclude("nodes/parsenodes.h");
});

pub const ParseError = error{
    /// libpg_query reported a syntax error; `out_err` has the detail.
    SyntaxError,
    OutOfMemory,
};

/// Detail captured from a libpg syntax error, owned by the caller.
pub const ErrorInfo = struct {
    message: []u8,
    /// 1-based byte position into the offending SQL, or 0 if unknown.
    cursor_pos: i32,

    pub fn deinit(self: *ErrorInfo, alloc: std.mem.Allocator) void {
        alloc.free(self.message);
    }
};

/// One parse's statements, alive until `deinit`.
pub const Tree = struct {
    ctx: c.MemoryContext,
    stmts: [*c]c.List,

    pub fn deinit(self: Tree) void {
        c.pg_query_exit_memory_context(self.ctx);
    }

    pub fn count(self: Tree) usize {
        return items(self.stmts).len;
    }

    /// Statement `i`, unwrapped from its RawStmt; null past the end.
    pub fn statement(self: Tree, i: usize) ?*c.Node {
        const raw = cast(c.RawStmt, nodeAt(self.stmts, i)) orelse return null;
        return asNode(raw.stmt);
    }
};

/// One-shot parse. On a syntax error, fills `out_err` (if non-null)
/// with the message and cursor position, copied into `alloc`.
pub fn parse(
    alloc: std.mem.Allocator,
    sql: String,
    out_err: ?*?ErrorInfo,
) ParseError!Tree {
    // libpg_query wants a NUL-terminated input.
    const c_sql = try alloc.allocSentinel(u8, sql.len, 0);
    defer alloc.free(c_sql);
    @memcpy(c_sql, sql);

    const ctx = c.pg_query_enter_memory_context();
    const raw = c.pg_query_raw_parse(c_sql.ptr, c.PG_QUERY_PARSE_DEFAULT);
    // Captured stderr, malloc'd on the success path; nothing reads it.
    if (raw.stderr_buffer != null) std.c.free(raw.stderr_buffer);

    if (raw.@"error") |err| {
        defer c.pg_query_exit_memory_context(ctx);
        defer c.pg_query_free_error(err);
        if (out_err) |slot| {
            slot.* = .{
                .message = try alloc.dupe(u8, str(err.*.message)),
                .cursor_pos = err.*.cursorpos,
            };
        }
        return error.SyntaxError;
    }
    return .{ .ctx = ctx, .stmts = raw.tree };
}

// ---------------------------------------------------------------------------
// Reading the tree. Every node starts with its NodeTag; lists are arrays
// of cells whose `ptr_value` is a node. A null list pointer is empty.
// ---------------------------------------------------------------------------

/// A list's cells.
pub fn items(l: [*c]const c.List) []const c.ListCell {
    if (l == null) return &.{};
    return l.*.elements[0..@intCast(l.*.length)];
}

/// The node in cell `i`, or null past the end.
pub fn nodeAt(l: [*c]const c.List, i: usize) ?*c.Node {
    const cells = items(l);
    if (i >= cells.len) return null;
    return @ptrCast(@alignCast(cells[i].ptr_value));
}

/// A node pointer of any spelling (`[*c]T`, `?*anyopaque`, a cell's
/// value) as `?*Node`.
pub fn asNode(p: anytype) ?*c.Node {
    return switch (@typeInfo(@TypeOf(p))) {
        .optional => if (p) |v| asNode(v) else null,
        .pointer => |info| blk: {
            if (info.size == .c and p == null) break :blk null;
            break :blk @ptrCast(@alignCast(p));
        },
        else => @compileError("not a node pointer: " ++ @typeName(@TypeOf(p))),
    };
}

/// `p` as a `*T` when its tag is T's, else null. T is a translated
/// node struct, `c.InsertStmt` say, whose tag is `c.T_InsertStmt`.
pub fn cast(comptime T: type, p: anytype) ?*T {
    const node = asNode(p) orelse return null;
    if (node.type != tagOf(T)) return null;
    return @ptrCast(@alignCast(node));
}

fn tagOf(comptime T: type) c.NodeTag {
    // "cimport.struct_A_Const" -> T_A_Const. A type that is not a
    // translated node struct fails the unwrap at compile time.
    const full = @typeName(T);
    const at = comptime std.mem.indexOf(u8, full, "struct_").?;
    return @intCast(@field(c, "T_" ++ full[at + "struct_".len ..]));
}

/// A C string as a slice; null is empty.
pub fn str(p: [*c]const u8) String {
    if (p == null) return "";
    return std.mem.span(p);
}

// Note on cleanup: libpg_query installs a pthread TSD destructor that
// frees each thread's TopMemoryContext when the OS thread exits. We do
// NOT expose pg_query_exit() — calling it manually trips a latent
// upstream bug where the TSD slot keeps a dangling pointer and the
// destructor double-frees on thread exit. Per-parse memory goes with
// the Tree's context, per-thread state with the thread.

test {
    std.testing.refAllDecls(@This());
}
