//! libpg_query wrapper. JSON output path.
//!
//! Why JSON instead of protobuf? Two reasons:
//!  1. Zig has a robust std.json; protobuf-c would mean vendoring +
//!     wrapping a separate runtime.
//!  2. The schema we need (top-level statement kinds) is small, so
//!     parsing cost is irrelevant — we just look at the first key.
//!
//! Lifetime: pg_query_parse() returns a malloc'd C string. We hand
//! that to std.json on a copy of the bytes, owned by `ParseTree`,
//! and free the C result before returning. So all memory in
//! `ParseTree` is allocator-owned and `deinit` is enough.

const std = @import("std");

const c = @cImport({
    @cInclude("pg_query.h");
});

pub const ParseError = error{
    /// libpg_query reported a syntax error. Inspect `last_error` for
    /// details. We use a single error tag so callers can route by it
    /// without committing to libpg's internal naming.
    SyntaxError,
    /// Allocation failure copying out of the C buffer.
    OutOfMemory,
    /// JSON output failed to parse — should be impossible if libpg
    /// is healthy, but we never trust C/JSON round-trips silently.
    JsonError,
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

/// Parsed SQL tree. The raw JSON bytes are kept around so callers can
/// re-walk if they need to; `value` is a pre-parsed std.json view of
/// the same bytes for ergonomic access.
pub const ParseTree = struct {
    json: []u8,
    parsed: std.json.Parsed(std.json.Value),

    pub fn deinit(self: *ParseTree, alloc: std.mem.Allocator) void {
        self.parsed.deinit();
        alloc.free(self.json);
    }

    /// Top-level "stmts" array (exposed by libpg_query). Returns null
    /// if the JSON had no statements (libpg quirks aside, this is
    /// effectively the empty-input case).
    pub fn statements(self: *const ParseTree) ?[]std.json.Value {
        const root = self.parsed.value;
        const obj = switch (root) {
            .object => |o| o,
            else => return null,
        };
        const stmts = obj.get("stmts") orelse return null;
        return switch (stmts) {
            .array => |a| a.items,
            else => null,
        };
    }
};

/// One-shot `parse`. On error, fills `out_err` (if non-null) with a
/// human-readable message + cursor position copied into the caller's
/// allocator.
pub fn parse(
    alloc: std.mem.Allocator,
    sql: []const u8,
    out_err: ?*?ErrorInfo,
) ParseError!ParseTree {
    // libpg_query insists on a NUL-terminated input. Allocate a copy
    // even when the caller's slice would suffice — it's the only safe
    // contract.
    const c_sql = try alloc.allocSentinel(u8, sql.len, 0);
    defer alloc.free(c_sql);
    @memcpy(c_sql, sql);

    const raw = c.pg_query_parse(c_sql.ptr);
    defer c.pg_query_free_parse_result(raw);

    if (raw.@"error" != null) {
        const err = raw.@"error".*;
        if (out_err) |slot| {
            const msg_z = std.mem.span(err.message);
            const dup = try alloc.dupe(u8, msg_z);
            slot.* = .{
                .message = dup,
                .cursor_pos = err.cursorpos,
            };
        }
        return error.SyntaxError;
    }
    if (raw.parse_tree == null) return error.JsonError;

    const json_bytes = std.mem.span(raw.parse_tree);
    const owned = try alloc.dupe(u8, json_bytes);
    errdefer alloc.free(owned);

    const parsed = std.json.parseFromSlice(
        std.json.Value,
        alloc,
        owned,
        .{},
    ) catch return error.JsonError;

    return .{ .json = owned, .parsed = parsed };
}

/// Free libpg_query's per-thread top memory context. The library
/// auto-cleans on thread exit, but tests benefit from an explicit
/// hook to drain leaks before testing-allocator checks.
pub fn shutdown() void {
    c.pg_query_exit();
}

test {
    std.testing.refAllDecls(@This());
}
