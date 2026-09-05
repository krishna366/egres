//! Boolean WHERE predicates over stored rows. `Pred` is the resolved
//! tree — parse.Where with its columns bound to the table and its
//! literals coerced to the columns' types — and `eval` is its SQL
//! three-valued evaluation against one row. Choosing HOW rows are read
//! (index vs. scan) is plan.zig's job; a Pred only says which rows
//! match.

const std = @import("std");
const pgwire = @import("pgwire");
const zdb = @import("zdb");

const _parse = @import("parse.zig");
const _table = @import("table.zig");
const _value = @import("value.zig");
const _server = @import("server.zig");

const replyErrorCode = _server.replyErrorCode;

/// Children of AND/OR live inline: a slice is already an indirection
/// and children are never shared between parents. NOT is a pointer
/// only because a union cannot contain itself by value.
pub const Pred = union(enum) {
    eq: Cmp,
    is_null: NullTest,
    and_: []const Pred,
    or_: []const Pred,
    not: *const Pred,

    /// `col = val`, `val` already coerced to `col.ty` (for the egid
    /// pseudo-column: a positive uint8, or NULL).
    pub const Cmp = struct { col: *const _table.Column, val: _value.Value };
    /// `col IS NULL` (negate = false) / `col IS NOT NULL`.
    pub const NullTest = struct { col: *const _table.Column, negate: bool };

    /// The top-level AND terms: a root AND's children, else the root
    /// alone. Every one must hold for a row to match, which is what
    /// lets a source pick ANY of them to drive the read. Only the root
    /// is looked at — `a AND (b AND c)` parses right-nested and yields
    /// two terms, the second an AND.
    pub fn topLevelAnds(self: *const Pred) []const Pred {
        return switch (self.*) {
            .and_ => |kids| kids,
            else => self[0..1],
        };
    }
};

pub const BindError = error{
    UndefinedColumn,
    InvalidValue,
    EgidZero,
    OutOfMemory,
};

/// Bind one WHERE node — and, recursively, its children — to `tbl`:
/// names → columns, literals → typed cells (parse.litToValue). Pure;
/// `bind` adds the client-facing error mapping. `arloc` (the statement
/// arena) backs the tree.
pub fn bindNode(
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    where: _parse.Where,
) BindError!Pred {
    switch (where) {
        .eq => |f| {
            const col = tbl.column(f.column) orelse return error.UndefinedColumn;
            const val = _parse.litToValue(arloc, f.literal, col.ty) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return error.InvalidValue,
            };
            // The egid IS the row key. Its column is uint8, so a
            // negative literal already failed to coerce; zero (never a
            // row) is rejected here, and `egid = NULL` is left to be
            // unknown like any other comparison.
            if (col.isEgid() and val == .uint8 and val.uint8 == 0)
                return error.EgidZero;
            return .{ .eq = .{ .col = col, .val = val } };
        },
        .is_null => |nt| {
            const col = tbl.column(nt.column) orelse return error.UndefinedColumn;
            return .{ .is_null = .{ .col = col, .negate = nt.negate } };
        },
        .and_, .or_ => |kids| {
            const out = try arloc.alloc(Pred, kids.len);
            for (kids, out) |kid, *slot| slot.* = try bindNode(arloc, tbl, kid);
            return if (where == .and_) .{ .and_ = out } else .{ .or_ = out };
        },
        .not => |child| {
            const out = try arloc.create(Pred);
            out.* = try bindNode(arloc, tbl, child.*);
            return .{ .not = out };
        },
    }
}

/// Bind a statement's WHERE to its table: bindNode plus the
/// client-facing error mapping. null = the SQL error was already sent
/// and the statement is over; only OOM propagates.
pub fn bind(
    state: *pgwire.ConnState,
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    where: _parse.Where,
) !?*const Pred {
    const bad_value = pgwire.sqlstate.invalid_text_representation;
    const p = try arloc.create(Pred);
    p.* = bindNode(arloc, tbl, where) catch |err| {
        switch (err) {
            error.OutOfMemory => return err,
            error.UndefinedColumn => try replyErrorCode(
                state,
                pgwire.sqlstate.undefined_column,
                "column does not exist",
            ),
            error.InvalidValue => try replyErrorCode(state, bad_value, "invalid value in WHERE"),
            error.EgidZero => try replyErrorCode(
                state,
                bad_value,
                "egid must be a positive integer",
            ),
        }
        return null;
    };
    return p;
}

/// SQL three-valued logic: null = unknown. `eq` is unknown when either
/// side is NULL; IS [NOT] NULL is never unknown; AND / OR / NOT follow
/// Kleene. Reads only the cells the tree names (Table.read — a null-bit
/// test plus one O(1) read each; no full-row decode).
pub fn eval(tbl: *const _table.Table, item: zdb.kv.Item, p: *const Pred) ?bool {
    switch (p.*) {
        .eq => |c| {
            if (c.val == .null_value) return null;
            const cell = tbl.parseCol(item, c.col);
            if (cell == .null_value) return null;
            return _value.valuesEqual(cell, c.val);
        },
        .is_null => |n| return (tbl.parseCol(item, n.col) == .null_value) != n.negate,
        .and_ => |kids| {
            var unknown = false;
            for (kids) |*kid| {
                if (eval(tbl, item, kid)) |b| {
                    if (!b) return false;
                } else unknown = true;
            }
            return if (unknown) null else true;
        },
        .or_ => |kids| {
            var unknown = false;
            for (kids) |*kid| {
                if (eval(tbl, item, kid)) |b| {
                    if (b) return true;
                } else unknown = true;
            }
            return if (unknown) null else false;
        },
        .not => |child| {
            const b = eval(tbl, item, child) orelse return null;
            return !b;
        },
    }
}

/// A row matches only when the predicate is TRUE — unknown rejects,
/// so `WHERE col = NULL` matches nothing.
pub fn matches(tbl: *const _table.Table, item: zdb.kv.Item, p: *const Pred) bool {
    return eval(tbl, item, p) orelse false;
}
