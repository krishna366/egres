//! The planner: the one place that chooses index vs. scan. A bound
//! Pred (pred.zig) in, a tree of exec nodes out — `[materialize →]
//! (seq_scan | fetch(bitmap))` — with the WHOLE predicate re-checked
//! on the source, so a bitmap only has to be a SUPERSET of the rows
//! and no remainder is ever computed. Files form a DAG: plan → exec →
//! pred; dml.zig calls `plan` and drives the tree.

const std = @import("std");
const zdb = @import("zdb");

const _exec = @import("exec.zig");
const _key = @import("key.zig");
const _pred = @import("pred.zig");
const _table = @import("table.zig");
const _value = @import("value.zig");

const BitmapNode = _exec.BitmapNode;
const Pred = _pred.Pred;
const RowNode = _exec.RowNode;
const zroar = zdb.zroar;

/// Planning allocates and nothing else can fail.
const Error = std.mem.Allocator.Error;

pub const Options = struct {
    /// Gather → lock every row → act: Materialize sits over the source
    /// and the write bodies (and FOR UPDATE) take the rows from it.
    /// Without it the plan streams (plain SELECT).
    lock: bool,
    /// Copy row values into the batch; false only for a key-only
    /// consumer (unindexed DELETE).
    keep_value: bool = true,
};

pub const Plan = struct {
    /// Materialize over the source when the statement locks, else the
    /// source itself: a seq_scan, or a fetch over the bitmap tree.
    root: *RowNode,

    /// Frees the tree's resources; the nodes' memory belongs to the
    /// statement arena.
    pub fn deinit(self: *Plan) void {
        self.root.deinit();
    }
};

/// Build the plan for `pred` (null: every row) over `tbl`. Nodes,
/// literal egid sets and encoded index keys live in `arloc`, the
/// statement arena. Choosing is only about which rows are READ — the
/// source re-checks the whole predicate per row, so a term the bitmap
/// already decided costs one more cell compare, never a wrong answer.
pub fn plan(
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    pred: ?*const Pred,
    opts: Options,
) !Plan {
    var bitmap: ?BitmapNode = null;
    if (pred) |p| bitmap = try bitmapFor(arloc, tbl, p);
    errdefer if (bitmap) |*bm| bm.deinit();

    var root = try arloc.create(RowNode);
    if (bitmap) |bm| {
        const bnode = try arloc.create(BitmapNode);
        bnode.* = bm;
        root.* = .{ .fetch = .{
            .bnode = bnode,
            .pred = pred,
            .keep_value = opts.keep_value,
        } };
    } else {
        root.* = .{ .seq_scan = .{
            .pred = pred,
            .keep_value = opts.keep_value,
        } };
    }
    if (opts.lock) {
        const mat = try arloc.create(RowNode);
        mat.* = .{ .materialize = .{ .child = root } };
        root = mat;
    }
    return .{ .root = root };
}

/// The egid set `p` can be answered from — a SUPERSET of its rows
/// when the tree is only partly indexed — or null when it cannot be:
/// then the rows are found by scanning. Leaves: `egid = N` is the
/// one-id set; `col = v` on a valid index its entry (unique: the one
/// row; non-unique: the posting bitmap); `col IS NULL` on a valid
/// non-unique index the posting under the NULL tag key. A comparison
/// against NULL can never be true (3VL), so it is the EMPTY set. An
/// AND is indexable when ANY child is (the rest is the residual's
/// job); an OR only when EVERY child is; NOT never.
fn bitmapFor(
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    p: *const Pred,
) Error!?BitmapNode {
    switch (p.*) {
        .eq => |cmp| {
            if (cmp.val == .null_value) return try emptySet(arloc);
            if (cmp.col.isEgid()) return try oneEgid(arloc, cmp.val.uint8);
            const idx = validIndex(tbl, cmp.col) orelse return null;
            return try lookup(arloc, idx, cmp.val);
        },
        .is_null => |nt| {
            if (nt.negate) return null;
            // A unique index holds no NULL entries; only a posting
            // bitmap can answer IS NULL.
            const idx = validIndex(tbl, nt.col) orelse return null;
            if (idx.unique) return null;
            return try lookup(arloc, idx, .null_value);
        },
        .and_ => |kids| return try andOf(arloc, tbl, kids),
        .or_ => |kids| return try orOf(arloc, tbl, kids),
        .not => return null,
    }
}

/// The valid index on `col`, if any. A mid-backfill index (valid =
/// false) is maintained by DML but must not be read yet.
fn validIndex(
    tbl: *const _table.Table,
    col: *const _table.Column,
) ?*const _table.Index {
    for (tbl.indexes) |*idx| {
        if (idx.column == col and idx.valid) return idx;
    }
    return null;
}

/// The entry of `idx` under `val`'s key — null when `val` has no
/// memcomparable encoding (interval): then the rows are found by
/// scanning.
fn lookup(
    arloc: std.mem.Allocator,
    idx: *const _table.Index,
    val: _value.Value,
) Error!?BitmapNode {
    const key = _key.encodeIndexKey(arloc, val, null) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        return null;
    };
    const node = BitmapNode{ .index_lookup = .{
        .index_table_id = idx.index_table_id,
        .key = key,
        .unique = idx.unique,
    } };
    return node;
}

/// `egid = N`: the one-id set. Literal sets are built here and owned
/// by the plan; Fetch evaluates them by clone.
fn oneEgid(arloc: std.mem.Allocator, egid: u64) Error!BitmapNode {
    var bm = try zroar.Bitmap.init(arloc);
    errdefer bm.deinit();
    _ = try bm.set(egid);
    return .{ .egids = bm };
}

/// No row at all — a comparison against NULL. Reads nothing, records
/// nothing.
fn emptySet(arloc: std.mem.Allocator) Error!BitmapNode {
    return .{ .egids = try zroar.Bitmap.init(arloc) };
}

fn isEmptySet(node: BitmapNode) bool {
    return node == .egids and node.egids.isEmpty();
}

/// AND: the intersection of every indexable child — a superset of the
/// AND's rows when some child is not. One empty child empties it all
/// and is then the whole answer: nothing is read for the others.
fn andOf(
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    kids: []const Pred,
) Error!?BitmapNode {
    var nodes: std.ArrayList(BitmapNode) = .empty;
    errdefer deinitNodes(arloc, &nodes);

    for (kids) |*kid| {
        var node = (try bitmapFor(arloc, tbl, kid)) orelse continue;
        errdefer node.deinit();
        if (isEmptySet(node)) {
            deinitNodes(arloc, &nodes);
            return node;
        }
        try nodes.append(arloc, node);
    }
    if (nodes.items.len == 0) return null;
    return try collapse(arloc, &nodes, .and_);
}

/// OR: the union of every child — only when each one is indexable,
/// or the rows the rest would add are lost. An empty child adds
/// nothing and is dropped.
fn orOf(
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    kids: []const Pred,
) Error!?BitmapNode {
    var nodes: std.ArrayList(BitmapNode) = .empty;
    errdefer deinitNodes(arloc, &nodes);
    for (kids) |*kid| {
        var node = (try bitmapFor(arloc, tbl, kid)) orelse {
            deinitNodes(arloc, &nodes);
            return null;
        };
        errdefer node.deinit();
        if (isEmptySet(node)) {
            node.deinit();
            continue;
        }
        try nodes.append(arloc, node);
    }
    if (nodes.items.len == 0) return try emptySet(arloc);
    return try collapse(arloc, &nodes, .or_);
}

/// One node stands alone; more are joined under `tag`. Consumes the
/// list.
fn collapse(
    arloc: std.mem.Allocator,
    nodes: *std.ArrayList(BitmapNode),
    comptime tag: std.meta.Tag(BitmapNode),
) Error!BitmapNode {
    std.debug.assert(nodes.items.len > 0);
    if (nodes.items.len == 1) {
        const one = nodes.items[0];
        nodes.deinit(arloc);
        return one;
    }
    const kids = try nodes.toOwnedSlice(arloc);
    return @unionInit(BitmapNode, @tagName(tag), kids);
}

fn deinitNodes(
    arloc: std.mem.Allocator,
    nodes: *std.ArrayList(BitmapNode),
) void {
    for (nodes.items) |*node| node.deinit();
    nodes.deinit(arloc);
}
