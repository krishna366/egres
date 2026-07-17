//! Reconstructing a user table's schema from its catalog rows.
//!
//! CREATE TABLE writes one catalog row per column (server.writeCatalogRows);
//! this reads them back so INSERT/SELECT can encode/decode the table's rows.
//! The catalog tree (catalog.table_id) holds every table's columns keyed
//! (table_id, column_id) big-endian, so the 4-byte table_id prefix scans
//! exactly one table's columns in column_id order — what schema.compile
//! consumes. The result owns its column names + compiled layout in one arena;
//! callers `deinit` it once done.

const std = @import("std");
const zdb = @import("zdb");

const value = @import("value.zig");
const schema = @import("schema.zig");
const row = @import("row.zig");
const catalog = @import("catalog.zig");

const Forest = zdb.forest.Forest;
const Key = zdb.key.Key;
const KeyRange = zdb.level.KeyRange;
const Item = zdb.key.Item;

pub const Error = error{UndefinedTable};

/// One live column of a loaded table. `name` is owned by the Table's arena.
/// Order matches the compiled PhysicalSchema positionally (both ascending by
/// column_id), so `columns[i]` describes `ps.columns[i]`.
pub const Column = struct {
    column_id: u32,
    name: []const u8,
    ty: value.Type,
    pkey_position: ?u16,
};

/// A user table as the SQL layer sees it: its live columns (logical schema)
/// and compiled byte layout (physical schema) for encoding/decoding rows, plus
/// the next row id to hand out. `loadTableSchema` fills the schema; `next_egid`
/// is runtime state (0 out of the load). Owns one arena — all of `columns`,
/// their names, and `ps` live in it, so `deinit` frees everything.
pub const Table = struct {
    columns: []Column,
    ps: schema.PhysicalSchema,
    version: u16,
    arena: std.heap.ArenaAllocator,
    /// Next egid (row id) to hand out. Maintained on EVERY node by the apply
    /// observer as `max(next_egid, committed_egid + 1)`, so it self-heals
    /// across leader changes without a term; seeded from forest.maxKey on
    /// cache-load (snapshot rows the observer never saw); advanced by the
    /// leader on hand-out. Not from the catalog. See sql.Server.
    next_egid: u64 = 0,

    pub fn deinit(self: *Table) void {
        self.arena.deinit();
    }

    /// The live column with this name, or null. Linear scan (columns is small).
    pub fn column(self: *const Table, name: []const u8) ?Column {
        for (self.columns) |c| {
            if (std.mem.eql(u8, c.name, name)) return c;
        }
        return null;
    }
};

const LoadCtx = struct {
    a: std.mem.Allocator, // arena
    catalog_ps: *const schema.PhysicalSchema,
    all: std.ArrayList(schema.LogicalColumn) = .empty, // every column, live or dropped
    scratch: value.Row = .{}, // reused across catalog rows
    err: ?anyerror = null,

    fn emit(ctx_raw: *anyopaque, item: Item) anyerror!bool {
        const c: *LoadCtx = @ptrCast(@alignCast(ctx_raw));
        // Decode all seven catalog cells (column_id order = SELECT *).
        // TODO: How do we know the version of the row, and whether the physical
        // schema corresponds to that version?
        row.unmarshalRow(c.a, c.catalog_ps, item.value, &.{}, &c.scratch) catch |e| {
            c.err = e;
            return false;
        };
        const tc = catalog.decodeColumn(&c.scratch) catch |e| {
            c.err = e;
            return false;
        };
        // Dupe the name out of the borrowed row buffer into the arena.
        // TODO: See if we can avoid this copy.
        const name = c.a.dupe(u8, tc.column.name) catch |e| {
            c.err = e;
            return false;
        };
        var lc = tc.column;
        lc.name = name;
        c.all.append(c.a, lc) catch |e| {
            c.err = e;
            return false;
        };
        return true;
    }
};

/// Load a user table's schema from namespace `ns`'s catalog. `catalog_ps` is
/// the caller's compiled catalog layout (Server.catalog_ps). Returns
/// `error.UndefinedTable` if the table has no catalog rows.
pub fn loadTableSchema(
    alloc: std.mem.Allocator,
    forest: *Forest,
    catalog_ps: *const schema.PhysicalSchema,
    ns: u64,
    table_id: u32,
) !Table {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const a = arena.allocator();

    // Scan the catalog tree for this table's column rows. Row keys are
    // (owner_table_id, column_id) BE; the scan window is [table_id.0,
    // (table_id+1).0) — exclusive on the right, and column_ids are 1-based so
    // no real key equals (table_id+1).0. ts=maxInt makes the primary compare
    // come first. These primaries live on the stack for the whole synchronous
    // scan below.
    const left_primary = catalog.encodeKey(table_id, 0);
    const right_primary = catalog.encodeKey(table_id + 1, 0);
    const has_upper = table_id != std.math.maxInt(u32);
    const scan_range = (KeyRange{
        .left = .{ .namespace = ns, .table_id = catalog.table_id, .primary = &left_primary, .timestamp = std.math.maxInt(u64) },
        .right = if (has_upper)
            .{ .namespace = ns, .table_id = catalog.table_id, .primary = &right_primary, .timestamp = std.math.maxInt(u64) }
        else
            .{},
    }).limitToTable(ns, catalog.table_id);

    var ctx = LoadCtx{ .a = a, .catalog_ps = catalog_ps };
    // scan uses `alloc` for its own transient arena (freed at scan end); the
    // callback dupes anything it keeps into `a` (the Table's arena).
    _ = try forest.scan(alloc, scan_range, std.math.maxInt(u64), .p0, &ctx, LoadCtx.emit);
    if (ctx.err) |e| return e;
    if (ctx.all.items.len == 0) return error.UndefinedTable;

    // Ascending by column_id (the scan already yields this order; sort defends
    // against any future reordering). compile() derives the layout from the
    // full history; reconstruct() derives current_version across all columns.
    std.mem.sort(schema.LogicalColumn, ctx.all.items, {}, lessByColumnId);
    const logical = catalog.reconstruct(ctx.all.items);
    const ps = try schema.compile(a, logical, logical.current_version);

    // Expose only the columns live at the current version, in ps order.
    var cols: std.ArrayList(Column) = .empty;
    for (ctx.all.items) |lc| {
        if (!lc.liveAt(logical.current_version)) continue;
        try cols.append(a, .{
            .column_id = lc.column_id,
            .name = lc.name,
            .ty = lc.ty,
            .pkey_position = lc.pkey_position,
        });
    }

    return .{
        .columns = try cols.toOwnedSlice(a),
        .ps = ps,
        .version = logical.current_version,
        .arena = arena,
    };
}

fn lessByColumnId(_: void, x: schema.LogicalColumn, y: schema.LogicalColumn) bool {
    return x.column_id < y.column_id;
}
