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
const Key = zdb.kv.Key;
const KeyRange = zdb.level.KeyRange;
const Item = zdb.kv.Item;

pub const Error = error{UndefinedTable};

// The SQL layer's catalog table ids and the engine's (which provisions the
// trees at ns-create) must agree — drift would silently drop catalog rows
// (buildFlush skips keys whose table has no tree). catalog.zig deliberately
// imports no zdb, so the tie-down lives here.
comptime {
    std.debug.assert(catalog.table_id == zdb.manifest.catalog_table_id);
    std.debug.assert(catalog.index_table_id ==
        zdb.manifest.index_catalog_table_id);
}

/// One live column of a loaded table. `name` is owned by the Table's arena.
/// Order matches the compiled PhysicalSchema positionally (both ascending by
/// column_id), so `columns[i]` describes `ps.columns[i]`.
pub const Column = struct {
    column_id: u32,
    name: []const u8,
    ty: value.Type,
    pkey_position: ?u16,
};

/// The PRIMARY KEY's summary, resolved once at load so statements needn't
/// rescan. `column_index` is the key column's position in `columns` (==
/// `ps.columns` position, for pulling its value out of a positional row);
/// `index_table_id` is its unique index's tree.
pub const Pkey = struct {
    column_index: usize,
    index_table_id: u32,
};

/// A user table as the SQL layer sees it: its live columns (logical schema)
/// and compiled byte layout (physical schema) for encoding/decoding rows, plus
/// the next row id to hand out. `loadTableSchema` fills the schema; `next_egid`
/// is runtime state (0 out of the load). Owns one arena — all of `columns`,
/// their names, and `ps` live in it, so `deinit` frees everything.
pub const Table = struct {
    columns: []Column,
    /// Every index of this table (owned by the arena). The PK index, if any,
    /// is also summarized in `pkey`.
    indexes: []catalog.IndexDef,
    /// The PRIMARY KEY index, or null for a table without one.
    pkey: ?Pkey,
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

/// The scan window selecting every catalog row owned by `owner_id` in
/// catalog tree `catalog_tid`. Keys are (owner_id, member_id) BE pairs and
/// member ids are 1-based, so the window is [owner.0, (owner+1).0) — an
/// exclusive right edge no real key equals — with the upper bound dropped
/// at owner_id maxInt. ts=maxInt makes the primary compare come first. The
/// struct owns the boundary key bytes: keep it alive (and in place) for the
/// whole scan, since range() borrows them.
const CatalogWindow = struct {
    ns: u64,
    catalog_tid: u32,
    has_upper: bool,
    left: [8]u8,
    right: [8]u8,

    fn init(ns: u64, catalog_tid: u32, owner_id: u32) CatalogWindow {
        return .{
            .ns = ns,
            .catalog_tid = catalog_tid,
            .has_upper = owner_id != std.math.maxInt(u32),
            .left = catalog.encodeKey(owner_id, 0),
            .right = catalog.encodeKey(owner_id +% 1, 0),
        };
    }

    fn range(self: *const CatalogWindow) KeyRange {
        return (KeyRange{
            .left = .{ .namespace = self.ns, .table_id = self.catalog_tid, .primary = &self.left, .timestamp = std.math.maxInt(u64) },
            .right = if (self.has_upper)
                .{ .namespace = self.ns, .table_id = self.catalog_tid, .primary = &self.right, .timestamp = std.math.maxInt(u64) }
            else
                .{},
        }).limitToTable(self.ns, self.catalog_tid);
    }
};

const LoadCtx = struct {
    a: std.mem.Allocator, // arena
    catalog_ps: *const schema.PhysicalSchema,
    all: std.ArrayList(schema.LogicalColumn) = .empty, // every column, live or dropped
    scratch: value.Row = .{}, // reused across catalog rows
    err: ?anyerror = null,

    fn emit(c: *LoadCtx, item: Item) anyerror!bool {
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
    index_catalog_ps: *const schema.PhysicalSchema,
    ns: u64,
    table_id: u32,
) !Table {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const a = arena.allocator();

    // Scan the catalog tree for this table's column rows; `win` owns the
    // boundary key bytes, so it lives for the whole synchronous scan below.
    const win = CatalogWindow.init(ns, catalog.table_id, table_id);
    var ctx = LoadCtx{ .a = a, .catalog_ps = catalog_ps };
    // scan uses `alloc` for its own transient arena (freed at scan end); the
    // callback dupes anything it keeps into `a` (the Table's arena).
    _ = try forest.scan(alloc, win.range(), .{}, &ctx, LoadCtx.emit);
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
    const columns = try cols.toOwnedSlice(a);

    // Indexes (table 2), plus the PK summary derived from them: the key
    // column (pkey_position set) paired with the index on it.
    const indexes = try loadIndexes(a, forest, index_catalog_ps, ns, table_id);
    var pkey: ?Pkey = null;
    for (columns, 0..) |c, i| {
        if (c.pkey_position == null) continue;
        for (indexes) |idx| if (idx.column_id == c.column_id) {
            pkey = .{
                .column_index = i,
                .index_table_id = idx.index_table_id,
            };
        };
        break; // single-column PK only
    }

    return .{
        .columns = columns,
        .indexes = indexes,
        .pkey = pkey,
        .ps = ps,
        .version = logical.current_version,
        .arena = arena,
    };
}

const IndexLoadCtx = struct {
    a: std.mem.Allocator,
    ips: *const schema.PhysicalSchema,
    out: std.ArrayList(catalog.IndexDef) = .empty,
    scratch: value.Row = .{}, // reused across index rows
    err: ?anyerror = null,

    fn emit(c: *IndexLoadCtx, item: Item) anyerror!bool {
        row.unmarshalRow(c.a, c.ips, item.value, &.{}, &c.scratch) catch |e| {
            c.err = e;
            return false;
        };
        const def = catalog.decodeIndex(&c.scratch) catch |e| {
            c.err = e;
            return false;
        };
        c.out.append(c.a, def) catch |e| {
            c.err = e;
            return false;
        };
        return true;
    }
};

/// Load every index of `base_table_id` from namespace `ns`'s index catalog
/// (table 2), prefix-scanned by base_table_id (empty if none). The returned
/// slice and its contents are allocated with `alloc`.
pub fn loadIndexes(
    alloc: std.mem.Allocator,
    forest: *Forest,
    index_catalog_ps: *const schema.PhysicalSchema,
    ns: u64,
    base_table_id: u32,
) ![]catalog.IndexDef {
    var ctx = IndexLoadCtx{ .a = alloc, .ips = index_catalog_ps };
    errdefer ctx.out.deinit(alloc);

    const win = CatalogWindow.init(ns, catalog.index_table_id, base_table_id);
    _ = try forest.scan(alloc, win.range(), .{}, &ctx, IndexLoadCtx.emit);

    ctx.scratch.deinit(alloc);
    if (ctx.err) |e| return e;
    return ctx.out.toOwnedSlice(alloc);
}

/// A key-only index catalog scan classifying `target`. Row keys are
/// (base_table_id, index_table_id), so both answers come from key bytes
/// alone — no value decode.
const IndexKeyCtx = struct {
    a: std.mem.Allocator,
    target: u32,
    is_index: bool = false,
    base_table_id: ?u32 = null,
    table_ids: std.ArrayList(u32) = .empty,
    err: ?anyerror = null,

    fn emit(c: *IndexKeyCtx, item: Item) anyerror!bool {
        const def = catalog.decodeIndexKey(item.key.primary) orelse {
            c.err = error.CorruptCatalogKey;
            return false;
        };
        if (def.index_table_id == c.target) {
            c.is_index = true;
            c.base_table_id = def.base_table_id;
            return false; // an index has no indexes; nothing more to learn
        }
        if (def.base_table_id == c.target)
            c.table_ids.append(c.a, def.index_table_id) catch |e| {
                c.err = e;
                return false;
            };
        return true;
    }
};

/// What one key-only pass over the index catalog says about a table_id:
/// it is itself an index (`base_table_id` then names its base and
/// `table_ids` is meaningless — the scan stops early), or it is a base
/// table with these index table_ids (empty if none). DROP TABLE rejects
/// the former (dropping an index tree out from under its base would
/// strand the base's PK; Postgres says use DROP INDEX), then appends
/// the base to `tids` and drops the lot together; DROP INDEX requires
/// it and uses `base_table_id` to find the IndexDef.
pub const IndexScan = struct {
    is_index: bool,
    base_table_id: ?u32,
    table_ids: std.ArrayList(u32),
};

/// One full scan of namespace `ns`'s small index catalog, classifying
/// `table_id` per IndexScan. Key-only: no row decode, no schema needed.
/// `tids` is allocated with `alloc`.
pub fn scanIndexKeys(
    alloc: std.mem.Allocator,
    forest: *Forest,
    ns: u64,
    table_id: u32,
) !IndexScan {
    var ctx = IndexKeyCtx{ .a = alloc, .target = table_id };
    errdefer ctx.table_ids.deinit(alloc);
    const range = (KeyRange{}).limitToTable(ns, catalog.index_table_id);

    _ = try forest.scan(alloc, range, .{}, &ctx, IndexKeyCtx.emit);
    if (ctx.err) |e| return e;
    return .{
        .is_index = ctx.is_index,
        .base_table_id = ctx.base_table_id,
        .table_ids = ctx.table_ids,
    };
}

fn lessByColumnId(_: void, x: schema.LogicalColumn, y: schema.LogicalColumn) bool {
    return x.column_id < y.column_id;
}
