//! DDL execution: CREATE/DROP DATABASE and TABLE. Each is a
//! choreography over the forest's manifest (name reservation, tree
//! creation, catalog rows) replicated through raft — the data plane
//! proper lives in dml.zig, and CREATE/DROP INDEX with the online
//! backfill in index_ddl.zig.
//!
//! These take `*Handler` rather than being its methods: Zig keeps a
//! struct's methods in one file, and the handler's DDL half is its own
//! concern from its session/txn half.

const std = @import("std");
const engine = @import("engine");
const pgwire = @import("pgwire");
const zdb = @import("zdb");
const zen = zdb.buffers;

const _catalog = @import("catalog.zig");
const _parse = @import("parse.zig");
const _key = @import("key.zig");
const _row = @import("row.zig");
const _schema = @import("schema.zig");
const _table = @import("table.zig");
const _value = @import("value.zig");
const _server = @import("server.zig");

const Handler = _server.Handler;
const String = _value.String;
const log = std.log.scoped(.sql);
const replyError = _server.replyError;
const replyErrorCode = _server.replyErrorCode;
const replyLockErr = _server.replyLockErr;
const default_database = _server.default_database;

pub fn createDatabase(self: *Handler, state: *pgwire.ConnState, name: []const u8) !void {
    const eng = self.srv.engine;
    self.srv.ddl_mu.lock(@src());
    defer self.srv.ddl_mu.unlock();
    if (!try self.ddlReady(state)) return;

    var arena = std.heap.ArenaAllocator.init(eng.allocator);
    defer arena.deinit();
    const created = eng.forest.buildCreateNamespace(arena.allocator(), name) catch |err| switch (err) {
        error.NameExists => return replyError(state, "database already exists"),
        else => |e| {
            log.err("create database '{s}': {s}", .{ name, @errorName(e) });
            return replyError(state, "create database failed");
        },
    };
    eng.proposeMC(created.change, .apply) catch |err| {
        log.err("create database '{s}': propose: {s}", .{ name, @errorName(err) });
        return replyError(state, "create database proposal failed");
    };
    log.info("created database '{s}' -> ns={d}", .{ name, created.namespace });
    try state.sendCommandComplete("CREATE DATABASE");
}

pub fn dropDatabase(self: *Handler, state: *pgwire.ConnState, name: []const u8) !void {
    const eng = self.srv.engine;
    self.srv.ddl_mu.lock(@src());
    defer self.srv.ddl_mu.unlock();
    if (!try self.ddlReady(state)) return;

    if (std.mem.eql(u8, name, default_database))
        return replyError(state, "cannot drop the default database");
    const ns = eng.forest.lookupNamespace(name) orelse
        return replyError(state, "database does not exist");

    // Exclusive namespace intent: queue behind every open txn and
    // in-flight statement (they hold it shared, first lock → txn
    // end), so the drop never lands mid-transaction; writer
    // preference parks new DML behind us meanwhile. `dropped` is
    // set under the exclusive hold — a statement waking from the
    // key afterwards sees it and answers 3D000 (see dmlTxn). Fresh
    // owner id — DDL runs outside any txn; parking under ddl_mu is
    // safe (global order ddl_mu → intent, DML never takes ddl_mu).
    const db = self.srv.database(ns) catch
        return replyError(state, "internal error: database state");
    const owner = self.srv.next_txn_id.fetchAdd(1, .monotonic);
    const nskey = zdb.kv.namespaceHash(ns);
    _ = db.locks.acquire(nskey, owner, .exclusive) catch |err|
        return replyLockErr(state, err);
    defer db.locks.release(nskey, owner);

    var arena = std.heap.ArenaAllocator.init(eng.allocator);
    defer arena.deinit();
    const mc = eng.forest.buildDropNamespace(arena.allocator(), ns) catch |err| {
        log.err("drop database '{s}': {s}", .{ name, @errorName(err) });
        return replyError(state, "drop database failed");
    };
    eng.proposeMC(mc, .apply) catch |err| {
        log.err("drop database '{s}': propose: {s}", .{ name, @errorName(err) });
        return replyError(state, "drop database proposal failed");
    };
    db.dropped = true;
    log.info("dropped database '{s}' (ns={d})", .{ name, ns });
    try state.sendCommandComplete("DROP DATABASE");
}

pub fn createTable(self: *Handler, state: *pgwire.ConnState, ct: _parse.CreateTable) !void {
    const eng = self.srv.engine;
    self.srv.ddl_mu.lock(@src());
    defer self.srv.ddl_mu.unlock();
    if (!try self.ddlReady(state)) return;

    const ns = try self.sessionNamespace(state) orelse return;
    if (eng.forest.lookupTable(ns, ct.name) != null) {
        if (ct.if_not_exists) return state.sendCommandComplete("CREATE TABLE");
        return replyErrorCode(state, pgwire.sqlstate.duplicate_table, "table already exists");
    }

    var arena = std.heap.ArenaAllocator.init(eng.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Mint the column ids ONCE: 1-based declaration position. Everything
    // downstream — the index descriptors and the catalog rows — reads
    // ids from this list, never re-derives them from position. The
    // PRIMARY KEY column (parse allows at most one) and every UNIQUE
    // column get an auto unique index each, in their own trees named
    // "<table>_pkey" and "<table>_<column>_key" (Postgres convention),
    // all created with the table in one atomic change; a name
    // collision on any of them fails the CREATE.
    const columns = try a.alloc(_schema.LogicalColumn, ct.columns.len);
    var names: std.ArrayList(String) = .empty;
    try names.append(a, ct.name);
    var unique_cols: std.ArrayList(u32) = .empty;
    for (ct.columns, columns, 1..) |c, *lc, id| {
        lc.* = .{
            .column_id = @intCast(id),
            .name = c.name,
            .ty = c.ty,
            .pkey_position = if (c.pkey) 1 else null,
        };
        if (!c.pkey and !c.unique) continue;
        // The same rule as CREATE INDEX: no index of any kind on an
        // interval column (it has no memcomparable encoding), so DML
        // never meets an unencodable indexed cell.
        if (c.ty == .interval)
            return replyErrorCode(
                state,
                pgwire.sqlstate.feature_not_supported,
                "indexes on interval columns are not supported",
            );
        try unique_cols.append(a, lc.column_id);
        try names.append(a, if (c.pkey)
            try std.fmt.allocPrint(a, "{s}_pkey", .{ct.name})
        else
            try std.fmt.allocPrint(a, "{s}_{s}_key", .{ ct.name, c.name }));
    }

    const created = eng.forest.buildCreateTables(
        a,
        ns,
        names.items,
    ) catch |err| switch (err) {
        error.NameExists => return replyErrorCode(state, pgwire.sqlstate.duplicate_table, "table or index name already exists"),
        else => |e| {
            log.err("create table '{s}': {s}", .{ ct.name, @errorName(e) });
            return replyError(state, "create table failed");
        },
    };
    eng.proposeMC(created.change, .apply) catch |err| {
        log.err("create table '{s}': propose: {s}", .{ ct.name, @errorName(err) });
        return replyError(state, "create table proposal failed");
    };
    // Phase 2: the table's column rows (+ the index descriptor rows)
    // into the namespace's catalogs — one proposal, atomic. A crash
    // between the two proposals leaves a named, column-less table (DROP
    // TABLE cleans it up) — acceptable pre-production. An index made
    // with its table is valid from ts 0: no row precedes it.
    const table_id = created.table_ids[0];
    const indexes = try a.alloc(_catalog.IndexDef, unique_cols.items.len);
    for (unique_cols.items, created.table_ids[1..], indexes) |col, tid, *def| {
        def.* = .{
            .base_table_id = table_id,
            .index_table_id = tid,
            .column_id = col,
            .unique = true,
        };
    }
    writeCatalogRows(self, a, ns, table_id, columns, indexes) catch |err| {
        log.err("create table '{s}': catalog rows: {s}", .{ ct.name, @errorName(err) });
        return replyError(state, "create table failed writing catalog rows");
    };

    log.info(
        "created table '{s}' -> ns={d} table_id={d} columns={d} unique={d}",
        .{ ct.name, ns, table_id, ct.columns.len, indexes.len },
    );
    try state.sendCommandComplete("CREATE TABLE");
}

/// Marshal one catalog row per column plus one per index descriptor and
/// replicate the batch into the namespace's catalog trees, waiting for
/// local apply. Column rows land in table 1, index rows in table 2 —
/// mixed table_ids in one atomic proposal. `columns` carry their minted
/// ids (createTable assigns them); nothing is derived here.
pub fn writeCatalogRows(
    self: *Handler,
    a: std.mem.Allocator,
    ns: u64,
    table_id: u32,
    columns: []const _schema.LogicalColumn,
    indexes: []const _catalog.IndexDef,
) !void {
    const eng = self.srv.engine;
    const n = columns.len + indexes.len;
    if (n == 0) return;

    const kvs = try a.alloc(zen.KeyValueStruct, n);
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(eng.allocator);
    for (columns, 0..) |lc, i| {
        const cells = _catalog.encodeColumn(table_id, lc);
        const bytes = try _row.marshalRow(eng.allocator, &self.srv.catalog_ps, &cells, &buf);
        const primary = _catalog.encodeKey(table_id, lc.column_id);
        const k = zdb.kv.Key{
            .namespace = ns,
            .table_id = _catalog.table_id,
            .primary = &primary,
        };
        // marshalRow reuses `buf` — copy the row out; marshal copies
        // the key.
        kvs[i] = .{ .key = try k.marshal(a), .value = try a.dupe(u8, bytes) };
    }
    for (indexes, columns.len..) |idx, i| {
        const cells = _catalog.encodeIndex(idx);
        const bytes = try _row.marshalRow(
            eng.allocator,
            &self.srv.index_catalog_ps,
            &cells,
            &buf,
        );
        const primary = _catalog.encodeIndexKey(idx.base_table_id, idx.index_table_id);
        const k = zdb.kv.Key{
            .namespace = ns,
            .table_id = _catalog.index_table_id,
            .primary = &primary,
        };
        kvs[i] = .{ .key = try k.marshal(a), .value = try a.dupe(u8, bytes) };
    }
    try eng.proposeWrites(ns, kvs, .apply);
}

pub fn dropTable(self: *Handler, state: *pgwire.ConnState, dt: _parse.DropTable) !void {
    const eng = self.srv.engine;
    self.srv.ddl_mu.lock(@src());
    defer self.srv.ddl_mu.unlock();
    if (!try self.ddlReady(state)) return;

    const ns = try self.sessionNamespace(state) orelse return;
    const table_id = eng.forest.lookupTable(ns, dt.name) orelse {
        if (dt.missing_ok) return state.sendCommandComplete("DROP TABLE");
        return replyErrorCode(state, pgwire.sqlstate.undefined_table, "table does not exist");
    };

    var arena = std.heap.ArenaAllocator.init(eng.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // One key-only catalog scan answers both DROP questions: is this
    // name an index (reject), and which index trees belong to it (drop
    // with the base below). Safe pre-lock: ddl_mu serializes every
    // catalog mutation; the intent locks only fence in-flight DML.
    var scan = _table.scanIndexKeys(a, eng.forest, ns, table_id) catch |err| {
        log.err("drop table '{s}': index scan: {s}", .{ dt.name, @errorName(err) });
        return replyError(state, "drop table failed");
    };
    if (scan.is_index)
        return replyErrorCode(state, pgwire.sqlstate.wrong_object_type, "cannot DROP TABLE an index; use DROP INDEX");

    // Shared namespace intent + exclusive table intent (ns → table,
    // the global order): queue behind every open txn's shared table
    // hold so the drop never lands mid-transaction, while the ns
    // hold makes a concurrent DROP DATABASE queue behind US. Writer
    // preference parks new DML behind the pending table key. Fresh
    // owner id — DDL runs outside any txn. Safe to park while
    // holding ddl_mu: the global order is ddl_mu → intent lock (DML
    // never takes ddl_mu), so no cycle can involve both.
    const db = self.srv.database(ns) catch
        return replyError(state, "internal error: database state");
    const owner = self.srv.next_txn_id.fetchAdd(1, .monotonic);
    const nskey = zdb.kv.namespaceHash(ns);

    _ = db.locks.acquire(nskey, owner, .shared) catch |err|
        return replyLockErr(state, err);
    defer db.locks.release(nskey, owner);
    const tkey = (zdb.kv.TableKey{
        .namespace = ns,
        .table_id = table_id,
    }).identityHash();
    _ = db.locks.acquire(tkey, owner, .exclusive) catch |err|
        return replyLockErr(state, err);
    defer db.locks.release(tkey, owner);

    // The table and its index tables drop together, in one atomic change
    // — order is irrelevant. Their rows in the CATALOG trees stay behind
    // as unreachable garbage (ids are never reused). TODO: issue
    // tombstones for them.
    try scan.table_ids.append(a, table_id);
    const mc = eng.forest.buildDropTables(a, ns, scan.table_ids.items) catch |err| {
        log.err("drop table '{s}': {s}", .{ dt.name, @errorName(err) });
        return replyError(state, "drop table failed");
    };
    eng.proposeMC(mc, .apply) catch |err| {
        log.err("drop table '{s}': propose: {s}", .{ dt.name, @errorName(err) });
        return replyError(state, "drop table proposal failed");
    };
    // Evict the cached Table (safe: we hold the exclusive table
    // intent, and DML holds its Table only under the shared one).
    // Index table_ids are never cached — they have no columns.
    try self.srv.forgetTable(ns, table_id);

    log.info("dropped table '{s}' (ns={d} table_id={d} indexes={d})", .{ dt.name, ns, table_id, scan.table_ids.items.len - 1 });
    try state.sendCommandComplete("DROP TABLE");
}
