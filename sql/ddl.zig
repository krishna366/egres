//! DDL execution: CREATE/DROP DATABASE, TABLE and INDEX. Each is a
//! choreography over the forest's manifest (name reservation, tree
//! creation, catalog rows) replicated through raft — the data plane
//! proper lives in dml.zig. CREATE INDEX also runs the online backfill
//! that fills a fresh index tree from the base table.
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
    // downstream — the PK index descriptor and the catalog rows — reads
    // ids from this list, never re-derives them from position. The
    // PRIMARY KEY column (parse allows at most one) gets an auto unique
    // index on its id.
    const columns = try a.alloc(_schema.LogicalColumn, ct.columns.len);
    var pkey_col: ?u32 = null;
    for (ct.columns, columns, 1..) |c, *lc, id| {
        lc.* = .{
            .column_id = @intCast(id),
            .name = c.name,
            .ty = c.ty,
            .pkey_position = if (c.pkey) 1 else null,
        };
        if (c.pkey) pkey_col = lc.column_id;
    }

    // The table's tree, plus the PK index's own tree named "<table>_pkey"
    // (Postgres convention) — both created in one atomic change. A name
    // collision on either fails the CREATE.
    var names: [2][]const u8 = .{ ct.name, "" };
    var n_names: usize = 1;
    if (pkey_col != null) {
        names[1] = try std.fmt.allocPrint(a, "{s}_pkey", .{ct.name});
        n_names = 2;
    }

    const created = eng.forest.buildCreateTables(
        a,
        ns,
        names[0..n_names],
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
    // Phase 2: the table's column rows (+ the index descriptor row, if
    // any) into the namespace's catalogs — one proposal, atomic. A crash
    // between the two proposals leaves a named, column-less table (DROP
    // TABLE cleans it up) — acceptable pre-production.
    var indexes: [1]_catalog.IndexDef = undefined;
    var n_indexes: usize = 0;
    if (pkey_col) |col| {
        indexes[0] = .{
            .base_table_id = created.table_ids[0],
            .index_table_id = created.table_ids[1],
            .column_id = col,
            .unique = true,
        };
        n_indexes = 1;
    }
    writeCatalogRows(self, a, ns, created.table_ids[0], columns, indexes[0..n_indexes]) catch |err| {
        log.err("create table '{s}': catalog rows: {s}", .{ ct.name, @errorName(err) });
        return replyError(state, "create table failed writing catalog rows");
    };

    if (n_indexes > 0)
        log.info("created table '{s}' -> ns={d} table_id={d} columns={d} pkey_index_id={d}", .{ ct.name, ns, created.table_ids[0], ct.columns.len, indexes[0].index_table_id })
    else
        log.info("created table '{s}' -> ns={d} table_id={d} columns={d}", .{ ct.name, ns, created.table_ids[0], ct.columns.len });
    try state.sendCommandComplete("CREATE TABLE");
}

/// Marshal one catalog row per column plus one per index descriptor and
/// replicate the batch into the namespace's catalog trees, waiting for
/// local apply. Column rows land in table 1, index rows in table 2 —
/// mixed table_ids in one atomic proposal. `columns` carry their minted
/// ids (createTable assigns them); nothing is derived here.
fn writeCatalogRows(
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

/// CREATE INDEX — the online-backfill protocol. Blocks the
/// issuing session until the index is built and valid (the reply
/// is a truthful completion signal, as in Postgres), but OTHER
/// sessions' writes flow throughout — CONCURRENTLY's semantics.
/// In order:
///  1. create the index tree (raft, applied);
///  2. reserve the tree against compactions for the whole build —
///     keeps its bottom level empty, the install condition;
///  3. take the exclusive table intent as a BARRIER only: drains
///     statements still holding a pre-index Table (openTable runs
///     after dmlTxn, so post-barrier statements load the reload);
///  4. under it: catalog row (valid=false) + reloadTable — every
///     future writer maintains the index; snap = that row's own
///     commit HLC, read back from the row. Durable and
///     self-describing: any node could re-run this backfill from
///     the catalog row alone;
///  5. release the barrier — writes flow for the entire build,
///     every posting delta landing at commit HLC > snap;
///  6. scan the base table at snap, fold per-key roaring states,
///     install them as BOTTOM-level SSTs at ts = snap: the oldest
///     data this tree will ever hold, which is exactly what the
///     bottom level means — level-ordering holds against all the
///     deltas above;
///  7. retake the table intent as a barrier again, and under it flip
///     valid=true + reloadTable — reads may now use it. The reload
///     destroys the cached Table, which is only safe while no
///     statement holds it (DML holds the shared intent from dmlTxn
///     to txn end); the barrier drains them and parks new ones.
/// Crash/failure windows: before 4 → an invisible orphan tree;
/// after 4 → an INVALID index accumulating deltas, never
/// consulted (DROP INDEX cleans it up; a re-run backfill could
/// finish it).
pub fn createIndex(self: *Handler, state: *pgwire.ConnState, ci: _parse.CreateIndex) !void {
    const eng = self.srv.engine;
    self.srv.ddl_mu.lock(@src());
    defer self.srv.ddl_mu.unlock();
    if (!try self.ddlReady(state)) return;

    const ns = try self.sessionNamespace(state) orelse return;
    const base_tid = eng.forest.lookupTable(ns, ci.table) orelse
        return replyErrorCode(
            state,
            pgwire.sqlstate.undefined_table,
            "table does not exist",
        );
    const t = self.srv.openTable(ns, base_tid) catch |err| {
        logCreateIndexErr(ci.table, "load schema", err);
        return replyError(state, "failed to load table schema");
    };
    const col = t.column(ci.column) orelse
        return replyErrorCode(
            state,
            pgwire.sqlstate.undefined_column,
            "column does not exist",
        );
    if (col.ty == .interval)
        return replyErrorCode(
            state,
            pgwire.sqlstate.feature_not_supported,
            "indexes on interval columns are not supported",
        );
    var arena = std.heap.ArenaAllocator.init(eng.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Same NAME first — that's this exact index existing, the
    // IF NOT EXISTS idempotence case — before the one-index-per-
    // column limit, which is about a SECOND index on the column.
    const name = ci.name orelse
        try std.fmt.allocPrint(a, "{s}_{s}_idx", .{ ci.table, ci.column });
    if (eng.forest.lookupTable(ns, name) != null) {
        if (ci.if_not_exists)
            return state.sendCommandComplete("CREATE INDEX");
        return replyErrorCode(
            state,
            pgwire.sqlstate.duplicate_table,
            "index already exists",
        );
    }
    // One non-unique index per column — postingIndexes' invariant.
    for (t.indexes) |idx| {
        if (!idx.unique and idx.column == col)
            return replyErrorCode(
                state,
                pgwire.sqlstate.feature_not_supported,
                "column is already indexed",
            );
    }
    // `t` (and `col` into it) dangle past reloadTable below; keep
    // what's needed.
    const column_id = col.column_id;

    const created = eng.forest.buildCreateTables(
        a,
        ns,
        &.{name},
    ) catch |err| switch (err) {
        error.NameExists => return replyErrorCode(
            state,
            pgwire.sqlstate.duplicate_table,
            "index already exists",
        ),
        else => {
            logCreateIndexErr(name, "create tree", err);
            return replyError(state, "create index failed");
        },
    };
    eng.proposeMC(created.change, .apply) catch |err| {
        logCreateIndexErr(name, "propose tree", err);
        return replyError(state, "create index failed");
    };
    const index_tid = created.table_ids[0];

    const tree_key = zdb.kv.TreeKey{
        .namespace = ns,
        .tree_id = zdb.kv.makeTreeId(index_tid, 0),
    };
    try eng.reserveTree(tree_key);
    defer eng.releaseTree(tree_key);

    const owner = self.srv.next_txn_id.fetchAdd(1, .monotonic);
    const db = try self.srv.database(ns);
    const nskey = zdb.kv.namespaceHash(ns);

    _ = db.locks.acquire(nskey, owner, .shared) catch |err|
        return replyLockErr(state, err);
    defer db.locks.release(nskey, owner);

    const tkey = (zdb.kv.TableKey{
        .namespace = ns,
        .table_id = base_tid,
    }).identityHash();
    _ = db.locks.acquire(tkey, owner, .exclusive) catch |err|
        return replyLockErr(state, err);

    var barrier_held = true;
    defer if (barrier_held) db.locks.release(tkey, owner);

    const def = _catalog.IndexDef{
        .base_table_id = base_tid,
        .index_table_id = index_tid,
        .column_id = column_id,
        .unique = false,
        .valid = false,
    };
    writeCatalogRows(self, a, ns, base_tid, &.{}, &.{def}) catch |err| {
        logCreateIndexErr(name, "catalog row", err);
        return replyError(state, "create index failed");
    };
    self.srv.reloadTable(ns, base_tid) catch |err| {
        logCreateIndexErr(name, "reload table", err);
        return replyError(state, "create index failed");
    };
    const snap = blk: {
        const ck = _catalog.encodeIndexKey(base_tid, index_tid);
        const found = eng.forest.get(eng.allocator, .{
            .namespace = ns,
            .table_id = _catalog.index_table_id,
            .primary = &ck,
            .timestamp = std.math.maxInt(u64),
        }, .p0) catch |err| {
            logCreateIndexErr(name, "read snap", err);
            return replyError(state, "create index failed");
        } orelse {
            logCreateIndexErr(name, "read snap", error.RowMissing);
            return replyError(state, "create index failed");
        };
        defer found.free(eng.allocator);
        break :blk found.key.timestamp;
    };

    db.locks.release(tkey, owner);
    barrier_held = false;
    log.info("create index '{s}': backfill starting (ns={d} base={d} index={d} snap={d})", .{ name, ns, base_tid, index_tid, snap });

    const t2 = self.srv.openTable(ns, base_tid) catch |err| {
        logCreateIndexErr(name, "reopen schema", err);
        return replyError(state, "create index failed");
    };
    const col2 = t2.column(ci.column) orelse {
        logCreateIndexErr(name, "reopen schema", error.ColumnMissing);
        return replyError(state, "create index failed");
    };
    const n_keys = backfillIndex(
        self,
        a,
        ns,
        base_tid,
        index_tid,
        t2,
        col2,
        snap,
    ) catch |err| {
        logCreateIndexErr(name, "backfill", err);
        return replyError(state, "create index failed");
    };
    if (n_keys > 0) {
        // applyBackfill skips (deterministically, on every node)
        // when the bottom level was not empty; surface that as a
        // hard failure instead of flipping a hollow index valid.
        const installed = eng.forest.bottomTableCount(ns, index_tid) catch 0;
        if (installed == 0) {
            logCreateIndexErr(name, "install", error.BackfillSkipped);
            return replyError(state, "create index failed");
        }
    }

    // Barrier again for the flip: reloadTable below frees the Table
    // every in-flight statement on this base holds.
    _ = db.locks.acquire(tkey, owner, .exclusive) catch |err|
        return replyLockErr(state, err);
    barrier_held = true;

    var vdef = def;
    vdef.valid = true;
    writeCatalogRows(self, a, ns, base_tid, &.{}, &.{vdef}) catch |err| {
        logCreateIndexErr(name, "valid flip", err);
        return replyError(state, "create index failed");
    };
    self.srv.reloadTable(ns, base_tid) catch |err| {
        logCreateIndexErr(name, "reload table", err);
        return replyError(state, "create index failed");
    };

    log.info("created index '{s}' (ns={d} base={d} index={d} keys={d} snap={d})", .{ name, ns, base_tid, index_tid, n_keys, snap });
    try state.sendCommandComplete("CREATE INDEX");
}

/// Step 6 of the protocol: scan the base table at read_ts =
/// `snap`, fold each row's indexed cell into a per-key roaring
/// bitmap, and install the result as bottom-level SSTs of
/// {.roaring, .state} values at ts = snap (cmd .index_backfill).
/// NULL cells are indexed (the tag_null key); interval never gets
/// here (rejected at CREATE INDEX). Memory ∝ table — distinct
/// keys + bitmaps — accepted for v1. Returns the number of
/// posting keys written; 0 = empty table, nothing installed.
fn backfillIndex(
    self: *Handler,
    a: std.mem.Allocator,
    ns: u64,
    base_tid: u32,
    index_tid: u32,
    t: *const _table.Table,
    col: *const _table.Column,
    snap: u64,
) !usize {
    const eng = self.srv.engine;
    const Fold = struct {
        a: std.mem.Allocator,
        t: *const _table.Table,
        col: *const _table.Column,
        rows: usize = 0,
        map: std.StringArrayHashMapUnmanaged(zdb.zroar.Bitmap) = .empty,

        fn emit(ctx: *@This(), item: zdb.kv.Item) anyerror!bool {
            const egid = _key.decodeEgid(item.key.primary) orelse
                return true; // not row-shaped: nothing to index
            ctx.rows += 1;
            const cell = ctx.t.read(item, ctx.col);
            const ik = _key.encodeIndexKey(ctx.a, cell, null) catch |err|
                switch (err) {
                    error.Unencodable => unreachable,
                    else => return err,
                };
            const gop = try ctx.map.getOrPut(ctx.a, ik);
            if (!gop.found_existing)
                gop.value_ptr.* = try zdb.zroar.Bitmap.init(ctx.a);
            _ = try gop.value_ptr.set(egid);
            return true;
        }
    };
    var fold = Fold{ .a = a, .t = t, .col = col };
    _ = try eng.forest.scan(
        a,
        (zdb.level.KeyRange{}).limitToTable(ns, base_tid),
        .{ .read_ts = snap, .priority = .p2 },
        &fold,
        Fold.emit,
    );
    log.info("backfill index={d}: scanned rows={d} keys={d}", .{ index_tid, fold.rows, fold.map.count() });
    if (fold.map.count() == 0) return 0;

    // The map collected in first-seen order; the Builder wants
    // key order.
    const SortCtx = struct {
        keys: []const String,
        pub fn lessThan(c: @This(), x: usize, y: usize) bool {
            return std.mem.lessThan(u8, c.keys[x], c.keys[y]);
        }
    };
    fold.map.sortUnstable(SortCtx{ .keys = fold.map.keys() });

    var items: std.ArrayList(zdb.kv.Item) = .empty;
    for (fold.map.keys(), fold.map.values()) |ik, *bm| {
        // Canonical minimal buffer for persistence.
        try bm.compact();
        try items.append(a, .{
            .key = .{
                .namespace = ns,
                .table_id = index_tid,
                .primary = ik,
                .timestamp = snap,
            },
            .value = bm.toBuffer(),
            .meta = .{ .kind = .state, .encoding = .roaring },
        });
    }
    var changes =
        try eng.forest.buildBottomTable(a, ns, index_tid, items.items);
    defer changes.deinit(a);
    // Upload + announce the backfill SSTs before proposing, like every
    // other SST creator — followers download from the shared store.
    try eng.distributeNewFiles(changes.items);
    try eng.proposeMC(.{
        .cmd = .index_backfill,
        .sstable_changes = changes.items,
        .tree_changes = &.{},
        .memtable_id = 0,
    }, .apply);
    log.info("backfill index={d}: installed tables={d}", .{ index_tid, changes.items.len });
    return fold.map.count();
}

/// DROP INDEX. Order matters: tombstone the catalog row FIRST,
/// then drop the tree — the reverse would leave a live IndexDef
/// pointing at a dead tree (reads would see silently-empty
/// results), and unlike dropTable's tolerated catalog garbage, a
/// live IndexDef row would resurrect the index on the next schema
/// load. The tombstone is mandatory.
pub fn dropIndex(self: *Handler, state: *pgwire.ConnState, di: _parse.DropIndex) !void {
    const eng = self.srv.engine;
    self.srv.ddl_mu.lock(@src());
    defer self.srv.ddl_mu.unlock();
    if (!try self.ddlReady(state)) return;

    const ns = try self.sessionNamespace(state) orelse return;
    const index_tid = eng.forest.lookupTable(ns, di.name) orelse {
        if (di.missing_ok) return state.sendCommandComplete("DROP INDEX");
        return replyErrorCode(
            state,
            pgwire.sqlstate.undefined_object,
            "index does not exist",
        );
    };

    var arena = std.heap.ArenaAllocator.init(eng.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var scan = _table.scanIndexKeys(
        a,
        eng.forest,
        ns,
        index_tid,
    ) catch |err| {
        logDropIndexErr(di.name, "scan catalog", err);
        return replyError(state, "drop index failed");
    };
    defer scan.table_ids.deinit(a);
    if (!scan.is_index)
        return replyErrorCode(
            state,
            pgwire.sqlstate.wrong_object_type,
            "not an index; use DROP TABLE",
        );
    const base_tid = scan.base_table_id.?;

    const defs = _table.loadIndexes(
        a,
        eng.forest,
        &self.srv.index_catalog_ps,
        ns,
        base_tid,
    ) catch |err| {
        logDropIndexErr(di.name, "load defs", err);
        return replyError(state, "drop index failed");
    };
    const def = for (defs) |d| {
        if (d.index_table_id == index_tid) break d;
    } else {
        logDropIndexErr(di.name, "find def", error.RowMissing);
        return replyError(state, "drop index failed");
    };
    // The pkey index is structural — it lives and dies with its
    // table.
    if (def.unique)
        return replyErrorCode(
            state,
            pgwire.sqlstate.feature_not_supported,
            "cannot drop a PRIMARY KEY index",
        );

    // Locks: same shape as dropTable, on the BASE table — the
    // reload below swaps its cached schema.
    const owner = self.srv.next_txn_id.fetchAdd(1, .monotonic);
    const db = try self.srv.database(ns);
    const nskey = zdb.kv.namespaceHash(ns);
    _ = db.locks.acquire(nskey, owner, .shared) catch |err|
        return replyLockErr(state, err);
    defer db.locks.release(nskey, owner);

    const tkey = (zdb.kv.TableKey{
        .namespace = ns,
        .table_id = base_tid,
    }).identityHash();
    _ = db.locks.acquire(tkey, owner, .exclusive) catch |err|
        return replyLockErr(state, err);
    defer db.locks.release(tkey, owner);

    const ck = _catalog.encodeIndexKey(base_tid, index_tid);
    const wire_key = (zdb.kv.Key{
        .namespace = ns,
        .table_id = _catalog.index_table_id,
        .primary = &ck,
        .timestamp = 0,
    }).marshal(a) catch |err| {
        logDropIndexErr(di.name, "marshal key", err);
        return replyError(state, "drop index failed");
    };

    var kvs = [_]zen.KeyValueStruct{.{
        .key = wire_key,
        .value = "",
        .meta = (zdb.kv.Meta{ .kind = .deleted }).toByte(),
    }};
    eng.proposeWrites(ns, &kvs, .apply) catch |err| {
        logDropIndexErr(di.name, "tombstone", err);
        return replyError(state, "drop index failed");
    };

    const mc = eng.forest.buildDropTables(a, ns, &.{index_tid}) catch |err| {
        logDropIndexErr(di.name, "drop tree", err);
        return replyError(state, "drop index failed");
    };
    eng.proposeMC(mc, .apply) catch |err| {
        logDropIndexErr(di.name, "propose drop", err);
        return replyError(state, "drop index failed");
    };
    self.srv.reloadTable(ns, base_tid) catch |err| {
        logDropIndexErr(di.name, "reload table", err);
        return replyError(state, "drop index failed");
    };

    log.info("dropped index '{s}' (ns={d} base={d} index={d})", .{ di.name, ns, base_tid, index_tid });
    try state.sendCommandComplete("DROP INDEX");
}

fn logCreateIndexErr(name: []const u8, what: []const u8, err: anyerror) void {
    log.err(
        "create index '{s}': {s}: {s}",
        .{ name, what, @errorName(err) },
    );
}

fn logDropIndexErr(name: []const u8, what: []const u8, err: anyerror) void {
    log.err(
        "drop index '{s}': {s}: {s}",
        .{ name, what, @errorName(err) },
    );
}
