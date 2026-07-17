//! The SQL front-end: a pgwire listener over the engine. Owns the
//! connection loop, the per-connection handler, and DDL execution —
//! CREATE/DROP DATABASE and CREATE/DROP TABLE resolve names via the
//! forest's manifest state and replicate through raft. Mirrors
//! AdminServer's accept loop, and shuts down with the engine via
//! `node.isShuttingDown()`.

const std = @import("std");
const engine = @import("engine");
const fiber = @import("zfiber");
const pgwire = @import("pgwire");
const zdb = @import("zdb");

const catalog = @import("catalog.zig");
const ddl = @import("ddl.zig");
const row = @import("row.zig");
const rowkey = @import("rowkey.zig");
const schema = @import("schema.zig");
const table = @import("table.zig");
const value = @import("value.zig");

const log = std.log.scoped(.sql);

/// The default database, present from a node's very first boot — created
/// by the WAL's genesis entry (see egres seedGenesis), never at runtime.
/// Sessions that name no database land here (psql defaults dbname to the
/// user name; that also misses and falls back per onStartup).
pub const default_database = "egres";

pub const Server = struct {
    engine: *engine.Engine,
    listen_addr: fiber.net.IpAddress,
    /// Compiled layout of the CATALOG table's own rows — NOT a user-table
    /// schema. Every namespace's catalog (table 1) has the identical
    /// compiled-in schema (catalog.logical_schema; it can't live in a
    /// catalog, that would be circular), so one compile serves all
    /// databases. User tables' schemas live as rows inside each catalog
    /// and get their own per-(table, version) compiles when reads land.
    catalog_ps: schema.PhysicalSchema,
    /// Serializes DDL: the leader-side name-uniqueness checks in
    /// buildCreateNamespace/buildCreateTable are only sound if one DDL
    /// runs at a time.
    ddl_mu: fiber.Mutex(.tracked) = .{},
    /// Per-table cache: compiled schema + egid counter, keyed by (namespace,
    /// table_id), so INSERT/SELECT don't re-scan the catalog per query. Guards
    /// the map AND every cached table's `next_egid` (mutated both here on
    /// hand-out and on the apply fiber via onDataApplied). Entries are
    /// heap-allocated so held pointers stay valid across map growth.
    tables_mu: fiber.Mutex(.tracked) = .{},
    tables: std.AutoHashMap(zdb.key.TableKey, *table.Table),

    pub fn init(eng: *engine.Engine, addr: []const u8) !Server {
        return .{
            .engine = eng,
            .listen_addr = try fiber.net.IpAddress.parseIpAndPort(addr),
            .catalog_ps = try schema.compile(eng.allocator, catalog.logical_schema, catalog.logical_schema.current_version),
            .tables = std.AutoHashMap(zdb.key.TableKey, *table.Table).init(eng.allocator),
        };
    }

    pub fn deinit(self: *Server) void {
        self.catalog_ps.deinit(self.engine.allocator);
        var it = self.tables.valueIterator();
        while (it.next()) |t| {
            t.*.deinit();
            self.engine.allocator.destroy(t.*);
        }
        self.tables.deinit();
        self.ddl_mu.deinit();
        self.tables_mu.deinit();
    }

    /// The commit-stream hook to register with the engine (before eng.start).
    pub fn applyObserver(self: *Server) engine.ApplyObserver {
        return .{ .ctx = self, .onData = onDataApplied };
    }

    /// Apply-fiber callback (every node): advance the cached table's egid
    /// counter past every committed row id. Keeping `next_egid` ≥ committed+1
    /// on all nodes is what lets a new leader hand out ids without a re-scan.
    /// Uncached tables are skipped — they get seeded from forest.maxKey when
    /// first cached, which covers rows applied before caching.
    fn onDataApplied(ctx: *anyopaque, namespace: u64, table_id: u32, items: []const zdb.key.Item) void {
        const self: *Server = @ptrCast(@alignCast(ctx));
        self.tables_mu.lock(@src());
        defer self.tables_mu.unlock();
        const t = self.tables.get(.{ .namespace = namespace, .table_id = table_id }) orelse return;
        for (items) |item| {
            if (rowkey.decodeEgid(item.key.primary)) |egid| {
                if (egid + 1 > t.next_egid) t.next_egid = egid + 1;
            }
        }
    }

    /// The cached Table for (namespace, table_id), loading + seeding it on
    /// first use. Caller holds `tables_mu`.
    fn openTableLocked(self: *Server, ns: u64, table_id: u32) !*table.Table {
        const key = zdb.key.TableKey{ .namespace = ns, .table_id = table_id };
        if (self.tables.get(key)) |t| return t;

        var loaded = try table.loadTableSchema(self.engine.allocator, self.engine.forest, &self.catalog_ps, ns, table_id);
        errdefer loaded.deinit();
        // Seed the egid counter past the table's max applied row id (covers
        // snapshot SSTs the apply observer never saw). Empty table → start at 1.
        if (try self.engine.forest.maxKey(self.engine.allocator, ns, table_id)) |mk| {
            defer mk.free(self.engine.allocator);
            loaded.next_egid = (rowkey.decodeEgid(mk.primary) orelse 0) + 1;
        } else {
            loaded.next_egid = 1;
        }

        const t = try self.engine.allocator.create(table.Table);
        errdefer self.engine.allocator.destroy(t);
        t.* = loaded;
        try self.tables.put(key, t);
        return t;
    }

    /// The cached Table for (namespace, table_id) — schema for INSERT/SELECT.
    pub fn openTable(self: *Server, ns: u64, table_id: u32) !*table.Table {
        self.tables_mu.lock(@src());
        defer self.tables_mu.unlock();
        return self.openTableLocked(ns, table_id);
    }

    /// Reserve the next egid for a table (leader path). `catchUp` guarantees
    /// the committed tail is applied — and thus observed into `next_egid` —
    /// before we hand one out.
    pub fn allocEgid(self: *Server, ns: u64, table_id: u32) !u64 {
        try self.engine.catchUp();
        self.tables_mu.lock(@src());
        defer self.tables_mu.unlock();
        const t = try self.openTableLocked(ns, table_id);
        const egid = t.next_egid;
        t.next_egid += 1;
        return egid;
    }

    pub fn runListener(self: *Server) void {
        const listener = self.listen_addr.listen(.{ .reuse_address = true }) catch |err| {
            log.err("listen failed: {s}", .{@errorName(err)});
            return;
        };
        defer listener.close();
        // Break the blocking `accept()` at shutdown (see AdminServer): a watcher
        // fiber self-connects to our address when the shutdown channel closes.
        fiber.spawnDetached(.{}, engine.Engine.stopListenerOnShutdown, .{ self.engine, self.listen_addr }) catch {};
        log.info("sql listener started on port {d} (pgwire)", .{listener.port()});
        while (!self.engine.node.isShuttingDown()) {
            const stream = listener.accept(.{}) catch |err| {
                log.err("accept failed: {s}", .{@errorName(err)});
                return;
            };
            // Shutdown watcher's self-connection: drop it and exit.
            if (self.engine.node.isShuttingDown()) {
                stream.close();
                return;
            }
            fiber.spawnDetached(.{}, handleConn, .{ self, stream }) catch {
                stream.close();
                continue;
            };
        }
    }
};

fn handleConn(self: *Server, stream: fiber.net.Stream) void {
    var s = stream;
    defer s.close();

    var handler = Handler{ .srv = self };
    pgwire.serve(self.engine.allocator, &s, &handler, .{}) catch |err| {
        log.warn("conn serve error: {s}", .{@errorName(err)});
    };
}

/// pgwire handler. Only `onQuery` is required by `pgwire.serve`
/// (onStartup/onExecute/onDescribe are @hasDecl-gated). Parses via
/// libpg_query and dispatches the DDL this slice supports; everything else
/// is a clean error.
const Handler = struct {
    srv: *Server,
    /// The session's namespace, resolved once from the startup packet's
    /// database (Postgres resolves at connect too). 0 = the name didn't
    /// exist at connect; every query then fails with 3D000. Re-validated
    /// per DDL — the database can be dropped mid-session, and a dead id
    /// must not reach the manifest (nextTableId's getOrPut would resurrect
    /// its counter entry).
    namespace: u64 = 0,

    pub fn onStartup(self: *Handler, state: *pgwire.ConnState, params: pgwire.proto.StartupParams) !void {
        _ = state;
        const requested = params.database orelse "";
        self.namespace = self.srv.engine.forest.lookupNamespace(
            if (requested.len > 0) requested else default_database,
        ) orelse 0;
    }

    /// The session's namespace if it still exists, else a SQL error and null.
    fn sessionNamespace(self: *Handler, state: *pgwire.ConnState) !?u64 {
        if (self.namespace != 0 and self.srv.engine.forest.hasNamespace(self.namespace))
            return self.namespace;
        try replyErrorCode(state, pgwire.sqlstate.invalid_catalog_name, "database does not exist");
        return null;
    }

    /// DDL preamble (caller holds ddl_mu): this node must be a CAUGHT-UP
    /// leader before the read-then-propose sequence reads any applied state
    /// (see Engine.catchUp). Replies and returns false otherwise.
    fn ddlReady(self: *Handler, state: *pgwire.ConnState) !bool {
        self.srv.engine.catchUp() catch |err| {
            switch (err) {
                error.NotLeader => try replyError(state, "not the leader"),
                else => {
                    log.err("ddl catch-up barrier: {s}", .{@errorName(err)});
                    try replyError(state, "raft catch-up failed");
                },
            }
            return false;
        };
        return true;
    }

    pub fn onQuery(self: *Handler, state: *pgwire.ConnState, sql_text: []const u8) !void {
        const alloc = self.srv.engine.allocator;
        const trimmed = std.mem.trim(u8, sql_text, " \t\r\n;");
        if (trimmed.len == 0) {
            try state.sendEmptyQueryResponse();
            return;
        }
        var stmt = ddl.parseDdl(alloc, sql_text) catch {
            try state.sendError(.{
                .code = pgwire.sqlstate.syntax_error,
                .message = "syntax error",
            });
            return;
        };
        defer stmt.deinit(alloc);
        switch (stmt) {
            .create_database => |name| try self.createDatabase(state, name),
            .drop_database => |name| try self.dropDatabase(state, name),
            .create_table => |ct| try self.createTable(state, ct),
            .drop_table => |dt| try self.dropTable(state, dt),
            .insert => |ins| try self.insert(state, ins),
            .select => |sel| try self.select(state, sel),
            .invalid => |msg| try replyErrorCode(state, pgwire.sqlstate.feature_not_supported, msg),
            .unsupported => try replyError(state, "unsupported statement"),
        }
    }

    fn createDatabase(self: *Handler, state: *pgwire.ConnState, name: []const u8) !void {
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

    fn dropDatabase(self: *Handler, state: *pgwire.ConnState, name: []const u8) !void {
        const eng = self.srv.engine;
        self.srv.ddl_mu.lock(@src());
        defer self.srv.ddl_mu.unlock();
        if (!try self.ddlReady(state)) return;

        if (std.mem.eql(u8, name, default_database))
            return replyError(state, "cannot drop the default database");
        const ns = eng.forest.lookupNamespace(name) orelse
            return replyError(state, "database does not exist");

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
        log.info("dropped database '{s}' (ns={d})", .{ name, ns });
        try state.sendCommandComplete("DROP DATABASE");
    }

    fn createTable(self: *Handler, state: *pgwire.ConnState, ct: ddl.CreateTable) !void {
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

        const created = eng.forest.buildCreateTable(a, ns, ct.name) catch |err| switch (err) {
            error.NameExists => return replyErrorCode(state, pgwire.sqlstate.duplicate_table, "table already exists"),
            else => |e| {
                log.err("create table '{s}': {s}", .{ ct.name, @errorName(e) });
                return replyError(state, "create table failed");
            },
        };
        eng.proposeMC(created.change, .apply) catch |err| {
            log.err("create table '{s}': propose: {s}", .{ ct.name, @errorName(err) });
            return replyError(state, "create table proposal failed");
        };
        // Phase 2: the table's schema rows into the namespace's catalog. A
        // crash between the two proposals leaves a named, column-less table
        // (DROP TABLE cleans it up) — acceptable pre-production.
        // TODO: Send multiple proposals together, to avoid multiple round trips.
        self.writeCatalogRows(a, ns, created.table_id, ct.columns) catch |err| {
            log.err("create table '{s}': catalog rows: {s}", .{ ct.name, @errorName(err) });
            return replyError(state, "create table failed writing catalog rows");
        };

        log.info("created table '{s}' -> ns={d} table_id={d} columns={d}", .{ ct.name, ns, created.table_id, ct.columns.len });
        try state.sendCommandComplete("CREATE TABLE");
    }

    /// Marshal one catalog row per column and replicate the batch into the
    /// namespace's catalog table, waiting for local apply.
    fn writeCatalogRows(self: *Handler, a: std.mem.Allocator, ns: u64, table_id: u32, columns: []const ddl.ColumnDef) !void {
        if (columns.len == 0) return;
        const eng = self.srv.engine;

        const KV = struct { key: []const u8, value: []const u8 };
        const kvs = try a.alloc(KV, columns.len);
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(eng.allocator);
        for (columns, 0..) |c, i| {
            const lc = schema.LogicalColumn{
                .column_id = @intCast(i + 1), // 1-based, declaration order
                .name = c.name,
                .ty = c.ty,
                .pkey_position = if (c.pkey) 1 else null,
            };
            const cells = catalog.encodeColumn(table_id, lc);
            const bytes = try row.marshalRow(eng.allocator, &self.srv.catalog_ps, &cells, &buf);
            const key = catalog.encodeKey(table_id, lc.column_id);
            // marshalRow reuses `buf` — copy both key and row out.
            kvs[i] = .{ .key = try a.dupe(u8, &key), .value = try a.dupe(u8, bytes) };
        }
        try eng.proposeKVs(ns, catalog.table_id, kvs, .apply);
    }

    fn dropTable(self: *Handler, state: *pgwire.ConnState, dt: ddl.DropTable) !void {
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
        // Drops the table's trees and name. Its schema rows in the CATALOG
        // tree stay behind as unreachable garbage (ids are never reused).
        // TODO: issue tombstones for the dropped table's catalog rows once
        // the data path supports deletes.
        const mc = eng.forest.buildDropTable(arena.allocator(), ns, table_id) catch |err| {
            log.err("drop table '{s}': {s}", .{ dt.name, @errorName(err) });
            return replyError(state, "drop table failed");
        };
        eng.proposeMC(mc, .apply) catch |err| {
            log.err("drop table '{s}': propose: {s}", .{ dt.name, @errorName(err) });
            return replyError(state, "drop table proposal failed");
        };

        log.info("dropped table '{s}' (ns={d} table_id={d})", .{ dt.name, ns, table_id });
        try state.sendCommandComplete("DROP TABLE");
    }

    fn insert(self: *Handler, state: *pgwire.ConnState, ins: ddl.Insert) !void {
        const eng = self.srv.engine;
        const ns = try self.sessionNamespace(state) orelse return;
        const table_id = eng.forest.lookupTable(ns, ins.table) orelse
            return replyErrorCode(state, pgwire.sqlstate.undefined_table, "table does not exist");

        const t = self.srv.openTable(ns, table_id) catch |err| {
            log.err("insert into '{s}': load schema: {s}", .{ ins.table, @errorName(err) });
            return replyError(state, "failed to load table schema");
        };

        var arena = std.heap.ArenaAllocator.init(eng.allocator);
        defer arena.deinit();
        const a = arena.allocator();

        // Map each VALUES position → an index into t.columns (== ps order).
        // Positional insert maps 1:1 to the live columns; an explicit list
        // resolves names. Inserting an explicit `egid` isn't supported yet.
        // TODO: allow user-provided egids once unique-index enforcement exists
        // — a below-counter egid needs a data-tree existence check, and
        // concurrent inserts of the same egid need apply-time validation.
        const positions: []usize = blk: {
            if (ins.columns) |cols| {
                const p = try a.alloc(usize, cols.len);
                for (cols, p) |name, *dst| {
                    if (std.ascii.eqlIgnoreCase(name, "egid"))
                        return replyErrorCode(state, pgwire.sqlstate.feature_not_supported, "inserting an explicit egid is not supported yet");
                    dst.* = columnIndex(t, name) orelse
                        return replyErrorCode(state, pgwire.sqlstate.undefined_column, "column does not exist");
                }
                break :blk p;
            }
            const p = try a.alloc(usize, t.columns.len);
            for (p, 0..) |*dst, i| dst.* = i;
            break :blk p;
        };

        const KV = struct { key: []const u8, value: []const u8 };
        const kvs = try a.alloc(KV, ins.rows.len);
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(eng.allocator);
        const values = try a.alloc(value.Value, t.columns.len);

        for (ins.rows, kvs) |lits, *kv| {
            if (lits.len != positions.len)
                return replyErrorCode(state, pgwire.sqlstate.syntax_error, "INSERT has the wrong number of values");
            // Omitted columns default to NULL (NOT NULL unenforced this slice).
            for (values) |*v| v.* = .null_value;
            for (lits, positions) |lit, pos| {
                values[pos] = litToValue(a, lit, t.columns[pos].ty) catch |err| switch (err) {
                    error.OutOfMemory => return err,
                    error.UnsupportedLiteral, error.UnsupportedType => return replyErrorCode(state, pgwire.sqlstate.feature_not_supported, "unsupported value or column type"),
                    else => return replyErrorCode(state, pgwire.sqlstate.invalid_text_representation, "invalid value for column type"),
                };
            }
            // marshalRow reuses `buf`; dupe the row bytes out before reuse.
            const bytes = try row.marshalRow(eng.allocator, &t.ps, values, &buf);
            const egid = self.srv.allocEgid(ns, table_id) catch |err| switch (err) {
                error.NotLeader => return replyError(state, "not the leader"),
                else => {
                    log.err("insert into '{s}': alloc egid: {s}", .{ ins.table, @errorName(err) });
                    return replyError(state, "row id allocation failed");
                },
            };
            const key = rowkey.encodeEgid(egid);
            kv.* = .{ .key = try a.dupe(u8, &key), .value = try a.dupe(u8, bytes) };
        }

        // One Mutations entry = one implicit single-statement transaction: the
        // whole VALUES list commits atomically (commit-wait — durable + ordered
        // is enough for user data; the egid counter already advanced at
        // hand-out). PRIMARY KEY is recorded but not enforced — egid is the key.
        eng.proposeKVs(ns, table_id, kvs, .commit) catch |err| {
            log.err("insert into '{s}': propose: {s}", .{ ins.table, @errorName(err) });
            return replyError(state, "insert failed");
        };

        try state.sendCommandComplete(try std.fmt.allocPrint(a, "INSERT 0 {d}", .{ins.rows.len}));
    }

    fn select(self: *Handler, state: *pgwire.ConnState, sel: ddl.Select) !void {
        const eng = self.srv.engine;
        const ns = try self.sessionNamespace(state) orelse return;
        const table_id = eng.forest.lookupTable(ns, sel.table) orelse
            return replyErrorCode(state, pgwire.sqlstate.undefined_table, "table does not exist");
        const t = self.srv.openTable(ns, table_id) catch |err| {
            log.err("select from '{s}': load schema: {s}", .{ sel.table, @errorName(err) });
            return replyError(state, "failed to load table schema");
        };

        var arena = std.heap.ArenaAllocator.init(eng.allocator);
        defer arena.deinit();
        const a = arena.allocator();

        // Resolve outputs: `egid` (from the key) or a payload column (its index
        // into ps.columns == the empty-projection decode order).
        var outs: std.ArrayList(SelectOut) = .empty;
        switch (sel.projection) {
            .star => for (t.columns) |c| {
                try outs.append(a, .{ .name = c.name, .ty = c.ty, .source = .{ .cell = psIndex(&t.ps, c.column_id) } });
            },
            .columns => |names| for (names) |nm| {
                if (std.ascii.eqlIgnoreCase(nm, "egid")) {
                    try outs.append(a, .{ .name = "egid", .ty = .int8, .source = .egid });
                } else if (t.column(nm)) |c| {
                    try outs.append(a, .{ .name = c.name, .ty = c.ty, .source = .{ .cell = psIndex(&t.ps, c.column_id) } });
                } else return replyErrorCode(state, pgwire.sqlstate.undefined_column, "column does not exist");
            },
        }

        const fields = try a.alloc(pgwire.FieldDescription, outs.items.len);
        for (outs.items, fields) |o, *fd| fd.* = .{ .name = o.name, .type_oid = value.pgTypeOid(o.ty), .format_code = 0 };
        try state.sendRowDescription(fields);

        // Filter: `egid` → one-hop point get; a payload column → per-row compare.
        var egid_filter: ?u64 = null;
        var cell_filter: ?CellFilter = null;
        if (sel.filter) |f| {
            if (std.ascii.eqlIgnoreCase(f.column, "egid")) {
                egid_filter = switch (f.literal) {
                    .int => |n| if (n >= 0) @intCast(n) else return replyErrorCode(state, pgwire.sqlstate.invalid_text_representation, "egid must be a non-negative integer"),
                    else => return replyErrorCode(state, pgwire.sqlstate.invalid_text_representation, "egid must be an integer"),
                };
            } else {
                const fc = t.column(f.column) orelse return replyErrorCode(state, pgwire.sqlstate.undefined_column, "column does not exist");
                cell_filter = .{
                    .idx = psIndex(&t.ps, fc.column_id),
                    .val = litToValue(a, f.literal, fc.ty) catch |err| switch (err) {
                        error.OutOfMemory => return err,
                        else => return replyErrorCode(state, pgwire.sqlstate.invalid_text_representation, "invalid value in WHERE"),
                    },
                };
            }
        }

        var emitter = RowEmitter{
            .state = state,
            .ps = &t.ps,
            .outs = outs.items,
            .cell_filter = cell_filter,
            .stmt_alloc = a,
        };
        defer emitter.deinit();

        if (egid_filter) |e| {
            // One-hop point get on the egid key.
            const k = rowkey.encodeEgid(e);
            const search = zdb.key.Key{ .namespace = ns, .table_id = table_id, .primary = &k, .timestamp = std.math.maxInt(u64) };
            if (try eng.forest.get(eng.allocator, search, .p0)) |item| {
                defer item.free(eng.allocator);
                _ = try emitter.emit(item);
            }
        } else {
            // Full-table scan in egid order (limitToTable confines it to this
            // table across the namespace-wide memtable).
            const scan_range = (zdb.level.KeyRange{}).limitToTable(ns, table_id);
            _ = eng.forest.scan(eng.allocator, scan_range, std.math.maxInt(u64), .p0, &emitter, RowEmitter.scanCb) catch |err| {
                log.err("select from '{s}': scan: {s}", .{ sel.table, @errorName(err) });
                return replyError(state, "scan failed");
            };
            if (emitter.err) |err| return err; // a send error mid-scan — the serve loop handles it
        }

        try state.sendCommandComplete(try std.fmt.allocPrint(a, "SELECT {d}", .{emitter.count}));
    }
};

/// Index of a table's live column by name (exact match — libpg_query lowercases
/// unquoted identifiers, matching how names are stored), or null.
fn columnIndex(t: *const table.Table, name: []const u8) ?usize {
    for (t.columns, 0..) |c, i| if (std.mem.eql(u8, c.name, name)) return i;
    return null;
}

/// Coerce a parsed VALUES literal to a cell of column type `ty`, reusing
/// value.parseValue by rendering non-string literals to text first (int/bool
/// stringified into `a`, so a text-typed column can borrow it). NULL passes
/// through; a non-literal item is `error.UnsupportedLiteral`.
fn litToValue(a: std.mem.Allocator, lit: ddl.Literal, ty: value.Type) !value.Value {
    const text: []const u8 = switch (lit) {
        .null_value => return .null_value,
        .unsupported => return error.UnsupportedLiteral,
        .str, .float => |s| s,
        .int => |n| try std.fmt.allocPrint(a, "{d}", .{n}),
        .bool_ => |b| if (b) "t" else "f",
    };
    return value.parseValue(ty, text);
}

/// Where a SELECT output column's value comes from.
const SelectSource = union(enum) { egid, cell: usize };
const SelectOut = struct { name: []const u8, ty: value.Type, source: SelectSource };
/// A resolved `column = value` filter: the column's decode-cell index + target.
const CellFilter = struct { idx: usize, val: value.Value };

/// Index of a column_id in ps.columns (== its cell index when decoding with an
/// empty projection). The column came from t.columns, so it's always present.
fn psIndex(ps: *const schema.PhysicalSchema, column_id: u32) usize {
    for (ps.columns, 0..) |c, i| if (c.column_id == column_id) return i;
    unreachable;
}

/// Turns each scanned/fetched row into a projected, filtered DataRow on the
/// wire. Holds the projection + filter so per-row work is one method call —
/// used directly for a point get and via `scanCb` for a scan. Columns format
/// straight into the frame (pgwire DataRowWriter), which the connection's
/// buffered writer batches to the socket.
const RowEmitter = struct {
    state: *pgwire.ConnState,
    ps: *const schema.PhysicalSchema,
    outs: []const SelectOut,
    cell_filter: ?CellFilter,
    stmt_alloc: std.mem.Allocator, // backs the reused `decoded` row
    decoded: value.Row = .{},
    count: u64 = 0,
    err: ?anyerror = null,

    fn deinit(self: *RowEmitter) void {
        self.decoded.deinit(self.stmt_alloc);
    }

    /// Filter, project, and send one row (true if sent, false if filtered out).
    fn emit(self: *RowEmitter, item: zdb.key.Item) !bool {
        try row.unmarshalRow(self.stmt_alloc, self.ps, item.value, &.{}, &self.decoded);
        if (self.cell_filter) |cf| {
            if (!value.valuesEqual(self.decoded.at(cf.idx), cf.val)) return false;
        }
        var dw = try self.state.beginDataRow(@intCast(self.outs.len));
        for (self.outs) |o| {
            const v: value.Value = switch (o.source) {
                .egid => .{ .int8 = @intCast(rowkey.decodeEgid(item.key.primary) orelse 0) },
                .cell => |idx| self.decoded.at(idx),
            };
            if (v == .null_value) {
                try dw.writeNull();
            } else {
                try value.formatValue(try dw.beginColumn(), v);
                dw.endColumn();
            }
        }
        try dw.finish();
        self.count += 1;
        return true;
    }

    /// forest.scan callback: emit, capturing any error so the scan stops.
    fn scanCb(raw: *anyopaque, item: zdb.key.Item) anyerror!bool {
        const self: *RowEmitter = @ptrCast(@alignCast(raw));
        _ = self.emit(item) catch |e| {
            self.err = e;
            return false;
        };
        return true;
    }
};

fn replyError(state: *pgwire.ConnState, msg: []const u8) !void {
    try state.sendError(.{
        .code = pgwire.sqlstate.internal_error,
        .message = msg,
    });
}

fn replyErrorCode(state: *pgwire.ConnState, code: [5]u8, msg: []const u8) !void {
    try state.sendError(.{ .code = code, .message = msg });
}
