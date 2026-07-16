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

const catalog = @import("catalog.zig");
const ddl = @import("ddl.zig");
const row = @import("row.zig");
const schema = @import("schema.zig");

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

    pub fn init(eng: *engine.Engine, addr: []const u8) !Server {
        return .{
            .engine = eng,
            .listen_addr = try fiber.net.IpAddress.parseIpAndPort(addr),
            .catalog_ps = try schema.compile(eng.allocator, catalog.logical_schema, catalog.logical_schema.current_version),
        };
    }

    pub fn deinit(self: *Server) void {
        self.catalog_ps.deinit(self.engine.allocator);
        self.ddl_mu.deinit();
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
            .invalid => |msg| try replyErrorCode(state, pgwire.sqlstate.feature_not_supported, msg),
            .unsupported => try replyError(state, "only CREATE/DROP DATABASE and CREATE/DROP TABLE are supported yet"),
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
        eng.proposeAndApplyMC(created.change) catch |err| {
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
        eng.proposeAndApplyMC(mc) catch |err| {
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
        eng.proposeAndApplyMC(created.change) catch |err| {
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
        try eng.proposeAndApplyKVs(ns, catalog.table_id, kvs);
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
        eng.proposeAndApplyMC(mc) catch |err| {
            log.err("drop table '{s}': propose: {s}", .{ dt.name, @errorName(err) });
            return replyError(state, "drop table proposal failed");
        };

        log.info("dropped table '{s}' (ns={d} table_id={d})", .{ dt.name, ns, table_id });
        try state.sendCommandComplete("DROP TABLE");
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
