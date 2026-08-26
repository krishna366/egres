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
const zen = zdb.buffers;

const catalog = @import("catalog.zig");
const ddl = @import("ddl.zig");
const row = @import("row.zig");
const key = @import("key.zig");
const schema = @import("schema.zig");
const table = @import("table.zig");
const txn = @import("txn.zig");
const value = @import("value.zig");
const Oracle = @import("oracle.zig").Oracle;

const log = std.log.scoped(.sql);

/// Readability alias — a borrowed byte string (row bytes, encoded
/// keys). Same type as []const u8, purely a shorter spelling.
const String = []const u8;

/// The default database, present from a node's very first boot — created
/// by the WAL's genesis entry (see egres seedGenesis), never at runtime.
/// Sessions that name no database land here (psql defaults dbname to the
/// user name; that also misses and falls back per onStartup).
pub const default_database = "egres";

/// One database's transaction machinery: the lock table and the commit
/// oracle. A SQL txn is connection-bound to one database, so wait-for
/// graphs and conflict tracking are naturally namespace-local — per-ns
/// instances keep tenants off each other's mutexes and give DROP
/// DATABASE one object to tear down (teardown itself is deferred).
pub const Database = struct {
    /// This database's namespace id (implied by every key it owns).
    ns: u64,
    /// DROP DATABASE landed. Written under the EXCLUSIVE namespace
    /// intent lock, read under the shared one (see Txn.lockNamespace) —
    /// the lock is the synchronization. A statement that was parked on
    /// the namespace key while the drop ran wakes to find this set and
    /// answers 3D000 instead of touching dropped trees. (The manifest is
    /// the authority for NEW statements — sessionNamespace; this flag
    /// covers the ones already past that gate.)
    dropped: bool = false,
    locks: fiber.SemaphoreMap,
    oracle: Oracle,
    /// Per-table cache: compiled schema + egid counter, keyed by table_id
    /// (the namespace is this database's). Guards the map AND every cached
    /// table's `next_egid` (mutated on hand-out and on the apply fiber via
    /// applyCommitted). Heap entries so held pointers survive map growth.
    tables_mu: fiber.Mutex(.tracked) = .{},
    tables: std.AutoHashMap(u32, *table.Table),

    fn init(allocator: std.mem.Allocator, ns: u64) !Database {
        return .{
            .ns = ns,
            .locks = try fiber.SemaphoreMap.init(allocator),
            .oracle = Oracle.init(allocator),
            .tables = std.AutoHashMap(u32, *table.Table).init(allocator),
        };
    }

    fn deinit(self: *Database, allocator: std.mem.Allocator) void {
        var it = self.tables.valueIterator();
        while (it.next()) |t| {
            t.*.deinit();
            allocator.destroy(t.*);
        }
        self.tables.deinit();
        self.tables_mu.deinit();
        self.locks.deinit();
        self.oracle.deinit();
    }

    /// Apply-fiber update for a run of committed items sharing this
    /// database's namespace: advance each cached table's egid counter
    /// past every committed row id, and record every key in the oracle at
    /// its commit ts (apply already rewrote it to the entry's HLC).
    /// Uncached tables are skipped — first cache seeds egid from
    /// forest.maxKey, covering rows applied before caching.
    fn applyCommitted(self: *Database, items: []const zdb.kv.Item) void {
        {
            self.tables_mu.lock(@src());
            defer self.tables_mu.unlock();
            // Items in a run share this namespace; only table_id varies,
            // and consecutive items share a table in practice.
            var tbl: ?*table.Table = null;
            var table_id: u32 = 0;
            for (items) |item| {
                if (tbl == null or item.key.table_id != table_id) {
                    table_id = item.key.table_id;
                    tbl = self.tables.get(table_id);
                }
                const cached = tbl orelse continue;
                if (key.decodeEgid(item.key.primary)) |egid| {
                    if (egid + 1 > cached.next_egid)
                        cached.next_egid = egid + 1;
                }
            }
        }
        self.oracle.updateItems(items) catch |err|
            log.err("oracle ns={d}: {s}", .{ self.ns, @errorName(err) });
    }
};

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
    /// Compiled layout of the INDEX catalog table's rows (table 2), the
    /// same compiled-in schema in every namespace — one compile serves all.
    index_catalog_ps: schema.PhysicalSchema,
    /// Serializes DDL: the leader-side name-uniqueness checks in
    /// buildCreateNamespace/buildCreateTable are only sound if one DDL
    /// runs at a time.
    ddl_mu: fiber.Mutex(.tracked) = .{},
    /// Per-database txn state (see Database), keyed by namespace id.
    /// Heap entries so held pointers stay valid across map growth, and a
    /// Database is NEVER deleted once created (DROP DATABASE teardown is
    /// deferred work) — so a pointer fetched under `databases_mu` stays
    /// valid after the lock drops. Created by the first SQL txn touching
    /// the database — see `database` for why creation must precede the
    /// txn's read_ts.
    databases_mu: fiber.Mutex(.tracked) = .{},
    databases: std.AutoHashMap(u64, *Database),
    next_txn_id: std.atomic.Value(u64) = .init(1),

    pub fn init(eng: *engine.Engine, addr: []const u8) !Server {
        return .{
            .engine = eng,
            .listen_addr = try fiber.net.IpAddress.parseIpAndPort(addr),
            .catalog_ps = try schema.compile(eng.allocator, catalog.logical_schema, catalog.logical_schema.current_version),
            .index_catalog_ps = try schema.compile(eng.allocator, catalog.index_logical_schema, catalog.index_logical_schema.current_version),
            .databases = std.AutoHashMap(u64, *Database).init(eng.allocator),
        };
    }

    pub fn deinit(self: *Server) void {
        self.catalog_ps.deinit(self.engine.allocator);
        self.index_catalog_ps.deinit(self.engine.allocator);
        var dbs = self.databases.valueIterator();
        while (dbs.next()) |d| {
            d.*.deinit(self.engine.allocator);
            self.engine.allocator.destroy(d.*);
        }
        self.databases.deinit();
        self.ddl_mu.deinit();
        self.databases_mu.deinit();
    }

    /// The Database for `ns`, created on first use and never deleted
    /// after — callers may hold the pointer without `databases_mu`. A
    /// txn must resolve this BEFORE reading its read_ts: once the
    /// Database exists, every later-applying commit lands in its oracle
    /// (the apply observer only updates existing Databases), so no
    /// commit a live txn could conflict with is ever missed.
    pub fn database(self: *Server, ns: u64) !*Database {
        self.databases_mu.lock(@src());
        defer self.databases_mu.unlock();
        const gop = try self.databases.getOrPut(ns);
        if (!gop.found_existing) {
            const d = self.engine.allocator.create(Database) catch |err| {
                _ = self.databases.remove(ns);
                return err;
            };
            d.* = Database.init(self.engine.allocator, ns) catch |err| {
                self.engine.allocator.destroy(d);
                _ = self.databases.remove(ns);
                return err;
            };
            gop.value_ptr.* = d;
        }
        return gop.value_ptr.*;
    }

    /// Age the per-database conflict oracles (see Oracle.rotate) on a
    /// fixed cadence: an oracle entry then lives one to two periods —
    /// the bound on oracle memory AND on how long a txn can run before
    /// horizon conservatism may force a bump or abort. Runs until
    /// shutdown; `waitTimeout` doubles as the sleep.
    pub fn runOracleRotation(self: *Server) void {
        const period_ms: u64 = 10 * 60 * 1000;
        while (!self.engine.node.isShuttingDown()) {
            if (self.engine.node.shutdown.waitTimeout(period_ms)) return;
            self.databases_mu.lock(@src());
            defer self.databases_mu.unlock();
            var it = self.databases.valueIterator();
            while (it.next()) |d| d.*.oracle.rotate();
        }
    }

    /// The commit-stream hook to register with the engine (before eng.start).
    pub fn applyObserver(self: *Server) engine.ApplyObserver {
        return .{ .ctx = self, .onData = onDataApplied };
    }

    /// Apply-fiber callback (every node): hand the committed entry to its
    /// database, which advances egid counters and records the oracle (see
    /// Database.applyCommitted). `databases_mu` is held only for the map
    /// probe — a Database is never deleted once created, so the pointer
    /// stays valid after the drop and applyCommitted runs under
    /// per-database locks only. No Database = no SQL txn state for this
    /// namespace (raw KV traffic) — nothing to maintain.
    fn onDataApplied(ctx: *anyopaque, ns: u64, items: []const zdb.kv.Item) void {
        const self: *Server = @ptrCast(@alignCast(ctx));
        self.databases_mu.lock(@src());
        const db = self.databases.get(ns);
        self.databases_mu.unlock();
        if (db) |d| d.applyCommitted(items);
    }

    /// The cached Table for `table_id` in `db`, loading + seeding it on
    /// first use. Caller holds `db.tables_mu`.
    fn openTableLocked(self: *Server, db: *Database, table_id: u32) !*table.Table {
        if (db.tables.get(table_id)) |t| return t;

        var loaded = try table.loadTableSchema(self.engine.allocator, self.engine.forest, &self.catalog_ps, &self.index_catalog_ps, db.ns, table_id);
        errdefer loaded.deinit();
        // Seed the egid counter past the table's max applied row id (covers
        // snapshot SSTs the apply observer never saw). Empty table → start at 1.
        if (try self.engine.forest.maxKey(self.engine.allocator, db.ns, table_id)) |mk| {
            defer mk.free(self.engine.allocator);
            loaded.next_egid = (key.decodeEgid(mk.primary) orelse 0) + 1;
        } else {
            loaded.next_egid = 1;
        }

        const t = try self.engine.allocator.create(table.Table);
        errdefer self.engine.allocator.destroy(t);
        t.* = loaded;
        try db.tables.put(table_id, t);
        return t;
    }

    /// The cached Table for (namespace, table_id) — schema for INSERT/SELECT.
    pub fn openTable(self: *Server, ns: u64, table_id: u32) !*table.Table {
        const db = try self.database(ns);
        db.tables_mu.lock(@src());
        defer db.tables_mu.unlock();
        return self.openTableLocked(db, table_id);
    }

    /// Reserve the next egid for a table (leader path). newTxn already ran
    /// the catch-up barrier, so `next_egid` reflects the applied committed
    /// tail (onDataApplied only advances it) — the id handed out is past
    /// every committed row.
    pub fn allocEgid(self: *Server, ns: u64, table_id: u32) !u64 {
        const db = try self.database(ns);
        db.tables_mu.lock(@src());
        defer db.tables_mu.unlock();
        const t = try self.openTableLocked(db, table_id);
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
    // LIFO: deinit (roll back any open txn — locks released, pending
    // writes dropped) runs BEFORE the socket closes above. Covers clean
    // Terminate and every error exit from serve.
    defer handler.deinit();
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
    /// The live transaction, `t.implicit` telling which kind: an open
    /// interactive block (BEGIN..COMMIT/ROLLBACK, implicit=false, lives
    /// across statements) or the auto-commit txn of the DML statement
    /// currently executing (implicit=true, ended by runDml). Never both
    /// — a block's statements run in the block's txn. Owned HERE, not in
    /// the statement handler, so a statement restart (read_ts bump) can
    /// re-run the handler without destroying the txn — its locks and
    /// bumped read_ts must survive the retry. Heap — it outlives
    /// individual statements.
    txn: ?*txn.Txn = null,

    /// Session teardown: roll back any open transaction. Wired as a
    /// defer in handleConn, so it runs on every exit from serve.
    pub fn deinit(self: *Handler) void {
        if (self.txn) |t| {
            self.destroyTxn(t);
            self.txn = null;
        }
    }

    fn destroyTxn(self: *Handler, t: *txn.Txn) void {
        t.deinit();
        self.srv.engine.allocator.destroy(t);
    }

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

    /// Catch-up barrier for DDL: this node must be a CAUGHT-UP leader
    /// (see Engine.catchUp), else it replies and returns false — DDL's
    /// leader-side name-uniqueness checks are only sound on one. Callers
    /// hold ddl_mu across it; the barrier itself takes no lock. (DML gets
    /// its freshness best-effort in newTxn, not here.)
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
        state.error_sent = false;
        // Any statement error inside an open block aborts the txn: locks
        // release NOW (a deadlock victim must not keep its cycle alive
        // until the client's ROLLBACK), and the session enters E state
        // until COMMIT/ROLLBACK ends the block.
        defer if (self.txn) |t| {
            // Blocks only: an implicit txn can linger here solely when a
            // reply failed mid-statement (dying connection) — session
            // teardown destroys it.
            if (state.error_sent and !t.implicit and !t.failed) {
                t.failed = true;
                t.releaseLocks();
                state.tx_status = .failed_tx;
            }
        };

        const trimmed = std.mem.trim(u8, sql_text, " \t\r\n;");
        if (trimmed.len == 0) {
            try state.sendEmptyQueryResponse();
            return;
        }
        // Parse into an arena: the DdlStmt borrows its strings from the
        // libpg_query tree, and the arena reclaims all of it — tree and
        // borrowed statement alike — when this scope ends.
        var arena = std.heap.ArenaAllocator.init(self.srv.engine.allocator);
        defer arena.deinit();
        const stmt = ddl.parseDdl(arena.allocator(), sql_text) catch {
            try state.sendError(.{
                .code = pgwire.sqlstate.syntax_error,
                .message = "syntax error",
            });
            return;
        };

        if (self.txn) |t| {
            // A failed block accepts only COMMIT/ROLLBACK (both roll
            // back) until the client ends it.
            if (t.failed and stmt != .commit_txn and stmt != .rollback_txn)
                return replyErrorCode(state, pgwire.sqlstate.in_failed_sql_transaction, "current transaction is aborted, commands ignored until end of transaction block");
            // Leadership moved since BEGIN: nothing this txn read or
            // buffered can be trusted — abort eagerly (the defer above
            // flips the failed state).
            if (!t.failed and self.srv.engine.node.current_term != t.term)
                return replyErrorCode(state, pgwire.sqlstate.serialization_failure, "leader changed, transaction aborted");
            // DDL mixes manifest state with the data plane; keep it out
            // of transaction blocks.
            switch (stmt) {
                .create_database, .drop_database, .create_table, .drop_table => return replyErrorCode(state, pgwire.sqlstate.active_sql_transaction, "DDL inside a transaction block is not supported"),
                else => {},
            }
        }

        switch (stmt) {
            .create_database => |name| try self.createDatabase(state, name),
            .drop_database => |name| try self.dropDatabase(state, name),
            .create_table => |ct| try self.createTable(state, ct),
            .drop_table => |dt| try self.dropTable(state, dt),
            .insert, .select, .delete, .update => try self.runDml(state, stmt),
            .begin_txn => try self.beginTxn(state),
            .commit_txn => try self.endTxn(state, true),
            .rollback_txn => try self.endTxn(state, false),
            .invalid => |msg| try replyErrorCode(state, pgwire.sqlstate.feature_not_supported, msg),
            .unsupported => try replyError(state, "unsupported statement"),
        }
    }

    /// Run one DML statement, transparently re-executing it when a row
    /// lock's read_ts bump invalidates its in-flight snapshot
    /// (error.Restart): the txn keeps its locks and bumped read_ts, its
    /// statement-scoped read tracking rewinds, and the handler re-runs
    /// against the new snapshot. The client never sees a restart. The
    /// implicit txn (if this statement created one) ends here, after
    /// the handler committed it — which keeps its locks held until the
    /// commit has applied.
    fn runDml(self: *Handler, state: *pgwire.ConnState, stmt: ddl.DdlStmt) !void {
        // A write can only commit on the leader — reject it up front
        // rather than scanning, locking, and failing at propose. Reads
        // run on any node (newTxn sets read_ts best-effort), but FOR
        // UPDATE takes row locks and bumps against the oracle — leader-
        // local state — so it is gated like a write. Re-routing to the
        // leader is future work.
        switch (stmt) {
            .insert, .delete, .update => if (!self.srv.engine.node.isLeader())
                return replyError(state, "not the leader"),
            .select => |sel| if (sel.for_update and
                !self.srv.engine.node.isLeader())
                return replyError(state, "not the leader"),
            else => {},
        }
        // Each restart re-raises the floor to a fresh, strictly higher
        // commit — progress, not a loop — but a pathological rate of
        // conflicting commits could starve us; cap and surface it.
        const max_restarts = 8;
        if (self.txn) |t| try t.beginStatement();
        var attempts: u32 = 0;
        while (true) {
            const result = switch (stmt) {
                .insert => |ins| self.insert(state, ins),
                .select => |sel| self.select(state, sel),
                .delete => |del| self.delete(state, del),
                .update => |up| self.update(state, up),
                else => unreachable,
            };
            result catch |err| {
                if (err != error.Restart) {
                    self.endImplicit();
                    return err;
                }
                // Restart statement
                attempts += 1;
                const t = self.txn orelse
                    unreachable; // Restart comes from a lock, locks from a txn
                if (attempts <= max_restarts) {
                    t.restartStatement();
                    continue;
                }
                try replyErrorCode(
                    state,
                    pgwire.sqlstate.serialization_failure,
                    "too many statement restarts, retry the transaction",
                );
            };
            break;
        }
        self.endImplicit();
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

        // Mint the column ids ONCE: 1-based declaration position. Everything
        // downstream — the PK index descriptor and the catalog rows — reads
        // ids from this list, never re-derives them from position. The
        // PRIMARY KEY column (parse allows at most one) gets an auto unique
        // index on its id.
        const columns = try a.alloc(schema.LogicalColumn, ct.columns.len);
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
        var indexes: [1]catalog.IndexDef = undefined;
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
        self.writeCatalogRows(a, ns, created.table_ids[0], columns, indexes[0..n_indexes]) catch |err| {
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
        columns: []const schema.LogicalColumn,
        indexes: []const catalog.IndexDef,
    ) !void {
        const eng = self.srv.engine;
        const n = columns.len + indexes.len;
        if (n == 0) return;

        const kvs = try a.alloc(zen.KeyValueStruct, n);
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(eng.allocator);
        for (columns, 0..) |lc, i| {
            const cells = catalog.encodeColumn(table_id, lc);
            const bytes = try row.marshalRow(eng.allocator, &self.srv.catalog_ps, &cells, &buf);
            const primary = catalog.encodeKey(table_id, lc.column_id);
            const k = zdb.kv.Key{
                .namespace = ns,
                .table_id = catalog.table_id,
                .primary = &primary,
            };
            // marshalRow reuses `buf` — copy the row out; marshal copies
            // the key.
            kvs[i] = .{ .key = try k.marshal(a), .value = try a.dupe(u8, bytes) };
        }
        for (indexes, columns.len..) |idx, i| {
            const cells = catalog.encodeIndex(idx);
            const bytes = try row.marshalRow(
                eng.allocator,
                &self.srv.index_catalog_ps,
                &cells,
                &buf,
            );
            const primary = catalog.encodeIndexKey(idx.base_table_id, idx.index_table_id);
            const k = zdb.kv.Key{
                .namespace = ns,
                .table_id = catalog.index_table_id,
                .primary = &primary,
            };
            kvs[i] = .{ .key = try k.marshal(a), .value = try a.dupe(u8, bytes) };
        }
        try eng.proposeWrites(ns, kvs, .apply);
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
        const a = arena.allocator();

        // One key-only catalog scan answers both DROP questions: is this
        // name an index (reject), and which index trees belong to it (drop
        // with the base below). Safe pre-lock: ddl_mu serializes every
        // catalog mutation; the intent locks only fence in-flight DML.
        var scan = table.scanIndexKeys(a, eng.forest, ns, table_id) catch |err| {
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

        log.info("dropped table '{s}' (ns={d} table_id={d} indexes={d})", .{ dt.name, ns, table_id, scan.table_ids.items.len - 1 });
        try state.sendCommandComplete("DROP TABLE");
    }

    /// Allocate and initialize a fresh txn (caller owns the pointer —
    /// destroyTxn). read_ts = this node's applied watermark. This is the
    /// ONE place snapshot freshness is set: a caught-up leader gets the
    /// freshest read_ts; catchUp is best-effort, so a follower (or a
    /// not-yet-caught-up node) reads its local watermark instead — a
    /// write there just fails later at propose, a read sees local state.
    fn newTxn(self: *Handler, ns: u64) !*txn.Txn {
        const eng = self.srv.engine;
        // Best-effort catch-up: a leader waits out its committed tail
        // (once per term) so read_ts covers it; a non-leader keeps what
        // it has rather than erroring here.
        eng.catchUp() catch {};
        // Database first, read_ts second: a commit applying between
        // the two lands in this database's now-existing oracle, so a
        // conflict newer than our snapshot can never go unrecorded.
        const db = try self.srv.database(ns);
        const t = try eng.allocator.create(txn.Txn);
        errdefer eng.allocator.destroy(t);
        t.* = txn.Txn.init(
            eng.allocator,
            self.srv.next_txn_id.fetchAdd(1, .monotonic),
            ns,
            eng.node.appliedTs(),
            eng.node.current_term,
            &db.locks,
            &db.oracle,
            eng.forest,
        );
        return t;
    }

    /// The transaction a DML statement runs in: the open interactive
    /// block, or a fresh implicit txn (implicit=true) the statement itself
    /// commits.
    fn statementTxn(self: *Handler, ns: u64) !*txn.Txn {
        // The open block's txn — or, on a restart re-entry, the
        // statement's own implicit txn (locks + bumped read_ts intact).
        if (self.txn) |t| return t;

        const t = try self.newTxn(ns);
        t.implicit = true;
        self.txn = t;
        return t;
    }

    /// The txn a DML statement runs in, holding its shared namespace-
    /// and table-intent locks (in that order — the global lock order is
    /// ns → table → row). Null means the statement is over: a lock error
    /// was already reported, or the database was dropped while we were
    /// parked on the namespace key (drops take it exclusive). DROP
    /// TABLE / DROP DATABASE queue behind these intents; writer
    /// preference then parks new DML behind the pending drop.
    fn dmlTxn(
        self: *Handler,
        state: *pgwire.ConnState,
        ns: u64,
        table_id: u32,
    ) !?*txn.Txn {
        const tx = try self.statementTxn(ns);
        tx.lockNamespace(.shared) catch |err| {
            try replyLockErr(state, err);
            return null;
        };
        const db = try self.srv.database(ns);
        if (db.dropped) {
            try replyErrorCode(
                state,
                pgwire.sqlstate.invalid_catalog_name,
                "database does not exist",
            );
            return null;
        }
        tx.lockTable(table_id, .shared) catch |err| {
            try replyLockErr(state, err);
            return null;
        };
        return tx;
    }

    /// End the statement's implicit txn, if that is what's live. The
    /// `implicit` guard is load-bearing: runDml calls this after every
    /// DML statement, including ones inside an open block — whose txn
    /// must survive until COMMIT/ROLLBACK.
    fn endImplicit(self: *Handler) void {
        if (self.txn) |t| {
            if (!t.implicit) return;
            self.destroyTxn(t);
            self.txn = null;
        }
    }

    fn beginTxn(self: *Handler, state: *pgwire.ConnState) !void {
        if (self.txn != null) {
            // Postgres warns and no-ops.
            log.warn("BEGIN inside a transaction block — no-op", .{});
            return state.sendCommandComplete("BEGIN");
        }
        const ns = try self.sessionNamespace(state) orelse return;
        const t = try self.newTxn(ns);
        self.txn = t;
        state.tx_status = .in_tx;
        try state.sendCommandComplete("BEGIN");
    }

    /// COMMIT (commit=true) or ROLLBACK ends the block either way:
    /// COMMIT of a failed txn rolls back and says so (tag ROLLBACK,
    /// Postgres behavior); outside a block both warn and no-op.
    fn endTxn(self: *Handler, state: *pgwire.ConnState, commit: bool) !void {
        const t = self.txn orelse {
            log.warn("{s} outside a transaction block — no-op", .{if (commit) "COMMIT" else "ROLLBACK"});
            return state.sendCommandComplete(if (commit) "COMMIT" else "ROLLBACK");
        };
        defer {
            self.destroyTxn(t);
            self.txn = null;
            state.tx_status = .idle;
        }
        if (commit and !t.failed) {
            var arena = std.heap.ArenaAllocator.init(self.srv.engine.allocator);
            defer arena.deinit();
            if (!try self.commitOrReply(state, arena.allocator(), t, "commit", "txn")) return;
            return state.sendCommandComplete("COMMIT");
        }
        try state.sendCommandComplete("ROLLBACK");
    }

    /// Commit an implicit txn: propose the pending write set as ONE
    /// .data entry and wait for local apply, so the statement's effects
    /// are readable the moment the client sees CommandComplete. Empty
    /// write set → nothing to propose. `error.LeaderChanged` if the term
    /// moved since begin: another leader may have committed writes our
    /// locked reads never saw, so the write set must not be proposed.
    fn commitTxn(self: *Handler, a: std.mem.Allocator, tx: *txn.Txn) !void {
        const kvs = try tx.wireKvs(a);
        if (kvs.len == 0) return;
        const node = self.srv.engine.node;
        if (!node.isLeader() or node.current_term != tx.term)
            return error.LeaderChanged;
        try self.srv.engine.proposeWrites(tx.ns, kvs, .apply);
    }

    /// Commit + shared reply mapping for DML: leader change → 40001
    /// (retry); anything else logs and reports a generic failure.
    /// Returns true when committed — the caller sends CommandComplete.
    fn commitOrReply(
        self: *Handler,
        state: *pgwire.ConnState,
        a: std.mem.Allocator,
        tx: *txn.Txn,
        comptime verb: []const u8,
        table_name: []const u8,
    ) !bool {
        self.commitTxn(a, tx) catch |err| switch (err) {
            error.LeaderChanged => {
                try replyErrorCode(state, pgwire.sqlstate.serialization_failure, "leader changed, transaction aborted");
                return false;
            },
            else => {
                log.err(
                    verb ++ " '{s}' (txn={d}): propose: {s}",
                    .{ table_name, tx.id, @errorName(err) },
                );
                try replyError(state, verb ++ " failed");
                return false;
            },
        };
        return true;
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
        const arloc = arena.allocator();

        const tx = (try self.dmlTxn(state, ns, table_id)) orelse return;

        // Map each VALUES position → the column it writes: `col` carries the
        // name and coercion type, `slot` is that column's cell in `values`
        // (parallel to t.columns, == ps order). Positional insert maps 1:1
        // to the live columns; an explicit list resolves names. Inserting an
        // explicit `egid` isn't supported yet.
        // TODO: allow user-provided egids once unique-index enforcement exists
        // — a below-counter egid needs a data-tree existence check, and
        // concurrent inserts of the same egid need apply-time validation.
        const values = try arloc.alloc(value.Value, t.columns.len);
        const Target = struct {
            col: *const table.Column,
            dst: *value.Value,
        };
        const targets: []Target = blk: {
            if (ins.columns) |cols| {
                const list = try arloc.alloc(Target, cols.len);
                for (cols, list) |name, *target| {
                    if (std.ascii.eqlIgnoreCase(name, "egid"))
                        return replyErrorCode(state, pgwire.sqlstate.feature_not_supported, "inserting an explicit egid is not supported yet");
                    const ci = columnIndex(t, name) orelse
                        return replyErrorCode(state, pgwire.sqlstate.undefined_column, "column does not exist");
                    target.* = .{ .col = &t.columns[ci], .dst = &values[ci] };
                }
                break :blk list;
            }
            const list = try arloc.alloc(Target, t.columns.len);
            for (list, t.columns, values) |*dst, *col, *slot|
                dst.* = .{ .col = col, .dst = slot };
            break :blk list;
        };

        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(eng.allocator);

        // One VALUES row, fully derived in Phase 0 — parse + marshal +
        // encode every key BEFORE any lock or write: a Restart re-runs
        // the whole statement, so nothing may be buffered until every
        // PK lock is held (§Phase 1). `values` is reused per row, so
        // anything derived from a row's cells is captured here.
        const PendingRow = struct {
            bytes: String = "", // marshaled row value
            pkey: String = "", // PK index key, when t.pkey != null
            /// Per idx_cols entry (same order): the index key this
            /// row lands under. Always present — NULL cells key on
            /// the NULL tag byte, and unencodable column types
            /// (interval) never carry an index (rejected at CREATE
            /// INDEX).
            postings: []String = &.{},

            fn lessByPkey(rows: []@This(), x: usize, y: usize) bool {
                return std.mem.lessThan(u8, rows[x].pkey, rows[y].pkey);
            }
        };
        const num_rows = ins.rows.len;
        const idx_cols = try indexCols(arloc, t);
        const pending = try arloc.alloc(PendingRow, num_rows);
        for (ins.rows, pending) |row_lits, *pr| {
            pr.* = .{};
            if (row_lits.len != targets.len)
                return replyErrorCode(state, pgwire.sqlstate.syntax_error, "INSERT has the wrong number of values");

            for (values) |*v| v.* = .null_value; // reset all to null

            for (row_lits, targets) |lit, target| {
                target.dst.* =
                    (try coerceLiteral(state, arloc, lit, target.col.ty)) orelse
                    return;
            }

            if (t.pkey) |pk| {
                const pkv = values[pk.column_index];
                if (pkv == .null_value)
                    return replyErrorCode(
                        state,
                        pgwire.sqlstate.not_null_violation,
                        "null value in primary key column violates not-null constraint",
                    );
                pr.pkey = key.encodeIndexKey(arloc, pkv, null) catch |err|
                    switch (err) {
                        error.OutOfMemory => return err,
                        error.Unencodable => return replyErrorCode(
                            state,
                            pgwire.sqlstate.feature_not_supported,
                            "PRIMARY KEY on this column type is not supported",
                        ),
                    };
            }
            if (idx_cols.count() > 0) {
                pr.postings = try arloc.alloc(String, idx_cols.count());
                for (idx_cols.keys(), pr.postings) |col_idx, *posting_key| {
                    posting_key.* = key.encodeIndexKey(
                        arloc,
                        values[col_idx],
                        null,
                    ) catch |err| switch (err) {
                        error.Unencodable => unreachable,
                        else => return err,
                    };
                }
            }
            // marshalRow reuses `buf`; dupe out before the next row clobbers it.
            const bytes = try row.marshalRow(eng.allocator, &t.ps, values, &buf);
            pr.bytes = try arloc.dupe(u8, bytes);
        }

        // Phase 1 (PK tables): reject duplicates, then lock every PK index
        // key — all before any write. Sorted order gives deadlock-free
        // multi-row locking AND an adjacency check for an in-statement
        // duplicate. lockRow may Restart (bump) / Conflict / Deadlock; on
        // Restart the driver re-runs from Phase 0 with nothing buffered.
        if (t.pkey) |pk| {
            const order = try arloc.alloc(usize, num_rows);
            for (order, 0..) |*o, j| o.* = j;
            std.mem.sort(usize, order, pending, PendingRow.lessByPkey);

            for (1..num_rows) |j| {
                if (std.mem.eql(u8, pending[order[j - 1]].pkey, pending[order[j]].pkey))
                    return replyErrorCode(state, pgwire.sqlstate.unique_violation, "duplicate key value violates unique constraint");
            }
            for (order) |j| tx.lockRow(pk.index_table_id, pending[j].pkey) catch |err|
                return replyLockErr(state, err);
            // Post-lock, read_ts covers the key's newest commit: a live
            // index entry means the PK value already exists.
            for (order) |j| {
                const found = tx.lookup(.{
                    .namespace = ns,
                    .table_id = pk.index_table_id,
                    .primary = pending[j].pkey,
                    .timestamp = tx.read_ts,
                }) catch |err| {
                    log.err("insert into '{s}': pk probe: {s}", .{ ins.table, @errorName(err) });
                    return replyError(state, "insert failed");
                };
                if (found) |f| {
                    f.release();
                    return replyErrorCode(state, pgwire.sqlstate.unique_violation, "duplicate key value violates unique constraint");
                }
            }
        }

        // Phase 2: every lock is held — allocate egids and buffer each row
        // and its index entries. No Restart can occur here (puts don't
        // bump). The whole VALUES list commits atomically as one .data
        // entry; an implicit txn waits for apply so an immediate SELECT
        // on this session sees the rows.
        for (pending) |pr| {
            const egid = self.srv.allocEgid(ns, table_id) catch |err| switch (err) {
                error.NotLeader => return replyError(state, "not the leader"),
                else => {
                    log.err("insert into '{s}': alloc egid: {s}", .{ ins.table, @errorName(err) });
                    return replyError(state, "row id allocation failed");
                },
            };
            const ekey = key.encodeEgid(egid);
            try tx.put(table_id, &ekey, pr.bytes, .{});
            // The index entry's value is the row key: the same 8 BE bytes.
            if (t.pkey) |pk|
                try tx.put(pk.index_table_id, pr.pkey, &ekey, .{});
            // Non-unique indexes: commuting posting deltas, NO locks —
            // the row lock (here, a fresh egid) serializes add/remove
            // of one egid (docs/transactions.md, lock classes).
            for (idx_cols.values(), pr.postings) |index_tid, posting_key|
                try tx.putPosting(index_tid, posting_key, egid, .add);
        }

        if (tx.implicit) {
            if (!try self.commitOrReply(state, arloc, tx, "insert", ins.table)) return;
        }

        try state.sendCommandComplete(try std.fmt.allocPrint(arloc, "INSERT 0 {d}", .{ins.rows.len}));
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

        const tx = (try self.dmlTxn(state, ns, table_id)) orelse return;

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
        // FOR UPDATE sends NOTHING until every lock is held: lockRow may
        // Restart the statement, and bytes already on the wire (the row
        // description, emitted rows) would repeat on the re-run.
        if (!sel.for_update) try state.sendRowDescription(fields);

        var emitter = RowEmitter{
            .state = state,
            .ps = &t.ps,
            .outs = outs.items,
            .stmt_alloc = a,
        };
        defer emitter.deinit();

        // Plain SELECT streams matches to the wire as run() visits
        // them; FOR UPDATE captures them (values included) to lock
        // every one BEFORE the first byte is sent — the UPDATE/DELETE
        // lock-all shape with "act" = emission.
        var sc = Scanner{
            .a = a,
            .ps = &t.ps,
            .mode = if (sel.for_update)
                .copy_value
            else
                .{ .emit_value = &emitter },
            .lock = sel.for_update,
        };
        if (sel.filter) |f| {
            switch ((try resolveFilter(state, a, t, f)) orelse return) {
                .egids => |es| sc.source = .{ .egids = es },
                .index_eq => |ix| sc.source = .{ .index_eq = ix },
                .cell => |c| sc.cell_filter = c,
            }
        }
        sc.run(eng, ns, table_id, tx) catch |err|
            return replyRunErr(state, "select from", sel.table, err);
        if (sc.err) |err| {
            // Streaming: an emit error is a wire error — surface it
            // to the serve loop. Capture: arena OOM — reply.
            if (!sel.for_update) return err;
            log.err("select from '{s}': collect: {s}", .{
                sel.table, @errorName(err),
            });
            return replyError(state, "select failed");
        }
        if (sel.for_update) {
            // run() returned with every match locked: none committed
            // above read_ts (that lockRow would have bumped →
            // restarted), so the captured values are the newest
            // versions, frozen by our locks — emit them now.
            try state.sendRowDescription(fields);
            for (sc.items.items) |item| try emitter.emit(item);
        }

        try state.sendCommandComplete(try std.fmt.allocPrint(a, "SELECT {d}", .{emitter.count}));
    }

    fn delete(self: *Handler, state: *pgwire.ConnState, del: ddl.Delete) !void {
        const eng = self.srv.engine;

        const ns = try self.sessionNamespace(state) orelse return;
        const table_id = eng.forest.lookupTable(ns, del.table) orelse
            return replyErrorCode(
                state,
                pgwire.sqlstate.undefined_table,
                "table does not exist",
            );
        const tbl = self.srv.openTable(ns, table_id) catch |err| {
            logDeleteErr(del.table, "load schema", err);
            return replyError(state, "failed to load table schema");
        };

        var arena = std.heap.ArenaAllocator.init(eng.allocator);
        defer arena.deinit();
        const arloc = arena.allocator();

        const tx = (try self.dmlTxn(state, ns, table_id)) orelse return;

        // Resolve WHERE: `egid = N` → a point lookup; a payload column
        // → scan + compare; no WHERE → tombstone every live row. Either
        // way run() captures the matches; every one is locked, then
        // tombstoned from the capture.
        const idx_cols = try indexCols(arloc, tbl);
        var scanner = Scanner{
            .a = arloc,
            .ps = &tbl.ps,
            // Indexed tables keep the old bytes so every index entry —
            // the PK's (PK value → egid) and each posting's remove —
            // can be derived from the row being tombstoned.
            .mode = if (tbl.pkey != null or idx_cols.count() > 0)
                .copy_value
            else
                .no_value,
            .lock = true,
        };
        if (del.filter) |f| {
            switch ((try resolveFilter(state, arloc, tbl, f)) orelse return) {
                .egids => |es| scanner.source = .{ .egids = es },
                .index_eq => |ix| scanner.source = .{ .index_eq = ix },
                .cell => |c| scanner.cell_filter = c,
            }
        }
        scanner.run(eng, ns, table_id, tx) catch |err|
            return replyRunErr(state, "delete from", del.table, err);
        if (scanner.err) |err| {
            logDeleteErr(del.table, "collect", err);
            return replyError(state, "delete failed");
        }

        // Lock and tombstone each captured row's PK index entry BEFORE the
        // row tombstones: the index-key lock serializes against a concurrent
        // re-insert of the same key (whose fresh entry a late tombstone would
        // otherwise shadow). Locking may Restart — safe, nothing buffered yet.
        if (tbl.pkey) |pk| {
            const items = scanner.items.items;
            const index_keys = try arloc.alloc([]const u8, items.len);

            for (items, index_keys) |item, *ik| {
                const pkv = row.readCell(&tbl.ps, item.value, pk.column_index);
                ik.* = key.encodeIndexKey(arloc, pkv, null) catch |err|
                    switch (err) {
                        error.OutOfMemory => return err,
                        error.Unencodable => {
                            logDeleteErr(del.table, "encode pk", err);
                            return replyError(state, "delete failed");
                        },
                    };
            }
            // The item↔key pairing is not needed past this point, so sort the
            // keys themselves (lock order = the deadlock-avoidance order).
            std.mem.sort([]const u8, index_keys, {}, lessThanBytes);
            for (index_keys) |ik| {
                tx.lockRow(pk.index_table_id, ik) catch |err|
                    return replyLockErr(state, err);
            }
            for (index_keys) |ik|
                try tx.put(pk.index_table_id, ik, "", .{ .kind = .deleted });
        }

        // Non-unique indexes: remove each row's egid from its postings
        // — commuting deltas, NO locks (docs/transactions.md, lock
        // classes). All locking is done (run()'s row locks, the PK
        // block above), so no Restart can strand these.
        for (scanner.items.items) |item| {
            const egid = key.decodeEgid(item.key.primary).?;
            for (idx_cols.keys(), idx_cols.values()) |col_idx, index_tid| {
                const cell = row.readCell(&tbl.ps, item.value, col_idx);
                const old_key = key.encodeIndexKey(arloc, cell, null) catch |err|
                    switch (err) {
                        error.Unencodable => unreachable,
                        else => return err,
                    };
                try tx.putPosting(index_tid, old_key, egid, .remove);
            }
        }

        // Past run()'s locks no candidate committed above read_ts — the
        // captured rows are the newest versions, frozen by our locks, so
        // they are tombstoned directly with no re-read.
        for (scanner.items.items) |item|
            try tx.put(table_id, item.key.primary, "", .{ .kind = .deleted });
        const n = scanner.items.items.len;
        if (n > 0 and tx.implicit) {
            if (!try self.commitOrReply(state, arloc, tx, "delete", del.table)) return;
        }
        const tag = try std.fmt.allocPrint(arloc, "DELETE {d}", .{n});
        try state.sendCommandComplete(tag);
    }

    fn update(self: *Handler, state: *pgwire.ConnState, up: ddl.Update) !void {
        const eng = self.srv.engine;

        const ns = try self.sessionNamespace(state) orelse return;
        const table_id = eng.forest.lookupTable(ns, up.table) orelse
            return replyErrorCode(
                state,
                pgwire.sqlstate.undefined_table,
                "table does not exist",
            );
        const t = self.srv.openTable(ns, table_id) catch |err| {
            log.err("update '{s}': load schema: {s}", .{ up.table, @errorName(err) });
            return replyError(state, "failed to load table schema");
        };

        var arena = std.heap.ArenaAllocator.init(eng.allocator);
        defer arena.deinit();
        const arloc = arena.allocator();

        const idx_cols = try indexCols(arloc, t);
        const tx = (try self.dmlTxn(state, ns, table_id)) orelse return;

        // A SET on an indexed column moves each matched row's egid
        // from its old posting to a statement-constant new one (the
        // SET value is one literal for every row): the encoded new
        // key plus where the old one comes from (col_idx into the
        // pre-image). Parse rejects duplicate SET columns, so one
        // entry per column.
        const MovedCol = struct {
            table_id: u32,
            col_idx: usize,
            new_key: String,
        };
        var moved_cols: std.ArrayList(MovedCol) = .empty;

        // sets is used to create the updated row. It tells us which cols have
        // been updated and what their new value is.
        const set_cols = try arloc.alloc(SetVal, up.sets.len);
        for (up.sets, set_cols) |clause, *sv| {
            if (std.ascii.eqlIgnoreCase(clause.column, "egid"))
                return replyErrorCode(
                    state,
                    pgwire.sqlstate.feature_not_supported,
                    "egid cannot be updated",
                );
            const col_idx = columnIndex(t, clause.column) orelse
                return replyErrorCode(
                    state,
                    pgwire.sqlstate.undefined_column,
                    "column does not exist",
                );
            // A changed PK would move the row's index entry; not supported
            // this arc (the index stays valid only while the key is fixed).
            if (t.columns[col_idx].pkey_position != null)
                return replyErrorCode(
                    state,
                    pgwire.sqlstate.feature_not_supported,
                    "PRIMARY KEY column cannot be updated",
                );
            const col = t.columns[col_idx];
            const val =
                (try coerceLiteral(state, arloc, clause.literal, col.ty)) orelse
                return;
            sv.* = .{
                .idx = psIndex(&t.ps, col.column_id),
                .val = val,
            };

            if (idx_cols.get(sv.idx)) |index_tid| {
                try moved_cols.append(arloc, .{
                    .table_id = index_tid,
                    .col_idx = sv.idx,
                    .new_key = key.encodeIndexKey(arloc, sv.val, null) catch |err|
                        switch (err) {
                            error.Unencodable => unreachable,
                            else => return err,
                        },
                });
            }
        }

        // Resolve WHERE: `egid = N` → a point lookup; a payload column
        // → scan + compare; no WHERE → rewrite every live row. run()
        // captures each match's PRE-image; the rewrite (and any
        // posting moves, whose old keys come from that pre-image)
        // happens post-lock.
        var sc = Scanner{
            .a = arloc,
            .ps = &t.ps,
            .mode = .copy_value,
            .lock = true,
        };
        if (up.filter) |f| {
            switch ((try resolveFilter(state, arloc, t, f)) orelse return) {
                .egids => |es| sc.source = .{ .egids = es },
                .index_eq => |ix| sc.source = .{ .index_eq = ix },
                .cell => |c| sc.cell_filter = c,
            }
        }
        sc.run(eng, ns, table_id, tx) catch |err|
            return replyRunErr(state, "update", up.table, err);
        if (sc.err) |err| {
            log.err("update '{s}': collect: {s}", .{ up.table, @errorName(err) });
            return replyError(state, "update failed");
        }
        // Past run()'s locks no candidate committed above read_ts — the
        // captured pre-images are the newest versions, frozen by our
        // locks. Move each row's egid between postings when an indexed
        // column changed (equal keys move nothing — commuting deltas,
        // NO locks per docs/transactions.md; all locking is done, so
        // no Restart can strand these), then rewrite and buffer the
        // row.
        var dec: value.Row = .{};
        // Reused across items: marshalRow resets it, and tx.put
        // copies the bytes into the txn arena before returning.
        var rbuf: std.ArrayList(u8) = .empty;
        for (sc.items.items) |item| {
            const egid = key.decodeEgid(item.key.primary).?;
            for (moved_cols.items) |mv| {
                const cell = row.readCell(&t.ps, item.value, mv.col_idx);
                const old_key = key.encodeIndexKey(arloc, cell, null) catch |err|
                    switch (err) {
                        error.Unencodable => unreachable,
                        else => return err,
                    };
                // If old_key and new_key match, there's no change. Skip.
                if (std.mem.eql(u8, old_key, mv.new_key)) continue;

                try tx.putPosting(mv.table_id, old_key, egid, .remove);
                try tx.putPosting(mv.table_id, mv.new_key, egid, .add);
            }
            const rewritten = try rewriteRowBytes(
                arloc,
                &t.ps,
                set_cols,
                &dec,
                item.value,
                &rbuf,
            );
            try tx.put(table_id, item.key.primary, rewritten, .{});
        }
        const n = sc.items.items.len;

        if (tx.implicit) {
            if (!try self.commitOrReply(state, arloc, tx, "update", up.table)) return;
        }
        try state.sendCommandComplete(try std.fmt.allocPrint(arloc, "UPDATE {d}", .{n}));
    }
};

/// litToValue plus the client-facing error mapping shared by INSERT
/// and UPDATE. null = the error reply was already sent (the
/// resolveFilter convention).
fn coerceLiteral(
    state: *pgwire.ConnState,
    a: std.mem.Allocator,
    lit: ddl.Literal,
    ty: value.Type,
) !?value.Value {
    return litToValue(a, lit, ty) catch |err| switch (err) {
        error.OutOfMemory => return err,
        error.UnsupportedLiteral, error.UnsupportedType => {
            try replyErrorCode(
                state,
                pgwire.sqlstate.feature_not_supported,
                "unsupported value or column type",
            );
            return null;
        },
        else => {
            try replyErrorCode(
                state,
                pgwire.sqlstate.invalid_text_representation,
                "invalid value for column type",
            );
            return null;
        },
    };
}

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

/// Order row indices by their encoded PK index key (memcomparable, so byte
/// order is value order) — used to lock a multi-row INSERT deadlock-free and
/// to spot an in-statement duplicate as an adjacent pair.
fn lessThanBytes(_: void, x: []const u8, y: []const u8) bool {
    return std.mem.lessThan(u8, x, y);
}

/// Where a SELECT output column's value comes from.
const SelectSource = union(enum) { egid, cell: usize };
const SelectOut = struct { name: []const u8, ty: value.Type, source: SelectSource };
/// A resolved `column = value` filter: the column's decode-cell index + target.
const CellFilter = struct { idx: usize, val: value.Value };
/// A resolved index point lookup: which index tree + the memcomparable key.
const IndexLookup = struct { index_table_id: u32, key: []const u8 };

/// Index of a column_id in ps.columns (== its cell index when decoding with an
/// empty projection). The column came from t.columns, so it's always present.
fn psIndex(ps: *const schema.PhysicalSchema, column_id: u32) usize {
    for (ps.columns, 0..) |c, i| if (c.column_id == column_id) return i;
    unreachable;
}

/// The table's non-unique indexes as col_idx → index table_id (the
/// key indexes t.columns, t.ps and a row's values alike — same
/// order): O(1) "is this column indexed?", and — ArrayHashMap —
/// stable order, so callers can zip keys()/values() against parallel
/// slices. At most one non-unique index per column (CREATE INDEX
/// rejects seconds; the assert enforces it here). These indexes carry
/// roaring postings maintained as commuting deltas (Txn.putPosting);
/// unique indexes (the pkey) keep their single-egid lock-then-check
/// path.
fn indexCols(
    a: std.mem.Allocator,
    t: *const table.Table,
) !std.AutoArrayHashMapUnmanaged(usize, u32) {
    var map: std.AutoArrayHashMapUnmanaged(usize, u32) = .empty;
    for (t.indexes) |idx| {
        if (idx.unique) continue;
        const gop = try map.getOrPut(a, psIndex(&t.ps, idx.column_id));
        std.debug.assert(!gop.found_existing);
        gop.value_ptr.* = idx.index_table_id;
    }
    return map;
}

fn logDeleteErr(table_name: []const u8, what: []const u8, err: anyerror) void {
    log.err(
        "delete from '{s}': {s}: {s}",
        .{ table_name, what, @errorName(err) },
    );
}

/// A resolved WHERE `col = value`: the `egid` pseudo-column becomes a
/// point-id list (one entry today; IN-lists later); an INDEXED column
/// becomes an index point lookup; any other payload column becomes a
/// per-row cell compare. Feeds Scanner's source / cell_filter.
const ResolvedFilter = union(enum) {
    egids: []const u64,
    index_eq: IndexLookup,
    cell: CellFilter,
};

/// Resolve a parsed `col = literal` filter against `t`. `a` (the statement
/// arena) backs the coerced compare value. On an unknown column, a bad egid,
/// or an uncoercible literal the SQL error is already sent and null returned
/// — the statement is over. Only OOM propagates as an error.
fn resolveFilter(
    state: *pgwire.ConnState,
    a: std.mem.Allocator,
    t: *const table.Table,
    f: ddl.Filter,
) !?ResolvedFilter {
    const bad_value = pgwire.sqlstate.invalid_text_representation;
    if (std.ascii.eqlIgnoreCase(f.column, "egid")) {
        const n = switch (f.literal) {
            .int => |v| v,
            else => {
                try replyErrorCode(state, bad_value, "egid must be an integer");
                return null;
            },
        };
        if (n < 0) {
            try replyErrorCode(
                state,
                bad_value,
                "egid must be a non-negative integer",
            );
            return null;
        }
        return .{ .egids = try a.dupe(u64, &.{@intCast(n)}) };
    }
    const fc = t.column(f.column) orelse {
        try replyErrorCode(
            state,
            pgwire.sqlstate.undefined_column,
            "column does not exist",
        );
        return null;
    };
    const val = litToValue(a, f.literal, fc.ty) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => {
            try replyErrorCode(state, bad_value, "invalid value in WHERE");
            return null;
        },
    };
    // An index on this column turns the filter into a point lookup. Matching
    // against the index list (not just the PK) means CREATE INDEX later needs
    // no routing change. An unencodable value (interval) falls back to a cell
    // compare, preserving today's semantics. NULL encodes (its own tag-byte
    // key), and `= NULL` matching NULL rows is deliberately consistent with
    // the cell path's valuesEqual(null, null) — Postgres-strict IS NULL
    // semantics are one future decision covering both paths.
    index: for (t.indexes) |idx| {
        if (idx.column_id != fc.column_id) continue;
        const ik = key.encodeIndexKey(a, val, null) catch |err| {
            if (err == error.OutOfMemory) return err;
            break :index;
        };
        return .{ .index_eq = .{
            .index_table_id = idx.index_table_id,
            .key = ik,
        } };
    }
    return .{ .cell = .{ .idx = psIndex(&t.ps, fc.column_id), .val = val } };
}

/// Turns each row into a projected DataRow on the wire — Scanner's
/// emitter for streaming (emit_value), and driven directly to emit a
/// FOR UPDATE capture post-lock (filtering is Scanner's job either
/// way). Columns format straight into the frame (pgwire DataRowWriter),
/// which the connection's buffered writer batches to the socket.
const RowEmitter = struct {
    state: *pgwire.ConnState,
    ps: *const schema.PhysicalSchema,
    outs: []const SelectOut,
    stmt_alloc: std.mem.Allocator, // backs the reused `decoded` row
    decoded: value.Row = .{},
    count: u64 = 0,

    fn deinit(self: *RowEmitter) void {
        self.decoded.deinit(self.stmt_alloc);
    }

    /// Project and send one row.
    fn emit(self: *RowEmitter, item: zdb.kv.Item) !void {
        try row.unmarshalRow(self.stmt_alloc, self.ps, item.value, &.{}, &self.decoded);

        var dw = try self.state.beginDataRow(@intCast(self.outs.len));
        for (self.outs) |o| {
            const v: value.Value = switch (o.source) {
                .egid => .{ .int8 = @intCast(key.decodeEgid(item.key.primary) orelse 0) },
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
    }
};

/// The statement scanner — the one forest.scan callback for DML. Filters
/// each row on the single cell the WHERE names (no full-row decode),
/// then handles the match per `mode` — streamed to the wire, or captured
/// into `items` for a post-lock pass (duped into the statement arena;
/// scan Items don't outlive the callback). Default ScanOptions skip
/// tombstones, so only live rows are seen — re-running a DELETE reports 0.
const Scanner = struct {
    a: std.mem.Allocator, // statement arena — backs items
    ps: *const schema.PhysicalSchema,
    cell_filter: ?CellFilter = null,
    source: Source = .table,
    mode: Mode,
    /// Lock every matched row (exclusive, held to txn end) before run()
    /// returns: point keys BEFORE their read (the lock's bump makes the
    /// read the newest version — the lost-update guard without a
    /// latest-read), scan captures after the scan, in key order so
    /// concurrent statements can't cycle. lockRow may raise Restart
    /// mid-run — safe exactly because run() has buffered no writes and
    /// sent no wire bytes; the re-run finds the locks still held
    /// (re-entrant) and takes a fresh capture. Streaming (emit_value)
    /// must NOT lock: its rows are already on the wire and a Restart
    /// would resend them.
    lock: bool = false,
    decoded: value.Row = .{}, // rewrite decode scratch, arena-backed
    /// Captured matches. The post-lock pass tombstones/puts/emits
    /// straight from these: reaching it means no candidate committed
    /// above read_ts (that lockRow would have bumped → restarted →
    /// re-scanned), so the scan-time source IS the newest version and
    /// no second forest read is needed.
    items: std.ArrayList(zdb.kv.Item) = .empty,
    err: ?anyerror = null,

    /// Where the statement's rows come from.
    const Source = union(enum) {
        /// Snapshot scan of the whole table; cell_filter prunes rows.
        table,
        /// Point lookups by row id — `WHERE egid = N` today (one
        /// entry), IN-lists later. With `lock` set, each key is locked
        /// BEFORE its read: the lock's bump has already advanced
        /// read_ts, so the value read is the newest. An absent row
        /// still leaves its key locked — harmless today, and the
        /// phantom-insert guard once explicit row ids exist.
        egids: []const u64,
        /// Point lookup through a unique index — `WHERE <indexed col> =
        /// v`: read the row id in `index_table_id` under `key`, then the
        /// base row. With `lock` set, the INDEX key is locked FIRST (even
        /// when absent — the phantom-insert guard: a concurrent INSERT of
        /// the same key blocks), then the row.
        index_eq: IndexLookup,
    };

    /// What each match's value gets used for.
    const Mode = union(enum) {
        /// DELETE: key-only capture — tombstoned by key, bytes unused.
        no_value,
        /// SELECT FOR UPDATE: capture the row verbatim; the post-lock
        /// pass emits it.
        copy_value,
        /// Plain SELECT: straight to the wire; nothing is captured.
        emit_value: *RowEmitter,
        /// Capture the REWRITTEN row (built here, while the scan's
        /// source bytes are alive), so the old value is never copied
        /// and the post-lock pass just buffers it verbatim.
        /// TODO: currently unused — UPDATE always captures the
        /// pre-image (.copy_value) since index maintenance needs the
        /// old cell values; revisit as the no-index fast path.
        update_value: []const SetVal,
    };

    fn collect(self: *Scanner, item: zdb.kv.Item) !void {
        // Filter on the ONE cell it names — no full-row decode here.
        if (self.cell_filter) |cf| {
            if (!value.valuesEqual(row.readCell(self.ps, item.value, cf.idx), cf.val))
                return;
        }
        const captured: zdb.kv.Item = switch (self.mode) {
            .emit_value => |em| return em.emit(item),
            .no_value => try item.dupeNoValue(self.a),
            .copy_value => try item.dupe(self.a),
            .update_value => |sets| rewritten: {
                var dup = try item.dupeNoValue(self.a);
                // Rewrite now, borrowing the scan's transient row
                // bytes: pure computation into the arena — no txn state
                // touched, so a later Restart just discards it with the
                // statement arena.
                var buf: std.ArrayList(u8) = .empty; // arena; persists
                dup.value = try rewriteRowBytes(
                    self.a,
                    self.ps,
                    sets,
                    &self.decoded,
                    item.value,
                    &buf,
                );
                break :rewritten dup;
            },
        };
        try self.items.append(self.a, captured);
    }

    /// forest.scan callback: collect, capturing any error so the scan stops.
    fn scanCb(self: *Scanner, item: zdb.kv.Item) anyerror!bool {
        self.collect(item) catch |e| {
            self.err = e;
            return false;
        };
        return true;
    }

    /// Feed the statement's rows through `collect`, per `source` — and
    /// when `lock` is set, return only once every match is locked.
    /// Point lookups read at the txn's read_ts (pending write first —
    /// read-your-own-writes; tombstones read as absent). The table scan
    /// is recorded on the txn first (SI: prior-scan revalidation on a
    /// later bump); limitToTable confines it across the namespace-wide
    /// memtable. Collect errors land in `err`; lock errors propagate —
    /// callers map them via replyRunErr.
    fn run(
        self: *Scanner,
        eng: *engine.Engine,
        ns: u64,
        table_id: u32,
        tx: *txn.Txn,
    ) !void {
        std.debug.assert(!(self.lock and self.mode == .emit_value));
        switch (self.source) {
            .egids => |egids| for (egids) |egid| {
                const k = key.encodeEgid(egid);
                if (self.lock) try tx.lockRow(table_id, &k);

                const found = (try tx.lookup(.{
                    .namespace = ns,
                    .table_id = table_id,
                    .primary = &k,
                    .timestamp = tx.read_ts,
                })) orelse continue;
                defer found.release();

                self.collect(found.item) catch |e| {
                    self.err = e;
                    return;
                };
            },
            .index_eq => |ix| {
                // Lock the index key FIRST — even absent, it is the
                // phantom-insert guard (a racing INSERT of this key blocks).
                if (self.lock) try tx.lockRow(ix.index_table_id, ix.key);

                const entry = (try tx.lookup(.{
                    .namespace = ns,
                    .table_id = ix.index_table_id,
                    .primary = ix.key,
                    .timestamp = tx.read_ts,
                })) orelse return; // no such key (index key stays locked)
                defer entry.release();

                // The entry's value IS the row key (8 BE egid bytes,
                // written by INSERT) — use it directly, after the shape
                // check that keeps corruption from reading as "0 rows".
                const rk = entry.item.value;
                if (rk.len != 8) {
                    self.err = error.CorruptIndexEntry;
                    return;
                }
                if (self.lock) try tx.lockRow(table_id, rk);
                const found = (try tx.lookup(.{
                    .namespace = ns,
                    .table_id = table_id,
                    .primary = rk,
                    .timestamp = tx.read_ts,
                })) orelse return; // index entry without a row: skip
                defer found.release();
                self.collect(found.item) catch |e| {
                    self.err = e;
                    return;
                };
            },
            .table => {
                tx.recordScan();
                _ = try eng.forest.scan(
                    eng.allocator,
                    (zdb.level.KeyRange{}).limitToTable(ns, table_id),
                    .{ .read_ts = tx.read_ts, .pending = tx.writes },
                    self,
                    Scanner.scanCb,
                );
                // A failed collect ends the statement — don't bother
                // locking a partial capture.
                if (self.lock and self.err == null)
                    for (self.items.items) |item|
                        try tx.lockRow(table_id, item.key.primary);
            },
        }
    }
};

/// Map a Scanner.run failure onto the wire: row-lock errors (run locks
/// the matches when Scanner.lock is set) ride replyLockErr — Restart
/// propagates to the statement driver — and anything else is an
/// internal scan failure.
fn replyRunErr(
    state: *pgwire.ConnState,
    comptime verb: []const u8,
    table_name: []const u8,
    err: anyerror,
) !void {
    return switch (err) {
        error.Restart,
        error.Conflict,
        error.Deadlock,
        error.Upgrade,
        => replyLockErr(state, err),
        else => {
            log.err(
                verb ++ " '{s}': scan: {s}",
                .{ table_name, @errorName(err) },
            );
            return replyError(state, "scan failed");
        },
    };
}

/// Map a lock failure onto the wire. Deadlock: this statement's wait
/// would have closed a cycle — it is the victim; 40P01, the client
/// retries. Upgrade: SQL never upgrades shared→exclusive by
/// construction (DDL is rejected inside txn blocks), so this is an
/// internal bug surfaced loudly rather than a hang.
fn replyLockErr(state: *pgwire.ConnState, err: anyerror) !void {
    return switch (err) {
        error.Deadlock => replyErrorCode(
            state,
            pgwire.sqlstate.deadlock_detected,
            "deadlock detected — retry the transaction",
        ),
        error.Conflict => replyErrorCode(
            state,
            pgwire.sqlstate.serialization_failure,
            "could not serialize access due to concurrent update",
        ),
        error.Upgrade => {
            log.err("shared->exclusive lock upgrade attempted", .{});
            return replyError(state, "internal error: lock upgrade");
        },
        // error.Restart propagates here: the statement driver retries.
        else => err,
    };
}

/// One resolved SET assignment: the cell's decode-order index + its typed
/// replacement value.
const SetVal = struct { idx: usize, val: value.Value };

/// UPDATE's rewrite: decode `src` (its cells borrow from src — the call
/// must finish while src is alive), overlay the SET values, marshal the
/// new row into `buf`, and return its bytes (a view into buf). Pure —
/// no txn state touched. The WHERE filter was already applied to these
/// same bytes when the row was found, so there is nothing to re-verify.
fn rewriteRowBytes(
    a: std.mem.Allocator,
    ps: *const schema.PhysicalSchema,
    sets: []const SetVal,
    dec: *value.Row,
    src: []const u8,
    buf: *std.ArrayList(u8),
) ![]const u8 {
    try row.unmarshalRow(a, ps, src, &.{}, dec);
    // Overlaying into the codec's backing store is fine: the next
    // unmarshalRow refills every cell.
    for (sets) |sv| dec.cells.items[sv.idx] = sv.val;
    return row.marshalRow(a, ps, dec.items(), buf);
}

fn replyError(state: *pgwire.ConnState, msg: []const u8) !void {
    try state.sendError(.{
        .code = pgwire.sqlstate.internal_error,
        .message = msg,
    });
}

fn replyErrorCode(state: *pgwire.ConnState, code: [5]u8, msg: []const u8) !void {
    try state.sendError(.{ .code = code, .message = msg });
}
