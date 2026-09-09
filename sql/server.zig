//! The SQL front-end: a pgwire listener over the engine. Owns the
//! accept loop, the per-database txn machinery (lock table + commit
//! oracle + table cache), and the per-connection Handler — session
//! state, statement dispatch, the DML restart driver, and transaction
//! lifetime. The statements themselves live in siblings: ddl.zig
//! (catalog/manifest choreography), dml.zig (INSERT/SELECT/DELETE/
//! UPDATE), plan.zig (index vs. scan) and exec.zig (the row
//! pipeline). Mirrors AdminServer's accept loop, and shuts down with
//! the engine via `node.isShuttingDown()`.

const std = @import("std");
const engine = @import("engine");
const fiber = @import("zfiber");
const pgwire = @import("pgwire");
const zdb = @import("zdb");
const zen = zdb.buffers;

const _catalog = @import("catalog.zig");
const _parse = @import("parse.zig");
const _ddl = @import("ddl.zig");
const _index_ddl = @import("index_ddl.zig");
const _dml = @import("dml.zig");
const _exec = @import("exec.zig");
const _row = @import("row.zig");
const _key = @import("key.zig");
const _schema = @import("schema.zig");
const _table = @import("table.zig");
const _txn = @import("txn.zig");
const _value = @import("value.zig");
const Oracle = @import("oracle.zig").Oracle;

const log = std.log.scoped(.sql);

const String = _value.String;

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
    tables: std.AutoHashMap(u32, *_table.Table),

    fn init(allocator: std.mem.Allocator, ns: u64) !Database {
        return .{
            .ns = ns,
            .locks = try fiber.SemaphoreMap.init(allocator),
            .oracle = Oracle.init(allocator),
            .tables = std.AutoHashMap(u32, *_table.Table).init(allocator),
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
            var tbl: ?*_table.Table = null;
            var table_id: u32 = 0;
            for (items) |item| {
                if (tbl == null or item.key.table_id != table_id) {
                    table_id = item.key.table_id;
                    tbl = self.tables.get(table_id);
                }
                const cached = tbl orelse continue;
                if (_key.decodeEgid(item.key.primary)) |egid| {
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
    catalog_ps: _schema.PhysicalSchema,
    /// Compiled layout of the INDEX catalog table's rows (table 2), the
    /// same compiled-in schema in every namespace — one compile serves all.
    index_catalog_ps: _schema.PhysicalSchema,
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
            .catalog_ps = try _schema.compile(eng.allocator, _catalog.logical_schema, _catalog.logical_schema.current_version),
            .index_catalog_ps = try _schema.compile(eng.allocator, _catalog.index_logical_schema, _catalog.index_logical_schema.current_version),
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
    fn openTableLocked(self: *Server, db: *Database, table_id: u32) !*_table.Table {
        if (db.tables.get(table_id)) |t| return t;

        var loaded = try _table.loadTableSchema(self.engine.allocator, self.engine.forest, &self.catalog_ps, &self.index_catalog_ps, db.ns, table_id);
        errdefer loaded.deinit();
        // Seed the egid counter past the table's max applied row id (covers
        // snapshot SSTs the apply observer never saw). Empty table → start at 1.
        if (try self.engine.forest.maxKey(self.engine.allocator, db.ns, table_id)) |mk| {
            defer mk.free(self.engine.allocator);
            loaded.next_egid = (_key.decodeEgid(mk.primary) orelse 0) + 1;
        } else {
            loaded.next_egid = 1;
        }

        const t = try self.engine.allocator.create(_table.Table);
        errdefer self.engine.allocator.destroy(t);
        t.* = loaded;
        try db.tables.put(table_id, t);
        return t;
    }

    /// The cached Table for (namespace, table_id) — schema for INSERT/SELECT.
    pub fn openTable(self: *Server, ns: u64, table_id: u32) !*_table.Table {
        const db = try self.database(ns);
        db.tables_mu.lock(@src());
        defer db.tables_mu.unlock();
        return self.openTableLocked(db, table_id);
    }

    /// Swap the cached Table for a freshly loaded one — DDL that
    /// changed the schema (CREATE/DROP INDEX) calls this while
    /// holding the table intent lock EXCLUSIVELY, which is what makes
    /// destroying the old entry safe: DML holds its Table only under
    /// the shared intent lock (taken before openTable), and
    /// applyCommitted touches cached tables only under tables_mu.
    /// `next_egid` is runtime state, not catalog state — carry the
    /// larger of the old counter and the fresh seed across the swap.
    pub fn reloadTable(self: *Server, ns: u64, table_id: u32) !void {
        const db = try self.database(ns);
        db.tables_mu.lock(@src());
        defer db.tables_mu.unlock();
        const old = db.tables.fetchRemove(table_id) orelse {
            // Nothing cached: the next openTable loads fresh anyway.
            return;
        };
        errdefer {
            old.value.deinit();
            self.engine.allocator.destroy(old.value);
        }
        const t = try self.openTableLocked(db, table_id);
        t.next_egid = @max(t.next_egid, old.value.next_egid);
        old.value.deinit();
        self.engine.allocator.destroy(old.value);
    }

    /// Drop a table's cache entry outright (DROP TABLE) — same
    /// exclusive-intent-lock safety argument as reloadTable.
    pub fn forgetTable(self: *Server, ns: u64, table_id: u32) !void {
        const db = try self.database(ns);
        db.tables_mu.lock(@src());
        defer db.tables_mu.unlock();
        if (db.tables.fetchRemove(table_id)) |old| {
            old.value.deinit();
            self.engine.allocator.destroy(old.value);
        }
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
        // egids start at 1 (openTableLocked seeds the counter); 0 is
        // never a row.
        std.debug.assert(egid >= 1);
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
/// libpg_query and dispatches the statements this slice supports;
/// everything else is a clean error.
pub const Handler = struct {
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
    txn: ?*_txn.Txn = null,
    /// The executor's row batch — one per connection, created on the
    /// first DML and reused by every statement after (a portal will
    /// keep it across Execute messages). See `batch`.
    exec_batch: ?_exec.Batch = null,

    /// Session teardown: roll back any open transaction. Wired as a
    /// defer in handleConn, so it runs on every exit from serve.
    pub fn deinit(self: *Handler) void {
        if (self.txn) |t| {
            self.destroyTxn(t);
            self.txn = null;
        }
        if (self.exec_batch) |*b| b.deinit();
    }

    /// The session's row batch, made on first use.
    pub fn batch(self: *Handler) !*_exec.Batch {
        if (self.exec_batch == null) {
            self.exec_batch = try _exec.Batch.init(self.srv.engine.allocator);
        }
        return &self.exec_batch.?;
    }

    pub fn destroyTxn(self: *Handler, t: *_txn.Txn) void {
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
    pub fn sessionNamespace(self: *Handler, state: *pgwire.ConnState) !?u64 {
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
    pub fn ddlReady(self: *Handler, state: *pgwire.ConnState) !bool {
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
        // Parse into an arena: the Stmt borrows its strings from the
        // libpg_query tree, and the arena reclaims all of it — tree and
        // borrowed statement alike — when this scope ends.
        var arena = std.heap.ArenaAllocator.init(self.srv.engine.allocator);
        defer arena.deinit();
        const stmt = _parse.statement(arena.allocator(), sql_text) catch {
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
                .create_database,
                .drop_database,
                .create_table,
                .drop_table,
                .create_index,
                .drop_index,
                => return replyErrorCode(state, pgwire.sqlstate.active_sql_transaction, "DDL inside a transaction block is not supported"),
                else => {},
            }
        }

        switch (stmt) {
            .create_database => |name| try _ddl.createDatabase(self, state, name),
            .drop_database => |name| try _ddl.dropDatabase(self, state, name),
            .create_table => |ct| try _ddl.createTable(self, state, ct),
            .drop_table => |dt| try _ddl.dropTable(self, state, dt),
            .create_index => |ci| try _index_ddl.createIndex(self, state, ci),
            .drop_index => |di| try _index_ddl.dropIndex(self, state, di),
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
    fn runDml(self: *Handler, state: *pgwire.ConnState, stmt: _parse.Stmt) !void {
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
                .insert => |ins| _dml.insert(self, state, ins),
                .select => |sel| _dml.select(self, state, sel),
                .delete => |del| _dml.delete(self, state, del),
                .update => |up| _dml.update(self, state, up),
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

    pub fn newTxn(self: *Handler, ns: u64) !*_txn.Txn {
        const eng = self.srv.engine;
        // Best-effort catch-up: a leader waits out its committed tail
        // (once per term) so read_ts covers it; a non-leader keeps what
        // it has rather than erroring here.
        eng.catchUp() catch {};
        // Database first, read_ts second: a commit applying between
        // the two lands in this database's now-existing oracle, so a
        // conflict newer than our snapshot can never go unrecorded.
        const db = try self.srv.database(ns);
        const t = try eng.allocator.create(_txn.Txn);
        errdefer eng.allocator.destroy(t);
        t.* = _txn.Txn.init(
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
    pub fn statementTxn(self: *Handler, ns: u64) !*_txn.Txn {
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
    pub fn dmlTxn(
        self: *Handler,
        state: *pgwire.ConnState,
        ns: u64,
        table_id: u32,
    ) !?*_txn.Txn {
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
    pub fn endImplicit(self: *Handler) void {
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
    pub fn commitTxn(self: *Handler, a: std.mem.Allocator, tx: *_txn.Txn) !void {
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
    pub fn commitOrReply(
        self: *Handler,
        state: *pgwire.ConnState,
        a: std.mem.Allocator,
        tx: *_txn.Txn,
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
};

/// Map a lock failure onto the wire. Deadlock: this statement's wait
/// would have closed a cycle — it is the victim; 40P01, the client
/// retries. Upgrade: SQL never upgrades shared→exclusive by
/// construction (DDL is rejected inside txn blocks), so this is an
/// internal bug surfaced loudly rather than a hang.
pub fn replyLockErr(state: *pgwire.ConnState, err: anyerror) !void {
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
pub fn replyError(state: *pgwire.ConnState, msg: []const u8) !void {
    try state.sendError(.{
        .code = pgwire.sqlstate.internal_error,
        .message = msg,
    });
}

pub fn replyErrorCode(state: *pgwire.ConnState, code: [5]u8, msg: []const u8) !void {
    try state.sendError(.{ .code = code, .message = msg });
}
