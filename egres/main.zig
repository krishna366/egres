const std = @import("std");
const raft = @import("raft");
const zdb = @import("zdb");
const fiber = @import("zfiber");
const pgwire = @import("pgwire");
const engine = @import("engine");
const sql = @import("sql");
const flags = @import("flags");
const utils = @import("utils");

pub const std_options = std.Options{
    .log_level = .debug,
};

/// `--listen` — where the server binds. Actual addresses are
/// `<host>:<port+offset>` for both raft and client. `offset` makes it
/// trivial to bring up N servers on one host without spelling out
/// per-node port assignments.
const ListenCfg = struct {
    host: []const u8 = "127.0.0.1",
    raft_port: u16 = 4210,
    client_port: u16 = 5210,
    sql_port: u16 = 3210,
    offset: u16 = 0,

    pub const docs = .{
        .self = "Where the server listens. raft, admin (client), and SQL ports each get +offset.",
        .host = "Hostname/IP to bind the raft, admin, and SQL listeners on.",
        .raft_port = "Base port for raft peer connections. Actual port = raft_port + offset.",
        .client_port = "Base port for the admin protocol. Actual port = client_port + offset.",
        .sql_port = "Base port for the SQL listener (pgwire protocol). Actual port = sql_port + offset.",
        .offset = "Added to every port. Convenient when running several servers on one host (e.g. offset=1 ⇒ raft=4211, admin=5211, sql=3211).",
    };
};

/// `--raft` — Raft node identity and cluster wiring. `id` is required (must
/// be > 0). `seeds` is comma-separated; if empty, the node bootstraps as a
/// single-node cluster.
const RaftCfg = struct {
    id: u64 = 0,
    cluster_id: u64 = 1,
    seeds: []const u8 = "",
    tick_ms: u32 = 20,

    pub const docs = .{
        .self = "Raft identity and cluster wiring.",
        .id = "Raft node ID (required, must be > 0).",
        .cluster_id = "Cluster ID for handshake gating.",
        .seeds = "Comma-separated raft addresses of existing peers. Empty = bootstrap as a single-node cluster.",
        .tick_ms = "Raft tick interval in milliseconds.",
    };
};

/// `--data` — on-disk locations. `dir` is required; `wal_dir` defaults to
/// `dir` if empty (single-disk setup) but can be pointed at a faster
/// device (e.g. NVMe) for write-ahead log throughput.
const DataCfg = struct {
    dir: []const u8 = "",
    wal_dir: []const u8 = "",

    pub const docs = .{
        .self = "Storage paths. WAL can live on a faster device than the LSM forest.",
        .dir = "Directory holding the LSM forest. Required.",
        .wal_dir = "Directory holding the raft WAL. Empty = same as `dir`.",
    };
};

fn parseSeedList(text: []const u8, allocator: std.mem.Allocator) ![]const []const u8 {
    var list = std.ArrayList([]const u8).empty;
    errdefer list.deinit(allocator);
    var it = std.mem.splitScalar(u8, text, ',');
    while (it.next()) |s| {
        if (s.len > 0) try list.append(allocator, s);
    }
    return list.toOwnedSlice(allocator);
}

/// Dump `--help` from the flag registry. flags.zig renders one line per
/// field with default + docstring, plus a header per registered module.
fn usage(alloc: std.mem.Allocator) void {
    var help: std.ArrayList(u8) = .empty;
    defer help.deinit(alloc);
    flags.g().writeHelpAll(&help) catch {};
    const io = utils.context.g().io;
    const stderr = std.Io.File.stderr();
    stderr.writeStreamingAll(io, help.items) catch {};
    stderr.writeStreamingAll(io, "\n") catch {};
}

const log = std.log.scoped(.egres);

/// The default database, present from a node's very first boot — created by
/// the WAL's genesis entry (see seedGenesis), never at runtime.
const default_database = "egres";

/// Seed a fresh WAL with the deterministic genesis prefix: entry 1 creates
/// the default database at the first user namespace id ('egres' is ns 16 on
/// every cluster). Every node writes the same bytes — fixed ids, term 0,
/// fixed timestamp — so raft's log matching holds across fresh nodes, and
/// the entry is committed by construction (HardState.commit_index covers
/// it): engine recovery applies it before any election or client traffic.
/// A restarted or joining node with a non-empty WAL skips this.
fn seedGenesis(allocator: std.mem.Allocator, wal: *raft.DiskWAL) !void {
    if (wal.lastIndex() != 0) return;

    var tc = [_]zdb.manifest.TreeChange{
        zdb.manifest.createTreeChange(0, zdb.manifest.first_namespace, "", ""),
    };
    tc[0].name = @constCast(default_database);
    const payload = try zdb.buffers.marshalRoot(allocator, zdb.manifest.ManifestChange{
        .cmd = 5, // ns_create
        .tree_changes = &tc,
        .sstable_changes = &.{},
        .memtable_id = 0,
    });
    defer allocator.free(payload);

    var buf = std.ArrayList(u8).empty;
    defer buf.deinit(allocator);
    var writer = try raft.buffers.LogEntryWriter.initWithBuf(allocator, &buf);
    defer writer.deinit();
    try writer.set(.{
        .index = 1,
        .term = 0,
        .timestamp = 1, // fixed; real HLC values are far larger
        .data = payload,
        .entry_type = .manifest,
    });
    _ = try writer.finishWithTail();
    const data = try buf.toOwnedSlice(allocator);
    var entry = try raft.buffers.LogEntryBuffer.initWithTail(allocator, data, .owned);
    defer entry.deinit();

    try wal.append(&.{entry});
    try wal.sync();
    try wal.setHardState(.{ .commit_index = 1 });
    log.info("seeded genesis: database '{s}' -> ns={d}", .{ default_database, zdb.manifest.first_namespace });
}

fn deinitWithLog(comptime msg: []const u8, ptr: anytype) void {
    ptr.deinit();
    log.info("deinit: " ++ msg ++ " done", .{});
}

/// Bundle passed to appMain — the parsed-flags snapshots plus derived
/// fields. `raft_addr` / `client_addr` are computed from
/// `host:port+offset` once at startup and own their backing buffers.
/// `seeds` is the comma-split version of `raft.seeds`. `wal_dir` is
/// `data.wal_dir` if set, else `data.dir`.
const Args = struct {
    listen: ListenCfg,
    raft: RaftCfg,
    data: DataCfg,
    raft_addr: []const u8,
    client_addr: []const u8,
    seeds: []const []const u8,
    wal_dir: []const u8,
};

pub fn main(init: std.process.Init) !void {
    defer log.info("shutdown complete", .{});
    utils.context.initGlobal(init);
    const allocator = utils.context.g().gpa;
    const argv = try utils.context.g().argv();

    // The in-tree fiber runtime. One worker per core; worker id 0 is the
    // dedicated `.raft` lane so raft loops get priority isolation, the rest
    // form the `.default` lane (client / compaction / snapshot work).
    // `run()` blocks this thread (worker 0) until `fiber.stop()`, which
    // `bootstrapAndStop` calls when bootstrap returns (i.e. on shutdown).
    var fiber_rt: fiber.Runtime = undefined;
    // num_workers/raft_workers omitted → auto: workers = CPU core count,
    // raft_workers ≈ 1/4 capped at 4 (see fiber.runtime.raftWorkerCount). The
    // coarse clock reads time via `context.g().io`, already initialized above.
    try fiber_rt.init(allocator, .{});
    defer fiber_rt.deinit();
    log.info("runtime initialized: {d} workers", .{fiber_rt.workers.len});

    _ = try fiber.spawn(.{}, bootstrapAndStop, .{ allocator, argv });
    fiber_rt.run();
}

/// Spawned on the default lane to drive process bootstrap. Stops the
/// runtime (unblocks `fiber_rt.run()` in `main`) whenever bootstrap
/// returns — normal shutdown or an early error.
fn bootstrapAndStop(allocator: std.mem.Allocator, argv: []const []const u8) void {
    defer fiber.stop();
    bootstrap(allocator, argv) catch |err| {
        log.err("bootstrap failed: {s}", .{@errorName(err)});
    };
}

fn bootstrap(allocator: std.mem.Allocator, argv: []const []const u8) !void {
    flags.initGlobal(allocator);
    defer flags.deinitGlobal();
    const live_listen = try flags.g().register("listen", ListenCfg);
    const live_raft = try flags.g().register("raft", RaftCfg);
    const live_data = try flags.g().register("data", DataCfg);
    const live_snapshot = try flags.g().register("snapshot", engine.SnapshotCfg);

    // --help is terminal; handle after registration so writeHelpAll has
    // every module's fields populated.
    for (argv[1..]) |a| {
        if (std.mem.eql(u8, a, "--help") or std.mem.eql(u8, a, "-h")) {
            usage(allocator);
            return;
        }
    }

    var leftover: std.ArrayList([]const u8) = .empty;
    defer leftover.deinit(allocator);
    flags.g().parseArgv(argv[1..], &leftover) catch |err| {
        std.log.err("flag parse failed: {s}", .{@errorName(err)});
        usage(allocator);
        std.process.exit(1);
    };
    if (leftover.items.len != 0) {
        std.log.err("unexpected positional '{s}' — config goes through --listen/--raft/--data flags", .{leftover.items[0]});
        usage(allocator);
        std.process.exit(1);
    }

    const listen = live_listen.snapshot();
    const raft_cfg = live_raft.snapshot();
    const data_cfg = live_data.snapshot();
    if (raft_cfg.id == 0 or data_cfg.dir.len == 0) {
        std.log.err("--raft requires id (>0); --data requires dir", .{});
        usage(allocator);
        std.process.exit(1);
    }

    // Resolve listen addresses from host + base port + offset.
    const raft_addr = try std.fmt.allocPrint(allocator, "{s}:{d}", .{ listen.host, listen.raft_port + listen.offset });
    defer allocator.free(raft_addr);
    const client_addr = try std.fmt.allocPrint(allocator, "{s}:{d}", .{ listen.host, listen.client_port + listen.offset });
    defer allocator.free(client_addr);
    // SQL (pgwire protocol) listen address.
    const sql_addr = try std.fmt.allocPrint(allocator, "{s}:{d}", .{ listen.host, listen.sql_port + listen.offset });
    defer allocator.free(sql_addr);

    // wal_dir defaults to data dir when empty — the common single-disk
    // case. Setting it lets you put WAL on a faster device than the LSM.
    const wal_dir = if (data_cfg.wal_dir.len > 0) data_cfg.wal_dir else data_cfg.dir;

    const seeds = try parseSeedList(raft_cfg.seeds, allocator);
    defer allocator.free(seeds);
    const args: Args = .{
        .listen = listen,
        .raft = raft_cfg,
        .data = data_cfg,
        .raft_addr = raft_addr,
        .client_addr = client_addr,
        .seeds = seeds,
        .wal_dir = wal_dir,
    };

    log.info("starting egres id={d} cluster={d} raft={s} admin={s} sql={s} dir={s} wal={s} tick={d}ms seeds={d}", .{
        raft_cfg.id, raft_cfg.cluster_id, raft_addr, client_addr, sql_addr, data_cfg.dir, wal_dir, raft_cfg.tick_ms, seeds.len,
    });

    // Initialize VFS with bandwidth limit.
    zdb.vfs.VFS.initGlobal(.{ .bytes_per_sec = 1024 * 1024 * 1024 });

    // Initialize global metrics before anything that might increment them.
    // zdb owns storage-layer metrics; raft owns its own propose-phase
    // histograms — they live in separate Metrics structs but both surface
    // through /stats once the server's handleStats walks both globals.
    try zdb.metrics.initGlobal(allocator);
    defer zdb.metrics.deinitGlobal();
    try raft.metrics.initGlobal(allocator);
    defer raft.metrics.deinitGlobal();

    // Open raft WAL.
    log.info("opening raft WAL at {s}", .{wal_dir});
    var wal = try raft.DiskWAL.openPath(wal_dir, allocator, 1024);
    defer deinitWithLog("wal", &wal);
    // Genesis before the node reads the log: a fresh WAL gets its
    // deterministic bootstrap prefix (default database), pre-committed.
    try seedGenesis(allocator, &wal);

    const node = try raft.Node.init(
        allocator,
        &.{},
        &wal,
        .{
            .id = raft_cfg.id,
            .cluster_id = raft_cfg.cluster_id,
            .listen_addr = raft_addr,
            .tick_interval_ms = raft_cfg.tick_ms,
        },
    );
    defer deinitWithLog("node", node);

    log.info("raft node initialized id={d}", .{raft_cfg.id});

    // Open zdb forest.
    log.info("opening forest at {s}", .{data_cfg.dir});
    var forest = try zdb.forest.Forest.open(allocator, .{
        .dir = data_cfg.dir,
    });
    defer {
        forest.close();
        log.info("deinit: forest done", .{});
    }
    log.info("forest opened", .{});

    // The replicated storage engine: builds the apply state machine + raft
    // runtime, installs the SM on the node, and binds the forest's file-id
    // allocator to the node + WAL. Owns all that node/forest wiring.
    const eng = try engine.Engine.open(allocator, node, forest, &wal, live_snapshot, raft_cfg.id);
    defer deinitWithLog("engine", eng);

    // The admin protocol front-end over the engine.
    var admin_srv = try engine.AdminServer.init(eng, args.client_addr);
    defer deinitWithLog("admin_srv", &admin_srv);

    // The SQL front-end (pgwire protocol) over the same engine. Database
    // names live in the zdb manifest, so there is no separate SQL store —
    // DDL resolves names via the forest and proposes manifest changes.
    var sql_srv = try SqlServer.init(eng, sql_addr);

    log.info("all components initialized", .{});

    var handle = try fiber.spawn(.{}, appMain, .{ eng, &admin_srv, &sql_srv, node, &args });
    handle.join();

    log.info("shutting down...", .{});
}

/// Drives the node lifecycle: bring up transport, (optionally) join the
/// cluster, then hand off to the engine (recovery + loops) and register the
/// admin front-end, finally blocking until a shutdown signal.
fn appMain(eng: *engine.Engine, admin_srv: *engine.AdminServer, sql_srv: *SqlServer, node: *raft.Node, args: *const Args) void {
    log.info("inside appMain", .{});

    // Transport must be up before the cluster join (seeded nodes) and before
    // recovery fetches missing SSTs from the leader.
    node.initTransport() catch |err| {
        log.err("failed to init transport: {s}", .{@errorName(err)});
        return;
    };

    // Seeded nodes join before any loop runs, so they receive their
    // add_learner entry instead of bootstrapping as a single-node cluster.
    if (args.seeds.len > 0) {
        log.info("appMain: joining cluster via seeds", .{});
        runJoin(args, eng.allocator);
    }

    // The engine owns recovery + applyCommits + the raft/compaction/snapshot
    // loops; register the admin front-end into its loop group.
    eng.start() catch |err| {
        log.err("engine.start failed: {s}", .{@errorName(err)});
        return;
    };
    eng.spawnListener(engine.AdminServer.runListener, .{admin_srv}) catch |err| {
        log.err("appMain: spawn admin server failed: {s}", .{@errorName(err)});
        return;
    };
    eng.spawnListener(SqlServer.runListener, .{sql_srv}) catch |err| {
        log.err("appMain: spawn sql server failed: {s}", .{@errorName(err)});
        return;
    };

    node.shutdown.install() catch |err| {
        log.err("appMain: installing signal handlers failed: {s}", .{@errorName(err)});
        return;
    };
    log.info("appMain: waiting for SIGINT/SIGTERM to shutdown", .{});
    // Blocks until SIGINT/SIGTERM fires the shutdown signal or an internal
    // fatal path fires it. The node reacts on its own (`shutdownWatchLoop`
    // closes everything raft fibers park on), so there is nothing to invoke
    // here — just wait for the fibers to drain.
    node.shutdown.wait();
    log.info("received shutdown signal, shutting down...", .{});
    eng.wait();
}

/// SQL front-end (pgwire protocol) over the engine. The handshake and
/// simple-query loop work; CREATE/DROP DATABASE execute against the manifest
/// (names resolve via the forest, changes replicate via raft). Mirrors
/// AdminServer's accept loop, and shuts down with the engine via
/// `node.isShuttingDown()`.
const SqlServer = struct {
    engine: *engine.Engine,
    listen_addr: fiber.net.IpAddress,
    /// Serializes DDL: the leader-side name-uniqueness check in
    /// buildCreateNamespace is only sound if one DDL runs at a time.
    ddl_mu: fiber.Mutex(.tracked) = .{},

    fn init(eng: *engine.Engine, addr: []const u8) !SqlServer {
        return .{ .engine = eng, .listen_addr = try fiber.net.IpAddress.parseIpAndPort(addr) };
    }

    fn deinit(self: *SqlServer) void {
        self.ddl_mu.deinit();
    }

    fn runListener(self: *SqlServer) void {
        const listener = self.listen_addr.listen(.{ .reuse_address = true }) catch |err| {
            log.err("sql: listen failed: {s}", .{@errorName(err)});
            return;
        };
        defer listener.close();
        // Break the blocking `accept()` at shutdown (see AdminServer): a watcher
        // fiber self-connects to our address when the shutdown channel closes.
        fiber.spawnDetached(.{}, engine.Engine.stopListenerOnShutdown, .{ self.engine, self.listen_addr }) catch {};
        log.info("sql listener started on port {d} (pgwire)", .{listener.port()});
        var conn_id: u64 = 0;
        while (!self.engine.node.isShuttingDown()) {
            const stream = listener.accept(.{}) catch |err| {
                log.err("sql: accept failed: {s}", .{@errorName(err)});
                return;
            };
            // Shutdown watcher's self-connection: drop it and exit.
            if (self.engine.node.isShuttingDown()) {
                stream.close();
                return;
            }
            conn_id += 1;
            fiber.spawnDetached(.{}, handleSqlConn, .{ self, stream }) catch {
                stream.close();
                continue;
            };
        }
    }
};

fn handleSqlConn(self: *SqlServer, stream: fiber.net.Stream) void {
    var s = stream;
    defer s.close();

    var handler = SqlHandler{ .srv = self };
    pgwire.serve(self.engine.allocator, &s, &handler, .{}) catch |err| {
        log.warn("sql conn serve error: {s}", .{@errorName(err)});
    };
}

/// pgwire handler. Only `onQuery` is required by `pgwire.serve`
/// (onExecute/onDescribe are @hasDecl-gated). Parses via libpg_query and
/// dispatches the DDL this slice supports; everything else is a clean error.
const SqlHandler = struct {
    srv: *SqlServer,

    pub fn onQuery(self: *SqlHandler, state: *pgwire.ConnState, sql_text: []const u8) !void {
        const alloc = self.srv.engine.allocator;
        const trimmed = std.mem.trim(u8, sql_text, " \t\r\n;");
        if (trimmed.len == 0) {
            try state.sendEmptyQueryResponse();
            return;
        }
        var stmt = sql.ddl.parseDdl(alloc, sql_text) catch {
            try state.sendError(.{
                .code = pgwire.sqlstate_syntax_error,
                .message = "syntax error",
            });
            return;
        };
        defer stmt.deinit(alloc);
        switch (stmt) {
            .create_database => |name| try self.createDatabase(state, name),
            .drop_database => |name| try self.dropDatabase(state, name),
            .unsupported => try replyError(state, "only CREATE DATABASE and DROP DATABASE are supported yet"),
        }
    }

    fn createDatabase(self: *SqlHandler, state: *pgwire.ConnState, name: []const u8) !void {
        const eng = self.srv.engine;
        self.srv.ddl_mu.lock(@src());
        defer self.srv.ddl_mu.unlock();

        var arena = std.heap.ArenaAllocator.init(eng.allocator);
        defer arena.deinit();
        const created = eng.forest.buildCreateNamespace(arena.allocator(), name) catch |err| switch (err) {
            error.NameExists => return replyError(state, "database already exists"),
            else => |e| {
                log.err("create database '{s}': {s}", .{ name, @errorName(e) });
                return replyError(state, "create database failed");
            },
        };
        eng.proposeManifestChange(created.change) catch |err| {
            log.err("create database '{s}': propose: {s}", .{ name, @errorName(err) });
            return replyError(state, "create database proposal failed");
        };
        log.info("created database '{s}' -> ns={d}", .{ name, created.namespace });
        try state.sendCommandComplete("CREATE DATABASE");
    }

    fn dropDatabase(self: *SqlHandler, state: *pgwire.ConnState, name: []const u8) !void {
        const eng = self.srv.engine;
        self.srv.ddl_mu.lock(@src());
        defer self.srv.ddl_mu.unlock();

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
        eng.proposeManifestChange(mc) catch |err| {
            log.err("drop database '{s}': propose: {s}", .{ name, @errorName(err) });
            return replyError(state, "drop database proposal failed");
        };
        log.info("dropped database '{s}' (ns={d})", .{ name, ns });
        try state.sendCommandComplete("DROP DATABASE");
    }
};

fn replyError(state: *pgwire.ConnState, msg: []const u8) !void {
    try state.sendError(.{
        .code = pgwire.sqlstate_internal_error,
        .message = msg,
    });
}

fn runJoin(args: *const Args, allocator: std.mem.Allocator) void {
    raft.joinCluster(
        allocator,
        args.seeds,
        args.raft.cluster_id,
        args.raft.id,
        args.raft_addr,
    ) catch |err| {
        std.log.err("cluster join failed: {s}", .{@errorName(err)});
    };
}
