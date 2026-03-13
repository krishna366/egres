const std = @import("std");
const zio = @import("zio");
const raft = @import("raft");
const zdb = @import("zdb");
const runtime = @import("runtime");
const zdb_server = @import("zdb_server");

pub const std_options = std.Options{
    .log_level = .debug,
};

const ParsedArgs = struct {
    id: raft.NodeId,
    cluster_id: u64,
    raft_addr: []const u8,
    client_addr: []const u8,
    data_dir: []const u8,
    peers: []PeerSpec,
    tick_ms: u32,
};

const PeerSpec = struct {
    id: raft.NodeId,
    addr: []const u8,
};

fn usage() void {
    std.debug.print(
        "usage: zdb-server --id <n> --cluster-id <n> --raft-addr <ip:port> " ++
            "--client-addr <ip:port> --data-dir <path> [--peer <id@ip:port>] [--tick-ms <n>]\n",
        .{},
    );
}

fn parsePeerSpec(text: []const u8) !PeerSpec {
    const at = std.mem.indexOfScalar(u8, text, '@') orelse return error.InvalidArgs;
    const id_text = text[0..at];
    const addr_text = text[at + 1 ..];
    const id = try std.fmt.parseInt(raft.NodeId, id_text, 10);
    return .{ .id = id, .addr = addr_text };
}

fn parseArgs(allocator: std.mem.Allocator, argv: []const []const u8) !ParsedArgs {
    var id: ?raft.NodeId = null;
    var cluster_id: u64 = 1;
    var raft_addr: ?[]const u8 = null;
    var client_addr: ?[]const u8 = null;
    var data_dir: ?[]const u8 = null;
    var peers = std.ArrayList(PeerSpec).empty;
    errdefer peers.deinit(allocator);
    var tick_ms: u32 = 20;

    var i: usize = 1;
    while (i < argv.len) {
        const arg = argv[i];
        if (std.mem.eql(u8, arg, "--id")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            id = try std.fmt.parseInt(raft.NodeId, argv[i], 10);
        } else if (std.mem.eql(u8, arg, "--cluster-id")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            cluster_id = try std.fmt.parseInt(u64, argv[i], 10);
        } else if (std.mem.eql(u8, arg, "--raft-addr")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            raft_addr = argv[i];
        } else if (std.mem.eql(u8, arg, "--client-addr")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            client_addr = argv[i];
        } else if (std.mem.eql(u8, arg, "--data-dir")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            data_dir = argv[i];
        } else if (std.mem.eql(u8, arg, "--peer")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            try peers.append(allocator, try parsePeerSpec(argv[i]));
        } else if (std.mem.eql(u8, arg, "--tick-ms")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            tick_ms = try std.fmt.parseInt(u32, argv[i], 10);
        } else {
            return error.InvalidArgs;
        }
        i += 1;
    }

    return .{
        .id = id orelse return error.InvalidArgs,
        .cluster_id = cluster_id,
        .raft_addr = raft_addr orelse return error.InvalidArgs,
        .client_addr = client_addr orelse return error.InvalidArgs,
        .data_dir = data_dir orelse return error.InvalidArgs,
        .peers = try peers.toOwnedSlice(allocator),
        .tick_ms = tick_ms,
    };
}

const log = std.log.scoped(.zdb_main);

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const argv = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, argv);

    const args = parseArgs(allocator, argv) catch |err| {
        usage();
        return err;
    };
    defer allocator.free(args.peers);

    log.info("starting zdb-server id={d} cluster={d} raft={s} client={s} data={s} tick={d}ms peers={d}", .{
        args.id, args.cluster_id, args.raft_addr, args.client_addr, args.data_dir, args.tick_ms, args.peers.len,
    });

    // Runtime pool singleton — created first so all runtimes are available.
    const pool = try runtime.Pool.init(allocator, .{});
    defer pool.deinit();
    log.info("runtime pool initialized", .{});

    // Build raft peer lists.
    var raft_peers = std.ArrayList(raft.Peer).empty;
    defer raft_peers.deinit(allocator);
    var transport_peers = std.ArrayList(raft.PeerSpec).empty;
    defer transport_peers.deinit(allocator);

    for (args.peers) |p| {
        if (p.id == args.id) continue;
        try raft_peers.append(allocator, .{
            .id = p.id,
            .next_index = 1,
            .match_index = 0,
            .role = .voter,
        });
        try transport_peers.append(allocator, .{ .id = p.id, .addr = p.addr });
    }

    // Open raft WAL.
    log.info("opening raft WAL at {s}", .{args.data_dir});
    var wal = try raft.DiskWAL.openPath(args.data_dir, allocator);
    defer wal.deinit();

    // Initialize raft node.
    const node = try raft.Node.init(
        allocator,
        args.id,
        raft_peers.items,
        wal.asRaftWAL(),
        .{},
        .voter,
    );
    defer node.deinit();
    log.info("raft node initialized id={d} peers={d}", .{ args.id, raft_peers.items.len });

    // Open zdb forest.
    log.info("opening forest at {s}", .{args.data_dir});
    var forest = try zdb.forest.Forest.open(allocator, .{
        .dir = args.data_dir,
    });
    defer forest.close();
    log.info("forest opened", .{});

    // Storage applier bridges raft commits to forest.
    var applier = zdb.applier.StorageApplier.init(allocator, forest);
    defer applier.deinit();

    // Raft runtime.
    var raft_rt = raft.RaftRuntime.init(allocator, node, .{
        .self_id = args.id,
        .cluster_id = args.cluster_id,
        .listen_addr = args.raft_addr,
        .peers = transport_peers.items,
        .tick_interval_ms = args.tick_ms,
    });
    defer raft_rt.deinit();

    // ZDB server (client-facing).
    var zdb_srv = try zdb_server.ZdbServer.init(
        allocator,
        forest,
        &applier,
        node,
        args.client_addr,
    );
    defer zdb_srv.deinit();

    log.info("all components initialized, spawning on raft pool", .{});

    var rt = try zio.Runtime.init(
        allocator,
        .{ .executors = .exact(1) },
    );
    defer rt.deinit();

    appMain(&raft_rt, &zdb_srv, node);
    // // Entry point: spawn appMain on the raft pool. It orchestrates everything.
    // var handle = try pool.get(.raft).spawn(appMain, .{ &raft_rt, &zdb_srv, node });
    // handle.join();

    log.info("shutdown complete", .{});
}

/// Runs on the raft pool. Spawns raft runtime, client server (on query pool
/// via runtime.pool() singleton), compaction loop (on background pool),
/// and waits for SIGINT to shut down.
fn appMain(raft_rt: *raft.RaftRuntime, zdb_srv: *zdb_server.ZdbServer, node: *raft.Node) void {
    log.info("inside appMain", .{});
    var group: zio.Group = .init;
    defer group.cancel();

    log.info("appMain: spawning raft runtime on raft pool", .{});
    group.spawn(runRaft, .{raft_rt}) catch return;

    log.info("appMain: spawning client server on query pool", .{});
    group.spawn(runServer, .{zdb_srv}) catch return;

    log.info("appMain: spawning compaction loop on background pool", .{});
    group.spawn(runCompaction, .{zdb_srv}) catch return;

    log.info("appMain: waiting for SIGINT (ctrl+c to shutdown)", .{});
    var sig = zio.Signal.init(.interrupt) catch return;
    defer sig.deinit();
    sig.wait() catch return;

    log.info("received SIGINT, shutting down...", .{});
    node.startShutdown();
}

fn runRaft(r: *raft.RaftRuntime) void {
    r.run() catch |err| {
        std.log.err("raft runtime stopped: {s}", .{@errorName(err)});
    };
}

fn runServer(s: *zdb_server.ZdbServer) void {
    s.runOnQueryPool() catch |err| {
        std.log.err("zdb server stopped: {s}", .{@errorName(err)});
    };
}

fn runCompaction(s: *zdb_server.ZdbServer) void {
    s.runCompactionOnBackgroundPool() catch |err| {
        std.log.err("compaction loop stopped: {s}", .{@errorName(err)});
    };
}
