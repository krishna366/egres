const std = @import("std");
const raft = @import("raft");

const ParsedArgs = struct {
    id: raft.NodeId,
    cluster_id: u64,
    raft_addr: std.net.Address,
    data_dir: []const u8,
    peers: []PeerSpec,
    tick_ms: u32,
    dial_retry_ms: u32,
};

const PeerSpec = struct {
    id: raft.NodeId,
    addr: std.net.Address,
};

fn usage() void {
    std.debug.print(
        "usage: raftd --id <n> --cluster-id <n> --raft-addr <ip:port> " ++
            "--data-dir <path> [--peer <id@ip:port>] [--tick-ms <n>] " ++
            "[--dial-retry-ms <n>]\n",
        .{},
    );
}

fn parsePeerSpec(text: []const u8) !PeerSpec {
    const at = std.mem.indexOfScalar(u8, text, '@') orelse return error.InvalidArgs;
    const id_text = text[0..at];
    const addr_text = text[at + 1 ..];
    const id = try std.fmt.parseInt(raft.NodeId, id_text, 10);
    const addr = try std.net.Address.parseIpAndPort(addr_text);
    return .{ .id = id, .addr = addr };
}

fn parseArgs(allocator: std.mem.Allocator, argv: []const []const u8) !ParsedArgs {
    var id: ?raft.NodeId = null;
    var cluster_id: u64 = 1;
    var raft_addr: ?std.net.Address = null;
    var data_dir: ?[]const u8 = null;
    var peers = std.ArrayList(PeerSpec).empty;
    errdefer peers.deinit(allocator);
    var tick_ms: u32 = 100;
    var dial_retry_ms: u32 = 250;

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
            raft_addr = try std.net.Address.parseIpAndPort(argv[i]);
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
        } else if (std.mem.eql(u8, arg, "--dial-retry-ms")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            dial_retry_ms = try std.fmt.parseInt(u32, argv[i], 10);
        } else {
            return error.InvalidArgs;
        }
        i += 1;
    }

    return .{
        .id = id orelse return error.InvalidArgs,
        .cluster_id = cluster_id,
        .raft_addr = raft_addr orelse return error.InvalidArgs,
        .data_dir = data_dir orelse return error.InvalidArgs,
        .peers = try peers.toOwnedSlice(allocator),
        .tick_ms = tick_ms,
        .dial_retry_ms = dial_retry_ms,
    };
}

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

    var raft_peers = std.ArrayList(raft.Peer).empty;
    defer raft_peers.deinit(allocator);
    var transport_peers = std.ArrayList(raft.PeerAddr).empty;
    defer transport_peers.deinit(allocator);

    for (args.peers) |p| {
        if (p.id == args.id) continue;
        try raft_peers.append(allocator, .{
            .id = p.id,
            .next_index = 1,
            .match_index = 0,
            .role = .voter,
        });
        try transport_peers.append(allocator, .{ .id = p.id, .address = p.addr });
    }

    var wal = try raft.DiskWAL.openPath(args.data_dir, allocator);
    defer wal.deinit();

    var node = try raft.Node.init(
        allocator,
        args.id,
        raft_peers.items,
        wal.asRaftWAL(),
        .{},
        .voter,
    );
    defer node.deinit();

    var runtime = try raft.RaftRuntime.init(allocator, &node, .{
        .self_id = args.id,
        .cluster_id = args.cluster_id,
        .listen_addr = args.raft_addr,
        .peers = transport_peers.items,
        .dial_retry_ms = args.dial_retry_ms,
        .tick_interval_ms = args.tick_ms,
    });
    defer runtime.deinit();

    try runtime.start();
    while (true) {
        std.Thread.sleep(200 * std.time.ns_per_ms);
    }
}
