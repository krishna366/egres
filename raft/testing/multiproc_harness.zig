const std = @import("std");
const raft = @import("raft");
const zen = raft.buffers;

const NodeProc = struct {
    id: raft.NodeId,
    raft_addr: std.net.Address,
    data_dir: []u8,
    child: std.process.Child,
    running: bool,
};

const NodeCfg = struct {
    id: raft.NodeId,
    raft_addr: std.net.Address,
};

fn reserveLocalPort() !u16 {
    const addr = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try addr.listen(.{ .reuse_address = true });
    defer server.deinit();
    return server.listen_address.getPort();
}

fn makeTempDir(allocator: std.mem.Allocator, id: raft.NodeId) ![]u8 {
    var buf: [192]u8 = undefined;
    const ts = std.time.nanoTimestamp();
    const path = try std.fmt.bufPrint(&buf, "/tmp/egres-raftd-{d}-{d}", .{ ts, id });
    try std.fs.cwd().makePath(path);
    return allocator.dupe(u8, path);
}

fn spawnNode(
    allocator: std.mem.Allocator,
    raftd_path: []const u8,
    cluster_id: u64,
    id: raft.NodeId,
    raft_addr: std.net.Address,
    all_nodes: []const NodeCfg,
) !NodeProc {
    const data_dir = try makeTempDir(allocator, id);
    errdefer {
        std.fs.cwd().deleteTree(data_dir) catch {};
        allocator.free(data_dir);
    }

    var addr_buf: [64]u8 = undefined;
    const addr_text = try std.fmt.bufPrint(&addr_buf, "127.0.0.1:{d}", .{raft_addr.getPort()});

    var arg_storage = std.ArrayList([]u8).empty;
    defer {
        for (arg_storage.items) |s| allocator.free(s);
        arg_storage.deinit(allocator);
    }

    var argv = std.ArrayList([]const u8).empty;
    defer argv.deinit(allocator);

    try argv.append(allocator, raftd_path);
    try argv.append(allocator, "--id");
    try arg_storage.append(allocator, try std.fmt.allocPrint(allocator, "{d}", .{id}));
    try argv.append(allocator, arg_storage.items[arg_storage.items.len - 1]);

    try argv.append(allocator, "--cluster-id");
    try arg_storage.append(allocator, try std.fmt.allocPrint(allocator, "{d}", .{cluster_id}));
    try argv.append(allocator, arg_storage.items[arg_storage.items.len - 1]);

    try argv.append(allocator, "--raft-addr");
    try arg_storage.append(allocator, try allocator.dupe(u8, addr_text));
    try argv.append(allocator, arg_storage.items[arg_storage.items.len - 1]);

    try argv.append(allocator, "--data-dir");
    try argv.append(allocator, data_dir);

    try argv.append(allocator, "--tick-ms");
    try argv.append(allocator, "30");
    try argv.append(allocator, "--dial-retry-ms");
    try argv.append(allocator, "50");

    for (all_nodes) |n| {
        if (n.id == id) continue;
        try argv.append(allocator, "--peer");
        const peer = try std.fmt.allocPrint(
            allocator,
            "{d}@127.0.0.1:{d}",
            .{ n.id, n.raft_addr.getPort() },
        );
        try arg_storage.append(allocator, peer);
        try argv.append(allocator, arg_storage.items[arg_storage.items.len - 1]);
    }

    var child = std.process.Child.init(argv.items, allocator);
    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Ignore;
    child.stderr_behavior = .Ignore;
    try child.spawn();

    return .{
        .id = id,
        .raft_addr = raft_addr,
        .data_dir = data_dir,
        .child = child,
        .running = true,
    };
}

fn stopNode(allocator: std.mem.Allocator, proc: *NodeProc) void {
    if (proc.running) {
        _ = proc.child.kill() catch {};
        _ = proc.child.wait() catch {};
        proc.running = false;
    }
    if (proc.data_dir.len != 0) {
        std.fs.cwd().deleteTree(proc.data_dir) catch {};
        allocator.free(proc.data_dir);
        proc.data_dir = "";
    }
}

fn probeLeaderId(
    allocator: std.mem.Allocator,
    node_addr: std.net.Address,
    cluster_id: u64,
) ?raft.NodeId {
    var addr_buf: [64]u8 = undefined;
    const addr_str = std.fmt.bufPrint(&addr_buf, "127.0.0.1:{d}", .{node_addr.getPort()}) catch return null;
    var resp = raft.requestJoin(allocator, addr_str, cluster_id + 1, 9_999, "127.0.0.1:1") catch {
        return null;
    };
    defer resp.deinit();

    const reader = resp.initReader() catch return null;
    _ = reader.reasonCode();
    const lid = reader.leaderId();
    if (lid == 0) return null;
    return @intCast(lid);
}

fn waitForClusterLeader(
    allocator: std.mem.Allocator,
    nodes: []NodeProc,
    cluster_id: u64,
    timeout_ms: u64,
) !raft.NodeId {
    const deadline = std.time.milliTimestamp() + @as(i64, @intCast(timeout_ms));
    while (std.time.milliTimestamp() < deadline) {
        var candidate: ?raft.NodeId = null;
        var all_match = true;

        for (nodes) |n| {
            if (!n.running) continue;
            const lid = probeLeaderId(allocator, n.raft_addr, cluster_id) orelse {
                all_match = false;
                break;
            };
            if (candidate == null) {
                candidate = lid;
            } else if (candidate.? != lid) {
                all_match = false;
                break;
            }
        }

        if (all_match and candidate != null) {
            var leader_is_running = false;
            for (nodes) |n| {
                if (!n.running) continue;
                if (n.id == candidate.?) {
                    leader_is_running = true;
                    break;
                }
            }
            if (!leader_is_running) {
                all_match = false;
            }
        }

        if (all_match and candidate != null) return candidate.?;
        std.Thread.sleep(100 * std.time.ns_per_ms);
    }
    return error.NoLeader;
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const argv = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, argv);
    if (argv.len != 2) {
        std.debug.print("usage: multiproc_harness <raftd-path>\n", .{});
        return error.InvalidArgs;
    }
    const raftd_path = argv[1];
    const cluster_id: u64 = 9001;

    var bootstrap = [_]NodeCfg{
        .{ .id = 1, .raft_addr = try std.net.Address.parseIp("127.0.0.1", try reserveLocalPort()) },
        .{ .id = 2, .raft_addr = try std.net.Address.parseIp("127.0.0.1", try reserveLocalPort()) },
        .{ .id = 3, .raft_addr = try std.net.Address.parseIp("127.0.0.1", try reserveLocalPort()) },
    };

    var nodes = try allocator.alloc(NodeProc, bootstrap.len);
    defer allocator.free(nodes);

    for (bootstrap, 0..) |cfg, i| {
        nodes[i] = try spawnNode(allocator, raftd_path, cluster_id, cfg.id, cfg.raft_addr, &bootstrap);
        // Stagger starts to avoid synchronized election timeout behavior.
        std.Thread.sleep(150 * std.time.ns_per_ms);
    }
    defer {
        for (nodes) |*n| stopNode(allocator, n);
    }

    const leader1 = try waitForClusterLeader(allocator, nodes, cluster_id, 8_000);
    std.debug.print("leader elected: {d}\n", .{leader1});

    std.Thread.sleep(2 * std.time.ns_per_s);
    const stable = try waitForClusterLeader(allocator, nodes, cluster_id, 4_000);
    if (stable != leader1) return error.LeaderUnstable;
    std.debug.print("leader stable with heartbeats: {d}\n", .{stable});

    var leader_addr: ?std.net.Address = null;
    for (nodes) |n| {
        if (n.id == leader1) {
            leader_addr = n.raft_addr;
            break;
        }
    }
    const addr = leader_addr orelse return error.NoLeader;
    var addr_buf: [64]u8 = undefined;
    const addr_str = try std.fmt.bufPrint(&addr_buf, "127.0.0.1:{d}", .{addr.getPort()});

    var join_resp = try raft.requestJoin(allocator, addr_str, cluster_id, 4, "127.0.0.1:19044");
    defer join_resp.deinit();
    const join_reader = try join_resp.initReader();
    if (!join_reader.accepted()) return error.JoinRejected;
    std.debug.print("join request accepted by leader: {d}\n", .{leader1});
}
