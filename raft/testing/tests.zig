const std = @import("std");
const raft = @import("raft");
const cluster_mod = @import("cluster.zig");
const zen = raft.buffers;

const TestCluster = cluster_mod.TestCluster;
const TestLog = cluster_mod.TestLog;
const Config = raft.Config;
const NodeId = raft.NodeId;

fn reserveLocalPort() !u16 {
    const addr = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try addr.listen(.{
        .reuse_address = true,
    });
    defer server.deinit();
    return server.listen_address.getPort();
}

fn defaultConfig() Config {
    return .{
        .heartbeat_ticks = 2,
        .election_timeout_min_ticks = 5,
        .election_timeout_max_ticks = 15,
        .max_entries_per_msg = 64,
        .max_msg_size = 1_048_576,
    };
}

fn findLeader(cluster: *TestCluster) ?NodeId {
    var it = cluster.nodes.valueIterator();
    while (it.next()) |entry| {
        const node = entry.*.node;
        if (node.isLeader()) return node.id;
    }
    return null;
}

fn countLeaders(cluster: *TestCluster) usize {
    var count: usize = 0;
    var it = cluster.nodes.valueIterator();
    while (it.next()) |entry| {
        const node = entry.*.node;
        if (node.isLeader()) count += 1;
    }
    return count;
}

test "leader election" {
    const cfg = defaultConfig();
    var ids = [_]NodeId{ 1, 2, 3 };

    var cluster = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg);
    defer cluster.deinit();

    try cluster.runTicks(20);

    const leader = findLeader(&cluster) orelse return error.NoLeader;
    _ = leader;
    try std.testing.expectEqual(@as(usize, 1), countLeaders(&cluster));
}

test "heartbeats keep followers stable" {
    const cfg = defaultConfig();
    var ids = [_]NodeId{ 1, 2, 3 };

    var cluster = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg);
    defer cluster.deinit();

    try cluster.runTicks(20);
    const leader = findLeader(&cluster) orelse return error.NoLeader;

    try cluster.runTicks(20);
    const leader2 = findLeader(&cluster) orelse return error.NoLeader;
    try std.testing.expectEqual(leader, leader2);
    try std.testing.expectEqual(@as(usize, 1), countLeaders(&cluster));
}

test "leader re-election after isolation" {
    const cfg = defaultConfig();
    var ids = [_]NodeId{ 1, 2, 3 };

    var cluster = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg);
    defer cluster.deinit();

    try cluster.runTicks(20);
    const leader1 = findLeader(&cluster) orelse return error.NoLeader;

    cluster.setDropFrom(leader1, true);
    try cluster.runTicks(30);

    const leader2 = findLeader(&cluster) orelse return error.NoLeader;
    try std.testing.expect(leader2 != leader1);
    try std.testing.expectEqual(@as(usize, 1), countLeaders(&cluster));
}

test "learner never votes or becomes leader" {
    const cfg = defaultConfig();
    var ids = [_]NodeId{ 1, 2, 3, 4 };
    var learners = [_]NodeId{4};

    var cluster = try TestCluster.init(std.testing.allocator, &ids, &learners, cfg);
    defer cluster.deinit();

    try cluster.runTicks(30);

    const leader = findLeader(&cluster) orelse return error.NoLeader;
    try std.testing.expect(leader != 4);

    if (cluster.getNode(4)) |node| {
        try std.testing.expect(!node.isLeader());
        try std.testing.expect(node.state == .learner);
    } else {
        return error.MissingNode;
    }
}

test "pre-vote avoids term bump without quorum" {
    var ids = [_]NodeId{ 1, 2, 3 };

    var cfg_pre = defaultConfig();
    cfg_pre.enable_prevote = true;
    var cluster_pre = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg_pre);
    defer cluster_pre.deinit();

    var cfg_no = defaultConfig();
    cfg_no.enable_prevote = false;
    var cluster_no = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg_no);
    defer cluster_no.deinit();

    // Drop all outbound messages so no quorum can be reached.
    inline for ([_]NodeId{ 1, 2, 3 }) |id| {
        cluster_pre.setDropFrom(id, true);
        cluster_no.setDropFrom(id, true);
    }

    const node1_pre = cluster_pre.getNode(1) orelse return error.MissingNode;
    const node1_no = cluster_no.getNode(1) orelse return error.MissingNode;
    const term_before = node1_pre.current_term;

    try cluster_pre.runTicks(30);
    try cluster_no.runTicks(30);

    // With pre-vote enabled, isolated node should not bump term.
    try std.testing.expectEqual(term_before, node1_pre.current_term);
    // Without pre-vote, the node will start elections and bump term.
    try std.testing.expect(node1_no.current_term > term_before);
}

test "convert to voter and learner adjust quorum" {
    var cfg = defaultConfig();
    cfg.enable_prevote = false;
    var ids = [_]NodeId{ 1, 2, 3 };

    var cluster = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg);
    defer cluster.deinit();

    try cluster.runTicks(20);
    const leader_id = findLeader(&cluster) orelse return error.NoLeader;
    const leader = cluster.getNode(leader_id) orelse return error.MissingNode;

    const quorum_before = leader.quorum_size;

    _ = try leader.proposeConfChange(.{ .change_type = .add_learner, .node_id = 4 });
    leader.commit_index = leader.log.lastIndex();
    leader.applyCommitted();
    try std.testing.expectEqual(quorum_before, leader.quorum_size);

    _ = try leader.proposeConfChange(.{ .change_type = .to_voter, .node_id = 4 });
    leader.commit_index = leader.log.lastIndex();
    leader.applyCommitted();
    try std.testing.expect(leader.quorum_size > quorum_before);

    _ = try leader.proposeConfChange(.{ .change_type = .to_learner, .node_id = 4 });
    leader.commit_index = leader.log.lastIndex();
    leader.applyCommitted();
    try std.testing.expectEqual(quorum_before, leader.quorum_size);
}

test "candidate steps down on higher term" {
    const cfg = defaultConfig();
    var ids = [_]NodeId{ 1, 2, 3 };

    var cluster = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg);
    defer cluster.deinit();

    try cluster.runTicks(20);

    const leader = findLeader(&cluster) orelse return error.NoLeader;
    const follower_id: NodeId = if (leader == 1) 2 else 1;

    const follower = cluster.getNode(follower_id) orelse return error.MissingNode;
    const higher_term = follower.current_term + 5;

    var writer = try zen.AppendEntriesWriter.init(std.testing.allocator);
    defer writer.deinit();
    try writer.set(.{
        .term = higher_term,
        .leader_id = follower_id,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &[_]u32{},
        .leader_commit = 0,
    });
    var buf = try writer.finishWithTail();
    defer buf.deinit();
    const encoded = buf.bytes orelse return error.Invalid;

    const leader_node = cluster.getNode(leader) orelse return error.MissingNode;
    try leader_node.handleInbound(encoded);

    try std.testing.expect(!leader_node.isLeader());
    try std.testing.expect(leader_node.current_term == higher_term);
}

test "proposal callback fires on commit" {
    var cfg = defaultConfig();
    cfg.enable_prevote = false;

    var test_wal = try TestLog.init(std.testing.allocator, 1);
    defer test_wal.deinit(std.testing.allocator);
    var node = try raft.Node.init(
        std.testing.allocator,
        1,
        &[_]raft.Peer{},
        test_wal.asRaftWAL(),
        cfg,
        .voter,
    );
    defer node.deinit();
    try node.becomeLeader();

    const Ctx = struct {
        called: bool = false,
        entry: ?raft.ProposalEntry = null,
        status: ?raft.CommitStatus = null,
    };
    const callback = struct {
        fn run(raw: *anyopaque, entry: raft.ProposalEntry, status: raft.CommitStatus) void {
            const ctx: *Ctx = @ptrCast(@alignCast(raw));
            ctx.called = true;
            ctx.entry = entry;
            ctx.status = status;
        }
    };

    var ctx = Ctx{};
    try node.proposeWithCallback("hello", callback.run, &ctx);
    _ = try node.processProposalRequests(16);
    _ = node.processCommitCallbacks(16);

    try std.testing.expect(ctx.called);
    try std.testing.expectEqual(@as(u64, 1), (ctx.entry orelse return error.Invalid).index);
    try std.testing.expectEqual(raft.CommitStatus.committed, ctx.status orelse return error.Invalid);
}

test "raft runtime starts and stops cleanly" {
    var cfg = defaultConfig();
    cfg.enable_prevote = false;

    const port = try reserveLocalPort();
    const listen_addr = try std.net.Address.parseIp("127.0.0.1", port);

    var test_wal = try TestLog.init(std.testing.allocator, 1);
    defer test_wal.deinit(std.testing.allocator);
    var node = try raft.Node.init(
        std.testing.allocator,
        1,
        &[_]raft.Peer{},
        test_wal.asRaftWAL(),
        cfg,
        .voter,
    );
    defer node.deinit();

    try node.becomeLeader();
    var runtime = try raft.RaftRuntime.init(std.testing.allocator, &node, .{
        .self_id = 1,
        .cluster_id = 1001,
        .listen_addr = listen_addr,
        .peers = &[_]raft.PeerAddr{},
        .dial_retry_ms = 10,
        .tick_interval_ms = 1,
    });
    defer runtime.deinit();
    try std.testing.expect(@intFromPtr(runtime.transport.node) == @intFromPtr(runtime.node));

    try runtime.start();
    std.Thread.sleep(5 * std.time.ns_per_ms);
    runtime.stop();
    runtime.join();
}

test "raft runtime processes proposal" {
    var cfg = defaultConfig();
    cfg.enable_prevote = false;

    const port = try reserveLocalPort();
    const listen_addr = try std.net.Address.parseIp("127.0.0.1", port);

    var test_wal = try TestLog.init(std.testing.allocator, 1);
    defer test_wal.deinit(std.testing.allocator);
    var node = try raft.Node.init(
        std.testing.allocator,
        1,
        &[_]raft.Peer{},
        test_wal.asRaftWAL(),
        cfg,
        .voter,
    );
    defer node.deinit();
    try node.becomeLeader();

    var runtime = try raft.RaftRuntime.init(std.testing.allocator, &node, .{
        .self_id = 1,
        .cluster_id = 1002,
        .listen_addr = listen_addr,
        .peers = &[_]raft.PeerAddr{},
        .dial_retry_ms = 10,
        .tick_interval_ms = 1,
    });
    defer runtime.deinit();
    try std.testing.expect(@intFromPtr(runtime.transport.node) == @intFromPtr(runtime.node));
    try runtime.start();
    const Ctx = struct {
        done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    };
    const callback = struct {
        fn run(raw: *anyopaque, entry: raft.ProposalEntry, status: raft.CommitStatus) void {
            const ctx: *Ctx = @ptrCast(@alignCast(raw));
            _ = entry;
            _ = status;
            ctx.done.store(true, .seq_cst);
        }
    };

    var ctx = Ctx{};
    try runtime.node.proposeWithCallback("runtime-cb", callback.run, &ctx);
    // const processed = try runtime.node.processProposalRequests(16);
    // try std.testing.expectEqual(@as(usize, 1), processed);
    // runtime.node.advance();

    var observed = false;
    var spins: usize = 0;
    while (spins < 2000) : (spins += 1) {
        if (ctx.done.load(.seq_cst)) {
            observed = true;
            break;
        }
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }

    try std.testing.expect(observed);
    try std.testing.expectEqual(@as(u64, 1), runtime.node.log.lastIndex());
    try std.testing.expectEqual(@as(u64, 1), runtime.node.last_applied.load(.seq_cst));
}

test "raft runtime stop wakes blocked callback thread" {
    const cfg = defaultConfig();

    const port = try reserveLocalPort();
    const listen_addr = try std.net.Address.parseIp("127.0.0.1", port);

    var test_wal = try TestLog.init(std.testing.allocator, 1);
    defer test_wal.deinit(std.testing.allocator);
    var node = try raft.Node.init(
        std.testing.allocator,
        1,
        &[_]raft.Peer{},
        test_wal.asRaftWAL(),
        cfg,
        .voter,
    );
    defer node.deinit();
    var runtime = try raft.RaftRuntime.init(std.testing.allocator, &node, .{
        .self_id = 1,
        .cluster_id = 1003,
        .listen_addr = listen_addr,
        .peers = &[_]raft.PeerAddr{},
        .dial_retry_ms = 10,
        .tick_interval_ms = 1,
    });
    defer runtime.deinit();

    try runtime.start();
    std.Thread.sleep(5 * std.time.ns_per_ms);
    runtime.stop();
    runtime.join();
}

test "raft runtime propose after shutdown returns QueueClosed" {
    const cfg = defaultConfig();

    const port = try reserveLocalPort();
    const listen_addr = try std.net.Address.parseIp("127.0.0.1", port);

    var test_wal = try TestLog.init(std.testing.allocator, 1);
    defer test_wal.deinit(std.testing.allocator);
    var node = try raft.Node.init(
        std.testing.allocator,
        1,
        &[_]raft.Peer{},
        test_wal.asRaftWAL(),
        cfg,
        .voter,
    );
    defer node.deinit();
    var runtime = try raft.RaftRuntime.init(std.testing.allocator, &node, .{
        .self_id = 1,
        .cluster_id = 1004,
        .listen_addr = listen_addr,
        .peers = &[_]raft.PeerAddr{},
        .dial_retry_ms = 10,
        .tick_interval_ms = 1,
    });
    defer runtime.deinit();

    runtime.stop();
    const callback = struct {
        fn run(_: *anyopaque, _: raft.ProposalEntry, _: raft.CommitStatus) void {}
    };
    try std.testing.expectError(
        raft.RaftError.QueueClosed,
        runtime.node.proposeWithCallback("x", callback.run, undefined),
    );
}

test "log replication" {
    const cfg = defaultConfig();
    var ids = [_]NodeId{ 1, 2, 3 };

    var cluster = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg);
    defer cluster.deinit();

    try cluster.runTicks(20);
    const leader = findLeader(&cluster) orelse return error.NoLeader;
    const leader_node = cluster.getNode(leader) orelse return error.MissingNode;

    var prng = std.Random.DefaultPrng.init(1234567);
    const rand = prng.random();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var expected = std.ArrayList([]const u8).empty;
    defer expected.deinit(alloc);

    var i: usize = 0;
    while (i < 10) : (i += 1) {
        const len = rand.intRangeAtMost(u8, 1, 16);
        const buf = try alloc.alloc(u8, len);
        rand.bytes(buf);
        const dup = try alloc.dupe(u8, buf);
        try expected.append(alloc, dup);
        _ = try leader_node._propose(buf);
        try cluster.runTicks(1);
    }

    var it = cluster.nodes.valueIterator();
    while (it.next()) |entry| {
        const node = entry.*.node;
        if (node.id == leader) continue;
        try std.testing.expectEqual(@as(u64, @intCast(expected.items.len)), node.log.lastIndex());
        for (expected.items, 0..) |data, idx| {
            const log_entry = (try node.log.entryRef(@intCast(idx + 1))) orelse return error.MissingEntry;
            const reader = try zen.LogEntryReader.parseRoot(log_entry.bytes);
            try std.testing.expectEqualSlices(u8, data, reader.data());
        }
    }
}

test "framed tcp transport ping-pong" {
    const port = try reserveLocalPort();
    const listen_addr = try std.net.Address.parseIp("127.0.0.1", port);
    var server = try listen_addr.listen(.{ .reuse_address = true });
    defer server.deinit();

    const Ctx = struct {
        server: *std.net.Server,
    };
    const worker = struct {
        fn run(ctx: *Ctx) !void {
            const conn = try ctx.server.accept();
            defer conn.stream.close();
            const incoming = try raft.transport.readMessage(std.heap.page_allocator, conn.stream);
            defer std.heap.page_allocator.free(incoming);
            try std.testing.expectEqualSlices(u8, "ping", incoming);
            try raft.transport.sendMessage(conn.stream, "pong");
        }
    };

    var ctx = Ctx{ .server = &server };
    const t = try std.Thread.spawn(.{}, worker.run, .{&ctx});
    defer t.join();

    const client = try std.net.tcpConnectToAddress(listen_addr);
    defer client.close();
    try raft.transport.sendMessage(client, "ping");
    const reply = try raft.transport.readMessage(std.testing.allocator, client);
    defer std.testing.allocator.free(reply);
    try std.testing.expectEqualSlices(u8, "pong", reply);
}

test "join request is rejected by non-leader" {
    const cfg = defaultConfig();

    const port = try reserveLocalPort();
    const listen_addr = try std.net.Address.parseIp("127.0.0.1", port);

    var test_wal = try TestLog.init(std.testing.allocator, 1);
    defer test_wal.deinit(std.testing.allocator);
    var node = try raft.Node.init(
        std.testing.allocator,
        1,
        &[_]raft.Peer{},
        test_wal.asRaftWAL(),
        cfg,
        .voter,
    );
    defer node.deinit();

    const tcfg = raft.RaftTransportConfig{
        .self_id = 1,
        .cluster_id = 42,
        .listen_addr = listen_addr,
        .peers = &[_]raft.PeerAddr{},
        .dial_retry_ms = 10,
        .tick_interval_ms = 1,
    };
    var transport = try raft.RaftTransport.init(std.testing.allocator, &node, tcfg);
    defer transport.deinit();

    const JoinCtx = struct {
        mutex: std.Thread.Mutex = .{},
        done: bool = false,
        err: ?anyerror = null,
        result: ?raft.buffers.JoinResponseBuffer = null,
    };
    var join_ctx = JoinCtx{};
    const join_worker = struct {
        fn run(ctx: *JoinCtx, addr: std.net.Address) void {
            const res = raft.requestJoin(std.heap.page_allocator, addr, 42, 2, "127.0.0.1:19001") catch |err| {
                ctx.mutex.lock();
                defer ctx.mutex.unlock();
                ctx.err = err;
                ctx.done = true;
                return;
            };
            ctx.mutex.lock();
            defer ctx.mutex.unlock();
            ctx.result = res;
            ctx.done = true;
        }
    };
    const t = try std.Thread.spawn(.{}, join_worker.run, .{ &join_ctx, listen_addr });
    defer t.join();

    var done = false;
    var spins: usize = 0;
    while (!done and spins < 2000) : (spins += 1) {
        try transport.runOnce();
        join_ctx.mutex.lock();
        done = join_ctx.done;
        join_ctx.mutex.unlock();
    }
    try std.testing.expect(done);
    join_ctx.mutex.lock();
    defer join_ctx.mutex.unlock();
    try std.testing.expect(join_ctx.err == null);
    var res = join_ctx.result orelse return error.Invalid;
    defer res.deinit();
    const res_reader = try res.initReader();
    try std.testing.expect(!res_reader.accepted());
}

test "leader accepts join request" {
    const cfg = defaultConfig();

    const port = try reserveLocalPort();
    const listen_addr = try std.net.Address.parseIp("127.0.0.1", port);

    var test_wal = try TestLog.init(std.testing.allocator, 1);
    defer test_wal.deinit(std.testing.allocator);
    var node = try raft.Node.init(
        std.testing.allocator,
        1,
        &[_]raft.Peer{},
        test_wal.asRaftWAL(),
        cfg,
        .voter,
    );
    defer node.deinit();
    try node.becomeLeader();

    const tcfg = raft.RaftTransportConfig{
        .self_id = 1,
        .cluster_id = 43,
        .listen_addr = listen_addr,
        .peers = &[_]raft.PeerAddr{},
        .dial_retry_ms = 10,
        .tick_interval_ms = 1,
    };
    var transport = try raft.RaftTransport.init(std.testing.allocator, &node, tcfg);
    defer transport.deinit();

    const JoinCtx = struct {
        mutex: std.Thread.Mutex = .{},
        done: bool = false,
        err: ?anyerror = null,
        result: ?raft.buffers.JoinResponseBuffer = null,
    };
    var join_ctx = JoinCtx{};
    const join_worker = struct {
        fn run(ctx: *JoinCtx, addr: std.net.Address) void {
            const res = raft.requestJoin(std.heap.page_allocator, addr, 43, 2, "127.0.0.1:19002") catch |err| {
                ctx.mutex.lock();
                defer ctx.mutex.unlock();
                ctx.err = err;
                ctx.done = true;
                return;
            };
            ctx.mutex.lock();
            defer ctx.mutex.unlock();
            ctx.result = res;
            ctx.done = true;
        }
    };
    const t = try std.Thread.spawn(.{}, join_worker.run, .{ &join_ctx, listen_addr });
    defer t.join();

    var done = false;
    var spins: usize = 0;
    while (!done and spins < 2000) : (spins += 1) {
        try transport.runOnce();
        join_ctx.mutex.lock();
        done = join_ctx.done;
        join_ctx.mutex.unlock();
    }
    try std.testing.expect(done);
    join_ctx.mutex.lock();
    defer join_ctx.mutex.unlock();
    try std.testing.expect(join_ctx.err == null);
    var res = join_ctx.result orelse return error.Invalid;
    defer res.deinit();
    const res_reader = try res.initReader();
    try std.testing.expect(res_reader.accepted());
}

test "single-node leader adds joiner as learner" {
    const alloc = std.heap.page_allocator;

    var cfg = defaultConfig();
    cfg.enable_prevote = false;
    cfg.election_timeout_min_ticks = 5000;
    cfg.election_timeout_max_ticks = 5000;

    const leader_port = try reserveLocalPort();
    const learner_port = try reserveLocalPort();
    const leader_addr = try std.net.Address.parseIp("127.0.0.1", leader_port);
    const learner_addr = try std.net.Address.parseIp("127.0.0.1", learner_port);

    var learner_addr_buf: [32]u8 = undefined;
    const learner_addr_str = try std.fmt.bufPrint(
        &learner_addr_buf,
        "127.0.0.1:{d}",
        .{learner_port},
    );

    var leader_wal = try TestLog.init(alloc, 1);
    defer leader_wal.deinit(alloc);
    var leader = try raft.Node.init(
        alloc,
        1,
        &[_]raft.Peer{},
        leader_wal.asRaftWAL(),
        cfg,
        .voter,
    );
    defer leader.deinit();
    try leader.becomeLeader();

    const payloads = [_][]const u8{ "a", "bb", "ccc" };
    for (payloads) |p| {
        _ = try leader._propose(p);
    }

    var learner_wal = try TestLog.init(alloc, 2);
    defer learner_wal.deinit(alloc);
    var learner = try raft.Node.init(
        alloc,
        2,
        &[_]raft.Peer{
            .{
                .id = 1,
                .next_index = 1,
                .match_index = 0,
                .role = .voter,
            },
        },
        learner_wal.asRaftWAL(),
        cfg,
        .learner,
    );
    defer learner.deinit();

    var leader_transport = try raft.RaftTransport.init(alloc, &leader, .{
        .self_id = 1,
        .cluster_id = 44,
        .listen_addr = leader_addr,
        .peers = &[_]raft.PeerAddr{},
        .dial_retry_ms = 10,
        .tick_interval_ms = 1,
    });
    defer leader_transport.deinit();

    var learner_transport = try raft.RaftTransport.init(alloc, &learner, .{
        .self_id = 2,
        .cluster_id = 44,
        .listen_addr = learner_addr,
        .peers = &[_]raft.PeerAddr{
            .{
                .id = 1,
                .address = leader_addr,
            },
        },
        .dial_retry_ms = 10,
        .tick_interval_ms = 1,
    });
    defer learner_transport.deinit();

    const LoopCtx = struct {
        stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
        err: ?anyerror = null,
        last_index: ?*std.atomic.Value(u64) = null,
        node: *raft.Node,
        transport: *raft.RaftTransport,

        fn run(ctx: *@This()) void {
            while (!ctx.stop.load(.seq_cst)) {
                ctx.transport.runOnce() catch |err| {
                    ctx.err = err;
                    return;
                };
                if (ctx.last_index) |ptr| {
                    ptr.store(ctx.node.log.lastIndex(), .seq_cst);
                }
            }
        }
    };

    var learner_last = std.atomic.Value(u64).init(0);
    var leader_loop = LoopCtx{
        .node = &leader,
        .transport = &leader_transport,
    };
    var learner_loop = LoopCtx{
        .node = &learner,
        .transport = &learner_transport,
        .last_index = &learner_last,
    };

    var leader_thread: ?std.Thread = null;
    var learner_thread: ?std.Thread = null;
    defer {
        leader_loop.stop.store(true, .seq_cst);
        learner_loop.stop.store(true, .seq_cst);
        if (leader_thread) |t| t.join();
        if (learner_thread) |t| t.join();
    }
    leader_thread = try std.Thread.spawn(.{}, LoopCtx.run, .{&leader_loop});

    var join_buf = try raft.requestJoin(
        alloc,
        leader_addr,
        44,
        2,
        learner_addr_str,
    );
    defer join_buf.deinit();
    const join_reader = try join_buf.initReader();
    try std.testing.expect(join_reader.accepted());

    learner_thread = try std.Thread.spawn(.{}, LoopCtx.run, .{&learner_loop});

    var replicated = false;
    var spins: usize = 0;
    while (spins < 10_000) : (spins += 1) {
        if (learner_last.load(.seq_cst) >= payloads.len) {
            replicated = true;
            break;
        }
        std.Thread.sleep(2 * std.time.ns_per_ms);
    }
    leader_loop.stop.store(true, .seq_cst);
    learner_loop.stop.store(true, .seq_cst);
    if (leader_thread) |t| t.join();
    if (learner_thread) |t| t.join();
    leader_thread = null;
    learner_thread = null;

    if (leader_loop.err) |err| return err;
    if (learner_loop.err) |err| return err;
    try std.testing.expect(replicated);

    const joined = leader.peers.get(2) orelse return error.MissingNode;
    try std.testing.expectEqual(raft.PeerRole.learner, joined.role);
    try std.testing.expectEqual(leader.log.lastIndex(), learner.log.lastIndex());
    for (payloads, 0..) |expected, idx| {
        const log_entry = (try learner.log.entryRef(@intCast(idx + 1))) orelse return error.MissingEntry;
        const reader = try zen.LogEntryReader.parseRoot(log_entry.bytes);
        try std.testing.expectEqualSlices(u8, expected, reader.data());
    }
}

test "follower catches up after disconnect and reconnect" {
    var cfg = defaultConfig();
    cfg.enable_prevote = false;

    var ids = [_]NodeId{ 1, 2, 3 };
    var cluster = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg);
    defer cluster.deinit();

    try cluster.runTicks(40);
    const leader_id = findLeader(&cluster) orelse return error.NoLeader;
    const lagging_id: NodeId = if (leader_id != 2) 2 else 3;
    const lagging = cluster.getNode(lagging_id) orelse return error.MissingNode;

    const disconnect_start: usize = 10;
    const reconnect_at: usize = 40;
    const total_props: usize = 80;

    var proposed_count: usize = 0;
    while (proposed_count < total_props) {
        if (proposed_count == disconnect_start) {
            cluster.setDropFrom(lagging_id, true);
            cluster.setDropTo(lagging_id, true);
        }
        if (proposed_count == reconnect_at) {
            cluster.setDropFrom(lagging_id, false);
            cluster.setDropTo(lagging_id, false);
        }

        const current_leader_id = findLeader(&cluster) orelse {
            try cluster.runTicks(1);
            continue;
        };
        const current_leader = cluster.getNode(current_leader_id) orelse return error.MissingNode;

        const payload = try std.fmt.allocPrint(
            std.testing.allocator,
            "{d}",
            .{proposed_count + 1},
        );
        defer std.testing.allocator.free(payload);
        _ = current_leader._propose(payload) catch |err| switch (err) {
            raft.RaftError.NotLeader => {
                try cluster.runTicks(1);
                continue;
            },
            else => return err,
        };
        proposed_count += 1;
        try cluster.runTicks(1);
    }

    try cluster.runTicks(80);
    const leader = cluster.getNode(findLeader(&cluster) orelse return error.NoLeader) orelse
        return error.MissingNode;
    try std.testing.expectEqual(leader.log.lastIndex(), lagging.log.lastIndex());

    var idx: u64 = 1;
    while (idx <= leader.log.lastIndex()) : (idx += 1) {
        const leader_entry = (try leader.log.entryRef(idx)) orelse
            return error.MissingEntry;
        const lagging_entry = (try lagging.log.entryRef(idx)) orelse
            return error.MissingEntry;

        const leader_reader = try zen.LogEntryReader.parseRoot(leader_entry.bytes);
        const lagging_reader = try zen.LogEntryReader.parseRoot(lagging_entry.bytes);
        try std.testing.expectEqual(leader_reader.term(), lagging_reader.term());
        try std.testing.expectEqualSlices(u8, leader_reader.data(), lagging_reader.data());
    }
}
