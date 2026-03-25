const std = @import("std");
const zio = @import("zio");
const node_mod = @import("node.zig");
const serial_mod = @import("serial");
const Serial = serial_mod.Serial;
const transport_mod = @import("transport");
const Transport = transport_mod.Transport;
const zen = @import("buffers_gen");
const log = std.log.scoped(.raft_runtime);

pub const NodeId = node_mod.NodeId;

const logEvery = @import("utils").logEvery;

pub const msg_type_raft = node_mod.msg_type_raft;
pub const msg_type_join_request = node_mod.msg_type_join_request;

// Module-level runtime reference for on_stream callbacks.
// Safe because: one runtime per process, set before run(), read from same zio runtime.
var raft_runtime: ?*RaftRuntime = null;

pub const RaftRuntime = struct {
    allocator: std.mem.Allocator,
    node: *node_mod.Node,
    transport: ?*Transport = null,

    pub fn init(allocator: std.mem.Allocator, node: *node_mod.Node) RaftRuntime {
        return .{
            .allocator = allocator,
            .node = node,
        };
    }

    pub fn deinit(self: *RaftRuntime) void {
        if (self.transport) |tr| {
            tr.deinit();
            self.allocator.destroy(tr);
            self.transport = null;
        }
        if (raft_runtime == self) raft_runtime = null;
    }

    /// Main entry point. Must be called inside a zio coroutine.
    /// Creates the server, starts peer connections, and runs concurrent
    /// coroutines for ticking, raft processing, and commit callbacks.
    /// Returns when node.isShuttingDown() or cancelled.
    pub fn run(self: *RaftRuntime) !void {
        raft_runtime = self;
        const cfg = self.node.config;
        const tr = try self.allocator.create(Transport);
        tr.* = try Transport.init(self.allocator, .{
            .self_id = cfg.id,
            .cluster_id = cfg.cluster_id,
            .listen_addr = cfg.listen_addr,
            .on_message_p0 = &handleP0Message,
            .on_message_p1 = &handleP1Message,
        });
        self.transport = tr;
        self.node.on_conf_applied = &onConfApplied;
        self.node.on_conf_applied_ctx = @ptrCast(self);
        self.node.on_send = &onSend;
        self.node.on_send_ctx = @ptrCast(self);
        errdefer {
            tr.deinit();
            self.allocator.destroy(tr);
            self.transport = null;
        }

        try tr.run(); // non-blocking: spawns accept/dial coroutines

        // Register peers restored from WAL so dial loops start.
        var peer_it = self.node.peers.iterator();
        while (peer_it.next()) |kv| {
            const peer = kv.value_ptr.*;
            if (peer.addr.len > 0) {
                tr.addPeer(peer.id, peer.addr) catch |err| {
                    log.err("failed to add restored peer {d}: {s}", .{ peer.id, @errorName(err) });
                };
            }
        }

        var group: zio.Group = .init;
        defer group.wait() catch {};
        try group.spawn(node_mod.Node.tickLoop, .{ self.node, cfg.tick_interval_ms });
        try group.spawn(node_mod.Node.mainLoop, .{ self.node, cfg.tick_interval_ms });
        try group.spawn(node_mod.Node.applyLoop, .{self.node});
    }

    // -- on_message callbacks (dispatched by Serial) --

    fn handleP0Message(conn: *Serial, message_type: u16, payload: []const u8) void {
        const rt = raft_runtime orelse return;
        switch (message_type) {
            msg_type_raft => {
                const owned = rt.allocator.dupe(u8, payload) catch return;
                rt.node.inbound_ch.send(.{
                    .message_type = message_type,
                    .payload = owned,
                }) catch {
                    rt.allocator.free(owned);
                    return;
                };
                rt.node.event_ch.trySend(1) catch {};
            },
            msg_type_join_request => {
                // Handle join requests in the Serial reader's coroutine directly.
                // This blocks the reader until the join is processed, which is fine —
                // the joiner waits for a response anyway.
                rt.processJoinRequest(conn, payload);
            },
            else => {},
        }
    }

    fn handleP1Message(_: *Serial, _: u16, _: []const u8) void {
        // TODO: file transfer handling
    }

    fn onSend(ctx: *anyopaque, _: NodeId, peer_id: NodeId, payload: []const u8) void {
        const self: *RaftRuntime = @ptrCast(@alignCast(ctx));
        const tr = self.transport orelse return;
        tr.sendMsg(peer_id, .p0, msg_type_raft, payload) catch |err| {
            logEvery(5, "sendMsg to peer {d} failed: {s}", .{ peer_id, @errorName(err) });
        };
    }

    fn onConfApplied(ctx: *anyopaque, cc: node_mod.ConfChange) void {
        const self: *RaftRuntime = @ptrCast(@alignCast(ctx));
        const tr = self.transport orelse return;
        log.info("onConfApplied called: {any}", .{cc});
        switch (cc.change_type) {
            .add_voter, .add_learner => {
                log.info("adding a peer: {d}", .{cc.node_id});
                if (cc.addr.len > 0 and cc.node_id != self.node.id) {
                    tr.addPeer(cc.node_id, cc.addr) catch |err| {
                        log.err("addPeer for {d} got err: {}", .{ cc.node_id, err });
                    };
                }
            },
            .remove_node => {
                if (cc.node_id != self.node.id) {
                    tr.removePeer(cc.node_id);
                }
            },
            else => {},
        }
    }

    // -- join request handling (server side) --

    pub const JoinReason = struct {
        pub const ok: u32 = 0;
        pub const not_leader: u32 = 1;
        pub const cluster_mismatch: u32 = 2;
        pub const conf_change_rejected: u32 = 3;
        pub const invalid_addr: u32 = 4;
    };

    const JoinCtx = struct {
        done: zio.Channel(u8) = undefined,
        status: node_mod.CommitStatus = .committed,

        fn init(self: *JoinCtx, alloc: std.mem.Allocator) !void {
            self.done = try zio.Channel(u8).initAlloc(alloc, 1);
        }

        fn deinit(self: *JoinCtx) void {
            self.done.deinit();
        }
    };

    fn onJoinCommit(ctx_raw: *anyopaque, _: node_mod.ProposalEntry, status: node_mod.CommitStatus) void {
        const ctx: *JoinCtx = @ptrCast(@alignCast(ctx_raw));
        ctx.status = status;
        ctx.done.send(1) catch {};
    }

    fn processJoinRequest(self: *RaftRuntime, conn: *Serial, payload: []const u8) void {
        const parsed = zen.parseMessageWithTail(payload) catch return;
        switch (parsed) {
            .JoinRequest => |req| {
                if (req.clusterId() != self.node.config.cluster_id) {
                    self.sendJoinResponse(conn, false, self.node.getLeaderId(), JoinReason.cluster_mismatch);
                    return;
                }
                if (!self.node.isLeader()) {
                    self.sendJoinResponse(conn, false, self.node.getLeaderId(), JoinReason.not_leader);
                    return;
                }

                const peer_id: NodeId = @intCast(req.nodeId());
                if (peer_id == self.node.id or self.node.peers.contains(peer_id)) {
                    // Already in the cluster — accept without proposing.
                    self.sendJoinResponse(conn, true, self.node.id, JoinReason.ok);
                    return;
                }

                var ctx = JoinCtx{};
                ctx.init(self.allocator) catch {
                    self.sendJoinResponse(conn, false, self.node.id, JoinReason.conf_change_rejected);
                    return;
                };
                defer ctx.deinit();

                self.node.proposeConfChange(.{
                    .change_type = .add_learner,
                    .node_id = @intCast(req.nodeId()),
                    .addr = req.addr(),
                }, &onJoinCommit, @ptrCast(&ctx)) catch |err| {
                    log.err("join proposeConfChange failed: {s}", .{@errorName(err)});
                    self.sendJoinResponse(conn, false, self.node.id, JoinReason.conf_change_rejected);
                    return;
                };

                _ = ctx.done.receive() catch {
                    self.sendJoinResponse(conn, false, self.node.id, JoinReason.conf_change_rejected);
                    return;
                };

                const accepted = ctx.status == .committed or ctx.status == .existing_node;
                self.sendJoinResponse(conn, accepted, self.node.id, if (accepted) JoinReason.ok else JoinReason.conf_change_rejected);
            },
            else => {},
        }
    }

    fn sendJoinResponse(self: *RaftRuntime, conn: *Serial, accepted: bool, leader_id: ?NodeId, reason_code: u32) void {
        const leader_addr: []const u8 = if (leader_id) |lid| blk: {
            if (lid == self.node.id) break :blk self.node.config.listen_addr;
            if (self.node.peers.get(lid)) |peer| break :blk peer.addr;
            break :blk "";
        } else "";
        var writer = zen.JoinResponseWriter.init(self.allocator) catch return;
        defer writer.deinit();
        writer.set(.{
            .accepted = accepted,
            .leader_id = if (leader_id) |lid| lid else 0,
            .reason_code = reason_code,
            .leader_addr = leader_addr,
        }) catch return;
        var reply_buf = writer.finishWithTail() catch return;
        defer reply_buf.deinit();
        const bytes = reply_buf.bytes orelse return;
        conn.send(msg_type_join_request, bytes) catch |err| {
            log.err("sendJoinResponse failed: {s}", .{@errorName(err)});
        };
    }
};

// -- requestJoin: client-side join protocol over mux --

/// Join a cluster by connecting to the leader. Must be called from
/// within a zio coroutine.
pub fn requestJoin(
    allocator: std.mem.Allocator,
    leader_addr: []const u8,
    cluster_id: u64,
    node_id: NodeId,
    self_addr: []const u8,
) !zen.JoinResponseBuffer {
    const addr = try zio.net.IpAddress.parseIpAndPort(leader_addr);
    const conn = try addr.connect(.{});
    conn.socket.setNoDelay(true) catch {};

    // Transport-level binary handshake.
    try Transport.sendHandshake(conn, cluster_id, node_id, .p0, .join);
    const hs = try Transport.recvHandshake(conn);
    if (hs.cluster_id != cluster_id) return error.ClusterMismatch;

    // Temporary Serial connection for the join exchange.
    const s = try Serial.init(allocator, conn);
    defer s.deinit();

    // Send join request.
    var writer = try zen.JoinRequestWriter.init(allocator);
    defer writer.deinit();
    try writer.set(.{
        .cluster_id = cluster_id,
        .node_id = node_id,
        .addr = self_addr,
    });
    var req_buf = try writer.finishWithTail();
    defer req_buf.deinit();
    const req_bytes = req_buf.bytes orelse return error.Invalid;
    try s.send(msg_type_join_request, req_bytes);

    // Read response.
    const reply = try s.recvOne();
    return zen.JoinResponseBuffer.initWithTail(allocator, reply.payload, .owned) catch {
        if (reply.payload.len > 0) allocator.free(reply.payload);
        return error.InvalidJoinResponse;
    };
}

/// Try seeds in order, follow NOT_LEADER redirects, retry with backoff.
/// Must be called from a zio coroutine.
pub fn joinCluster(
    allocator: std.mem.Allocator,
    seeds: []const []const u8,
    cluster_id: u64,
    node_id: NodeId,
    self_addr: []const u8,
) !void {
    const max_attempts: u32 = 30;
    var backoff_ms: u32 = 250;

    var attempt: u32 = 0;
    while (attempt < max_attempts) : (attempt += 1) {
        log.info("joinCluster: attempt {d}/{d}", .{ attempt + 1, max_attempts });
        for (seeds) |seed| {
            const target = seed;
            log.info("joinCluster: trying seed {s}", .{target});
            var resp = requestJoin(allocator, target, cluster_id, node_id, self_addr) catch |err| {
                log.err("joinCluster: requestJoin to {s} failed: {s}", .{ target, @errorName(err) });
                continue;
            };
            defer resp.deinit();

            const reader = resp.initReader() catch |err| {
                log.err("joinCluster: initReader failed: {s}", .{@errorName(err)});
                continue;
            };
            if (reader.accepted()) {
                log.info("join accepted by {s}", .{target});
                return;
            }

            const reason = reader.reasonCode();
            log.info("joinCluster: rejected by {s}, reason={d}", .{ target, reason });
            if (reason == RaftRuntime.JoinReason.not_leader) {
                const leader_addr = reader.leaderAddr();
                if (leader_addr.len > 0) {
                    log.info("joinCluster: redirecting to leader at {s}", .{leader_addr});
                    var resp2 = requestJoin(allocator, leader_addr, cluster_id, node_id, self_addr) catch |err| {
                        log.err("joinCluster: redirect requestJoin failed: {s}", .{@errorName(err)});
                        continue;
                    };
                    defer resp2.deinit();
                    const r2 = resp2.initReader() catch continue;
                    if (r2.accepted()) {
                        log.info("join accepted by leader at {s}", .{leader_addr});
                        return;
                    }
                    log.info("joinCluster: leader redirect also rejected, reason={d}", .{r2.reasonCode()});
                }
            }
        }

        log.info("joinCluster: backing off {d}ms", .{backoff_ms});
        zio.sleep(zio.Duration.fromMilliseconds(backoff_ms)) catch return error.Cancelled;
        backoff_ms = @min(backoff_ms * 2, 5_000);
    }
    return error.JoinFailed;
}
