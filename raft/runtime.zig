const std = @import("std");
const zio = @import("zio");
const node_mod = @import("node.zig");
const mux_mod = @import("mux");
const Mux = mux_mod.Mux;
const FrameHeader = mux_mod.FrameHeader;
const transport_mod = @import("transport");
const Transport = transport_mod.Transport;
const zen = @import("buffers_gen");
const log = std.log.scoped(.raft_runtime);

pub const NodeId = node_mod.NodeId;

/// Mux frame message types.
pub const msg_type_raft: u16 = 0x0001;
pub const msg_type_join_request: u16 = 0x0002;

pub const JoinReason = struct {
    pub const ok: u32 = 0;
    pub const not_leader: u32 = 1;
    pub const cluster_mismatch: u32 = 2;
    pub const invalid_addr: u32 = 3;
    pub const conf_change_rejected: u32 = 4;
};

pub const PeerSpec = Transport.PeerSpec;

pub const Config = struct {
    self_id: NodeId,
    cluster_id: u64,
    listen_addr: []const u8, // "ip:port"
    peers: []const PeerSpec,
    tick_interval_ms: u32 = 100,
};

// Module-level runtime reference for on_stream callbacks.
// Safe because: one runtime per process, set before run(), read from same zio runtime.
var g_runtime: ?*RaftRuntime = null;

pub const RaftRuntime = struct {
    allocator: std.mem.Allocator,
    node: *node_mod.Node,
    cfg: Config,
    transport: ?*Transport = null,

    pub fn init(allocator: std.mem.Allocator, node: *node_mod.Node, cfg: Config) RaftRuntime {
        return .{
            .allocator = allocator,
            .node = node,
            .cfg = cfg,
        };
    }

    pub fn deinit(self: *RaftRuntime) void {
        if (self.transport) |srv| {
            srv.deinit();
            self.allocator.destroy(srv);
            self.transport = null;
        }
        if (g_runtime == self) g_runtime = null;
    }

    /// Main entry point. Must be called inside a zio coroutine.
    /// Creates the server, starts peer connections, and runs concurrent
    /// coroutines for ticking, raft processing, and commit callbacks.
    /// Returns when node.isShuttingDown() or cancelled.
    pub fn run(self: *RaftRuntime) !void {
        g_runtime = self;

        const srv = try self.allocator.create(Transport);
        srv.* = try Transport.init(self.allocator, .{
            .self_id = self.cfg.self_id,
            .cluster_id = self.cfg.cluster_id,
            .listen_addr = self.cfg.listen_addr,
            .peers = self.cfg.peers,
            .on_stream_p0 = &handleP0Stream,
            .on_stream_p1 = &handleP1Stream,
        });
        self.transport = srv;
        errdefer {
            srv.deinit();
            self.allocator.destroy(srv);
            self.transport = null;
        }

        try srv.run(); // non-blocking: spawns accept/dial coroutines

        var group: zio.Group = .init;
        defer group.cancel();
        try group.spawn(node_mod.Node.tickLoop, .{ self.node, self.cfg.tick_interval_ms });
        try group.spawn(mainLoop, .{self});
        try group.spawn(node_mod.Node.applyLoop, .{self.node});
        try group.wait();
    }

    /// Main processing loop: blocks on event_ch, then does raft work —
    /// tick, process proposals, flush outbox.
    /// All raft state mutation happens in this single coroutine.
    // This loop can't be moved to within node, because we need to flush the outbox and it's best
    // to do this flush from this main loop. node doesn't have access to the transport layer.
    fn mainLoop(self: *RaftRuntime) void {
        const tick_us: i64 = @as(i64, self.cfg.tick_interval_ms) * 1_000;
        var last_tick: i64 = std.time.microTimestamp();

        while (!self.node.isShuttingDown()) {
            _ = self.node.event_ch.receive() catch return;

            const now = std.time.microTimestamp();
            if (now - last_tick >= tick_us) {
                self.node.tick() catch {};
                last_tick = now;
            }
            _ = self.node.processProposalRequests(64) catch {};
            self.flushOutbox();
        }
    }

    fn flushOutbox(self: *RaftRuntime) void {
        const tr = self.transport orelse return;
        const outbox = self.node.takeOutbox();
        var all_sent = true;
        for (outbox) |msg| {
            tr.sendMsg(msg.to, .p0, msg_type_raft, msg.payload) catch {
                all_sent = false;
                break;
            };
        }
        if (all_sent) {
            self.node.clearOutbox();
        }
    }

    /// Add a peer dynamically (e.g., after a join request is accepted).
    pub fn addPeer(self: *RaftRuntime, id: NodeId, addr: []const u8) !void {
        const srv = self.transport orelse return error.PeerNotConnected;
        try srv.addPeer(id, addr);
    }

    pub fn hasPeerConnection(self: *const RaftRuntime, peer_id: NodeId) bool {
        const srv = self.transport orelse return false;
        return srv.getPeerMux(peer_id, .p0) != null;
    }

    // -- on_stream callbacks (dispatched by mux) --

    fn handleP0Stream(stream: *Mux.Stream) void {
        const rt = g_runtime orelse return;
        var frame = stream.recv() catch return;

        switch (frame.header.message_type) {
            msg_type_raft => {
                defer frame.deinit(stream.mux.allocator);
                rt.node.handleInbound(frame.payload) catch {};
                rt.node.event_ch.trySend(1) catch {};
            },
            msg_type_join_request => {
                defer frame.deinit(stream.mux.allocator);
                rt.processJoinRequest(stream, frame.payload);
            },
            else => {
                frame.deinit(stream.mux.allocator);
            },
        }
    }

    fn handleP1Stream(_: *Mux.Stream) void {
        // TODO: file transfer handling
    }

    // -- join request handling (server side) --

    fn processJoinRequest(self: *RaftRuntime, stream: *Mux.Stream, payload: []u8) void {
        const parsed = zen.parseMessageWithTail(payload) catch return;
        switch (parsed) {
            .JoinRequest => |req| {
                if (req.clusterId() != self.cfg.cluster_id) {
                    self.sendJoinResponse(stream, false, self.node.getLeaderId(), JoinReason.cluster_mismatch);
                    return;
                }
                if (!self.node.isLeader()) {
                    self.sendJoinResponse(stream, false, self.node.getLeaderId(), JoinReason.not_leader);
                    return;
                }

                const peer_id: NodeId = @intCast(req.nodeId());
                self.addPeer(peer_id, req.addr()) catch {
                    self.sendJoinResponse(stream, false, self.node.id, JoinReason.invalid_addr);
                    return;
                };

                _ = self.node.proposeConfChange(.{
                    .change_type = .add_learner,
                    .node_id = peer_id,
                }) catch {
                    self.sendJoinResponse(stream, false, self.node.id, JoinReason.conf_change_rejected);
                    return;
                };
                self.sendJoinResponse(stream, true, self.node.id, JoinReason.ok);
            },
            else => {},
        }
    }

    fn sendJoinResponse(self: *RaftRuntime, stream: *Mux.Stream, accepted: bool, leader_id: ?NodeId, reason_code: u32) void {
        var writer = zen.JoinResponseWriter.init(self.allocator) catch return;
        defer writer.deinit();
        writer.set(.{
            .accepted = accepted,
            .leader_id = if (leader_id) |id| id else 0,
            .reason_code = reason_code,
            .leader_addr = "", // TODO: resolve leader address
        }) catch return;
        var reply_buf = writer.finishWithTail() catch return;
        defer reply_buf.deinit();
        const bytes = reply_buf.bytes orelse return;
        stream.send(msg_type_join_request, bytes, FrameHeader.fin_flag) catch return;
    }
};

// -- requestJoin: client-side join protocol over mux --

fn noopHandler(_: *Mux.Stream) void {}

/// Join a cluster by connecting to the leader over mux. Must be called from
/// within a zio coroutine (e.g., inside the main appMain spawned by the
/// zio.Runtime that main() creates).
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
    try Transport.sendHandshake(conn, cluster_id, node_id, .p0);
    const hs = try Transport.recvHandshake(conn);
    if (hs.cluster_id != cluster_id) return error.ClusterMismatch;

    // Temporary mux (client = even stream IDs).
    const mux = try Mux.init(allocator, conn, 0, &noopHandler);
    defer mux.deinit();

    var group: zio.Group = .init;
    defer group.cancel();
    try group.spawn(Mux.run, .{mux});

    // Send join request.
    const stream = try mux.newStream();

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
    try stream.send(msg_type_join_request, req_bytes, FrameHeader.fin_flag);

    // Read response — transfer payload ownership to JoinResponseBuffer.
    var reply = try stream.recv();
    return zen.JoinResponseBuffer.initWithTail(allocator, reply.payload, .owned) catch {
        reply.deinit(allocator);
        return error.InvalidJoinResponse;
    };
}
