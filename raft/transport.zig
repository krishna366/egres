const std = @import("std");
const builtin = @import("builtin");
const node_mod = @import("node.zig");
const zen = @import("buffers_gen");
const linux = std.os.linux;
const log = std.log.scoped(.raft_transport);

pub const NodeId = node_mod.NodeId;
pub const max_message_size: usize = 4 * 1024 * 1024;

pub const JoinReason = struct {
    pub const ok: u32 = 0;
    pub const not_leader: u32 = 1;
    pub const cluster_mismatch: u32 = 2;
    pub const invalid_addr: u32 = 3;
    pub const conf_change_rejected: u32 = 4;
};

pub const TransportError = error{
    MessageTooLarge,
    InvalidFrameLength,
} || std.Io.Reader.Error || std.Io.Writer.Error || std.mem.Allocator.Error;

pub fn sendMessage(stream: std.net.Stream, payload: []const u8) TransportError!void {
    if (payload.len > max_message_size) return error.MessageTooLarge;
    if (payload.len > std.math.maxInt(u32)) return error.MessageTooLarge;

    var len_prefix: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_prefix, @intCast(payload.len), .little);

    var writer_buffer: [512]u8 = undefined;
    var writer = stream.writer(&writer_buffer);
    try writer.interface.writeAll(&len_prefix);
    if (payload.len > 0) {
        try writer.interface.writeAll(payload);
    }
    try writer.interface.flush();
}

pub fn readMessage(
    allocator: std.mem.Allocator,
    stream: std.net.Stream,
) TransportError![]u8 {
    var len_prefix: [4]u8 = undefined;
    var reader_buffer: [512]u8 = undefined;
    var reader = stream.reader(&reader_buffer);
    try reader.interface().readSliceAll(&len_prefix);

    const frame_len: usize = @intCast(std.mem.readInt(u32, &len_prefix, .little));
    if (frame_len > max_message_size) return error.InvalidFrameLength;

    const frame = try allocator.alloc(u8, frame_len);
    errdefer allocator.free(frame);
    if (frame_len > 0) {
        try reader.interface().readSliceAll(frame);
    }
    return frame;
}

pub const PeerAddr = struct {
    id: NodeId,
    address: std.net.Address,
};

pub const Config = struct {
    self_id: NodeId,
    cluster_id: u64,
    listen_addr: std.net.Address,
    peers: []const PeerAddr,
    dial_retry_ms: u32 = 500,
    tick_interval_ms: u32 = 100,
    epoll_max_events: usize = 128,
};

pub const Error = error{
    DuplicatePeerId,
    InvalidTransportMessage,
    InvalidJoinRequest,
    InvalidJoinResponse,
    UnsupportedPlatform,
} || node_mod.RaftError || TransportError || std.mem.Allocator.Error || std.posix.SocketError || std.posix.ConnectError || std.posix.BindError || std.posix.ListenError || std.posix.SetSockOptError || std.posix.GetSockNameError || std.posix.AcceptError || std.posix.EpollCreateError || std.posix.EpollCtlError;

pub fn requestJoin(
    allocator: std.mem.Allocator,
    leader_addr: std.net.Address,
    cluster_id: u64,
    node_id: NodeId,
    addr: []const u8,
) Error!zen.JoinResponseBuffer {
    const stream = try std.net.tcpConnectToAddress(leader_addr);
    defer stream.close();

    var writer = try zen.JoinRequestWriter.init(allocator);
    defer writer.deinit();
    try writer.set(.{
        .cluster_id = cluster_id,
        .node_id = node_id,
        .addr = addr,
    });
    var req_buf = try writer.finishWithTail();
    defer req_buf.deinit();
    const req_bytes = req_buf.bytes orelse return zen.Error.Invalid;
    try sendMessage(stream, req_bytes);

    const reply_bytes = try readMessage(allocator, stream);
    errdefer allocator.free(reply_bytes);
    return zen.JoinResponseBuffer.initWithTail(allocator, reply_bytes, .owned) catch return error.InvalidJoinResponse;
}

pub const RaftTransport = struct {
    const PeerState = struct {
        address: std.net.Address,
        connected_fd: ?std.posix.fd_t = null,
        last_dial_ns: ?i128 = null,
    };

    const SocketState = struct {
        stream: std.net.Stream,
        peer_id: ?NodeId = null,
        expected_peer: ?NodeId = null,
        connecting: bool = false,
    };

    allocator: std.mem.Allocator,
    node: *node_mod.Node,
    cfg: Config,

    server: std.net.Server,
    epoll_fd: std.posix.fd_t,
    epoll_events: []std.posix.system.epoll_event,

    peers: std.AutoHashMap(NodeId, PeerState),
    sockets: std.AutoHashMap(std.posix.fd_t, SocketState),

    last_tick_ns: i128,

    pub fn init(allocator: std.mem.Allocator, node: *node_mod.Node, cfg: Config) Error!RaftTransport {
        if (builtin.os.tag != .linux) return error.UnsupportedPlatform;

        var peers = std.AutoHashMap(NodeId, PeerState).init(allocator);
        errdefer peers.deinit();
        try peers.ensureTotalCapacity(@intCast(cfg.peers.len));
        for (cfg.peers) |p| {
            if (p.id == cfg.self_id) continue;
            const gop = try peers.getOrPut(p.id);
            if (gop.found_existing) return error.DuplicatePeerId;
            gop.value_ptr.* = .{ .address = p.address };
        }

        var server = try cfg.listen_addr.listen(.{
            .reuse_address = true,
            .force_nonblocking = true,
        });
        errdefer server.deinit();

        const epoll_fd = try std.posix.epoll_create1(linux.EPOLL.CLOEXEC); // Create epoll instance for this transport loop.
        errdefer std.posix.close(epoll_fd); // Release epoll fd if init fails.

        var listen_event = std.posix.system.epoll_event{
            .events = linux.EPOLL.IN,
            .data = .{ .fd = server.stream.handle },
        };
        try std.posix.epoll_ctl(epoll_fd, linux.EPOLL.CTL_ADD, server.stream.handle, &listen_event); // Watch listener fd for inbound accepts.

        const proposal_wake_fd = node.proposalWakeFd();
        var wake_event = std.posix.system.epoll_event{
            .events = linux.EPOLL.IN,
            .data = .{ .fd = proposal_wake_fd },
        };
        try std.posix.epoll_ctl(epoll_fd, linux.EPOLL.CTL_ADD, proposal_wake_fd, &wake_event);

        const epoll_events = try allocator.alloc(
            std.posix.system.epoll_event,
            if (cfg.epoll_max_events == 0) 128 else cfg.epoll_max_events,
        );
        errdefer allocator.free(epoll_events);

        return .{
            .allocator = allocator,
            .node = node,
            .cfg = cfg,
            .server = server,
            .epoll_fd = epoll_fd,
            .epoll_events = epoll_events,
            .peers = peers,
            .sockets = std.AutoHashMap(std.posix.fd_t, SocketState).init(allocator),
            .last_tick_ns = std.time.nanoTimestamp(),
        };
    }

    pub fn deinit(self: *RaftTransport) void {
        var it = self.sockets.iterator();
        while (it.next()) |kv| {
            kv.value_ptr.stream.close();
        }
        self.sockets.deinit();
        self.peers.deinit();
        self.allocator.free(self.epoll_events);
        std.posix.close(self.epoll_fd); // Close epoll instance during transport shutdown.
        self.server.deinit();
    }

    pub fn run(self: *RaftTransport) Error!void {
        while (true) {
            try self.runOnce();
        }
    }

    // This function is what drives the ticks. We can run this in a loop from run,
    // in a thread, so we can get the main node functionality happening in a
    // dedicated thread.
    pub fn runOnce(self: *RaftTransport) Error!void {
        try self.connectMissingPeers();

        const tick_ns = @as(i128, self.cfg.tick_interval_ms) * std.time.ns_per_ms;
        const elapsed_ns = std.time.nanoTimestamp() - self.last_tick_ns;
        const wait_ms: u32 = if (self.cfg.tick_interval_ms == 0 or elapsed_ns >= tick_ns)
            0
        else
            @intCast(@divFloor(tick_ns - elapsed_ns, std.time.ns_per_ms));

        // Wait for socket readiness or next tick deadline.
        const ready = std.posix.epoll_wait(self.epoll_fd, self.epoll_events, @intCast(wait_ms));

        const proposal_wake_fd = self.node.proposalWakeFd();
        var saw_proposal_wake = false;

        var i: usize = 0;
        while (i < ready) : (i += 1) {
            const ev = self.epoll_events[i];
            if (ev.data.fd == proposal_wake_fd) {
                saw_proposal_wake = true;
            } else if (ev.data.fd == self.server.stream.handle) {
                // Listener fd means accept new connections; all others are connected peer sockets.
                try self.acceptReady();
            } else {
                try self.processPeerFd(ev.data.fd, ev.events);
            }
        }

        const processed = try self.node.processProposalRequests(64);
        if (saw_proposal_wake and processed < 64) {
            self.drainProposalWakeFd();
        }

        const now = std.time.nanoTimestamp();
        if (self.cfg.tick_interval_ms == 0) {
            try self.node.tick();
        } else {
            // Catch up missed logical ticks if loop work took longer than one interval.
            while (now - self.last_tick_ns >= tick_ns) {
                self.last_tick_ns += tick_ns;
                try self.node.tick();
            }
        }

        try self.flushOutbox();
    }

    fn drainProposalWakeFd(self: *RaftTransport) void {
        var buf: [8]u8 = undefined;
        const fd = self.node.proposalWakeFd();
        while (true) {
            _ = std.posix.read(fd, &buf) catch |err| switch (err) {
                error.WouldBlock => break,
                else => break,
            };
        }
    }

    pub fn hasPeerConnection(self: *const RaftTransport, peer_id: NodeId) bool {
        const peer = self.peers.get(peer_id) orelse return false;
        return peer.connected_fd != null;
    }

    fn connectMissingPeers(self: *RaftTransport) Error!void {
        const now = std.time.nanoTimestamp();
        var it = self.peers.iterator();
        while (it.next()) |kv| {
            const peer_id = kv.key_ptr.*;
            const peer_state = kv.value_ptr;
            if (peer_state.connected_fd != null) continue;

            if (peer_state.last_dial_ns) |last| {
                const elapsed_ms = @divFloor(now - last, std.time.ns_per_ms);
                if (elapsed_ms < self.cfg.dial_retry_ms) continue;
            }
            peer_state.last_dial_ns = now;

            const opened = self.openPeerSocket(peer_state.address) catch |err| {
                log.info("dial node={d} addr={f} failed: {}", .{ peer_id, peer_state.address, err });
                continue;
            };
            const stream = opened.stream;

            self.registerStream(stream, peer_id, opened.connecting) catch |err| {
                log.info("register peer socket node={d} fd={d} failed: {}", .{ peer_id, stream.handle, err });
                stream.close();
                continue;
            };
            if (opened.connecting) continue;

            self.sendHandshake(stream) catch |err| {
                log.info("send handshake node={d} fd={d} failed: {}", .{ peer_id, stream.handle, err });
                self.closeFd(stream.handle);
                continue;
            };
            try self.bindPeer(peer_id, stream.handle);
        }
    }

    fn acceptReady(self: *RaftTransport) Error!void {
        while (true) {
            const conn = self.server.accept() catch |err| switch (err) {
                error.WouldBlock => return,
                else => return err,
            };
            errdefer conn.stream.close();
            _ = conn.address;
            try self.registerStream(conn.stream, null, false);
        }
    }

    fn registerStream(self: *RaftTransport, stream: std.net.Stream, expected_peer: ?NodeId, connecting: bool) Error!void {
        const fd = stream.handle;
        try self.sockets.put(fd, .{
            .stream = stream,
            .expected_peer = expected_peer,
            .connecting = connecting,
        });
        errdefer _ = self.sockets.remove(fd);

        try self.updateSocketEvents(fd, connecting, .CTL_ADD);
    }

    fn processPeerFd(self: *RaftTransport, fd: std.posix.fd_t, events: u32) Error!void {
        const socket_state = self.sockets.getPtr(fd) orelse return;

        if ((events & (linux.EPOLL.ERR | linux.EPOLL.HUP | linux.EPOLL.RDHUP)) != 0) {
            self.closeFd(fd);
            return;
        }

        if (socket_state.connecting) {
            if ((events & linux.EPOLL.OUT) == 0) return;
            std.posix.getsockoptError(fd) catch |err| { // Resolve non-blocking connect result via SO_ERROR.
                log.info("connect complete failed fd={d}: {}", .{ fd, err });
                self.closeFd(fd);
                return;
            };
            socket_state.connecting = false;
            try self.updateSocketEvents(fd, false, .CTL_MOD);
            self.sendHandshake(socket_state.stream) catch |err| {
                log.info("send handshake after connect failed fd={d}: {}", .{ fd, err });
                self.closeFd(fd);
                return;
            };
            if (socket_state.expected_peer) |peer_id| {
                try self.bindPeer(peer_id, fd);
            }
            if ((events & linux.EPOLL.IN) == 0) return;
        }

        if ((events & linux.EPOLL.IN) == 0) return;

        // Read exactly one length-prefixed frame from this ready socket.
        const payload = readMessage(self.allocator, socket_state.stream) catch |err| {
            self.closeFd(fd);
            return switch (err) {
                error.EndOfStream, error.ReadFailed, error.InvalidFrameLength => {},
                else => err,
            };
        };
        defer self.allocator.free(payload);

        // Once a socket is bound to a peer, every frame is a raft message.
        if (socket_state.peer_id != null) {
            // Both sides can race to dial each other; an extra handshake can
            // arrive even after this fd is bound. Ignore it idempotently.
            if (zen.parseMessageWithTail(payload)) |msg| {
                if (msg == .TransportHandshake) return;
            } else |_| {}

            // This does the handling of the raft message.
            try self.node.handleInbound(payload);
            return;
        }

        switch (zen.parseMessageWithTail(payload) catch {
            self.closeFd(fd);
            return;
        }) {
            .TransportHandshake => |hs| {
                if (hs.clusterId() != self.cfg.cluster_id) {
                    self.closeFd(fd);
                    return;
                }
                if (socket_state.expected_peer) |expected_peer| {
                    if (@as(NodeId, @intCast(hs.nodeId())) != expected_peer) {
                        self.closeFd(fd);
                        return;
                    }
                }
                // Handshake validated: bind fd <-> peer_id so subsequent frames go to node.handleInbound.
                try self.bindPeer(@intCast(hs.nodeId()), fd);
            },
            .JoinRequest => |req| try self.handleJoinRequest(fd, socket_state, req),
            else => self.closeFd(fd),
        }
    }

    fn flushOutbox(self: *RaftTransport) Error!void {
        const outbox = self.node.takeOutbox();
        var all_sent = true;
        for (outbox) |msg| {
            const peer_state = self.peers.get(msg.to) orelse {
                all_sent = false;
                break;
            };
            const fd = peer_state.connected_fd orelse {
                all_sent = false;
                break;
            };
            const socket_state = self.sockets.get(fd) orelse {
                all_sent = false;
                break;
            };
            sendMessage(socket_state.stream, msg.payload) catch |err| {
                self.closeFd(fd);
                switch (err) {
                    error.WriteFailed => {
                        all_sent = false;
                        break;
                    },
                    else => return err,
                }
            };
        }
        if (all_sent) {
            self.node.clearOutbox();
        }
    }

    fn sendHandshake(self: *RaftTransport, stream: std.net.Stream) Error!void {
        var writer = try zen.TransportHandshakeWriter.init(self.allocator);
        defer writer.deinit();
        try writer.set(.{
            .cluster_id = self.cfg.cluster_id,
            .node_id = self.cfg.self_id,
        });
        var hs_buf = try writer.finishWithTail();
        defer hs_buf.deinit();
        const bytes = hs_buf.bytes orelse return zen.Error.Invalid;
        try sendMessage(stream, bytes);
    }

    fn handleJoinRequest(self: *RaftTransport, fd: std.posix.fd_t, socket_state: *SocketState, req: zen.JoinRequestReader) Error!void {
        defer self.closeFd(fd);

        if (req.clusterId() != self.cfg.cluster_id) {
            try self.sendJoinResponse(socket_state.stream, false, self.node.getLeaderId(), JoinReason.cluster_mismatch);
            return;
        }
        if (!self.node.isLeader()) {
            try self.sendJoinResponse(socket_state.stream, false, self.node.getLeaderId(), JoinReason.not_leader);
            return;
        }

        const peer_addr = std.net.Address.parseIpAndPort(req.addr()) catch {
            try self.sendJoinResponse(socket_state.stream, false, self.node.id, JoinReason.invalid_addr);
            return;
        };
        try self.addOrUpdatePeer(@intCast(req.nodeId()), peer_addr);

        _ = self.node.proposeConfChange(.{
            .change_type = .add_learner,
            .node_id = @intCast(req.nodeId()),
        }) catch {
            try self.sendJoinResponse(socket_state.stream, false, self.node.id, JoinReason.conf_change_rejected);
            return;
        };
        try self.sendJoinResponse(socket_state.stream, true, self.node.id, JoinReason.ok);
    }

    fn sendJoinResponse(self: *RaftTransport, stream: std.net.Stream, accepted: bool, leader_id: ?NodeId, reason_code: u32) Error!void {
        var leader_addr_buf: [64]u8 = undefined;
        const leader_addr = self.formatLeaderAddr(leader_id, &leader_addr_buf);

        var writer = try zen.JoinResponseWriter.init(self.allocator);
        defer writer.deinit();
        try writer.set(.{
            .accepted = accepted,
            .leader_id = if (leader_id) |id| id else 0,
            .reason_code = reason_code,
            .leader_addr = leader_addr,
        });
        var reply_buf = try writer.finishWithTail();
        defer reply_buf.deinit();
        const bytes = reply_buf.bytes orelse return zen.Error.Invalid;
        try sendMessage(stream, bytes);
    }

    fn addOrUpdatePeer(self: *RaftTransport, peer_id: NodeId, address: std.net.Address) Error!void {
        const gop = try self.peers.getOrPut(peer_id);
        if (gop.found_existing) {
            gop.value_ptr.address = address;
            return;
        }
        gop.value_ptr.* = .{ .address = address };
    }

    fn bindPeer(self: *RaftTransport, peer_id: NodeId, fd: std.posix.fd_t) Error!void {
        const peer_state = self.peers.getPtr(peer_id) orelse return;

        if (peer_state.connected_fd) |old_fd| {
            if (old_fd != fd) self.closeFd(old_fd);
        }
        peer_state.connected_fd = fd;

        const socket_state = self.sockets.getPtr(fd) orelse return;
        socket_state.peer_id = peer_id;
        socket_state.expected_peer = null;
    }

    fn closeFd(self: *RaftTransport, fd: std.posix.fd_t) void {
        // Best-effort unregister before closing fd.
        _ = std.posix.epoll_ctl(self.epoll_fd, linux.EPOLL.CTL_DEL, fd, null) catch {};
        if (self.sockets.fetchRemove(fd)) |kv| {
            kv.value.stream.close();
            if (kv.value.peer_id) |peer_id| {
                if (self.peers.getPtr(peer_id)) |peer_state| {
                    if (peer_state.connected_fd == fd) {
                        peer_state.connected_fd = null;
                    }
                }
            }
        }
    }

    fn updateSocketEvents(self: *RaftTransport, fd: std.posix.fd_t, connecting: bool, comptime op: enum { CTL_ADD, CTL_MOD }) Error!void {
        var events: u32 = linux.EPOLL.IN | linux.EPOLL.ERR | linux.EPOLL.HUP | linux.EPOLL.RDHUP;
        if (connecting) events |= linux.EPOLL.OUT;
        var ev = std.posix.system.epoll_event{
            .events = events,
            .data = .{ .fd = fd },
        };
        // Register/update epoll interest mask.
        try std.posix.epoll_ctl(self.epoll_fd, if (op == .CTL_ADD) linux.EPOLL.CTL_ADD else linux.EPOLL.CTL_MOD, fd, &ev);
    }

    fn openPeerSocket(self: *RaftTransport, addr: std.net.Address) Error!struct { stream: std.net.Stream, connecting: bool } {
        _ = self;
        const sockfd = try std.posix.socket( // Create non-blocking TCP socket for outbound peer dial.
            addr.any.family,
            std.posix.SOCK.STREAM | std.posix.SOCK.CLOEXEC | std.posix.SOCK.NONBLOCK,
            std.posix.IPPROTO.TCP,
        );
        errdefer std.posix.close(sockfd); // Close socket if connect/setup fails.

        var connecting = false;
        // Start connect; non-blocking sockets return in-progress via errors.
        std.posix.connect(sockfd, &addr.any, addr.getOsSockLen()) catch |err| switch (err) {
            error.WouldBlock, error.ConnectionPending => connecting = true,
            else => return err,
        };

        return .{ .stream = .{ .handle = sockfd }, .connecting = connecting };
    }

    fn formatLeaderAddr(self: *RaftTransport, leader_id: ?NodeId, out: *[64]u8) []const u8 {
        const id = leader_id orelse return "";
        const addr = if (id == self.cfg.self_id)
            self.cfg.listen_addr
        else blk: {
            const peer = self.peers.get(id) orelse return "";
            break :blk peer.address;
        };
        return std.fmt.bufPrint(out, "{f}", .{addr}) catch "";
    }
};
