const std = @import("std");
const zio = @import("zio");
const mux_mod = @import("mux");
const Mux = mux_mod.Mux;
const FrameHeader = mux_mod.FrameHeader;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.transport);

const handshake_size: usize = 17;

pub const Transport = struct {
    pub const Priority = enum(u8) { p0 = 0, p1 = 1 };

    pub const PeerSpec = struct {
        id: u64,
        addr: []const u8, // "ip:port"
    };

    pub const Config = struct {
        self_id: u64,
        cluster_id: u64,
        listen_addr: []const u8, // "ip:port"
        peers: []const PeerSpec,
        on_stream_p0: *const fn (*Mux.Stream) void,
        on_stream_p1: *const fn (*Mux.Stream) void,
    };

    const PeerSlot = struct {
        addr: ?zio.net.IpAddress, // null for inbound-only peers
        mux: [2]?*Mux, // indexed by @intFromEnum(Priority)
    };

    allocator: Allocator,
    cfg: Config,
    listen_addr: zio.net.IpAddress,
    mu: zio.Mutex,
    peers: std.AutoHashMap(u64, *PeerSlot),
    group: zio.Group,
    listener: ?zio.net.Server,

    pub fn init(allocator: Allocator, cfg: Config) !Transport {
        const listen_addr = try zio.net.IpAddress.parseIpAndPort(cfg.listen_addr);

        var peers = std.AutoHashMap(u64, *PeerSlot).init(allocator);
        for (cfg.peers) |spec| {
            const addr = try zio.net.IpAddress.parseIpAndPort(spec.addr);
            const slot = try allocator.create(PeerSlot);
            slot.* = .{ .addr = addr, .mux = .{ null, null } };
            try peers.put(spec.id, slot);
        }
        log.info("node {d}: initialized with {d} peers, cluster={d}", .{ cfg.self_id, cfg.peers.len, cfg.cluster_id });
        return .{
            .allocator = allocator,
            .cfg = cfg,
            .listen_addr = listen_addr,
            .mu = .init,
            .peers = peers,
            .group = .init,
            .listener = null,
        };
    }

    pub fn deinit(self: *Transport) void {
        log.info("node {d}: shutting down", .{self.cfg.self_id});
        self.group.cancel();
        log.debug("node {d}: group cancelled", .{self.cfg.self_id});

        if (self.listener) |*l| l.close();

        var it = self.peers.valueIterator();
        while (it.next()) |slot_ptr| {
            const slot = slot_ptr.*;
            for (0..2) |i| {
                if (slot.mux[i]) |m| {
                    m.deinit();
                }
            }
            self.allocator.destroy(slot);
        }
        self.peers.deinit();
    }

    /// Add a peer dynamically and start dialing it.
    /// Safe to call while the server is running.
    pub fn addPeer(self: *Transport, id: u64, addr: []const u8) !void {
        const parsed = try zio.net.IpAddress.parseIpAndPort(addr);

        self.mu.lock() catch return error.Cancelled;
        defer self.mu.unlock();

        if (self.peers.get(id)) |slot| {
            // Peer already known; update address.
            log.debug("node {d}: updating addr for peer {d}", .{ self.cfg.self_id, id });
            slot.addr = parsed;
            return;
        }

        const slot = try self.allocator.create(PeerSlot);
        slot.* = .{ .addr = parsed, .mux = .{ null, null } };
        try self.peers.put(id, slot);

        log.info("node {d}: added peer {d} addr={s}", .{ self.cfg.self_id, id, addr });

        // Start dial loops for this peer.
        for ([_]Priority{ .p0, .p1 }) |pri| {
            self.group.spawn(dialLoop, .{ self, id, pri }) catch {};
        }
    }

    /// Start the server. Spawns accept and dial coroutines into the server's
    /// internal group. Non-blocking — returns immediately. Must be called inside
    /// zio runtime. Call deinit() to stop everything.
    pub fn run(self: *Transport) !void {
        if (self.listener == null) {
            self.listener = self.listen_addr.listen(.{ .reuse_address = true }) catch |err| {
                log.err("node {d}: listen failed on {s}: {}", .{ self.cfg.self_id, self.cfg.listen_addr, err });
                return error.ListenFailed;
            };
        }
        log.info("node {d}: listening on {s}", .{ self.cfg.self_id, self.cfg.listen_addr });

        try self.group.spawn(acceptLoop, .{self});

        // Spawn one dial coroutine per (peer, priority) pair.
        var it = self.peers.iterator();
        while (it.next()) |kv| {
            const peer_id = kv.key_ptr.*;
            for ([_]Priority{ .p0, .p1 }) |pri| {
                self.group.spawn(dialLoop, .{ self, peer_id, pri }) catch {};
            }
        }
    }

    /// Get mux for a peer at given priority. Returns null if not connected.
    pub fn getPeerMux(self: *Transport, peer_id: u64, priority: Priority) ?*Mux {
        self.mu.lock() catch return null;
        defer self.mu.unlock();
        const slot = self.peers.get(peer_id) orelse return null;
        return slot.mux[@intFromEnum(priority)];
    }

    /// Send a single message to a peer. Opens a stream, sends with syn+fin.
    pub fn sendMsg(self: *Transport, peer_id: u64, priority: Priority, message_type: u16, payload: []const u8) !void {
        const m = self.getPeerMux(peer_id, priority) orelse return error.PeerNotConnected;
        const stream = try m.newStream();
        // TODO: Ensure that the payload is smaller than allowed length.
        try stream.send(message_type, payload, FrameHeader.fin_flag);
    }

    fn acceptLoop(self: *Transport) void {
        defer log.debug("node {d}: accept loop exiting", .{self.cfg.self_id});
        while (true) {
            const conn = self.listener.?.accept() catch return;
            conn.socket.setNoDelay(true) catch {};
            self.group.spawn(handleInbound, .{ self, conn }) catch {
                conn.close();
            };
        }
    }

    fn handleInbound(self: *Transport, conn: zio.net.Stream) void {
        const hs = recvHandshake(conn) catch {
            log.warn("node {d}: inbound handshake failed", .{self.cfg.self_id});
            conn.close();
            return;
        };
        log.debug("node {d}: inbound handshake from node {d} pri={s}", .{
            self.cfg.self_id, hs.node_id, @tagName(hs.priority),
        });

        if (hs.cluster_id != self.cfg.cluster_id) {
            log.warn("node {d}: rejected inbound from node {d}: cluster mismatch (got {d}, want {d})", .{
                self.cfg.self_id, hs.node_id, hs.cluster_id, self.cfg.cluster_id,
            });
            conn.close();
            return;
        }

        self.getOrCreateSlot(hs.node_id) catch {
            conn.close();
            return;
        };

        sendHandshake(conn, self.cfg.cluster_id, self.cfg.self_id, hs.priority) catch {
            log.warn("node {d}: failed to send reply handshake to node {d}", .{ self.cfg.self_id, hs.node_id });
            conn.close();
            return;
        };

        // Create Mux. Acceptor uses odd stream IDs.
        const on_stream = if (hs.priority == .p0) self.cfg.on_stream_p0 else self.cfg.on_stream_p1;
        const m = Mux.init(self.allocator, conn, 1, on_stream) catch {
            conn.close();
            return;
        };

        const installed = self.installMux(hs.node_id, hs.priority, m);
        if (installed != m) {
            m.deinit();
            return;
        }

        log.info("node {d}: inbound mux established with node {d} pri={s}", .{
            self.cfg.self_id, hs.node_id, @tagName(hs.priority),
        });
        m.run();
        log.info("node {d}: inbound mux disconnected from node {d} pri={s}", .{
            self.cfg.self_id, hs.node_id, @tagName(hs.priority),
        });
        self.removeMux(hs.node_id, hs.priority, m);
    }

    fn dialLoop(self: *Transport, peer_id: u64, priority: Priority) void {
        const pri_idx = @intFromEnum(priority);
        const addr = blk: {
            self.mu.lock() catch return;
            defer self.mu.unlock();
            const slot = self.peers.get(peer_id) orelse return;
            break :blk slot.addr orelse return; // No address — inbound-only peer.
        };

        log.debug("node {d}: starting dial loop for peer {d} pri={s}", .{
            self.cfg.self_id, peer_id, @tagName(priority),
        });

        var backoff_ms: u64 = 250;

        while (true) {
            // Check if already connected.
            const connected = blk: {
                self.mu.lock() catch return;
                defer self.mu.unlock();
                const slot = self.peers.get(peer_id) orelse return;
                break :blk slot.mux[pri_idx] != null;
            };
            if (connected) {
                zio.sleep(zio.Duration.fromMilliseconds(backoff_ms)) catch return;
                continue;
            }

            const conn = addr.connect(.{}) catch {
                zio.sleep(zio.Duration.fromMilliseconds(backoff_ms)) catch return;
                backoff_ms = @min(backoff_ms * 2, 5000);
                continue;
            };
            conn.socket.setNoDelay(true) catch {};

            sendHandshake(conn, self.cfg.cluster_id, self.cfg.self_id, priority) catch {
                conn.close();
                zio.sleep(zio.Duration.fromMilliseconds(backoff_ms)) catch return;
                backoff_ms = @min(backoff_ms * 2, 5000);
                continue;
            };

            const hs = recvHandshake(conn) catch {
                conn.close();
                zio.sleep(zio.Duration.fromMilliseconds(backoff_ms)) catch return;
                backoff_ms = @min(backoff_ms * 2, 5000);
                continue;
            };

            if (hs.cluster_id != self.cfg.cluster_id) {
                log.warn("node {d}: dial to peer {d}: cluster mismatch (got {d})", .{
                    self.cfg.self_id, peer_id, hs.cluster_id,
                });
                conn.close();
                zio.sleep(zio.Duration.fromMilliseconds(backoff_ms)) catch return;
                backoff_ms = @min(backoff_ms * 2, 5000);
                continue;
            }

            // Create Mux. Dialer uses even stream IDs.
            const on_stream = if (priority == .p0) self.cfg.on_stream_p0 else self.cfg.on_stream_p1;
            const m = Mux.init(self.allocator, conn, 0, on_stream) catch {
                conn.close();
                zio.sleep(zio.Duration.fromMilliseconds(backoff_ms)) catch return;
                continue;
            };

            const installed = self.installMux(peer_id, priority, m);
            if (installed != m) {
                m.deinit();
                zio.sleep(zio.Duration.fromMilliseconds(backoff_ms)) catch return;
                continue;
            }

            backoff_ms = 250; // Reset on success.
            log.info("node {d}: outbound mux established with peer {d} pri={s}", .{
                self.cfg.self_id, peer_id, @tagName(priority),
            });
            m.run(); // Blocks until disconnect.
            log.info("node {d}: outbound mux disconnected from peer {d} pri={s}", .{
                self.cfg.self_id, peer_id, @tagName(priority),
            });
            self.removeMux(peer_id, priority, m);
        }
    }

    /// Get or create a PeerSlot for a peer (no address, inbound-only).
    fn getOrCreateSlot(self: *Transport, peer_id: u64) !void {
        self.mu.lock() catch return error.Cancelled;
        defer self.mu.unlock();

        if (self.peers.contains(peer_id)) return;

        const slot = try self.allocator.create(PeerSlot);
        slot.* = .{ .addr = null, .mux = .{ null, null } };
        try self.peers.put(peer_id, slot);
    }

    /// Install a mux into the peer slot. Returns the mux that ended up installed.
    /// If the returned mux != the provided mux, the caller should clean up theirs.
    fn installMux(self: *Transport, peer_id: u64, priority: Priority, m: *Mux) *Mux {
        const pri_idx = @intFromEnum(priority);

        self.mu.lock() catch return m;
        defer self.mu.unlock();

        const slot = self.peers.get(peer_id) orelse return m;

        if (slot.mux[pri_idx]) |existing| {
            // Race: both sides connected simultaneously. Replace with the new one.
            log.debug("node {d}: replacing existing mux for peer {d} pri={d}", .{
                self.cfg.self_id, peer_id, pri_idx,
            });
            existing.deinit();
        }

        slot.mux[pri_idx] = m;
        return m;
    }

    fn removeMux(self: *Transport, peer_id: u64, priority: Priority, m: *Mux) void {
        const pri_idx = @intFromEnum(priority);

        self.mu.lock() catch {
            m.deinit();
            return;
        };
        defer self.mu.unlock();

        const slot = self.peers.get(peer_id) orelse {
            m.deinit();
            return;
        };

        // Only clear slot if it's still the same mux (could have been replaced).
        if (slot.mux[pri_idx] == m) {
            slot.mux[pri_idx] = null;
        }
        m.deinit();
    }

    pub const Handshake = struct {
        cluster_id: u64,
        node_id: u64,
        priority: Priority,
    };

    pub fn sendHandshake(conn: zio.net.Stream, cluster_id: u64, node_id: u64, priority: Priority) !void {
        var buf: [handshake_size]u8 = undefined;
        std.mem.writeInt(u64, buf[0..8], cluster_id, .little);
        std.mem.writeInt(u64, buf[8..16], node_id, .little);
        buf[16] = @intFromEnum(priority);
        conn.writeAll(&buf, .none) catch return error.HandshakeFailed;
    }

    pub fn recvHandshake(conn: zio.net.Stream) !Handshake {
        var buf: [handshake_size]u8 = undefined;
        conn.readAll(&buf, .none) catch return error.HandshakeFailed;
        const pri_byte = buf[16];
        const priority: Priority = switch (pri_byte) {
            0 => .p0,
            1 => .p1,
            else => return error.InvalidHandshake,
        };
        return .{
            .cluster_id = std.mem.readInt(u64, buf[0..8], .little),
            .node_id = std.mem.readInt(u64, buf[8..16], .little),
            .priority = priority,
        };
    }
};
