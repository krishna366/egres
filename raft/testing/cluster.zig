const std = @import("std");
const zio = @import("zio");
const raft = @import("raft");
pub const Node = raft.Node;
pub const NodeId = raft.NodeId;
pub const Config = raft.Config;

pub const TestWalBackend = enum {
    mem,
    disk,
};

pub const test_wal_backend: TestWalBackend = .disk;

pub const TestLog = struct {
    mem: ?raft.MemWAL = null,
    disk: ?raft.DiskWAL = null,
    wal_dir: ?[]u8 = null,

    pub fn init(allocator: std.mem.Allocator, id: NodeId) !TestLog {
        var out: TestLog = .{};
        switch (test_wal_backend) {
            .mem => out.mem = raft.MemWAL.init(allocator),
            .disk => {
                const wal_dir = try makeWalDir(allocator, id);
                out.wal_dir = wal_dir;
                const vfs = @import("vfs");
                out.disk = try raft.DiskWAL.openPath(wal_dir, allocator, &vfs.VFS.init, 1024);
            },
        }
        return out;
    }

    pub fn deinit(self: *TestLog, allocator: std.mem.Allocator) void {
        if (self.mem) |*log| log.deinit();
        if (self.disk) |*storage| storage.deinit();
        if (self.wal_dir) |wal_dir| {
            std.fs.cwd().deleteTree(wal_dir) catch {};
            allocator.free(wal_dir);
        }
    }

    pub fn asRaftWAL(self: *TestLog) raft.RaftWAL {
        return switch (test_wal_backend) {
            .mem => self.mem.?.asRaftWAL(),
            .disk => self.disk.?.asRaftWAL(),
        };
    }

    fn makeWalDir(allocator: std.mem.Allocator, id: NodeId) ![]u8 {
        var buf: [192]u8 = undefined;
        const ts = std.time.nanoTimestamp();
        const path = try std.fmt.bufPrint(&buf, "/tmp/egres-test-cluster-{d}-{d}", .{ ts, id });
        try std.fs.cwd().makePath(path);
        return allocator.dupe(u8, path);
    }
};

pub const TestCluster = struct {
    allocator: std.mem.Allocator,
    nodes: std.AutoHashMap(NodeId, *MetaNode),
    drop_from: std.AutoHashMap(NodeId, void),
    drop_to: std.AutoHashMap(NodeId, void),

    pub const MetaNode = struct {
        node: *Node,
        wal: TestLog,
        peers: []const raft.Peer,
        group: zio.Group,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        ids: []const NodeId,
        learners: []const NodeId,
        config: Config,
    ) !*TestCluster {
        var nodes = std.AutoHashMap(NodeId, *MetaNode).init(allocator);
        const drop_from = std.AutoHashMap(NodeId, void).init(allocator);
        const drop_to = std.AutoHashMap(NodeId, void).init(allocator);
        try nodes.ensureTotalCapacity(@intCast(ids.len));

        for (ids) |id| {
            const peers = try buildPeers(allocator, ids, learners, id);
            const self_role: raft.PeerRole = if (containsNode(learners, id)) .learner else .voter;

            var entry = try allocator.create(MetaNode);
            entry.* = .{
                .node = undefined,
                .wal = try TestLog.init(allocator, id),
                .peers = peers,
                .group = .init,
            };
            var ncfg = config;
            ncfg.id = id;
            ncfg.tick_interval_ms = 1; // Fast ticks for tests.
            entry.node = try Node.init(
                allocator,
                peers,
                entry.wal.asRaftWAL(),
                ncfg,
                self_role,
            );

            try nodes.put(id, entry);
        }

        const cluster = try allocator.create(TestCluster);
        cluster.* = .{
            .allocator = allocator,
            .nodes = nodes,
            .drop_from = drop_from,
            .drop_to = drop_to,
        };

        // Register on_send and start coroutines for each node.
        var node_it = cluster.nodes.valueIterator();
        while (node_it.next()) |entry_ptr| {
            const entry = entry_ptr.*;
            entry.node.on_send = &testSend;
            entry.node.on_send_ctx = @ptrCast(cluster);
            entry.group.spawn(Node.tickLoop, .{ entry.node, entry.node.config.tick_interval_ms }) catch {};
            entry.group.spawn(Node.mainLoop, .{ entry.node, entry.node.config.tick_interval_ms }) catch {};
            entry.group.spawn(Node.applyLoop, .{entry.node}) catch {};
        }
        return cluster;
    }

    fn testSend(ctx: *anyopaque, from: NodeId, to: NodeId, payload: []const u8) void {
        const self: *TestCluster = @ptrCast(@alignCast(ctx));
        if (self.drop_from.contains(from)) return;
        if (self.drop_to.contains(to)) return;
        const dest = self.nodes.get(to) orelse return;
        const owned = self.allocator.dupe(u8, payload) catch return;
        dest.node.inbound_ch.send(.{
            .message_type = raft.msg_type_raft,
            .payload = owned,
        }) catch {
            self.allocator.free(owned);
            return;
        };
        dest.node.event_ch.trySend(1) catch {};
    }

    pub fn deinit(self: *TestCluster) void {
        // Shut down all nodes — this makes mainLoop/tickLoop/applyLoop exit.
        var shutdown_it = self.nodes.valueIterator();
        while (shutdown_it.next()) |entry_ptr| {
            entry_ptr.*.node.startShutdown();
        }
        // Wait for ALL coroutines to finish before any cleanup.
        var wait_it = self.nodes.valueIterator();
        while (wait_it.next()) |entry_ptr| {
            entry_ptr.*.group.wait() catch {};
        }
        // Now safe to clean up — no coroutines are running.
        var it = self.nodes.valueIterator();
        while (it.next()) |entry_ptr| {
            const entry = entry_ptr.*;
            entry.node.deinit();
            entry.wal.deinit(self.allocator);
            self.allocator.free(entry.peers);
            self.allocator.destroy(entry);
        }
        self.nodes.deinit();
        self.drop_from.deinit();
        self.drop_to.deinit();
        self.allocator.destroy(self);
    }

    /// Wait until every node has advanced by at least `n` ticks.
    pub fn waitForTicks(self: *TestCluster, n: u64) void {
        // Record starting tick counts.
        var starts: [16]u64 = undefined;
        var count: usize = 0;
        var it = self.nodes.valueIterator();
        while (it.next()) |entry_ptr| {
            starts[count] = entry_ptr.*.node.tick_count;
            count += 1;
        }

        // Poll until all nodes have advanced by n ticks.
        while (true) {
            var all_done = true;
            var i: usize = 0;
            var it2 = self.nodes.valueIterator();
            while (it2.next()) |entry_ptr| {
                if (entry_ptr.*.node.tick_count < starts[i] + n) {
                    all_done = false;
                    break;
                }
                i += 1;
            }
            if (all_done) return;
            zio.sleep(zio.Duration.fromMilliseconds(1)) catch return;
        }
    }

    pub fn getNode(self: *TestCluster, id: NodeId) ?*Node {
        const entry = self.nodes.get(id) orelse return null;
        return entry.node;
    }

    pub fn setDropFrom(self: *TestCluster, id: NodeId, drop: bool) void {
        if (drop) {
            _ = self.drop_from.put(id, {}) catch {};
        } else {
            _ = self.drop_from.remove(id);
        }
    }

    pub fn setDropTo(self: *TestCluster, id: NodeId, drop: bool) void {
        if (drop) {
            _ = self.drop_to.put(id, {}) catch {};
        } else {
            _ = self.drop_to.remove(id);
        }
    }

    fn buildPeers(
        allocator: std.mem.Allocator,
        ids: []const NodeId,
        learners: []const NodeId,
        self_id: NodeId,
    ) ![]const raft.Peer {
        var peers = std.ArrayList(raft.Peer).empty;
        for (ids) |id| {
            if (id == self_id) continue;
            const role: raft.PeerRole = if (containsNode(learners, id)) .learner else .voter;
            try peers.append(allocator, .{
                .id = id,
                .next_index = 1,
                .match_index = 0,
                .role = role,
            });
        }
        return peers.toOwnedSlice(allocator);
    }

    fn containsNode(list: []const NodeId, id: NodeId) bool {
        for (list) |item| if (item == id) return true;
        return false;
    }
};
