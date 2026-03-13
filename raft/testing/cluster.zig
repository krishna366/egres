const std = @import("std");
const raft = @import("raft");
pub const Node = raft.Node;
pub const NodeId = raft.NodeId;
pub const Inbound = raft.Inbound;
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
                out.disk = try raft.DiskWAL.openPath(wal_dir, allocator);
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
    tick_count: u64,

    pub const MetaNode = struct {
        node: *Node,
        wal: TestLog,
        inbox: std.ArrayList(Inbound),
        peers: []const raft.Peer,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        ids: []const NodeId,
        learners: []const NodeId,
        config: Config,
    ) !TestCluster {
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
                .inbox = std.ArrayList(Inbound).empty,
                .peers = peers,
            };
            entry.node = try Node.init(
                allocator,
                id,
                peers,
                entry.wal.asRaftWAL(),
                config,
                self_role,
            );

            try nodes.put(id, entry);
        }

        return .{
            .allocator = allocator,
            .nodes = nodes,
            .drop_from = drop_from,
            .drop_to = drop_to,
            .tick_count = 0,
        };
    }

    pub fn deinit(self: *TestCluster) void {
        var it = self.nodes.valueIterator();
        while (it.next()) |entry_ptr| {
            const entry = entry_ptr.*;
            for (entry.inbox.items) |msg| {
                self.allocator.free(msg);
            }
            entry.node.deinit();
            entry.wal.deinit(self.allocator);
            entry.inbox.deinit(self.allocator);
            self.allocator.free(entry.peers);
            self.allocator.destroy(entry);
        }
        self.nodes.deinit();
        self.drop_from.deinit();
        self.drop_to.deinit();
    }

    pub fn tickAll(self: *TestCluster) !void {
        var it = self.nodes.valueIterator();
        while (it.next()) |entry_ptr| {
            try entry_ptr.*.node.tick();
        }
        self.deliverOutbox();
    }

    pub fn deliverOutbox(self: *TestCluster) void {
        var it = self.nodes.valueIterator();
        while (it.next()) |entry_ptr| {
            const entry = entry_ptr.*;
            if (self.drop_from.contains(entry.node.id)) {
                // std.debug.print(
                //     "[raft] cluster tick={d} drop-from node={d} outbox={d}\n",
                //     .{ self.tick_count, entry.node.id, entry.node.takeOutbox().len },
                // );
                entry.node.clearOutbox();
                continue;
            }
            const outbox = entry.node.takeOutbox();
            if (outbox.len == 0) continue;
            for (outbox) |msg| {
                // std.debug.print(
                //     "[raft] cluster tick={d} deliver {d}->{d} kind={s} term={d}\n",
                //     .{ self.tick_count, entry.node.id, msg.to, @tagName(msg.msg), msg.msg.term() },
                // );
                if (self.nodes.get(msg.to)) |dest| {
                    if (self.drop_to.contains(msg.to)) continue;
                    const inbound: Inbound = self.allocator.dupe(u8, msg.payload) catch {
                        @panic("raft: encoded message dup failed");
                    };
                    _ = dest.inbox.append(self.allocator, inbound) catch {
                        self.allocator.free(inbound);
                        @panic("raft: inbox append failed");
                    };
                }
            }
            entry.node.clearOutbox();
        }
    }

    pub fn deliverInbox(self: *TestCluster) !void {
        var it = self.nodes.valueIterator();
        while (it.next()) |entry_ptr| {
            const entry = entry_ptr.*;
            if (entry.inbox.items.len == 0) continue;
            for (entry.inbox.items) |msg| {
                try entry.node.handleInbound(msg);
                self.allocator.free(msg);
            }
            entry.inbox.clearRetainingCapacity();
        }
        self.deliverOutbox();
    }

    pub fn runTicks(self: *TestCluster, ticks: u32) !void {
        var i: u32 = 0;
        while (i < ticks) : (i += 1) {
            self.tick_count += 1;
            // std.debug.print("[raft] cluster tick={d} begin\n", .{self.tick_count});
            try self.tickAll();
            try self.deliverInbox();
        }
    }

    pub fn getNode(self: *TestCluster, id: NodeId) ?*Node {
        const entry = self.nodes.get(id) orelse return null;
        return entry.node;
    }

    pub fn setDropFrom(self: *TestCluster, id: NodeId, drop: bool) void {
        if (drop) {
            _ = self.drop_from.put(id, {}) catch {
                @panic("raft: drop_from insert failed");
            };
        } else {
            _ = self.drop_from.remove(id);
        }
    }

    pub fn setDropTo(self: *TestCluster, id: NodeId, drop: bool) void {
        if (drop) {
            _ = self.drop_to.put(id, {}) catch {
                @panic("raft: drop_to insert failed");
            };
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
