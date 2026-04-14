const std = @import("std");
const zio = @import("zio");
const raft = @import("raft");
const zdb = @import("zdb");
const runtime = @import("runtime");
const zen = @import("buffers_gen");
const proto = @import("protocol.zig");
const WaitGroup = @import("raft").utils.WaitGroup;

const Forest = zdb.forest.Forest;
const StorageApplier = zdb.applier.StorageApplier;
const Item = zdb.key.Item;
const Key = zdb.key.Key;
const log = std.log.scoped(.zdb_server);

pub const ZdbServer = struct {
    allocator: std.mem.Allocator,
    forest: *Forest,
    applier: *StorageApplier,
    node: *raft.Node,
    listen_addr: zio.net.IpAddress,

    pub fn init(
        allocator: std.mem.Allocator,
        forest: *Forest,
        applier: *StorageApplier,
        node: *raft.Node,
        client_addr: []const u8,
    ) !ZdbServer {
        const addr = try zio.net.IpAddress.parseIpAndPort(client_addr);
        return .{
            .allocator = allocator,
            .forest = forest,
            .applier = applier,
            .node = node,
            .listen_addr = addr,
        };
    }

    pub fn deinit(self: *ZdbServer) void {
        _ = self;
    }

    pub fn runClientLoop(self: *ZdbServer) void {
        log.info("inside runClientLoop", .{});
        defer log.info("client loop exiting (shutdown)", .{});

        const listener = self.listen_addr.listen(.{ .reuse_address = true }) catch |err| {
            log.err("failed to listen on client addr: {}", .{err});
            return;
        };
        defer listener.close();

        log.info("client TCP listener started, accepting connections", .{});

        var group = zio.group();
        defer group.cancel();

        var conn_id: u64 = 0;
        while (!self.node.isShuttingDown()) {
            const stream = listener.accept() catch |err| {
                log.err("runClientLoop: accept failed: {}", .{err});
                return;
            };
            stream.socket.setNoDelay(true) catch {};
            conn_id += 1;
            log.info("accepted client connection #{d}", .{conn_id});
            group.spawn(handleClient, .{ self, stream }) catch {
                log.err("failed to spawn handler for connection #{d}", .{conn_id});
                stream.close();
            };
        }
    }

    pub fn runCompactionLoop(self: *ZdbServer) void {
        log.info("compaction loop started", .{});
        const interval = zio.Duration.fromMilliseconds(500);
        while (!self.node.isShuttingDown()) {
            const did_compact = self.forest.compact() catch false;
            if (did_compact) {
                log.debug("compaction completed a round", .{});
            }
            if (!did_compact) {
                zio.sleep(interval) catch return;
            }
        }
        log.info("compaction loop exiting (shutdown)", .{});
    }

    fn handleClient(self: *ZdbServer, stream: zio.net.Stream) void {
        defer stream.close();
        var ops: u64 = 0;
        var buf: [64 * 1024]u8 = undefined;

        while (!self.node.isShuttingDown()) {
            const cmd = proto.readCommand(stream, &buf) catch |err| {
                if (ops > 0) {
                    log.info("client disconnected after {d} ops: {s}", .{ ops, @errorName(err) });
                } else {
                    log.debug("client error: {s}", .{@errorName(err)});
                }
                // Send error back for parse errors, then continue.
                proto.writeErr(stream, @errorName(err)) catch return;
                continue;
            } orelse {
                log.info("client disconnected after {d} ops (EOF)", .{ops});
                return;
            };
            ops += 1;
            switch (cmd) {
                .set => |req| {
                    log.debug("SET ns={d} key={s} ts={d} value_len={d}", .{
                        req.ns, req.key, req.ts, req.value.len,
                    });
                    self.handleSet(stream, req) catch return;
                },
                .get => |req| {
                    log.debug("GET ns={d} key={s} ts={d}", .{ req.ns, req.key, req.ts });
                    self.handleGet(stream, req) catch return;
                },
                .conf => |req| {
                    log.info("CONF {s} node={d}", .{ @tagName(req.action), req.node_id });
                    self.handleConf(stream, req) catch return;
                },
            }
        }
        log.info("client handler exiting after {d} ops (shutdown)", .{ops});
    }

    fn handleSet(self: *ZdbServer, stream: zio.net.Stream, req: proto.SetCmd) !void {
        // Encode mutation as zbuf for the raft proposal.
        var mutation_writer = try zen.MutationWriter.init(self.allocator);
        defer mutation_writer.deinit();
        try mutation_writer.set(.{
            .namespace = req.ns,
            .key = req.key,
            .value = req.value,
            .timestamp = req.ts,
        });
        var mutation_buf = try mutation_writer.finishWithTail();
        defer mutation_buf.deinit();
        const proposal_bytes = mutation_buf.bytes orelse return error.EncodeFailed;

        // TODO: Fire callback at commit time (quorum achieved) instead of after apply.
        // Currently the client blocks until both commit + apply are done. To fix:
        // 1. Move callback fire before on_apply in applyCommits
        // 2. For reads, track applied_timestamp in StorageApplier and block reads
        //    until applied_timestamp >= read timestamp for linearizability.
        // Set up the commit context with WaitGroup.
        var ctx = CommitCtx{
            .status = .committed,
        };
        ctx.wg.init();
        ctx.wg.add(1);

        self.node.proposeDataChange(proposal_bytes, &onCommit, @ptrCast(&ctx)) catch |err| {
            log.err("propose failed: {s}", .{@errorName(err)});
            try proto.writeErr(stream, "propose failed");
            return;
        };

        // Block until the commit callback fires.
        ctx.wg.wait() catch {
            log.err("wg.wait failed for set", .{});
            try proto.writeErr(stream, "commit wait failed");
            return;
        };

        switch (ctx.status) {
            .committed => {
                log.debug("SET committed", .{});
                try proto.writeOk(stream);
            },
            .not_leader => {
                const leader = self.node.getLeaderId() orelse 0;
                log.info("SET rejected: not leader (leader={d})", .{leader});
                try proto.writeNotLeader(stream, leader);
            },
            else => {
                log.err("SET failed: status={s}", .{@tagName(ctx.status)});
                try proto.writeErr(stream, @tagName(ctx.status));
            },
        }
    }

    fn handleGet(self: *ZdbServer, stream: zio.net.Stream, req: proto.GetCmd) !void {
        const search_key = Key{
            .namespace = req.ns,
            .primary = req.key,
            .timestamp = req.ts,
        };
        const result = self.forest.get(self.allocator, search_key, .p0) catch |err| {
            log.err("forest.get failed: {s}", .{@errorName(err)});
            try proto.writeErr(stream, "get failed");
            return;
        };
        if (result) |item| {
            defer item.free(self.allocator);
            log.debug("GET found: ts={d} value_len={d}", .{ item.key.timestamp, item.value.len });
            try proto.writeFound(stream, item.key.timestamp, item.value);
        } else {
            log.debug("GET miss", .{});
            try proto.writeNotFound(stream);
        }
    }

    fn handleConf(self: *ZdbServer, stream: zio.net.Stream, req: proto.ConfCmd) !void {
        const change_type: raft.ConfChangeType = switch (req.action) {
            .add => .add_learner,
            .to_voter => .to_voter,
            .to_learner => .to_learner,
            .remove => .remove_node,
            .addr => .update_addr,
        };

        // Validate address for commands that require it.
        if (req.action == .add or req.action == .addr) {
            _ = zio.net.IpAddress.parseIpAndPort(req.addr) catch {
                try proto.writeErr(stream, "invalid address");
                return;
            };
        }

        var ctx = CommitCtx{ .status = .committed };
        ctx.wg.init();
        ctx.wg.add(1);

        self.node.proposeConfChange(.{
            .change_type = change_type,
            .node_id = req.node_id,
            .addr = req.addr,
        }, &onCommit, @ptrCast(&ctx)) catch |err| {
            log.err("conf change failed: {s}", .{@errorName(err)});
            try proto.writeErr(stream, @errorName(err));
            return;
        };

        ctx.wg.wait() catch {
            try proto.writeErr(stream, "cancelled");
            return;
        };

        switch (ctx.status) {
            .committed => try proto.writeOk(stream),
            .not_leader => {
                const leader = self.node.getLeaderId() orelse 0;
                try proto.writeNotLeader(stream, leader);
            },
            else => try proto.writeErr(stream, @tagName(ctx.status)),
        }
    }
};

const CommitCtx = struct {
    wg: WaitGroup = .{},
    status: raft.CommitStatus,
};

fn onCommit(ctx_raw: *anyopaque, _entry: raft.ProposalEntry, status: raft.CommitStatus) void {
    _ = _entry;
    const ctx: *CommitCtx = @ptrCast(@alignCast(ctx_raw));
    ctx.status = status;
    ctx.wg.done();
}

/// Called by raft node's applyCommits for each committed normal entry.
/// Decodes the mutation and applies it to the storage engine.
pub fn applyEntryToStorage(ctx_raw: *anyopaque, index: u64, _term: u64, data: []const u8) void {
    _ = _term;
    const applier: *StorageApplier = @ptrCast(@alignCast(ctx_raw));
    const parsed = zen.parseMessageWithTail(data) catch |err| {
        log.err("on_apply: failed to parse entry at index={d}: {s}", .{ index, @errorName(err) });
        return;
    };
    switch (parsed) {
        .Mutation => |mutation| {
            const items = [_]Item{.{
                .key = .{
                    .namespace = mutation.namespace(),
                    .primary = mutation.key(),
                    .timestamp = mutation.timestamp(),
                },
                .value = mutation.value(),
            }};
            _ = applier.applyMutations(index, &items) catch |err| {
                log.err("on_apply: applyMutations failed at index={d}: {s}", .{ index, @errorName(err) });
            };
        },
    }
}
