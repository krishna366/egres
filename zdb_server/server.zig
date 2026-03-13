const std = @import("std");
const zio = @import("zio");
const raft = @import("raft");
const zdb = @import("zdb");
const runtime = @import("runtime");
const zen = @import("buffers_gen");
const proto = @import("protocol.zig");
const WaitGroup = @import("wait_group").WaitGroup;

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

    /// Spawn the client TCP accept loop on the query runtime pool.
    /// Must be called from a zio coroutine. Blocks until cancelled.
    pub fn runOnQueryPool(self: *ZdbServer) !void {
        var handle = try runtime.pool().get(.query).spawn(runClientLoop, .{self});
        handle.join();
    }

    /// Spawn the compaction loop on the background runtime pool.
    /// Must be called from a zio coroutine. Blocks until cancelled.
    pub fn runCompactionOnBackgroundPool(self: *ZdbServer) !void {
        var handle = try runtime.pool().get(.background).spawn(runCompactionLoop, .{self});
        handle.join();
    }

    pub fn deinit(self: *ZdbServer) void {
        _ = self;
    }

    fn runClientLoop(self: *ZdbServer) void {
        log.info("inside runClientLoop", .{});
        const listener = self.listen_addr.listen(.{ .reuse_address = true }) catch |err| {
            log.err("failed to listen on client addr: {}", .{err});
            return;
        };
        defer listener.close();

        log.info("client TCP listener started, accepting connections", .{});

        var group: zio.Group = .init;
        defer group.cancel();

        var conn_id: u64 = 0;
        while (!self.node.isShuttingDown()) {
            const stream = listener.accept() catch |err| {
                log.err("accept failed: {}", .{err});
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
        log.info("client loop exiting (shutdown)", .{});
    }

    fn runCompactionLoop(self: *ZdbServer) void {
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

        // Set up the commit context with WaitGroup.
        var ctx = CommitCtx{
            .applier = self.applier,
            .status = .committed,
            .ns = req.ns,
            .key = req.key,
            .value = req.value,
            .ts = req.ts,
        };
        ctx.wg.init();
        ctx.wg.add(1);

        self.node.proposeWithCallback(proposal_bytes, &onCommit, @ptrCast(&ctx)) catch |err| {
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
};

const CommitCtx = struct {
    wg: WaitGroup = .{},
    applier: *StorageApplier,
    status: raft.CommitStatus,
    ns: u32,
    key: []const u8,
    value: []const u8,
    ts: u64,
};

fn onCommit(ctx_raw: *anyopaque, entry: raft.ProposalEntry, status: raft.CommitStatus) void {
    const ctx: *CommitCtx = @ptrCast(@alignCast(ctx_raw));
    ctx.status = status;
    if (status == .committed) {
        const items = [_]Item{.{
            .key = .{
                .namespace = ctx.ns,
                .primary = ctx.key,
                .timestamp = ctx.ts,
            },
            .value = ctx.value,
        }};
        _ = ctx.applier.applyMutations(entry.index, &items) catch |err| {
            log.err("applyMutations failed at index={d}: {s}", .{ entry.index, @errorName(err) });
        };
        log.debug("onCommit: applied index={d}", .{entry.index});
    } else {
        log.debug("onCommit: status={s} index={d}", .{ @tagName(status), entry.index });
    }
    ctx.wg.done();
}
