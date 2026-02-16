const std = @import("std");
const node_mod = @import("node.zig");
const transport_mod = @import("transport.zig");

pub const Error = transport_mod.Error || std.Thread.SpawnError;

pub const RaftRuntime = struct {
    allocator: std.mem.Allocator,
    node: *node_mod.Node,
    transport: transport_mod.RaftTransport,
    run_thread: ?std.Thread = null,
    callback_thread: ?std.Thread = null,

    const callback_batch_size: usize = 256;

    pub fn init(
        allocator: std.mem.Allocator,
        node: *node_mod.Node,
        transport_cfg: transport_mod.Config,
    ) Error!RaftRuntime {
        var out = RaftRuntime{
            .allocator = allocator,
            .node = node,
            .transport = undefined,
        };
        out.transport = try transport_mod.RaftTransport.init(allocator, out.node, transport_cfg);
        return out;
    }

    pub fn deinit(self: *RaftRuntime) void {
        self.stop();
        self.join();
        self.transport.deinit();
    }

    pub fn runOnce(self: *RaftRuntime) Error!void {
        try self.transport.runOnce();
        _ = self.node.processCommitCallbacks(callback_batch_size);
    }

    pub fn run(self: *RaftRuntime) Error!void {
        while (!self.node.isShuttingDown()) {
            try self.runOnce();
        }
    }

    pub fn start(self: *RaftRuntime) Error!void {
        if (self.run_thread != null or self.callback_thread != null) return;
        self.run_thread = try std.Thread.spawn(.{}, runTransportMain, .{self});
        errdefer {
            self.stop();
            self.join();
        }
        self.callback_thread = try std.Thread.spawn(.{}, callbackMain, .{self});
    }

    pub fn stop(self: *RaftRuntime) void {
        self.node.startShutdown();
    }

    pub fn join(self: *RaftRuntime) void {
        if (self.run_thread) |thread| {
            thread.join();
            self.run_thread = null;
        }
        if (self.callback_thread) |thread| {
            thread.join();
            self.callback_thread = null;
        }
    }

    fn runTransportMain(self: *RaftRuntime) void {
        while (!self.node.isShuttingDown()) {
            self.transport.runOnce() catch |err| {
                std.log.err("raft runtime stopped: {s}", .{@errorName(err)});
                self.node.startShutdown();
                break;
            };
        }
    }

    fn callbackMain(self: *RaftRuntime) void {
        while (!self.node.isShuttingDown()) {
            _ = self.node.processCommitCallbacks(callback_batch_size);
            self.node.waitForCommitCallbacks();
        }
        // We don't need to processCommitCallbacks here, because after
        // shutdown node.deinit would call them.
    }

    fn runMain(self: *RaftRuntime) void {
        self.run() catch |err| {
            std.log.err("raft runtime stopped: {s}", .{@errorName(err)});
        };
    }
};
