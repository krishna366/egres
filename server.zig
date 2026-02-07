const std = @import("std");
const net = std.net;
const posix = std.posix;
const u = @import("u/utils.zig");

const port: u16 = 8080;
const num_workers = 4;
const queue_capacity = 128;
const max_msg_size = 64 << 20;
var nextConnId = std.atomic.Value(u64).init(0);

const Job = struct {
    connId: u64,
    conn: net.Server.Connection,
};

var conns: u.Queue(Job, queue_capacity) = .{};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create a thread pool which can pull connection requests off conns.
    for (0..num_workers) |_| {
        const t = try std.Thread.spawn(.{}, workerLoop, .{allocator});
        t.detach();
    }

    const addr = try net.Address.parseIp("0.0.0.0", port);
    var server = try addr.listen(.{ .reuse_address = true }); // Gives us net.Server
    defer server.deinit();

    std.log.debug("listening on :{d} ({d} workers)\n", .{ port, num_workers });

    // Accept loop — the only thing the main thread does is accept and
    // enqueue. It never reads from or writes to a client socket, so a
    // slow client cannot stall new connections.
    while (true) {
        const conn = server.accept() catch |err| {
            std.log.debug("accept error: {}\n", .{err});
            continue;
        };
        conns.push(.{
            .connId = nextConnId.fetchAdd(1, std.builtin.AtomicOrder.monotonic),
            .conn = conn,
        });
    }
}

fn workerLoop(allocator: std.mem.Allocator) void {
    while (true) {
        const job = conns.pop();
        processJob(allocator, job) catch |err| {
            std.log.debug("Got error while processing job: {}", .{err});
        };
    }
}

// TODO: Switch this an epoll based design.
fn processJob(allocator: std.mem.Allocator, job: Job) !void {
    std.log.debug("Got a job to process {d}\n", .{job.connId});
    const stream = job.conn.stream; // net.Stream
    defer stream.close();

    // Read 4-byte length prefix (big-endian).
    var buf: [4096]u8 = undefined;
    var reader = stream.reader(&buf);
    const r = reader.interface();

    const msg_len = try r.takeInt(u32, .big);
    std.log.debug("Got message of length: {d}\n", .{msg_len});
    const msg = try allocator.alloc(u8, msg_len);
    defer allocator.free(msg);

    try r.readSliceAll(msg);
    std.log.debug("Got message: {s}\n", .{msg});
    for (msg) |*c| {
        if (c.* >= 'a' and c.* <= 'z') c.* -= 32; // Simple make UPPER
    }

    var writer = stream.writer(&buf);
    const w = &writer.interface;
    defer w.flush() catch {};

    try w.writeInt(u32, msg_len, .big);
    try w.writeAll(msg);
    std.log.debug("sent the message back: {s}\n", .{msg});
}
