const std = @import("std");
const net = std.net;
const posix = std.posix;
const u = @import("u/utils.zig");

const port: u16 = 8080;
const num_workers = 4;
const queue_capacity = 128;
const max_msg_size = 64 << 20;

const Job = struct {
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

    std.debug.print("listening on :{d} ({d} workers)\n", .{ port, num_workers });

    // Accept loop — the only thing the main thread does is accept and
    // enqueue. It never reads from or writes to a client socket, so a
    // slow client cannot stall new connections.
    while (true) {
        const conn = server.accept() catch |err| {
            std.debug.print("accept error: {}\n", .{err});
            continue;
        };
        conns.push(.{ .conn = conn });
    }
}

// ── Worker pool ──────────────────────────────────────────────────────

fn workerLoop(allocator: std.mem.Allocator) void {
    while (true) {
        const job = conns.pop();
        processJob(allocator, job) catch {};
    }
}

fn processJob(allocator: std.mem.Allocator, job: Job) !void {
    const stream = job.conn.stream; // net.Stream
    defer stream.close();

    // Read 4-byte length prefix (big-endian).
    var buf: [4096]u8 = undefined;
    var reader = stream.reader(&buf);
    const r = reader.interface();

    const msg_len = try r.takeInt(u32, .big);
    const msg = try allocator.alloc(u8, msg_len);
    defer allocator.free(msg);

    try r.readSliceAll(msg);
    for (msg) |*c| {
        if (c.* >= 'a' and c.* <= 'z') c.* -= 32; // Simple make UPPER
    }

    var writer = stream.writer(&buf);
    const w = &writer.interface;
    try w.writeInt(u32, msg_len + 1, .big);
    try w.writeByte(0xaa);
    try w.writeAll(msg);

    // try readExact(stream.handle, &len_buf);
    // const msg_len: usize = std.mem.readInt(u32, &len_buf, .big);
    //
    // if (msg_len == 0 or msg_len > max_msg_size) return;
    //
    // // Allocate a buffer for the full message.
    // const msg = try allocator.alloc(u8, msg_len);
    // defer allocator.free(msg);
    //
    // // Read exactly msg_len bytes — may take multiple read() calls.
    // try readExact(stream.handle, msg);
    //
    // // ── Your processing logic goes here ──
    // // For demonstration: convert request to uppercase.
    // for (msg) |*c| {
    //     if (c.* >= 'a' and c.* <= 'z') c.* -= 32;
    // }
    //
    // // Reply with the same length-prefix framing.
    // var reply_len: [4]u8 = undefined;
    // std.mem.writeInt(u32, &reply_len, @intCast(msg_len), .big);
    // writeAll(stream.handle, &reply_len) catch return;
    // writeAll(stream.handle, msg) catch return;
}

// ── Helpers ──────────────────────────────────────────────────────────

//// Read exactly buf.len bytes, looping over partial reads.
// fn readExact(socket: posix.socket_t, buf: []u8) !void {
//     var filled: usize = 0;
//     while (filled < buf.len) {
//         const n = try posix.read(socket, buf[filled..]);
//         if (n == 0) return error.EndOfStream;
//         filled += n;
//     }
// }
//
// fn writeAll(socket: posix.socket_t, data: []const u8) !void {
//     var pos: usize = 0;
//     while (pos < data.len) {
//         const n = try posix.write(socket, data[pos..]);
//         if (n == 0) return error.ConnectionClosed;
//         pos += n;
//     }
// }
