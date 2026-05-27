const std = @import("std");
const fiber = @import("fiber");

test "io.sleep: 100 fibers sleeping 10ms in parallel finish under 50ms" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var counter: u32 = 0;

    const Body = struct {
        fn run(c: *u32) void {
            fiber.sleep(10);
            c.* += 1;
        }
    };

    for (0..100) |_| {
        _ = try fiber.spawn(&rt, .{}, Body.run, .{&counter});
    }

    const start = rt.ms();
    rt.run();
    const elapsed = rt.ms() - start;

    try std.testing.expectEqual(@as(u32, 100), counter);
    // Concurrency check: 100 × sleep(10) serially would be ~1000 ms;
    // parallel should be one timer-tick. The upper bound is what we
    // actually care about — proves the loop multiplexes the timers.
    try std.testing.expect(elapsed >= 9);
    try std.testing.expect(elapsed < 50);
}

test "io.sleep: single fiber sleeps for the requested duration" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    const Body = struct {
        fn run() void {
            fiber.sleep(20);
        }
    };

    _ = try fiber.spawn(&rt, .{}, Body.run, .{});

    const start = rt.ms();
    rt.run();
    const elapsed = rt.ms() - start;

    try std.testing.expect(elapsed >= 19);
    try std.testing.expect(elapsed < 50);
}

test "io.tcp: echo server with multiple clients" {
    const N = 8;

    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    const loopback: std.Io.net.IpAddress = .{ .ip4 = std.Io.net.Ip4Address.loopback(0) };
    var listener = try fiber.listen(loopback, 64);
    defer listener.close();

    const EchoCtx = struct { stream: fiber.Stream };
    var echo_ctxs: [N]EchoCtx = undefined;

    const ClientCtx = struct { addr: std.Io.net.IpAddress, id: u8, ok: *u32 };
    var oks: [N]u32 = .{0} ** N;
    var client_ctxs: [N]ClientCtx = undefined;

    const AcceptorCtx = struct {
        listener: *fiber.Listener,
        rt: *fiber.Runtime,
        target: usize,
        echo_ctxs: []EchoCtx,
    };
    var acceptor_ctx: AcceptorCtx = .{
        .listener = &listener,
        .rt = &rt,
        .target = N,
        .echo_ctxs = &echo_ctxs,
    };

    const Echo = struct {
        fn run(c: *EchoCtx) void {
            var buf: [16]u8 = undefined;
            const n = c.stream.read(&buf) catch return;
            c.stream.writeAll(buf[0..n]) catch return;
            c.stream.close();
        }
    };

    const Acceptor = struct {
        fn run(c: *AcceptorCtx) void {
            for (0..c.target) |i| {
                c.echo_ctxs[i].stream = c.listener.accept() catch unreachable;
                _ = fiber.spawn(c.rt, .{}, Echo.run, .{&c.echo_ctxs[i]}) catch unreachable;
            }
        }
    };

    const Client = struct {
        fn run(c: *ClientCtx) void {
            var stream = fiber.connect(c.addr) catch return;
            defer stream.close();

            var sent: [16]u8 = .{0} ** 16;
            const msg = std.fmt.bufPrint(&sent, "hello-{d:0>3}", .{c.id}) catch unreachable;
            stream.writeAll(msg) catch return;

            var recv: [16]u8 = undefined;
            const n = stream.read(&recv) catch return;
            if (n == msg.len and std.mem.eql(u8, msg, recv[0..n])) {
                c.ok.* = 1;
            }
        }
    };

    _ = try fiber.spawn(&rt, .{}, Acceptor.run, .{&acceptor_ctx});

    for (&client_ctxs, &oks, 0..) |*c, *ok, i| {
        c.* = .{
            .addr = listener.bound_addr,
            .id = @intCast(i),
            .ok = ok,
        };
        _ = try fiber.spawn(&rt, .{}, Client.run, .{c});
    }

    rt.run();

    for (oks) |ok| try std.testing.expectEqual(@as(u32, 1), ok);
}
