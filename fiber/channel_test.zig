const std = @import("std");
const fiber = @import("fiber");

test "Channel: send + receive round-trip" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var ch = try fiber.Channel(u32).initAlloc(std.testing.allocator, 4);
    defer ch.deinit();

    var received: [4]u32 = .{ 0, 0, 0, 0 };

    const Ctx = struct { ch: *fiber.Channel(u32), received: *[4]u32 };
    var ctx: Ctx = .{ .ch = &ch, .received = &received };

    const Body = struct {
        fn run(c: *Ctx) void {
            c.ch.send(10) catch unreachable;
            c.ch.send(20) catch unreachable;
            c.ch.send(30) catch unreachable;
            c.ch.send(40) catch unreachable;
            c.received[0] = c.ch.receive() catch unreachable;
            c.received[1] = c.ch.receive() catch unreachable;
            c.received[2] = c.ch.receive() catch unreachable;
            c.received[3] = c.ch.receive() catch unreachable;
        }
    };

    _ = try fiber.spawn(&rt, .{}, Body.run, .{&ctx});
    rt.run();

    try std.testing.expectEqualSlices(u32, &.{ 10, 20, 30, 40 }, &received);
}

test "Channel: bounded blocking handoff with capacity 1" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var ch = try fiber.Channel(u32).initAlloc(std.testing.allocator, 1);
    defer ch.deinit();

    var total: u32 = 0;

    const Ctx = struct { ch: *fiber.Channel(u32), total: *u32 };
    var ctx: Ctx = .{ .ch = &ch, .total = &total };

    const Producer = struct {
        fn run(c: *Ctx) void {
            // 100 items through a capacity-1 channel — every send
            // after the first blocks until the receiver drains.
            var i: u32 = 1;
            while (i <= 100) : (i += 1) c.ch.send(i) catch return;
            c.ch.close(.graceful);
        }
    };
    const Consumer = struct {
        fn run(c: *Ctx) void {
            while (true) {
                const v = c.ch.receive() catch return;
                c.total.* += v;
            }
        }
    };

    _ = try fiber.spawn(&rt, .{}, Producer.run, .{&ctx});
    _ = try fiber.spawn(&rt, .{}, Consumer.run, .{&ctx});
    rt.run();

    try std.testing.expectEqual(@as(u32, 100 * 101 / 2), total);
}

test "Channel: trySend on full returns ChannelFull" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var ch = try fiber.Channel(u32).initAlloc(std.testing.allocator, 2);
    defer ch.deinit();

    var saw_full = false;

    const Ctx = struct { ch: *fiber.Channel(u32), saw_full: *bool };
    var ctx: Ctx = .{ .ch = &ch, .saw_full = &saw_full };

    const Body = struct {
        fn run(c: *Ctx) void {
            c.ch.trySend(1) catch unreachable;
            c.ch.trySend(2) catch unreachable;
            c.ch.trySend(3) catch |err| switch (err) {
                error.ChannelFull => {
                    c.saw_full.* = true;
                    return;
                },
                else => unreachable,
            };
        }
    };

    _ = try fiber.spawn(&rt, .{}, Body.run, .{&ctx});
    rt.run();

    try std.testing.expect(saw_full);
}

test "Channel: tryReceive on empty returns ChannelEmpty; on closed-empty returns ChannelClosed" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var ch = try fiber.Channel(u32).initAlloc(std.testing.allocator, 2);
    defer ch.deinit();

    var saw_empty = false;
    var saw_closed = false;

    const Ctx = struct {
        ch: *fiber.Channel(u32),
        saw_empty: *bool,
        saw_closed: *bool,
    };
    var ctx: Ctx = .{
        .ch = &ch,
        .saw_empty = &saw_empty,
        .saw_closed = &saw_closed,
    };

    const Body = struct {
        fn run(c: *Ctx) void {
            _ = c.ch.tryReceive() catch |err| switch (err) {
                error.ChannelEmpty => c.saw_empty.* = true,
                error.ChannelClosed => unreachable,
            };
            c.ch.close(.immediate);
            _ = c.ch.tryReceive() catch |err| switch (err) {
                error.ChannelClosed => c.saw_closed.* = true,
                error.ChannelEmpty => unreachable,
            };
        }
    };

    _ = try fiber.spawn(&rt, .{}, Body.run, .{&ctx});
    rt.run();

    try std.testing.expect(saw_empty);
    try std.testing.expect(saw_closed);
}

test "Channel: graceful close lets receivers drain buffered items" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var ch = try fiber.Channel(u32).initAlloc(std.testing.allocator, 4);
    defer ch.deinit();

    var drained: [3]u32 = .{ 0, 0, 0 };
    var saw_closed = false;

    const Ctx = struct {
        ch: *fiber.Channel(u32),
        drained: *[3]u32,
        saw_closed: *bool,
    };
    var ctx: Ctx = .{ .ch = &ch, .drained = &drained, .saw_closed = &saw_closed };

    const Body = struct {
        fn run(c: *Ctx) void {
            c.ch.send(7) catch unreachable;
            c.ch.send(8) catch unreachable;
            c.ch.send(9) catch unreachable;
            c.ch.close(.graceful);
            c.drained[0] = c.ch.receive() catch unreachable;
            c.drained[1] = c.ch.receive() catch unreachable;
            c.drained[2] = c.ch.receive() catch unreachable;
            _ = c.ch.receive() catch |err| switch (err) {
                error.ChannelClosed => c.saw_closed.* = true,
            };
        }
    };

    _ = try fiber.spawn(&rt, .{}, Body.run, .{&ctx});
    rt.run();

    try std.testing.expectEqualSlices(u32, &.{ 7, 8, 9 }, &drained);
    try std.testing.expect(saw_closed);
}

test "Channel: immediate close drops buffered items" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var ch = try fiber.Channel(u32).initAlloc(std.testing.allocator, 4);
    defer ch.deinit();

    var saw_closed = false;

    const Ctx = struct { ch: *fiber.Channel(u32), saw_closed: *bool };
    var ctx: Ctx = .{ .ch = &ch, .saw_closed = &saw_closed };

    const Body = struct {
        fn run(c: *Ctx) void {
            c.ch.send(1) catch unreachable;
            c.ch.send(2) catch unreachable;
            c.ch.close(.immediate);
            _ = c.ch.receive() catch |err| switch (err) {
                error.ChannelClosed => c.saw_closed.* = true,
            };
        }
    };

    _ = try fiber.spawn(&rt, .{}, Body.run, .{&ctx});
    rt.run();

    try std.testing.expect(saw_closed);
}

test "Channel: cross-worker MPMC stress (4 producers x 4 consumers x cap 8)" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{ .num_workers = 4 });
    defer rt.deinit();

    var ch = try fiber.Channel(u32).initAlloc(std.testing.allocator, 8);
    defer ch.deinit();

    const N_PRODUCERS = 4;
    const N_CONSUMERS = 4;
    const ITEMS_PER_PRODUCER: u32 = 2000;
    const TOTAL: u64 = N_PRODUCERS * ITEMS_PER_PRODUCER;

    var produced_total: std.atomic.Value(u64) = .init(0);
    var received_total: std.atomic.Value(u64) = .init(0);
    var producers_done: std.atomic.Value(u32) = .init(0);

    const ProdCtx = struct {
        ch: *fiber.Channel(u32),
        produced_total: *std.atomic.Value(u64),
        producers_done: *std.atomic.Value(u32),
        n_producers: u32,
        items: u32,
    };
    var prod_ctx: ProdCtx = .{
        .ch = &ch,
        .produced_total = &produced_total,
        .producers_done = &producers_done,
        .n_producers = N_PRODUCERS,
        .items = ITEMS_PER_PRODUCER,
    };

    const ConsCtx = struct {
        ch: *fiber.Channel(u32),
        received_total: *std.atomic.Value(u64),
    };
    var cons_ctx: ConsCtx = .{ .ch = &ch, .received_total = &received_total };

    const Producer = struct {
        fn run(c: *ProdCtx) void {
            var i: u32 = 0;
            while (i < c.items) : (i += 1) {
                c.ch.send(i) catch return;
                _ = c.produced_total.fetchAdd(1, .release);
            }
            const done = c.producers_done.fetchAdd(1, .acq_rel) + 1;
            if (done == c.n_producers) c.ch.close(.graceful);
        }
    };
    const Consumer = struct {
        fn run(c: *ConsCtx) void {
            while (true) {
                _ = c.ch.receive() catch return;
                _ = c.received_total.fetchAdd(1, .release);
            }
        }
    };

    for (0..N_PRODUCERS) |i| {
        const wid: u8 = @intCast(i % 4);
        _ = try fiber.spawn(&rt, .{ .worker_id = wid }, Producer.run, .{&prod_ctx});
    }
    for (0..N_CONSUMERS) |i| {
        const wid: u8 = @intCast(i % 4);
        _ = try fiber.spawn(&rt, .{ .worker_id = wid }, Consumer.run, .{&cons_ctx});
    }

    rt.run();

    try std.testing.expectEqual(TOTAL, produced_total.load(.acquire));
    try std.testing.expectEqual(TOTAL, received_total.load(.acquire));
}
