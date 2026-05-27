const std = @import("std");
const fiber = @import("fiber");

test "Watermark: waitFor returns when value crosses target" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var wm = fiber.Watermark.init(std.testing.allocator);
    defer wm.deinit();

    var observed: u64 = 0;

    const Ctx = struct { wm: *fiber.Watermark, observed: *u64 };
    var ctx: Ctx = .{ .wm = &wm, .observed = &observed };

    const Waiter = struct {
        fn run(c: *Ctx) void {
            c.wm.waitFor(5) catch return;
            c.observed.* = c.wm.load();
        }
    };
    const Bumper = struct {
        fn run(c: *Ctx) void {
            c.wm.advance(5);
        }
    };

    _ = try fiber.spawn(&rt, .{}, Waiter.run, .{&ctx});
    _ = try fiber.spawn(&rt, .{}, Bumper.run, .{&ctx});
    rt.run();

    try std.testing.expect(observed >= 5);
}

test "Watermark: advance wakes only waiters whose target is met" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var wm = fiber.Watermark.init(std.testing.allocator);
    defer wm.deinit();

    var low_ok = false;
    var mid_ok = false;
    var high_ok = false;
    var high_err_closed = false;

    const Ctx = struct {
        wm: *fiber.Watermark,
        low_ok: *bool,
        mid_ok: *bool,
        high_ok: *bool,
        high_err_closed: *bool,
    };
    var ctx: Ctx = .{
        .wm = &wm,
        .low_ok = &low_ok,
        .mid_ok = &mid_ok,
        .high_ok = &high_ok,
        .high_err_closed = &high_err_closed,
    };

    const Low = struct {
        fn run(c: *Ctx) void {
            c.wm.waitFor(3) catch return;
            c.low_ok.* = true;
        }
    };
    const Mid = struct {
        fn run(c: *Ctx) void {
            c.wm.waitFor(5) catch return;
            c.mid_ok.* = true;
        }
    };
    const High = struct {
        fn run(c: *Ctx) void {
            c.wm.waitFor(20) catch |err| {
                if (err == error.Closed) c.high_err_closed.* = true;
                return;
            };
            c.high_ok.* = true;
        }
    };
    const Driver = struct {
        fn run(c: *Ctx) void {
            // Let the waiters park first.
            fiber.yield();
            fiber.yield();
            // Cross 3 and 5 (wake Low + Mid), leave High parked.
            c.wm.advance(10);
            // Now close; High should wake with error.Closed.
            fiber.yield();
            c.wm.close();
        }
    };

    _ = try fiber.spawn(&rt, .{}, Low.run, .{&ctx});
    _ = try fiber.spawn(&rt, .{}, Mid.run, .{&ctx});
    _ = try fiber.spawn(&rt, .{}, High.run, .{&ctx});
    _ = try fiber.spawn(&rt, .{}, Driver.run, .{&ctx});
    rt.run();

    try std.testing.expect(low_ok);
    try std.testing.expect(mid_ok);
    try std.testing.expect(!high_ok);
    try std.testing.expect(high_err_closed);
}

test "Watermark: close wakes everyone with error.Closed" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var wm = fiber.Watermark.init(std.testing.allocator);
    defer wm.deinit();

    var closed_count: std.atomic.Value(u32) = .init(0);

    const Ctx = struct { wm: *fiber.Watermark, count: *std.atomic.Value(u32) };
    var ctx: Ctx = .{ .wm = &wm, .count = &closed_count };

    const Waiter = struct {
        fn run(c: *Ctx) void {
            c.wm.waitFor(100) catch |err| switch (err) {
                error.Closed => _ = c.count.fetchAdd(1, .release),
                else => return,
            };
        }
    };
    const Closer = struct {
        fn run(c: *Ctx) void {
            // Let all waiters park.
            fiber.yield();
            fiber.yield();
            c.wm.close();
        }
    };

    for (0..10) |_| {
        _ = try fiber.spawn(&rt, .{}, Waiter.run, .{&ctx});
    }
    _ = try fiber.spawn(&rt, .{}, Closer.run, .{&ctx});
    rt.run();

    try std.testing.expectEqual(@as(u32, 10), closed_count.load(.acquire));
}

test "Watermark: cross-worker — many waiters across multiple advances" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{ .num_workers = 4 });
    defer rt.deinit();

    var wm = fiber.Watermark.init(std.testing.allocator);
    defer wm.deinit();

    const N_WAITERS = 100;
    var completed: std.atomic.Value(u32) = .init(0);

    const Ctx = struct {
        wm: *fiber.Watermark,
        completed: *std.atomic.Value(u32),
        target: u64,
    };
    var ctxs: [N_WAITERS]Ctx = undefined;

    const Waiter = struct {
        fn run(c: *Ctx) void {
            c.wm.waitFor(c.target) catch return;
            _ = c.completed.fetchAdd(1, .release);
        }
    };

    for (&ctxs, 0..) |*c, i| {
        // Targets spread 1..N_WAITERS, distributed across all 4 workers.
        c.* = .{ .wm = &wm, .completed = &completed, .target = @intCast(i + 1) };
        const wid: u8 = @intCast(i % 4);
        _ = try fiber.spawn(&rt, .{ .worker_id = wid }, Waiter.run, .{c});
    }

    // Driver: bump the watermark in steps so multiple advance
    // events partition the waiters into batches.
    const Driver = struct {
        fn run(w: *fiber.Watermark) void {
            var v: u64 = 0;
            while (v < N_WAITERS) : (v += 10) {
                w.advance(v + 10);
                fiber.yield();
            }
        }
    };
    _ = try fiber.spawn(&rt, .{}, Driver.run, .{&wm});

    rt.run();

    try std.testing.expectEqual(@as(u32, N_WAITERS), completed.load(.acquire));
}
