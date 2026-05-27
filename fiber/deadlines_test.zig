const std = @import("std");
const fiber = @import("fiber");

test "Deadlines: timer fires after the registered duration" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    const Ctx = struct {
        fired: bool = false,
        waiter: ?*fiber.Fiber = null,
        fire_at_ms: i64 = 0,
    };
    var ctx: Ctx = .{};

    const Cb = struct {
        fn onFire(opaque_ctx: *anyopaque) void {
            const c: *Ctx = @ptrCast(@alignCast(opaque_ctx));
            c.fired = true;
            c.fire_at_ms = fiber.currentWorker().rt.ms();
            if (c.waiter) |f| fiber.wake(f);
        }
    };

    const Body = struct {
        fn run(c: *Ctx) void {
            const me = fiber.currentFiber();
            c.waiter = me;

            const w = fiber.currentWorker();
            const before = w.rt.ms();

            // Timeout lives on this fiber's stack; valid until run()
            // returns. We park before that, and only return after the
            // callback wakes us — so the heap never sees a dangling
            // pointer.
            var t: fiber.Timeout = .{
                .deadline_ms = undefined,
                .fiber = me,
                .callback = &Cb.onFire,
                .callback_ctx = c,
            };
            w.deadlines.register(std.testing.allocator, &t, before, 20) catch unreachable;
            fiber.park();

            const elapsed = c.fire_at_ms - before;
            std.testing.expect(c.fired) catch unreachable;
            std.testing.expect(elapsed >= 18) catch unreachable;
            std.testing.expect(elapsed < 60) catch unreachable;
        }
    };

    _ = try fiber.spawn(&rt, .{}, Body.run, .{&ctx});
    rt.run();

    try std.testing.expect(ctx.fired);
}

test "Deadlines: cancel(t) before fire prevents the callback" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    const Ctx = struct {
        fired_canceled: bool = false,
        fired_wake: bool = false,
        waiter: ?*fiber.Fiber = null,
    };
    var ctx: Ctx = .{};

    const Cb = struct {
        fn onCanceled(opaque_ctx: *anyopaque) void {
            const c: *Ctx = @ptrCast(@alignCast(opaque_ctx));
            c.fired_canceled = true;
        }
        fn onWake(opaque_ctx: *anyopaque) void {
            const c: *Ctx = @ptrCast(@alignCast(opaque_ctx));
            c.fired_wake = true;
            if (c.waiter) |f| fiber.wake(f);
        }
    };

    const Body = struct {
        fn run(c: *Ctx) void {
            const me = fiber.currentFiber();
            c.waiter = me;

            const w = fiber.currentWorker();
            const now = w.rt.ms();

            var t_canceled: fiber.Timeout = .{
                .deadline_ms = undefined,
                .fiber = me,
                .callback = &Cb.onCanceled,
                .callback_ctx = c,
            };
            var t_wake: fiber.Timeout = .{
                .deadline_ms = undefined,
                .fiber = me,
                .callback = &Cb.onWake,
                .callback_ctx = c,
            };

            // Register the to-be-canceled one first (earlier — would
            // be the heap head + the in-flight timer's target if not
            // removed). Cancel it, then register a later deadline we
            // actually want to wait on.
            w.deadlines.register(std.testing.allocator, &t_canceled, now, 5) catch unreachable;
            w.deadlines.cancel(&t_canceled);
            w.deadlines.register(std.testing.allocator, &t_wake, now, 30) catch unreachable;

            fiber.park();

            std.testing.expect(c.fired_wake) catch unreachable;
            std.testing.expect(!c.fired_canceled) catch unreachable;
        }
    };

    _ = try fiber.spawn(&rt, .{}, Body.run, .{&ctx});
    rt.run();

    try std.testing.expect(ctx.fired_wake);
    try std.testing.expect(!ctx.fired_canceled);
}

test "Deadlines: re-arm to earlier deadline registered after a later one" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    // Two callbacks: one for the 10 ms deadline, one for the 40 ms
    // deadline. We register the 40 ms one FIRST, so the timer starts
    // armed for 40 ms; then the 10 ms `register` must reset it to fire
    // at ~10 ms. If the re-arm didn't happen, both callbacks would
    // fire together when the 40 ms timer expires — the wall-time
    // assertion catches that bug.
    const Ctx = struct {
        short_fire_at: i64 = 0,
        long_fire_at: i64 = 0,
        fired_count: u32 = 0,
        waiter: ?*fiber.Fiber = null,
    };
    var ctx: Ctx = .{};

    const Cb = struct {
        fn onShort(opaque_ctx: *anyopaque) void {
            const c: *Ctx = @ptrCast(@alignCast(opaque_ctx));
            c.short_fire_at = fiber.currentWorker().rt.ms();
            c.fired_count += 1;
            if (c.fired_count == 2) if (c.waiter) |f| fiber.wake(f);
        }
        fn onLong(opaque_ctx: *anyopaque) void {
            const c: *Ctx = @ptrCast(@alignCast(opaque_ctx));
            c.long_fire_at = fiber.currentWorker().rt.ms();
            c.fired_count += 1;
            if (c.fired_count == 2) if (c.waiter) |f| fiber.wake(f);
        }
    };

    const Body = struct {
        fn run(c: *Ctx) void {
            const me = fiber.currentFiber();
            c.waiter = me;

            const w = fiber.currentWorker();
            const now = w.rt.ms();

            var t_long: fiber.Timeout = .{
                .deadline_ms = undefined,
                .fiber = me,
                .callback = &Cb.onLong,
                .callback_ctx = c,
            };
            var t_short: fiber.Timeout = .{
                .deadline_ms = undefined,
                .fiber = me,
                .callback = &Cb.onShort,
                .callback_ctx = c,
            };

            // 40 ms first — arms timer for 40 ms.
            w.deadlines.register(std.testing.allocator, &t_long, now, 40) catch unreachable;
            // 10 ms second — must reset timer to 10 ms.
            w.deadlines.register(std.testing.allocator, &t_short, now, 10) catch unreachable;

            fiber.park();

            const short_elapsed = c.short_fire_at - now;
            const long_elapsed = c.long_fire_at - now;
            // Short fires near 10 ms — if the reset didn't happen,
            // this would be near 40 ms instead.
            std.testing.expect(short_elapsed >= 8) catch unreachable;
            std.testing.expect(short_elapsed < 30) catch unreachable;
            // Long fires near 40 ms — re-armed by onTimer's tail
            // after the short one drained.
            std.testing.expect(long_elapsed >= 35) catch unreachable;
            std.testing.expect(long_elapsed < 80) catch unreachable;
        }
    };

    _ = try fiber.spawn(&rt, .{}, Body.run, .{&ctx});
    rt.run();

    try std.testing.expectEqual(@as(u32, 2), ctx.fired_count);
}
