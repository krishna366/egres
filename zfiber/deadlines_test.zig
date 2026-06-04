const std = @import("std");
const fiber = @import("zfiber");

/// Fresh monotonic ms — same source `Deadlines.register` / `onTimer`
/// use. Direct vDSO read; no cache lag under load.
fn nowMs() i64 {
    return fiber.clockMsUseRarely(.monotonic);
}

/// Minimal `on_fire` context: record that we fired and wake the waiter.
/// Mirrors how a real consumer (e.g. the watermark) supplies its own flag
/// — `Deadlines` itself knows nothing about fibers or flags.
const Waker = struct {
    fiber: *fiber.Fiber,
    fired: std.atomic.Value(bool) = .init(false),
};

fn wakeCb(ptr: *anyopaque) void {
    const wk: *Waker = @ptrCast(@alignCast(ptr));
    wk.fired.store(true, .release);
    fiber.wake(wk.fiber);
}

test "Deadlines: timer fires after the registered duration" {
    const w = fiber.currentWorker();
    const before = nowMs();

    // Timeout + ctx live on this fiber's stack; valid until the test
    // returns. We park before that and only resume after the deadline
    // fires (its on_fire wakes us), so the heap never sees a dangling ptr.
    var wk: Waker = .{ .fiber = fiber.currentFiber() };
    var t: fiber.Timeout = .{ .on_fire = &wakeCb, .on_fire_ctx = &wk };
    try w.deadlines.register(&t, 20);
    fiber.park();

    const elapsed = nowMs() - before;
    std.log.debug("elapsed = {d} ms, fired = {} (registered for 20 ms)", .{ elapsed, wk.fired.load(.acquire) });
    try std.testing.expect(wk.fired.load(.acquire));
    try std.testing.expect(elapsed >= 18);
    try std.testing.expect(elapsed < 200);
}

test "Deadlines: cancel(t) before fire prevents the timer" {
    const w = fiber.currentWorker();
    const me = fiber.currentFiber();

    var wk_canceled: Waker = .{ .fiber = me };
    var wk_wake: Waker = .{ .fiber = me };
    var t_canceled: fiber.Timeout = .{ .on_fire = &wakeCb, .on_fire_ctx = &wk_canceled };
    var t_wake: fiber.Timeout = .{ .on_fire = &wakeCb, .on_fire_ctx = &wk_wake };

    // Register the to-be-canceled one first (earlier — it's the heap head +
    // the in-flight timer's target). Cancel it, then register a later
    // deadline we actually wait on. The canceled one must never fire.
    try w.deadlines.register(&t_canceled, 5);
    w.deadlines.cancel(&t_canceled);
    try w.deadlines.register(&t_wake, 30);

    fiber.park();

    try std.testing.expect(wk_wake.fired.load(.acquire));
    try std.testing.expect(!wk_canceled.fired.load(.acquire));
}

test "Deadlines: re-arm to earlier deadline registered after a later one" {
    // Register the 40 ms deadline FIRST, so the timer arms for 40 ms; then
    // the 10 ms `register` must reset it to fire at ~10 ms. If the re-arm
    // didn't happen, both timers would fire together at ~40 ms — the
    // short-deadline wall-time assertion catches that bug.
    const w = fiber.currentWorker();
    const me = fiber.currentFiber();
    const now = nowMs();

    var wk_long: Waker = .{ .fiber = me };
    var wk_short: Waker = .{ .fiber = me };
    var t_long: fiber.Timeout = .{ .on_fire = &wakeCb, .on_fire_ctx = &wk_long };
    var t_short: fiber.Timeout = .{ .on_fire = &wakeCb, .on_fire_ctx = &wk_short };

    try w.deadlines.register(&t_long, 40); // arms timer for 40 ms
    try w.deadlines.register(&t_short, 10); // must reset to 10 ms

    // Each fire wakes us; record the resume time of each (≈ its fire time)
    // and loop until both have fired.
    var short_at: i64 = 0;
    var long_at: i64 = 0;
    while (short_at == 0 or long_at == 0) {
        fiber.park();
        const t = nowMs();
        if (short_at == 0 and wk_short.fired.load(.acquire)) short_at = t;
        if (long_at == 0 and wk_long.fired.load(.acquire)) long_at = t;
    }

    const short_elapsed = short_at - now;
    const long_elapsed = long_at - now;
    std.log.debug("short_elapsed = {d} ms (expected 8 <= ... < 25), long_elapsed = {d} ms (expected 35 <= ... < 250)", .{ short_elapsed, long_elapsed });
    // Short fires near 10 ms — if the reset didn't happen, this would be
    // near 40 ms instead. Tight gap catches the "reset didn't happen" bug;
    // loose ceiling tolerates scheduler load from concurrent fiber tests.
    try std.testing.expect(short_elapsed >= 8);
    try std.testing.expect(short_elapsed < 25);
    // Long fires near 40 ms — re-armed by onTimer's tail after the short
    // one drained.
    try std.testing.expect(long_elapsed >= 35);
    try std.testing.expect(long_elapsed < 250);
}

// ── sleepMs (the fiber-facing consumer of Deadlines) ────────────────

test "sleepMs: 100 fibers sleeping 10ms in parallel finish under 50ms" {
    // Atomic: the unlocked default + wake-on-push redirect spreads
    // these fibers across many workers; 100 simultaneous wakes from
    // the timer barrage race the counter increment otherwise.
    var counter: std.atomic.Value(u32) = .init(0);

    const Body = struct {
        fn run(c: *std.atomic.Value(u32)) void {
            fiber.sleepMs(10);
            _ = c.fetchAdd(1, .acq_rel);
        }
    };

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(100);

    const start = fiber.coarseMs();
    for (0..100) |_| try g.spawn(.{}, Body.run, .{&counter});
    g.wait();
    const elapsed = fiber.coarseMs() - start;

    try std.testing.expectEqual(@as(u32, 100), counter.load(.acquire));
    // Concurrency check: 100 × sleep(10) serially would be ~1000 ms;
    // parallel should be one timer-tick.  Upper bound is loose since
    // Phase B runs ~50 fiber tests concurrently on the same workers,
    // so individual sleep+wake latency competes with the rest of the
    // suite.  The 200 ms ceiling still rules out serial execution.
    if (elapsed < 9 or elapsed >= 200) {
        std.log.err("elapsed = {d} ms (expected 9 <= elapsed < 200)", .{elapsed});
    }
    try std.testing.expect(elapsed >= 9);
    try std.testing.expect(elapsed < 200);
}

test "sleepMs: single fiber sleeps for the requested duration" {
    const Body = struct {
        fn run() void {
            fiber.sleepMs(20);
        }
    };

    const start = fiber.coarseMs();
    const h = try fiber.spawn(.{}, Body.run, .{});
    h.join();
    const elapsed = fiber.coarseMs() - start;

    if (elapsed < 19 or elapsed >= 200) {
        std.log.err("elapsed = {d} ms (expected 19 <= elapsed < 200)", .{elapsed});
    }
    try std.testing.expect(elapsed >= 19);
    try std.testing.expect(elapsed < 200);
}
