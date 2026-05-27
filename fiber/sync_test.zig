const std = @import("std");
const fiber = @import("fiber");

test "Mutex: uncontended lock/unlock cycles correctly" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var mu: fiber.Mutex = .{};
    var counter: u32 = 0;

    const Ctx = struct { mu: *fiber.Mutex, counter: *u32 };
    var ctx: Ctx = .{ .mu = &mu, .counter = &counter };

    const Body = struct {
        fn run(c: *Ctx) void {
            for (0..100) |_| {
                c.mu.lock();
                c.counter.* += 1;
                c.mu.unlock();
            }
        }
    };

    _ = try fiber.spawn(&rt, .{}, Body.run, .{&ctx});
    rt.run();

    try std.testing.expectEqual(@as(u32, 100), counter);
}

test "Mutex: 10 fibers each incrementing 1000 times under lock" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var mu: fiber.Mutex = .{};
    var counter: u32 = 0;

    const Ctx = struct { mu: *fiber.Mutex, counter: *u32 };
    var ctx: Ctx = .{ .mu = &mu, .counter = &counter };

    const Body = struct {
        fn run(c: *Ctx) void {
            for (0..1000) |_| {
                c.mu.lock();
                c.counter.* += 1;
                c.mu.unlock();
                fiber.yield();
            }
        }
    };

    for (0..10) |_| {
        _ = try fiber.spawn(&rt, .{}, Body.run, .{&ctx});
    }
    rt.run();

    try std.testing.expectEqual(@as(u32, 10_000), counter);
}

test "Mutex: hand-off preserves FIFO order under contention" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var mu: fiber.Mutex = .{};

    // attempt[i] = id of the fiber that was the i-th to call mu.lock().
    // actual[i] = id of the fiber that was the i-th to be granted the
    // lock. FIFO hand-off means these two slices match, regardless of
    // whatever order the scheduler happened to run the fibers in.
    var attempt: [4]u8 = .{ 0, 0, 0, 0 };
    var actual: [4]u8 = .{ 0, 0, 0, 0 };
    var attempt_idx: std.atomic.Value(usize) = .init(0);
    var acquired_idx: std.atomic.Value(usize) = .init(0);

    const Ctx = struct {
        mu: *fiber.Mutex,
        attempt: *[4]u8,
        actual: *[4]u8,
        attempt_idx: *std.atomic.Value(usize),
        acquired_idx: *std.atomic.Value(usize),
        id: u8,
    };
    var ctxs: [4]Ctx = undefined;

    const Body = struct {
        fn run(c: *Ctx) void {
            // Stamp our place in the attempt order just before the
            // call that may park us. Single-worker means fetchAdd
            // happens-before the matching mu.lock() the same fiber
            // makes, and only one fiber runs at a time — so the
            // attempt index matches the order we enter mu's queue.
            const my_attempt = c.attempt_idx.fetchAdd(1, .acq_rel);
            c.attempt[my_attempt] = c.id;
            c.mu.lock();
            // Stamp our place in the grant order just after wake-up.
            const my_acquired = c.acquired_idx.fetchAdd(1, .acq_rel);
            c.actual[my_acquired] = c.id;
            // Yield while holding the lock so the others actually
            // get to enqueue behind us before we unlock.
            fiber.yield();
            c.mu.unlock();
        }
    };

    // Pre-acquire the mutex from the main thread so contenders queue
    // up behind it. `tryLock` is the right call — `lock` would
    // deadlock if contended (no fiber context); `tryLock` is pure
    // atomic state, no parking path.
    std.debug.assert(mu.tryLock());

    for (&ctxs, 0..) |*c, i| {
        c.* = .{
            .mu = &mu,
            .attempt = &attempt,
            .actual = &actual,
            .attempt_idx = &attempt_idx,
            .acquired_idx = &acquired_idx,
            .id = @intCast(i + 1),
        };
        _ = try fiber.spawn(&rt, .{}, Body.run, .{c});
    }

    // Fifth fiber releases the pre-acquired lock once it gets a turn.
    // Order of execution against the contenders doesn't matter — if
    // it runs first, the first contender takes the free lock and the
    // rest queue; if it runs after some have queued, the hand-off
    // path is exercised. Either way the FIFO invariant holds.
    const Releaser = struct {
        fn run(m: *fiber.Mutex) void {
            m.unlock();
        }
    };
    _ = try fiber.spawn(&rt, .{}, Releaser.run, .{&mu});

    rt.run();

    // FIFO hand-off invariant: grant order equals attempt order.
    try std.testing.expectEqualSlices(u8, &attempt, &actual);
}

test "Mutex: cross-worker contention preserves the increment count" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{ .num_workers = 4 });
    defer rt.deinit();

    var mu: fiber.Mutex = .{};
    var counter: u64 = 0;

    const Ctx = struct { mu: *fiber.Mutex, counter: *u64 };
    var ctx: Ctx = .{ .mu = &mu, .counter = &counter };

    const Body = struct {
        fn run(c: *Ctx) void {
            for (0..500) |_| {
                c.mu.lock();
                c.counter.* += 1;
                c.mu.unlock();
            }
        }
    };

    // 16 fibers across 4 workers — every Mutex.lock/unlock crosses
    // a worker boundary at least sometimes. If the spinlock + the
    // re-check pattern is correct, counter == 16 * 500. If lost
    // wakeups or torn queue mutations happen, the test deadlocks
    // (lost wake) or the counter is short (torn increment).
    const N_PER_WORKER = 4;
    const N_TOTAL = 4 * N_PER_WORKER;
    for (0..N_TOTAL) |i| {
        const wid: u8 = @intCast(i % 4);
        _ = try fiber.spawn(&rt, .{ .worker_id = wid }, Body.run, .{&ctx});
    }

    rt.run();

    try std.testing.expectEqual(@as(u64, N_TOTAL * 500), counter);
}

test "RWMutex: multiple readers can hold simultaneously" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var rw: fiber.RWMutex = .{};
    var max_concurrent: u32 = 0;
    var active: u32 = 0;

    const Ctx = struct { rw: *fiber.RWMutex, active: *u32, max_concurrent: *u32 };
    var ctx: Ctx = .{ .rw = &rw, .active = &active, .max_concurrent = &max_concurrent };

    const Reader = struct {
        fn run(c: *Ctx) void {
            c.rw.lockShared();
            c.active.* += 1;
            if (c.active.* > c.max_concurrent.*) c.max_concurrent.* = c.active.*;
            // Yield while holding the shared lock so other readers
            // get to join in.
            fiber.yield();
            c.active.* -= 1;
            c.rw.unlockShared();
        }
    };

    for (0..5) |_| {
        _ = try fiber.spawn(&rt, .{}, Reader.run, .{&ctx});
    }
    rt.run();

    try std.testing.expectEqual(@as(u32, 5), max_concurrent);
}

test "RWMutex: writer excludes other writers and readers" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var rw: fiber.RWMutex = .{};
    var observed_inside: u32 = 0;
    var concurrent_in_writer: u32 = 0;

    const Ctx = struct {
        rw: *fiber.RWMutex,
        observed: *u32,
        concurrent: *u32,
    };
    var ctx: Ctx = .{
        .rw = &rw,
        .observed = &observed_inside,
        .concurrent = &concurrent_in_writer,
    };

    const Writer = struct {
        fn run(c: *Ctx) void {
            c.rw.lock();
            c.observed.* += 1;
            if (c.observed.* > c.concurrent.*) c.concurrent.* = c.observed.*;
            fiber.yield();
            c.observed.* -= 1;
            c.rw.unlock();
        }
    };

    for (0..5) |_| {
        _ = try fiber.spawn(&rt, .{}, Writer.run, .{&ctx});
    }
    rt.run();

    try std.testing.expectEqual(@as(u32, 1), concurrent_in_writer);
}

test "RWMutex: writer-preferred — pending writer blocks new readers" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var rw: fiber.RWMutex = .{};
    var order: [3]u8 = .{ 0, 0, 0 };
    var slot: usize = 0;

    const Ctx = struct {
        rw: *fiber.RWMutex,
        order: *[3]u8,
        slot: *usize,
        id: u8,
    };
    var w_ctx: Ctx = .{ .rw = &rw, .order = &order, .slot = &slot, .id = 'W' };
    var r1_ctx: Ctx = .{ .rw = &rw, .order = &order, .slot = &slot, .id = '1' };
    var r2_ctx: Ctx = .{ .rw = &rw, .order = &order, .slot = &slot, .id = '2' };

    const WriterRun = struct {
        fn run(c: *Ctx) void {
            c.rw.lock();
            c.order[c.slot.*] = c.id;
            c.slot.* += 1;
            c.rw.unlock();
        }
    };
    const ReaderRun = struct {
        fn run(c: *Ctx) void {
            c.rw.lockShared();
            c.order[c.slot.*] = c.id;
            c.slot.* += 1;
            c.rw.unlockShared();
        }
    };

    // Pre-acquire as a reader from the main thread (no fiber
    // context). `tryLockShared` is the right call — it's a pure
    // atomic check, no parking path, so it doesn't need a fiber.
    std.debug.assert(rw.tryLockShared());

    // The semantic under test ("a queued writer blocks new readers")
    // requires a sequencing precondition: the writer must already be
    // parked on `rw` when the readers attempt their `lockShared`.
    // The Coord fiber enforces that by spawning each fiber, then
    // polling its `state.tag` until it's `.waiting`. This makes the
    // test agnostic to scheduler order — Coord doesn't trust spawn
    // order to translate into run order.
    const Coord = struct {
        fn run(
            rt_p: *fiber.Runtime,
            rw_p: *fiber.RWMutex,
            w_p: *Ctx,
            r1_p: *Ctx,
            r2_p: *Ctx,
        ) void {
            const wh = fiber.spawn(rt_p, .{}, WriterRun.run, .{w_p}) catch unreachable;
            while (wh.fiber.loadState().tag != .waiting) fiber.yield();
            // Writer is now queued. Spawn the readers — writer-pref
            // must make them park instead of acquiring the shared
            // lock (because only writer-pref blocks them; without it
            // they'd find the lock in shared mode and succeed).
            const r1h = fiber.spawn(rt_p, .{}, ReaderRun.run, .{r1_p}) catch unreachable;
            const r2h = fiber.spawn(rt_p, .{}, ReaderRun.run, .{r2_p}) catch unreachable;
            while (r1h.fiber.loadState().tag != .waiting) fiber.yield();
            while (r2h.fiber.loadState().tag != .waiting) fiber.yield();
            // All three parked. Release the pre-acquired shared
            // lock — writer-pref hands the lock to the writer.
            rw_p.unlockShared();
        }
    };
    _ = try fiber.spawn(&rt, .{}, Coord.run, .{ &rt, &rw, &w_ctx, &r1_ctx, &r2_ctx });

    rt.run();

    // Writer runs first (writer preference); readers follow.
    try std.testing.expectEqual(@as(u8, 'W'), order[0]);
}

test "RWMutex: cross-worker torture — reader observations are monotonic" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{ .num_workers = 4 });
    defer rt.deinit();

    var rw: fiber.RWMutex = .{};
    // `u128` is two 64-bit stores even on x86_64 — a reader caught
    // mid-write (lock exclusion failed) could observe a torn value
    // with the lower half of update N and the upper half of N-1.
    // The "value can only go up" invariant catches this.
    var counter: u128 = 0;
    var violation: std.atomic.Value(bool) = .init(false);

    const Ctx = struct {
        rw: *fiber.RWMutex,
        counter: *u128,
        violation: *std.atomic.Value(bool),
    };
    var ctx: Ctx = .{ .rw = &rw, .counter = &counter, .violation = &violation };

    const Reader = struct {
        fn run(c: *Ctx) void {
            var last_seen: u128 = 0;
            for (0..200) |_| {
                c.rw.lockShared();
                const observed = c.counter.*;
                c.rw.unlockShared();
                // Monotonic invariant: counter only increments;
                // a reader can only see values >= what it saw
                // previously. A decrease means lock exclusion
                // failed — either a torn 128-bit read or a
                // missed wakeup that let a writer run mid-read.
                if (observed < last_seen) {
                    c.violation.store(true, .release);
                    return;
                }
                last_seen = observed;
            }
        }
    };
    const Writer = struct {
        fn run(c: *Ctx) void {
            // Bump by `1 << 64 + 1` each iteration so both halves
            // of the u128 are exercised — a torn read showing the
            // new lower half with the old upper half would be a
            // dramatic, easily-detected decrease.
            const stride: u128 = (@as(u128, 1) << 64) + 1;
            for (0..100) |_| {
                c.rw.lock();
                c.counter.* += stride;
                c.rw.unlock();
            }
        }
    };

    // 100 readers + 10 writers across 4 workers. Writers do 100
    // increments each → 1000 total stride applications. Final
    // counter == 1000 * stride. Two invariants checked:
    //   1. counter == expected (no lost writes — exclusion held).
    //   2. no reader observed a decrease (no torn read / race).
    const N_READERS = 100;
    const N_WRITERS = 10;
    for (0..N_READERS) |i| {
        const wid: u8 = @intCast(i % 4);
        _ = try fiber.spawn(&rt, .{ .worker_id = wid }, Reader.run, .{&ctx});
    }
    for (0..N_WRITERS) |i| {
        const wid: u8 = @intCast(i % 4);
        _ = try fiber.spawn(&rt, .{ .worker_id = wid }, Writer.run, .{&ctx});
    }
    rt.run();

    const expected: u128 = 1000 * ((@as(u128, 1) << 64) + 1);
    try std.testing.expectEqual(expected, counter);
    try std.testing.expect(!violation.load(.acquire));
}

test "Notify: wait blocks until notify is called" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var sig: fiber.Notify = .{};
    var resumed = false;

    const Waiter = struct {
        sig: *fiber.Notify,
        resumed: *bool,

        fn run(self: *@This()) void {
            self.sig.wait();
            self.resumed.* = true;
        }
    };
    const Notifier = struct {
        sig: *fiber.Notify,

        fn run(self: *@This()) void {
            self.sig.notify();
        }
    };

    var w_ctx = Waiter{ .sig = &sig, .resumed = &resumed };
    var n_ctx = Notifier{ .sig = &sig };
    _ = try fiber.spawn(&rt, .{}, Waiter.run, .{&w_ctx});
    _ = try fiber.spawn(&rt, .{}, Notifier.run, .{&n_ctx});

    rt.run();

    try std.testing.expect(resumed);
}

test "Notify: pending flag lets wait return immediately when notify ran first" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var sig: fiber.Notify = .{};
    var observed_wait_yielded = false;

    const Ctx = struct {
        sig: *fiber.Notify,
        observed_wait_yielded: *bool,
    };
    var ctx: Ctx = .{ .sig = &sig, .observed_wait_yielded = &observed_wait_yielded };

    const Body = struct {
        fn run(c: *Ctx) void {
            // Notify first: pending becomes true, no waiter parked.
            c.sig.notify();
            // wait should consume pending and return without
            // yielding. We detect "did we yield" by checking that
            // the fiber's intent never went to .park. We can't
            // observe that directly, so just check that wait
            // returns at all (a parked-forever fiber would deadlock
            // the runtime — `rt.run` would not return).
            c.sig.wait();
            c.observed_wait_yielded.* = false; // hit unconditionally
        }
    };

    _ = try fiber.spawn(&rt, .{}, Body.run, .{&ctx});
    rt.run();

    // If wait had parked, the runtime would have deadlocked (no
    // notifier ever fires after this point) and rt.run wouldn't
    // have returned cleanly. The fact that the test reaches the
    // assertion at all proves the fast-path works.
    try std.testing.expect(!observed_wait_yielded);
}
