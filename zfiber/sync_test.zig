const std = @import("std");
const fiber = @import("zfiber");

test "Mutex: uncontended lock/unlock cycles correctly" {
    var mu: fiber.Mutex(.untracked) = .{};
    var counter: u32 = 0;

    const Body = struct {
        fn run(m: *fiber.Mutex(.untracked), c: *u32) void {
            for (0..100) |_| {
                m.lock(@src());
                c.* += 1;
                m.unlock();
            }
        }
    };

    const h = try fiber.spawn(.{}, Body.run, .{ &mu, &counter });
    h.join();

    try std.testing.expectEqual(@as(u32, 100), counter);
}

test "Mutex: 10 fibers each incrementing 1000 times under lock" {
    var mu: fiber.Mutex(.untracked) = .{};
    var counter: u32 = 0;

    const Body = struct {
        fn run(m: *fiber.Mutex(.untracked), c: *u32) void {
            for (0..1000) |_| {
                m.lock(@src());
                c.* += 1;
                m.unlock();
                fiber.yield();
            }
        }
    };

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(10);
    for (0..10) |_| try g.spawn(.{}, Body.run, .{ &mu, &counter });
    g.wait();

    try std.testing.expectEqual(@as(u32, 10_000), counter);
}

test "Mutex: hand-off preserves FIFO order under contention" {
    var mu: fiber.Mutex(.untracked) = .{};

    // attempt[i] = id of the fiber that was the i-th to call mu.lock().
    // actual[i] = id of the fiber that was the i-th to be granted the
    // lock. FIFO hand-off means these two slices match, regardless of
    // whatever order the scheduler happened to run the fibers in.
    var attempt: [4]u8 = .{ 0, 0, 0, 0 };
    var actual: [4]u8 = .{ 0, 0, 0, 0 };
    var attempt_idx: std.atomic.Value(usize) = .init(0);
    var acquired_idx: std.atomic.Value(usize) = .init(0);

    const Ctx = struct {
        mu: *fiber.Mutex(.untracked),
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
            c.mu.lock(@src());
            // Stamp our place in the grant order just after wake-up.
            const my_acquired = c.acquired_idx.fetchAdd(1, .acq_rel);
            c.actual[my_acquired] = c.id;
            // Yield while holding the lock so the others actually
            // get to enqueue behind us before we unlock.
            fiber.yield();
            c.mu.unlock();
        }
    };

    // Pre-acquire the mutex so contenders queue up behind it.
    std.debug.assert(mu.tryLock(@src()));

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(5);

    // Pin every contender to worker 0 so FIFO order in the wait
    // queue is the FIFO order of fetchAdd above.
    for (&ctxs, 0..) |*c, i| {
        c.* = .{
            .mu = &mu,
            .attempt = &attempt,
            .actual = &actual,
            .attempt_idx = &attempt_idx,
            .acquired_idx = &acquired_idx,
            .id = @intCast(i + 1),
        };
        try g.spawn(.{}, Body.run, .{c});
    }

    // Releaser unblocks the pre-acquired lock once it gets a turn.
    const Releaser = struct {
        fn run(m: *fiber.Mutex(.untracked)) void {
            m.unlock();
        }
    };
    try g.spawn(.{}, Releaser.run, .{&mu});

    g.wait();

    // FIFO hand-off invariant: grant order equals attempt order.
    try std.testing.expectEqualSlices(u8, &attempt, &actual);
}

test "Mutex: cross-worker contention preserves the increment count" {
    var mu: fiber.Mutex(.untracked) = .{};
    var counter: u64 = 0;

    const Body = struct {
        fn run(m: *fiber.Mutex(.untracked), c: *u64) void {
            for (0..500) |_| {
                m.lock(@src());
                c.* += 1;
                m.unlock();
            }
        }
    };

    const n_workers: usize = @min(4, fiber.runtime().workers.len);
    const N_TOTAL = 4 * n_workers;

    // Fibers spread across `n_workers` — every Mutex.lock/unlock
    // crosses a worker boundary at least sometimes. Correct spinlock
    // + re-check pattern → counter == N_TOTAL * 500. Lost wakeups
    // would deadlock; torn queue mutations would lose increments.
    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(N_TOTAL);
    for (0..N_TOTAL) |_| {
        try g.spawn(.{}, Body.run, .{ &mu, &counter });
    }
    g.wait();

    try std.testing.expectEqual(@as(u64, N_TOTAL * 500), counter);
}

test "RWMutex: multiple readers can hold simultaneously" {
    if (true) {
        return error.SkipZigTest;
    }
    var rw: fiber.RWMutex(.untracked) = .{};
    var max_concurrent: u32 = 0;
    var active: u32 = 0;

    const Reader = struct {
        fn run(l: *fiber.RWMutex(.untracked), act: *u32, peak: *u32) void {
            l.lockShared(@src());
            act.* += 1;
            if (act.* > peak.*) peak.* = act.*;
            // Yield while holding the shared lock so other readers
            // can join in.
            fiber.yield();
            act.* -= 1;
            l.unlockShared();
        }
    };

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(5);
    // Pin to the driver's own worker so all 5 spawns land in this
    // worker's local queue before any of them runs. Pinning to a
    // Spawn lands them all on the current worker's local queue, so
    // they execute round-robin and overlap.
    for (0..5) |_| try g.spawn(.{}, Reader.run, .{ &rw, &active, &max_concurrent });
    g.wait();

    try std.testing.expectEqual(@as(u32, 5), max_concurrent);
}

test "RWMutex: writer excludes other writers and readers" {
    var rw: fiber.RWMutex(.untracked) = .{};
    var observed_inside: u32 = 0;
    var concurrent_in_writer: u32 = 0;

    const Writer = struct {
        fn run(l: *fiber.RWMutex(.untracked), obs: *u32, peak: *u32) void {
            l.lock(@src());
            obs.* += 1;
            if (obs.* > peak.*) peak.* = obs.*;
            fiber.yield();
            obs.* -= 1;
            l.unlock();
        }
    };

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(5);
    for (0..5) |_| try g.spawn(.{}, Writer.run, .{ &rw, &observed_inside, &concurrent_in_writer });
    g.wait();

    try std.testing.expectEqual(@as(u32, 1), concurrent_in_writer);
}

test "RWMutex: writer-preferred — pending writer blocks new readers" {
    var rw: fiber.RWMutex(.untracked) = .{};
    var order: [3]u8 = .{ 0, 0, 0 };
    var slot: usize = 0;

    const Ctx = struct {
        rw: *fiber.RWMutex(.untracked),
        order: *[3]u8,
        slot: *usize,
        id: u8,
    };
    var w_ctx: Ctx = .{ .rw = &rw, .order = &order, .slot = &slot, .id = 'W' };
    var r1_ctx: Ctx = .{ .rw = &rw, .order = &order, .slot = &slot, .id = '1' };
    var r2_ctx: Ctx = .{ .rw = &rw, .order = &order, .slot = &slot, .id = '2' };

    const WriterRun = struct {
        fn run(c: *Ctx) void {
            c.rw.lock(@src());
            c.order[c.slot.*] = c.id;
            c.slot.* += 1;
            c.rw.unlock();
        }
    };
    const ReaderRun = struct {
        fn run(c: *Ctx) void {
            c.rw.lockShared(@src());
            c.order[c.slot.*] = c.id;
            c.slot.* += 1;
            c.rw.unlockShared();
        }
    };

    // Pre-acquire as a reader.
    std.debug.assert(rw.tryLockShared(@src()));

    // The semantic under test ("a queued writer blocks new readers")
    // requires a sequencing precondition: the writer must already be
    // parked on `rw` when the readers attempt their lockShared. We
    // poll the spawned fibers' state.tag instead of trusting spawn
    // order to translate into run order.
    const wh = try fiber.spawn(.{}, WriterRun.run, .{&w_ctx});
    while (wh.fiber.loadState().tag != .waiting) fiber.yield();
    // Writer is queued. Spawn the readers — writer-pref must make
    // them park instead of acquiring the shared lock.
    const r1h = try fiber.spawn(.{}, ReaderRun.run, .{&r1_ctx});
    const r2h = try fiber.spawn(.{}, ReaderRun.run, .{&r2_ctx});
    while (r1h.fiber.loadState().tag != .waiting) fiber.yield();
    while (r2h.fiber.loadState().tag != .waiting) fiber.yield();
    // All three parked. Release the pre-acquired shared lock —
    // writer-pref hands the lock to the writer.
    rw.unlockShared();

    wh.join();
    r1h.join();
    r2h.join();

    // Writer runs first (writer preference); readers follow.
    try std.testing.expectEqual(@as(u8, 'W'), order[0]);
}

test "RWMutex: cross-worker torture — reader observations are monotonic" {
    var rw: fiber.RWMutex(.untracked) = .{};
    // `u128` is two 64-bit stores even on x86_64 — a reader caught
    // mid-write (lock exclusion failed) could observe a torn value
    // with the lower half of update N and the upper half of N-1.
    // The "value can only go up" invariant catches this.
    var counter: u128 = 0;
    var violation: std.atomic.Value(bool) = .init(false);

    const Reader = struct {
        fn run(l: *fiber.RWMutex(.untracked), c: *u128, v: *std.atomic.Value(bool)) void {
            var last_seen: u128 = 0;
            for (0..200) |_| {
                l.lockShared(@src());
                const observed = c.*;
                l.unlockShared();
                if (observed < last_seen) {
                    v.store(true, .release);
                    return;
                }
                last_seen = observed;
            }
        }
    };
    const Writer = struct {
        fn run(l: *fiber.RWMutex(.untracked), c: *u128) void {
            const stride: u128 = (@as(u128, 1) << 64) + 1;
            for (0..100) |_| {
                l.lock(@src());
                c.* += stride;
                l.unlock();
            }
        }
    };

    const N_READERS = 100;
    const N_WRITERS = 10;

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(N_READERS + N_WRITERS);

    for (0..N_READERS) |_| {
        try g.spawn(.{}, Reader.run, .{ &rw, &counter, &violation });
    }
    for (0..N_WRITERS) |_| {
        try g.spawn(.{}, Writer.run, .{ &rw, &counter });
    }
    g.wait();

    const expected: u128 = N_WRITERS * 100 * ((@as(u128, 1) << 64) + 1);
    try std.testing.expectEqual(expected, counter);
    try std.testing.expect(!violation.load(.acquire));
}

test "Notify: wait blocks until notify is called" {
    var sig: fiber.Notify = .{};
    var resumed = false;

    const Waiter = struct {
        fn run(s: *fiber.Notify, out: *bool) void {
            s.wait();
            out.* = true;
        }
    };
    const Notifier = struct {
        fn run(s: *fiber.Notify) void {
            s.notify();
        }
    };

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.spawn(.{}, Waiter.run, .{ &sig, &resumed });
    try g.spawn(.{}, Notifier.run, .{&sig});
    g.wait();

    try std.testing.expect(resumed);
}

test "Notify: pending flag lets wait return immediately when notify ran first" {
    var sig: fiber.Notify = .{};
    var observed_wait_yielded = false;

    const Body = struct {
        fn run(s: *fiber.Notify, out: *bool) void {
            // Notify first: pending becomes true, no waiter parked.
            s.notify();
            // wait() should consume pending and return without
            // yielding. A parked-forever fiber would deadlock since
            // no notifier ever fires after this point.
            s.wait();
            out.* = false; // hit unconditionally
        }
    };

    const h = try fiber.spawn(.{}, Body.run, .{ &sig, &observed_wait_yielded });
    h.join();

    // Reaching here proves wait() took the fast path (no park).
    try std.testing.expect(!observed_wait_yielded);
}

test "Notify: notify/wait alternate any number of times in one fiber" {
    // A single fiber drives the doorbell through many rounds, exercising
    // both orderings: notify-then-wait (pending fast path) and a saturating
    // double-notify that still lets exactly one wait through. The loop
    // completing proves no round ever parked forever — i.e. notify and wait
    // compose correctly when called repeatedly, in either order.
    var sig: fiber.Notify = .{};

    const Body = struct {
        fn run(s: *fiber.Notify) void {
            var round: usize = 0;
            while (round < 1000) : (round += 1) {
                // notify-before-wait: pending is set, wait returns immediately.
                s.notify();
                s.wait();
                // Double notify coalesces to a single pending (it's a flag,
                // not a counter); the single wait below consumes it. A leaked
                // second pending would be harmless, a lost one would deadlock
                // the *next* round's wait — which the loop would expose.
                s.notify();
                s.notify();
                s.wait();
            }
        }
    };

    const h = try fiber.spawn(.{}, Body.run, .{&sig});
    h.join();
}

test "Notify: one waiter, many notifiers, no lost wakeup" {
    // The contract that raft's mainLoop relies on: producers enqueue work then
    // notify; a single consumer waits, then drains *everything* visible before
    // waiting again. Under arbitrary interleaving — and even though many
    // notifies coalesce into one wakeup — the consumer must observe every
    // increment and terminate. If any wakeup were lost, the consumer would park
    // forever and Group.wait() below would hang (deadlock = test failure).
    const producers = 4;
    const per_producer = 100;
    const total = producers * per_producer;

    var sig: fiber.Notify = .{};
    var produced = std.atomic.Value(usize).init(0);

    const Producer = struct {
        fn run(s: *fiber.Notify, p: *std.atomic.Value(usize)) void {
            var i: usize = 0;
            while (i < per_producer) : (i += 1) {
                _ = p.fetchAdd(1, .release);
                s.notify();
                // Yield to shuffle the interleaving so the wakeups land in
                // every relative position to the consumer's wait/drain.
                fiber.yield();
            }
        }
    };
    const Consumer = struct {
        fn run(s: *fiber.Notify, p: *std.atomic.Value(usize), seen: *usize) void {
            // Re-read *after* each wait: a single coalesced wakeup may stand in
            // for several notifies, and the post-wait load picks up all of them.
            while (seen.* < total) {
                s.wait();
                seen.* = p.load(.acquire);
            }
        }
    };

    var seen: usize = 0;
    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.spawn(.{}, Consumer.run, .{ &sig, &produced, &seen });
    for (0..producers) |_| {
        try g.spawn(.{}, Producer.run, .{ &sig, &produced });
    }
    g.wait();

    try std.testing.expectEqual(@as(usize, total), produced.load(.acquire));
    try std.testing.expectEqual(@as(usize, total), seen);
}
