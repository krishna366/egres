const std = @import("std");
const fiber = @import("fiber");

test "Runtime: 100 fibers each yielding 10 times all finish" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    const N = 100;
    var counters = [_]u32{0} ** N;

    const Body = struct {
        fn run(c: *u32) void {
            while (c.* < 10) : (c.* += 1) {
                fiber.yield();
            }
        }
    };

    for (&counters) |*c| {
        _ = try fiber.spawn(&rt, .{}, Body.run, .{c});
    }

    rt.run();

    for (counters) |c| try std.testing.expectEqual(@as(u32, 10), c);

    const s = rt.stats();
    try std.testing.expectEqual(@as(usize, N), s.total);
    try std.testing.expectEqual(@as(usize, N), s.finished);
    try std.testing.expectEqual(@as(usize, 0), s.ready);
    try std.testing.expectEqual(@as(usize, 0), s.waiting);
}

test "Runtime: producer parks, consumer wakes, producer resumes" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var producer_handle: ?*fiber.Fiber = null;
    var producer_resumed = false;

    const Producer = struct {
        out: *?*fiber.Fiber,
        resumed: *bool,

        fn run(self: *@This()) void {
            self.out.* = fiber.currentFiber();
            fiber.park();
            self.resumed.* = true;
        }
    };
    const Consumer = struct {
        target: *?*fiber.Fiber,

        fn run(self: *@This()) void {
            // drainRemote is LIFO so we may run before the producer;
            // yield until it records its handle.
            while (self.target.* == null) fiber.yield();
            fiber.wake(self.target.*.?);
        }
    };

    var p_ctx = Producer{ .out = &producer_handle, .resumed = &producer_resumed };
    var c_ctx = Consumer{ .target = &producer_handle };

    _ = try fiber.spawn(&rt, .{}, Producer.run, .{&p_ctx});
    _ = try fiber.spawn(&rt, .{}, Consumer.run, .{&c_ctx});

    rt.run();

    try std.testing.expect(producer_resumed);
    try std.testing.expect(producer_handle != null);

    const s = rt.stats();
    try std.testing.expectEqual(@as(usize, 2), s.finished);
    try std.testing.expectEqual(@as(usize, 0), s.waiting);
}

test "Runtime: CPU accounting per fiber" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    const target_ns: u64 = 10 * std.time.ns_per_ms;

    const Body = struct {
        fn run(dummy: *u64) void {
            var tp: std.os.linux.timespec = undefined;
            _ = std.os.linux.clock_gettime(std.os.linux.CLOCK.THREAD_CPUTIME_ID, &tp);
            const start: i64 = @as(i64, tp.sec) * std.time.ns_per_s + tp.nsec;
            while (true) {
                _ = std.os.linux.clock_gettime(std.os.linux.CLOCK.THREAD_CPUTIME_ID, &tp);
                const now: i64 = @as(i64, tp.sec) * std.time.ns_per_s + tp.nsec;
                if (now - start >= target_ns) break;
                dummy.* +%= 1;
            }
        }
    };

    var dummies = [_]u64{ 0, 0, 0 };
    const h0 = try fiber.spawn(&rt, .{}, Body.run, .{&dummies[0]});
    const h1 = try fiber.spawn(&rt, .{}, Body.run, .{&dummies[1]});
    const h2 = try fiber.spawn(&rt, .{}, Body.run, .{&dummies[2]});

    rt.run();

    // 20% tolerance both sides — clock granularity + the busy-loop's
    // own clock_gettime overhead inflate the measured CPU slightly.
    const low: u64 = target_ns * 80 / 100;
    const high: u64 = target_ns * 130 / 100;
    for ([_]*fiber.Fiber{ h0.fiber, h1.fiber, h2.fiber }) |f| {
        try std.testing.expect(f.cpu_total_ns >= low);
        try std.testing.expect(f.cpu_total_ns <= high);
    }
}

test "Runtime: TSC accounting times a fibonacci workload" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    // Naive recursive fibonacci — does meaningful CPU work even in
    // Debug mode (~500K recursive calls for fib(28)) without slowing
    // down the test suite. fib(28) = 317_811.
    const N: u64 = 28;
    var result: u64 = 0;

    const Body = struct {
        fn fib(n: u64) u64 {
            if (n < 2) return n;
            return fib(n - 1) + fib(n - 2);
        }
        fn run(out: *u64, n: u64) void {
            out.* = fib(n);
        }
    };

    const h = try fiber.spawn(&rt, .{}, Body.run, .{ &result, N });
    rt.run();

    try std.testing.expectEqual(@as(u64, 317_811), result);
    try std.testing.expect(h.fiber.cpu_total_ns > 0);

    // Convert the Q32.32 ns/cycle factor back to a real number and
    // derive an approximate TSC frequency from it.
    const factor_q32 = fiber.tscNsPerCycleQ32();
    const ns_per_cycle: f64 = @as(f64, @floatFromInt(factor_q32)) /
        @as(f64, @floatFromInt(@as(u64, 1) << 32));
    const tsc_ghz: f64 = if (ns_per_cycle > 0.0) 1.0 / ns_per_cycle else 0.0;

    std.debug.print(
        "\n  fib({d}) = {d}\n" ++
            "  cpu_total = {d} ns ({d:.3} ms)\n" ++
            "  tsc factor (Q32.32) = {d}  →  {d:.4} ns/cycle  ({d:.3} GHz)\n",
        .{
            N,
            result,
            h.fiber.cpu_total_ns,
            @as(f64, @floatFromInt(h.fiber.cpu_total_ns)) / @as(f64, std.time.ns_per_ms),
            factor_q32,
            ns_per_cycle,
            tsc_ghz,
        },
    );
}

test "Runtime: high priority runs before normal in queue scan" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var first: ?u8 = null;

    const Body = struct {
        flag: *?u8,
        letter: u8,

        fn run(self: *@This()) void {
            if (self.flag.* == null) self.flag.* = self.letter;
        }
    };

    // Normal first, high second. Without priority FIFO would run
    // normal first; with priority the high-queue scan wins.
    var n_ctx = Body{ .flag = &first, .letter = 'n' };
    var h_ctx = Body{ .flag = &first, .letter = 'h' };
    _ = try fiber.spawn(&rt, .{ .priority = .normal }, Body.run, .{&n_ctx});
    _ = try fiber.spawn(&rt, .{ .priority = .high }, Body.run, .{&h_ctx});

    rt.run();

    try std.testing.expectEqual(@as(?u8, 'h'), first);
}

test "Runtime: cross-worker spawn routes via remote queue" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{ .num_workers = 4 });
    defer rt.deinit();

    const N_PER_WORKER = 25;
    const N = 4 * N_PER_WORKER;

    // Each fiber records which worker ran it. Counters per worker.
    var per_worker: [4]std.atomic.Value(u32) = .{
        .init(0), .init(0), .init(0), .init(0),
    };

    const Body = struct {
        fn run(pw: *[4]std.atomic.Value(u32)) void {
            const wid = fiber.currentWorker().id;
            _ = pw[wid].fetchAdd(1, .release);
        }
    };

    // From main thread (no current worker): explicitly pin to each
    // of the 4 workers. Spawning before rt.run() pushes to each
    // worker's remote_queue; on rt.run() the workers drain those
    // and run the fibers.
    for (0..N) |i| {
        const target: u8 = @intCast(i % 4);
        _ = try fiber.spawn(&rt, .{ .worker_id = target }, Body.run, .{&per_worker});
    }

    rt.run();

    // Every spawned fiber ran. Each pinned worker ran exactly its
    // share — proves the remote_queue + wake_async routing works.
    for (&per_worker, 0..) |*counter, i| {
        const got = counter.load(.acquire);
        std.testing.expectEqual(@as(u32, N_PER_WORKER), got) catch |err| {
            std.log.err("worker {d} ran {d} fibers, expected {d}", .{ i, got, N_PER_WORKER });
            return err;
        };
    }
}

test "Runtime: JoinHandle returns the spawned fiber's value" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    const Producer = struct {
        fn run(x: u32) u64 {
            return @as(u64, x) * 2;
        }
    };
    const Consumer = struct {
        fn run(h: fiber.JoinHandle(u64), out: *u64) void {
            out.* = h.join();
        }
    };

    var observed: u64 = 0;
    const ph = try fiber.spawn(&rt, .{}, Producer.run, .{21});
    _ = try fiber.spawn(&rt, .{}, Consumer.run, .{ ph, &observed });

    rt.run();

    try std.testing.expectEqual(@as(u64, 42), observed);
}

test "Runtime: JoinHandle fast-path after producer already finished" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    const Producer = struct {
        fn run() u32 {
            return 7;
        }
    };
    const Consumer = struct {
        fn run(h: fiber.JoinHandle(u32), out: *u32) void {
            // Yield enough times that the no-op producer finishes
            // first. join() should then take the fast path (sees
            // .finished, returns cached result without parking).
            fiber.yield();
            fiber.yield();
            out.* = h.join();
        }
    };

    var observed: u32 = 0;
    const ph = try fiber.spawn(&rt, .{}, Producer.run, .{});
    _ = try fiber.spawn(&rt, .{}, Consumer.run, .{ ph, &observed });

    rt.run();

    try std.testing.expectEqual(@as(u32, 7), observed);
}

test "Runtime: JoinHandle(void) blocks for completion without a value" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var produced = false;
    var consumer_saw_finished = false;

    const Producer = struct {
        fn run(out: *bool) void {
            fiber.yield();
            out.* = true;
        }
    };
    const Consumer = struct {
        fn run(h: fiber.JoinHandle(void), prod_done: *bool, observed: *bool) void {
            h.join();
            observed.* = prod_done.*;
        }
    };

    const ph = try fiber.spawn(&rt, .{}, Producer.run, .{&produced});
    _ = try fiber.spawn(&rt, .{}, Consumer.run, .{ ph, &produced, &consumer_saw_finished });

    rt.run();

    try std.testing.expect(produced);
    try std.testing.expect(consumer_saw_finished);
}

test "Runtime: dumpFibers and stats reflect a parked fiber" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var producer_handle: ?*fiber.Fiber = null;
    var dump_saw_waiting = false;
    var stats_waiting_count: usize = 0;

    const Producer = struct {
        out: *?*fiber.Fiber,

        fn run(self: *@This()) void {
            self.out.* = fiber.currentFiber();
            fiber.park();
        }
    };
    const Dumper = struct {
        rt: *fiber.Runtime,
        target: *?*fiber.Fiber,
        saw_waiting: *bool,
        waiting_count: *usize,

        fn run(self: *@This()) void {
            // drainRemote is LIFO so the producer may not have parked
            // yet when we first run; yield until it has.
            while (self.target.* == null) fiber.yield();

            var aw: std.Io.Writer.Allocating = .init(std.testing.allocator);
            defer aw.deinit();
            fiber.dumpFibers(self.rt, &aw.writer) catch unreachable;
            const text = aw.writer.buffered();
            self.saw_waiting.* = std.mem.indexOf(u8, text, "parked_for=") != null;

            const s = self.rt.stats();
            self.waiting_count.* = s.waiting;

            fiber.wake(self.target.*.?);
        }
    };

    var p_ctx = Producer{ .out = &producer_handle };
    var d_ctx = Dumper{
        .rt = &rt,
        .target = &producer_handle,
        .saw_waiting = &dump_saw_waiting,
        .waiting_count = &stats_waiting_count,
    };

    _ = try fiber.spawn(&rt, .{}, Producer.run, .{&p_ctx});
    _ = try fiber.spawn(&rt, .{}, Dumper.run, .{&d_ctx});

    rt.run();

    try std.testing.expect(dump_saw_waiting);
    try std.testing.expectEqual(@as(usize, 1), stats_waiting_count);
}
