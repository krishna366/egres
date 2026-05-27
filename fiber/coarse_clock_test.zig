const std = @import("std");
const fiber = @import("fiber");

test "CoarseClock: ms() advances during a parked fiber's sleep" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var observed_delta: i64 = 0;

    const Ctx = struct {
        rt: *fiber.Runtime,
        observed_delta: *i64,

        fn run(self: *@This()) void {
            const before = self.rt.ms();
            fiber.sleep(30);
            const after = self.rt.ms();
            self.observed_delta.* = after - before;
        }
    };

    var ctx: Ctx = .{ .rt = &rt, .observed_delta = &observed_delta };
    _ = try fiber.spawn(&rt, .{}, Ctx.run, .{&ctx});

    rt.run();

    // The ticker fires once a millisecond regardless of whether the
    // fiber is running, so the coarse clock should reflect ~30 ms of
    // wall passing during the sleep. Generous bounds for CI noise.
    try std.testing.expect(observed_delta >= 25);
    try std.testing.expect(observed_delta < 60);
}

test "CoarseClock: ms() is stable across many cheap reads" {
    var rt: fiber.Runtime = undefined;
    try rt.init(std.testing.allocator, .{});
    defer rt.deinit();

    var monotonic_violation: bool = false;

    const Ctx = struct {
        rt: *fiber.Runtime,
        violation: *bool,

        fn run(self: *@This()) void {
            var last = self.rt.ms();
            // Read the clock many times; verify monotonicity.
            // Without a sleep, all reads land between two ticks and
            // should return the same cached value — but even across
            // a yield, the value must never go backwards.
            for (0..1000) |_| {
                const now = self.rt.ms();
                if (now < last) {
                    self.violation.* = true;
                    return;
                }
                last = now;
                fiber.yield();
            }
        }
    };

    var ctx: Ctx = .{ .rt = &rt, .violation = &monotonic_violation };
    _ = try fiber.spawn(&rt, .{}, Ctx.run, .{&ctx});

    rt.run();

    try std.testing.expect(!monotonic_violation);
}
