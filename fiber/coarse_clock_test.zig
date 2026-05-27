const std = @import("std");
const fiber = @import("fiber");

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
