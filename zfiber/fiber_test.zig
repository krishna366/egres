const std = @import("std");
const fiber = @import("zfiber");

test "Fiber: two fibers, manual drive, both finish" {
    const Body = struct {
        fn run(counter: *u32) void {
            counter.* += 1;
        }
    };

    var a: u32 = 0;
    var b: u32 = 0;

    const fa = try fiber.Fiber.init(std.testing.allocator, .{}, Body.run, .{&a});
    defer fa.deinit(std.testing.allocator);
    const fb = try fiber.Fiber.init(std.testing.allocator, .{}, Body.run, .{&b});
    defer fb.deinit(std.testing.allocator);

    try std.testing.expect(!fa.isFinished());
    try std.testing.expect(!fb.isFinished());

    fa.step();
    fb.step();

    try std.testing.expect(fa.isFinished());
    try std.testing.expect(fb.isFinished());
    try std.testing.expectEqual(@as(u32, 1), a);
    try std.testing.expectEqual(@as(u32, 1), b);
}

test "Fiber: yields N times then finishes" {
    const Ctx = struct {
        fb: ?*fiber.Fiber = null,
        counter: u32 = 0,
        yields: u32 = 5,

        fn run(self: *@This()) void {
            while (self.counter < self.yields) {
                self.counter += 1;
                self.fb.?.yield();
            }
        }
    };

    var ctx: Ctx = .{};
    const f = try fiber.Fiber.init(std.testing.allocator, .{}, Ctx.run, .{&ctx});
    defer f.deinit(std.testing.allocator);
    ctx.fb = f;

    var resumes: u32 = 0;
    while (!f.isFinished()) : (resumes += 1) {
        f.step();
    }

    try std.testing.expectEqual(@as(u32, 5), ctx.counter);
    // 5 explicit yields + 1 step that drives the body past the loop
    // and into the trailing yield Coroutine.setup emits on return.
    try std.testing.expectEqual(@as(u32, 6), resumes);
}

test "Fiber: many fibers allocate, run, free without leaks" {
    const Body = struct {
        fn run(counter: *u32) void {
            counter.* += 1;
        }
    };

    const N = 100;
    var counters = [_]u32{0} ** N;
    var fibers: [N]*fiber.Fiber = undefined;

    for (&fibers, &counters) |*f, *c| {
        f.* = try fiber.Fiber.init(std.testing.allocator, .{}, Body.run, .{c});
    }
    defer for (fibers) |f| f.deinit(std.testing.allocator);

    for (fibers) |f| f.step();

    for (counters) |c| try std.testing.expectEqual(@as(u32, 1), c);
    for (fibers) |f| try std.testing.expect(f.isFinished());
}
