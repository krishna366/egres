const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const Clock = utils.hlc.Clock;
const HLC = utils.hlc.HLC;

test "concurrent next yields distinct monotonic HLCs" {
    const t = std.testing;
    var clock = Clock{};
    const Ctx = struct {
        clock: *Clock,
        results: [256]HLC = undefined,
        fn run(ctx: *@This()) void {
            for (0..ctx.results.len) |i| ctx.results[i] = ctx.clock.next();
        }
    };
    var a = Ctx{ .clock = &clock };
    var b = Ctx{ .clock = &clock };
    var ta = try fiber.spawn(.{}, Ctx.run, .{&a});
    var tb = try fiber.spawn(.{}, Ctx.run, .{&b});
    ta.join();
    tb.join();

    // Every HLC emitted must be unique (CAS + strict increment guarantees it).
    var all: [512]u64 = undefined;
    for (a.results, 0..) |h, i| all[i] = h.asU64();
    for (b.results, 0..) |h, i| all[256 + i] = h.asU64();
    std.mem.sort(u64, &all, {}, comptime std.sort.asc(u64));
    for (1..all.len) |i| try t.expect(all[i] > all[i - 1]);
}
