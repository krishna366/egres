const std = @import("std");
const fiber = @import("zfiber");

fn ctxOffFiber(out: **fiber.Ctx) void {
    out.* = fiber.ctx();
}

test "ctx: no statement means the background context, record is a no-op" {
    // The test fiber has no context attached.
    const bg = fiber.ctx();
    try std.testing.expectEqual(0, bg.name.len);
    bg.record("dropped", .{});
    bg.record("dropped {d}", .{1});
    try std.testing.expectEqual(0, bg.records.items.len);

    // Off any fiber: the same background context.
    var seen: *fiber.Ctx = undefined;
    const t = try std.Thread.spawn(.{}, ctxOffFiber, .{&seen});
    t.join();
    try std.testing.expectEqual(bg, seen);
}

test "ctx: records carry labels and offsets, a slow statement is kept" {
    var qctx = fiber.Ctx.init("select 1");
    qctx.attach();
    try std.testing.expectEqual(&qctx, fiber.ctx());

    fiber.ctx().record("first", .{});
    fiber.sleepMs(3);
    fiber.ctx().record("n={d}", .{7});
    const recs = qctx.records.items;
    try std.testing.expectEqual(2, recs.len);
    try std.testing.expectEqualStrings("first", recs[0].label);
    try std.testing.expectEqualStrings("n=7", recs[1].label);
    try std.testing.expect(recs[1].at_us >= recs[0].at_us + 2_000);

    // Over 1 ms: the sampler keeps it, records included.
    qctx.detach();
    try std.testing.expectEqual(null, fiber.currentFiber().ctx);
    var body: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer body.deinit();
    try fiber.writeTraces(&body.writer, 0);
    const out = body.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "select 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "n=7") != null);
    // A higher floor than it took hides it.
    var none: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer none.deinit();
    try fiber.writeTraces(&none.writer, 100_000);
    const hidden = none.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, hidden, "select 1") == null);
}

fn recordHelper() void {
    fiber.ctx().record("helper", .{});
}

fn noteCtx(out: *std.atomic.Value(usize)) void {
    out.store(@intFromPtr(fiber.ctx()), .release);
}

test "ctx: a joined helper records into the statement, detached does not" {
    const bg = fiber.ctx();
    var qctx = fiber.Ctx.init("select 2");
    qctx.attach();
    defer qctx.detach();

    var h = try fiber.spawn(.{}, recordHelper, .{});
    h.join();
    try std.testing.expectEqual(1, qctx.records.items.len);
    try std.testing.expectEqualStrings("helper", qctx.records.items[0].label);

    var seen: std.atomic.Value(usize) = .init(0);
    try fiber.spawnDetached(.{}, noteCtx, .{&seen});
    while (seen.load(.acquire) == 0) fiber.sleepMs(1);
    try std.testing.expectEqual(@intFromPtr(bg), seen.load(.acquire));

    // An explicit context wins over inheritance.
    var other = fiber.Ctx.init("other");
    var h2 = try fiber.spawn(.{ .ctx = &other }, recordHelper, .{});
    h2.join();
    try std.testing.expectEqual(1, other.records.items.len);
    try std.testing.expectEqual(1, qctx.records.items.len);
    other.arena.deinit();
}

test "ctx: a bucket keeps the newest traces and drops the oldest" {
    // Nine statements into one bucket's ring of eight: the first is
    // gone, the last is there, whatever else the suite adds. The start
    // is backdated by 200 ms rather than slept through, so the bucket
    // ([100 ms, 500 ms)) does not depend on how loaded the box is.
    var names: [9][5]u8 = undefined;
    for (&names, 0..) |*name, i| {
        name.* = .{ 'r', 'i', 'n', 'g', '0' + @as(u8, @intCast(i)) };
        var qctx = fiber.Ctx.init(name);
        qctx.start_mono_us -= 200_000;
        qctx.attach();
        qctx.detach();
    }
    var body: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer body.deinit();
    try fiber.writeTraces(&body.writer, 0);
    const out = body.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "ring8") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "ring0") == null);
}
