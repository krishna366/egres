const std = @import("std");
const file_ids = @import("zdb").file_ids;

test "file_ids: encode/decode round-trip" {
    const fid = file_ids.encode(7, 1234);
    try std.testing.expectEqual(@as(u16, 7), file_ids.node(fid));
    try std.testing.expectEqual(@as(u64, 1234), file_ids.local(fid));
}

test "file_ids: allocate persists once per batch" {
    const Ctx = struct {
        persists: u32 = 0,
        last_max: u64 = 0,
        fn call(ctx: *anyopaque, new_max: u64) anyerror!void {
            const c: *@This() = @ptrCast(@alignCast(ctx));
            c.persists += 1;
            c.last_max = new_max;
        }
    };
    var ctx: Ctx = .{};
    var ids: file_ids.FileIds = .{};
    ids.bind(3, 0, @ptrCast(&ctx), Ctx.call);

    // First allocate → extends (next=0 catches up to lease_high=0).
    _ = try ids.allocate();
    try std.testing.expectEqual(@as(u32, 1), ctx.persists);
    try std.testing.expectEqual(file_ids.LEASE_BATCH, ctx.last_max);

    // Allocate up to LEASE_BATCH-1 more — lease_high not crossed.
    for (0..file_ids.LEASE_BATCH - 1) |_| _ = try ids.allocate();
    try std.testing.expectEqual(@as(u32, 1), ctx.persists);

    // The next one trips the next batch.
    _ = try ids.allocate();
    try std.testing.expectEqual(@as(u32, 2), ctx.persists);
    try std.testing.expectEqual(file_ids.LEASE_BATCH * 2, ctx.last_max);
}

test "file_ids: lease ceilings are aligned multiples of LEASE_BATCH" {
    const Ctx = struct {
        seen: std.ArrayList(u64) = .empty,
        fn call(ctx: *anyopaque, new_max: u64) anyerror!void {
            const c: *@This() = @ptrCast(@alignCast(ctx));
            try c.seen.append(std.testing.allocator, new_max);
        }
    };
    var ctx: Ctx = .{};
    defer ctx.seen.deinit(std.testing.allocator);
    var ids: file_ids.FileIds = .{};
    // Start at a non-aligned initial_max to confirm the first extend
    // realigns to a multiple of LEASE_BATCH.
    ids.bind(1, 17, @ptrCast(&ctx), Ctx.call);

    // Force several lease extensions.
    for (0..file_ids.LEASE_BATCH * 4) |_| _ = try ids.allocate();

    try std.testing.expect(ctx.seen.items.len >= 4);
    for (ctx.seen.items) |hi| {
        try std.testing.expect(hi % file_ids.LEASE_BATCH == 0);
    }
    // Successive ceilings step up by exactly one batch.
    for (1..ctx.seen.items.len) |i| {
        try std.testing.expectEqual(file_ids.LEASE_BATCH, ctx.seen.items[i] - ctx.seen.items[i - 1]);
    }
}

test "file_ids: allocated ids carry the node tag" {
    const Ctx = struct {
        fn call(_: *anyopaque, _: u64) anyerror!void {}
    };
    var stub: u8 = 0;
    var ids: file_ids.FileIds = .{};
    ids.bind(42, 0, @ptrCast(&stub), Ctx.call);
    const fid = try ids.allocate();
    try std.testing.expectEqual(@as(u16, 42), file_ids.node(fid));
    try std.testing.expectEqual(@as(u64, 0), file_ids.local(fid));
}
