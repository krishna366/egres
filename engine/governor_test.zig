// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const engine = @import("engine");
const zdb = @import("zdb");

const Governor = engine.governor.Governor;
const gib: u64 = 1 << 30;

test "governor: the margin rises to what the rest holds at once" {
    var cache = try zdb.cache.ShardedCache(u128).init(
        std.testing.allocator,
        1 << 20,
        1,
    );
    defer cache.deinit();
    var gov = Governor.init(48 * gib, &cache);

    // Little held elsewhere: the margin stays at its floor.
    const floor_cap = 48 * gib - Governor.min_margin;
    try std.testing.expectEqual(floor_cap, gov.nextCap(100 << 20));

    // 20 GiB held elsewhere: the margin is 20 GiB this tick.
    try std.testing.expectEqual(28 * gib, gov.nextCap(20 * gib));
    try std.testing.expectEqual(20 * gib, gov.margin);

    // Everything held elsewhere: the floor.
    try std.testing.expectEqual(Governor.min_cap, gov.nextCap(70 * gib));
}

test "governor: the margin falls back over minutes" {
    var cache = try zdb.cache.ShardedCache(u128).init(
        std.testing.allocator,
        1 << 20,
        1,
    );
    defer cache.deinit();
    var gov = Governor.init(48 * gib, &cache);
    _ = gov.nextCap(20 * gib);

    // Back to 4 GiB held: one tick closes a 600th of the 16 GiB gap.
    const cap = gov.nextCap(4 * gib);
    try std.testing.expectEqual(20 * gib - 16 * gib / 600, gov.margin);
    try std.testing.expectEqual(48 * gib - gov.margin, cap);

    // Ten minutes of ticks: within 16 MiB of what is held, never under.
    for (0..6000) |_| _ = gov.nextCap(4 * gib);
    try std.testing.expect(gov.margin >= 4 * gib);
    try std.testing.expect(gov.margin - 4 * gib < 16 << 20);
}
