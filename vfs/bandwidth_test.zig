const std = @import("std");
const testing = std.testing;
const bandwidth = @import("bandwidth.zig");
const vfs = @import("vfs.zig");
const Limiter = bandwidth.Limiter;
const page_size = vfs.page_size;

// ── 1. P0 never blocks ──────────────────────────────────────────────

test "P0 never blocks" {
    var limiter = Limiter.init(.{ .bytes_per_sec = 1024 * 1024 });
    const granted = limiter.acquireAligned(.p0, 8 * page_size, .read);
    try testing.expectEqual(@as(usize, 8 * page_size), granted);
}

// ── 2. P0 drains bucket ─────────────────────────────────────────────

test "P0 drains bucket" {
    var limiter = Limiter.init(.{ .bytes_per_sec = 64 * page_size });
    const granted = limiter.acquireAligned(.p0, 128 * page_size, .read);
    try testing.expectEqual(@as(usize, 128 * page_size), granted);
    try testing.expectEqual(@as(u64, 0), limiter.tokens.load(.monotonic));
}

// ── 3. Bucket never negative ────────────────────────────────────────

test "bucket never negative" {
    var limiter = Limiter.init(.{ .bytes_per_sec = page_size });
    _ = limiter.acquireAligned(.p0, 256 * page_size, .read);
    try testing.expectEqual(@as(u64, 0), limiter.tokens.load(.monotonic));
}

// ── 4. Token refill ─────────────────────────────────────────────────

test "token refill" {
    var limiter = Limiter.init(.{ .bytes_per_sec = 100_000_000 }); // 100 MB/s
    _ = limiter.acquireAligned(.p0, 200_000_000, .read);
    try testing.expectEqual(@as(u64, 0), limiter.tokens.load(.monotonic));

    std.Thread.sleep(50 * std.time.ns_per_ms);

    const granted = limiter.acquireAligned(.p1, 100_000_000, .read);
    try testing.expect(granted > 0);
    try testing.expect(granted % page_size == 0);
}

// ── 5. Write cost multiplier ────────────────────────────────────────

test "write cost multiplier" {
    var limiter = Limiter.init(.{ .bytes_per_sec = 64 * page_size, .write_cost = 8 });

    // Bucket starts at 64 pages. Write of 4 pages costs 4*8=32 pages of tokens.
    const granted = limiter.acquireAligned(.p0, 4 * page_size, .write);
    try testing.expectEqual(@as(usize, 4 * page_size), granted);
    // 64 - 32 = 32 pages of tokens left.
    try testing.expectEqual(@as(u64, 32 * page_size), limiter.tokens.load(.monotonic));
}

// ── 6. Partial grant ────────────────────────────────────────────────

test "partial grant" {
    // 128 pages of bandwidth.
    var limiter = Limiter.init(.{ .bytes_per_sec = 128 * page_size });
    // Drain most of the bucket with P0 — leaves 8 pages.
    _ = limiter.acquireAligned(.p0, 120 * page_size, .read);

    // P1 requesting 64 pages gets only what's left (~8 pages), page-aligned.
    const granted = limiter.acquireAligned(.p1, 64 * page_size, .read);
    try testing.expect(granted > 0);
    try testing.expect(granted <= 64 * page_size);
    try testing.expect(granted % page_size == 0);
}

// ── 7. Burst cap ────────────────────────────────────────────────────

test "burst cap" {
    var limiter = Limiter.init(.{ .bytes_per_sec = 32 * page_size });

    // Simulate long idle: push last_refill 60s back, drain tokens.
    limiter.last_refill.store(limiter.last_refill.load(.monotonic) - 60_000, .monotonic);
    limiter.tokens.store(0, .monotonic);

    // After long idle, refill caps at bytes_per_sec (32 pages), not 60x that.
    const granted = limiter.acquireAligned(.p1, 256 * page_size, .read);
    try testing.expect(granted <= 32 * page_size);
    try testing.expect(granted % page_size == 0);
}

// ── 8. P1 priority over P2 ─────────────────────────────────────────

test "P1 priority over P2" {
    var limiter = Limiter.init(.{ .bytes_per_sec = 1_000_000, .min_alloc = page_size });

    // Simulate P1 waiting.
    _ = limiter.waiting_p1.fetchAdd(1, .monotonic);
    defer _ = limiter.waiting_p1.fetchSub(1, .monotonic);

    // P2 yields because waiting_p1 > 0, hits timeout, gets minimum grant.
    const granted = limiter.acquireAligned(.p2, 64 * page_size, .read);
    try testing.expectEqual(@as(usize, page_size), granted);
}

// ── 9. Timeout minimum grant ────────────────────────────────────────

test "timeout minimum grant" {
    var limiter = Limiter.init(.{ .bytes_per_sec = 1, .min_alloc = 16 * page_size });
    _ = limiter.acquireAligned(.p0, page_size, .read);

    var start = std.time.milliTimestamp();
    var granted = limiter.acquireAligned(.p1, 64 * page_size, .read);
    var elapsed_ms = std.time.milliTimestamp() - start;

    try testing.expectEqual(@as(usize, 16 * page_size), granted);
    try testing.expect(elapsed_ms >= 90);

    start = elapsed_ms;
    granted = limiter.acquireAligned(.p1, 64 * page_size, .write);
    elapsed_ms = std.time.milliTimestamp() - start;

    try testing.expectEqual(@as(usize, 2 * page_size), granted);
    try testing.expect(elapsed_ms >= 90);
}

// ── 10. Grants are always page-aligned ──────────────────────────────

test "grants are page-aligned" {
    // Non-page-multiple rate and request sizes — grants must still be page-aligned.
    var limiter = Limiter.init(.{ .bytes_per_sec = 50_000 });

    const g1 = limiter.acquireAligned(.p0, 5_000, .read);
    try testing.expect(g1 % page_size == 0);

    const g2 = limiter.acquireAligned(.p1, 13_000, .read);
    try testing.expect(g2 % page_size == 0);

    const g3 = limiter.acquireAligned(.p0, 7_777, .write);
    try testing.expect(g3 % page_size == 0);
}
