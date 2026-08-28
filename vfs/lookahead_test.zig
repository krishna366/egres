const std = @import("std");
const testing = std.testing;
const lookahead = @import("vfs").lookahead;

// Synthesize per-trial samples that obey latency(s) = a + s/bw, with
// noise, and check the fit recovers a×bw.
fn synth(comptime sizes: []const u64, a_ns: u64, bw: u64, jitter_ns: []const u64) [sizes.len * 3]lookahead.Probe {
    var out: [sizes.len * 3]lookahead.Probe = undefined;
    var i: usize = 0;
    for (sizes) |sz| {
        for (0..3) |t| {
            const j = jitter_ns[(i + t) % jitter_ns.len];
            const xfer = sz * std.time.ns_per_s / bw;
            out[i] = .{ .size = sz, .ttfb_ns = a_ns + j, .total_ns = a_ns + j + xfer };
            i += 1;
        }
    }
    return out;
}

test "lookahead: pickLookahead fits a + s/bw and returns a*bw" {
    const sizes = [_]u64{ 4 << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20, 4 << 20, 16 << 20 };
    // a ≈ 100ms (plus jitter, median ~103ms), bw = 40 MB/s → a*bw ≈ 4.1 MB,
    // rounded to a 64 KiB multiple — assert the neighborhood, not the
    // exact granule.
    const jitter = [_]u64{ 0, 3_000_000, 9_000_000 };
    const trials = synth(&sizes, 100_000_000, 40_000_000, &jitter);
    const la = try lookahead.pickLookahead(testing.allocator, &trials);
    try testing.expect(la >= 3_900_000 and la <= 4_400_000);
    try testing.expectEqual(@as(u64, 0), la % (64 << 10));

    // Slow bandwidth: a*bw ≈ 10.3 KB — below 64 KiB the granule drops to
    // a page, so the answer stays honest instead of rounding to zero.
    const slow = synth(&sizes, 100_000_000, 100_000, &jitter);
    try testing.expectEqual(@as(u64, 12 << 10), try lookahead.pickLookahead(testing.allocator, &slow));

    // Blazing local store (10 TB/s): even 16 MiB transfers in ~1.7us,
    // never transfer-dominated → the largest probed size wins.
    const fast = synth(&sizes, 50_000, 10_000_000_000_000, &.{0});
    try testing.expectEqual(@as(u64, 16 << 20), try lookahead.pickLookahead(testing.allocator, &fast));

    // a*bw below the smallest probed size clamps to the floor.
    const tiny = synth(&sizes, 1_000_000, 100_000, &.{0}); // 100 bytes
    try testing.expectEqual(@as(u64, 4 << 10), try lookahead.pickLookahead(testing.allocator, &tiny));

    try testing.expectEqual(@as(u64, 0), try lookahead.pickLookahead(testing.allocator, &.{}));
}

test "lookahead: probe pattern verifies at any offset" {
    var buf: [4096]u8 = undefined;
    const off: u64 = 1234567;
    for (&buf, 0..) |*b, i| b.* = lookahead.probeByte(off + i);
    try testing.expect(lookahead.verifyProbeData(off, &buf));
    // Wrong offset or a flipped byte must fail.
    try testing.expect(!lookahead.verifyProbeData(off + 1, &buf));
    buf[100] +%= 1;
    try testing.expect(!lookahead.verifyProbeData(off, &buf));
}
