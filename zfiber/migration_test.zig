// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");

// Isolates the coro layer under work-stealing MIGRATION. Each fiber repeatedly
// stamps a stack buffer + keeps several locals live, then `yield()`s — which
// re-queues it so a thief on a DIFFERENT OS thread can resume it — and verifies
// its own state survived the cross-thread context switch. No I/O, no zdb/pgwire:
// a failure here implicates coro `switchContext` / stack handling under
// migration (the thing zio's own single-thread-per-coro runtime never does),
// NOT the workload. `BIG:` so it only runs under --bigtest.
test "BIG: coro state survives work-stealing migration" {
    const N = 96;
    const ITERS = 4000;
    const Body = struct {
        fn run(seed: u64, failures: *std.atomic.Value(u64)) void {
            var buf: [512]u8 = undefined;
            // Locals the compiler must keep live across the yield — exercises the
            // callee-saved register spill/restore that switchContext relies on.
            var a: u64 = seed *% 0x9E3779B97F4A7C15;
            var b: u64 = seed ^ 0xDEADBEEFCAFEF00D;
            var c: u64 = ~seed;
            var iter: u64 = 0;
            while (iter < ITERS) : (iter += 1) {
                const pat: u8 = @truncate(seed ^ iter);
                for (&buf) |*p| p.* = pat;
                const ea = a +% iter;
                const eb = b ^ iter;
                const ec = c -% iter;
                a = ea;
                b = eb;
                c = ec;
                // Re-queue this fiber; under work-stealing a peer on another
                // thread may resume it — the whole point of the test.
                fiber.yield();
                // Did the stack buffer + the live locals survive the switch?
                for (buf) |p| {
                    if (p != pat) {
                        _ = failures.fetchAdd(1, .monotonic);
                        break;
                    }
                }
                if (a != ea or b != eb or c != ec) _ = failures.fetchAdd(1, .monotonic);
            }
        }
    };

    var failures = std.atomic.Value(u64).init(0);
    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(N);
    var i: u64 = 0;
    while (i < N) : (i += 1) try g.spawn(.{}, Body.run, .{ i, &failures });
    g.wait();
    try std.testing.expectEqual(@as(u64, 0), failures.load(.monotonic));
}
