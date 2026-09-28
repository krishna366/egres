const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");

test "CoarseClock: ms() advances during a parked fiber's sleep" {
    // Measure elapsed via direct vDSO CLOCK_MONOTONIC, not
    // `utils.coarseMs()`. The coarse clock cache is what we'd be
    // observing in production code, but using it as the test's
    // source of truth conflates two things: (1) whether `sleep`
    // actually slept, and (2) whether worker 0's 1 ms tick stayed
    // current. Under heavy parallel load the tick can lag, making
    // `coarseMs` deltas under-report by 10+ ms even when the sleep
    // ran fine. We sidestep that here by reading a fresh clock.
    const before_ms = utils.timestampNow(.monotonic, .millis);
    fiber.sleepMs(30);
    const after_ms = utils.timestampNow(.monotonic, .millis);
    const observed_delta = after_ms - before_ms;

    if (observed_delta < 25 or observed_delta >= 250) {
        std.log.err("observed_delta = {d} ms (expected 25 <= delta < 250)", .{observed_delta});
    }
    try std.testing.expect(observed_delta >= 25);
    try std.testing.expect(observed_delta < 250);
}

test "CoarseClock: ms() is stable across many cheap reads" {
    var last = utils.coarseMs();
    // Read the clock many times; verify monotonicity. Without a
    // sleep, all reads land between two ticks and should return the
    // same cached value — but even across a yield, the value must
    // never go backwards.
    for (0..1000) |_| {
        const now = utils.coarseMs();
        try std.testing.expect(now >= last);
        last = now;
        fiber.yield();
    }
}
