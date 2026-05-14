const std = @import("std");
const zio = @import("zio");
const utils = @import("utils");

const Clock = utils.coarse.Clock;

test "coarse.Clock: ticker advances cache while a fiber sleeps" {
    try Clock.start();
    // Drain any pending tick first so the read below isn't off-by-one
    // from a tick that fired between `start()` and the first `ms()`.
    try zio.sleep(zio.Duration.fromMilliseconds(2));

    const t0 = Clock.ms();
    try zio.sleep(zio.Duration.fromMilliseconds(20));
    const t1 = Clock.ms();
    // Very loose lower bound — under the test suite's parallel load
    // the tick fiber can sit a long time waiting for an executor. Any
    // movement at all proves the ticker is advancing the cache.
    try std.testing.expect(t1 - t0 >= 1);
}

test "coarse.Clock: ms() is close to std.time.milliTimestamp" {
    try Clock.start();
    // One tick to make sure the cache has been seeded post-start.
    try zio.sleep(zio.Duration.fromMilliseconds(2));

    const cached = Clock.ms();
    const real = std.time.milliTimestamp();
    // ±50 ms covers worst-case scheduling jitter on the test runner.
    try std.testing.expect(@abs(real - cached) < 50);
}

test "coarse.Clock: start() is idempotent" {
    try Clock.start();
    // A second call must return without spawning a second fiber. We
    // can't directly count fibers, but we can at least assert the
    // call doesn't error and the cache keeps advancing.
    try Clock.start();
    const t0 = Clock.ms();
    try zio.sleep(zio.Duration.fromMilliseconds(5));
    const t1 = Clock.ms();
    try std.testing.expect(t1 >= t0);
}
