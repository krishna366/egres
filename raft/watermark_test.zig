//! Tests for `raft.Watermark` — the monotonic u64 progress marker used
//! by Node for commit_index / applied_ts. Covers fast-path loads, the
//! mu+cv lost-wakeup-safe wait/signal pair, monotonicity, timeouts, and
//! `close()`-driven shutdown.
//!
//! The unified test runner already runs each test as a zio fiber, so
//! these test bodies use `zio.yield` / `zio.sleep` / `zio.group()`
//! directly — no per-test Runtime needed.

const std = @import("std");
const zio = @import("zio");
const raft = @import("raft");
const Watermark = raft.Watermark;

test "load: fresh watermark reads zero without locking" {
    var wm: Watermark = .{};
    try std.testing.expectEqual(@as(u64, 0), wm.load());
}

test "advance: monotonic — larger values stick, smaller are no-ops" {
    var wm: Watermark = .{};
    wm.advance(5);
    try std.testing.expectEqual(@as(u64, 5), wm.load());
    // Equal: no-op (broadcast would be wasted; visible state unchanged).
    wm.advance(5);
    try std.testing.expectEqual(@as(u64, 5), wm.load());
    // Smaller: discarded outright.
    wm.advance(3);
    try std.testing.expectEqual(@as(u64, 5), wm.load());
    // Larger: takes effect.
    wm.advance(9);
    try std.testing.expectEqual(@as(u64, 9), wm.load());
}

test "waitForUntimed: target already reached returns immediately" {
    var wm: Watermark = .{};
    wm.advance(10);
    try wm.waitForUntimed(10);
    try wm.waitForUntimed(7);
    // target=0 is the trivial case — always satisfied.
    try wm.waitForUntimed(0);
}

test "waitForUntimed: blocks until advance crosses target" {
    const Ctx = struct {
        wm: Watermark = .{},
        woke: std.atomic.Value(bool) = .init(false),

        fn waiter(self: *@This()) !void {
            try self.wm.waitForUntimed(42);
            self.woke.store(true, .release);
        }

        fn advancer(self: *@This()) !void {
            // Give the waiter a couple of ticks to actually park on cv.
            for (0..16) |_| try zio.yield();
            try std.testing.expect(!self.woke.load(.acquire));
            // Below target — must not wake.
            self.wm.advance(40);
            for (0..16) |_| try zio.yield();
            try std.testing.expect(!self.woke.load(.acquire));
            // At/above target — must wake.
            self.wm.advance(42);
        }
    };

    var ctx = Ctx{};
    var grp = zio.group();
    defer grp.cancel();
    try grp.spawn(Ctx.waiter, .{&ctx});
    try grp.spawn(Ctx.advancer, .{&ctx});
    try grp.wait();
    try std.testing.expect(!grp.hasFailed());
    try std.testing.expect(ctx.woke.load(.acquire));
}

test "waitForUntimed: broadcast wakes every waiter on a single advance" {
    const Ctx = struct {
        wm: Watermark = .{},
        woke: std.atomic.Value(u32) = .init(0),

        fn waiter(self: *@This()) !void {
            try self.wm.waitForUntimed(7);
            _ = self.woke.fetchAdd(1, .release);
        }

        fn advancer(self: *@This()) !void {
            // Let all three waiters reach cv.wait before we publish.
            for (0..32) |_| try zio.yield();
            self.wm.advance(7);
        }
    };

    var ctx = Ctx{};
    var grp = zio.group();
    defer grp.cancel();
    try grp.spawn(Ctx.waiter, .{&ctx});
    try grp.spawn(Ctx.waiter, .{&ctx});
    try grp.spawn(Ctx.waiter, .{&ctx});
    try grp.spawn(Ctx.advancer, .{&ctx});
    try grp.wait();
    try std.testing.expect(!grp.hasFailed());
    try std.testing.expectEqual(@as(u32, 3), ctx.woke.load(.acquire));
}

test "waitFor: returns Timeout when target never reached" {
    var wm: Watermark = .{};
    const err = wm.waitFor(100, .fromMilliseconds(20));
    try std.testing.expectError(error.Timeout, err);
    // State must be untouched after timeout.
    try std.testing.expectEqual(@as(u64, 0), wm.load());
}

test "waitFor: returns successfully when advance crosses target before deadline" {
    const Ctx = struct {
        wm: Watermark = .{},

        fn waiter(self: *@This()) !void {
            try self.wm.waitFor(3, .fromSeconds(5));
        }

        fn advancer(self: *@This()) !void {
            try zio.sleep(.fromMilliseconds(5));
            self.wm.advance(3);
        }
    };

    var ctx = Ctx{};
    var grp = zio.group();
    defer grp.cancel();
    try grp.spawn(Ctx.waiter, .{&ctx});
    try grp.spawn(Ctx.advancer, .{&ctx});
    try grp.wait();
    try std.testing.expect(!grp.hasFailed());
}

test "close: wakes pending waiters with error.Closed" {
    const Ctx = struct {
        wm: Watermark = .{},
        result_untimed: std.atomic.Value(i32) = .init(0),
        result_timed: std.atomic.Value(i32) = .init(0),

        // 1 = expected error.Closed; 2 = unexpected success; 3 = other err
        fn untimed(self: *@This()) !void {
            if (self.wm.waitForUntimed(1_000_000)) |_| {
                self.result_untimed.store(2, .release);
            } else |err| switch (err) {
                error.Closed => self.result_untimed.store(1, .release),
                else => self.result_untimed.store(3, .release),
            }
        }

        fn timed(self: *@This()) !void {
            if (self.wm.waitFor(1_000_000, .fromSeconds(5))) |_| {
                self.result_timed.store(2, .release);
            } else |err| switch (err) {
                error.Closed => self.result_timed.store(1, .release),
                else => self.result_timed.store(3, .release),
            }
        }

        fn closer(self: *@This()) !void {
            // Let both waiters park before closing.
            for (0..32) |_| try zio.yield();
            self.wm.close();
        }
    };

    var ctx = Ctx{};
    var grp = zio.group();
    defer grp.cancel();
    try grp.spawn(Ctx.untimed, .{&ctx});
    try grp.spawn(Ctx.timed, .{&ctx});
    try grp.spawn(Ctx.closer, .{&ctx});
    try grp.wait();
    try std.testing.expect(!grp.hasFailed());
    try std.testing.expectEqual(@as(i32, 1), ctx.result_untimed.load(.acquire));
    try std.testing.expectEqual(@as(i32, 1), ctx.result_timed.load(.acquire));
}

test "close: idempotent — second call is a no-op" {
    var wm: Watermark = .{};
    wm.close();
    wm.close(); // must not deadlock / re-broadcast spuriously
    try std.testing.expectError(error.Closed, wm.waitForUntimed(1));
}

test "close after advance: target-met waiters succeed, beyond-target see Closed" {
    // Watermark closes don't roll back: a waiter whose target has already
    // been satisfied returns Ok even after close(), because the value
    // check happens before the `closed` check in the wait loop. Only
    // waiters still parked observe `error.Closed`.
    const Ctx = struct {
        wm: Watermark = .{},

        fn satisfied(self: *@This()) !void {
            // target=5, advance already happened — should NOT see Closed.
            try self.wm.waitForUntimed(5);
        }

        fn beyond(self: *@This()) !void {
            try std.testing.expectError(
                error.Closed,
                self.wm.waitForUntimed(100),
            );
        }

        fn driver(self: *@This()) !void {
            self.wm.advance(5);
            for (0..16) |_| try zio.yield();
            self.wm.close();
        }
    };

    var ctx = Ctx{};
    var grp = zio.group();
    defer grp.cancel();
    try grp.spawn(Ctx.satisfied, .{&ctx});
    try grp.spawn(Ctx.beyond, .{&ctx});
    try grp.spawn(Ctx.driver, .{&ctx});
    try grp.wait();
    try std.testing.expect(!grp.hasFailed());
}

// Regression: the mu+cv pair must prevent the lost-wakeup race that
// would otherwise plague a naive `load(); if (<target) park;` pattern.
// We hammer many small advances against many small waiters; if any
// broadcast were dropped, at least one waiter would block forever and
// the group wait would never return.
test "waitForUntimed: stress — no waiter is lost under interleaved advances" {
    const Ctx = struct {
        wm: Watermark = .{},
        finished: std.atomic.Value(u32) = .init(0),

        fn waiter(self: *@This(), target: u64) !void {
            try self.wm.waitForUntimed(target);
            _ = self.finished.fetchAdd(1, .release);
        }

        fn advancer(self: *@This()) !void {
            var v: u64 = 0;
            while (v < 20) : (v += 1) {
                self.wm.advance(v + 1);
                try zio.yield();
            }
        }
    };

    var ctx = Ctx{};
    var grp = zio.group();
    defer grp.cancel();
    // One waiter per integer target in [1, 20]. Some are spawned before,
    // some interleaved with, the advancer's loop.
    var i: u64 = 1;
    while (i <= 10) : (i += 1) {
        try grp.spawn(Ctx.waiter, .{ &ctx, i });
    }
    try grp.spawn(Ctx.advancer, .{&ctx});
    while (i <= 20) : (i += 1) {
        try grp.spawn(Ctx.waiter, .{ &ctx, i });
    }
    try grp.wait();
    try std.testing.expect(!grp.hasFailed());
    try std.testing.expectEqual(@as(u32, 20), ctx.finished.load(.acquire));
    try std.testing.expectEqual(@as(u64, 20), ctx.wm.load());
}
