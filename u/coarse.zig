//! Cached ms timestamp; one atomic load (~1 ns) vs ~20 ns for
//! `std.time.milliTimestamp`. Use on any hot path that just needs
//! "what time is it, roughly."
//!
//! Stale by up to `tick_interval` (1 ms). Monotonic: NTP slews can't
//! pull the cache back — durations read as 0 during a backwards-jump
//! freeze, never negative.
//!
//! Precondition: `start()` must run once at process init from inside
//! a zio runtime, BEFORE any `ms()` reader. `ms()` returns 0 (Unix
//! epoch) until `start()` seeds the cache — calling early is a bug,
//! not a soft-fallback case.

const std = @import("std");
const zio = @import("zio");
const context = @import("context.zig");

const tick_interval = zio.Duration.fromMilliseconds(1);

inline fn nowMs() i64 {
    return std.Io.Timestamp.now(context.g().io, .real).toMilliseconds();
}

pub const Clock = struct {
    // Own cache line: `cached` is read by every TrackedMutex acquire
    // across all cores. A neighboring global getting written would
    // invalidate the line on every reader's L1.
    var cached: std.atomic.Value(i64) align(std.atomic.cache_line) = .init(0);
    var started: std.atomic.Value(bool) = .init(false);
    var stopping: std.atomic.Value(bool) = .init(false);
    var tick_handle: ?zio.JoinHandle(void) = null;

    /// Cached ms-since-epoch. Precondition: `start()` was called.
    /// `inline` so the call site is just the atomic load — no frame.
    pub inline fn ms() i64 {
        return cached.load(.monotonic);
    }

    /// Idempotent: spawn the ticker fiber. Seeds the cache so the
    /// next `ms()` returns a real value even before the first tick.
    pub fn start() !void {
        if (started.swap(true, .monotonic)) return;
        cached.store(nowMs(), .monotonic);
        stopping.store(false, .monotonic);
        tick_handle = try zio.spawn(tickLoop, .{});
    }

    /// Signal the ticker fiber to exit and join it. Intended for tests
    /// / orderly shutdown — production lets the process exit kill the
    /// fiber. Idempotent: no-op if `start()` was never called.
    pub fn stop() void {
        if (!started.swap(false, .monotonic)) return;
        stopping.store(true, .release);
        if (tick_handle) |*h| {
            h.join();
            tick_handle = null;
        }
    }

    fn tickLoop() void {
        while (!stopping.load(.acquire)) {
            // Monotonicity guard: only advance, never regress. Single
            // writer, so load+conditional-store suffices (no CAS).
            const fresh = nowMs();
            if (fresh > cached.load(.monotonic)) {
                cached.store(fresh, .monotonic);
            }
            zio.sleep(tick_interval) catch return;
        }
    }
};
