//! Process-wide coarse wall-clock. A single atomic `cached_ms` that any
//! fiber on any worker reads via `ms()` (one acquire load, syscall-free
//! on the hot path).
//!
//! Who advances it: EVERY worker, from the top of its run loop, gated
//! to ~once per millisecond by a per-worker TSC check (see
//! `Worker.run`). Two guards keep this cheap regardless of worker count:
//!   - the per-worker TSC gate turns "every loop" into "once per ms", so
//!     the `clock_gettime` vDSO + atomic happen ~once/ms/worker, not per
//!     dispatch;
//!   - the `if (last < now)` write-guard in `update` means only the
//!     first worker past a millisecond boundary actually stores — every
//!     other worker that ms reads `last == now` and does nothing but a
//!     load. So the read-hot `cached_ms` line is invalidated ~once/ms
//!     TOTAL, not once per worker.
//!
//! No `xev.Timer`. The clock is driven by work, not by a wall-time
//! interrupt: as long as ANY worker is dispatching fibers it stays
//! fresh. When EVERY worker is idle (all parked in `loop.run(.once)`)
//! the clock freezes — which is benign, because a reader only exists on
//! a running fiber, and the first worker to wake runs the top-of-loop
//! gated `update` before it dispatches anything, so no fiber ever
//! observes the stale value. Dropping the timer also lets idle workers
//! deep-sleep instead of being kicked awake every millisecond.
//!
//! Epoch is wall-clock realtime (CLOCK_REALTIME): `ms()` is a true
//! Unix-epoch millisecond, so consumers that log or persist "when did
//! this happen" get a real timestamp. The `last < now` write-guard
//! doubles as regression protection — a backward NTP step pauses the
//! cache until wall-clock catches back up, so reads never go backwards.
//! (Deadline math lives in `Deadlines`, which reads CLOCK_MONOTONIC
//! directly — it must not ride a clock that NTP can step.)
//!
//! Per-worker deadline heaps for I/O timeouts live in `Deadlines`
//! (`fiber/deadlines.zig`), not here — cancellation has to run on the
//! loop that owns the op, which forces a per-worker design.
//!
//! Staleness contract: `ms()` is at most ~1 ms stale while the runtime
//! is doing any work at all. A CPU-bound fiber that never yields stalls
//! only ITS worker's contribution; peers keep the clock current. The
//! clock only stops when the whole runtime is idle.

const std = @import("std");
const _context = @import("context.zig");

pub const ClockKind = enum { monotonic, realtime };
pub const TimeUnit = enum { millis, micros };

/// The current time on `kind`'s clock, in `unit`, via `std.Io.Timestamp.now`
/// on the process io (`context.g().io`) — the same time source the rest of
/// the codebase uses. `context.g()` asserts the process context was
/// initialized; every caller here (CoarseClock, Deadlines, the tests) runs
/// inside a started runtime, which is well after `context.initGlobal`.
///
/// A full clock read, uncached — unlike `ms()`, which can be up to ~1 ms
/// stale. Fine in init/calibration paths, once per tick, per timeout
/// registration or per timeline mark; the hot-path CPU accounting in
/// `Worker.run` sticks to `rdtsc`.
pub fn timestampNow(comptime kind: ClockKind, comptime unit: TimeUnit) i64 {
    // std.Io.Clock spells monotonic as `.awake` (Linux CLOCK_MONOTONIC,
    // macOS CLOCK_UPTIME_RAW); `.real` is wall-clock Unix time.
    const ts = std.Io.Timestamp.now(_context.g().io, switch (kind) {
        .monotonic => .awake,
        .realtime => .real,
    });
    return switch (unit) {
        .millis => ts.toMilliseconds(),
        .micros => ts.toMicroseconds(),
    };
}

pub const CoarseClock = struct {
    /// Process-wide coarse-clock cache. Many writers (any worker's
    /// gated `update`, but only the ms-boundary-crosser actually
    /// stores), many readers (any fiber on any worker via `ms()`). Own
    /// cache line so reader fan-out doesn't invalidate neighboring
    /// fields on a writing worker.
    cached_ms: std.atomic.Value(i64) align(std.atomic.cache_line) = .init(0),

    /// Seed the cache so `ms()` is valid the instant `Runtime.init`
    /// returns — before any worker thread starts ticking it.
    pub fn init() CoarseClock {
        return .{ .cached_ms = .init(timestampNow(.realtime, .millis)) };
    }

    pub fn deinit(self: *CoarseClock) void {
        _ = self;
    }

    /// Publish the current wall-clock millisecond if it's newer than the
    /// cached value. Called by every worker, gated to ~once/ms. The
    /// `last < now` check makes all but the first writer each ms a plain
    /// load (no store, no invalidation); the surviving CAS is best
    /// effort — if a peer won the boundary, ours no-ops harmlessly.
    pub fn update(self: *CoarseClock) void {
        const now = timestampNow(.realtime, .millis);
        const last = self.cached_ms.load(.monotonic);
        if (last < now) {
            _ = self.cached_ms.cmpxchgWeak(last, now, .acquire, .monotonic);
        }
    }

    /// Coarse wall-clock milliseconds since the Unix epoch. One atomic
    /// load — cheap on every call, safe from any worker thread.
    /// Stale by up to ~1 ms while the runtime is busy; never torn.
    pub inline fn ms(self: *const CoarseClock) i64 {
        return self.cached_ms.load(.acquire);
    }
};
