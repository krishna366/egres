//! Monotonically-advancing u64 with targeted wait/wake.
//!
//! Built directly on `Spinlock` + a centralized `ArrayList(Entry)`
//! held by the Watermark itself. Walks on `advance` are
//! cache-coherent (contiguous array of 16-byte entries) rather
//! than chasing pointers across fiber stacks.
//!
//! `advance` walks the entries, swap-removes those whose target
//! is satisfied, and wakes only those fibers — higher-target
//! waiters stay parked. No spurious wake / re-park churn and no
//! needless cross-thread routing.
//!
//! Lost-wake race is closed by the spinlock + re-check pattern: a
//! waiter re-reads `value` and `closed` under `sl` before
//! appending its entry; the fiber's awaken-bit handles wake
//! arriving in the sl.unlock → park gap.

const std = @import("std");
const fb = @import("fiber.zig");
const rt = @import("runtime.zig");
const dd = @import("deadlines.zig");

const Fiber = fb.Fiber;
const Spinlock = rt.Spinlock;
const Allocator = std.mem.Allocator;

const Entry = struct {
    fiber: *Fiber,
    target: u64,
    /// Non-null for a timed wait. Lets `advance`/`close` claim the
    /// timeout's first-fire token before waking, so the deadline can't
    /// also act on a waiter that's already being woken (and vice-versa).
    timeout: ?*dd.Timeout = null,
};

/// Per-timed-wait callback context. Lives on the waiter's stack; the
/// deadline's `on_fire` (run on the owner worker) flips `timed_out` and
/// wakes the parked fiber, which then self-removes its entry and returns
/// `error.Timeout`. The "timed out" flag is the watermark's own state —
/// `Deadlines` knows nothing about it.
const TimeoutCtx = struct {
    fiber: *Fiber,
    timed_out: std.atomic.Value(bool) = .init(false),
};

fn timeoutFire(ptr: *anyopaque) void {
    const ctx: *TimeoutCtx = @ptrCast(@alignCast(ptr));
    // Release-store before the wake so the resumed waiter observes it;
    // touch nothing after `wake` — the fiber may free `ctx`'s frame at once.
    ctx.timed_out.store(true, .release);
    rt.wake(ctx.fiber);
}

pub const Watermark = struct {
    allocator: Allocator,
    value: std.atomic.Value(u64) = .init(0),
    closed: std.atomic.Value(bool) = .init(false),
    sl: Spinlock = .{},
    waiters: std.ArrayList(Entry) = .empty,

    pub fn init(allocator: Allocator) Watermark {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Watermark) void {
        self.waiters.deinit(self.allocator);
    }

    /// Lockless load. Cheap — safe on hot paths.
    pub fn load(self: *const Watermark) u64 {
        return self.value.load(.acquire);
    }

    /// Monotonic advance. No-op if `new_value <= current`. On a
    /// successful CAS, walks the waiter array and wakes exactly
    /// those whose target is now satisfied — higher-target
    /// waiters stay parked.
    pub fn advance(self: *Watermark, new_value: u64) void {
        var cur = self.value.load(.monotonic);
        while (new_value > cur) {
            if (self.value.cmpxchgWeak(cur, new_value, .release, .monotonic)) |actual| {
                cur = actual;
                continue;
            }
            self.wakeReached(new_value);
            return;
        }
    }

    /// Mark the watermark closed and wake every waiter with
    /// `error.Closed`. Idempotent. `advance` still works after
    /// close — `closed` only affects the wait paths.
    pub fn close(self: *Watermark) void {
        const was_closed = self.closed.swap(true, .release);
        if (!was_closed) self.wakeAll();
    }

    /// Block until `value >= target` or the watermark is closed.
    /// Returns `error.Closed` if `close()` was called or
    /// `error.OutOfMemory` if the waiter table can't grow.
    pub fn waitForUntimed(self: *Watermark, target: u64) error{ Closed, OutOfMemory }!void {
        self.waitForImpl(target, null) catch |err| switch (err) {
            error.Closed => return error.Closed,
            error.OutOfMemory => return error.OutOfMemory,
            // A null timeout never arms a deadline, so it can't time out.
            error.Timeout => unreachable,
        };
    }

    /// Like `waitForUntimed`, but gives up after `timeout_ms` with
    /// `error.Timeout`.
    pub fn waitFor(self: *Watermark, target: u64, timeout_ms: u64) error{ Closed, Timeout, OutOfMemory }!void {
        return self.waitForImpl(target, timeout_ms);
    }

    /// Shared wait loop. `timeout_ms == null` is the untimed wait (no
    /// `Deadlines` entry at all); a value arms a per-worker deadline that
    /// fires, sets `timed_out`, and wakes us. `advance`/`close` and the
    /// deadline arbitrate via the timeout's first-fire `claim()` so exactly
    /// one of them wakes us — the loser never touches our stack frame.
    fn waitForImpl(self: *Watermark, target: u64, timeout_ms: ?u64) error{ Closed, Timeout, OutOfMemory }!void {
        // Fast paths — skip arming a timer when the answer is already known.
        if (self.value.load(.acquire) >= target) return;
        if (self.closed.load(.acquire)) return error.Closed;

        const me = rt.currentFiber();
        var ctx: TimeoutCtx = .{ .fiber = me };
        var timeout: dd.Timeout = .{ .on_fire = &timeoutFire, .on_fire_ctx = &ctx };

        // Arm a deadline only for a timed wait. Capture the owning
        // Deadlines so the cancel targets the same heap even if we resume
        // on a different worker after migration (`cancel` is spinlock-safe
        // cross-thread).
        const owner: ?*dd.Deadlines = if (timeout_ms) |ms| blk: {
            const o = &rt.currentWorker().deadlines;
            try o.register(&timeout, ms);
            break :blk o;
        } else null;
        defer if (owner) |o| o.cancel(&timeout);

        while (true) {
            if (self.value.load(.acquire) >= target) return;
            if (self.closed.load(.acquire)) return error.Closed;
            if (ctx.timed_out.load(.acquire)) {
                // The deadline woke us and (unlike advance/close) left our
                // entry queued; pull it before the frame dies. Prefer a
                // satisfied/closed result if one landed alongside the timer.
                self.sl.lock();
                self.removeWaiter(me);
                self.sl.unlock();
                if (self.value.load(.acquire) >= target) return;
                if (self.closed.load(.acquire)) return error.Closed;
                return error.Timeout;
            }

            self.sl.lock();
            // Re-check under sl to close the race between predicate-load
            // and entry-insert.
            if (self.value.load(.acquire) >= target) {
                self.sl.unlock();
                return;
            }
            if (self.closed.load(.acquire)) {
                self.sl.unlock();
                return error.Closed;
            }
            if (ctx.timed_out.load(.acquire)) {
                self.sl.unlock();
                continue; // re-loop to hit the removal branch above
            }
            self.waiters.append(self.allocator, .{
                .fiber = me,
                .target = target,
                .timeout = if (timeout_ms != null) &timeout else null,
            }) catch |err| {
                self.sl.unlock();
                return err;
            };
            self.sl.unlock();
            rt.park();
            // On resume: a waker removed our entry (timed-out path
            // excepted). Loop and re-check predicate / closed / timed_out.
        }
    }

    /// Remove this fiber's entry from the waiter array if present. Caller
    /// holds `sl`. Used by the timed `waitFor` on the timeout path — the
    /// deadline wakes us but doesn't touch the array.
    fn removeWaiter(self: *Watermark, f: *Fiber) void {
        for (self.waiters.items, 0..) |e, i| {
            if (e.fiber == f) {
                _ = self.waiters.swapRemove(i);
                return;
            }
        }
    }

    /// Walk the waiter array, swap-removing entries whose target
    /// is at or below `new_value`. Collected fibers are linked
    /// via the transient `waker_next` field and woken after sl
    /// is released — cross-thread wake may push to remote_queue
    /// + issue an eventfd notify, neither of which belongs under
    /// our spinlock.
    fn wakeReached(self: *Watermark, new_value: u64) void {
        self.sl.lock();
        var to_wake: ?*Fiber = null;
        var i: usize = 0;
        while (i < self.waiters.items.len) {
            const e = self.waiters.items[i];
            if (e.target <= new_value) {
                // For a timed waiter, win its first-fire token before
                // waking. If the deadline already claimed it, leave the
                // entry — the deadline owns the wake and the waiter will
                // self-remove on resume.
                if (e.timeout) |t| {
                    if (!t.claim()) {
                        i += 1;
                        continue;
                    }
                }
                e.fiber.waker_next = to_wake;
                to_wake = e.fiber;
                _ = self.waiters.swapRemove(i);
                // Don't increment i — the entry now at slot i is
                // whatever the last element was; we still need
                // to check it.
            } else {
                i += 1;
            }
        }
        self.sl.unlock();

        while (to_wake) |f| {
            const next = f.waker_next;
            f.waker_next = null;
            rt.wake(f);
            to_wake = next;
        }
    }

    /// Wake every queued waiter (used by `close`). They'll see
    /// `closed == true` when they re-check and return `error.Closed`.
    /// Mirrors `wakeReached`'s first-fire claim: a timed waiter whose
    /// deadline already fired is left for that deadline to wake.
    fn wakeAll(self: *Watermark) void {
        self.sl.lock();
        var to_wake: ?*Fiber = null;
        var i: usize = 0;
        while (i < self.waiters.items.len) {
            const e = self.waiters.items[i];
            if (e.timeout) |t| {
                if (!t.claim()) {
                    i += 1;
                    continue;
                }
            }
            e.fiber.waker_next = to_wake;
            to_wake = e.fiber;
            _ = self.waiters.swapRemove(i);
        }
        self.sl.unlock();

        while (to_wake) |f| {
            const next = f.waker_next;
            f.waker_next = null;
            rt.wake(f);
            to_wake = next;
        }
    }
};
