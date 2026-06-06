//! Per-worker timeout machinery: a flat array of pending deadlines plus one
//! `xev.Timer` re-armed dynamically to the earliest deadline.
//!
//! Each `Worker` owns one `Deadlines` instance. Timeouts registered on worker N
//! live in worker N's array, and the xev `timer` is only ever armed/reset on
//! worker N's thread — libxev per-loop state is not thread-safe, so
//! `register`/`drainExpired`/`onTimer`/`armForHead` stay owner-thread-only.
//!
//! `cancel`, however, IS safe to call from any worker: a fiber that registered a
//! timeout here can migrate and complete on a different worker, then cancel from
//! there. The array is guarded by `sl` (a spinlock) on every access — the only
//! cross-thread mutation is `cancel`'s removal; the timer SQE ops stay off the
//! lock and on the owner thread.

const std = @import("std");
const xev = @import("xev");
const heap = @import("heap.zig");
const coarse = @import("coarse_clock.zig");
// `sleepMs` (bottom of file) is the one fiber-aware helper here, so we import
// the runtime for park/wake. The `Deadlines` struct itself stays oblivious to
// fibers — this is a mutual file import, which Zig resolves lazily (runtime.zig
// imports us back for the `Deadlines` type).
const rt = @import("runtime.zig");
const fb = @import("fiber.zig");
const Fiber = fb.Fiber;

const Spinlock = @import("spinlock.zig").Spinlock;
const Allocator = std.mem.Allocator;

/// Minimum honored timeout. Requests below this are clamped up. The
/// clock cache that consumers measure against has 1 ms granularity,
/// so sub-ms timeouts would either round to zero or fire immediately
/// — neither useful. Pin the floor at the boundary instead.
pub const min_timeout_ms: u64 = 1;

/// The deadline heap entry. Each `Fiber` OWNS one (`Fiber.timeout`) so a fire
/// that arrives after its op exited dereferences stable fiber memory and can
/// only wake that one fiber — never an unrelated one via a reused stack frame.
/// Definition lives in `fiber.zig` (the fiber owns it); see `Timeout` there.
/// When the deadline fires (before `cancel`), the owner worker invokes
/// `on_fire(on_fire_ctx)` exactly once (gated by `_done`).
pub const Timeout = fb.Timeout;

pub const Deadlines = struct {
    loop: *xev.Loop,
    timer: xev.Timer,
    /// Backs the `timeouts` array — grown by `register`, freed by `deinit`.
    /// Captured at init (the runtime's allocator) so callers never pass one and
    /// can't mismatch grow vs. free.
    allocator: Allocator,
    /// SQE backing the in-flight timer expiration.
    completion: xev.Completion = undefined,
    /// SQE backing the cancellation half of an `xev.Timer.reset` call.
    /// Required by `reset` because the old SQE has to be canceled
    /// independently of the new SQE being submitted.
    cancel_completion: xev.Completion = undefined,
    /// True while a timer SQE is in flight (between `arm`/`reset` and
    /// the matching callback returning `.disarm`). Decides whether
    /// `armForHead` should call `timer.run` (fresh) or `timer.reset`
    /// (cancel-then-re-arm on the same completion).
    armed: bool = false,
    /// Pending timeouts as a flat array of pointers — NOT the `MinHeap`. We scan
    /// it (O(N), N tiny) for the min deadline and for removal. Removal matches by
    /// pointer identity.
    timeouts: std.ArrayList(*Timeout) = .empty,
    /// Guards every access to `timeouts`. The only cross-thread caller is
    /// `cancel` (a migrated waiter removing its own timeout); the owner thread
    /// holds it for the brief mutations in `register`/`drainExpired`/`armForHead`.
    /// Uncontended on the hot path.
    sl: Spinlock = .{},

    pub fn init(loop: *xev.Loop, allocator: Allocator) !Deadlines {
        return .{
            .loop = loop,
            .timer = try xev.Timer.init(),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Deadlines) void {
        self.timeouts.deinit(self.allocator);
        self.timer.deinit();
    }

    /// Earliest pending deadline, or null if none. Caller holds `sl`.
    fn minDeadlineLocked(self: *const Deadlines) ?i64 {
        var m: ?i64 = null;
        for (self.timeouts.items) |e| {
            if (m == null or e._deadline_ms < m.?) m = e._deadline_ms;
        }
        return m;
    }

    /// Register a deadline. `ms_from_now` is clamped up to `min_timeout_ms`.
    /// Appends `t` to the `timeouts` array (the caller owns `t`'s storage).
    ///
    /// "Now" is read via `clockMsUseRarely(.monotonic)` — direct vDSO
    /// CLOCK_MONOTONIC, no caching. Using `loop.now()` would hand us
    /// libxev's cached value, which can be arbitrarily stale between
    /// `loop.run` iterations under heavy worker load; that stale
    /// baseline could let `onTimer`'s drainExpired see both a short and
    /// a long deadline "expired" in one sweep when only the short fired.
    ///
    /// Re-arms the xev timer iff this entry becomes the new earliest
    /// deadline. If the heap was non-empty and the existing head
    /// expires sooner, the in-flight timer is left alone — it'll fire
    /// first and the callback's re-arm will pick up the next head.
    pub fn register(
        self: *Deadlines,
        t: *Timeout,
        ms_from_now: u64,
    ) !void {
        const current_ms = coarse.clockMsUseRarely(.monotonic);
        const ms = @max(ms_from_now, min_timeout_ms);
        t._deadline_ms = current_ms + @as(i64, @intCast(ms));

        // Snapshot the min BEFORE append so we know whether the new entry is the
        // new earliest. A tie is *not* a new minimum — the existing timer fires
        // at the same moment and drainExpired sweeps both.
        self.sl.lock();
        if (std.debug.runtime_safety) {
            // Leak tripwire: a `Timeout` must never be registered twice (a
            // missing cancel). With the array this is an O(N) scan, debug-only.
            for (self.timeouts.items) |e| if (e == t) std.debug.panic(
                "deadline register: timeout 0x{x} already registered (missing cancel)",
                .{@intFromPtr(t)},
            );
        }
        const prev_min = self.minDeadlineLocked();
        const becomes_head = prev_min == null or t._deadline_ms < prev_min.?;
        self.timeouts.append(self.allocator, t) catch |err| {
            self.sl.unlock();
            return err;
        };
        self.sl.unlock();

        // Arm outside the lock — `register` only ever runs on the owner
        // thread, so the timer SQE op is safe; keeping it off `sl` avoids
        // pinning a cross-thread `cancel` behind a libxev call.
        if (becomes_head) self.resetTimer(current_ms);
    }

    /// O(N) removal by POINTER IDENTITY. Safe even when `t` isn't enqueued
    /// (no-op). Hot path: called from an I/O completion / wait exit when the op
    /// finished before the deadline fired.
    ///
    /// The reason we put spinlock here is to allow for cancel call by fibers
    /// who have moved away to another worker.
    ///
    /// Doesn't reset the timer even when removing the current earliest — the
    /// in-flight SQE fires harmlessly (drainExpired finds the new min still in
    /// the future and bails). One spurious timer tick; no extra reset SQE per
    /// op-completes-first case (the common path).
    pub fn cancel(self: *Deadlines, t: *Timeout) void {
        self.sl.lock();
        defer self.sl.unlock();
        for (self.timeouts.items, 0..) |e, i| {
            if (e == t) {
                _ = self.timeouts.swapRemove(i);
                return;
            }
        }
    }

    /// Fire every entry whose deadline has passed. Owner-thread only.
    ///
    /// Drains the whole expired prefix under a SINGLE `sl` acquisition,
    /// threading the timeouts we claim onto a local fire list, then
    /// invokes their callbacks after unlock. Two reasons not to fire under
    /// the lock: a callback may `wake` (which can route cross-thread —
    /// remote queue + eventfd notify), and a concurrent `cancel` would
    /// otherwise contend on every single expiry instead of once per tick.
    pub fn drainExpired(self: *Deadlines, now: i64) void {
        var fire_list: ?*Timeout = null;
        self.sl.lock();
        var i: usize = 0;
        while (i < self.timeouts.items.len) {
            const t = self.timeouts.items[i];
            if (t._deadline_ms <= now) {
                // swap-remove: the last entry moves into slot i, so DON'T
                // advance i — re-check the element now sitting here.
                _ = self.timeouts.swapRemove(i);
                // Loser (op completed first, already claimed) is dropped — that
                // path owns the cleanup; we just needed it off the array.
                if (t.winRace()) {
                    t._wake_next = fire_list;
                    fire_list = t;
                }
            } else {
                i += 1;
            }
        }
        self.sl.unlock();

        while (fire_list) |t| {
            // Read `next` before firing: an `on_fire` that wakes a fiber may
            // see that fiber free its frame (and thus `t`) the instant it
            // resumes, so touch nothing on `t` after the callback.
            const next = t._wake_next;
            t._wake_next = null;
            t.on_fire(t.on_fire_ctx);
            fire_list = next;
        }
    }

    /// Reset the timer for the earliest pending deadline. Owner-thread
    /// only (touches the xev timer). No-op when there are no timeouts.
    fn resetTimer(self: *Deadlines, current_ms: i64) void {
        self.sl.lock();
        const head = self.minDeadlineLocked();
        self.sl.unlock();
        const deadline = head orelse return;

        const delta = deadline - current_ms;
        const next_ms: u64 = if (delta < @as(i64, @intCast(min_timeout_ms)))
            min_timeout_ms
        else
            @intCast(delta);

        if (self.armed) {
            // reset cancels the in-flight SQE (callback fires with
            // error.Canceled) and queues a new one for `next_ms`.
            self.timer.reset(
                self.loop,
                &self.completion,
                &self.cancel_completion,
                next_ms,
                Deadlines,
                self,
                onTimer,
            );
        } else {
            self.timer.run(
                self.loop,
                &self.completion,
                next_ms,
                Deadlines,
                self,
                onTimer,
            );
            self.armed = true;
        }
    }
};

fn onTimer(
    ud: ?*Deadlines,
    _: *xev.Loop,
    _: *xev.Completion,
    result: xev.Timer.RunError!void,
) xev.CallbackAction {
    const self = ud.?;
    _ = result catch |err| switch (err) {
        // A `reset` fires the old SQE with Canceled. The reset has
        // already queued a fresh run on `self.completion`; just bow
        // out and let that one call us back later.
        error.Canceled => return .disarm,
        // Unexpected: nothing to do, drop the tick. The next register
        // will re-arm.
        error.Unexpected => {
            self.armed = false;
            return .disarm;
        },
    };

    // Normal expiration: drain everything whose deadline has passed,
    // then re-arm for the next entry if any remain. Same monotonic
    // source as `register` — see its comment.
    const now = coarse.clockMsUseRarely(.monotonic);
    self.drainExpired(now);
    self.armed = false;
    // armForHead re-locks the heap and no-ops if it's empty.
    self.resetTimer(now);
    return .disarm;
}

// ── Per-fiber owned timeout: fire / arm / disarm ────────────────────────
//
// A timed op arms THIS fiber's own `Fiber.timeout`, parks, and disarms on exit.
// Because the Timeout lives in the Fiber rather than on the op's stack, a fire
// that races the disarm can only ever wake this fiber — never an unrelated one
// through a reused frame.

/// `on_fire` for a fiber's own timeout: record the fire (bump the generation)
/// then wake the fiber. Runs on the owner worker (from `drainExpired`); `ctx`
/// is the `*Fiber`.
///
/// NOTE that the caller of this function should already have won the race
/// before calling this.
pub fn wonRaceAndTimedOut(ctx: *anyopaque) void {
    const f: *Fiber = @ptrCast(@alignCast(ctx));
    f.timeout._fired.store(true, .release);
    rt.wake(f);
}

/// Arm fiber `f`'s own timeout `ms` from now on its current worker's heap.
/// Snapshot `f.timed_out` BEFORE calling to later detect the fire. Returns the
/// OWNING `Deadlines` so a migrated op disarms on the same heap. Asserts the
/// timeout isn't already armed — a leak tripwire: a fiber waits in one place at
/// a time, so a previous op must have disarmed.
pub fn armSelf(f: *Fiber, ms: u64) !*Deadlines {
    f.timeout.reset();
    f.timeout.on_fire = &wonRaceAndTimedOut;
    f.timeout.on_fire_ctx = f;
    const o = &rt.currentWorker().deadlines;
    try o.register(&f.timeout, ms);
    return o;
}

/// Disarm fiber `f`'s timeout on `owner` (captured by `armSelf`). Claims first —
/// so a late `drainExpired` pop can't fire — then removes it from the heap.
pub fn disarmSelf(f: *Fiber, owner: *Deadlines) void {
    _ = f.timeout.winRace();
    owner.cancel(&f.timeout);
}

/// Park the calling fiber for `ms` milliseconds on the current worker's
/// `Deadlines` (one `xev.Timer` per worker, re-armed to the earliest pending
/// deadline) rather than arming a fresh libxev timer per call. `ms` is clamped
/// up to `min_timeout_ms` — to merely give up the CPU, use `yield`.
///
/// Parks EXACTLY ONCE and panics if resumed before our own deadline fired
/// (`timed_out` unchanged). `fiberTimedOut` bumps `timed_out` (release) before
/// it wakes us, so a resume from our own deadline is always observable here; any
/// other resume is an unexplained spurious wake. We refuse to paper over it with
/// a re-park — a park must only return for a real reason, so a violation is a
/// runtime bug surfaced loudly with an attributable stack.
pub fn sleepMs(ms: u64) void {
    const f = rt.currentFiber();
    const owner = armSelf(f, ms) catch |err| {
        std.debug.panic("sleepMs: deadline register failed: {}", .{err});
    };
    defer disarmSelf(f, owner);
    rt.park();
    if (!f.timeout.didFire()) std.debug.panic(
        "sleepMs: resumed before our own deadline fired — spurious wake",
        .{},
    );
}
