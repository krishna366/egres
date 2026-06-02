//! Per-worker timeout machinery: an intrusive min-heap of pending
//! deadlines plus one `xev.Timer` re-armed dynamically to the
//! earliest deadline.
//!
//! Each `Worker` owns one `Deadlines` instance. Timeouts registered
//! on worker N live in worker N's heap, and the xev `timer` is only
//! ever armed/reset on worker N's thread — libxev per-loop state is
//! not thread-safe, so `register`/`drainExpired`/`onTimer`/`armForHead`
//! stay owner-thread-only.
//!
//! `cancel`, however, IS safe to call from any worker: a fiber that
//! registered a timeout here can migrate and complete on a different
//! worker, then cancel from there. The heap array is therefore guarded
//! by `sl` (a spinlock) on every access — the only cross-thread
//! mutation is `cancel`'s `heap.remove`; the timer SQE ops stay off the
//! lock and on the owner thread.
//!
//! Heap impl lives in `fiber/heap.zig` as a generic intrusive
//! `MinHeap(T, key_field, index_field)`. We pay one `usize` per
//! `Timeout` (`heap_index`) in exchange for O(log N) push, pop, AND
//! middle-remove. Middle-remove is the load-bearing op: typical
//! pattern is "register a timeout, op completes first, cancel the
//! timeout" — cancel runs roughly once per op, while the deadline
//! firing only happens when things actually go wrong.

const std = @import("std");
const xev = @import("xev");
const heap = @import("heap.zig");
const coarse = @import("coarse_clock.zig");

const Spinlock = @import("spinlock.zig").Spinlock;
const Allocator = std.mem.Allocator;

/// Minimum honored timeout. Requests below this are clamped up. The
/// clock cache that consumers measure against has 1 ms granularity,
/// so sub-ms timeouts would either round to zero or fire immediately
/// — neither useful. Pin the floor at the boundary instead.
pub const min_timeout_ms: u64 = 1;

/// One pending deadline. Lives on the consumer's stack (same pattern
/// as the I/O `Waiter`s). The heap holds pointers to these so the
/// caller's frame is the storage.
///
/// When the deadline fires (expires before `Deadlines.cancel(t)`), the
/// owner worker invokes `on_fire(on_fire_ctx)` — exactly once, gated by
/// `_done`. That callback owns everything that must run on this worker's
/// thread: a watermark waiter sets its own flag and wakes itself; a
/// socket read submits an io_uring cancel against the still-in-flight op.
/// `Deadlines` itself stays oblivious to fibers and waking.
pub const Timeout = struct {
    on_fire: *const fn (ctx: *anyopaque) void,
    on_fire_ctx: *anyopaque,

    /// Absolute monotonic-ms deadline (signed: `armForHead` needs
    /// `deadline - now` to go negative for already-past timers). Filled
    /// in by `register` from `ms_from_now`; consumers leave it default.
    _deadline_ms: i64 = 0,

    /// First-fire-wins token: whichever of (deadline fires, the
    /// underlying I/O op completes) gets to swap this from false to
    /// true owns the wake. The loser is a no-op.
    _done: std.atomic.Value(bool) = .init(false),

    /// Slot in `Deadlines.heap`. Maintained by `MinHeap`; consumers
    /// should not touch.
    _heap_index: usize = Heap.not_in_heap,

    /// Transient link used only by `drainExpired` to batch a tick's worth
    /// of expired timeouts into a wake list under one lock, then wake them
    /// after unlock. Not in use while parked.
    _wake_next: ?*Timeout = null,

    /// Win the first-fire race. Returns true if THIS caller claimed the
    /// timeout (and now owns waking the parked fiber), false if it was
    /// already claimed. Used by `drainExpired` (deadline fired) and by
    /// consumers whose op completes first (e.g. the watermark's
    /// advance/close path) to arbitrate which side does the single wake.
    pub fn claim(t: *Timeout) bool {
        return !t._done.swap(true, .acq_rel);
    }
};

const Heap = heap.MinHeap(Timeout, "_deadline_ms", "_heap_index");

pub const Deadlines = struct {
    loop: *xev.Loop,
    timer: xev.Timer,
    /// Owns the heap's backing array — grown by `register`, freed by
    /// `deinit`. Captured at init (the runtime's allocator) so callers
    /// never pass one and can't mismatch grow vs. free.
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
    heap: Heap = .{},
    /// Guards every access to `heap`. The only cross-thread caller is
    /// `cancel` (a migrated waiter removing its own timeout); the owner
    /// thread holds it for the brief heap mutations in
    /// `register`/`drainExpired`/`armForHead`. Uncontended on the hot path.
    sl: Spinlock = .{},

    pub fn init(loop: *xev.Loop, allocator: Allocator) !Deadlines {
        return .{
            .loop = loop,
            .timer = try xev.Timer.init(),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Deadlines) void {
        self.heap.deinit(self.allocator);
        self.timer.deinit();
    }

    /// Register a deadline. `ms_from_now` is clamped up to
    /// `min_timeout_ms`. The `Timeout` lives on the caller's stack;
    /// this call only links it into the heap.
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

        // Snapshot the head BEFORE push so we know whether the new
        // entry will be the new minimum. A tie (equal deadline) is
        // *not* a new minimum — the existing timer fires at the same
        // moment and drainExpired sweeps both.
        self.sl.lock();
        const becomes_head = blk: {
            if (self.heap.peekMin()) |h| break :blk t._deadline_ms < h._deadline_ms;
            break :blk true;
        };
        self.heap.push(self.allocator, t) catch |err| {
            self.sl.unlock();
            return err;
        };
        self.sl.unlock();

        // Arm outside the lock — `register` only ever runs on the owner
        // thread, so the timer SQE op is safe; keeping it off `sl` avoids
        // pinning a cross-thread `cancel` behind a libxev call.
        if (becomes_head) self.armForHead(current_ms);
    }

    /// O(log N) middle removal. Safe even when the entry isn't
    /// enqueued (no-op). Hot path: called from an I/O completion
    /// callback when the op finished before the deadline fired.
    ///
    /// Doesn't reset the timer even when removing the current head —
    /// the in-flight SQE will fire harmlessly (drainExpired finds the
    /// new head's deadline still in the future and bails). The cost
    /// is one spurious wake-up; the win is no extra reset SQE per
    /// op-completes-first case (the common path).
    pub fn cancel(self: *Deadlines, t: *Timeout) void {
        self.sl.lock();
        self.heap.remove(t);
        self.sl.unlock();
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
        while (self.heap.peekMin()) |t| {
            if (t._deadline_ms > now) break;
            _ = self.heap.popMin();
            // Loser (op completed first, already claimed) is dropped — that
            // path owns the cleanup; we just needed it off the heap.
            if (t.claim()) {
                t._wake_next = fire_list;
                fire_list = t;
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

    /// Arm (or reset) the timer for the current heap head. Owner-thread
    /// only (touches the xev timer). No-op when the heap is empty.
    fn armForHead(self: *Deadlines, current_ms: i64) void {
        self.sl.lock();
        const head = self.heap.peekMin();
        const deadline = if (head) |h| h._deadline_ms else 0;
        self.sl.unlock();
        if (head == null) return;

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
    self.armForHead(now);
    return .disarm;
}
