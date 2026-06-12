const std = @import("std");
const Allocator = std.mem.Allocator;
const Fiber = @import("fiber.zig").Fiber;
const Spinlock = @import("spinlock.zig").Spinlock;

/// Per-worker, per-priority ready queue: a fixed-size ring buffer of
/// `*Fiber`. Capacity is fixed at `local_queue_capacity`. `push` returns
/// `error.Full` when the ring is full; the caller decides what to do: spill
/// normal-priority fibers to the `GlobalQueue`, or panic for high priority
/// (high-priority work is pinned and expected to fit). See
/// `Worker.scheduleLocal`.
pub const local_queue_capacity: usize = 32;

/// Whether the `LocalQueue` guards its mutating ops with a `Spinlock`.
///   - .none: no locking. Does not allow task stealing.
///   - .withlock: every `push`/`pop` takes a `Spinlock`, so a peer worker
///     could steal without racing the owner. Required for task stealing.
pub const LockMode = enum { none, withlock };

/// `LocalQueue(.none)` is the zero-overhead owner-only ring; `LocalQueue(.withlock)`
/// is the same ring with a `Spinlock` around `push`/`pop`. Both expose the
/// identical API; the lock branches and the `sl` field are comptime-elided
/// in `.none` mode, so it keeps the exact layout/codegen it had before.
pub fn LocalQueue(comptime lock: LockMode) type {
    return struct {
        const Self = @This();
        comptime {
            std.debug.assert(std.math.isPowerOfTwo(local_queue_capacity));
        }
        const mask: usize = local_queue_capacity - 1;

        // Slot index of the next pop is `head`; the live range is
        // `[head, head + size)` taken `& mask`. `tail` is derivable so it
        // isn't stored. Slots are OPTIONAL and zeroed (`null`) the instant a
        // fiber is picked (pop/steal), so a stale/duplicate slot read — e.g.
        // a `head`/`size` drift re-reading an already-stolen slot — surfaces
        // as a LOUD null rather than silently re-dispatching a real fiber
        // that is already running on another worker.
        fibers: [local_queue_capacity]?*Fiber = @splat(null),
        head: usize = 0,
        // Atomic: the owner mutates it under the lock, but `len`/`isEmpty`
        // peek it WITHOUT the lock (hot-path hints), and a `.withlock` peer
        // mutates it under the lock during a steal. A plain field would make
        // those lock-free peeks a data race (UB → miscompile → `head`/`size`
        // drift → garbage slot reads). `head` and `fibers` are touched only
        // under the lock, so they stay plain.
        size: std.atomic.Value(usize) = .init(0),
        // Present only in `.withlock`; `void` (zero-sized) otherwise.
        sl: if (lock == .withlock) Spinlock else void =
            if (lock == .withlock) .{} else {},

        inline fn acquire(self: *Self) void {
            if (lock == .withlock) self.sl.lock();
        }
        inline fn release(self: *Self) void {
            if (lock == .withlock) self.sl.unlock();
        }

        /// Append `f` to the back. Returns `error.Full` if the ring is at
        /// capacity (caller handles the spill/panic policy).
        pub fn push(self: *Self, f: *Fiber) error{Full}!void {
            self.acquire();
            defer self.release();
            const size = self.size.load(.monotonic);
            if (size == local_queue_capacity) return error.Full;
            const idx = (self.head + size) & mask;
            std.debug.assert(self.fibers[idx] == null); // slot must be free
            self.fibers[idx] = f;
            self.size.store(size + 1, .monotonic);
            f.assertEnQ();
        }

        /// Remove and return the front fiber, or null if empty.
        pub fn pop(self: *Self) ?*Fiber {
            self.acquire();
            defer self.release();
            const size = self.size.load(.monotonic);
            if (size == 0) return null;
            const f = self.fibers[self.head] orelse std.debug.panic(
                "LocalQueue.pop: NULL slot[{d}] dispatched; size={d} — head/size drift or stale re-read",
                .{ self.head, size },
            );
            self.fibers[self.head] = null; // zero on pick: no stale re-dispatch
            self.head = (self.head + 1) & mask;
            self.size.store(size - 1, .monotonic);
            f.assertDQ();
            return f;
        }

        /// Steal up to `out.len` fibers from the FRONT of this queue into
        /// `out`, returning the count moved. Called by a PEER worker (the
        /// thief): it takes this queue's lock, so the owner's concurrent
        /// `push`/`pop` serialize against it — neither can ever take the same
        /// fiber. The thief enqueues the returned fibers into its OWN ring
        /// AFTER this returns; it must not hold a second queue lock here
        /// (deadlock risk), which is why we copy into a caller buffer rather
        /// than push directly. Steals ~half the live entries (rounded up, so
        /// a lone fiber is taken too) to leave the victim a share. Only valid
        /// on `.withlock` queues.
        pub fn stealInto(self: *Self, out: []*Fiber) usize {
            if (lock != .withlock) @compileError("stealInto requires .withlock");
            self.acquire();
            defer self.release();

            const size = self.size.load(.monotonic);
            if (size == 0) return 0;

            const half = (size + 1) / 2; // ceil: size 1 -> 1
            const take = @min(out.len, half);
            for (0..take) |k| {
                const idx = (self.head + k) & mask;
                out[k] = self.fibers[idx] orelse std.debug.panic(
                    "LocalQueue.steal: NULL slot[{d}]; size={d} take={d} — stale re-read",
                    .{ idx, size, take },
                );
                self.fibers[idx] = null; // zero on pick: no stale re-dispatch
                out[k].assertDQ();
            }
            self.head = (self.head + take) & mask;
            self.size.store(size - take, .monotonic);
            return take;
        }

        // Lock-free peeks (like `RemoteQueue.isEmpty`): a monotonic atomic
        // load — well-defined even while a `.withlock` peer mutates `size`
        // under the lock. Returns a recent snapshot, all these hint callers
        // (hasReady / refill room / pile-up notify) need; the actual push/pop
        // re-checks under the lock.
        pub fn len(self: *const Self) usize {
            return self.size.load(.monotonic);
        }

        pub fn capacity(_: *const Self) usize {
            return local_queue_capacity;
        }

        pub fn isEmpty(self: *const Self) bool {
            return self.size.load(.monotonic) == 0;
        }
    };
}

/// Lock-free Treiber stack for cross-thread fiber injection. Push is
/// one CAS; drain is one atomic swap. Used ONLY for high-priority
/// cross-thread tasks targeted at a specific worker (normal-priority
/// cross-thread tasks go through the `GlobalQueue` instead). Pushed to
/// by peer workers / external threads; drained by the home worker at
/// the top of every outer-loop iteration.
pub const RemoteQueue = struct {
    head: std.atomic.Value(?*Fiber) align(std.atomic.cache_line) = .init(null),

    pub fn push(self: *RemoteQueue, f: *Fiber) void {
        f.assertEnQ();
        var current = self.head.load(.monotonic);
        while (true) {
            f.remote_next = current;
            if (self.head.cmpxchgWeak(current, f, .release, .monotonic)) |actual| {
                current = actual;
                continue;
            }
            return;
        }
    }

    /// Drain the whole stack and return it oldest-first (FIFO), or null
    /// if empty. The Treiber-stack swap yields newest-first; we reverse
    /// in place via `remote_next` so a burst pushed by a peer keeps its
    /// spawn order. The caller walks `remote_next`.
    pub fn drainFifo(self: *RemoteQueue) ?*Fiber {
        var lifo = self.head.swap(null, .acquire);
        var fifo: ?*Fiber = null;
        while (lifo) |f| {
            lifo = f.remote_next;
            f.remote_next = fifo;
            fifo = f;
            f.assertDQ();
        }
        return fifo;
    }

    /// Load-only emptiness peek — no swap, so it never dirties the cache
    /// line. Lets the home worker cheaply skip `drainFifo` when no peer has
    /// pushed: the head load stays a cache-hit until a `push` writes it.
    pub fn isEmpty(self: *const RemoteQueue) bool {
        return self.head.load(.acquire) == null;
    }
};

/// Largest worker count we support; bounds the idle bitmap. Worker ids
/// are `u8`, so 256 is the ceiling.
pub const max_workers: usize = 256;
const idle_words: usize = (max_workers + 63) / 64;

/// Idle-worker bitmap for one lane. Bit `i` set ⇒ the worker at
/// lane-relative offset `i` is parked in `loop.run(.once)` waiting for
/// work. A producer making work available calls `claim` to atomically
/// take one idle worker's bit, then wakes exactly that peer as a
/// SEARCHER — see `Runtime.notifyIdlePeer` for the throttle around it.
/// Lane-agnostic (indexes by offset within a lane) so it unit-tests
/// standalone.
///
/// Bit ownership: a worker only ever sets its OWN bit (`markIdle`); it
/// is cleared exactly once per park cycle, by either a producer's
/// `claim` (a deliberate wake) or the worker's own `selfClear` (it woke
/// for another reason — timer, I/O, stop). Both are single RMWs on the
/// word, so the two sides can never both think they cleared it; that
/// exactly-once handoff is what `Worker.unpark` uses to decide whether
/// it inherited the searching state.
///
/// Memory ordering policy: EVERY atomic op here is seq_cst, uniformly.
/// Several ops sit in Dekker (store-then-load-other-location) pairs with
/// the producers' push-then-peek sequence, where acquire/release is
/// formally insufficient (a plain acquire load may return a stale word and
/// both sides can miss → lost wakeup); the rest could be weaker but are
/// kept seq_cst so nobody has to re-derive which is which. This is free on
/// our targets: seq_cst RMWs and loads compile to the same instructions as
/// acq_rel RMWs and acquire loads on both x86-64 and aarch64. Two
/// load-bearing invariants regardless: all mutations are RMWs (a plain
/// store would sever the release sequence carrying the num_searching +1
/// handoff from `claim` to `selfClear`), and RMW reads are never stale (an
/// RMW always reads the latest value in modification order).
pub const IdleWorkers = struct {
    words: [idle_words]std.atomic.Value(u64) align(std.atomic.cache_line) = @splat(.init(0)),

    pub fn markIdle(self: *IdleWorkers, i: u8) void {
        const w = @as(usize, i) >> 6;
        const bit = @as(u64, 1) << @as(u6, @intCast(i & 63));
        _ = self.words[w].fetchOr(bit, .seq_cst);
    }

    /// Atomically clear worker `i`'s own bit, returning true if WE cleared
    /// it. False means a producer's `claim` got there first: the wake was
    /// deliberate and the worker was registered as a searcher (see
    /// `Runtime.notifyIdlePeer`). The single RMW (vs. a load-then-clear
    /// pair) is what makes the handoff exactly-once.
    pub fn selfClear(self: *IdleWorkers, i: u8) bool {
        const w = @as(usize, i) >> 6;
        const bit = @as(u64, 1) << @as(u6, @intCast(i & 63));
        return (self.words[w].fetchAnd(~bit, .seq_cst) & bit) != 0;
    }

    /// "Is anyone parked?" peek for producers — loads only, so the
    /// all-workers-busy steady state pays no RMW on the notify path.
    pub fn anySet(self: *IdleWorkers) bool {
        for (&self.words) |*word| {
            if (word.load(.seq_cst) != 0) return true;
        }
        return false;
    }

    /// Atomically claim (clear) one set bit and return its offset, or
    /// null if no worker is idle. The successful CAS's write side also
    /// publishes the producer's prior num_searching fetchAdd to the woken
    /// worker's `selfClear` (the searcher +1 handoff).
    pub fn claim(self: *IdleWorkers) ?u8 {
        for (&self.words, 0..) |*word, wi| {
            var m = word.load(.seq_cst);
            while (m != 0) {
                const bit_i: u6 = @intCast(@ctz(m));
                const bit = @as(u64, 1) << bit_i;
                if (word.cmpxchgWeak(m, m & ~bit, .seq_cst, .seq_cst)) |actual| {
                    m = actual;
                    continue;
                }
                return @intCast(wi * 64 + bit_i);
            }
        }
        return null;
    }

    /// Targeted "is THIS worker parked?" peek — the producer side of the
    /// high-priority push-then-peek Dekker pair (see
    /// `Worker.notifyIfParked`); same shape as `anySet`.
    pub fn isIdle(self: *IdleWorkers, i: u8) bool {
        const w = @as(usize, i) >> 6;
        const bit = @as(u64, 1) << @as(u6, @intCast(i & 63));
        return (self.words[w].load(.seq_cst) & bit) != 0;
    }
};

/// Runtime-wide overflow / admission queue. Spinlock-protected ring
/// buffer of `*Fiber`. Capacity starts at `initial_capacity` and
/// doubles on push when full. One per `Lane`; holds **normal-priority
/// tasks only**. Used for:
///
///   - Bootstrap spawns from a non-worker thread (the host thread
///     calling `fiber.spawn` before `Runtime.run` has been entered).
///   - Off-worker / cross-lane normal spawns and wakes.
///   - Spillover from a worker whose normal `LocalQueue` is full.
///
/// Multi-producer, multi-consumer; producers (`push`) and consumers
/// (`popBatchTo`) all serialize on the same spinlock.
///
/// Not a hot path. Workers prefer their own local queue first and
/// consult the global queue only when it is empty.
pub const GlobalQueue = struct {
    allocator: Allocator,
    sl: Spinlock = .{},

    // Ring buffer for *Fiber. Capacity is a power of two so the slot
    // index is `(head + i) & mask`. All access under `sl`, so none of
    // these need atomic ops. `tail` is derivable as `(head + size) &
    // mask` and isn't stored.
    fibers: []*Fiber,
    mask: usize,
    head: usize = 0,
    size: usize = 0,

    pub fn init(allocator: Allocator, initial_capacity: usize) !GlobalQueue {
        std.debug.assert(std.math.isPowerOfTwo(initial_capacity));
        std.debug.assert(initial_capacity >= 2);
        const fibers = try allocator.alloc(*Fiber, initial_capacity);
        return .{
            .allocator = allocator,
            .fibers = fibers,
            .mask = initial_capacity - 1,
        };
    }

    pub fn deinit(self: *GlobalQueue) void {
        self.allocator.free(self.fibers);
    }

    pub fn capacity(self: *const GlobalQueue) usize {
        return self.mask + 1;
    }

    /// Append `f` to the back. Grows the ring (doubling) when full.
    /// Safe from any thread.
    pub fn push(self: *GlobalQueue, f: *Fiber) Allocator.Error!void {
        self.sl.lock();
        defer self.sl.unlock();

        if (self.size == self.capacity()) try self.grow();
        self.fibers[(self.head + self.size) & self.mask] = f;
        self.size += 1;
        f.assertEnQ();
    }

    fn grow(self: *GlobalQueue) Allocator.Error!void {
        const new_cap = self.capacity() * 2;
        const new_fibers = try self.allocator.alloc(*Fiber, new_cap);

        // Compact live range into new buffer starting at index 0.
        // The old range may wrap, so check head + size vs capacity.
        const head_to_end = self.capacity() - self.head;
        if (self.size <= head_to_end) {
            // Contiguous in old buffer: one memcpy.
            @memcpy(new_fibers[0..self.size], self.fibers[self.head..][0..self.size]);
        } else {
            // Wraps. Copy [head..capacity) then [0..size - head_to_end).
            @memcpy(new_fibers[0..head_to_end], self.fibers[self.head..]);
            @memcpy(new_fibers[head_to_end..self.size], self.fibers[0 .. self.size - head_to_end]);
        }
        self.allocator.free(self.fibers);
        self.fibers = new_fibers;
        self.mask = new_cap - 1;
        self.head = 0;
    }

    /// Pop a single fiber from the front, or null when empty. The fairness
    /// path: every `global_queue_interval`-th dispatch a busy worker runs
    /// one global fiber AHEAD of its local normal ring (see
    /// `Worker.nextFiber`) so globally-queued work can't be starved by a
    /// ring that never drains.
    pub fn popOne(self: *GlobalQueue) ?*Fiber {
        self.sl.lock();
        defer self.sl.unlock();
        if (self.size == 0) return null;
        const f = self.fibers[self.head];
        self.head = (self.head + 1) & self.mask;
        self.size -= 1;
        f.assertDQ();
        return f;
    }

    /// Pull up to `max_take` fibers from the front of the global queue
    /// into `dest`'s tail. `dest` exposes `push`/`capacity` (the
    /// per-worker `LocalQueue`), so we just call `push` per fiber.
    /// Returns the count transferred (may be less than `max_take` if
    /// `dest` hits capacity mid-transfer). Caller must own `dest`.
    pub fn popBatchTo(
        self: *GlobalQueue,
        dest: anytype,
        max_take: usize,
    ) usize {
        self.sl.lock();
        defer self.sl.unlock();

        if (self.size == 0) return 0;
        const take = @min(max_take, self.size);
        var transferred: usize = 0;
        while (transferred < take) : (transferred += 1) {
            const src_idx = (self.head + transferred) & self.mask;
            const f = self.fibers[src_idx];
            f.assertDQ(); // leaving global; dest.push re-enqueues
            dest.push(f) catch {
                f.assertEnQ(); // dest full → stays in global
                break;
            };
        }
        self.head = (self.head + transferred) & self.mask;
        self.size -= transferred;
        return transferred;
    }

    /// Snapshot length. Takes the spinlock; intended for tests / stats,
    /// not for control flow on the hot path (`popBatchTo`'s return value
    /// already tells you if work was available).
    pub fn len(self: *GlobalQueue) usize {
        self.sl.lock();
        defer self.sl.unlock();
        return self.size;
    }
};
