//! SemaphoreMap: keyed reader/writer locks with owner tokens and
//! block-time deadlock detection.
//!
//! `acquire(key, owner, mode)` blocks — FIFO, writer-preferred, all
//! delegated to a per-entry `sync.RWMutex` — while conflicting owners
//! hold `key`; it is a no-op for an owner re-acquiring at the same or
//! weaker mode, and returns `error.Deadlock` when granting would close
//! a wait cycle. `owner_id` is an opaque caller token (a transaction
//! id), not fiber identity, so a logical owner can span fibers and any
//! of them may release.
//!
//! There are NO timeouts. Every wait-for cycle has a last edge, and the
//! requester inserting it runs a DFS over the graph under `sl` BEFORE
//! parking — so deadlocks are refused at the door (victim = the
//! edge-closer), no cycle can ever form among parked fibers, and no
//! parked fiber ever needs interrupting.
//!
//! This file never touches fibers, parks, or deadlines: blocking is the
//! entry RWMutex's job. `sl` guards the entry table and the wait-for
//! graph (holders + registration-ordered waiters + `waiting`), which
//! mirror the RWMutex state for detection but never replace it.
//!
//! Keys are bare u64s — typically a hash of the caller's logical key so the map
//! never touches key bytes and the hot path allocates nothing once the entry
//! pool is warm. For hashing callers, a 64-bit collision silently MERGES two
//! keys' locks (false contention, or a false re-entrant no-op for one owner) —
//! never corruption — and with a handful of live entries the odds are ~2^-64;
//! we accept that.

const std = @import("std");
const _sync = @import("sync.zig");
const _spinlock = @import("spinlock.zig");

pub const SemaphoreMap = struct {
    allocator: std.mem.Allocator,
    /// A spinlock, not a parking mutex: every critical section under it
    /// is a hash probe, a short list edit or the deadlock DFS — pure CPU,
    /// never a park — and it is taken 4 times per SQL statement by every
    /// connection. As a hand-off mutex it ran at 330k hand-offs/s under
    /// pgbench, each one parking the waiter and waking it on another
    /// worker while the map stayed locked.
    sl: _spinlock.Spinlock = .{},
    /// Held-or-contended keys only; an entry is created on first
    /// acquire and recycled by whichever release/abort leaves it with
    /// no holders and no waiters.
    entries: std.AutoHashMapUnmanaged(u64, *Entry) = .empty,
    /// The wait-for graph's edge source: which entry each BLOCKED owner
    /// is queued on (an owner waits on one key at a time), and in what
    /// mode. Registered before the fiber ever parks.
    waiting: std.AutoHashMapUnmanaged(u64, Waiting) = .empty,
    /// Drained entries kept for reuse with their list capacity intact,
    /// so steady-state acquire/release does no allocator work. Bounded:
    /// beyond `max_pooled` (or after a spike balloons an entry's lists)
    /// entries are freed instead of held forever.
    pool: std.ArrayList(*Entry) = .empty,
    /// wouldDeadlock's walker, reused across runs. Only touched under
    /// `sl`, which serializes detection.
    dfs: Dfs,

    const max_pooled = 128;
    /// A pooled entry keeps its holders/waiters capacity; anything that
    /// grew past this many slots during a spike is freed, not pooled.
    const max_kept_capacity = 32;

    pub const Mode = enum { shared, exclusive };

    const Waiting = struct { entry: *Entry, mode: Mode };
    const OwnerMode = struct { owner_id: u64, mode: Mode };

    const Entry = struct {
        /// Does ALL the blocking; writer preference means a pending
        /// exclusive blocks new shared acquirers (see sync.RWMutex).
        rw: _sync.RWMutex(.untracked) = .{},
        /// One exclusive holder, or any number of shared ones.
        holders: std.ArrayList(OwnerMode) = .empty,
        /// Blocked acquirers, registration order.
        waiters: std.ArrayList(OwnerMode) = .empty,

        // Mode-dispatched RWMutex ops. `rw` is `.untracked`, so the
        // `@src()` these pass is comptime-elided — no source location is
        // lost by routing through a helper.
        fn tryLock(e: *Entry, mode: Mode) bool {
            return switch (mode) {
                .exclusive => e.rw.tryLock(@src()),
                .shared => e.rw.tryLockShared(@src()),
            };
        }
        fn lock(e: *Entry, mode: Mode) void {
            switch (mode) {
                .exclusive => e.rw.lock(@src()),
                .shared => e.rw.lockShared(@src()),
            }
        }
        fn unlock(e: *Entry, mode: Mode) void {
            switch (mode) {
                .exclusive => e.rw.unlock(),
                .shared => e.rw.unlockShared(),
            }
        }
    };

    /// One walk of the wait-for graph: owners still to expand, plus the
    /// ones already expanded so a diamond costs a single visit. Kept
    /// (with its capacity) between walks, so steady-state detection
    /// does no allocator work.
    const Dfs = struct {
        allocator: std.mem.Allocator,
        stack: std.ArrayList(u64) = .empty,
        visited: std.AutoHashMapUnmanaged(u64, void) = .empty,

        fn deinit(self: *Dfs) void {
            self.stack.deinit(self.allocator);
            self.visited.deinit(self.allocator);
        }

        /// Drop the previous walk, keep its capacity.
        fn restart(self: *Dfs) void {
            self.stack.clearRetainingCapacity();
            self.visited.clearRetainingCapacity();
        }

        /// Queue `owner` for expansion. Duplicates are fine: `next`
        /// hands out each owner at most once.
        fn push(self: *Dfs, owner: u64) error{OutOfMemory}!void {
            try self.stack.append(self.allocator, owner);
        }

        /// The next owner to expand, or null once the walk is done.
        fn next(self: *Dfs) error{OutOfMemory}!?u64 {
            while (self.stack.pop()) |owner| {
                const gop = try self.visited.getOrPut(self.allocator, owner);
                if (!gop.found_existing) return owner;
            }
            return null;
        }
    };

    pub fn init(allocator: std.mem.Allocator) !SemaphoreMap {
        var pool: std.ArrayList(*Entry) = .empty;
        try pool.ensureTotalCapacityPrecise(allocator, max_pooled);
        return .{
            .allocator = allocator,
            .pool = pool,
            .dfs = .{ .allocator = allocator },
        };
    }

    pub fn deinit(self: *SemaphoreMap) void {
        // Callers release everything they hold before tearing the map
        // down; anything left here is an unclean shutdown.
        var it = self.entries.valueIterator();
        while (it.next()) |e| self.destroyEntry(e.*);
        self.entries.deinit(self.allocator);
        for (self.pool.items) |e| self.destroyEntry(e);
        self.pool.deinit(self.allocator);
        self.dfs.deinit();
        self.waiting.deinit(self.allocator);
    }

    fn destroyEntry(self: *SemaphoreMap, e: *Entry) void {
        e.holders.deinit(self.allocator);
        e.waiters.deinit(self.allocator);
        self.allocator.destroy(e);
    }

    /// Block until `owner_id` holds `key` in `mode`. Returns true if
    /// this call took (or strengthened nothing — took) the hold, false
    /// if the owner already held it at this or a stronger mode
    /// (re-entrant no-op) — callers pair exactly one release with each
    /// true. `error.Deadlock` if blocking would close a wait cycle; the
    /// requester never parks in that case. `error.Upgrade` if the owner
    /// already holds `key` shared and asks for exclusive — the caller
    /// decides how to treat that (this map does not upgrade in place).
    pub fn acquire(
        self: *SemaphoreMap,
        key: u64,
        owner_id: u64,
        mode: Mode,
    ) error{ Deadlock, Upgrade, OutOfMemory }!bool {
        const owner_mode = OwnerMode{ .owner_id = owner_id, .mode = mode };

        self.sl.lock();
        const e = self.entryFor(key) catch {
            self.sl.unlock();
            return error.OutOfMemory;
        };

        for (e.holders.items) |hold| {
            if (hold.owner_id != owner_id) continue;
            if (hold.mode == .exclusive or mode == .shared) {
                // If we're already holding an exclusive lock, OR we just want a
                // shared lock, we're good.
                self.sl.unlock();
                return false;
            }
            // Shared→exclusive upgrade in place would have to wait for
            // the owner's own shared hold to drain — a self-deadlock,
            // and two owners upgrading in lockstep deadlock classically.
            // This map does not support it; hand the decision up.
            self.sl.unlock();
            return error.Upgrade;
        }

        // Fast path: uncontended. The probes only take rw's spinlock —
        // safe under `sl` — and tryLockShared already defers to a
        // pending writer, so preference is preserved.
        if (e.tryLock(mode)) {
            e.holders.append(self.allocator, owner_mode) catch {
                // Roll the probe back; hand-off (if anyone was parked)
                // and reclamation behave as a normal release would.
                self.rollbackHold(key, e, mode);
                self.sl.unlock();
                return error.OutOfMemory;
            };
            self.sl.unlock();
            return true;
        }

        // Contended: publish our wait edge, then refuse-or-park. The
        // edge must be visible before we park so any later requester's
        // DFS can see it — that ordering is the completeness argument.
        e.waiters.append(self.allocator, owner_mode) catch {
            self.sl.unlock();
            return error.OutOfMemory;
        };
        // An owner can only be waiting on exactly one key at any time.
        self.waiting.put(self.allocator, owner_id, .{
            .entry = e,
            .mode = mode,
        }) catch {
            _ = removeOwner(&e.waiters, owner_id);
            self.sl.unlock();
            return error.OutOfMemory;
        };

        const cyclic = self.wouldDeadlock(owner_id, e, mode) catch {
            self.deregisterWaiter(e, owner_id);
            self.sl.unlock();
            return error.OutOfMemory;
        };
        if (cyclic) {
            self.deregisterWaiter(e, owner_id);
            self.sl.unlock();
            return error.Deadlock;
        }
        self.sl.unlock();

        e.lock(mode); // Block here waiting for lock.

        // Granted. Move waiter → holder; until this runs we are still
        // in `waiters`, which is what keeps the entry alive across the
        // wake (release only frees an entry with no holders AND no
        // waiters).
        self.sl.lock();
        self.deregisterWaiter(e, owner_id);
        e.holders.append(self.allocator, owner_mode) catch {
            self.rollbackHold(key, e, mode);
            self.sl.unlock();
            return error.OutOfMemory;
        };
        self.sl.unlock();
        return true;
    }

    /// Undo a granted-but-unrecorded rw hold (OOM recording it): unlock
    /// the rw — waking any parked contender; they re-take `sl`, which we
    /// hold, so they simply queue behind us — and reclaim the entry if
    /// that left it unused. Caller holds `sl`.
    fn rollbackHold(
        self: *SemaphoreMap,
        key: u64,
        e: *Entry,
        mode: Mode,
    ) void {
        e.unlock(mode);
        self.maybeRecycle(key, e);
    }

    /// Release `owner_id`'s hold on `key`. Frees the entry when nothing
    /// holds or waits on it; otherwise the RWMutex hands off (writer
    /// first, else all readers). Releasing a key the owner doesn't hold
    /// is a caller bookkeeping bug (one release per acquire that
    /// returned true), so it panics; a caller that legitimately may or
    /// may not hold the key uses `tryRelease`.
    pub fn release(self: *SemaphoreMap, key: u64, owner_id: u64) void {
        if (!self.tryRelease(key, owner_id))
            std.debug.panic(
                "SemaphoreMap.release: owner {d} does not hold key {x}",
                .{ owner_id, key },
            );
    }

    /// `release` for a caller that may not hold `key`: releases and
    /// returns true when it does, else a no-op returning false. For a
    /// hold that is taken and dropped more than once over its life (a
    /// DDL barrier retaken for the finish, and by its failure path),
    /// where the map is the one place that knows the current state.
    pub fn tryRelease(self: *SemaphoreMap, key: u64, owner_id: u64) bool {
        self.sl.lock();
        const kv = self.entries.getEntry(key) orelse {
            self.sl.unlock();
            return false;
        };
        const e = kv.value_ptr.*;
        const mode = removeOwner(&e.holders, owner_id) orelse {
            self.sl.unlock();
            return false;
        };
        if (e.holders.items.len == 0 and e.waiters.items.len == 0) {
            // We hold `sl` and just proved the entry drained, so drop it
            // straight from the map — no need for reclaimLocked's
            // defensive re-lookup (that guards the error paths, which
            // don't keep a `key_ptr`). The still-locked rw needs no
            // unlock before recycling — `recycle` resets it.
            self.entries.removeByPtr(kv.key_ptr);
            self.recycle(e);
            self.sl.unlock();
            return true;
        }
        // Unlock the rw while still holding `sl`. Once `sl` is dropped a
        // concurrent release can drain and recycle `e` — reset its rw, or
        // free it outright when a contention spike grew its lists — and
        // an rw unlock after that lands on a reset lock or freed memory
        // (the first word of a freed Entry is its rw: a cross-thread
        // double free under a 64-client pgbench). The wake this may
        // issue costs a peer at most a short spin.
        e.unlock(mode);
        self.sl.unlock();
        return true;
    }

    /// The entry for `key`, pulled from the pool (or created) on first
    /// use. Caller holds `sl`.
    fn entryFor(self: *SemaphoreMap, key: u64) !*Entry {
        const gop = try self.entries.getOrPut(self.allocator, key);
        if (gop.found_existing) return gop.value_ptr.*;

        if (self.pool.pop()) |e| {
            gop.value_ptr.* = e;
            return e;
        }
        const e = self.allocator.create(Entry) catch |err| {
            self.entries.removeByPtr(gop.key_ptr);
            return err;
        };
        e.* = .{};
        gop.value_ptr.* = e;
        return e;
    }

    /// Drop an entry with no holders and no waiters. Caller holds `sl`.
    /// Looks the entry up by key and compares pointers BEFORE any
    /// deref: callers on error paths may hold an `e` that a concurrent
    /// release already recycled (and the key may even map to a fresh
    /// entry).
    fn maybeRecycle(self: *SemaphoreMap, key: u64, e: *Entry) void {
        const kv = self.entries.getEntry(key) orelse return;
        if (kv.value_ptr.* != e) return;
        if (e.holders.items.len > 0 or e.waiters.items.len > 0) return;

        // No one else is holding this key-entry. Remove.
        self.entries.removeByPtr(kv.key_ptr);
        self.recycle(e);
    }

    /// Reset a drained entry and pool it for reuse — or free it when
    /// the pool is full or a contention spike ballooned its lists.
    /// The rw may still be locked (release skips the unlock on the
    /// reclaim path); nothing references it anymore, so a plain reset
    /// is sound. Caller holds `sl`.
    fn recycle(self: *SemaphoreMap, e: *Entry) void {
        if (self.pool.items.len >= max_pooled or
            e.holders.capacity > max_kept_capacity or
            e.waiters.capacity > max_kept_capacity)
            return self.destroyEntry(e);
        e.rw = .{};
        e.holders.clearRetainingCapacity();
        e.waiters.clearRetainingCapacity();
        // Capacity for max_pooled is reserved at init; the length guard
        // above makes this infallible.
        self.pool.appendAssumeCapacity(e);
    }

    /// Pull `owner_id`'s published wait edge: its slot in the entry's
    /// waiter list and its entry in the `waiting` graph. Caller holds
    /// `sl`.
    fn deregisterWaiter(self: *SemaphoreMap, e: *Entry, owner_id: u64) void {
        _ = removeOwner(&e.waiters, owner_id);
        _ = self.waiting.remove(owner_id);
    }

    fn removeOwner(list: *std.ArrayList(OwnerMode), owner_id: u64) ?Mode {
        for (list.items, 0..) |om, i| {
            if (om.owner_id == owner_id) {
                _ = list.orderedRemove(i);
                return om.mode;
            }
        }
        return null;
    }

    /// Would `start` blocking on `entry` in `mode` close a wait cycle
    /// through `start` itself? Walks the wait-for graph from start's
    /// own blockers outward (iteratively — fiber stacks are small): if
    /// following "waits for" edges leads back to `start`, granting the
    /// wait would deadlock. Runs under `sl`. A cycle NOT through
    /// `start` is impossible here — its own closing edge was refused
    /// when it was requested.
    fn wouldDeadlock(
        self: *SemaphoreMap,
        start: u64,
        entry: *Entry,
        mode: Mode,
    ) error{OutOfMemory}!bool {
        self.dfs.restart();
        try self.pushBlockers(entry, start, mode);
        while (try self.dfs.next()) |owner| {
            if (owner == start) return true;
            // Absent from `waiting` = runnable, so it blocks no one
            // else: a dead end for the walk.
            const w = self.waiting.get(owner) orelse continue;
            try self.pushBlockers(w.entry, owner, w.mode);
        }
        return false;
    }

    /// `mario` is blocked on `entry` wanting `mode`. Push everyone he
    /// has to wait for. Two kinds of blocker:
    ///   - holders: they have the lock right now.
    ///   - queued exclusive waiters: they hold nothing yet, but the
    ///     RWMutex can still grant them before mario.
    ///
    /// THE ONE ASSUMPTION: `waiters` is in `sync.RWMutex`'s grant order
    /// and that lock is writer-preferring FIFO. Every rule below follows
    /// from that single fact — it is not extra policy, it is a shadow of
    /// the lock's. Detection cannot be made order-agnostic: writer
    /// preference is what CREATES the reader-behind-writer deadlocks
    /// these waiter edges catch (drop them and such cycles hang, with no
    /// timeout backstop), and the "ahead of mario" cut is what stops two
    /// writers queued on one key from looking like a cycle. If
    /// sync.RWMutex's discipline ever changes, the "FIFO hand-off" and
    /// "writer preference holds" tests fail here rather than hanging in
    /// production — they pin this contract.
    fn pushBlockers(
        self: *SemaphoreMap,
        entry: *Entry,
        mario: u64,
        want_mode: Mode,
    ) error{OutOfMemory}!void {
        // Holders:
        //   - mario wants exclusive: he waits for all of them.
        //   - mario wants shared: he can share with shared holders, so
        //     only an exclusive holder blocks him.
        for (entry.holders.items) |holder| {
            if (want_mode == .shared and holder.mode == .shared) continue;
            try self.dfs.push(holder.owner_id);
        }
        // Waiters, in registration order, with mario somewhere in the
        // list:
        //   - queued shared waiters never block him — they are granted
        //     alongside him, or stuck behind the same writer he is.
        //   - mario wants exclusive: writers are FIFO among
        //     themselves, so only the exclusives AHEAD of him run
        //     first. Stop at his own slot.
        //   - mario wants shared: mutex writer preference parks readers
        //     while any writer is queued, whenever it arrived, so every
        //     queued exclusive runs first. Scan the whole list.
        for (entry.waiters.items) |waiter| {
            if (waiter.owner_id == mario) {
                if (want_mode == .exclusive) break; // out of for loop
                continue;
            }
            if (waiter.mode == .exclusive) try self.dfs.push(waiter.owner_id);
        }
    }
};
