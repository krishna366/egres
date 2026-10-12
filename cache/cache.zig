// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! S3-FIFO cache policy over caller-owned values.
//!
//! Two FIFO queues and a ghost: S (small, 10%), M (main, 90%), G.
//!
//!   S  — admission filter; cost budget = 10% of total capacity.
//!   M  — proven-reuse entries; cost budget = 90% of total capacity.
//!   G  — key hashes of recently evicted S one-hit-wonders, an
//!          approximate set (`Ghost`) remembering about as many as M
//!          holds entries.
//!
//! The cache owns the policy and the key → value map; the values are
//! the caller's. `V` is a pointer type whose target provides
//!
//!   incrRef() — the cache takes a reference on put and hands one out
//!               on get; the caller decrRefs what get returns.
//!   decrRef() — the cache lets go: evicted, removed, or replaced.
//!   cost()    — what the value charges against the budget: bytes for
//!               a memory buffer, 1 for a disk slot counted in slots.
//!
//! Eviction triggers when a queue's cost would exceed its budget. An
//! entry leaving S unreused is promoted while M is under nine tenths
//! of its budget, so nothing is evicted before the cache is nearly
//! full; the last tenth is kept for entries that proved their reuse.
//! Ring buffers grow dynamically (doubling) when all slots are
//! occupied.
//!
//! - Cache(K, V)        — single shard, not thread-safe.
//! - ShardedCache(K, V) — N shards, each guarded by a mutex; thread-safe.
//!
//! For String keys, the cache takes ownership of the key on put.
//!
//! `charge` says what an entry costs the budget: `.cost_only` is the
//! value's `cost()` alone, for a budget counted in units the cache does
//! not hold itself (disk slots); `.cost_and_overhead` adds the cache's
//! own per-entry bookkeeping, for a byte budget that should cover what
//! the process pays.

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;
const Allocator = std.mem.Allocator;

pub const Ghost = @import("ghost.zig").Ghost;

pub const Charge = enum { cost_only, cost_and_overhead };

fn isSliceOfU8(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .pointer => |p| p.size == .slice and p.child == u8,
        else => false,
    };
}

pub fn hashKey(comptime K: type, key: K) u64 {
    if (comptime isSliceOfU8(K)) {
        return std.hash.Wyhash.hash(0, key);
    } else {
        var h = std.hash.Wyhash.init(0);
        std.hash.autoHash(&h, key);
        return h.final();
    }
}

/// Compile-time check of the value contract described in the header.
fn checkValue(comptime V: type) void {
    const info = @typeInfo(V);
    if (info != .pointer or info.pointer.size != .one)
        @compileError("cache value must be a single-item pointer");
    const T = info.pointer.child;
    inline for (.{ "incrRef", "decrRef", "cost" }) |name| {
        if (!@hasDecl(T, name))
            @compileError(@typeName(T) ++ " lacks " ++ name ++ "()");
    }
}

// ---------------------------------------------------------------------------
// Cache(K, V) — single shard, not thread-safe
// ---------------------------------------------------------------------------

pub fn Cache(
    comptime K: type,
    comptime V: type,
    comptime charge: Charge,
) type {
    comptime checkValue(V);
    return struct {
        const Self = @This();

        /// Which queue holds an entry.
        const Tag = enum(u1) { s_queue, m_queue };

        /// Payload stored in the main hash map: queue tag, ring slot,
        /// value, and frequency bit. Keeping val + freq in the map means
        /// `get` never touches the ring buffers.
        const Entry = struct {
            val: V,
            slot: usize,
            freq: std.atomic.Value(bool),
            tag: Tag,
        };

        /// The key → entry map, String keys specialised. An array hash
        /// map: its index deletes by shifting back, with no tombstones.
        /// `std.HashMap` leaves one per removal and rehashes only when
        /// out of room, which removals keep from happening, so under the
        /// cache's put and evict churn it filled with tombstones until a
        /// missing key probed the whole table (a page-cache run spent 82%
        /// of its CPU in that probe).
        const Map = if (isSliceOfU8(K))
            std.StringArrayHashMapUnmanaged(Entry)
        else
            std.AutoArrayHashMapUnmanaged(K, Entry);

        /// The bookkeeping an entry costs beyond its value: the key in
        /// the map and in one ring, the map's entry and its index slot.
        const fixed_overhead: usize = switch (charge) {
            .cost_only => 0,
            .cost_and_overhead => 2 * @sizeOf(K) + @sizeOf(Entry) + 8,
        };

        /// What `key` → `val` charges the budget. A String key's bytes
        /// are the cache's too once it owns the key.
        fn entryCost(key: K, val: V) usize {
            const cost = val.cost() + fixed_overhead;
            if (comptime (charge == .cost_and_overhead and isSliceOfU8(K)))
                return cost + key.len;
            return cost;
        }

        /// One FIFO of keys with a cost budget. The entries (value,
        /// freq, slot) live in the map; the ring holds keys only, so
        /// `get` never touches it. `len` counts slots from tail to
        /// head, dead ones included: a slot is live while the map's
        /// entry still points at it (`liveSlot`).
        const Queue = struct {
            ring: std.ArrayList(K),
            head: usize = 0,
            tail: usize = 0,
            len: usize = 0,
            cap: usize, // cost budget
            cost: usize = 0, // live cost

            const Popped = struct { key: K, slot: usize };

            fn init(alloc: Allocator, cap: usize, slots: usize) !Queue {
                var ring = try std.ArrayList(K).initCapacity(alloc, slots);
                ring.expandToCapacity();
                return .{ .ring = ring, .cap = cap };
            }

            fn deinit(self: *Queue, alloc: Allocator) void {
                self.ring.deinit(alloc);
            }

            /// Whether `cost` more stays within the budget.
            fn fits(self: *const Queue, cost: usize) bool {
                return self.cost + cost <= self.cap;
            }

            /// Whether an eviction is due for `cost` more: it does not
            /// fit and there is something to evict.
            fn needsRoom(self: *const Queue, cost: usize) bool {
                return !self.fits(cost) and self.len > 0;
            }

            /// Whether `cost` more fits with a tenth of the budget left
            /// over: the reserve for entries that proved their reuse.
            fn fitsWithReserve(self: *const Queue, cost: usize) bool {
                return self.cost + cost + self.cap / 10 <= self.cap;
            }

            /// Every ring slot taken: the next push needs `grow`.
            fn full(self: *const Queue) bool {
                return self.len == self.ring.items.len;
            }

            /// The slot at the head takes `key`; returns the slot.
            fn push(self: *Queue, key: K) usize {
                std.debug.assert(!self.full());
                const slot = self.head;
                self.ring.items[slot] = key;
                self.head = (self.head + 1) % self.ring.items.len;
                self.len += 1;
                return slot;
            }

            /// The slot at the tail, passed over; null when empty.
            fn pop(self: *Queue) ?Popped {
                if (self.len == 0) return null;
                const slot = self.tail;
                self.tail = (self.tail + 1) % self.ring.items.len;
                self.len -= 1;
                return .{ .key = self.ring.items[slot], .slot = slot };
            }

            /// Double the ring. Slots [tail..old_cap) stay in place;
            /// only the wrapped portion [0..tail) moves to
            /// [old_cap..old_cap+tail) so the ring stays contiguous
            /// from tail → head:
            ///
            ///   Before (cap=8, tail=3):  [e5 e6 e7 | e0 e1 e2 e3 e4]
            ///                             0  1  2    3  4  5  6  7
            ///                                        ^tail
            ///
            ///   After  (cap=16):  [__ __ __ | e0 .. e4 | e5 e6 e7 | __ .. __]
            ///                      0  1  2    3     7    8  9  10   11 .. 15
            ///                                 ^tail                 ^head=11
            ///
            /// Returns the old capacity: the moved slots are
            /// [old_cap..old_cap+tail), for the caller's map fixup.
            fn grow(self: *Queue, alloc: Allocator) !usize {
                const old_cap = self.ring.items.len;
                // ensureTotalCapacity grows super-linearly beyond the
                // ask; expandToCapacity makes the ring whatever it gave.
                try self.ring.ensureTotalCapacity(alloc, old_cap * 2);
                self.ring.expandToCapacity();
                const moved = self.ring.items[old_cap .. old_cap + self.tail];
                @memcpy(moved, self.ring.items[0..self.tail]);
                self.head = old_cap + self.tail;
                return old_cap;
            }
        };

        allocator: Allocator,
        map: Map,
        ghost: Ghost,
        /// Puts of a key the ghost remembered: misses a cache a
        /// few times larger would have served. Measured at 0.07% of
        /// misses on the orders workload: the misses are cold, and a
        /// larger or compressed cache would not catch them.
        ghost_hits: u64 = 0,

        s: Queue, // admission: a tenth of the budget
        m: Queue, // proven reuse: the rest

        // ----------------------------------------------------------------
        // Public API
        // ----------------------------------------------------------------

        pub fn init(alloc: Allocator, budget: usize) !Self {
            const s_cap = @max(1, budget / 10);
            var ghost = try Ghost.init(alloc);
            errdefer ghost.deinit(alloc);
            const slots = @max(16, s_cap / 4096);
            var s = try Queue.init(alloc, s_cap, slots);
            errdefer s.deinit(alloc);
            const m = try Queue.init(alloc, budget - s_cap, 8 * slots);
            return Self{
                .allocator = alloc,
                .map = .empty,
                .ghost = ghost,
                .s = s,
                .m = m,
            };
        }

        pub fn deinit(self: *Self) void {
            // Dead ring slots own their keys (see `remove`); the map
            // must still be there to tell them from live ones.
            self.freeDeadKeys(.s_queue);
            self.freeDeadKeys(.m_queue);
            var it = self.map.iterator();
            while (it.next()) |kv| {
                self.freeKey(kv.key_ptr.*);
                kv.value_ptr.val.decrRef();
            }
            self.map.deinit(self.allocator);
            self.ghost.deinit(self.allocator);
            self.s.deinit(self.allocator);
            self.m.deinit(self.allocator);
        }

        /// Returns the cached value for `key`, marking it as recently
        /// used. The returned value has been incrRef'd; the caller must
        /// decrRef when done. Only touches the map; no ring access.
        pub fn get(self: *Self, key: K) ?V {
            const entry = self.map.getPtr(key) orelse return null;
            entry.freq.store(true, .release);
            _ = entry.val.incrRef();
            return entry.val;
        }

        /// `get` without the reuse mark: for a writer adding to a
        /// cached value, which says nothing about readers wanting it.
        pub fn peek(self: *Self, key: K) ?V {
            const entry = self.map.getPtr(key) orelse return null;
            _ = entry.val.incrRef();
            return entry.val;
        }

        /// Inserts `key` → `val`. The cache takes its own reference on
        /// `val` (incrRef); the caller retains their existing ref. Key
        /// ownership is transferred for String keys. On error, the key
        /// is freed (for String) but val is NOT decrRef'd.
        pub fn put(self: *Self, key: K, val: V) !void {
            // Key already present: update in place, adjusting costs.
            if (self.map.getPtr(key)) |entry| {
                const old_cost = entryCost(key, entry.val);
                const new_cost = entryCost(key, val);
                self.freeKey(key);

                entry.val.decrRef(); // release old
                _ = val.incrRef(); // cache holds new
                entry.val = val;
                const q = self.queue(entry.tag);
                q.cost = q.cost - old_cost + new_cost;
                return;
            }

            const h = hashKey(K, key);
            if (self.ghost.contains(h)) {
                self.ghost_hits += 1;
                // Ghost hit: skip S, go directly to M.
                // pushMain takes ownership of a ref, so incrRef here.
                _ = val.incrRef();
                self.pushMain(key, val) catch |err| {
                    val.decrRef(); // undo incrRef on error
                    return err;
                };
                return;
            }

            // Regular S insertion with cost-based eviction.
            const cost = entryCost(key, val);
            while (self.s.needsRoom(cost)) {
                self.evictSmall() catch |err| {
                    self.freeKey(key);
                    return err;
                };
            }
            _ = val.incrRef(); // cache's ref
            self.insert(.s_queue, key, val) catch |err| {
                val.decrRef(); // undo cache's ref
                self.freeKey(key);
                return err;
            };
            self.s.cost += cost;
        }

        /// Inserts `key` → `val` straight into M, the way a put that
        /// proved its reuse before a restart would have ended up. Evicts
        /// from M if over budget. Same ownership rules as `put`.
        pub fn restore(self: *Self, key: K, val: V) !void {
            if (self.map.contains(key)) return self.put(key, val);
            _ = val.incrRef();
            self.pushMain(key, val) catch |err| {
                val.decrRef();
                return err;
            };
        }

        /// Evicts until `cost` more fits: S first, then M. For a caller
        /// that must obtain storage (a freed slot) before it can `put`.
        pub fn makeRoom(self: *Self, cost: usize) void {
            while (self.s.needsRoom(cost))
                self.evictSmall() catch break;
            while (self.totalCost() + cost > self.capacity() and
                self.m.len > 0)
                self.evictMain();
        }

        /// Whether `key` is cached, without touching its freq bit.
        pub fn contains(self: *const Self, key: K) bool {
            return self.map.contains(key);
        }

        /// Drop `key` now, whatever queue holds it: the map entry goes,
        /// the cache's ref on the value is released and the queue's cost
        /// shrinks. The ring slot is left as it is — rings cannot be
        /// spliced — and the eviction paths recognise it as dead when
        /// they reach it: its key is no longer in the map, or the map's
        /// entry for that key points at a newer slot. The slot keeps
        /// the key (a String is hashed to tell a dead slot from a live
        /// one) and whoever reclaims the slot frees it. Returns whether
        /// the key was present.
        pub fn remove(self: *Self, key: K) bool {
            const entry = self.map.fetchSwapRemove(key) orelse return false;
            const cost = entryCost(entry.key, entry.value.val);
            self.queue(entry.value.tag).cost -= cost;
            entry.value.val.decrRef();
            return true;
        }

        /// Total live cost in S + M, in whatever unit `V.cost()`
        /// charges: bytes for the block cache, slots for the chunk cache.
        pub fn totalCost(self: *const Self) usize {
            return self.s.cost + self.m.cost;
        }

        /// The cost budget, S and M together, in the same unit.
        pub fn capacity(self: *const Self) usize {
            return self.s.cap + self.m.cap;
        }

        /// Re-budgets the cache to `budget`, S keeping its tenth. A
        /// shrink evicts down to the new budget here, so the storage is
        /// released now and not at the next put. The ghost keeps its
        /// size: it was sized once and holds hashes only.
        pub fn setCapacity(self: *Self, budget: usize) void {
            self.s.cap = @max(1, budget / 10);
            self.m.cap = budget - self.s.cap;
            while (self.s.needsRoom(0))
                self.evictSmall() catch break;
            while (self.m.needsRoom(0))
                self.evictMain();
        }

        /// Live entries. Not the ring occupancy: a slot left behind by
        /// `remove` stays in its ring until eviction reaches it, but the
        /// entry is gone.
        pub fn count(self: *const Self) usize {
            return self.map.count();
        }

        // ----------------------------------------------------------------
        // Internal helpers
        // ----------------------------------------------------------------

        fn queue(self: *Self, tag: Tag) *Queue {
            return if (tag == .s_queue) &self.s else &self.m;
        }

        /// Free a key if K = String; no-op for scalar K.
        fn freeKey(self: *Self, key: K) void {
            if (comptime isSliceOfU8(K)) self.allocator.free(key);
        }

        /// Frees the keys of the dead slots in queue `tag`, for deinit.
        fn freeDeadKeys(self: *Self, comptime tag: Tag) void {
            if (comptime !isSliceOfU8(K)) return;
            const q = self.queue(tag);
            for (0..q.len) |i| {
                const slot = (q.tail + i) % q.ring.items.len;
                const key = q.ring.items[slot];
                if (!self.liveSlot(tag, key, slot)) self.freeKey(key);
            }
        }

        /// Whether the ring slot `slot` of queue `tag`, holding `key`,
        /// still backs a live entry. False for a slot left behind by
        /// `remove`, and for one made stale by a remove followed by a
        /// re-put of the same key (the map then points at the newer slot).
        fn liveSlot(self: *Self, comptime tag: Tag, key: K, slot: usize) bool {
            const entry = self.map.getPtr(key) orelse return false;
            return entry.tag == tag and entry.slot == slot;
        }

        /// A new entry for `key` → `val` in queue `tag`, at its head.
        /// The queue's cost is the caller's to add; nothing to undo on
        /// error.
        fn insert(self: *Self, comptime tag: Tag, key: K, val: V) !void {
            const q = self.queue(tag);
            if (q.full()) try self.growQueue(tag);
            try self.map.put(self.allocator, key, .{
                .tag = tag,
                .slot = q.head,
                .val = val,
                .freq = std.atomic.Value(bool).init(false),
            });
            _ = q.push(key);
        }

        /// Doubles queue `tag`'s ring and points the moved slots'
        /// entries at their new slots. A dead slot has no entry to move.
        fn growQueue(self: *Self, comptime tag: Tag) !void {
            const q = self.queue(tag);
            const old_cap = try q.grow(self.allocator);
            for (old_cap..q.head) |new_slot| {
                const key = q.ring.items[new_slot];
                if (self.liveSlot(tag, key, new_slot - old_cap))
                    self.map.getPtr(key).?.slot = new_slot;
            }
        }

        /// Push (key, val) into M, evicting first if M is over budget.
        /// Takes ownership of one ref on val AND key (for String).
        /// On error, val is decrRef'd and key is freed.
        fn pushMain(self: *Self, key: K, val: V) !void {
            const cost = entryCost(key, val);
            while (self.m.needsRoom(cost))
                self.evictMain();
            self.insert(.m_queue, key, val) catch |err| {
                val.decrRef();
                self.freeKey(key);
                return err;
            };
            self.m.cost += cost;
        }

        /// Evict the oldest live S entry. freq=true, or M under budget
        /// → promote to M (transferring key ownership); otherwise →
        /// record hash in G and free key + value. Dead slots left by
        /// `remove` are skipped; if S holds nothing live this returns
        /// having only reclaimed slots.
        fn evictSmall(self: *Self) !void {
            std.debug.assert(self.s.len > 0);
            while (self.s.pop()) |p| {
                if (!self.liveSlot(.s_queue, p.key, p.slot)) {
                    self.freeKey(p.key);
                    continue;
                }
                const entry = self.map.get(p.key).?;
                _ = self.map.swapRemove(p.key);
                const cost = entryCost(p.key, entry.val);
                self.s.cost -= cost;
                // Promoted on reuse — or on no reuse while M is under
                // its reserve: until the cache is nearly full nothing
                // is worth evicting, and M's own pass drops a
                // never-reused entry first. The reserve keeps a reused
                // entry from evicting for the slack an unreused one
                // took.
                const reuse = entry.freq.load(.acquire);
                if (reuse or self.m.fitsWithReserve(cost)) {
                    return self.pushMain(p.key, entry.val);
                }
                const h = hashKey(K, p.key);
                self.ghost.insert(self.allocator, h, self.m.len);
                self.freeKey(p.key);
                entry.val.decrRef();
                return;
            }
        }

        /// Evict one live entry from M. Entries with freq=true are
        /// reinserted at the M head with freq reset to false; the loop
        /// continues until a freq=false entry is found and freed. Dead
        /// slots left by `remove` are skipped; if M holds nothing live
        /// this returns having only reclaimed slots.
        fn evictMain(self: *Self) void {
            while (self.m.pop()) |p| {
                if (!self.liveSlot(.m_queue, p.key, p.slot)) {
                    self.freeKey(p.key);
                    continue;
                }
                const entry = self.map.getPtr(p.key).?;
                if (entry.freq.load(.acquire)) {
                    // Reinsert at head with freq=false (no cost change);
                    // the pop made room, so no growth.
                    entry.slot = self.m.push(p.key);
                    entry.freq.store(false, .release);
                    continue;
                }
                self.m.cost -= entryCost(p.key, entry.val);
                entry.val.decrRef();
                _ = self.map.swapRemove(p.key);
                self.freeKey(p.key);
                return;
            }
        }
    };
}

// ---------------------------------------------------------------------------
// ShardedCache(K, V) — N shards, thread-safe
// ---------------------------------------------------------------------------

pub fn ShardedCache(
    comptime K: type,
    comptime V: type,
    comptime charge: Charge,
) type {
    return struct {
        const Self = @This();

        const Shard = struct {
            cache: Cache(K, V, charge),
            lock: fiber.RWMutex(.untracked),
        };

        allocator: Allocator,
        shards: []Shard,
        /// `get` outcomes, for the stats dump.
        hits: std.atomic.Value(u64) = .init(0),
        misses: std.atomic.Value(u64) = .init(0),

        pub fn init(
            alloc: Allocator,
            total_capacity: usize,
            shard_count: usize,
        ) !Self {
            std.debug.assert(shard_count > 0);
            const per = @max(total_capacity / shard_count, 2);
            const shards = try alloc.alloc(Shard, shard_count);
            errdefer alloc.free(shards);
            var n: usize = 0;
            errdefer for (shards[0..n]) |*s| s.cache.deinit();
            for (shards) |*s| {
                s.cache = try Cache(K, V, charge).init(alloc, per);
                s.lock = .{};
                n += 1;
            }
            return Self{ .allocator = alloc, .shards = shards };
        }

        pub fn deinit(self: *Self) void {
            for (self.shards) |*s| {
                s.cache.deinit();
                s.lock.deinit(); // no-op while shards are .untracked
            }
            self.allocator.free(self.shards);
        }

        fn pickShard(self: *const Self, key: K) *Shard {
            return &self.shards[hashKey(K, key) % self.shards.len];
        }

        /// A shared lock suffices: `get` only touches atomics (freq bit,
        /// refcount) in a map that no put is reshaping meanwhile.
        pub fn get(self: *Self, key: K) ?V {
            const s = self.pickShard(key);
            s.lock.lockShared(@src());
            defer s.lock.unlockShared();
            const val = s.cache.get(key);
            const counter = if (val != null) &self.hits else &self.misses;
            _ = counter.fetchAdd(1, .monotonic);
            return val;
        }

        /// See `Cache.put` for ownership semantics.
        pub fn put(self: *Self, key: K, val: V) !void {
            const s = self.pickShard(key);
            s.lock.lock(@src());
            defer s.lock.unlock();
            return s.cache.put(key, val);
        }

        /// See `Cache.remove`.
        pub fn remove(self: *Self, key: K) bool {
            const s = self.pickShard(key);
            s.lock.lock(@src());
            defer s.lock.unlock();
            return s.cache.remove(key);
        }

        /// The cost the small queues hold in all: what a burst of new
        /// entries can be admitted without evicting its own earlier
        /// entries before they are touched (a new entry sits in S until
        /// its first hit). A prefetch sizes its batch by this.
        pub fn admissionBudget(self: *const Self) usize {
            var total: usize = 0;
            for (self.shards) |*s| total += s.cache.s.cap;
            return total;
        }

        /// Ghost hits across shards (`Cache.ghost_hits`).
        pub fn ghostHits(self: *Self) u64 {
            var total: u64 = 0;
            for (self.shards) |*s| {
                s.lock.lockShared(@src());
                defer s.lock.unlockShared();
                total += s.cache.ghost_hits;
            }
            return total;
        }

        /// Re-budgets every shard to its share of `total_capacity`
        /// (`Cache.setCapacity`).
        pub fn setCapacity(self: *Self, total_capacity: usize) void {
            const per = @max(total_capacity / self.shards.len, 2);
            for (self.shards) |*s| {
                s.lock.lock(@src());
                defer s.lock.unlock();
                s.cache.setCapacity(per);
            }
        }

        /// The cost budget across shards.
        pub fn capacity(self: *Self) usize {
            var total: usize = 0;
            for (self.shards) |*s| {
                s.lock.lockShared(@src());
                defer s.lock.unlockShared();
                total += s.cache.capacity();
            }
            return total;
        }

        /// Approximate total count across shards.
        pub fn count(self: *Self) usize {
            var total: usize = 0;
            for (self.shards) |*s| {
                s.lock.lockShared(@src());
                defer s.lock.unlockShared();
                total += s.cache.count();
            }
            return total;
        }

        /// Approximate total cost across shards.
        pub fn totalCost(self: *Self) usize {
            var total: usize = 0;
            for (self.shards) |*s| {
                s.lock.lockShared(@src());
                defer s.lock.unlockShared();
                total += s.cache.totalCost();
            }
            return total;
        }
    };
}
