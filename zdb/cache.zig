// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! S3-FIFO block cache for ZDB.
//!
//! Two FIFO queues and a ghost: S (small, 10%), M (main, 90%), G.
//!
//!   S  — admission filter; byte budget = 10% of total capacity.
//!   M  — proven-reuse entries; byte budget = 90% of total capacity.
//!   G  — key hashes of recently evicted S one-hit-wonders, an
//!          approximate set (`Ghost`) remembering about as many as M
//!          holds entries.
//!
//! Eviction triggers when s_bytes + entry_bytes > s_cap_bytes (for S) or
//! m_bytes + entry_bytes > m_cap_bytes (for M), where entry_bytes =
//! keySize(key) + val.len.  Ring buffers grow dynamically (doubling) when
//! all slots are occupied.
//!
//! - Cache(K)        — single shard, not thread-safe.
//! - ShardedCache(K) — N shards, each guarded by a Mutex; thread-safe.
//!
//! Values are ref-counted `*Value` structs. The cache incrRefs on put/get and
//! decrRefs on eviction/deinit. Callers must decrRef values returned by get.
//! For String keys, the cache takes ownership of the key on put.

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;
const Allocator = std.mem.Allocator;

// Re-export the canonical refcounted buffer from utils. Kept under the old
// name so existing cache call sites (`_cache.Value.init`, `incrRef`, etc.)
// keep compiling without churn.
pub const Value = utils.RefValue;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

/// Returns the byte footprint of a key: slice length for String,
/// or @sizeOf(K) for scalar types.
fn keySize(comptime K: type, key: K) usize {
    if (comptime isSliceOfU8(K)) return key.len;
    return @sizeOf(K);
}

/// What S evicted lately, as an approximate set: two bloom filters.
/// Inserts go to the active one and a lookup asks both; once the
/// active one has taken `quota` inserts the other is cleared and
/// takes over, so a hash is remembered for one to two quotas of
/// evictions, about what a ring of `quota` slots remembers, at 4
/// bytes a slot instead of 32. S3-FIFO keeps as many ghost entries as
/// M holds, so each generation's quota is M's entry count when it
/// starts. The error is bounded whatever the workload: a remembered
/// hash is never inserted again (its key goes to M), so a filter
/// holds at most `quota` hashes and its false positive rate stays at
/// the design point; and a false positive only puts a block into M
/// at freq 0, which M's next pass drops.
pub const Ghost = struct {
    filters: [2]BitSet,
    active: u1 = 0,
    /// Inserts into the active filter so far.
    inserted: usize = 0,
    /// Inserts the active generation takes.
    quota: usize = min_quota,

    const BitSet = std.bit_set.DynamicBitSetUnmanaged;

    /// 16 bits a key and 11 probes: about 0.05% false positives at
    /// the quota, 0.1% across both filters. Kept low so a long scan
    /// leaks almost nothing into M.
    pub const bits_per_key = 16;
    pub const probes = 11;
    /// A generation's floor, for an M still filling.
    pub const min_quota = 1024;

    pub fn init(alloc: Allocator) !Ghost {
        var a = try BitSet.initEmpty(alloc, bitsFor(min_quota));
        errdefer a.deinit(alloc);
        const b = try BitSet.initEmpty(alloc, bitsFor(min_quota));
        return .{ .filters = .{ a, b } };
    }

    pub fn deinit(self: *Ghost, alloc: Allocator) void {
        self.filters[0].deinit(alloc);
        self.filters[1].deinit(alloc);
    }

    /// Bits for a filter taking `quota` hashes: a power of two, so a
    /// probe wraps with a mask.
    fn bitsFor(quota: usize) usize {
        return std.math.ceilPowerOfTwo(usize, quota * bits_per_key) catch
            unreachable;
    }

    /// The bit `h`'s probe `i` lands on in `f`: double hashing over
    /// the two halves of the hash.
    inline fn bit(f: BitSet, h: u64, i: u64) usize {
        const h1: u64 = @as(u32, @truncate(h));
        const h2: u64 = @as(u32, @truncate(h >> 32)) | 1;
        return @intCast((h1 +% i *% h2) & (f.bit_length - 1));
    }

    /// `m_len` is M's entry count now. Once the active filter has
    /// taken `quota` inserts the other takes over, sized for `m_len`:
    /// reallocated when its bit count changes, cleared when not. If
    /// the allocation fails the old size serves one more generation.
    pub fn insert(self: *Ghost, alloc: Allocator, h: u64, m_len: usize) void {
        if (self.inserted == self.quota) self.turnover(alloc, m_len);
        var f = &self.filters[self.active];
        for (0..probes) |i| f.set(bit(f.*, h, i));
        self.inserted += 1;
    }

    fn turnover(self: *Ghost, alloc: Allocator, m_len: usize) void {
        self.active +%= 1;
        self.inserted = 0;
        self.quota = @max(min_quota, m_len);

        var f = &self.filters[self.active];
        const bits = bitsFor(self.quota);
        if (bits == f.bit_length) {
            f.unsetAll();
            return;
        }
        const next = BitSet.initEmpty(alloc, bits) catch {
            f.unsetAll();
            return;
        };
        f.deinit(alloc);
        f.* = next;
    }

    pub fn contains(self: *const Ghost, h: u64) bool {
        return containsIn(self.filters[self.active], h) or
            containsIn(self.filters[self.active +% 1], h);
    }

    fn containsIn(f: BitSet, h: u64) bool {
        for (0..probes) |i| {
            if (!f.isSet(bit(f, h, i))) return false;
        }
        return true;
    }
};

// ---------------------------------------------------------------------------
// Cache(K) — single shard, not thread-safe
// ---------------------------------------------------------------------------

pub fn Cache(comptime K: type) type {
    return struct {
        const Self = @This();

        /// Which queue holds an entry.
        const Tag = enum(u1) { s_queue, m_queue };

        /// Payload stored in the main hash map: queue tag, ring slot, value,
        /// and frequency bit.  Keeping val + freq in the map means `get` never
        /// touches the ring buffers.
        const Entry = struct {
            val: *Value,
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

        // ----------------------------------------------------------------
        // Fields
        // ----------------------------------------------------------------

        allocator: Allocator,
        map: Map,
        ghost: Ghost,
        /// Puts of a key the ghost remembered: misses a cache a
        /// few times larger would have served. Measured at 0.07% of
        /// misses on the orders workload: the misses are cold, and a
        /// larger or compressed cache would not catch them.
        ghost_hits: u64 = 0,

        /// S ring: stores only keys; val + freq live in the map Entry.
        s_ring: std.ArrayList(K),
        s_head: usize, // absolute head counter
        s_tail: usize, // absolute tail counter
        s_len: usize, // live entry count in S
        s_cap_bytes: usize, // byte budget for S (10% of total capacity)
        s_bytes: usize, // current live bytes in S

        /// M ring: stores only keys; val + freq live in the map Entry.
        m_ring: std.ArrayList(K),
        m_head: usize,
        m_tail: usize,
        m_len: usize,
        m_cap_bytes: usize, // byte budget for M (90% of total capacity)
        m_bytes: usize, // current live bytes in M

        // ----------------------------------------------------------------
        // Public API
        // ----------------------------------------------------------------

        pub fn init(alloc: Allocator, budget: usize) !Self {
            const s_cap_bytes = @max(1, budget / 10);
            const m_cap_bytes = budget - s_cap_bytes;
            var ghost = try Ghost.init(alloc);
            errdefer ghost.deinit(alloc);

            const s_ring_len = @max(16, s_cap_bytes / 4096);
            var s_ring = try std.ArrayList(K).initCapacity(alloc, s_ring_len);
            errdefer s_ring.deinit(alloc);
            s_ring.expandToCapacity();

            var m_ring = try std.ArrayList(K).initCapacity(alloc, 8 * s_ring_len);
            errdefer m_ring.deinit(alloc);
            m_ring.expandToCapacity();

            return Self{
                .allocator = alloc,
                .map = .empty,
                .ghost = ghost,
                .s_ring = s_ring,
                .s_head = 0,
                .s_tail = 0,
                .s_len = 0,
                .s_cap_bytes = s_cap_bytes,
                .s_bytes = 0,
                .m_ring = m_ring,
                .m_head = 0,
                .m_tail = 0,
                .m_len = 0,
                .m_cap_bytes = m_cap_bytes,
                .m_bytes = 0,
            };
        }

        pub fn deinit(self: *Self) void {
            var it = self.map.iterator();
            while (it.next()) |kv| {
                self.freeKey(kv.key_ptr.*);
                kv.value_ptr.val.decrRef();
            }
            self.map.deinit(self.allocator);
            self.ghost.deinit(self.allocator);
            self.s_ring.deinit(self.allocator);
            self.m_ring.deinit(self.allocator);
        }

        /// Returns the cached value for `key`, marking it as recently used.
        /// The returned *Value has been incrRef'd; the caller must decrRef
        /// when done.  Only touches the map; no ring access needed.
        pub fn get(self: *Self, key: K) ?*Value {
            const entry = self.map.getPtr(key) orelse return null;
            entry.freq.store(true, .release);
            return entry.val.incrRef();
        }

        /// Inserts `key` → `val` into the cache. The cache takes its own
        /// reference on `val` (incrRef); the caller retains their existing ref.
        /// Key ownership is transferred for String keys.
        /// On error, the key is freed (for String) but val is NOT decrRef'd.
        pub fn put(self: *Self, key: K, val: *Value) !void {
            // Key already present: update value in-place, adjusting byte counts.
            if (self.map.getPtr(key)) |entry| {
                self.freeKey(key);

                const old_len = entry.val.data.len;
                entry.val.decrRef(); // release old
                _ = val.incrRef(); // cache holds new
                entry.val = val;
                switch (entry.tag) {
                    .s_queue => self.s_bytes = self.s_bytes - old_len + val.data.len,
                    .m_queue => self.m_bytes = self.m_bytes - old_len + val.data.len,
                }
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

            // Regular S insertion with byte-based eviction.
            const entry_bytes = keySize(K, key) + val.data.len;
            while (self.s_bytes + entry_bytes > self.s_cap_bytes and self.s_len > 0) {
                self.evictSmall() catch |err| {
                    self.freeKey(key);
                    return err;
                };
            }
            if (self.s_len == self.s_ring.items.len) {
                self.growRing(.s_queue) catch |err| {
                    self.freeKey(key);
                    return err;
                };
            }
            const slot = self.s_head;
            _ = val.incrRef(); // cache's ref
            self.map.put(self.allocator, key, .{
                .tag = .s_queue,
                .slot = slot,
                .val = val,
                .freq = std.atomic.Value(bool).init(false),
            }) catch |err| {
                val.decrRef(); // undo cache's ref
                self.freeKey(key);
                return err;
            };
            self.s_ring.items[slot] = key;
            self.s_head = (self.s_head + 1) % self.s_ring.items.len;
            self.s_len += 1;
            self.s_bytes += entry_bytes;
        }

        /// Drop `key` now, whatever queue holds it: the map entry goes, the
        /// cache's ref on the value is released (the bytes come back as
        /// soon as any reader holding the value is done) and the queue's
        /// byte count shrinks. The ring slot is left as it is — rings
        /// cannot be spliced — and the eviction paths recognise it as
        /// dead when they reach it: its key is no longer in the map, or
        /// the map's entry for that key points at a newer slot. Returns
        /// whether the key was present.
        pub fn remove(self: *Self, key: K) bool {
            const entry = self.map.fetchSwapRemove(key) orelse return false;
            const bytes = keySize(K, entry.key) + entry.value.val.data.len;
            switch (entry.value.tag) {
                .s_queue => self.s_bytes -= bytes,
                .m_queue => self.m_bytes -= bytes,
            }
            entry.value.val.decrRef();
            self.freeKey(entry.key);
            return true;
        }

        /// Whether the ring slot `slot` of queue `tag`, holding `key`, still
        /// backs a live entry. False for a slot left behind by `remove`,
        /// and for one made stale by a remove followed by a re-put of the
        /// same key (the map then points at the newer slot).
        fn liveSlot(self: *Self, comptime tag: Tag, key: K, slot: usize) bool {
            const entry = self.map.getPtr(key) orelse return false;
            return entry.tag == tag and entry.slot == slot;
        }

        /// Total live bytes in S + M.
        pub fn byteSize(self: *const Self) usize {
            return self.s_bytes + self.m_bytes;
        }

        /// The byte budget, S and M together.
        pub fn capacity(self: *const Self) usize {
            return self.s_cap_bytes + self.m_cap_bytes;
        }

        /// Re-budgets the cache to `budget` bytes, S keeping its tenth.
        /// A shrink evicts down to the new budget here, so the memory is
        /// released now and not at the next put. The ghost keeps its
        /// size: it was sized once and holds hashes only.
        pub fn setCapacity(self: *Self, budget: usize) void {
            self.s_cap_bytes = @max(1, budget / 10);
            self.m_cap_bytes = budget - self.s_cap_bytes;
            while (self.s_bytes > self.s_cap_bytes and self.s_len > 0)
                self.evictSmall() catch break;
            while (self.m_bytes > self.m_cap_bytes and self.m_len > 0)
                self.evictMain();
        }

        /// Total live entry count (S + M).
        /// Live entries. Not the ring occupancy: a slot left behind by
        /// `remove` stays in its ring until eviction reaches it, but the
        /// entry is gone.
        pub fn count(self: *const Self) usize {
            return self.map.count();
        }

        // ----------------------------------------------------------------
        // Internal helpers
        // ----------------------------------------------------------------

        /// Free a key if K = String; no-op for scalar K.
        fn freeKey(self: *Self, key: K) void {
            if (comptime isSliceOfU8(K)) self.allocator.free(key);
        }

        /// Double the ring capacity. Entries at [tail..old_cap) stay in place.
        /// Only the wrapped portion [0..tail) must move to [old_cap..old_cap+tail)
        /// so the ring remains contiguous from tail → head:
        ///
        ///   Before (cap=8, tail=3):  [e5 e6 e7 | e0 e1 e2 e3 e4]
        ///                             0  1  2    3  4  5  6  7
        ///                                        ^tail
        ///
        ///   After  (cap=16):         [__ __ __ | e0 e1 e2 e3 e4 | e5 e6 e7 | __ __ __ __ __]
        ///                             0  1  2    3  4  5  6  7     8  9  10   11 .. 15
        ///                                        ^tail                        ^head=11
        ///
        /// Slots [tail..old_cap) are untouched. Only the `tail` relocated entries
        /// need map fixup via direct getPtr lookups (no full map iteration).
        fn growRing(self: *Self, comptime tag: Tag) !void {
            const ring = if (tag == .s_queue) &self.s_ring else &self.m_ring;
            const tail = if (tag == .s_queue) self.s_tail else self.m_tail;
            const old_cap = ring.items.len;

            // ensureTotalCapacity would grow by at least the new_capacity. I'm seeing it
            // grow beyond that because it grows super linearly.
            try ring.ensureTotalCapacity(self.allocator, old_cap * 2);
            ring.expandToCapacity();

            if (tail > 0) {
                // Move the entries up until the tail to live together with the
                // rest in the doubled memory slot. See comment above.
                @memcpy(ring.items[old_cap .. old_cap + tail], ring.items[0..tail]);
                for (old_cap..old_cap + tail) |new_slot| {
                    // A dead slot (see `remove`) has no entry to move.
                    const old_slot = new_slot - old_cap;
                    if (self.liveSlot(tag, ring.items[new_slot], old_slot))
                        self.map.getPtr(ring.items[new_slot]).?.slot = new_slot;
                }
            }
            if (tag == .s_queue) {
                self.s_head = old_cap + tail;
            } else {
                self.m_head = old_cap + tail;
            }
        }

        /// Push (key, val) into M, evicting first if M is over byte budget.
        /// Takes ownership of one ref on val AND key (for String).
        /// On error, val is decrRef'd and key is freed.
        fn pushMain(self: *Self, key: K, val: *Value) !void {
            const entry_bytes = keySize(K, key) + val.data.len;
            while (self.m_bytes + entry_bytes > self.m_cap_bytes and self.m_len > 0)
                self.evictMain();

            if (self.m_len == self.m_ring.items.len) {
                self.growRing(.m_queue) catch |err| {
                    self.freeKey(key);
                    val.decrRef();
                    return err;
                };
            }
            const slot = self.m_head;
            self.map.put(self.allocator, key, .{
                .tag = .m_queue,
                .slot = slot,
                .val = val,
                .freq = std.atomic.Value(bool).init(false),
            }) catch |err| {
                val.decrRef();
                self.freeKey(key);
                return err;
            };
            self.m_ring.items[slot] = key;
            self.m_head = (self.m_head + 1) % self.m_ring.items.len;
            self.m_len += 1;
            self.m_bytes += entry_bytes;
        }

        /// Evict the oldest live S entry.  freq=true → promote to M
        /// (transferring key ownership); freq=false → record hash in G and
        /// free key + value. Dead slots left by `remove` are skipped; if S
        /// holds nothing live this returns having only reclaimed slots.
        fn evictSmall(self: *Self) !void {
            std.debug.assert(self.s_len > 0);
            while (self.s_len > 0) {
                const slot = self.s_tail;
                const key = self.s_ring.items[slot];
                self.s_tail = (self.s_tail + 1) % self.s_ring.items.len;
                self.s_len -= 1;
                if (!self.liveSlot(.s_queue, key, slot)) continue;

                const entry = self.map.get(key).?;
                _ = self.map.swapRemove(key);
                self.s_bytes -= keySize(K, key) + entry.val.data.len;
                if (entry.freq.load(.acquire)) {
                    return self.pushMain(key, entry.val);
                } else {
                    const h = hashKey(K, key);
                    self.ghost.insert(self.allocator, h, self.m_len);
                    self.freeKey(key);
                    entry.val.decrRef();
                }
                return;
            }
        }

        /// Evict one live entry from M.  Entries with freq=true are
        /// reinserted at the M head with freq reset to false; the loop
        /// continues until a freq=false entry is found and freed. Dead
        /// slots left by `remove` are skipped; if M holds nothing live this
        /// returns having only reclaimed slots.
        fn evictMain(self: *Self) void {
            while (self.m_len > 0) {
                const slot = self.m_tail;
                const key = self.m_ring.items[slot];

                // Advance tail as we already considered this slot.
                self.m_tail = (self.m_tail + 1) % self.m_ring.items.len;
                self.m_len -= 1;
                if (!self.liveSlot(.m_queue, key, slot)) continue;
                const entry = self.map.getPtr(key).?;
                if (entry.freq.load(.acquire)) {
                    // Reinsert at head with freq=false (no net byte change).
                    const new_slot = self.m_head;
                    self.m_ring.items[new_slot] = key;
                    entry.slot = new_slot;
                    entry.freq.store(false, .release);

                    self.m_head = (self.m_head + 1) % self.m_ring.items.len;
                    self.m_len += 1;
                    // m_bytes unchanged — try the next tail.
                } else {
                    self.m_bytes -= keySize(K, key) + entry.val.data.len;
                    entry.val.decrRef();
                    _ = self.map.swapRemove(key);
                    self.freeKey(key);
                    return;
                }
            }
        }
    };
}

// ---------------------------------------------------------------------------
// ShardedCache(K) — N shards, thread-safe
// ---------------------------------------------------------------------------

pub fn ShardedCache(comptime K: type) type {
    return struct {
        const Self = @This();

        const Shard = struct {
            cache: Cache(K),
            lock: fiber.RWMutex(.untracked),
        };

        allocator: Allocator,
        shards: []Shard,
        /// `get` outcomes, for the stats dump.
        hits: std.atomic.Value(u64) = .init(0),
        misses: std.atomic.Value(u64) = .init(0),

        pub fn init(alloc: Allocator, total_capacity: usize, shard_count: usize) !Self {
            std.debug.assert(shard_count > 0);
            const per = @max(total_capacity / shard_count, 2);
            const shards = try alloc.alloc(Shard, shard_count);
            errdefer alloc.free(shards);
            var n: usize = 0;
            errdefer for (shards[0..n]) |*s| s.cache.deinit();
            for (shards) |*s| {
                s.cache = try Cache(K).init(alloc, per);
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

        pub fn get(self: *Self, key: K) ?*Value {
            const s = self.pickShard(key);
            s.lock.lockShared(@src());
            defer s.lock.unlockShared();
            const val = s.cache.get(key);
            const counter = if (val != null) &self.hits else &self.misses;
            _ = counter.fetchAdd(1, .monotonic);
            return val;
        }

        /// See `Cache.put` for ownership semantics.
        pub fn put(self: *Self, key: K, val: *Value) !void {
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

        /// The bytes the small queues hold in all: what a burst of new
        /// entries can be admitted without evicting its own earlier
        /// entries before they are touched (a new entry sits in S until
        /// its first hit). A prefetch sizes its batch by this.
        pub fn admissionBudget(self: *const Self) usize {
            var total: usize = 0;
            for (self.shards) |*s| total += s.cache.s_cap_bytes;
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

        /// The byte budget across shards.
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

        /// Approximate total byte size across shards.
        pub fn byteSize(self: *Self) usize {
            var total: usize = 0;
            for (self.shards) |*s| {
                s.lock.lockShared(@src());
                defer s.lock.unlockShared();
                total += s.cache.byteSize();
            }
            return total;
        }
    };
}

// Tests live in zdb/cache_test.zig. They access the cache module by name
// (`@import("zdb").cache`) so cache.zig stays purely in the zdb module's
// file tree without being claimed by the unified test binary.
