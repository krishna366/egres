// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const Allocator = std.mem.Allocator;

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
