// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The block cache's governor: sizes the cache from what the rest of
//! the process holds, so the cache takes the memory nothing else needs
//! and gives it back when something does.
//!
//! It governs live bytes only, the program's own (the counting
//! allocator), against `total` (`--cache total_mb`). The cache gets
//! `total` less a margin kept for everything else. The margin rises to
//! what the rest holds at once and falls back over minutes, so the
//! cache shrinks for a burst at once and regrows slowly, and recurring
//! bursts (a compaction's buffers) do not swing it. What the allocator
//! holds on top (size-class rounding, freed pages not yet returned) is
//! outside `total`; the default leaves a tenth of the memory for it.
const std = @import("std");
const zdb = @import("zdb");
const utils = @import("raft").utils;

const BlockCache = zdb.cache.ShardedCache(u128);

/// The cache's cap: the value in force, and the cache it is applied to.
pub const Knob = struct {
    cache: *BlockCache,
    now: u64,

    pub fn init(cache: *BlockCache) Knob {
        return .{ .cache = cache, .now = cache.capacity() };
    }

    pub fn set(self: *Knob, next: u64) void {
        self.now = next;
        self.cache.setCapacity(next);
    }
};

pub const Governor = struct {
    /// The live bytes the process may hold (`--cache total_mb`).
    total: u64,
    /// Kept for what the process holds outside the cache: the recent
    /// peak of it, never under `min_margin`.
    margin: u64 = min_margin,
    knob: Knob,

    pub const min_cap: u64 = 64 << 20;
    pub const min_margin: u64 = 256 << 20;
    /// Each tick closes this fraction of the margin's gap to what the
    /// rest holds: about 63% a minute at 100 ms ticks.
    const decay_ticks = 600;

    pub fn init(total: u64, cache: *BlockCache) Governor {
        // Allocations past it are counted, for `mem`.
        utils.context.counting().limit.store(total, .monotonic);
        return .{ .total = total, .knob = Knob.init(cache) };
    }

    /// Moves the margin for `others` live bytes held outside the cache
    /// and returns the cap, `total` less the margin. The margin rises
    /// to `others` at once and falls toward it a step per tick.
    pub fn nextCap(self: *Governor, others: u64) u64 {
        const held = @max(others, min_margin);
        if (held >= self.margin)
            self.margin = held
        else
            self.margin -= (self.margin - held) / decay_ticks;
        return @max(min_cap, self.total -| self.margin);
    }

    /// Reads the counters, moves the margin and applies the cap.
    pub fn tick(self: *Governor) void {
        const live = utils.context.counting().live_bytes.load(.monotonic);
        // What the cap governs is the cache's block bytes. Blocks it
        // evicted that readers still hold come from its allocator too
        // but not under the cap, so they count with the rest.
        const others = live -| self.knob.cache.byteSize();
        const cap = self.nextCap(others);
        if (cap != self.knob.now) {
            self.knob.set(cap);
            // The cap moves a little on most ticks: sample it every 5 s.
            utils.logEvery(5, "governor: cache cap {d} MiB (margin " ++
                "{d} MiB)", .{ cap >> 20, self.margin >> 20 });
        }
    }
};

var global: Governor = undefined;
var installed: bool = false;

/// Installs the process's governor over `cache`: the engine runs its
/// ticks once started and `mem` prints it. The cache must outlive the
/// engine (`main` waits on it before the cache is freed).
pub fn init(total: u64, cache: *BlockCache) void {
    global = Governor.init(total, cache);
    installed = true;
}

/// The process's governor, or null when none is installed (tests and
/// tools that start an engine without `main`).
pub fn g() ?*Governor {
    return if (installed) &global else null;
}
