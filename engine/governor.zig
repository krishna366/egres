// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The block cache's governor: sizes the cache from what the rest of
//! the process holds, so the cache takes the memory nothing else needs
//! and gives it back when something does.
//!
//! `total` (`--cache total_mb`) is the memory the process may hold as
//! the allocator holds it: resident, what the cgroup charges. The
//! program counts its own live bytes (the counting allocator); the
//! allocator holds more on top (size-class rounding, freed pages not
//! yet returned), measured as `ratio`, resident bytes per live byte. The
//! live bytes allowed are `total / ratio`, and the cache gets those less
//! a margin kept for everything else. Ratio and margin both rise at once
//! and fall back within a minute, so the cache shrinks for a burst at once
//! and regrows slowly, and recurring bursts (a compaction's buffers) do
//! not swing it.
const std = @import("std");
const zdb = @import("zdb");
const galloc = @import("galloc");
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

/// Resident bytes per live byte, the allocator's overhead (size-class
/// rounding, freed pages not yet returned). `cur` is the recent peak
/// of what `measure` read: up at once, down a step per `update`.
pub const Ratio = struct {
    cur: f64 = default,
    measured: f64 = default,

    /// Until one is measured, and for an allocator that reports no
    /// resident bytes: the high end of what the orders runs saw (1.06
    /// to 1.11 under O_DIRECT).
    pub const default: f64 = 1.10;

    /// Takes the allocator's resident bytes against `live`, never under
    /// 1. Skipped while live is under `min_live`: on a small heap the
    /// allocator's fixed costs overstate it (1.69 at 1 GiB in a 64 GiB
    /// run).
    pub fn measure(self: *Ratio, live: u64, min_live: u64) void {
        if (live < min_live) return;
        const resident = galloc.snapshot().resident orelse return;
        const r = @as(f64, @floatFromInt(resident)) /
            @as(f64, @floatFromInt(live));
        self.measured = @max(1.0, r);
    }

    /// Moves `cur` toward `measured`: up at once, down
    /// `Governor.decay_ticks`th of the gap, as the margin moves.
    pub fn update(self: *Ratio) void {
        if (self.measured >= self.cur)
            self.cur = self.measured
        else
            self.cur -= (self.cur - self.measured) / Governor.decay_ticks;
    }

    /// The live bytes `total` resident bytes allow: `total / cur`.
    pub fn liveFor(self: Ratio, total: u64) u64 {
        return @intFromFloat(@as(f64, @floatFromInt(total)) / self.cur);
    }
};

pub const Governor = struct {
    /// The memory the process may hold, resident (`--cache total_mb`).
    total: u64,
    /// Kept for what the process holds outside the cache: the recent
    /// peak of it, never under `min_margin`.
    margin: u64 = min_margin,
    ratio: Ratio = .{},
    ticks: u32 = 0,
    knob: Knob,

    pub const min_cap: u64 = 64 << 20;
    pub const min_margin: u64 = 256 << 20;
    /// Each tick closes this fraction of the margin's and the ratio's
    /// gaps to what they follow.
    pub const decay_ticks = 120;
    /// Ticks between measures of the ratio: reading the allocator's
    /// stats refreshes them, cheap once a second, not every tick.
    const ratio_every_ticks = 10;

    pub fn init(total: u64, cache: *BlockCache) Governor {
        const gov: Governor = .{ .total = total, .knob = Knob.init(cache) };
        gov.storeLimit();
        return gov;
    }

    /// The live bytes the process may hold: `total` over the ratio.
    pub fn liveTarget(self: *const Governor) u64 {
        return self.ratio.liveFor(self.total);
    }

    /// Allocations past the live target are counted, for `mem`.
    fn storeLimit(self: *const Governor) void {
        const limit = &utils.context.counting().limit;
        limit.store(self.liveTarget(), .monotonic);
    }

    /// Moves the margin for `others` live bytes held outside the cache
    /// and returns the cap, the live target less the margin. The margin
    /// rises to `others` at once and falls toward it a step per tick.
    pub fn nextCap(self: *Governor, others: u64) u64 {
        const held = @max(others, min_margin);
        if (held >= self.margin)
            self.margin = held
        else
            self.margin -= (self.margin - held) / decay_ticks;
        return @max(min_cap, self.liveTarget() -| self.margin);
    }

    /// Reads the counters, moves the ratio and the margin, and applies
    /// the cap.
    pub fn tick(self: *Governor) void {
        const live = utils.context.counting().live_bytes.load(.monotonic);
        if (self.ticks % ratio_every_ticks == 0)
            self.ratio.measure(live, self.total / 4);
        self.ticks +%= 1;
        self.ratio.update();
        self.storeLimit();

        // What the cap governs is the cache's block bytes. Blocks it
        // evicted that readers still hold come from its allocator too
        // but not under the cap, so they count with the rest.
        const others = live -| self.knob.cache.byteSize();
        const cap = self.nextCap(others);
        if (cap != self.knob.now) {
            self.knob.set(cap);
            // The cap moves a little on most ticks: sample it every 5 s.
            utils.logEvery(5, "governor: cache cap {d} MiB (margin " ++
                "{d} MiB, ratio {d:.3})", .{
                cap >> 20, self.margin >> 20, self.ratio.cur,
            });
        }
    }
};

/// The cache's cap before the first tick, for its creation.
pub fn startCap(total: u64) u64 {
    const unmeasured: Ratio = .{};
    return @max(
        Governor.min_cap,
        unmeasured.liveFor(total) -| Governor.min_margin,
    );
}

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
