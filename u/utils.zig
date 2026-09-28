const std = @import("std");
const builtin = @import("builtin");
pub const compression = @import("compress.zig");
pub const hlc = @import("hlc.zig");
pub const host = @import("host.zig");
pub const date = @import("date.zig");
/// Process-wide context (gpa/arena/io/args) and the coarse clock live
/// here, at the bottom of the module graph, so every layer including
/// the fiber runtime reaches them without a cycle.
pub const context = @import("context.zig");
pub const coarse_clock = @import("coarse_clock.zig");
pub const coarseMs = coarse_clock.ms;
pub const timestampNow = coarse_clock.timestampNow;

/// A borrowed byte string: a name, row bytes, an encoded key. The
/// same type as []const u8; it reads better in a slice of them.
pub const String = []const u8;
pub const GuardAllocator = @import("guard_allocator.zig").GuardAllocator;

/// Empty an ArrayList and keep its buffer. `clearRetainingCapacity`
/// does the same but first memsets the whole slice with `undefined`,
/// a real 0xAA fill in Debug and ReleaseSafe, which showed up on hot
/// resets (a version merge per key on a full-table scan). One place to
/// change if the reset strategy ever changes.
pub fn clearRetainCap(list: anytype) void {
    list.items.len = 0;
}

pub fn testPrint(comptime fmt: String, args: anytype) void {
    if (!builtin.is_test) return;
    // std.debug.print(fmt, args);
    _ = fmt;
    _ = args;
}

/// Rate-limited log: each call site (unique comptime instantiation) logs at most once per interval.
pub fn logEvery(comptime interval_secs: i64, comptime fmt: String, args: anytype) void {
    const S = struct {
        var last: i64 = 0;
    };
    const now = coarse_clock.ms();
    if (now - S.last > interval_secs * 1_000) {
        std.log.debug(fmt, args);
        S.last = now;
    }
}

/// search finds the first index in [0, items.len) for which pred(c, items[i]) is true,
/// assuming the range is partitioned as [false…false, true…true].
/// Returns items.len when the predicate is never true (mirrors Go's sort.Search).
pub fn search(
    comptime T: type,
    items: []const T,
    c: anytype,
    comptime pred: fn (@TypeOf(c), T) bool,
) usize {
    var lo: usize = 0;
    var hi: usize = items.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (pred(c, items[mid])) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    return lo;
}

/// Lock-free, first-error-wins container. Safe to call set() from many threads;
/// only the first error is retained.
pub const SafeError = struct {
    raw: std.atomic.Value(u16) = std.atomic.Value(u16).init(0),

    pub fn set(self: *SafeError, e: anyerror) void {
        _ = self.raw.cmpxchgStrong(0, @intFromError(e), .acq_rel, .monotonic);
    }

    pub fn get(self: *const SafeError) ?anyerror {
        const v = self.raw.load(.acquire);
        if (v == 0) return null;
        return @errorFromInt(v);
    }
};

/// Heap-allocated, ref-counted byte buffer. Used to share payloads across
/// fan-out send paths (e.g. AE / file push) without per-peer memcpy: caller
/// wraps an owned slice in `RefValue.init`, then `incrRef` per peer and
/// hands each ref off to the transport queue. Each writer fiber `decrRef`s
/// after writing — when the count reaches zero, the data slice and the
/// RefValue itself are freed with the original allocator.
pub const RefValue = struct {
    data: []u8,
    allocator: std.mem.Allocator,
    ref: std.atomic.Value(i32),

    pub fn init(allocator: std.mem.Allocator, data: []u8) !*RefValue {
        const v = try allocator.create(RefValue);
        v.* = .{ .data = data, .allocator = allocator, .ref = std.atomic.Value(i32).init(1) };
        return v;
    }

    /// A freed value's count is left at this poison, so a late take or
    /// release on it trips the guards below instead of corrupting the
    /// heap silently. These checks stay on in every build mode: a
    /// refcount bug here is a cross-thread double free.
    const dead: i32 = -0x4000_0000;

    pub fn incrRef(self: *RefValue) *RefValue {
        const prev = self.ref.fetchAdd(1, .monotonic);
        if (prev <= 0) @panic("RefValue.incrRef on a freed value");
        return self;
    }

    pub fn decrRef(self: *RefValue) void {
        const prev = self.ref.fetchSub(1, .acq_rel);
        if (prev <= 0) @panic("RefValue.decrRef below zero (double release)");
        if (prev == 1) {
            self.ref.store(dead, .release);
            self.allocator.free(self.data);
            self.allocator.destroy(self);
        }
    }

    pub fn byteSize(self: *const RefValue) usize {
        return self.data.len;
    }
};
