// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Process memory accounting. `CountingAlloc` passes every call to the
//! allocator under it and counts what the program holds, in live
//! bytes, one atomic add per call. The process allocator is always one
//! (`context.gpa`, read through `context.counting()`); the admin `mem`
//! command and the cache governor read it.
const std = @import("std");
const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;

pub const CountingAlloc = struct {
    inner: Allocator,
    live_bytes: std.atomic.Value(u64) = .init(0),
    /// Live bytes past which an allocation is counted in `over_limit`.
    /// Nothing is evicted here: an allocation may be made under a lock
    /// the eviction would need. The governor's tick gives memory back.
    limit: std.atomic.Value(u64) = .init(std.math.maxInt(u64)),
    over_limit: std.atomic.Value(u64) = .init(0),

    pub fn init(inner: Allocator) CountingAlloc {
        return .{ .inner = inner };
    }

    pub fn allocator(self: *CountingAlloc) Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable: Allocator.VTable = .{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };

    fn of(ctx: *anyopaque) *CountingAlloc {
        return @ptrCast(@alignCast(ctx));
    }

    fn alloc(ctx: *anyopaque, len: usize, al: Alignment, ra: usize) ?[*]u8 {
        const self = of(ctx);
        const p = self.inner.rawAlloc(len, al, ra) orelse return null;

        const live = self.live_bytes.fetchAdd(len, .monotonic) + len;

        if (live > self.limit.load(.monotonic))
            _ = self.over_limit.fetchAdd(1, .monotonic);
        return p;
    }

    fn resize(
        ctx: *anyopaque,
        memory: []u8,
        al: Alignment,
        new_len: usize,
        ra: usize,
    ) bool {
        const self = of(ctx);
        if (!self.inner.rawResize(memory, al, new_len, ra)) return false;
        self.resized(memory.len, new_len);
        return true;
    }

    fn remap(
        ctx: *anyopaque,
        memory: []u8,
        al: Alignment,
        new_len: usize,
        ra: usize,
    ) ?[*]u8 {
        const self = of(ctx);
        const p = self.inner.rawRemap(memory, al, new_len, ra) orelse
            return null;
        self.resized(memory.len, new_len);
        return p;
    }

    fn free(ctx: *anyopaque, memory: []u8, al: Alignment, ra: usize) void {
        const self = of(ctx);
        self.inner.rawFree(memory, al, ra);
        _ = self.live_bytes.fetchSub(memory.len, .monotonic);
    }

    fn resized(self: *CountingAlloc, old_len: usize, new_len: usize) void {
        if (new_len >= old_len)
            _ = self.live_bytes.fetchAdd(new_len - old_len, .monotonic)
        else
            _ = self.live_bytes.fetchSub(old_len - new_len, .monotonic);
    }
};
