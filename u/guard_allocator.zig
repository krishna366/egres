// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! GuardAllocator — a diagnostic allocator that page-isolates every allocation
//! so a use-after-free or buffer overflow faults (SIGSEGV) AT the offending
//! instruction, making the backtrace name the WRITER rather than a later
//! victim. Built to chase the work-stealing wild-write ([[project_steal_
//! context_corruption]]): wrap the suspect allocator with this, run under
//! load, and the corruptor's own stack shows up at the fault.
//!
//! Each `alloc` is a fresh `mmap` of N data pages (READ|WRITE) followed by one
//! PROT_NONE guard page; the user slice is placed flush against the guard so a
//! right-overflow faults. On `free` the whole mapping flips to PROT_NONE and
//! its RAM is dropped (MADV_DONTNEED), but it is NEVER unmapped and NEVER
//! reused — so a dangling write lands on a reserved, inaccessible page and
//! faults exactly where it happens. Addresses never recycle, so there is no
//! ABA aliasing to muddy the diagnosis.
//!
//! Cost: >= 2 syscalls and a full page per allocation. This is a HUNT TOOL —
//! wrap only the allocator under suspicion (e.g. the fiber runtime's), never
//! the whole process, and only in Debug/ReleaseSafe.

const std = @import("std");
const linux = std.os.linux;
const posix = std.posix;

/// Page size used both for rounding and as the mmap pointer alignment.
const page: usize = std.heap.page_size_min;

pub const GuardAllocator = struct {
    /// One mapping we handed out, keyed by the user pointer.
    const Region = struct {
        base: usize, // mmap base (page-aligned)
        map_len: usize, // total mapped bytes (data pages + 1 guard page)
        freed: bool = false, // double-free tripwire
    };

    // A bare spinlock, NOT a fiber Mutex: this allocator backs the fiber
    // runtime itself, so a fiber-aware lock could re-enter the scheduler. The
    // critical section is a single hashmap op, so spinning is cheap.
    lock_state: std.atomic.Value(u32) align(std.atomic.cache_line) = .init(0),
    regions: std.AutoHashMapUnmanaged(usize, Region) = .empty,
    // Bookkeeping allocations must NOT be guarded (infinite recursion); go
    // straight to the OS page allocator.
    meta: std.mem.Allocator = std.heap.page_allocator,
    live_bytes: usize = 0,
    alloc_count: usize = 0,
    freed_count: usize = 0,

    pub fn init() GuardAllocator {
        return .{};
    }

    pub fn deinit(self: *GuardAllocator) void {
        // Leave the quarantined mappings reserved (the process is exiting); just
        // drop the side table.
        self.regions.deinit(self.meta);
    }

    pub fn allocator(self: *GuardAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable: std.mem.Allocator.VTable = .{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };

    fn lock(self: *GuardAllocator) void {
        while (true) {
            if (self.lock_state.load(.monotonic) == 0 and
                self.lock_state.cmpxchgWeak(0, 1, .acquire, .monotonic) == null) return;
            std.atomic.spinLoopHint();
        }
    }
    fn unlock(self: *GuardAllocator) void {
        self.lock_state.store(0, .release);
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        _ = ra;
        const self: *GuardAllocator = @ptrCast(@alignCast(ctx));
        const need = @max(len, 1);
        // Alignments beyond a page would need an oversized mapping; the runtime
        // never asks for that, so keep it simple and loud.
        if (alignment.toByteUnits() > page) @panic("GuardAllocator: alignment exceeds page size");
        const data_len = std.mem.alignForward(usize, need, page);
        const map_len = data_len + page; // trailing guard page

        const region = posix.mmap(
            null,
            map_len,
            .{ .READ = true, .WRITE = true },
            .{ .TYPE = .PRIVATE, .ANONYMOUS = true },
            -1,
            0,
        ) catch return null;
        // Flip the trailing page into a guard.
        const guard_ptr: [*]const u8 = @ptrFromInt(@intFromPtr(region.ptr) + data_len);
        if (linux.mprotect(guard_ptr, page, .{}) != 0) {
            posix.munmap(region);
            return null;
        }
        // Place the user slice flush against the guard, aligned down. `base` is
        // page-aligned (hence aligned to `alignment` <= page), so backward never
        // crosses below it.
        const user = alignment.backward(@intFromPtr(region.ptr) + data_len - need);

        self.lock();
        self.regions.put(self.meta, user, .{
            .base = @intFromPtr(region.ptr),
            .map_len = map_len,
        }) catch {
            self.unlock();
            posix.munmap(region);
            return null;
        };
        self.live_bytes += map_len;
        self.alloc_count += 1;
        self.unlock();
        return @ptrFromInt(user);
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
        _ = alignment;
        _ = ra;
        const self: *GuardAllocator = @ptrCast(@alignCast(ctx));
        const user = @intFromPtr(memory.ptr);

        self.lock();
        const r = self.regions.getPtr(user) orelse {
            self.unlock();
            std.debug.panic("GuardAllocator.free: unknown ptr 0x{x} (foreign or double free)", .{user});
        };
        if (r.freed) {
            self.unlock();
            std.debug.panic("GuardAllocator.free: DOUBLE FREE of 0x{x}", .{user});
        }
        r.freed = true;
        const base = r.base;
        const map_len = r.map_len;
        self.live_bytes -= map_len;
        self.freed_count += 1;
        self.unlock();

        // Whole mapping inaccessible: any later access through a dangling ptr
        // faults AT the access. Keep it reserved (no munmap) so the address is
        // never recycled; drop the physical pages so quarantine is RAM-cheap.
        const base_ptr: [*]const u8 = @ptrFromInt(base);
        _ = linux.mprotect(base_ptr, map_len, .{});
        const aligned: [*]align(page) u8 = @ptrFromInt(base);
        posix.madvise(aligned, map_len, posix.MADV.DONTNEED) catch {};
    }

    // No in-place growth: returning false/null forces realloc to alloc-new +
    // copy + free-old, which quarantines the old block — exactly what we want.
    fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }
    fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }
};
