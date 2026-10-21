// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Zig allocator adapter and production policy for the pinned jemalloc.
//!
//! The policy is embedded instead of relying on a shell environment.
//! `MALLOC_CONF` is parsed after `malloc_conf`, so operators can still
//! override individual settings for experiments or incident response.

const std = @import("std");
const builtin = @import("builtin");

const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;

/// Which backend this module is, for the build-selected "galloc" module:
/// main logs it and comptime-gates backend-specific pieces on it.
pub const kind = "jemalloc";

extern fn mallocx(size: usize, flags: c_int) ?*anyopaque;
extern fn xallocx(p: *anyopaque, size: usize, extra: usize, flags: c_int) usize;
extern fn nallocx(size: usize, flags: c_int) usize;
extern fn sdallocx(ptr: *anyopaque, size: usize, flags: c_int) void;
extern fn mallctl(
    name: [*:0]const u8,
    oldp: ?*anyopaque,
    oldlenp: ?*usize,
    newp: ?*anyopaque,
    newlen: usize,
) c_int;

// The libc entry points, namespaced to avoid clashing with the vtable
// fns below. These resolve to the very symbols C libraries link
// against — jemalloc's non-prefixed archive must own them, and
// verifyInUse proves it.
const libc = struct {
    extern fn malloc(size: usize) ?*anyopaque;
    extern fn free(ptr: ?*anyopaque) void;
    extern fn strdup(s: [*:0]const u8) ?[*:0]u8;
};

// macOS has no symbol interposition for the libc allocator: linking
// jemalloc's `malloc` catches calls bound at link time (Zig, our C
// archives) but not the ones libSystem makes internally — strdup in
// libpg_query, asprintf, getaddrinfo. Those reach jemalloc only once
// it is the default malloc zone, which `zone_register` arranges from
// a load-time constructor in zone.o. A static archive member links
// only when something references it, so without this reference the
// constructor never runs, libSystem keeps allocating from the system
// zone and jemalloc's `free` faults on those blocks (libpg_query's
// stderr buffer was the first victim). `allocator()` is the anchor:
// every binary that uses jemalloc at all calls it once at startup,
// and the call is harmless after the constructor since it returns
// early once a non-system zone is the default. The private namespace
// keeps its je_ prefix even in the unprefixed public build galloc
// links against.
const darwin = struct {
    extern fn je_zone_register() void;
};

// Derived from zmem experimentation.
pub const default_configuration =
    "abort_conf:true," ++
    "tcache:true," ++
    // Linux-only knobs. percpu arenas need sched_getcpu, which only
    // Linux has. Background threads are the production reclaim path
    // (purging runs off the worker threads, on jemalloc's own adaptive
    // schedule); jemalloc does not build them on macOS, whose mutexes
    // are os_unfair_lock (no condition variable) and whose Mach-O
    // linking defeats the pthread_create interposition they rely on.
    // There the maintenance tick in main.zig stands in (`decay`,
    // gated on `has_background_reclaim`). Elsewhere jemalloc would
    // silently drop the first option and warn about the second.
    (if (builtin.os.tag == .linux)
        "percpu_arena:percpu," ++
            "background_thread:true," ++
            "max_background_threads:2,"
    else
        "") ++
    // Freed pages return to the kernel within ~2 s: long enough that
    // a page freed and wanted again soon is reused, short enough that
    // the block cache's churn does not pile up resident.
    "dirty_decay_ms:2000," ++
    "muzzy_decay_ms:0," ++
    // No spare page per large allocation for cache-line randomization:
    // a 16 KiB block cost 20 KiB with it, and the cache governor counts
    // live bytes, leaving the allocator's overhead to the reserve.
    "cache_oblivious:false";

/// The archive supplies a weak default. This strong definition embeds the
/// egres policy and is read by jemalloc before `main` starts.
pub export var malloc_conf: [*:0]const u8 = default_configuration;

const vtable: Allocator.VTable = .{
    .alloc = alloc,
    .resize = resize,
    .remap = remap,
    .free = free,
};

pub fn allocator() Allocator {
    // See `darwin`: links zone.o and makes jemalloc the default zone.
    if (comptime builtin.os.tag.isDarwin()) darwin.je_zone_register();
    return .{ .ptr = undefined, .vtable = &vtable };
}

fn flags(alignment: Alignment) c_int {
    // MALLOCX_LG_ALIGN is a base-two logarithm, like Alignment's tag.
    return @intCast(@intFromEnum(alignment));
}

fn alloc(
    _: *anyopaque,
    len: usize,
    alignment: Alignment,
    _: usize,
) ?[*]u8 {
    const ptr = mallocx(
        len,
        flags(alignment),
    ) orelse return null;
    return @ptrCast(ptr);
}

// In-place reclass via xallocx, shared by resize and remap. `free`
// reports the slice length to jemalloc (`sdallocx`), and jemalloc uses
// that length to find the allocation's size class. So this may only
// claim success if `new_len` maps to the size class the allocation
// actually ends up in — which is what `nallocx(new_len)` computes.
//
// Checking `xallocx(...) >= new_len` instead is unsound for shrinks:
// when jemalloc refuses to move a small allocation into a smaller
// class, `xallocx` returns the unchanged usable size — which always
// passes `>=` — and the eventual free would then use the wrong class.
fn fitsInPlace(memory: []u8, alignment: Alignment, new_len: usize) bool {
    const f = flags(alignment);
    return xallocx(memory.ptr, new_len, 0, f) == nallocx(new_len, f);
}

fn resize(
    _: *anyopaque,
    memory: []u8,
    alignment: Alignment,
    new_len: usize,
    _: usize,
) bool {
    return fitsInPlace(memory, alignment, new_len);
}

fn remap(
    _: *anyopaque,
    memory: []u8,
    alignment: Alignment,
    new_len: usize,
    _: usize,
) ?[*]u8 {
    // In-place or null — never rallocx, whose "success" on a move is a
    // hidden whole-block memcpy. The interface prefers null there so
    // the caller performs the copy itself: sometimes smarter (an
    // insertion scoot copies head and tail straight into their final
    // positions), never worse. jemalloc cannot relocate a block
    // without copying, so a moving remap has no upside.
    return if (fitsInPlace(memory, alignment, new_len)) memory.ptr else null;
}

fn free(
    _: *anyopaque,
    memory: []u8,
    alignment: Alignment,
    _: usize,
) void {
    sdallocx(memory.ptr, memory.len, flags(alignment));
}

pub const Snapshot = struct {
    allocated: ?u64 = null,
    active: ?u64 = null,
    mapped: ?u64 = null,
    resident: ?u64 = null,
    retained: ?u64 = null,
};

pub const ThreadActivity = struct {
    allocated: u64,
    deallocated: u64,
};

fn readStat(comptime name: [:0]const u8) ?u64 {
    var value: usize = 0;
    var len: usize = @sizeOf(usize);
    if (mallctl(name, &value, &len, null, 0) != 0) return null;
    return @intCast(value);
}

const ThreadCounters = struct {
    allocated: *const u64,
    deallocated: *const u64,
};

/// Resolved once per thread. jemalloc guarantees the pointers reference the
/// calling thread's TSD counters and stay valid for the thread's lifetime.
threadlocal var thread_counters: ?ThreadCounters = null;

fn counterPointer(comptime name: [:0]const u8) ?*const u64 {
    var ptr: *const u64 = undefined;
    var len: usize = @sizeOf(*const u64);
    if (mallctl(name, @ptrCast(&ptr), &len, null, 0) != 0) return null;
    return ptr;
}

/// Read cumulative allocation counters for the calling OS thread. The first
/// call per thread resolves jemalloc's counter pointers (~100 ns); every
/// later call is two plain loads (~1 ns per benchmark), cheap enough for
/// per-fiber-slice accounting on the dispatch path. The counters are
/// per-OS-thread: a migrating fiber must be accounted per slice, and a
/// `ThreadActivity` must never be compared across threads.
pub fn threadActivity() ?ThreadActivity {
    const counters = thread_counters orelse blk: {
        const resolved: ThreadCounters = .{
            .allocated = counterPointer("thread.allocatedp") orelse
                return null,
            .deallocated = counterPointer("thread.deallocatedp") orelse
                return null,
        };
        thread_counters = resolved;
        break :blk resolved;
    };
    return .{
        .allocated = counters.allocated.*,
        .deallocated = counters.deallocated.*,
    };
}

/// Refresh and read allocator-owned counters. Process RSS is separate. The cost
/// of calling this is ~60us, as per benchmark with no memory allocated.
pub fn snapshot() Snapshot {
    var epoch: u64 = 1;
    var epoch_len: usize = @sizeOf(u64);
    _ = mallctl(
        "epoch",
        &epoch,
        &epoch_len,
        &epoch,
        @sizeOf(u64),
    );
    return .{
        .allocated = readStat("stats.allocated"),
        .active = readStat("stats.active"),
        .mapped = readStat("stats.mapped"),
        .resident = readStat("stats.resident"),
        .retained = readStat("stats.retained"),
    };
}

/// `LibcNotJemalloc`: the gpa routes to jemalloc but libc's malloc does
/// not, so C libraries' allocations go elsewhere (a link-order fault).
pub const VerifyError = error{ JemallocNotInUse, LibcNotJemalloc };

/// Startup probe: allocate through `gpa`, confirm this thread's jemalloc
/// counters moved, free, confirm again — then repeat through libc
/// malloc, proving jemalloc owns the symbols C-library allocations
/// resolve. Catches wiring regressions on either path (wrong allocator
/// passed at init, adapter dropped, a prefixed or stats-less jemalloc
/// build — the latter would also break the accounting this module
/// provides). Must run before other threads matter but works at any
/// point; the probes and all reads stay on the calling thread.
pub fn verifyInUse(gpa: Allocator) VerifyError!void {
    const probe_len = 4096;
    const before = threadActivity() orelse return error.JemallocNotInUse;

    const probe = gpa.alloc(u8, probe_len) catch return error.JemallocNotInUse;
    probe[0] = 0xa5;
    const during = threadActivity() orelse return error.JemallocNotInUse;
    gpa.free(probe);

    const after = threadActivity() orelse return error.JemallocNotInUse;
    if (during.allocated < before.allocated + probe_len or
        after.deallocated < before.deallocated + probe_len)
    {
        return error.JemallocNotInUse;
    }

    // `malloc`/`free` are builtins to the optimizer: a pair whose
    // memory is never read is removed in release builds, and the
    // counters would not move. The pointer has to escape.
    const c_probe = libc.malloc(probe_len) orelse
        return error.LibcNotJemalloc;
    @as([*]u8, @ptrCast(c_probe))[0] = 0xa5;
    std.mem.doNotOptimizeAway(c_probe);
    const c_during = threadActivity() orelse return error.LibcNotJemalloc;
    libc.free(c_probe);

    const c_after = threadActivity() orelse return error.LibcNotJemalloc;
    if (c_during.allocated < after.allocated + probe_len or
        c_after.deallocated < after.deallocated + probe_len)
    {
        return error.LibcNotJemalloc;
    }

    // The probe above binds `malloc` at link time, which passes even
    // where libc's own internal allocations bypass jemalloc. strdup
    // allocates from inside libc, so this is the probe that proves C
    // libraries' allocations land here too (on macOS, that the zone
    // registration in `allocator()` took). A failure here means a
    // free of such a block would fault, as it did before the zone
    // was linked.
    const s_probe = libc.strdup("galloc probe") orelse
        return error.LibcNotJemalloc;
    s_probe[0] = 'G';
    std.mem.doNotOptimizeAway(s_probe);
    const s_during = threadActivity() orelse return error.LibcNotJemalloc;
    libc.free(s_probe);

    const s_after = threadActivity() orelse return error.LibcNotJemalloc;
    if (s_during.allocated <= c_after.allocated or
        s_after.deallocated <= c_after.deallocated)
    {
        return error.LibcNotJemalloc;
    }
}

pub const DecayError = error{DecayFailed};

/// Whether this build reclaims on its own: true where jemalloc's
/// background threads are on (`default_configuration`). Where false,
/// main.zig runs the maintenance tick that calls `decay`.
pub const has_background_reclaim = builtin.os.tag == .linux;

/// The periodic reclaim step for builds without background threads
/// (`has_background_reclaim`): purge only the dirty pages whose decay
/// deadline has passed, in every arena, which is the work those threads
/// would do. The allocation path already runs decay on an arena it
/// touches, so a busy arena has nothing left here; the call exists for
/// arenas that went quiet with dirty pages, which nothing else would
/// ever return to the OS. Harmless where the threads are on.
pub fn decay() DecayError!void {
    // MALLCTL_ARENAS_ALL is 4096 in the pinned jemalloc headers.
    if (mallctl("arena.4096.decay", null, null, null, 0) != 0) {
        return error.DecayFailed;
    }
}
