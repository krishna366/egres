// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Zig allocator adapter and production policy for the pinned mimalloc.
//!
//! The policy is: stock defaults, no configuration. The zmem study probed
//! mimalloc's knobs and every one was inert or harmful (a shorter
//! MIMALLOC_PURGE_DELAY cost 17% request-arena throughput by purging
//! pages the next request faulted back in). What mimalloc does need is a
//! caller: it has no background threads, so freed memory returns to the
//! OS only when something enters the allocator after the 1 s purge
//! deadline. A maintenance fiber calling `decay()` about once a second
//! is that something — main.zig spawns it whenever this backend is
//! selected.
//!
//! The library is built with MI_OVERRIDE=ON (see
//! scripts/bootstrap-mimalloc.sh): its archive defines libc malloc/free
//! too, so in a mimalloc build (`-Dallocator=mimalloc`; jemalloc is the
//! default: on the orders benchmark mimalloc held 20 GiB of freed 64 KiB
//! blocks beside a 30 GiB block cache, jemalloc under 2 GiB)
//! the whole process allocates through mimalloc — the Zig gpa via this
//! adapter, C libraries (libpg_query, lz4/zstd) via the overridden libc
//! entry points. Symmetric with a jemalloc build, where jemalloc's
//! non-prefixed archive owns libc malloc; jemalloc is absent from a
//! mimalloc binary and vice versa.

const std = @import("std");

const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;

/// Which backend this module is, for the build-selected "galloc" module:
/// main logs it and comptime-gates the maintenance fiber on it.
pub const kind = "mimalloc";

extern fn mi_malloc(size: usize) ?*anyopaque;
extern fn mi_malloc_aligned(size: usize, alignment: usize) ?*anyopaque;
extern fn mi_expand(ptr: *anyopaque, new_size: usize) ?*anyopaque;
extern fn mi_free_size_aligned(
    ptr: *anyopaque,
    size: usize,
    alignment: usize,
) void;
extern fn mi_collect(force: bool) void;

// The libc entry points, namespaced to avoid clashing with the vtable
// fns above. These resolve to the very symbols C libraries link
// against — with MI_OVERRIDE=ON that must be mimalloc, and verifyInUse
// proves it.
const libc = struct {
    extern fn malloc(size: usize) ?*anyopaque;
    extern fn free(ptr: ?*anyopaque) void;
    extern fn strdup(s: [*:0]const u8) ?[*:0]u8;
};
extern fn mi_is_in_heap_region(p: ?*const anyopaque) bool;
extern fn mi_process_info(
    elapsed_msecs: *usize,
    user_msecs: *usize,
    system_msecs: *usize,
    current_rss: *usize,
    peak_rss: *usize,
    current_commit: *usize,
    peak_commit: *usize,
    page_faults: *usize,
) void;

const vtable: Allocator.VTable = .{
    .alloc = alloc,
    .resize = resize,
    .remap = remap,
    .free = free,
};

pub fn allocator() Allocator {
    return .{ .ptr = undefined, .vtable = &vtable };
}

fn alloc(
    _: *anyopaque,
    len: usize,
    alignment: Alignment,
    _: usize,
) ?[*]u8 {
    // mimalloc guarantees 8-byte alignment on plain mi_malloc; skip the
    // aligned entry point (and its bookkeeping) for the common case.
    const bytes = alignment.toByteUnits();
    const ptr = (if (bytes <= 8)
        mi_malloc(len)
    else
        mi_malloc_aligned(len, bytes)) orelse return null;
    return @ptrCast(ptr);
}

fn resize(
    _: *anyopaque,
    memory: []u8,
    _: Alignment,
    new_len: usize,
    _: usize,
) bool {
    // `free` reports the slice length via sized deallocation
    // (mi_free_size_aligned), which mimalloc validates against the
    // block's usable size. mi_expand succeeds only when the existing
    // block already covers `new_len` — the block never moves and never
    // changes class — so a later sized free at `new_len` stays within
    // the block's real usable size for both grows and shrinks.
    return mi_expand(memory.ptr, new_len) != null;
}

fn remap(
    _: *anyopaque,
    memory: []u8,
    _: Alignment,
    new_len: usize,
    _: usize,
) ?[*]u8 {
    // In-place or null — never mi_realloc, whose "success" on a move
    // is a hidden whole-block memcpy. The interface prefers null there
    // so the caller performs the copy itself: sometimes smarter (an
    // insertion scoot copies head and tail straight into their final
    // positions), never worse. mimalloc cannot relocate a block
    // without copying, so a moving remap has no upside. The block
    // stays put, so its original alignment is preserved.
    return if (mi_expand(memory.ptr, new_len) != null) memory.ptr else null;
}

fn free(
    _: *anyopaque,
    memory: []u8,
    alignment: Alignment,
    _: usize,
) void {
    mi_free_size_aligned(memory.ptr, memory.len, alignment.toByteUnits());
}

/// Process-level numbers from mi_process_info. mimalloc's stats API needs
/// a vendored header + C shim for per-heap detail; the process view is
/// enough for metrics, and RSS is what capacity planning watches anyway.
pub const Snapshot = struct {
    resident: ?u64 = null,
    peak_resident: ?u64 = null,
    committed: ?u64 = null,
    peak_committed: ?u64 = null,
};

pub fn snapshot() Snapshot {
    var elapsed: usize = 0;
    var user: usize = 0;
    var system: usize = 0;
    var rss: usize = 0;
    var peak_rss: usize = 0;
    var commit: usize = 0;
    var peak_commit: usize = 0;
    var faults: usize = 0;
    mi_process_info(
        &elapsed,
        &user,
        &system,
        &rss,
        &peak_rss,
        &commit,
        &peak_commit,
        &faults,
    );
    return .{
        .resident = rss,
        .peak_resident = peak_rss,
        .committed = commit,
        .peak_committed = peak_commit,
    };
}

pub const VerifyError = error{MimallocNotInUse};

/// Startup probe mirroring jemalloc.verifyInUse: allocate through `gpa`
/// and confirm the pointer lands inside a mimalloc heap region, then do
/// the same through libc malloc — proving the MI_OVERRIDE routing that
/// C-library allocations (libpg_query, lz4/zstd) depend on. Catches
/// wiring regressions on either path (wrong allocator passed at init,
/// the adapter dropped, or an override-less archive linked).
pub fn verifyInUse(gpa: Allocator) VerifyError!void {
    const probe = gpa.alloc(u8, 4096) catch
        return error.MimallocNotInUse;
    defer gpa.free(probe);
    probe[0] = 0xa5;
    if (!mi_is_in_heap_region(probe.ptr)) {
        return error.MimallocNotInUse;
    }

    const c_probe = libc.malloc(4096) orelse
        return error.MimallocNotInUse;
    defer libc.free(c_probe);
    @as([*]u8, @ptrCast(c_probe))[0] = 0xa5;
    if (!mi_is_in_heap_region(c_probe)) {
        return error.MimallocNotInUse;
    }

    // `malloc` above binds at link time; strdup allocates from inside
    // libc itself, so only this probe proves C libraries' internal
    // allocations are overridden too. On macOS that needs mimalloc's
    // malloc zone to be the default, which a static link does not
    // guarantee (see jemalloc/root.zig for the same trap).
    const s_probe = libc.strdup("galloc probe") orelse
        return error.MimallocNotInUse;
    defer libc.free(s_probe);
    s_probe[0] = 'G';
    if (!mi_is_in_heap_region(s_probe)) {
        return error.MimallocNotInUse;
    }
}

pub const DecayError = error{DecayFailed};

/// mimalloc has no background threads on any platform, so main.zig's
/// maintenance tick always runs for it.
pub const has_background_reclaim = false;

/// The periodic reclaim step the maintenance tick calls (see
/// jemalloc.decay). Gentle by design (`mi_collect(false)`): it honors
/// the 1 s purge deadlines and rotates through the arenas, so a freed
/// burst returns to the OS within a couple of ticks while memory freed
/// and reused inside the window stays warm. The forced variant madvised
/// reused pools away every tick, 80x slower on a burst-reuse cycle in
/// the zmem study, and no collect can release pages pinned by live
/// objects, so there is no stronger lever worth exposing.
///
/// Never fails here; the error union exists so both backends share one
/// decay() signature.
pub fn decay() DecayError!void {
    mi_collect(false);
}
