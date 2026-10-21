// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Generic allocator-contract tests for the build-selected backend.
//!
//! `galloc` is whichever adapter `-Dallocator` picked (see build.zig),
//! so `zig build test` exercises mimalloc and
//! `zig build test -Dallocator=jemalloc` exercises jemalloc — same
//! tests, no backend-specific C APIs. Everything here goes through the
//! std.mem.Allocator interface plus the small API both adapters share
//! (kind, allocator, verifyInUse, snapshot, decay).

const std = @import("std");
const fiber = @import("zfiber");
const galloc = @import("galloc");
const utils = @import("utils");

test "galloc: verifyInUse accepts its own allocator" {
    try galloc.verifyInUse(galloc.allocator());
}

test "galloc: verifyInUse accepts the counting wrapper main uses" {
    var counting = utils.mem.CountingAlloc.init(
        galloc.allocator(),
    );
    try galloc.verifyInUse(counting.allocator());
}

test "galloc: verifyInUse rejects a foreign allocator" {
    // std.testing.allocator draws pages straight from mmap (debug
    // allocator over the page allocator), never from the backend, so
    // the probe must fail for either backend.
    if (galloc.verifyInUse(std.testing.allocator)) {
        return error.ForeignAllocatorAccepted;
    } else |_| {}
}

test "galloc: round-trips sizes across class boundaries" {
    const gpa = galloc.allocator();
    // p-1 / p / p+1 around every power of two up to 1 MiB: both sides
    // of any size-class rounding edge a backend might have.
    var power: usize = 8;
    while (power <= 1 << 20) : (power *= 2) {
        for ([_]usize{ power - 1, power, power + 1 }) |len| {
            const memory = try gpa.alloc(u8, len);
            defer gpa.free(memory);
            memory[0] = 0xa5;
            memory[len - 1] = 0x5a;
            try std.testing.expectEqual(0xa5, memory[0]);
            try std.testing.expectEqual(0x5a, memory[len - 1]);
        }
    }
}

test "galloc: honors requested alignment" {
    const gpa = galloc.allocator();
    inline for (.{ 16, 64, 4096 }) |bytes| {
        const alignment = comptime std.mem.Alignment.fromByteUnits(bytes);
        const memory = try gpa.alignedAlloc(u8, alignment, bytes + 1);
        defer gpa.free(memory);
        try std.testing.expectEqual(0, @intFromPtr(memory.ptr) % bytes);
        @memset(memory, 0xa5);
    }
}

test "galloc: realloc preserves data across grow and shrink" {
    const gpa = galloc.allocator();
    var memory = try gpa.alloc(u8, 1000);
    for (memory, 0..) |*byte, i| byte.* = @truncate(i);
    memory = try gpa.realloc(memory, 64 * 1024);
    for (memory[0..1000], 0..) |byte, i| {
        try std.testing.expectEqual(@as(u8, @truncate(i)), byte);
    }
    memory = try gpa.realloc(memory, 100);
    for (memory, 0..) |byte, i| {
        try std.testing.expectEqual(@as(u8, @truncate(i)), byte);
    }
    gpa.free(memory);
}

test "galloc: remap never moves — in-place success or null" {
    // Both adapters implement remap as in-place-or-null: neither
    // backend can relocate a block without a memcpy, and the interface
    // contract says to return null in that case so the caller does the
    // copy itself. A returned pointer must therefore be the original.
    const gpa = galloc.allocator();
    var memory = try gpa.alloc(u8, 1000);
    for (memory, 0..) |*byte, i| byte.* = @truncate(i);
    const old_ptr = memory.ptr;
    // A same-class grow, a cross-class grow, and a shrink.
    for ([_]usize{ 1024, 512 * 1024, 100 }) |new_len| {
        if (gpa.remap(memory, new_len)) |moved| {
            try std.testing.expectEqual(old_ptr, moved.ptr);
            memory = moved;
        }
    }
    try std.testing.expectEqual(99, memory[99]);
    gpa.free(memory);
}

test "galloc: successful resize leaves the block freeable at new_len" {
    // Both adapters free with sized deallocation, so resize may claim
    // success only when a later free at `new_len` still names the
    // block's real size class — the contract this test pins down.
    const gpa = galloc.allocator();
    var small = try gpa.alloc(u8, 120);
    if (gpa.resize(small, 64)) small.len = 64;
    @memset(small, 0xa5);
    gpa.free(small);

    var big = try gpa.alloc(u8, 2 * 1024 * 1024);
    if (gpa.resize(big, 1024 * 1024)) big.len = 1024 * 1024;
    big[big.len - 1] = 0x5a;
    gpa.free(big);
}

test "galloc: blocks allocated here can be freed on another fiber" {
    // The deployment pattern: fibers land on whichever worker is free,
    // so ownership routinely crosses OS threads between alloc and free.
    const gpa = galloc.allocator();
    const Freer = struct {
        fn run(alloc: std.mem.Allocator, blocks: [][]u8) void {
            for (blocks) |block| alloc.free(block);
        }
    };
    var blocks: [64][]u8 = undefined;
    for (&blocks, 0..) |*block, i| {
        block.* = try gpa.alloc(u8, 64 + i * 137);
        block.*[0] = @truncate(i);
    }
    var handle = try fiber.spawn(.{}, Freer.run, .{ gpa, blocks[0..] });
    handle.join();
}

test "galloc: decay and snapshot are callable, with and without dirty pages" {
    // The maintenance tick calls decay once a second whatever the
    // allocator has been doing, so both the "something to purge" and
    // the "nothing to do" paths must be fine, repeatedly.
    _ = galloc.snapshot();
    try galloc.decay();
    const burst = try galloc.allocator().alloc(u8, 8 * 1024 * 1024);
    galloc.allocator().free(burst);
    try galloc.decay();
    try galloc.decay();
    _ = galloc.snapshot();
}
