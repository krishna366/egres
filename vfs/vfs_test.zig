const std = @import("std");
const testing = std.testing;
const zio = @import("zio");
const vfs = @import("vfs.zig");
const bandwidth = @import("bandwidth.zig");
const VFS = vfs.VFS;
const Priority = vfs.Priority;
const page_size = vfs.page_size;

// ── Helpers ──────────────────────────────────────────────────────────

fn writeTempFile(path: []const u8, data: []const u8) !void {
    const file = try std.fs.createFileAbsolute(path, .{});
    defer file.close();
    try file.writeAll(data);
}

// ── 1. Open / close round-trip ───────────────────────────────────────

test "open and close" {
    const path = "/tmp/vfs_test_open_close";
    try writeTempFile(path, "hello world");
    defer std.fs.deleteFileAbsolute(path) catch {};

    const v = vfs.g().*;
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .direct });
    defer handle.close();

    try testing.expectEqual(@as(u64, 11), handle.file_size);
}

// ── 2. readAligned at aligned offset ─────────────────────────────────

test "readAligned at aligned offset" {
    const path = "/tmp/vfs_test_aligned";

    // Write exactly 8192 bytes (two pages).
    var data: [8192]u8 = undefined;
    for (&data, 0..) |*b, i| b.* = @truncate(i);
    try writeTempFile(path, &data);
    defer std.fs.deleteFileAbsolute(path) catch {};

    const v = vfs.g().*;
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .direct });
    defer handle.close();

    // Read 100 bytes at offset 0 (aligned).
    const r = try handle.readAligned(testing.allocator, 0, 100, .p0);
    defer r.free();

    try testing.expectEqual(@as(usize, 100), r.data.len);
    try testing.expectEqualSlices(u8, data[0..100], r.data);
}

// ── 3. readAligned at unaligned offset ───────────────────────────────

test "readAligned at unaligned offset" {
    const path = "/tmp/vfs_test_unaligned";

    var data: [8192]u8 = undefined;
    for (&data, 0..) |*b, i| b.* = @truncate(i);
    try writeTempFile(path, &data);
    defer std.fs.deleteFileAbsolute(path) catch {};

    const v = vfs.g().*;
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .direct });
    defer handle.close();

    // Read 12 bytes at offset 5000 (unaligned).
    const r = try handle.readAligned(testing.allocator, 5000, 12, .p1);
    defer r.free();

    try testing.expectEqual(@as(usize, 12), r.data.len);
    try testing.expectEqualSlices(u8, data[5000..5012], r.data);
}

// ── 4. readAligned alignment math ────────────────────────────────────

test "readAligned alignment math" {
    const path = "/tmp/vfs_test_math";

    var data: [8192]u8 = undefined;
    for (&data, 0..) |*b, i| b.* = @truncate(i);
    try writeTempFile(path, &data);
    defer std.fs.deleteFileAbsolute(path) catch {};

    const v = vfs.g().*;
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .direct });
    defer handle.close();

    // Read 12 bytes at offset 5000.
    // aligned_offset = 4096, prefix = 904, aligned_size = 4096.
    const r = try handle.readAligned(testing.allocator, 5000, 12, .p2);
    defer r.free();

    // Buffer must be 4KB-aligned.
    const buf = r.buf.?;
    try testing.expect(@intFromPtr(buf.ptr) % vfs.page_size == 0);
    // Buffer size is a multiple of page_size.
    try testing.expect(buf.len % vfs.page_size == 0);
    // Data slice is correct.
    try testing.expectEqual(@as(usize, 12), r.data.len);
}

// ── 5. O_DIRECT fallback on tmpfs ────────────────────────────────────

test "O_DIRECT fallback on tmpfs" {
    // /tmp is typically tmpfs on Linux; O_DIRECT is unsupported there.
    // VFS.openFile must succeed via the fallback path.
    const path = "/tmp/vfs_test_direct_fallback";
    try writeTempFile(path, "fallback test");
    defer std.fs.deleteFileAbsolute(path) catch {};

    const v = vfs.g().*;
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .direct });
    defer handle.close();

    try testing.expectEqual(@as(u64, 13), handle.file_size);

    // Also verify we can read through the fallback fd.
    const r = try handle.readAligned(testing.allocator, 0, 13, .p0);
    defer r.free();

    try testing.expectEqualSlices(u8, "fallback test", r.data);
}

// ── 6. Bandwidth limiter integration ─────────────────────────────────

test "bandwidth limiter deducts on read" {
    const path = "/tmp/vfs_test_bw";

    var data: [8192]u8 = undefined;
    @memset(&data, 0xFF);
    try writeTempFile(path, &data);
    defer std.fs.deleteFileAbsolute(path) catch {};

    // High rate so P0 grants everything, but we can observe token deduction.
    var limiter = bandwidth.Limiter.init(.{ .bytes_per_sec = 1_000_000 });
    const pages_before = limiter.pages.load(.monotonic);

    var v = VFS.new(&limiter);
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .direct });
    defer handle.close();

    const r = try handle.readAligned(testing.allocator, 0, 100, .p0);
    defer r.free();

    const pages_after = limiter.pages.load(.monotonic);
    // P0 read of 100 bytes (1 page) should deduct at least 1 page.
    try testing.expect(pages_before - pages_after >= 1);
}

// ── 7. mmap open/close ──────────────────────────────────────────────

test "mmap open and close" {
    const path = "/tmp/vfs_test_mmap_open";
    try writeTempFile(path, "mmap test data");
    defer std.fs.deleteFileAbsolute(path) catch {};

    var v = vfs.g().*;
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .mmap });
    defer handle.close();

    try testing.expectEqual(@as(u64, 14), handle.file_size);
    try testing.expect(handle.mapped != null);
}

// ── 8. mmap readAligned ─────────────────────────────────────────────

test "mmap readAligned" {
    const path = "/tmp/vfs_test_mmap_read";

    var data: [8192]u8 = undefined;
    for (&data, 0..) |*b, i| b.* = @truncate(i);
    try writeTempFile(path, &data);
    defer std.fs.deleteFileAbsolute(path) catch {};

    var v = vfs.g().*;
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .mmap });
    defer handle.close();

    // Aligned offset.
    const r1 = try handle.readAligned(testing.allocator, 0, 100, .p0);
    defer r1.free();
    try testing.expectEqualSlices(u8, data[0..100], r1.data);

    // Unaligned offset.
    const r2 = try handle.readAligned(testing.allocator, 5000, 12, .p1);
    defer r2.free();
    try testing.expectEqualSlices(u8, data[5000..5012], r2.data);
}

// ── 9. mmap readAligned returns no buf ──────────────────────────────

test "mmap readAligned no allocation" {
    const path = "/tmp/vfs_test_mmap_nobuf";
    try writeTempFile(path, "no alloc test!");
    defer std.fs.deleteFileAbsolute(path) catch {};

    var v = vfs.g().*;
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .mmap });
    defer handle.close();

    const r = try handle.readAligned(testing.allocator, 0, 14, .p0);
    defer r.free();

    try testing.expect(r.buf == null);
    try testing.expectEqualSlices(u8, "no alloc test!", r.data);
}

// ── 10. mmap bandwidth ─────────────────────────────────────────────

test "mmap enforces bandwidth limiter" {
    const path = "/tmp/vfs_test_mmap_bw";

    var data: [8192]u8 = undefined;
    @memset(&data, 0xFF);
    try writeTempFile(path, &data);
    defer std.fs.deleteFileAbsolute(path) catch {};

    var limiter = bandwidth.Limiter.init(.{ .bytes_per_sec = 1_000_000 });
    const pages_before = limiter.pages.load(.monotonic);

    var v = VFS.new(&limiter);
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .mmap });
    defer handle.close();

    const r = try handle.readAligned(testing.allocator, 0, 100, .p0);
    defer r.free();

    // mmap reads now go through the limiter — tokens consumed.
    const pages_after = limiter.pages.load(.monotonic);
    try testing.expect(pages_after < pages_before);
}

// ── 11. Read bandwidth enforcement ──────────────────────────────────

test "read bandwidth enforcement" {
    const path = "/tmp/vfs_test_bw_read_rate";
    const file_size = 1024 * page_size; // 4 MB
    const data = try testing.allocator.alloc(u8, file_size);
    defer testing.allocator.free(data);
    @memset(data, 0xAB);
    try writeTempFile(path, data);
    defer std.fs.deleteFileAbsolute(path) catch {};

    // Rate must be >= 1024*page_size so per-ms refill >= page_size
    // (avoids alignToPage inflation). At 4 MB/s, reading 4 MB takes ~1s.
    const rate = 1024 * page_size; // 4 MB/s
    var limiter = bandwidth.Limiter.init(.{
        .bytes_per_sec = rate,
        .timeout_ms = 5000,
        .min_alloc_bytes = page_size,
    });

    var v = VFS.new(&limiter);
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .direct });
    defer handle.close();

    // Drain the initial bucket with a P0 read.
    _ = blk: {
        var s = limiter.spinner(.p0, rate, .read);
        defer s.deinit();
        while (true) {
            if (s.acquireBytes() == 0) break;
        }
        break :blk;
    };
    try testing.expectEqual(@as(u64, 0), limiter.pages.load(.monotonic));

    // Time a P1 read of the full file — must wait for refill.
    const start = std.time.milliTimestamp();
    const r = try handle.readAligned(testing.allocator, 0, file_size, .p1);
    defer r.free();
    const elapsed_ms = std.time.milliTimestamp() - start;

    try testing.expectEqual(file_size, r.data.len);
    // At 4 MB/s, 4 MB should take ~1000ms. Allow wide range for CI variance.
    try testing.expect(elapsed_ms >= 500);
    try testing.expect(elapsed_ms <= 5000);
}

// ── 12. Write bandwidth enforcement ─────────────────────────────────

test "write bandwidth enforcement" {
    const path = "/tmp/vfs_test_bw_write_rate";

    // Create the file first.
    const file_size = 512 * page_size; // 2 MB
    const data = try testing.allocator.alignedAlloc(u8, vfs.page_alignment, file_size);
    defer testing.allocator.free(data);
    @memset(data, 0xCD);
    try writeTempFile(path, data);
    defer std.fs.deleteFileAbsolute(path) catch {};

    // Rate must be >= 8192*page_size so (per-ms refill)/write_cost >= page_size.
    // At 32 MB/s with write_cost=8, effective write rate ~4 MB/s.
    // Writing 2 MB should take ~500ms.
    const rate = 8192 * page_size; // 32 MB/s
    var limiter = bandwidth.Limiter.init(.{
        .bytes_per_sec = rate,
        .write_cost = 8,
        .timeout_ms = 5000,
        .min_alloc_bytes = page_size,
    });

    // Open for writing (need a real fd, not O_DIRECT for writes on tmpfs).
    const fd = try std.posix.open(path, .{ .ACCMODE = .WRONLY }, 0);
    defer std.posix.close(fd);

    var v = VFS.new(&limiter);
    const fh = vfs.FileHandle{ .file = zio.File.fromFd(fd), .file_size = file_size, .vfs = &v };

    // Drain the bucket.
    _ = blk: {
        var s = limiter.spinner(.p0, rate, .read);
        defer s.deinit();
        while (true) {
            if (s.acquireBytes() == 0) break;
        }
        break :blk;
    };

    const start = std.time.milliTimestamp();
    try fh.write(data, 0, .p1);
    const elapsed_ms = std.time.milliTimestamp() - start;

    // Effective write rate ~4 MB/s. 2 MB should take ~500ms. Allow wide range.
    try testing.expect(elapsed_ms >= 250);
    try testing.expect(elapsed_ms <= 5000);
}

// ── 13. P1 prioritized over P2 ──────────────────────────────────────

const PriorityTestCtx = struct {
    handle: vfs.FileHandle,
    bytes_read: std.atomic.Value(u64),
    finish_order: *std.atomic.Value(u32),
    my_order: std.atomic.Value(u32),

    fn readerFn(ctx: *PriorityTestCtx, priority: Priority) void {
        const r = ctx.handle.readAligned(testing.allocator, 0, 128 * page_size, priority) catch return;
        defer r.free();
        ctx.bytes_read.store(@intCast(r.data.len), .monotonic);
        // Record completion order: first finisher gets 0, second gets 1.
        ctx.my_order.store(ctx.finish_order.fetchAdd(1, .seq_cst), .seq_cst);
    }
};

test "P1 prioritized over P2" {
    const path = "/tmp/vfs_test_bw_priority";
    const file_size = 128 * page_size; // 512 KB
    const data = try testing.allocator.alloc(u8, file_size);
    defer testing.allocator.free(data);
    @memset(data, 0xEF);
    try writeTempFile(path, data);
    defer std.fs.deleteFileAbsolute(path) catch {};

    // Rate >= 1024*page_size so per-ms refill >= page_size (no alignment inflation).
    const rate = 4096 * page_size; // 16 MB/s
    var limiter = bandwidth.Limiter.init(.{
        .bytes_per_sec = rate,
        .timeout_ms = 5000,
        .min_alloc_bytes = page_size,
    });

    var v = VFS.new(&limiter);
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .direct });
    defer handle.close();

    // Drain the bucket so both threads must wait for refill.
    _ = blk: {
        var s = limiter.spinner(.p0, rate, .read);
        defer s.deinit();
        while (true) {
            if (s.acquireBytes() == 0) break;
        }
        break :blk;
    };

    var finish_order = std.atomic.Value(u32).init(0);
    var p1_ctx = PriorityTestCtx{ .handle = handle, .bytes_read = std.atomic.Value(u64).init(0), .finish_order = &finish_order, .my_order = std.atomic.Value(u32).init(99) };
    var p2_ctx = PriorityTestCtx{ .handle = handle, .bytes_read = std.atomic.Value(u64).init(0), .finish_order = &finish_order, .my_order = std.atomic.Value(u32).init(99) };

    // Launch P2 first, then P1 — P1 should still finish first.
    const p2_thread = try std.Thread.spawn(.{}, PriorityTestCtx.readerFn, .{ &p2_ctx, .p2 });
    // Small delay so P2 enters the wait loop first.
    std.Thread.sleep(5 * std.time.ns_per_ms);
    const p1_thread = try std.Thread.spawn(.{}, PriorityTestCtx.readerFn, .{ &p1_ctx, .p1 });

    p1_thread.join();
    p2_thread.join();

    // Both should have read the full file.
    try testing.expectEqual(@as(u64, file_size), p1_ctx.bytes_read.load(.monotonic));
    try testing.expectEqual(@as(u64, file_size), p2_ctx.bytes_read.load(.monotonic));

    // P1 should have finished before P2.
    const p1_order = p1_ctx.my_order.load(.seq_cst);
    const p2_order = p2_ctx.my_order.load(.seq_cst);
    try testing.expect(p1_order < p2_order);
}

// ── batchRead tests ─────────────────────────────────────────────────

fn batchReadTestImpl() !void {
    const alloc = testing.allocator;
    const path = "/tmp/vfs_test_batch_read";
    defer std.fs.deleteFileAbsolute(path) catch {};

    // Write a 2 MiB file. Each page filled with a byte derived from page index.
    const num_pages = 512;
    {
        const file = try std.fs.createFileAbsolute(path, .{});
        defer file.close();
        var buf: [page_size]u8 = undefined;
        for (0..num_pages) |i| {
            @memset(&buf, @intCast(i % 251)); // distinct fill per page (mod 251 to stay in u8)
            try file.writeAll(&buf);
        }
    }

    const v = vfs.g().*;
    const handle = try v.openFile(alloc, path, .{ .mode = .default });
    defer handle.close();

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    // Build 512 read requests — exceeds io_uring SQ depth (256), tests chunking.
    const num_requests = 512;
    const read_len = 100;
    const intra_page_offset = 10;
    var requests = try alloc.alloc(vfs.FileHandle.ReadRequest, num_requests);
    defer alloc.free(requests);
    for (0..num_requests) |i| {
        requests[i] = .{
            .offset = @as(u64, i) * page_size + intra_page_offset,
            .len = read_len,
        };
    }

    try handle.batchRead(&arena, requests, .p0);

    // Verify each read got the correct fill byte.
    for (0..num_requests) |i| {
        try testing.expectEqual(@as(usize, read_len), requests[i].data.len);
        const expected_fill: u8 = @intCast(i % 251);
        for (requests[i].data) |b| {
            try testing.expectEqual(expected_fill, b);
        }
    }
}

test "batchRead 512 requests" {
    const rt = try zio.Runtime.init(testing.allocator, .{});
    defer rt.deinit();
    var handle = try rt.spawn(batchReadTestImpl, .{});
    try handle.join();
}
