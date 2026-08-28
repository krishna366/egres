const std = @import("std");
const testing = std.testing;
const utils = @import("utils");
const vfs = @import("vfs");

fn tmpPath(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir) ![:0]u8 {
    return tmp.dir.realPathFileAlloc(utils.context.g().io, ".", alloc);
}

test "filestore: dir backend put/get/exists/delete round-trip" {
    const alloc = testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmpPath(alloc, &tmp);
    defer alloc.free(root);
    const depot = try std.fmt.allocPrint(alloc, "{s}/depot", .{root});
    defer alloc.free(depot);

    var store = try vfs.FileStore.initDir(alloc, depot);
    defer store.deinit();

    // Missing object: exists false, open fails.
    try testing.expect(!try store.exists("0001_000000000001.sst"));
    try testing.expectError(error.FileNotFound, store.open("0001_000000000001.sst"));

    // put writes O_DIRECT: page-aligned, page-multiple buffers only
    // (SST buffers are both by construction).
    const payload = try alloc.alignedAlloc(u8, vfs.page_alignment, vfs.page_size);
    defer alloc.free(payload);
    for (payload, 0..) |*b, i| b.* = @truncate(i *% 31);

    try store.put("0001_000000000001.sst", payload);
    try testing.expect(try store.exists("0001_000000000001.sst"));

    // Whole-object read.
    const got = try store.getAll(alloc, "0001_000000000001.sst");
    defer alloc.free(got);
    try testing.expectEqualSlices(u8, payload, got);

    // Ranged read through the handle.
    var obj = try store.open("0001_000000000001.sst");
    defer obj.close();
    try testing.expectEqual(@as(u64, vfs.page_size), obj.size());
    const mid = try obj.read(alloc, 100, 32);
    defer alloc.free(mid);
    try testing.expectEqualSlices(u8, payload[100..132], mid);

    // Overwrite is idempotent (write-once contract: same bytes).
    try store.put("0001_000000000001.sst", payload);
    const again = try store.getAll(alloc, "0001_000000000001.sst");
    defer alloc.free(again);
    try testing.expectEqualSlices(u8, payload, again);

    // Unaligned buffers (address or length) are rejected.
    try testing.expectError(error.UnalignedPut, store.put("bad.sst", payload[1..]));
    try testing.expectError(error.UnalignedPut, store.put("bad.sst", payload[0 .. vfs.page_size - 1]));

    // Delete, then gone.
    try store.delete("0001_000000000001.sst");
    try testing.expect(!try store.exists("0001_000000000001.sst"));
}

test "filestore: dir probeRead times an O_DIRECT ranged read" {
    const alloc = testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmpPath(alloc, &tmp);
    defer alloc.free(root);
    const depot = try std.fmt.allocPrint(alloc, "{s}/depot", .{root});
    defer alloc.free(depot);

    var store = try vfs.FileStore.initDir(alloc, depot);
    defer store.deinit();

    // A small probe-patterned object (position-dependent bytes from 0);
    // page-aligned because put writes O_DIRECT.
    const data = try alloc.alignedAlloc(u8, vfs.page_alignment, 64 << 10);
    defer alloc.free(data);
    for (data, 0..) |*b, i| b.* = vfs.lookahead.probeByte(i);
    try store.put(vfs.lookahead.probe_object, data);

    // Unaligned request: offset rounds down to a page boundary, the
    // window keeps its length; both timings are real (the fixed-cost
    // page and the ranged read are separate preads, so no ordering
    // between them is guaranteed).
    const p = try store.dir.probeRead(5000, 8192);
    try testing.expectEqual(@as(u64, 8192), p.size);
    try testing.expect(p.ttfb_ns > 0 and p.total_ns > 0);

    // A probe object holding the wrong bytes must fail verification.
    @memset(data, 0);
    try store.put(vfs.lookahead.probe_object, data);
    try testing.expectError(error.BadProbeData, store.dir.probeRead(0, 4096));
}

test "filestore: none backend refuses every op" {
    var store: vfs.FileStore = .none;
    defer store.deinit();
    try testing.expectError(error.NoFileStore, store.put("x", "y"));
    try testing.expectError(error.NoFileStore, store.exists("x"));
    try testing.expectError(error.NoFileStore, store.open("x"));
}
