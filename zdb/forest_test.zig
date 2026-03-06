const std = @import("std");
const zio = @import("zio");
const zdb = @import("zdb");
const vfs = zdb.vfs;

const Key = zdb.key.Key;
const table_mod = zdb.table;
const Builder = table_mod.builder.Builder;
const Table = table_mod.Table;

const manifest = zdb.manifest;
const KeyRange = zdb.level.KeyRange;

const forest_mod = zdb.forest;
const Forest = forest_mod.Forest;
const RouteTable = forest_mod.RouteTable;

const options_mod = zdb.options;

test {
    _ = try zio.Runtime.init(std.heap.smp_allocator, .{});
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn tmpPath(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir) ![]u8 {
    return tmp.dir.realpathAlloc(alloc, ".");
}

// ---------------------------------------------------------------------------
// 1. RouteTable lookup
// ---------------------------------------------------------------------------

test "route table single entry" {
    const alloc = std.testing.allocator;
    var rt = RouteTable.init(alloc, 1);
    defer rt.deinit();

    // Single tree with infinite range covers everything.
    try rt.addRoute(10, .{ .inf = true });
    try std.testing.expectEqual(@as(u64, 10), rt.lookup(.{ .namespace = 1, .primary = "anything" }).?);
}

test "route table multi entry" {
    const alloc = std.testing.allocator;
    var rt = RouteTable.init(alloc, 1);
    defer rt.deinit();

    // Two trees: [a, m) and [m, z].
    const ak = try (Key{ .namespace = 1, .primary = "a" }).marshal(alloc);
    defer alloc.free(ak);
    const mk = try (Key{ .namespace = 1, .primary = "m" }).marshal(alloc);
    defer alloc.free(mk);
    const lk = try (Key{ .namespace = 1, .primary = "lzzz" }).marshal(alloc);
    defer alloc.free(lk);
    const zk = try (Key{ .namespace = 1, .primary = "z" }).marshal(alloc);
    defer alloc.free(zk);

    try rt.addRoute(1, .{ .left = Key.init(ak), .right = Key.init(lk) });
    try rt.addRoute(2, .{ .left = Key.init(mk), .right = Key.init(zk) });

    // "c" falls in tree 1.
    try std.testing.expectEqual(@as(u64, 1), rt.lookup(.{ .namespace = 1, .primary = "c" }).?);
    // "n" falls in tree 2.
    try std.testing.expectEqual(@as(u64, 2), rt.lookup(.{ .namespace = 1, .primary = "n" }).?);
}

// ---------------------------------------------------------------------------
// 2. Forest open/close
// ---------------------------------------------------------------------------

test "forest open close empty" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f = try Forest.open(alloc, .{ .dir = dir_path });
    f.close();
}

// ---------------------------------------------------------------------------
// 3. Forest create tree + get
// ---------------------------------------------------------------------------

test "forest create tree and get" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f = try Forest.open(alloc, .{ .dir = dir_path });
    defer f.close();

    // Create a tree for namespace 1 covering all keys.
    const tree_id = try f.createTree(1, .{ .inf = true });
    try std.testing.expect(tree_id > 0);

    // Verify the tree exists.
    const tree = f.getTree(tree_id);
    try std.testing.expect(tree != null);
    try std.testing.expectEqual(tree_id, tree.?.tree_id);
}

// ---------------------------------------------------------------------------
// 4. Forest create tree survives reopen
// ---------------------------------------------------------------------------

test "forest tree persists across reopen" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // Create forest and add a tree.
    var tree_id: u64 = undefined;
    {
        const f = try Forest.open(alloc, .{ .dir = dir_path });
        tree_id = try f.createTree(42, .{ .inf = true });
        f.close();
    }

    // Reopen and verify tree is still there.
    {
        const f = try Forest.open(alloc, .{ .dir = dir_path });
        defer f.close();

        const tree = f.getTree(tree_id);
        try std.testing.expect(tree != null);
        try std.testing.expectEqual(tree_id, tree.?.tree_id);
    }
}

// ---------------------------------------------------------------------------
// 5. Forest route table lookup
// ---------------------------------------------------------------------------

test "forest routes to correct tree" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f = try Forest.open(alloc, .{ .dir = dir_path });
    defer f.close();

    // Create two trees in different namespaces.
    _ = try f.createTree(1, .{ .inf = true });
    _ = try f.createTree(2, .{ .inf = true });

    // Both namespaces should have a route table.
    {
        f.mu.lockShared();
        defer f.mu.unlockShared();
        try std.testing.expect(f.ns_routes.contains(1));
        try std.testing.expect(f.ns_routes.contains(2));
    }
}
