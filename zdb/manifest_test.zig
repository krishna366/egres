const std = @import("std");
const zdb = @import("zdb");

const manifest = zdb.manifest;
const ManifestError = manifest.ManifestError;
const createTableChange = manifest.createTableChange;
const deleteTableChange = manifest.deleteTableChange;
const createTreeChange = manifest.createTreeChange;
const deleteTreeChange = manifest.deleteTreeChange;

// -------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------

/// Returns the absolute path of the temp directory (caller frees).
fn tmpPath(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir) ![]u8 {
    return tmp.dir.realpathAlloc(alloc, ".");
}

const tree_id: u64 = 1;

/// Commits a tree CREATE for tree_id, then the given table changes.
fn commitWithTree(ms: *manifest.ForestState, table_changes: []const manifest.TableChange) !void {
    try ms.commit(&.{}, &.{createTreeChange(tree_id, 0, &.{}, &.{})});
    if (table_changes.len > 0) {
        try ms.commit(table_changes, &.{});
    }
}

// -------------------------------------------------------------------------
// Test 1: Basic replay
// -------------------------------------------------------------------------

test "manifest basic replay" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // Create and populate.
    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        defer ms.close();

        try ms.commit(&.{}, &.{createTreeChange(tree_id, 0, &.{}, &.{})});
        try ms.commit(&.{
            createTableChange(1, tree_id, 0),
            createTableChange(2, tree_id, 1),
            createTableChange(3, tree_id, 2),
        }, &.{});
    }

    // Reopen and verify.
    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        defer ms.close();

        try std.testing.expectEqual(@as(u32, 3), ms.tableCount());
        const tree = ms.trees.get(tree_id).?;
        try std.testing.expect(tree.levels.items.len >= 3);
        try std.testing.expect(tree.levels.items[0].tables.contains(1));
        try std.testing.expect(tree.levels.items[1].tables.contains(2));
        try std.testing.expect(tree.levels.items[2].tables.contains(3));
    }
}

// -------------------------------------------------------------------------
// Test 2: Magic corruption → BadMagic
// -------------------------------------------------------------------------

test "manifest bad magic" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // Create a valid manifest.
    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        ms.close();
    }

    // Corrupt byte 3 of the magic field.
    {
        const f = try tmp.dir.openFile("MANIFEST", .{ .mode = .read_write });
        defer f.close();
        try f.seekTo(3);
        try f.writeAll(&[_]u8{0xFF});
    }

    // Reopening should return BadMagic.
    try std.testing.expectError(
        ManifestError.BadMagic,
        manifest.openOrCreate(alloc, dir_path),
    );
}

// -------------------------------------------------------------------------
// Test 3: Version corruption → UnsupportedVersion
// -------------------------------------------------------------------------

test "manifest unsupported version" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        ms.close();
    }

    // Corrupt byte 6 (low byte of version field at offset 6).
    {
        const f = try tmp.dir.openFile("MANIFEST", .{ .mode = .read_write });
        defer f.close();
        try f.seekTo(6);
        try f.writeAll(&[_]u8{0x02}); // version 2 is unsupported
    }

    try std.testing.expectError(
        ManifestError.UnsupportedVersion,
        manifest.openOrCreate(alloc, dir_path),
    );
}

// -------------------------------------------------------------------------
// Test 4: Checksum corruption → ChecksumMismatch
// -------------------------------------------------------------------------

test "manifest checksum mismatch" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // Write at least one record so there is a CRC field to corrupt.
    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        defer ms.close();
        try ms.commit(&.{}, &.{createTreeChange(tree_id, 0, &.{}, &.{})});
        try ms.commit(&.{createTableChange(42, tree_id, 0)}, &.{});
    }

    // Corrupt the first byte of the CRC field in the first record.
    // Layout: [8 header][4 payload_len][4 crc][payload]
    // CRC starts at byte 8 + 4 = 12.
    {
        const f = try tmp.dir.openFile("MANIFEST", .{ .mode = .read_write });
        defer f.close();
        try f.seekTo(12);
        var b: [1]u8 = undefined;
        _ = try f.readAll(&b);
        b[0] ^= 0xFF;
        try f.seekTo(12);
        try f.writeAll(&b);
    }

    try std.testing.expectError(
        ManifestError.ChecksumMismatch,
        manifest.openOrCreate(alloc, dir_path),
    );
}

// -------------------------------------------------------------------------
// Test 5: Rewrite trigger
// -------------------------------------------------------------------------

test "manifest rewrite trigger" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        defer ms.close();

        ms.deletions_rewrite_threshold = 10;

        // Create tree first.
        try ms.commit(&.{}, &.{createTreeChange(tree_id, 0, &.{}, &.{})});

        // Create 12 tables (IDs 0..11) at level 0.
        var creates: std.ArrayList(manifest.TableChange) = .empty;
        defer creates.deinit(alloc);
        for (0..12) |id| try creates.append(alloc, createTableChange(@intCast(id), tree_id, 0));
        try ms.commit(creates.items, &.{});

        // Delete tables 1..11 — this commit should trigger the rewrite.
        var deletes: std.ArrayList(manifest.TableChange) = .empty;
        defer deletes.deinit(alloc);
        for (1..12) |id| try deletes.append(alloc, deleteTableChange(@intCast(id), tree_id, 0));
        try ms.commit(deletes.items, &.{});
    }

    // After reopen, only table 0 should be present and the file should be compact.
    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        defer ms.close();

        try std.testing.expectEqual(@as(u32, 1), ms.tableCount());
        const tree = ms.trees.get(tree_id).?;
        try std.testing.expect(tree.levels.items[0].tables.contains(0));
        // After rewrite: creations=1, deletions=0.
        try std.testing.expectEqual(@as(u32, 1), ms.creations);
        try std.testing.expectEqual(@as(u32, 0), ms.deletions);
    }
}

// -------------------------------------------------------------------------
// Test 6: Partial record truncation
// -------------------------------------------------------------------------

test "manifest partial record truncation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // Write a valid record.
    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        defer ms.close();
        try ms.commit(&.{}, &.{createTreeChange(tree_id, 0, &.{}, &.{})});
        try ms.commit(&.{createTableChange(7, tree_id, 0)}, &.{});
    }

    // Append 5 bytes of partial garbage after the valid records.
    {
        const f = try tmp.dir.openFile("MANIFEST", .{ .mode = .read_write });
        defer f.close();
        try f.seekFromEnd(0);
        try f.writeAll(&[_]u8{ 0xDE, 0xAD, 0xBE, 0xEF, 0x00 });
    }

    // Reopen — replay should stop cleanly at the end of the last full record.
    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        defer ms.close();

        try std.testing.expectEqual(@as(u32, 1), ms.tableCount());
        try std.testing.expect(ms.trees.get(tree_id).?.levels.items[0].tables.contains(7));
    }

    // Verify the partial bytes were truncated: write a new record and reopen again.
    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        defer ms.close();
        try ms.commit(&.{createTableChange(8, tree_id, 0)}, &.{});
    }

    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        defer ms.close();

        try std.testing.expectEqual(@as(u32, 2), ms.tableCount());
        const tree = ms.trees.get(tree_id).?;
        try std.testing.expect(tree.levels.items[0].tables.contains(7));
        try std.testing.expect(tree.levels.items[0].tables.contains(8));
    }
}

// -------------------------------------------------------------------------
// Test 7: Tree create and delete persistence
// -------------------------------------------------------------------------

test "manifest tree create and delete" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // Create two trees, add tables, delete one tree.
    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        defer ms.close();

        try ms.commit(&.{}, &.{ createTreeChange(1, 0, &.{}, &.{}), createTreeChange(2, 0, &.{}, &.{}) });
        try ms.commit(&.{
            createTableChange(10, 1, 0),
            createTableChange(20, 2, 0),
        }, &.{});

        // Delete tree 2 — its tables are automatically cleaned up.
        try ms.commit(&.{}, &.{deleteTreeChange(2)});
    }

    // Reopen and verify.
    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        defer ms.close();

        try std.testing.expectEqual(@as(u32, 1), ms.trees.count());
        try std.testing.expect(ms.trees.contains(1));
        try std.testing.expect(!ms.trees.contains(2));
        try std.testing.expectEqual(@as(u32, 1), ms.tableCount());
        try std.testing.expect(ms.trees.get(1).?.levels.items[0].tables.contains(10));
    }
}

// -------------------------------------------------------------------------
// Test 8: Multi-tree table ownership
// -------------------------------------------------------------------------

test "manifest multi-tree table" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        defer ms.close();

        try ms.commit(&.{}, &.{ createTreeChange(1, 0, &.{}, &.{}), createTreeChange(2, 0, &.{}, &.{}) });

        // Same table_id=100 belongs to both trees.
        try ms.commit(&.{
            createTableChange(100, 1, 0),
            createTableChange(100, 2, 0),
        }, &.{});

        // Delete from tree 1 — table should still exist in tree 2.
        try ms.commit(&.{deleteTableChange(100, 1, 0)}, &.{});
        try std.testing.expect(!ms.trees.get(1).?.levels.items[0].tables.contains(100));
        try std.testing.expect(ms.trees.get(2).?.levels.items[0].tables.contains(100));

        // Delete from tree 2 — now table should be gone from all trees.
        try ms.commit(&.{deleteTableChange(100, 2, 0)}, &.{});
        try std.testing.expectEqual(@as(u32, 0), ms.tableCount());
    }
}

// -------------------------------------------------------------------------
// Test 9: Tree key range update
// -------------------------------------------------------------------------

test "manifest tree key range update" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const Key = zdb.key.Key;
    const updateTreeChange = manifest.updateTreeChange;

    const left = Key{ .primary = "aaa" };
    const mid = Key{ .primary = "mmm" };
    const right = Key{ .primary = "zzz" };

    const left_raw = try left.marshal(alloc);
    defer alloc.free(left_raw);
    const mid_raw = try mid.marshal(alloc);
    defer alloc.free(mid_raw);
    const right_raw = try right.marshal(alloc);
    defer alloc.free(right_raw);

    // Create a tree with an initial range, then update it.
    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        defer ms.close();

        try ms.commit(&.{}, &.{createTreeChange(1, 0, left_raw, mid_raw)});
        const tree = ms.trees.get(1).?;
        try std.testing.expectEqualStrings("aaa", tree.left.primary);
        try std.testing.expectEqualStrings("mmm", tree.right.primary);

        // Update range.
        try ms.commit(&.{}, &.{updateTreeChange(1, left_raw, right_raw)});
        const updated = ms.trees.get(1).?;
        try std.testing.expectEqualStrings("aaa", updated.left.primary);
        try std.testing.expectEqualStrings("zzz", updated.right.primary);
    }

    // Reopen and verify persistence.
    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        defer ms.close();

        const tree = ms.trees.get(1).?;
        try std.testing.expectEqualStrings("aaa", tree.left.primary);
        try std.testing.expectEqualStrings("zzz", tree.right.primary);
    }
}
