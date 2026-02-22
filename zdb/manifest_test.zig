const std = @import("std");
const zdb = @import("zdb");

const manifest = zdb.manifest;
const ManifestError = manifest.ManifestError;
const Change = manifest.Change;

// -------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------

/// Returns the absolute path of the temp directory (caller frees).
fn tmpPath(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir) ![]u8 {
    return tmp.dir.realpathAlloc(alloc, ".");
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

        try ms.commit(&.{
            Change.create(1, 0),
            Change.create(2, 1),
            Change.create(3, 2),
        });
    }

    // Reopen and verify.
    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        defer ms.close();

        try std.testing.expectEqual(@as(u32, 3), ms.tables.count());
        try std.testing.expect(ms.tables.contains(1));
        try std.testing.expect(ms.tables.contains(2));
        try std.testing.expect(ms.tables.contains(3));
        try std.testing.expectEqual(@as(u8, 0), ms.tables.get(1).?.level);
        try std.testing.expectEqual(@as(u8, 1), ms.tables.get(2).?.level);
        try std.testing.expectEqual(@as(u8, 2), ms.tables.get(3).?.level);

        // Verify level sets
        try std.testing.expect(ms.levels.items.len >= 3);
        try std.testing.expect(ms.levels.items[0].tables.contains(1));
        try std.testing.expect(ms.levels.items[1].tables.contains(2));
        try std.testing.expect(ms.levels.items[2].tables.contains(3));
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
        try ms.commit(&.{Change.create(42, 0)});
    }

    // Corrupt the first byte of the CRC field in the first record.
    // Layout: [8 header][4 payload_len][4 crc][payload]
    // CRC starts at byte 8 + 4 = 12.
    {
        const f = try tmp.dir.openFile("MANIFEST", .{ .mode = .read_write });
        defer f.close();
        try f.seekTo(12);
        // Read and flip the byte so it's guaranteed wrong.
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

    // Create 12 tables, then delete 11 of them. With threshold=10 and ratio=10,
    // the condition `deletions(11) > threshold(10) AND 11 > 10*(12-11)=10` is met.
    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        defer ms.close();

        // Set a low threshold so the rewrite fires in this test.
        ms.deletions_rewrite_threshold = 10;

        // Create 12 tables (IDs 0..11) at level 0.
        var creates: std.ArrayList(Change) = .empty;
        defer creates.deinit(alloc);
        for (0..12) |id| try creates.append(alloc, Change.create(@intCast(id), 0));
        try ms.commit(creates.items);

        // Delete tables 1..11 — this commit should trigger the rewrite.
        var deletes: std.ArrayList(Change) = .empty;
        defer deletes.deinit(alloc);
        for (1..12) |id| try deletes.append(alloc, Change.delete(@intCast(id)));
        try ms.commit(deletes.items);
    }

    // After reopen, only table 0 should be present and the file should be compact.
    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        defer ms.close();

        try std.testing.expectEqual(@as(u32, 1), ms.tables.count());
        try std.testing.expect(ms.tables.contains(0));
        try std.testing.expectEqual(@as(u8, 0), ms.tables.get(0).?.level);
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
        try ms.commit(&.{Change.create(7, 0)});
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

        try std.testing.expectEqual(@as(u32, 1), ms.tables.count());
        try std.testing.expect(ms.tables.contains(7));
    }

    // Verify the partial bytes were truncated: write a new record and reopen again.
    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        defer ms.close();
        try ms.commit(&.{Change.create(8, 0)});
    }

    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        defer ms.close();

        try std.testing.expectEqual(@as(u32, 2), ms.tables.count());
        try std.testing.expect(ms.tables.contains(7));
        try std.testing.expect(ms.tables.contains(8));
    }
}
