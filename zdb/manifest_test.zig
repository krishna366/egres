const std = @import("std");
const zdb = @import("zdb");

const manifest = zdb.manifest;
const ManifestError = manifest.ManifestError;
const createTableChange = manifest.createTableChange;
const deleteTableChange = manifest.deleteTableChange;
const createTreeChange = manifest.createTreeChange;
const deleteTreeChange = manifest.deleteTreeChange;
const updateTreeChange = manifest.updateTreeChange;

const tree_id: u64 = 1;

// -------------------------------------------------------------------------
// In-memory commit + apply
// -------------------------------------------------------------------------

test "manifest commit applies tree + table changes" {
    const alloc = std.testing.allocator;
    var ms = manifest.ForestState.init(alloc);
    defer ms.deinit();

    try ms.commit(&.{}, &.{createTreeChange(tree_id, 0, &.{}, &.{})});
    try ms.commit(&.{
        createTableChange(1, tree_id, 0),
        createTableChange(2, tree_id, 1),
        createTableChange(3, tree_id, 2),
    }, &.{});

    try std.testing.expectEqual(@as(u32, 3), ms.tableCount());
    const tree = ms.trees.get(tree_id).?;
    try std.testing.expect(tree.levels.items[0].tables.contains(1));
    try std.testing.expect(tree.levels.items[1].tables.contains(2));
    try std.testing.expect(tree.levels.items[2].tables.contains(3));
}

// -------------------------------------------------------------------------
// Tree create and delete (DELETE cascades through tables)
// -------------------------------------------------------------------------

test "manifest tree create and delete" {
    const alloc = std.testing.allocator;
    var ms = manifest.ForestState.init(alloc);
    defer ms.deinit();

    try ms.commit(&.{}, &.{ createTreeChange(1, 0, &.{}, &.{}), createTreeChange(2, 0, &.{}, &.{}) });
    try ms.commit(&.{
        createTableChange(10, 1, 0),
        createTableChange(20, 2, 0),
    }, &.{});

    // Delete tree 2 — its tables are automatically cleaned up.
    try ms.commit(&.{}, &.{deleteTreeChange(2)});

    try std.testing.expectEqual(@as(u32, 1), ms.trees.count());
    try std.testing.expect(ms.trees.contains(1));
    try std.testing.expect(!ms.trees.contains(2));
    try std.testing.expectEqual(@as(u32, 1), ms.tableCount());
    try std.testing.expect(ms.trees.get(1).?.levels.items[0].tables.contains(10));
}

// -------------------------------------------------------------------------
// Multi-tree table ownership (same table_id, different trees)
// -------------------------------------------------------------------------

test "manifest multi-tree table" {
    const alloc = std.testing.allocator;
    var ms = manifest.ForestState.init(alloc);
    defer ms.deinit();

    try ms.commit(&.{}, &.{ createTreeChange(1, 0, &.{}, &.{}), createTreeChange(2, 0, &.{}, &.{}) });

    // Same table_id=100 belongs to both trees (post-split shared SST scenario).
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

// -------------------------------------------------------------------------
// Tree key range update
// -------------------------------------------------------------------------

test "manifest tree key range update" {
    const alloc = std.testing.allocator;
    var ms = manifest.ForestState.init(alloc);
    defer ms.deinit();

    const Key = zdb.key.Key;

    const left = Key{ .primary = "aaa" };
    const mid = Key{ .primary = "mmm" };
    const right = Key{ .primary = "zzz" };

    const left_raw = try left.marshal(alloc);
    defer alloc.free(left_raw);
    const mid_raw = try mid.marshal(alloc);
    defer alloc.free(mid_raw);
    const right_raw = try right.marshal(alloc);
    defer alloc.free(right_raw);

    try ms.commit(&.{}, &.{createTreeChange(1, 0, left_raw, mid_raw)});
    {
        const tree = ms.trees.get(1).?;
        try std.testing.expectEqualStrings("aaa", tree.left.primary);
        try std.testing.expectEqualStrings("mmm", tree.right.primary);
    }

    try ms.commit(&.{}, &.{updateTreeChange(1, left_raw, right_raw)});
    {
        const tree = ms.trees.get(1).?;
        try std.testing.expectEqualStrings("aaa", tree.left.primary);
        try std.testing.expectEqualStrings("zzz", tree.right.primary);
    }
}

// -------------------------------------------------------------------------
// Tree-not-found errors
// -------------------------------------------------------------------------

test "manifest applyTableChange on missing tree errors" {
    const alloc = std.testing.allocator;
    var ms = manifest.ForestState.init(alloc);
    defer ms.deinit();

    try std.testing.expectError(
        ManifestError.TreeNotFound,
        ms.commit(&.{createTableChange(1, 999, 0)}, &.{}),
    );
}

// -------------------------------------------------------------------------
// serialize → restoreFrom round-trip
// -------------------------------------------------------------------------

test "manifest serialize/restoreFrom round-trip" {
    const alloc = std.testing.allocator;

    // Build a non-trivial state: 2 trees with key ranges + namespaces, tables
    // spread across L0–L3, including the "shared table across trees" case.
    var src = manifest.ForestState.init(alloc);
    defer src.deinit();

    const Key = zdb.key.Key;
    const a_raw = try (Key{ .primary = "aaa" }).marshal(alloc);
    defer alloc.free(a_raw);
    const m_raw = try (Key{ .primary = "mmm" }).marshal(alloc);
    defer alloc.free(m_raw);
    const z_raw = try (Key{ .primary = "zzz" }).marshal(alloc);
    defer alloc.free(z_raw);

    try src.commit(&.{}, &.{
        createTreeChange(1, 7, a_raw, m_raw),
        createTreeChange(2, 7, m_raw, z_raw),
    });
    try src.commit(&.{
        createTableChange(10, 1, 0),
        createTableChange(11, 1, 0),
        createTableChange(12, 1, 1),
        createTableChange(13, 1, 3),
        createTableChange(20, 2, 2),
        // Same table_id present in both trees — shared post-split SST.
        createTableChange(50, 1, 3),
        createTableChange(50, 2, 3),
    }, &.{});

    // Serialize.
    const bytes = try src.serialize(alloc);
    defer alloc.free(bytes);
    try std.testing.expect(bytes.len > 0);

    // Restore into a fresh state.
    var dst = manifest.ForestState.init(alloc);
    defer dst.deinit();
    try dst.restoreFrom(bytes);

    // Counts match.
    try std.testing.expectEqual(src.trees.count(), dst.trees.count());
    try std.testing.expectEqual(src.tableCount(), dst.tableCount());

    // Tree 1: range, namespace, level 0/1/3 tables, shared table at L3.
    const t1 = dst.trees.get(1).?;
    try std.testing.expectEqual(@as(u32, 7), t1.namespace_id);
    try std.testing.expectEqualStrings("aaa", t1.left.primary);
    try std.testing.expectEqualStrings("mmm", t1.right.primary);
    try std.testing.expect(t1.levels.items[0].tables.contains(10));
    try std.testing.expect(t1.levels.items[0].tables.contains(11));
    try std.testing.expect(t1.levels.items[1].tables.contains(12));
    try std.testing.expect(t1.levels.items[3].tables.contains(13));
    try std.testing.expect(t1.levels.items[3].tables.contains(50));

    // Tree 2: range, namespace, table 20 at L2, shared 50 at L3.
    const t2 = dst.trees.get(2).?;
    try std.testing.expectEqual(@as(u32, 7), t2.namespace_id);
    try std.testing.expectEqualStrings("mmm", t2.left.primary);
    try std.testing.expectEqualStrings("zzz", t2.right.primary);
    try std.testing.expect(t2.levels.items[2].tables.contains(20));
    try std.testing.expect(t2.levels.items[3].tables.contains(50));
}

// -------------------------------------------------------------------------
// Restoring into a non-empty state replaces (not merges) the contents
// -------------------------------------------------------------------------

test "manifest restoreFrom replaces existing state" {
    const alloc = std.testing.allocator;

    var src = manifest.ForestState.init(alloc);
    defer src.deinit();
    try src.commit(&.{}, &.{createTreeChange(1, 0, &.{}, &.{})});
    try src.commit(&.{createTableChange(10, 1, 0)}, &.{});
    const bytes = try src.serialize(alloc);
    defer alloc.free(bytes);

    // Fill `dst` with garbage that should disappear after restore.
    var dst = manifest.ForestState.init(alloc);
    defer dst.deinit();
    try dst.commit(&.{}, &.{createTreeChange(99, 0, &.{}, &.{})});
    try dst.commit(&.{createTableChange(999, 99, 0)}, &.{});
    try std.testing.expect(dst.trees.contains(99));

    try dst.restoreFrom(bytes);

    try std.testing.expect(!dst.trees.contains(99));
    try std.testing.expect(dst.trees.contains(1));
    try std.testing.expectEqual(@as(u32, 1), dst.tableCount());
}

// -------------------------------------------------------------------------
// clone produces an independent deep copy
// -------------------------------------------------------------------------

test "manifest clone is independent of source" {
    const alloc = std.testing.allocator;

    var src = manifest.ForestState.init(alloc);
    defer src.deinit();
    try src.commit(&.{}, &.{createTreeChange(1, 5, &.{}, &.{})});
    try src.commit(&.{createTableChange(10, 1, 0)}, &.{});

    var copy = try src.clone(alloc);
    defer copy.deinit();

    // Mutating src must not affect copy.
    try src.commit(&.{createTableChange(11, 1, 0)}, &.{});
    try std.testing.expectEqual(@as(u32, 2), src.tableCount());
    try std.testing.expectEqual(@as(u32, 1), copy.tableCount());
    try std.testing.expect(copy.trees.get(1).?.levels.items[0].tables.contains(10));
    try std.testing.expect(!copy.trees.get(1).?.levels.items[0].tables.contains(11));

    // Mutating copy must not affect src.
    try copy.commit(&.{}, &.{deleteTreeChange(1)});
    try std.testing.expectEqual(@as(u32, 0), copy.trees.count());
    try std.testing.expectEqual(@as(u32, 1), src.trees.count());
}
