const std = @import("std");
const utils = @import("utils");
const zdb = @import("zdb");
const vfs = zdb.vfs;

const Key = zdb.key.Key;
const table_mod = zdb.table;
const Builder = table_mod.builder.Builder;
const Table = table_mod.Table;

const manifest = zdb.manifest;
const KeyRange = zdb.level.KeyRange;
const Tree = zdb.tree.Tree;

// A stand-in Tree for TableTrees unit tests: only `tree_id` and `key_range`
// are read by addTree/lookup/removeTree, so the rest is left undefined.
fn fakeTree(tree_id: u64, range: KeyRange) Tree {
    var t: Tree = undefined;
    t.tree_id = tree_id;
    t.key_range = range;
    return t;
}

// Total trees across all tables (replaces the old Forest.trees count).
fn treeCount(f: *Forest) usize {
    var n: usize = 0;
    var it = f.tables.valueIterator();
    while (it.next()) |trees| n += trees.*.entries.items.len;
    return n;
}

const forest_mod = zdb.forest;
const Forest = forest_mod.Forest;
const TableTrees = forest_mod.TableTrees;

const config = zdb.config;


// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn tmpPath(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir) ![:0]u8 {
    // realPathFileAlloc returns [:0]u8 (allocates len+1, sentinel at end).
    // Returning []u8 would make callers free the wrong size and trip
    // DebugAllocator's "alloc 49 != free 48" check.
    return tmp.dir.realPathFileAlloc(utils.context.g().io, ".", alloc);
}

// Force semantic analysis of every Forest/TableTrees method, even ones the
// other tests never call (e.g. the flush path). Zig only type-checks functions
// that are referenced, so an uninstantiated method can carry a type error
// indefinitely; this makes those surface as a compile error in `zig build test`.
test "forest: all decls type-check" {
    std.testing.refAllDecls(Forest);
    std.testing.refAllDecls(TableTrees);
}

// ---------------------------------------------------------------------------
// 1. TableTrees lookup
// ---------------------------------------------------------------------------

test "forest: route table single entry" {
    const alloc = std.testing.allocator;
    var trees = TableTrees.init(alloc);
    defer trees.deinit();

    // Single tree with infinite range covers everything.
    var t10 = fakeTree(10, .{ .inf = true });
    try trees.addTree(&t10);
    try std.testing.expectEqual(@as(u64, 10), trees.lookup(.{ .namespace = 1, .primary = "anything" }).?.tree_id);
}

test "forest: route table multi-entry" {
    const alloc = std.testing.allocator;
    var trees = TableTrees.init(alloc);
    defer trees.deinit();

    // Two trees: [a, m) and [m, z].
    const ak = try (Key{ .namespace = 1, .primary = "a" }).marshal(alloc);
    defer alloc.free(ak);
    const mk = try (Key{ .namespace = 1, .primary = "m" }).marshal(alloc);
    defer alloc.free(mk);
    const lk = try (Key{ .namespace = 1, .primary = "lzzz" }).marshal(alloc);
    defer alloc.free(lk);
    const zk = try (Key{ .namespace = 1, .primary = "z" }).marshal(alloc);
    defer alloc.free(zk);

    var t1 = fakeTree(1, .{ .left = Key.init(ak), .right = Key.init(lk) });
    var t2 = fakeTree(2, .{ .left = Key.init(mk), .right = Key.init(zk) });
    try trees.addTree(&t1);
    try trees.addTree(&t2);

    // "c" falls in tree 1.
    try std.testing.expectEqual(@as(u64, 1), trees.lookup(.{ .namespace = 1, .primary = "c" }).?.tree_id);
    // "n" falls in tree 2.
    try std.testing.expectEqual(@as(u64, 2), trees.lookup(.{ .namespace = 1, .primary = "n" }).?.tree_id);
}

// ---------------------------------------------------------------------------
// 2. Forest open/close
// ---------------------------------------------------------------------------

test "forest: open close empty" {
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

test "forest: create tree and get" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f = try Forest.open(alloc, .{ .dir = dir_path });
    defer f.close();

    // Create a tree for namespace 1's default table (table_id 0). A fresh
    // table starts at split 0, so the derived tree_id is 0.
    const tree_id = try f.createTree(1, 0, .{ .inf = true });
    try std.testing.expectEqual(@as(u64, 0), tree_id);

    // Verify the tree exists.
    const tree = f.getTree(1, tree_id);
    try std.testing.expect(tree != null);
    try std.testing.expectEqual(tree_id, tree.?.tree_id);
}

// Note: There used to be a `testForestTreePersistsAcrossReopen` test here.
// With the snapshot-resident manifest design, the on-disk manifest is gone:
// persistence flows through the Raft log + periodic snapshots. The
// equivalent end-to-end recovery test will live in raft/snapshot_test.zig
// once the snapshot apply path is wired up.

// ---------------------------------------------------------------------------
// 5. Forest route table lookup
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// 6. Tree splitting
// ---------------------------------------------------------------------------

// Helper: build a minimal SST table containing one key and write it to disk.
fn buildAndWriteTable(alloc: std.mem.Allocator, path: []const u8, primary: []const u8) !void {
    var bld = Builder.init(alloc, .{ .target_block_size = 4 * 1024 });
    defer bld.deinit();
    try bld.add(.{ .primary = primary }, "v");
    var tb = try bld.finish();
    defer tb.deinit();
    try vfs.writeFile(path, tb.bytes);
}

test "forest: split tree" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // findSplitKey now requires Lmax to have ≥ 10 tables and pulls
    // candidates from there. Install 10 single-key SSTs ("k01"…"k10")
    // directly into L3 (default max_levels=4 → lmax_idx=3) so the
    // splitter has a clean, evenly-sized Lmax to work with.
    const f = try Forest.open(alloc, .{ .dir = dir_path });
    defer f.close();

    // Default table (table_id 0) → derived tree_id 0.
    const tree_id = try f.createTree(42, 0, .{ .inf = true });
    try std.testing.expectEqual(@as(u64, 0), tree_id);

    const table_keys = [_][]const u8{ "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10" };
    for (table_keys, 1..) |key, tbl_id| {
        const path = try zdb.file_ids.sstPath(alloc, dir_path, tbl_id);
        defer alloc.free(path);
        try buildAndWriteTable(alloc, path, key);
    }

    {
        const tree_ptr = f.getTree(42, tree_id).?;
        var changes: [10]manifest.SSTableChange = undefined;
        for (&changes, 1..) |*c, tbl_id| c.* = manifest.createSSTableChange(@intCast(tbl_id), 42, tree_id, 3); // L3
        try tree_ptr.applyCompaction(&changes);
        try std.testing.expectEqual(@as(usize, 10), tree_ptr.levels[3].numTables());
    }

    // splitTreeForTests requires the write lock.
    {
        f.mu.lock(@src());
        defer f.mu.unlock();
        try f.splitTreeForTests(42, tree_id);
    }

    // Now 2 trees should exist.
    try std.testing.expectEqual(@as(usize, 2), treeCount(f));

    // Best balance with 10 equal-size L3 tables: split at items[5].smallest
    // = "k06" (left=5 tables, right=5 tables, |L−R| = 0). Old tree keeps
    // [k01..k05]; new tree gets [k06..k10]. The split's new half is the
    // table's next split index → tree_id 1; the parent stays tree_id 0.
    const old_tree = f.getTree(42, 0).?;
    const new_tree = f.getTree(42, 1).?;
    try std.testing.expectEqual(@as(usize, 5), old_tree.levels[3].numTables());
    try std.testing.expectEqual(@as(usize, 5), new_tree.levels[3].numTables());

    // Key ranges: old right = "k06", new left = "k06".
    try std.testing.expectEqualStrings("k06", old_tree.key_range.right.primary);
    try std.testing.expectEqualStrings("k06", new_tree.key_range.left.primary);

    // Route table for namespace 42, table 0, should have 2 entries.
    const trees = f.tables.get(.{ .namespace = 42, .table_id = 0 }).?;
    try std.testing.expectEqual(@as(usize, 2), trees.entries.items.len);

    // Routing: "k01" → old tree (0), "k10" → new tree (1).
    try std.testing.expectEqual(@as(u64, 0), trees.lookup(.{ .primary = "k01" }).?.tree_id);
    try std.testing.expectEqual(@as(u64, 1), trees.lookup(.{ .primary = "k10" }).?.tree_id);
}

// ---------------------------------------------------------------------------
// 6b. Two-level routing — per-table_id table isolation
// ---------------------------------------------------------------------------

// Build a one-key SST carrying the full (namespace, table_id, primary) key so
// the stored key is table-specific, and write it to disk.
fn buildAndWriteTableKV(alloc: std.mem.Allocator, path: []const u8, key: Key, value: []const u8) !void {
    var bld = Builder.init(alloc, .{ .target_block_size = 4 * 1024 });
    defer bld.deinit();
    try bld.add(key, value);
    var tb = try bld.finish();
    defer tb.deinit();
    try vfs.writeFile(path, tb.bytes);
}

test "forest: two-level routing isolates by table_id" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f = try Forest.open(alloc, .{ .dir = dir_path });
    defer f.close();

    // Three tables in namespace 2: table_id 0 (the default), 1, and 2. Each is its
    // own tree, so the same primary key "row" lives independently in each.
    const t0 = try f.createTree(2, 0, .{ .inf = true });
    const t1 = try f.createTree(2, 1, .{ .inf = true });
    const t2 = try f.createTree(2, 2, .{ .inf = true });
    try std.testing.expect(t0 != t1 and t1 != t2 and t0 != t2);
    // An unrouted table_id has no table.
    try std.testing.expect(f.tables.get(.{ .namespace = 2, .table_id = 3 }) == null);

    // One SST per table: same primary, distinct value, installed at L1.
    const Case = struct { tid: u32, tree: u64, val: []const u8 };
    const cases = [_]Case{
        .{ .tid = 0, .tree = t0, .val = "v0" },
        .{ .tid = 1, .tree = t1, .val = "v1" },
        .{ .tid = 2, .tree = t2, .val = "v2" },
    };
    for (cases, 1..) |b, file_id| {
        const path = try zdb.file_ids.sstPath(alloc, dir_path, file_id);
        defer alloc.free(path);
        try buildAndWriteTableKV(alloc, path, .{
            .namespace = 2,
            .table_id = b.tid,
            .primary = "row",
            .timestamp = 1,
        }, b.val);
        const changes = [_]manifest.SSTableChange{manifest.createSSTableChange(@intCast(file_id), 2, b.tree, 1)};
        try f.getTree(2, b.tree).?.applyCompaction(&changes);
    }

    // A get routes to the tree for its (namespace, table_id) table, so each
    // table_id returns only its own value despite sharing the primary key.
    for (cases) |b| {
        const got = try f.get(alloc, .{
            .namespace = 2,
            .table_id = b.tid,
            .primary = "row",
            .timestamp = std.math.maxInt(u64),
        }, .p0);
        try std.testing.expect(got != null);
        defer got.?.free(alloc);
        try std.testing.expectEqualStrings(b.val, got.?.value);
    }

    // Unrouted table (table_id 3) and an absent primary both miss.
    try std.testing.expect((try f.get(alloc, .{ .namespace = 2, .table_id = 3, .primary = "row", .timestamp = std.math.maxInt(u64) }, .p0)) == null);
    try std.testing.expect((try f.get(alloc, .{ .namespace = 2, .table_id = 1, .primary = "absent", .timestamp = std.math.maxInt(u64) }, .p0)) == null);
}

// ---------------------------------------------------------------------------
// 6c. create_tree / drop_tree manifest ops
// ---------------------------------------------------------------------------

test "forest: create_tree + drop_tree round-trip" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f = try Forest.open(alloc, .{ .dir = dir_path });
    defer f.close();

    // Create a new table (ns=2, table_id=100) via the manifest op.
    const mc = try f.buildCreateTree(alloc, 2, 100);
    defer alloc.free(mc.tree_changes);
    try f.applyCreateTree(mc);
    const tree_id = mc.tree_changes[0].tree_id;
    try std.testing.expect(f.tables.get(.{ .namespace = 2, .table_id = 100 }) != null);

    // Install one SST and confirm the row reads back through routing.
    const path = try zdb.file_ids.sstPath(alloc, dir_path, 1);
    defer alloc.free(path);
    try buildAndWriteTableKV(alloc, path, .{ .namespace = 2, .table_id = 100, .primary = "row", .timestamp = 1 }, "v");
    {
        const ch = [_]manifest.SSTableChange{manifest.createSSTableChange(1, 2, tree_id, 1)};
        try f.getTree(2, tree_id).?.applyCompaction(&ch);
    }
    {
        const got = try f.get(alloc, .{ .namespace = 2, .table_id = 100, .primary = "row", .timestamp = std.math.maxInt(u64) }, .p0);
        try std.testing.expect(got != null);
        defer got.?.free(alloc);
        try std.testing.expectEqualStrings("v", got.?.value);
    }

    // Drop the table: one tree in the table → one delete change.
    const dc = try f.buildDropTable(alloc, 2, 100);
    defer alloc.free(dc.tree_changes);
    try std.testing.expectEqual(@as(usize, 1), dc.tree_changes.len);
    try f.applyDropTree(dc);

    // Data is gone, the tree is gone, the table is removed.
    try std.testing.expect((try f.get(alloc, .{ .namespace = 2, .table_id = 100, .primary = "row", .timestamp = std.math.maxInt(u64) }, .p0)) == null);
    try std.testing.expect(f.getTree(2, tree_id) == null);
    try std.testing.expect(f.tables.get(.{ .namespace = 2, .table_id = 100 }) == null);
}

test "forest: drop_ns removes every table and forgets the id counter" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f = try Forest.open(alloc, .{ .dir = dir_path });
    defer f.close();

    // Namespace 7 with three tables: the default (installed by ns_create),
    // a system tree at a reserved table_id, and an allocated user table.
    const nc = try f.buildNamespaceCreate(alloc, 7, "seven");
    defer alloc.free(nc.tree_changes);
    try f.applyNamespaceCreate(nc);
    try std.testing.expect(f.hasNamespace(7));
    try std.testing.expectEqual(@as(?u64, 7), f.lookupNamespace("seven"));

    const sys = try f.buildCreateTree(alloc, 7, 2);
    defer alloc.free(sys.tree_changes);
    try f.applyCreateTree(sys);

    const ct = try f.buildCreateTable(alloc, 7);
    defer alloc.free(ct.change.tree_changes);
    try f.applyCreateTree(ct.change);
    try std.testing.expectEqual(manifest.first_table_id, ct.table_id);

    // Drop the namespace: one delete per tree across all three tables.
    const dc = try f.buildDropNamespace(alloc, 7);
    defer alloc.free(dc.tree_changes);
    try std.testing.expectEqual(@as(usize, 3), dc.tree_changes.len);
    try std.testing.expectEqual(@as(u32, 8), dc.cmd);
    try f.applyDropNamespace(dc);

    try std.testing.expect(!f.hasNamespace(7));
    try std.testing.expect(!f.hasTable(7, 2));
    // The name died with the namespace and is free for a re-CREATE.
    try std.testing.expect(f.lookupNamespace("seven") == null);
    try std.testing.expect(f.tables.get(.{ .namespace = 7, .table_id = ct.table_id }) == null);

    // Idempotent: a rebuild on the dropped namespace is empty and re-applies
    // cleanly.
    const dc2 = try f.buildDropNamespace(alloc, 7);
    defer alloc.free(dc2.tree_changes);
    try std.testing.expectEqual(@as(usize, 0), dc2.tree_changes.len);
    try f.applyDropNamespace(dc2);

    // Re-creating the namespace works, and its table ids restart at the
    // floor — the counter was forgotten with the namespace. (Production
    // never reuses a namespace id; the same-id re-create here just
    // exercises idempotence.)
    const nc2 = try f.buildNamespaceCreate(alloc, 7, "seven");
    defer alloc.free(nc2.tree_changes);
    try f.applyNamespaceCreate(nc2);
    const ct2 = try f.buildCreateTable(alloc, 7);
    defer alloc.free(ct2.change.tree_changes);
    try std.testing.expectEqual(manifest.first_table_id, ct2.table_id);
    try std.testing.expectEqual(@as(?u64, 7), f.lookupNamespace("seven"));
}

test "forest: drop_tree keeps an SST shared with another tree" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f = try Forest.open(alloc, .{ .dir = dir_path });
    defer f.close();

    // Two tables (tables 100 and 101) that share one SST file (id 1) — the
    // shape a cheap CLONE produces: the same *Table referenced by both trees.
    const mcA = try f.buildCreateTree(alloc, 2, 100);
    defer alloc.free(mcA.tree_changes);
    try f.applyCreateTree(mcA);
    const treeA = mcA.tree_changes[0].tree_id;

    const mcB = try f.buildCreateTree(alloc, 2, 101);
    defer alloc.free(mcB.tree_changes);
    try f.applyCreateTree(mcB);
    const treeB = mcB.tree_changes[0].tree_id;

    const path = try zdb.file_ids.sstPath(alloc, dir_path, 1);
    defer alloc.free(path);
    try buildAndWriteTableKV(alloc, path, .{ .namespace = 2, .table_id = 100, .primary = "row", .timestamp = 1 }, "shared");
    {
        const cha = [_]manifest.SSTableChange{manifest.createSSTableChange(1, 2, treeA, 1)};
        const chb = [_]manifest.SSTableChange{manifest.createSSTableChange(1, 2, treeB, 1)};
        try f.getTree(2, treeA).?.applyCompaction(&cha);
        try f.getTree(2, treeB).?.applyCompaction(&chb);
    }
    try std.testing.expect(f.table_cache.contains(1));

    // Drop table A. The shared SST must survive — table B still references it,
    // so the refcount stays > 0 and the *Table is neither freed nor evicted.
    const dc = try f.buildDropTable(alloc, 2, 100);
    defer alloc.free(dc.tree_changes);
    try f.applyDropTree(dc);

    try std.testing.expect(f.getTree(2, treeA) == null);
    try std.testing.expect(f.getTree(2, treeB) != null);
    try std.testing.expect(f.table_cache.contains(1));
    try std.testing.expectEqual(@as(usize, 1), f.getTree(2, treeB).?.levels[1].numTables());
}

// ---------------------------------------------------------------------------
// 6d. buildCreateTable allocates table_ids; DROP never reuses one
// ---------------------------------------------------------------------------

test "forest: buildCreateTable allocates and never reuses a dropped id" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f = try Forest.open(alloc, .{ .dir = dir_path });
    defer f.close();

    const first = manifest.first_table_id;

    // First user table in namespace 2 gets first_table_id; second gets +1.
    const r1 = try f.buildCreateTable(alloc, 2);
    defer alloc.free(r1.change.tree_changes);
    try std.testing.expectEqual(first, r1.table_id);
    try f.applyCreateTree(r1.change);
    try std.testing.expect(f.hasTable(2, first));

    const r2 = try f.buildCreateTable(alloc, 2);
    defer alloc.free(r2.change.tree_changes);
    try std.testing.expectEqual(first + 1, r2.table_id);
    try f.applyCreateTree(r2.change);

    // Drop the first table — its id must NOT be handed out again.
    const dc = try f.buildDropTable(alloc, 2, r1.table_id);
    defer alloc.free(dc.tree_changes);
    try f.applyDropTree(dc);
    try std.testing.expect(!f.hasTable(2, r1.table_id));

    const r3 = try f.buildCreateTable(alloc, 2);
    defer alloc.free(r3.change.tree_changes);
    try std.testing.expectEqual(first + 2, r3.table_id);
}

test "forest: buildCreateNamespace allocates global namespace ids" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f = try Forest.open(alloc, .{ .dir = dir_path });
    defer f.close();

    const first = manifest.first_namespace;

    // CREATE DATABASE allocates the next namespace and installs its default table.
    const r1 = try f.buildCreateNamespace(alloc, "one");
    defer alloc.free(r1.change.tree_changes);
    try std.testing.expectEqual(first, r1.namespace);
    try f.applyNamespaceCreate(r1.change);
    try std.testing.expect(f.hasNamespace(first));

    const r2 = try f.buildCreateNamespace(alloc, "two");
    defer alloc.free(r2.change.tree_changes);
    try std.testing.expectEqual(first + 1, r2.namespace);
    try f.applyNamespaceCreate(r2.change);
    try std.testing.expect(f.hasNamespace(first + 1));

    // Names resolve to their namespaces; a taken name is rejected at build.
    try std.testing.expectEqual(@as(?u64, first), f.lookupNamespace("one"));
    try std.testing.expectEqual(@as(?u64, first + 1), f.lookupNamespace("two"));
    try std.testing.expectError(error.NameExists, f.buildCreateNamespace(alloc, "one"));
}

// ---------------------------------------------------------------------------
// 7. Snapshot manifest round-trip — recovery path
// ---------------------------------------------------------------------------
//
// Build a forest with two trees (ns=42 with key range, ns=43 covering all)
// plus an SST file at L1 of the first tree. Serialize the manifest, install
// it into a fresh forest pointing at the same on-disk dir, and assert: tree
// set, namespace routes, level membership, and ID counters are all restored.

test "forest: snapshot manifest round-trip" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // Phase 1: build source forest with content + serialize manifest.
    const ak = try (Key{ .namespace = 42, .primary = "a" }).marshal(alloc);
    defer alloc.free(ak);
    const mk = try (Key{ .namespace = 42, .primary = "m" }).marshal(alloc);
    defer alloc.free(mk);

    const bytes = blk: {
        const src = try Forest.open(alloc, .{ .dir = dir_path });
        defer src.close();

        // Tree in ns=42, range [a, m). Tree in ns=43, infinite. Both are the
        // default table (table_id 0) of their namespace → tree_id 0, distinct
        // because the namespace is part of the tree's global identity.
        _ = try src.createTree(42, 0, .{ .left = Key.init(ak), .right = Key.init(mk) });
        _ = try src.createTree(43, 0, .{ .inf = true });
        try src.ms.registerNamespaceName(42, "answers");

        // Drop one SST into the ns=42 tree's L1, both on disk and in the manifest.
        const sst_path = try zdb.file_ids.sstPath(alloc, dir_path, 1);
        defer alloc.free(sst_path);
        try buildAndWriteTable(alloc, sst_path, "ccc");

        const tree1 = src.getTree(42, 0).?;
        const changes = [_]manifest.SSTableChange{manifest.createSSTableChange(1, 42, 0, 1)};
        try tree1.applyCompaction(&changes);

        break :blk try src.ms.serialize(alloc);
    };
    defer alloc.free(bytes);

    // Phase 2: fresh forest, install snapshot, verify.
    const dst = try Forest.open(alloc, .{ .dir = dir_path });
    defer dst.close();

    try dst.ms.restoreFrom(bytes);
    try dst.openTablesFromManifest();

    // Both trees present — same namespace-local tree_id 0, distinct namespaces.
    try std.testing.expectEqual(@as(usize, 2), treeCount(dst));
    try std.testing.expect(dst.getTree(42, 0) != null);
    // Database names ride the snapshot.
    try std.testing.expectEqual(@as(?u64, 42), dst.lookupNamespace("answers"));
    try std.testing.expect(dst.getTree(43, 0) != null);

    // Namespace routes wired.
    try std.testing.expect(dst.tables.contains(.{ .namespace = 42, .table_id = 0 }));
    try std.testing.expect(dst.tables.contains(.{ .namespace = 43, .table_id = 0 }));
    try std.testing.expectEqual(@as(u64, 0), dst.tables.get(.{ .namespace = 42, .table_id = 0 }).?.lookup(.{ .namespace = 42, .primary = "ccc" }).?.tree_id);
    try std.testing.expectEqual(@as(u64, 0), dst.tables.get(.{ .namespace = 43, .table_id = 0 }).?.lookup(.{ .namespace = 43, .primary = "any" }).?.tree_id);

    // The ns=42 tree's L1 contains the SST.
    try std.testing.expectEqual(@as(usize, 1), dst.getTree(42, 0).?.levels[1].numTables());

    // No tree-id counter to recover: ids are derived from (table_id, split_id).
    // The file_id counter is restored from the per-node HardState lease
    // (`Forest.file_ids`), independently of the manifest.
}

// ---------------------------------------------------------------------------
// 8. Memtables.popOldestIfMatches — recovery-time pop semantics
// ---------------------------------------------------------------------------
//
// Helper that builds a Memtables with a chain of immutables of the given ids.
// The current memtable is the placeholder (id=0); each `swapCurrentOut(id_i)`
// freezes the prior current and makes a new current with id_i. After N swaps,
// immutables = [id_1, id_2, ..., id_{N-1}], current = id_N.

fn buildMemtables(alloc: std.mem.Allocator, ids: []const u64) !forest_mod.Memtables {
    const initial = try zdb.skl.SkipList.init(alloc);
    var mts = forest_mod.Memtables.init(alloc, initial);
    errdefer mts.deinit();
    for (ids) |id| try mts.swapCurrentOut(id, 0);
    return mts;
}

test "forest: popOldestIfMatches empty" {
    const alloc = std.testing.allocator;
    var mts = try buildMemtables(alloc, &.{}); // current=placeholder, immutables=[]
    defer mts.deinit();
    mts.popOldestIfMatches(7); // no-op
    try std.testing.expectEqual(@as(usize, 0), mts.immutables.items.len);
}

test "forest: popOldestIfMatches exact" {
    const alloc = std.testing.allocator;
    // immutables=[5], current=8.
    var mts = try buildMemtables(alloc, &.{ 5, 8 });
    defer mts.deinit();
    mts.popOldestIfMatches(5);
    try std.testing.expectEqual(@as(usize, 0), mts.immutables.items.len);
    try std.testing.expectEqual(@as(u64, 8), mts.current.id);
}

test "forest: popOldestIfMatches greater" {
    const alloc = std.testing.allocator;
    // immutables=[7], current=10. Flush proposal expects 5 — older than oldest.
    var mts = try buildMemtables(alloc, &.{ 7, 10 });
    defer mts.deinit();
    mts.popOldestIfMatches(5); // skip — oldest > expected
    try std.testing.expectEqual(@as(usize, 1), mts.immutables.items.len);
    try std.testing.expectEqual(@as(u64, 7), mts.immutables.items[0].id);
}

test "forest: popOldestIfMatches phantom" {
    const alloc = std.testing.allocator;
    // immutables=[3], current=10. Flush proposal expects 7. M_3 is a phantom.
    var mts = try buildMemtables(alloc, &.{ 3, 10 });
    defer mts.deinit();
    mts.popOldestIfMatches(7); // drops phantom M_3, no match for 7 → done
    try std.testing.expectEqual(@as(usize, 0), mts.immutables.items.len);
}

test "forest: popOldestIfMatches multi phantom then match" {
    const alloc = std.testing.allocator;
    // immutables=[2, 4, 7], current=10. Flush proposal expects 7.
    // Loop drops M_2 and M_4 as phantoms, then pops M_7.
    var mts = try buildMemtables(alloc, &.{ 2, 4, 7, 10 });
    defer mts.deinit();
    mts.popOldestIfMatches(7);
    try std.testing.expectEqual(@as(usize, 0), mts.immutables.items.len);
    try std.testing.expectEqual(@as(u64, 10), mts.current.id);
}

test "forest: popOldestIfMatches multi phantom no match" {
    const alloc = std.testing.allocator;
    // immutables=[2, 4], current=10. Flush proposal expects 7. Both phantoms;
    // no match — immutables drained, current untouched.
    var mts = try buildMemtables(alloc, &.{ 2, 4, 10 });
    defer mts.deinit();
    mts.popOldestIfMatches(7);
    try std.testing.expectEqual(@as(usize, 0), mts.immutables.items.len);
    try std.testing.expectEqual(@as(u64, 10), mts.current.id);
}

test "forest: getIterators outlives a concurrent flush+pop" {
    // The race we're guarding against: a scan calls
    // `Memtables.getIterators` and then drops the lock; before the
    // scan is done iterating, a flush completes and `popOldestIfMatches`
    // calls `release()` on the immutable memtable. If the iterator
    // hadn't already bumped the refcount, the SkipList's arena would
    // be freed mid-iteration and the next .next() would UAF.
    const alloc = std.testing.allocator;

    // Build immutables=[5], current=10, then put a key into M_5 so we
    // can read it after the pop.
    var mts = try buildMemtables(alloc, &.{ 5, 10 });
    defer mts.deinit();
    try std.testing.expectEqual(@as(usize, 1), mts.immutables.items.len);
    const m5 = mts.immutables.items[0];
    try m5.put(.{ .namespace = 1, .primary = "alpha", .timestamp = 100 }, "v1");

    // Snapshot under the lock — each skl.Iterator.init bumps
    // refcount before getIterators returns.
    const iters = try mts.getIterators(alloc);

    // Simulate flush completion: drop M_5 from immutables (release fires
    // inside popOldestIfMatches). M_5's refcount goes from 2 → 1
    // because the snapshot iterator still holds a ref.
    mts.popOldestIfMatches(5);
    try std.testing.expectEqual(@as(usize, 0), mts.immutables.items.len);

    // Iterate AFTER the pop; the snapshotted iterator for M_5 should
    // still see the key. Use ts=maxInt because same-primary keys sort
    // newest-first (timestamp is reverse-ordered) — seeking with ts=0
    // would land on the *next* primary, missing alpha.
    var found: ?[]const u8 = null;
    for (iters) |*it| {
        var copy = it.*;
        try copy.seekRange(.{
            .namespace = 1,
            .primary = "alpha",
            .timestamp = std.math.maxInt(u64),
        }, .{});
        if (copy.itemRef()) |item| {
            if (std.mem.eql(u8, item.key.primary, "alpha")) {
                found = item.value;
            }
        }
    }
    try std.testing.expect(found != null);
    try std.testing.expectEqualStrings("v1", found.?);

    // Final release: deinit each iterator (mirroring what
    // MergeIterator.deinit would do), then free the slice. Without
    // this, M_5's last ref leaks and the arena is never freed.
    for (iters) |*it| it.deinit();
    alloc.free(iters);
}

test "forest: routes to correct tree" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f = try Forest.open(alloc, .{ .dir = dir_path });
    defer f.close();

    // Create two trees in different namespaces.
    _ = try f.createTree(1, 0, .{ .inf = true });
    _ = try f.createTree(2, 0, .{ .inf = true });

    // Both namespaces should have a route table.
    {
        f.mu.lockShared(@src());
        defer f.mu.unlockShared();
        try std.testing.expect(f.tables.contains(.{ .namespace = 1, .table_id = 0 }));
        try std.testing.expect(f.tables.contains(.{ .namespace = 2, .table_id = 0 }));
    }
}
