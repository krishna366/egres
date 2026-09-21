const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;
const zdb = @import("zdb");
const flags = @import("flags");
const vfs = zdb.vfs;

const Key = zdb.kv.Key;
const _table = zdb.table;
const Builder = _table.builder.Builder;
const Table = _table.Table;

const _manifest = zdb.manifest;
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

const _forest = zdb.forest;
const Forest = _forest.Forest;
const Config = zdb.config.Config;
const TableTrees = _forest.TableTrees;

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
    const held = trees.mu.hold(.exclusive, @src());
    defer held.release();
    try trees.addTree(held, &t10);
    try std.testing.expectEqual(@as(u64, 10), trees.lookup(held.shared(), .{ .namespace = 1, .primary = "anything" }).?.tree_id);
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
    const held = trees.mu.hold(.exclusive, @src());
    defer held.release();
    try trees.addTree(held, &t1);
    try trees.addTree(held, &t2);

    // "c" falls in tree 1.
    try std.testing.expectEqual(@as(u64, 1), trees.lookup(held.shared(), .{ .namespace = 1, .primary = "c" }).?.tree_id);
    // "n" falls in tree 2.
    try std.testing.expectEqual(@as(u64, 2), trees.lookup(held.shared(), .{ .namespace = 1, .primary = "n" }).?.tree_id);
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

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
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

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();

    // Create a tree for namespace 1's default table (table_id 0). A fresh
    // table starts at split 0, so the derived tree_id is 0.
    const tree_id = try f.createTree(1, 0, .{ .inf = true });
    try std.testing.expectEqual(@as(u64, 0), tree_id);

    // Verify the tree exists.
    const tree = f.getTree(1, tree_id) orelse return error.TestExpectedTree;
    defer tree.release();
    try std.testing.expectEqual(tree_id, tree.tree_id);
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
fn buildAndWriteTable(alloc: std.mem.Allocator, path: String, primary: String) !void {
    var bld = Builder.init(alloc, .{ .target_block_size = 4 * 1024 });
    defer bld.deinit();
    try bld.add(.{ .primary = primary }, 1, "v");
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
    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });
    defer f_live.destroy();
    const f = try Forest.open(alloc, f_live);
    defer f.close();

    // Default table (table_id 0) → derived tree_id 0.
    const tree_id = try f.createTree(42, 0, .{ .inf = true });
    try std.testing.expectEqual(@as(u64, 0), tree_id);

    const table_keys = [_]String{ "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10" };
    for (table_keys, 1..) |key, tbl_id| {
        const path = try zdb.sstPath(alloc, dir_path, tbl_id);
        defer alloc.free(path);
        try buildAndWriteTable(alloc, path, key);
    }

    {
        const tree_ptr = f.getTree(42, tree_id).?;
        defer tree_ptr.release();
        var changes: [10]_manifest.SSTableChange = undefined;
        for (&changes, 1..) |*c, tbl_id| c.* = _manifest.createSSTableChange(@intCast(tbl_id), 42, tree_id, 3); // L3
        try tree_ptr.applyCompaction(1, &changes);
        try std.testing.expectEqual(@as(usize, 10), tree_ptr.numTablesAt(3));
    }

    try f.splitTreeForTests(42, tree_id);

    // Now 2 trees should exist.
    try std.testing.expectEqual(@as(usize, 2), treeCount(f));

    // Best balance with 10 equal-size L3 tables: split at items[5].smallest
    // = "k06" (left=5 tables, right=5 tables, |L−R| = 0). Old tree keeps
    // [k01..k05]; new tree gets [k06..k10]. The split's new half is the
    // table's next split index → tree_id 1; the parent stays tree_id 0.
    const old_tree = f.getTree(42, 0).?;
    defer old_tree.release();
    const new_tree = f.getTree(42, 1).?;
    defer new_tree.release();
    try std.testing.expectEqual(@as(usize, 5), old_tree.numTablesAt(3));
    try std.testing.expectEqual(@as(usize, 5), new_tree.numTablesAt(3));

    // Key ranges: old right = "k06", new left = "k06".
    try std.testing.expectEqualStrings("k06", old_tree.key_range.right.primary);
    try std.testing.expectEqualStrings("k06", new_tree.key_range.left.primary);

    // Route table for namespace 42, table 0, should have 2 entries.
    const trees = f.tables.get(.{ .namespace = 42, .table_id = 0 }).?;
    try std.testing.expectEqual(@as(usize, 2), trees.entries.items.len);

    // Routing: "k01" → old tree (0), "k10" → new tree (1).
    const held = trees.mu.hold(.shared, @src());
    defer held.release();
    try std.testing.expectEqual(@as(u64, 0), trees.lookup(held, .{ .primary = "k01" }).?.tree_id);
    try std.testing.expectEqual(@as(u64, 1), trees.lookup(held, .{ .primary = "k10" }).?.tree_id);
}

// ---------------------------------------------------------------------------
// 6b. Two-level routing — per-table_id table isolation
// ---------------------------------------------------------------------------

// Build a one-key SST carrying the full (namespace, table_id, primary) key so
// the stored key is table-specific, and write it to disk.
fn buildAndWriteTableKV(
    alloc: std.mem.Allocator,
    path: String,
    key: Key,
    ts: u64,
    value: String,
) !void {
    var bld = Builder.init(alloc, .{ .target_block_size = 4 * 1024 });
    defer bld.deinit();
    try bld.add(key, ts, value);
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

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
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
    const Case = struct { tid: u32, tree: u64, val: String };
    const cases = [_]Case{
        .{ .tid = 0, .tree = t0, .val = "v0" },
        .{ .tid = 1, .tree = t1, .val = "v1" },
        .{ .tid = 2, .tree = t2, .val = "v2" },
    };
    for (cases, 1..) |b, file_id| {
        const path = try zdb.sstPath(alloc, dir_path, file_id);
        defer alloc.free(path);
        try buildAndWriteTableKV(alloc, path, .{
            .namespace = 2,
            .table_id = b.tid,
            .primary = "row",
        }, 1, b.val);
        const changes = [_]_manifest.SSTableChange{_manifest.createSSTableChange(@intCast(file_id), 2, b.tree, 1)};
        {
            const t = f.getTree(2, b.tree).?;
            defer t.release();
            try t.applyCompaction(1, &changes);
        }
    }

    // A get routes to the tree for its (namespace, table_id) table, so each
    // table_id returns only its own value despite sharing the primary key.
    for (cases) |b| {
        const got = try f.get(alloc, .{
            .key = .{ .namespace = 2, .table_id = b.tid, .primary = "row" },
        });
        try std.testing.expect(got != null);
        defer got.?.free(alloc);
        try std.testing.expectEqualStrings(b.val, got.?.newest().value);
    }

    // Unrouted table (table_id 3) and an absent primary both miss.
    try std.testing.expect((try f.get(alloc, .{
        .key = .{ .namespace = 2, .table_id = 3, .primary = "row" },
    })) == null);
    try std.testing.expect((try f.get(alloc, .{
        .key = .{ .namespace = 2, .table_id = 1, .primary = "absent" },
    })) == null);
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

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();

    // Create a new table (ns=2, table_id=100) via the manifest op.
    const mc = try f.buildCreateTree(alloc, 2, 100);
    defer alloc.free(mc.tree_changes);
    try f.applyCreateTree(1, mc);
    const tree_id = mc.tree_changes[0].tree_id;
    try std.testing.expect(f.tables.get(.{ .namespace = 2, .table_id = 100 }) != null);

    // Install one SST and confirm the row reads back through routing.
    const path = try zdb.sstPath(alloc, dir_path, 1);
    defer alloc.free(path);
    try buildAndWriteTableKV(alloc, path, .{
        .namespace = 2,
        .table_id = 100,
        .primary = "row",
    }, 1, "v");
    {
        const ch = [_]_manifest.SSTableChange{_manifest.createSSTableChange(1, 2, tree_id, 1)};
        {
            const t = f.getTree(2, tree_id).?;
            defer t.release();
            try t.applyCompaction(1, &ch);
        }
    }
    {
        const got = try f.get(alloc, .{
            .key = .{ .namespace = 2, .table_id = 100, .primary = "row" },
        });
        try std.testing.expect(got != null);
        defer got.?.free(alloc);
        try std.testing.expectEqualStrings("v", got.?.newest().value);
    }

    // Drop the table: one tree in the table → one delete change.
    const dc = try f.buildDropTables(alloc, 2, &.{100});
    defer alloc.free(dc.tree_changes);
    try std.testing.expectEqual(@as(usize, 1), dc.tree_changes.len);
    try f.applyDropTree(1, dc);

    // Data is gone, the tree is gone, the table has no trees (its TableTrees
    // stays in the map, empty — ids are never reused).
    try std.testing.expect((try f.get(alloc, .{
        .key = .{ .namespace = 2, .table_id = 100, .primary = "row" },
    })) == null);
    try std.testing.expect(!f.hasTree(2, tree_id));
    try std.testing.expect(!f.hasTable(2, 100));
}

test "forest: drop_ns removes every table and forgets the id counter" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();

    // Namespace 7 with five tables: the default + column + index catalogs
    // (installed by ns_create), a system tree at a reserved table_id, and an
    // allocated, named user table.
    const nc = try f.buildNamespaceCreate(alloc, 7, "seven");
    defer alloc.free(nc.tree_changes);
    try f.applyNamespaceCreate(1, nc);
    try std.testing.expect(f.hasNamespace(7));
    try std.testing.expect(f.hasTable(7, _manifest.catalog_table_id));
    try std.testing.expect(f.hasTable(7, _manifest.index_catalog_table_id));
    try std.testing.expectEqual(@as(?u64, 7), f.manifest.lookupNamespace("seven"));

    const sys = try f.buildCreateTree(alloc, 7, 3);
    defer alloc.free(sys.tree_changes);
    try f.applyCreateTree(1, sys);

    const ct = try f.buildCreateTables(alloc, 7, &.{"users"});
    defer alloc.free(ct.table_ids);
    defer alloc.free(ct.change.tree_changes);
    try f.applyCreateTree(1, ct.change);
    try std.testing.expectEqual(_manifest.first_table_id, ct.table_ids[0]);
    try std.testing.expectEqual(@as(?u32, ct.table_ids[0]), f.manifest.lookupTable(7, "users"));

    // Drop the namespace: one delete per tree across all five tables.
    const dc = try f.buildDropNamespace(alloc, 7);
    defer alloc.free(dc.tree_changes);
    try std.testing.expectEqual(@as(usize, 5), dc.tree_changes.len);
    try std.testing.expectEqual(_manifest.Command.drop_ns, dc.cmd);
    try f.applyNamespaceDrop(1, dc);

    try std.testing.expect(!f.hasNamespace(7));
    try std.testing.expect(!f.hasTable(7, 3));
    try std.testing.expect(!f.hasTable(7, _manifest.catalog_table_id));
    // Names — database and table — died with the namespace and are free
    // for a re-CREATE.
    try std.testing.expect(f.manifest.lookupNamespace("seven") == null);
    try std.testing.expect(f.manifest.lookupTable(7, "users") == null);
    try std.testing.expect(!f.hasTable(7, ct.table_ids[0]));

    // Idempotent: a rebuild on the dropped namespace is empty and re-applies
    // cleanly.
    const dc2 = try f.buildDropNamespace(alloc, 7);
    defer alloc.free(dc2.tree_changes);
    try std.testing.expectEqual(@as(usize, 0), dc2.tree_changes.len);
    try f.applyNamespaceDrop(1, dc2);

    // Re-creating the namespace works, and its table ids restart at the
    // floor — the counter was forgotten with the namespace. (Production
    // never reuses a namespace id; the same-id re-create here just
    // exercises idempotence.)
    const nc2 = try f.buildNamespaceCreate(alloc, 7, "seven");
    defer alloc.free(nc2.tree_changes);
    try f.applyNamespaceCreate(1, nc2);
    const ct2 = try f.buildCreateTables(alloc, 7, &.{"users"});
    defer alloc.free(ct2.table_ids);
    defer alloc.free(ct2.change.tree_changes);
    try std.testing.expectEqual(_manifest.first_table_id, ct2.table_ids[0]);
    try std.testing.expectEqual(@as(?u64, 7), f.manifest.lookupNamespace("seven"));
}

test "forest: drop_tree keeps an SST shared with another tree" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();

    // Two tables (tables 100 and 101) that share one SST file (id 1) — the
    // shape a cheap CLONE produces: the same *Table referenced by both trees.
    const mcA = try f.buildCreateTree(alloc, 2, 100);
    defer alloc.free(mcA.tree_changes);
    try f.applyCreateTree(1, mcA);
    const treeA = mcA.tree_changes[0].tree_id;

    const mcB = try f.buildCreateTree(alloc, 2, 101);
    defer alloc.free(mcB.tree_changes);
    try f.applyCreateTree(1, mcB);
    const treeB = mcB.tree_changes[0].tree_id;

    const path = try zdb.sstPath(alloc, dir_path, 1);
    defer alloc.free(path);
    try buildAndWriteTableKV(alloc, path, .{
        .namespace = 2,
        .table_id = 100,
        .primary = "row",
    }, 1, "shared");
    {
        const cha = [_]_manifest.SSTableChange{_manifest.createSSTableChange(1, 2, treeA, 1)};
        const chb = [_]_manifest.SSTableChange{_manifest.createSSTableChange(1, 2, treeB, 1)};
        {
            const t = f.getTree(2, treeA).?;
            defer t.release();
            try t.applyCompaction(1, &cha);
        }
        {
            const t = f.getTree(2, treeB).?;
            defer t.release();
            try t.applyCompaction(1, &chb);
        }
    }
    try std.testing.expect(f.table_cache.contains(1));

    // Drop table A. The shared SST must survive — table B still references it,
    // so the refcount stays > 0 and the *Table is neither freed nor evicted.
    const dc = try f.buildDropTables(alloc, 2, &.{100});
    defer alloc.free(dc.tree_changes);
    try f.applyDropTree(1, dc);

    try std.testing.expect(!f.hasTree(2, treeA));
    try std.testing.expect(f.hasTree(2, treeB));
    try std.testing.expect(f.table_cache.contains(1));
    {
        const t = f.getTree(2, treeB).?;
        defer t.release();
        try std.testing.expectEqual(@as(usize, 1), t.numTablesAt(1));
    }
}

// ---------------------------------------------------------------------------
// 6d. buildCreateTables allocates table_ids; DROP never reuses one
// ---------------------------------------------------------------------------

test "forest: buildCreateTables allocates and never reuses a dropped id" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();

    const first = _manifest.first_table_id;

    // First user table in namespace 2 gets first_table_id; second gets +1.
    // (Unnamed — id allocation is what's under test here.)
    const r1 = try f.buildCreateTables(alloc, 2, &.{""});
    defer alloc.free(r1.table_ids);
    defer alloc.free(r1.change.tree_changes);
    try std.testing.expectEqual(first, r1.table_ids[0]);
    try f.applyCreateTree(1, r1.change);
    try std.testing.expect(f.hasTable(2, first));

    const r2 = try f.buildCreateTables(alloc, 2, &.{""});
    defer alloc.free(r2.table_ids);
    defer alloc.free(r2.change.tree_changes);
    try std.testing.expectEqual(first + 1, r2.table_ids[0]);
    try f.applyCreateTree(1, r2.change);

    // Drop the first table — its id must NOT be handed out again.
    const dc = try f.buildDropTables(alloc, 2, &.{r1.table_ids[0]});
    defer alloc.free(dc.tree_changes);
    try f.applyDropTree(1, dc);
    try std.testing.expect(!f.hasTable(2, r1.table_ids[0]));

    const r3 = try f.buildCreateTables(alloc, 2, &.{""});
    defer alloc.free(r3.table_ids);
    defer alloc.free(r3.change.tree_changes);
    try std.testing.expectEqual(first + 2, r3.table_ids[0]);
}

test "forest: buildCreateNamespace allocates global namespace ids" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();

    const first = _manifest.first_namespace;

    // CREATE DATABASE allocates the next namespace and installs its default table.
    const r1 = try f.buildCreateNamespace(alloc, "one");
    defer alloc.free(r1.change.tree_changes);
    try std.testing.expectEqual(first, r1.namespace);
    try f.applyNamespaceCreate(1, r1.change);
    try std.testing.expect(f.hasNamespace(first));

    const r2 = try f.buildCreateNamespace(alloc, "two");
    defer alloc.free(r2.change.tree_changes);
    try std.testing.expectEqual(first + 1, r2.namespace);
    try f.applyNamespaceCreate(1, r2.change);
    try std.testing.expect(f.hasNamespace(first + 1));

    // Names resolve to their namespaces; a taken name is rejected at build.
    try std.testing.expectEqual(@as(?u64, first), f.manifest.lookupNamespace("one"));
    try std.testing.expectEqual(@as(?u64, first + 1), f.manifest.lookupNamespace("two"));
    try std.testing.expectError(error.NameExists, f.buildCreateNamespace(alloc, "one"));
}

test "forest: ns_create installs default + catalog trees, idempotent re-apply" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();

    const nc = try f.buildNamespaceCreate(alloc, 9, "nine");
    defer alloc.free(nc.tree_changes);
    try std.testing.expectEqual(@as(usize, 3), nc.tree_changes.len);
    try f.applyNamespaceCreate(1, nc);
    try std.testing.expect(f.hasTable(9, 0));
    try std.testing.expect(f.hasTable(9, _manifest.catalog_table_id));
    try std.testing.expect(f.hasTable(9, _manifest.index_catalog_table_id));

    // Re-apply (WAL replay shape): all three trees already exist — no error,
    // no duplicates. Regression: the old per-namespace check skipped the
    // catalog trees whenever table 0 existed.
    try f.applyNamespaceCreate(1, nc);
    try std.testing.expect(f.hasTable(9, 0));
    try std.testing.expect(f.hasTable(9, _manifest.catalog_table_id));
    try std.testing.expect(f.hasTable(9, _manifest.index_catalog_table_id));
}

test "forest: table names — create registers, duplicate rejects, drop frees" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();

    const ct = try f.buildCreateTables(alloc, 3, &.{"users"});
    defer alloc.free(ct.table_ids);
    defer alloc.free(ct.change.tree_changes);
    try f.applyCreateTree(1, ct.change);
    try std.testing.expectEqual(@as(?u32, ct.table_ids[0]), f.manifest.lookupTable(3, "users"));

    // A taken name is rejected at build; re-applying the create is a no-op.
    try std.testing.expectError(error.NameExists, f.buildCreateTables(alloc, 3, &.{"users"}));
    try f.applyCreateTree(1, ct.change);
    try std.testing.expectEqual(@as(?u32, ct.table_ids[0]), f.manifest.lookupTable(3, "users"));

    // DROP frees the name for a re-CREATE, which gets a fresh id.
    const dc = try f.buildDropTables(alloc, 3, &.{ct.table_ids[0]});
    defer alloc.free(dc.tree_changes);
    try f.applyDropTree(1, dc);
    try std.testing.expect(f.manifest.lookupTable(3, "users") == null);

    const ct2 = try f.buildCreateTables(alloc, 3, &.{"users"});
    defer alloc.free(ct2.table_ids);
    defer alloc.free(ct2.change.tree_changes);
    try f.applyCreateTree(1, ct2.change);
    try std.testing.expect(ct2.table_ids[0] > ct.table_ids[0]);
    try std.testing.expectEqual(@as(?u32, ct2.table_ids[0]), f.manifest.lookupTable(3, "users"));
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
        const src_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });
        defer src_live.destroy();
        const src = try Forest.open(alloc, src_live);
        defer src.close();

        // Tree in ns=42, range [a, m). Tree in ns=43, infinite. Both are the
        // default table (table_id 0) of their namespace → tree_id 0, distinct
        // because the namespace is part of the tree's global identity.
        _ = try src.createTree(42, 0, .{ .left = Key.init(ak), .right = Key.init(mk) });
        _ = try src.createTree(43, 0, .{ .inf = true });
        try src.manifest.registerNamespaceName(42, "answers");

        // Drop one SST into the ns=42 tree's L1, both on disk and in the manifest.
        const sst_path = try zdb.sstPath(alloc, dir_path, 1);
        defer alloc.free(sst_path);
        try buildAndWriteTable(alloc, sst_path, "ccc");

        const tree1 = src.getTree(42, 0).?;
        defer tree1.release();
        const changes = [_]_manifest.SSTableChange{_manifest.createSSTableChange(1, 42, 0, 1)};
        try tree1.applyCompaction(1, &changes);

        break :blk try src.manifest.serialize(alloc);
    };
    defer alloc.free(bytes);

    // Phase 2: fresh forest, install snapshot, verify.
    const dst_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });
    defer dst_live.destroy();
    const dst = try Forest.open(alloc, dst_live);
    defer dst.close();

    try dst.manifest.restoreFrom(bytes);
    try dst.openTablesFromManifest();

    // Both trees present — same namespace-local tree_id 0, distinct namespaces.
    try std.testing.expectEqual(@as(usize, 2), treeCount(dst));
    try std.testing.expect(dst.hasTree(42, 0));
    // Database names ride the snapshot.
    try std.testing.expectEqual(@as(?u64, 42), dst.manifest.lookupNamespace("answers"));
    try std.testing.expect(dst.hasTree(43, 0));

    // Namespace routes wired.
    try std.testing.expect(dst.tables.contains(.{ .namespace = 42, .table_id = 0 }));
    try std.testing.expect(dst.tables.contains(.{ .namespace = 43, .table_id = 0 }));
    {
        const t42 = dst.tables.get(.{ .namespace = 42, .table_id = 0 }).?;
        const held = t42.mu.hold(.shared, @src());
        defer held.release();
        try std.testing.expectEqual(@as(u64, 0), t42.lookup(held, .{ .namespace = 42, .primary = "ccc" }).?.tree_id);
    }
    {
        const t43 = dst.tables.get(.{ .namespace = 43, .table_id = 0 }).?;
        const held = t43.mu.hold(.shared, @src());
        defer held.release();
        try std.testing.expectEqual(@as(u64, 0), t43.lookup(held, .{ .namespace = 43, .primary = "any" }).?.tree_id);
    }

    // The ns=42 tree's L1 contains the SST.
    {
        const t = dst.getTree(42, 0).?;
        defer t.release();
        try std.testing.expectEqual(@as(usize, 1), t.numTablesAt(1));
    }

    // No tree-id counter to recover: ids are derived from (table_id, split_id).
    // Nothing recovers file ids either — `Manifest.newFileId` draws them at
    // random.
}

// ---------------------------------------------------------------------------
// 8. Memtables.popOldestIfMatches — recovery-time pop semantics
// ---------------------------------------------------------------------------
//
// Helper that builds a Memtables with a chain of immutables of the given ids.
// The current memtable is the placeholder (id=0); each `swapCurrentOut(id_i)`
// freezes the prior current and makes a new current with id_i. After N swaps,
// immutables = [id_{N-1}, ..., id_2, id_1] (newest first), current = id_N.
// An id is the log index of its `new_memtable` entry, so the ids passed
// here are increasing, as they are in a real log.

fn buildMemtables(alloc: std.mem.Allocator, ids: []const u64) !_forest.Memtables {
    const initial = try zdb.skl.SkipList.init(alloc);
    var mts = _forest.Memtables.init(alloc, initial);
    errdefer mts.deinit();
    for (ids) |id| try mts.swapCurrentOut(id);
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
    try std.testing.expectEqual(@as(u64, 8), mts.current.raft_index);
}

test "forest: popOldestIfMatches greater" {
    const alloc = std.testing.allocator;
    // immutables=[7], current=10. Flush proposal expects 5 — older than oldest.
    var mts = try buildMemtables(alloc, &.{ 7, 10 });
    defer mts.deinit();
    mts.popOldestIfMatches(5); // skip — oldest > expected
    try std.testing.expectEqual(@as(usize, 1), mts.immutables.items.len);
    try std.testing.expectEqual(@as(u64, 7), mts.immutables.items[0].raft_index);
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
    // immutables=[7, 4, 2] (newest first), current=10. Flush proposal
    // expects 7. Loop drops M_2 and M_4 as phantoms, then pops M_7.
    var mts = try buildMemtables(alloc, &.{ 2, 4, 7, 10 });
    defer mts.deinit();
    mts.popOldestIfMatches(7);
    try std.testing.expectEqual(@as(usize, 0), mts.immutables.items.len);
    try std.testing.expectEqual(@as(u64, 10), mts.current.raft_index);
}

test "forest: popOldestIfMatches multi phantom no match" {
    const alloc = std.testing.allocator;
    // immutables=[4, 2] (newest first), current=10. Flush proposal
    // expects 7. Both phantoms; no match — immutables drained, current
    // untouched.
    var mts = try buildMemtables(alloc, &.{ 2, 4, 10 });
    defer mts.deinit();
    mts.popOldestIfMatches(7);
    try std.testing.expectEqual(@as(usize, 0), mts.immutables.items.len);
    try std.testing.expectEqual(@as(u64, 10), mts.current.raft_index);
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
    try m5.put(.{ .namespace = 1, .primary = "alpha" }, 100, "v1");

    // Snapshot under the lock — each skl.Iterator.init bumps
    // refcount before getIterators returns.
    const iters = try mts.getIterators(alloc, .{}, .{});

    // Simulate flush completion: drop M_5 from immutables (release fires
    // inside popOldestIfMatches). M_5's refcount goes from 2 → 1
    // because the snapshot iterator still holds a ref.
    mts.popOldestIfMatches(5);
    try std.testing.expectEqual(@as(usize, 0), mts.immutables.items.len);

    // Iterate AFTER the pop; the snapshotted iterator for M_5 should
    // still see the key. The seek key holds the primary alone, and the
    // item at that position carries every version of it.
    var found: ?String = null;
    for (iters) |*it| {
        var copy = it.*;
        try copy.seekRange(.{
            .namespace = 1,
            .primary = "alpha",
        }, .{});
        if (copy.itemRef()) |item| {
            if (std.mem.eql(u8, item.key.primary, "alpha")) {
                found = item.newest().value;
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

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();

    // Create two trees in different namespaces.
    _ = try f.createTree(1, 0, .{ .inf = true });
    _ = try f.createTree(2, 0, .{ .inf = true });

    // Both namespaces should have a route table.
    try std.testing.expect(f.tables.contains(.{ .namespace = 1, .table_id = 0 }));
    try std.testing.expect(f.tables.contains(.{ .namespace = 2, .table_id = 0 }));
}

// ---------------------------------------------------------------------------
// maxKey — largest row id in a table (drives SQL egid allocation)
// ---------------------------------------------------------------------------

fn beKey(egid: u64) [8]u8 {
    var b: [8]u8 = undefined;
    std.mem.writeInt(u64, &b, egid, .big);
    return b;
}

test "forest: maxKey finds the largest row id (memtable), or null when empty" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();

    const ns: u64 = 16;
    const tid: u32 = 16;
    _ = try f.createTree(ns, tid, .{ .inf = true });

    // No keys yet → null (tree exists but is empty).
    try std.testing.expect((try f.maxKey(alloc, ns, tid)) == null);
    // A table with no tree at all → null.
    try std.testing.expect((try f.maxKey(alloc, ns, 17)) == null);

    // Insert egids out of order; the max (big-endian key order == numeric) is 5.
    const k1 = beKey(1);
    const k5 = beKey(5);
    const k3 = beKey(3);
    try f.put(&.{
        .{
            .key = .{ .namespace = ns, .table_id = tid, .primary = &k1 },
            .versions = .{ .one = .{ .ts = 1, .meta = .{}, .value = "a" } },
        },
        .{
            .key = .{ .namespace = ns, .table_id = tid, .primary = &k5 },
            .versions = .{ .one = .{ .ts = 1, .meta = .{}, .value = "b" } },
        },
        .{
            .key = .{ .namespace = ns, .table_id = tid, .primary = &k3 },
            .versions = .{ .one = .{ .ts = 1, .meta = .{}, .value = "c" } },
        },
    });

    const mk = (try f.maxKey(alloc, ns, tid)).?;
    defer mk.free(alloc);
    try std.testing.expectEqual(@as(u64, 5), std.mem.readInt(u64, mk.primary[0..8], .big));
}

test "forest: maxKey counts a tombstoned max key" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();

    const ns: u64 = 16;
    const tid: u32 = 16;
    _ = try f.createTree(ns, tid, .{ .inf = true });

    // Seed egids 1/3/5, then tombstone the max (5) at a higher timestamp — as
    // a DELETE would. maxKey must still report 5: seeding next_egid from a
    // lower key would re-mint id 5 onto the deleted row.
    const k1 = beKey(1);
    const k3 = beKey(3);
    const k5 = beKey(5);
    try f.put(&.{
        .{
            .key = .{ .namespace = ns, .table_id = tid, .primary = &k1 },
            .versions = .{ .one = .{ .ts = 1, .meta = .{}, .value = "a" } },
        },
        .{
            .key = .{ .namespace = ns, .table_id = tid, .primary = &k3 },
            .versions = .{ .one = .{ .ts = 1, .meta = .{}, .value = "b" } },
        },
        .{
            .key = .{ .namespace = ns, .table_id = tid, .primary = &k5 },
            .versions = .{ .one = .{ .ts = 1, .meta = .{}, .value = "c" } },
        },
    });
    try f.put(&.{.{
        .key = .{ .namespace = ns, .table_id = tid, .primary = &k5 },
        .versions = .{ .one = .{
            .ts = 2,
            .meta = .{ .kind = .deleted },
            .value = "",
        } },
    }});

    const mk = (try f.maxKey(alloc, ns, tid)).?;
    defer mk.free(alloc);
    const got = std.mem.readInt(u64, mk.primary[0..8], .big);
    try std.testing.expectEqual(@as(u64, 5), got);
}

// ---------------------------------------------------------------------------
// Meta byte: survives the whole write path
// ---------------------------------------------------------------------------

test "forest: meta survives put, flush to L0, compaction, and get" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();

    _ = try f.createTree(1, 0, .{ .inf = true });

    // Forest.open leaves an id-0 placeholder memtable, and swapCurrentOut
    // releases rather than freezes that one — writes into it would be dropped
    // instead of flushed. Swap in a real memtable before writing.
    try f.newMemtable(1);

    const cases = [_]struct { primary: String, meta: zdb.kv.Meta }{
        .{ .primary = "a", .meta = .{ .kind = .deleted } },
        .{ .primary = "b", .meta = .{ .encoding = .roaring } },
        .{ .primary = "c", .meta = .{ .kind = .deleted, .encoding = .roaring } },
        .{ .primary = "d", .meta = .{} },
    };

    var items: [cases.len]zdb.kv.Item = undefined;
    for (cases, 0..) |c, i| {
        items[i] = .{
            .key = .{ .namespace = 1, .table_id = 0, .primary = c.primary },
            .versions = .{ .one = .{
                .ts = 1,
                .meta = c.meta,
                .value = "v",
            } },
        };
    }
    try f.put(&items);

    // getRaw, not get: this asserts meta survives the round trip, and
    // Forest.get deliberately reports a tombstoned key as absent (covered
    // separately by the tombstone tests below).
    for (cases) |c| {
        const got = try f.getRaw(alloc, .{
            .key = .{ .namespace = 1, .table_id = 0, .primary = c.primary },
        }) orelse return error.TestExpectedItem;
        defer got.free(alloc);
        try std.testing.expectEqual(c.meta, got.newest().meta);
    }

    // Freeze and flush to an L0 SST, so the next reads decode meta back out of
    // the on-disk entry encoding rather than the skiplist node.
    try f.newMemtable(2);
    var flush = (try f.buildFlush()) orelse return error.TestExpectedFlush;
    defer flush.changes.deinit(alloc);
    try f.applyFlush(3, flush.changes.items, flush.memtable_id);

    for (cases) |c| {
        const got = try f.getRaw(alloc, .{
            .key = .{ .namespace = 1, .table_id = 0, .primary = c.primary },
        }) orelse return error.TestExpectedItem;
        defer got.free(alloc);
        try std.testing.expectEqual(c.meta, got.newest().meta);
        try std.testing.expectEqualStrings("v", got.newest().value);
    }

    // Compact L0 → L1. Compaction rewrites every entry through a fresh
    // Builder, so meta has to survive the merge iterator and the re-encode.
    const tree = f.getTree(1, 0) orelse return error.TestExpectedTree;
    defer tree.release();
    var comp = try tree.buildCompaction(0, 0);
    defer comp.deinit(alloc);
    // Guard against the compaction being a silent no-op, which would make the
    // assertions below vacuous.
    try std.testing.expect(comp.items.len > 0);
    try tree.applyCompaction(1, comp.items);

    // Every row is still here: compaction preserves meta but never acts on it,
    // so `delete` must NOT have dropped anything (keys_in == keys_out).
    for (cases) |c| {
        const got = try f.getRaw(alloc, .{
            .key = .{ .namespace = 1, .table_id = 0, .primary = c.primary },
        }) orelse return error.TestExpectedItem;
        defer got.free(alloc);
        try std.testing.expectEqual(c.meta, got.newest().meta);
        try std.testing.expectEqualStrings("v", got.newest().value);
    }
}

// ---------------------------------------------------------------------------
// Tombstones + the read horizon
// ---------------------------------------------------------------------------

test "forest: a tombstone reads as absent" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    _ = try f.createTree(1, 0, .{ .inf = true });
    try f.newMemtable(1);

    // Write a value, then delete it at a later timestamp.
    try f.put(&[_]zdb.kv.Item{.{
        .key = .{ .namespace = 1, .table_id = 0, .primary = "k" },
        .versions = .{ .one = .{ .ts = 10, .meta = .{}, .value = "v" } },
    }});
    const live = try f.get(alloc, .{
        .key = .{ .namespace = 1, .table_id = 0, .primary = "k" },
    }) orelse return error.TestExpectedItem;
    live.free(alloc);

    try f.put(&[_]zdb.kv.Item{.{
        .key = .{ .namespace = 1, .table_id = 0, .primary = "k" },
        .versions = .{ .one = .{
            .ts = 20,
            .meta = .{ .kind = .deleted },
            .value = "",
        } },
    }});

    // Deleted as of now.
    try std.testing.expect((try f.get(alloc, .{
        .key = .{ .namespace = 1, .table_id = 0, .primary = "k" },
    })) == null);

    // The tombstone is still real data underneath — get just refuses to
    // surface it.
    const raw = try f.getRaw(alloc, .{
        .key = .{ .namespace = 1, .table_id = 0, .primary = "k" },
    }) orelse return error.TestExpectedItem;
    defer raw.free(alloc);
    try std.testing.expect(raw.newest().meta.isDeleted());

    // Reading before the delete still sees the value: the tombstone shadows
    // older versions, it doesn't erase them.
    const before = try f.get(alloc, .{
        .key = .{ .namespace = 1, .table_id = 0, .primary = "k" },
        .read_ts = 15,
    }) orelse return error.TestExpectedItem;
    defer before.free(alloc);
    try std.testing.expectEqualStrings("v", before.newest().value);
}

test "forest: globalMinRead rejects older reads and only moves forward" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    _ = try f.createTree(1, 0, .{ .inf = true });
    try f.newMemtable(1);

    // Default 0 means nothing is rejected and nothing is droppable.
    try std.testing.expectEqual(@as(u64, 0), f.globalMinRead());
    try std.testing.expect((try f.get(alloc, .{
        .key = .{ .namespace = 1, .table_id = 0, .primary = "k" },
        .read_ts = 1,
    })) == null);

    try std.testing.expectEqual(@as(u64, 100), f.setGlobalMinRead(100));

    // Below the horizon: the versions may already be compacted away, so the
    // read is refused rather than answered wrongly.
    try std.testing.expectError(error.ReadBelowMinRead, f.get(alloc, .{
        .key = .{ .namespace = 1, .table_id = 0, .primary = "k" },
        .read_ts = 99,
    }));

    // At the horizon is allowed.
    try std.testing.expect((try f.get(alloc, .{
        .key = .{ .namespace = 1, .table_id = 0, .primary = "k" },
        .read_ts = 100,
    })) == null);

    // Monotonic: a stale caller can't walk it back and un-discard data that
    // a compaction already dropped.
    try std.testing.expectEqual(@as(u64, 100), f.setGlobalMinRead(50));
    try std.testing.expectEqual(@as(u64, 100), f.globalMinRead());
    try std.testing.expectEqual(@as(u64, 150), f.setGlobalMinRead(150));
}

// ---------------------------------------------------------------------------
// Scan: version selection + tombstone visibility
// ---------------------------------------------------------------------------

/// Collects what a scan emits. Dupes each item: the callback's Item borrows
/// the merge iterator's buffers, which the next step overwrites.
const ScanCollector = struct {
    alloc: std.mem.Allocator,
    got: std.ArrayList(zdb.kv.Item) = .empty,

    fn emit(self: *ScanCollector, item: zdb.kv.Item) anyerror!bool {
        try self.got.append(self.alloc, try item.dupe(self.alloc));
        return true;
    }

    fn deinit(self: *ScanCollector) void {
        for (self.got.items) |it| it.free(self.alloc);
        self.got.deinit(self.alloc);
    }
};

/// A forest holding, in namespace 1:
///   a@20="a2", a@10="a1"          (two live versions)
///   b@20=tombstone, b@10="b1"     (deleted, one version behind it)
///   c@10="c1"                     (one live version)
fn seedVersionedForest(f: *Forest) !void {
    _ = try f.createTree(1, 0, .{ .inf = true });
    try f.newMemtable(1);

    const rows = [_]struct { p: String, ts: u64, v: String, m: zdb.kv.Meta }{
        .{ .p = "a", .ts = 10, .v = "a1", .m = .{} },
        .{ .p = "a", .ts = 20, .v = "a2", .m = .{} },
        .{ .p = "b", .ts = 10, .v = "b1", .m = .{} },
        .{ .p = "b", .ts = 20, .v = "", .m = .{ .kind = .deleted } },
        .{ .p = "c", .ts = 10, .v = "c1", .m = .{} },
    };
    for (rows) |r| {
        try f.put(&[_]zdb.kv.Item{.{
            .key = .{ .namespace = 1, .table_id = 0, .primary = r.p },
            .versions = .{ .one = .{ .ts = r.ts, .meta = r.m, .value = r.v } },
        }});
    }
}

fn runScan(
    alloc: std.mem.Allocator,
    f: *Forest,
    opts: Forest.ScanOptions,
    col: *ScanCollector,
) !u64 {
    const open = Key{ .namespace = 1, .table_id = 0, .primary = "" };
    return f.scan(alloc, .{ .left = open, .right = open }, opts, col, ScanCollector.emit);
}

test "scan: by default emits one version per key and hides deleted keys" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    try seedVersionedForest(f);

    var col = ScanCollector{ .alloc = alloc };
    defer col.deinit();
    const n = try runScan(alloc, f, .{}, &col);

    // a's newest, c's only. b is deleted and vanishes entirely — its
    // tombstone is not emitted, and neither is b@10 behind it.
    try std.testing.expectEqual(@as(u64, 2), n);
    try std.testing.expectEqualStrings("a", col.got.items[0].key.primary);
    try std.testing.expectEqualStrings("a2", col.got.items[0].newest().value);
    try std.testing.expectEqualStrings("c", col.got.items[1].key.primary);
    try std.testing.expectEqualStrings("c1", col.got.items[1].newest().value);
}

test "scan: num_versions returns extra versions but still stops at a tombstone" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    try seedVersionedForest(f);

    var col = ScanCollector{ .alloc = alloc };
    defer col.deinit();
    const n = try runScan(alloc, f, .{ .num_versions = 10 }, &col);

    // One item per key: a with a@20 and a@10, c with c@10. b is still
    // hidden: without include_deleted, a tombstone ends the key
    // regardless of how many versions were asked for.
    try std.testing.expectEqual(@as(u64, 2), n);
    const a = col.got.items[0];
    const a_vers = a.versions.slice();
    try std.testing.expectEqual(@as(usize, 2), a_vers.len);
    try std.testing.expectEqualStrings("a2", a_vers[0].value);
    try std.testing.expectEqual(@as(u64, 20), a_vers[0].ts);
    try std.testing.expectEqualStrings("a1", a_vers[1].value);
    try std.testing.expectEqual(@as(u64, 10), a_vers[1].ts);
    const c = col.got.items[1];
    try std.testing.expectEqual(@as(usize, 1), c.versions.len());
    try std.testing.expectEqualStrings("c1", c.newest().value);
}

test "scan: include_deleted surfaces tombstones and the versions behind them" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    try seedVersionedForest(f);

    var col = ScanCollector{ .alloc = alloc };
    defer col.deinit();
    const n = try runScan(alloc, f, .{ .num_versions = 10, .include_deleted = true }, &col);

    // Everything, one item per key: a with a@20 and a@10, b with its
    // tombstone at 20 and b@10 behind it, c with c@10.
    try std.testing.expectEqual(@as(u64, 3), n);
    try std.testing.expectEqual(
        @as(usize, 2),
        col.got.items[0].versions.len(),
    );
    const b = col.got.items[1];
    const b_vers = b.versions.slice();
    try std.testing.expectEqualStrings("b", b.key.primary);
    try std.testing.expectEqual(@as(usize, 2), b_vers.len);
    try std.testing.expect(b_vers[0].meta.isDeleted());
    try std.testing.expectEqual(@as(u64, 20), b_vers[0].ts);
    try std.testing.expectEqualStrings("b1", b_vers[1].value);
    try std.testing.expectEqualStrings("c1", col.got.items[2].newest().value);
}

test "scan: num_versions caps deleted runs too" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    try seedVersionedForest(f);

    var col = ScanCollector{ .alloc = alloc };
    defer col.deinit();
    const n = try runScan(alloc, f, .{ .num_versions = 1, .include_deleted = true }, &col);

    // One version each: a@20, b@20 (the tombstone), c@10. b@10 is cut by the
    // version cap, not by the tombstone rule.
    try std.testing.expectEqual(@as(u64, 3), n);
    try std.testing.expectEqualStrings("a2", col.got.items[0].newest().value);
    try std.testing.expect(col.got.items[1].newest().meta.isDeleted());
    try std.testing.expectEqualStrings("c1", col.got.items[2].newest().value);
}

test "scan: num_versions 0 behaves as 1 rather than returning nothing" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    try seedVersionedForest(f);

    var col = ScanCollector{ .alloc = alloc };
    defer col.deinit();
    const n = try runScan(alloc, f, .{ .num_versions = 0 }, &col);
    try std.testing.expectEqual(@as(u64, 2), n);
}

test "scan: max_rows bounds emitted items, not versions walked" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    try seedVersionedForest(f);

    var col = ScanCollector{ .alloc = alloc };
    defer col.deinit();
    // Default opts skip a@10, both b versions. max_rows=2 must still yield
    // the 2 keys a scan would emit, not get consumed by the skipped rows.
    const n = try runScan(alloc, f, .{ .max_rows = 2 }, &col);
    try std.testing.expectEqual(@as(u64, 2), n);
    try std.testing.expectEqualStrings("a2", col.got.items[0].newest().value);
    try std.testing.expectEqualStrings("c1", col.got.items[1].newest().value);
}

test "scan: read_ts reports the world as of that timestamp" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    try seedVersionedForest(f);

    var col = ScanCollector{ .alloc = alloc };
    defer col.deinit();
    // At ts=15 only the @10 writes have happened.
    const n = try runScan(alloc, f, .{ .read_ts = 15 }, &col);

    // a@20 hasn't been written yet, so a reads as "a1".
    //
    // And b: its tombstone is at @20, which is ALSO in the future here — so
    // b is not deleted as of 15 and must come back live. This is the case
    // that breaks if the tombstone rule runs before the read_ts filter.
    try std.testing.expectEqual(@as(u64, 3), n);
    try std.testing.expectEqualStrings("a1", col.got.items[0].newest().value);
    try std.testing.expectEqualStrings("b", col.got.items[1].key.primary);
    try std.testing.expectEqualStrings("b1", col.got.items[1].newest().value);
    try std.testing.expect(!col.got.items[1].newest().meta.isDeleted());
    try std.testing.expectEqualStrings("c1", col.got.items[2].newest().value);

    // The same key at the same instant via get must agree.
    const g = try f.get(alloc, .{
        .key = .{ .namespace = 1, .table_id = 0, .primary = "b" },
        .read_ts = 15,
    }) orelse return error.TestExpectedItem;
    defer g.free(alloc);
    try std.testing.expectEqualStrings("b1", g.newest().value);
}

test "scan: min_ts emits only what changed after it" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    _ = try f.createTree(1, 0, .{ .inf = true });
    try f.newMemtable(1);

    // Three SST generations plus a memtable: the base at @10 (an SST
    // whose max_version is 10, below every min_ts used here), the
    // changes at @20 (a rewritten, b deleted, d inserted), and e at
    // @30 still in the memtable.
    const Put = struct {
        p: String,
        ts: u64,
        v: String,
        m: zdb.kv.Meta,
    };
    const put = struct {
        fn one(forest: *Forest, r: Put) !void {
            try forest.put(&[_]zdb.kv.Item{.{
                .key = .{ .namespace = 1, .table_id = 0, .primary = r.p },
                .versions = .{ .one = .{
                    .ts = r.ts,
                    .meta = r.m,
                    .value = r.v,
                } },
            }});
        }
    }.one;
    try put(f, .{ .p = "a", .ts = 10, .v = "a1", .m = .{} });
    try put(f, .{ .p = "b", .ts = 10, .v = "b1", .m = .{} });
    try put(f, .{ .p = "c", .ts = 10, .v = "c1", .m = .{} });
    try roar.flush(f, alloc, 2);
    try put(f, .{ .p = "a", .ts = 20, .v = "a2", .m = .{} });
    try put(f, .{ .p = "b", .ts = 20, .v = "", .m = .{ .kind = .deleted } });
    try put(f, .{ .p = "d", .ts = 20, .v = "d1", .m = .{} });
    try roar.flush(f, alloc, 3);
    try put(f, .{ .p = "e", .ts = 30, .v = "e1", .m = .{} });

    // Changed after 10: a (rewritten), b (its tombstone, with
    // include_deleted), d and e (inserted). c is unchanged and absent.
    // a's @10 version is not emitted even when asked for more
    // versions: it is at or below min_ts, and its SST is not opened.
    {
        var col = ScanCollector{ .alloc = alloc };
        defer col.deinit();
        const n = try runScan(alloc, f, .{
            .min_ts = 10,
            .include_deleted = true,
            .num_versions = 3,
        }, &col);
        try std.testing.expectEqual(@as(u64, 4), n);
        const rows = col.got.items;
        try std.testing.expectEqualStrings("a", rows[0].key.primary);
        try std.testing.expectEqualStrings("a2", rows[0].newest().value);
        try std.testing.expectEqualStrings("b", rows[1].key.primary);
        try std.testing.expect(rows[1].newest().meta.isDeleted());
        try std.testing.expectEqualStrings("d1", rows[2].newest().value);
        try std.testing.expectEqualStrings("e1", rows[3].newest().value);
    }
    // Without include_deleted the deletion is a change with nothing
    // to show, as for any scan.
    {
        var col = ScanCollector{ .alloc = alloc };
        defer col.deinit();
        const n = try runScan(alloc, f, .{ .min_ts = 10 }, &col);
        try std.testing.expectEqual(@as(u64, 3), n);
        const rows = col.got.items;
        try std.testing.expectEqualStrings("a2", rows[0].newest().value);
        try std.testing.expectEqualStrings("d1", rows[1].newest().value);
        try std.testing.expectEqualStrings("e1", rows[2].newest().value);
    }
    // min_ts and read_ts bound the window from both sides.
    {
        var col = ScanCollector{ .alloc = alloc };
        defer col.deinit();
        const n = try runScan(alloc, f, .{ .min_ts = 20, .read_ts = 25 }, &col);
        try std.testing.expectEqual(@as(u64, 0), n);
    }
    {
        var col = ScanCollector{ .alloc = alloc };
        defer col.deinit();
        const n = try runScan(alloc, f, .{ .min_ts = 20 }, &col);
        try std.testing.expectEqual(@as(u64, 1), n);
        const rows = col.got.items;
        try std.testing.expectEqualStrings("e1", rows[0].newest().value);
    }
}

test "scan: versions above read_ts do not consume num_versions" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    try seedVersionedForest(f);

    var col = ScanCollector{ .alloc = alloc };
    defer col.deinit();
    // a@20 is invisible at ts=15. With num_versions=1, the one version we
    // return must be a@10 — the invisible a@20 must not have spent the quota.
    const n = try runScan(alloc, f, .{ .num_versions = 1, .read_ts = 15 }, &col);
    try std.testing.expectEqual(@as(u64, 3), n);
    try std.testing.expectEqualStrings("a1", col.got.items[0].newest().value);
    try std.testing.expectEqual(@as(u64, 10), col.got.items[0].newest().ts);
}

test "scan: read_ts at the delete sees the key as gone" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    try seedVersionedForest(f);

    var col = ScanCollector{ .alloc = alloc };
    defer col.deinit();
    // At ts=20 the tombstone IS visible, so b disappears again.
    const n = try runScan(alloc, f, .{ .read_ts = 20 }, &col);
    try std.testing.expectEqual(@as(u64, 2), n);
    try std.testing.expectEqualStrings("a2", col.got.items[0].newest().value);
    try std.testing.expectEqualStrings("c1", col.got.items[1].newest().value);
}

test "scan: rejects a read_ts below globalMinRead" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    try seedVersionedForest(f);

    _ = f.setGlobalMinRead(100);

    var col = ScanCollector{ .alloc = alloc };
    defer col.deinit();
    try std.testing.expectError(error.ReadBelowMinRead, runScan(alloc, f, .{ .read_ts = 99 }, &col));

    // At the horizon is fine, and the default (latest) is always fine.
    _ = try runScan(alloc, f, .{ .read_ts = 100 }, &col);
    _ = try runScan(alloc, f, .{}, &col);
}

// ---------------------------------------------------------------------------
// Cursor: the pull form of scan
// ---------------------------------------------------------------------------

/// The whole of one table, both bounds open.
fn tableRange(ns: u64, table_id: u32) KeyRange {
    const open = Key{ .namespace = ns, .table_id = table_id, .primary = "" };
    return .{ .left = open, .right = open };
}

/// Drain a cursor the way `scan` does, duping each item at hand-out time.
fn drainCursor(cur: *Forest.Cursor, col: *ScanCollector) !void {
    while (try cur.next()) |item| _ = try col.emit(item);
}

/// `scan` and a cursor over the same table and options must agree item for
/// item — and on the count both report.
fn expectCursorMatchesScan(alloc: std.mem.Allocator, f: *Forest, ns: u64, table_id: u32, opts: Forest.ScanOptions) !void {
    var via_scan = ScanCollector{ .alloc = alloc };
    defer via_scan.deinit();
    const n = try f.scan(alloc, tableRange(ns, table_id), opts, &via_scan, ScanCollector.emit);

    var via_cursor = ScanCollector{ .alloc = alloc };
    defer via_cursor.deinit();
    var cur = try f.openCursor(alloc, tableRange(ns, table_id), opts);
    defer cur.deinit();
    try drainCursor(&cur, &via_cursor);

    try std.testing.expectEqual(n, cur.emitted);
    try std.testing.expectEqual(via_scan.got.items.len, via_cursor.got.items.len);
    for (via_scan.got.items, via_cursor.got.items) |a, b| {
        try std.testing.expectEqualStrings(a.key.primary, b.key.primary);
        try std.testing.expectEqual(a.newest().ts, b.newest().ts);
        try std.testing.expectEqualStrings(a.newest().value, b.newest().value);
        try std.testing.expectEqual(a.newest().meta, b.newest().meta);
    }
}

test "cursor: agrees with scan under every option" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    try seedVersionedForest(f);

    // Every combination the scan tests above exercise, plus a few crossings.
    const variants = [_]Forest.ScanOptions{
        .{},
        .{ .num_versions = 10 },
        .{ .num_versions = 10, .include_deleted = true },
        .{ .num_versions = 1, .include_deleted = true },
        .{ .num_versions = 0 },
        .{ .max_rows = 2 },
        .{ .max_rows = 1 },
        .{ .max_rows = 0 },
        .{ .read_ts = 15 },
        .{ .read_ts = 20 },
        .{ .read_ts = 15, .num_versions = 5, .include_deleted = true },
        .{ .read_ts = 10, .max_rows = 2, .include_deleted = true },
    };
    for (variants) |opts| try expectCursorMatchesScan(alloc, f, 1, 0, opts);
}

test "cursor: null is sticky and emitted counts hand-outs" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    try seedVersionedForest(f);

    // Walked to the end: a's newest, c's only (b is deleted).
    {
        var cur = try f.openCursor(alloc, tableRange(1, 0), .{});
        defer cur.deinit();
        const first = (try cur.next()) orelse return error.TestExpectedItem;
        try std.testing.expectEqualStrings("a2", first.newest().value);
        const second = (try cur.next()) orelse return error.TestExpectedItem;
        try std.testing.expectEqualStrings("c1", second.newest().value);
        try std.testing.expectEqual(null, try cur.next());
        try std.testing.expectEqual(null, try cur.next());
        try std.testing.expectEqual(@as(u64, 2), cur.emitted);
    }
    // Cut off by max_rows: the cap counts hand-outs, and holds afterwards.
    {
        var cur = try f.openCursor(alloc, tableRange(1, 0), .{ .max_rows = 1 });
        defer cur.deinit();
        const first = (try cur.next()) orelse return error.TestExpectedItem;
        try std.testing.expectEqualStrings("a2", first.newest().value);
        try std.testing.expectEqual(null, try cur.next());
        try std.testing.expectEqual(null, try cur.next());
        try std.testing.expectEqual(@as(u64, 1), cur.emitted);
    }
}

test "cursor: max_rows 0 opens exhausted without the min-read check" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    try seedVersionedForest(f);
    _ = f.setGlobalMinRead(100);

    // Below the horizon is refused at open — unless nothing will be read.
    try std.testing.expectError(error.ReadBelowMinRead, f.openCursor(alloc, tableRange(1, 0), .{ .read_ts = 99 }));
    {
        var cur = try f.openCursor(alloc, tableRange(1, 0), .{ .read_ts = 99, .max_rows = 0 });
        defer cur.deinit();
        try std.testing.expectEqual(null, try cur.next());
        try std.testing.expectEqual(@as(u64, 0), cur.emitted);
    }
    // At the horizon is fine, and the default (latest) is always fine.
    {
        var cur = try f.openCursor(alloc, tableRange(1, 0), .{ .read_ts = 100 });
        defer cur.deinit();
        try std.testing.expect((try cur.next()) != null);
    }
    {
        var cur = try f.openCursor(alloc, tableRange(1, 0), .{});
        defer cur.deinit();
        try std.testing.expect((try cur.next()) != null);
    }
}

test "cursor: deinit mid-walk releases the sources" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    try seedVersionedForest(f);

    // Take one item and walk away. The testing allocator catches a leaked
    // iterator slice or key buffer; Forest.close catches a memtable or
    // table ref that was never given back.
    {
        var cur = try f.openCursor(alloc, tableRange(1, 0), .{ .num_versions = 10, .include_deleted = true });
        defer cur.deinit();
        const first = (try cur.next()) orelse return error.TestExpectedItem;
        try std.testing.expectEqualStrings("a2", first.newest().value);
    }
    // And the forest is none the worse for it.
    try expectCursorMatchesScan(alloc, f, 1, 0, .{});
}

test "cursor: a flush between two next calls does not disturb the walk" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    _ = try f.createTree(1, 0, .{ .inf = true });
    try f.newMemtable(1);

    const primaries = [_]String{ "a", "b", "c", "d", "e" };
    const values = [_]String{ "va", "vb", "vc", "vd", "ve" };
    for (primaries, values) |p, v| {
        try f.put(&[_]zdb.kv.Item{.{
            .key = .{ .namespace = 1, .table_id = 0, .primary = p },
            .versions = .{ .one = .{ .ts = 5, .meta = .{}, .value = v } },
        }});
    }

    var cur = try f.openCursor(alloc, tableRange(1, 0), .{});
    defer cur.deinit();
    const first = (try cur.next()) orelse return error.TestExpectedItem;
    try std.testing.expectEqualStrings("a", first.key.primary);

    // The memtable the cursor is reading is frozen and flushed to L0 under
    // it. The cursor holds a ref on that skiplist, so its walk carries on
    // from the snapshot it opened — nothing skipped, nothing repeated.
    try roar.flush(f, alloc, 2);

    var rest = ScanCollector{ .alloc = alloc };
    defer rest.deinit();
    try drainCursor(&cur, &rest);
    try std.testing.expectEqual(@as(usize, 4), rest.got.items.len);
    for (rest.got.items, primaries[1..], values[1..]) |it, p, v| {
        try std.testing.expectEqualStrings(p, it.key.primary);
        try std.testing.expectEqualStrings(v, it.newest().value);
    }
    try std.testing.expectEqual(@as(u64, 5), cur.emitted);

    // A cursor opened now reads the same rows back from the L0 SST.
    var again = ScanCollector{ .alloc = alloc };
    defer again.deinit();
    var cur2 = try f.openCursor(alloc, tableRange(1, 0), .{});
    defer cur2.deinit();
    try drainCursor(&cur2, &again);
    try std.testing.expectEqual(@as(usize, 5), again.got.items.len);
    for (again.got.items, primaries, values) |it, p, v| {
        try std.testing.expectEqualStrings(p, it.key.primary);
        try std.testing.expectEqualStrings(v, it.newest().value);
    }
}

test "cursor: pin_values holds handed-out values until releasePins" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    _ = try f.createTree(1, 0, .{ .inf = true });
    try f.newMemtable(1);

    // 200 rows of 1 KiB, value[0] = the row number: one memtable arena
    // now, several SST blocks (the builder targets 32 KiB) once flushed.
    const n_rows = 200;
    var keys: [n_rows][3]u8 = undefined;
    for (&keys, 0..) |*k, i| _ = try std.fmt.bufPrint(k, "{d:0>3}", .{i});
    var value: [1024]u8 = undefined;
    for (keys, 0..) |k, i| {
        @memset(&value, 'w');
        value[0] = @intCast(i);
        try f.put(&[_]zdb.kv.Item{.{
            .key = .{ .namespace = 1, .table_id = 0, .primary = &k },
            .versions = .{ .one = .{ .ts = 5, .meta = .{}, .value = &value } },
        }});
    }
    const pinning = Forest.ScanOptions{ .pin_values = true };

    const Walk = struct {
        /// Hand out `out.len` items, keeping only their value slices.
        fn take(cur: *Forest.Cursor, out: []String) !void {
            for (out) |*slot| {
                const it = (try cur.next()) orelse
                    return error.TestExpectedItem;
                slot.* = it.newest().value;
            }
        }
        /// The slices still read as the rows they were, from `first` on.
        fn check(vals: []const String, first: usize) !void {
            for (vals, first..) |v, i| {
                try std.testing.expectEqual(@as(usize, 1024), v.len);
                try std.testing.expectEqual(@as(u8, @intCast(i)), v[0]);
                try std.testing.expectEqual(@as(u8, 'w'), v[1023]);
            }
        }
    };

    // Memtable-backed: every value lives in one arena — one pin.
    {
        var cur = try f.openCursor(alloc, tableRange(1, 0), pinning);
        defer cur.deinit();
        var vals: [n_rows]String = undefined;
        try Walk.take(&cur, &vals);
        try std.testing.expectEqual(@as(usize, 1), cur.pins.items.len);
        try Walk.check(&vals, 0);
    }

    try roar.flush(f, alloc, 2);

    // SST-backed: the first half spans blocks the table iterator has
    // moved past by the time the walk pauses — the pins keep them.
    var cur = try f.openCursor(alloc, tableRange(1, 0), pinning);
    defer cur.deinit();
    var first: [n_rows / 2]String = undefined;
    try Walk.take(&cur, &first);
    try std.testing.expect(cur.pins.items.len >= 2);
    try Walk.check(&first, 0);

    // Release: only the parked item's storage stays held, and that item
    // (the last one handed out) still reads.
    try cur.releasePins();
    try std.testing.expectEqual(@as(usize, 1), cur.pins.items.len);
    try Walk.check(first[first.len - 1 ..], first.len - 1);

    // The rest pins afresh; once the walk is over nothing is parked, so
    // a release leaves nothing held.
    var rest: [n_rows / 2]String = undefined;
    try Walk.take(&cur, &rest);
    try Walk.check(&rest, n_rows / 2);
    try std.testing.expect(cur.pins.items.len >= 2);
    try std.testing.expect((try cur.next()) == null);
    try cur.releasePins();
    try std.testing.expectEqual(@as(usize, 0), cur.pins.items.len);
}

/// Ten one-key L3 SSTs k01..k10 in (namespace 42, table 0), value = the
/// key, in one tree. Returns its id.
fn seedTenKeyTable(alloc: std.mem.Allocator, f: *Forest, dir_path: String) !u64 {
    const tree_id = try f.createTree(42, 0, .{ .inf = true });
    const keys = [_]String{ "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10" };
    for (keys, 1..) |k, tbl_id| {
        const path = try zdb.sstPath(alloc, dir_path, tbl_id);
        defer alloc.free(path);
        try buildAndWriteTableKV(alloc, path, .{
            .namespace = 42,
            .table_id = 0,
            .primary = k,
        }, 1, k);
    }
    {
        const tree_ptr = f.getTree(42, tree_id).?;
        defer tree_ptr.release();
        var changes: [keys.len]_manifest.SSTableChange = undefined;
        for (&changes, 1..) |*c, tbl_id| c.* = _manifest.createSSTableChange(@intCast(tbl_id), 42, tree_id, 3); // L3
        try tree_ptr.applyCompaction(1, &changes);
    }
    return tree_id;
}

/// `seedTenKeyTable`, then a split: two trees, the boundary somewhere in
/// the middle.
fn seedSplitForest(alloc: std.mem.Allocator, f: *Forest, dir_path: String) !void {
    const tree_id = try seedTenKeyTable(alloc, f, dir_path);
    try f.splitTreeForTests(42, tree_id);
    try std.testing.expectEqual(@as(usize, 2), treeCount(f));
}

test "cursor: a split applied by another fiber mid-walk loses and repeats nothing" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    const tree_id = try seedTenKeyTable(alloc, f, dir_path);

    // Three keys in: the merge is open with the tree's tables pinned, and
    // between calls the cursor holds neither a lock nor a tree ref.
    var got = ScanCollector{ .alloc = alloc };
    defer got.deinit();
    var cur = try f.openCursor(alloc, tableRange(42, 0), .{});
    defer cur.deinit();
    for (0..3) |_| _ = try got.emit((try cur.next()) orelse return error.TestExpectedItem);

    // The split runs on another fiber, as raft apply would: fresh objects
    // for both halves swapped in, the old one retired.
    const Split = struct {
        fn run(forest: *Forest, id: u64) void {
            forest.splitTreeForTests(42, id) catch |e| std.debug.panic("split: {}", .{e});
        }
    };
    var h = try fiber.spawn(.{}, Split.run, .{ f, tree_id });
    h.join();
    try std.testing.expectEqual(@as(usize, 2), treeCount(f));

    // The pinned tables still cover the whole pre-split range, so the walk
    // finishes from them and the hop lands past the new right tree.
    try drainCursor(&cur, &got);
    try std.testing.expectEqual(@as(usize, 10), got.got.items.len);
    try std.testing.expectEqual(@as(u64, 10), cur.emitted);
    const keys = [_]String{ "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10" };
    for (got.got.items, keys) |it, k| {
        try std.testing.expectEqualStrings(k, it.key.primary);
        try std.testing.expectEqualStrings(k, it.newest().value);
    }

    // A cursor opened after the split walks both trees.
    try expectCursorMatchesScan(alloc, f, 42, 0, .{});
}

test "split: a reader holding the old tree keeps its full range until it lets go" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    const tree_id = try seedTenKeyTable(alloc, f, dir_path);

    // Take a ref the way a reader does, then split underneath it.
    const old = f.getTree(42, tree_id) orelse return error.TestExpectedTree;
    try f.splitTreeForTests(42, tree_id);
    try std.testing.expectEqual(@as(usize, 2), treeCount(f));

    // The old object is untouched: unbounded right edge, all ten tables.
    try std.testing.expect(old.key_range.right.primary.len == 0);
    try std.testing.expectEqual(@as(usize, 10), old.numTablesAt(3));
    const k10 = try old.get(.{
        .key = .{ .namespace = 42, .table_id = 0, .primary = "k10" },
    }) orelse return error.TestExpectedItem;
    defer k10.free(alloc);
    try std.testing.expectEqualStrings("k10", k10.newest().value);

    // The listed left half is a fresh object with the narrowed range.
    const left = f.getTree(42, tree_id) orelse return error.TestExpectedTree;
    defer left.release();
    try std.testing.expect(left != old);
    try std.testing.expectEqualStrings("k06", left.key_range.right.primary);
    try std.testing.expectEqual(@as(usize, 5), left.numTablesAt(3));

    // Letting go closes the old object (the testing allocator would report
    // a leak otherwise).
    old.release();
}

test "cursor: hops across a split like scan does" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    try seedSplitForest(alloc, f, dir_path);

    try expectCursorMatchesScan(alloc, f, 42, 0, .{});
    // A cap that lands inside the second tree.
    try expectCursorMatchesScan(alloc, f, 42, 0, .{ .max_rows = 7 });

    // All ten, in order, across the hop: the first tree holds only the
    // lower keys, so k10 arriving proves the second tree was visited.
    var col = ScanCollector{ .alloc = alloc };
    defer col.deinit();
    var cur = try f.openCursor(alloc, tableRange(42, 0), .{});
    defer cur.deinit();
    try drainCursor(&cur, &col);
    try std.testing.expectEqual(@as(usize, 10), col.got.items.len);
    try std.testing.expectEqualStrings("k01", col.got.items[0].key.primary);
    try std.testing.expectEqualStrings("k10", col.got.items[9].key.primary);
    for (col.got.items) |it| {
        try std.testing.expectEqualStrings(it.key.primary, it.newest().value);
    }
    try std.testing.expect(cur.done);
}

// ---------------------------------------------------------------------------
// getBitmap (roaring postings)
// ---------------------------------------------------------------------------

const zroar = zdb.zroar;

/// Helpers for the roaring-posting tests. Writers emit RoaringDelta
/// messages exactly the way the future index write path will: with the
/// generated writer, tail-less, meta = {.delta, .roaring}.
const roar = struct {
    const delta_meta: zdb.kv.Meta = .{ .kind = .delta, .encoding = .roaring };
    const state_meta: zdb.kv.Meta = .{ .kind = .state, .encoding = .roaring };

    fn key(primary: String) Key {
        return .{
            .namespace = 1,
            .table_id = 0,
            .primary = primary,
        };
    }

    fn putDelta(
        f: *Forest,
        alloc: std.mem.Allocator,
        primary: String,
        ts: u64,
        adds: []const u64,
        rems: []const u64,
    ) !void {
        var list: std.ArrayList(u8) = .empty;
        defer list.deinit(alloc);
        var w = try zdb.buffers.RoaringDeltaWriter.initWithBuf(alloc, &list);
        if (adds.len > 0) try w.setAdds(adds);
        if (rems.len > 0) try w.setRemoves(rems);
        _ = try w.finish();
        const items = [_]zdb.kv.Item{.{
            .key = key(primary),
            .versions = .{ .one = .{
                .ts = ts,
                .meta = delta_meta,
                .value = list.items,
            } },
        }};
        try f.put(&items);
    }

    fn putState(
        f: *Forest,
        alloc: std.mem.Allocator,
        primary: String,
        ts: u64,
        vals: []const u64,
    ) !void {
        var bm = try zroar.Bitmap.fromSortedList(alloc, vals);
        defer bm.deinit();
        const items = [_]zdb.kv.Item{.{
            .key = key(primary),
            .versions = .{ .one = .{
                .ts = ts,
                .meta = state_meta,
                .value = bm.toBuffer(),
            } },
        }};
        try f.put(&items);
    }

    fn expectBitmap(
        f: *Forest,
        alloc: std.mem.Allocator,
        primary: String,
        read_ts: u64,
        expected: []const u64,
    ) !void {
        var bm = try f.getBitmap(alloc, .{
            .key = key(primary),
            .read_ts = read_ts,
        });
        defer bm.deinit();
        try std.testing.expectEqual(expected.len, bm.getCardinality());
        for (expected) |v| try std.testing.expect(bm.contains(v));
    }

    /// Freeze the current memtable and flush it to an L0 SST. `next_id`
    /// stands in for the log index of both entries, so successive flushes
    /// land in L0 in the order they ran.
    fn flush(f: *Forest, alloc: std.mem.Allocator, next_id: u64) !void {
        try f.newMemtable(next_id);
        var fl = (try f.buildFlush()) orelse return error.TestExpectedFlush;
        defer fl.changes.deinit(alloc);
        try f.applyFlush(next_id, fl.changes.items, fl.memtable_id);
    }
};

test "forest: getBitmap folds deltas from memtable, L0, and history" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    _ = try f.createTree(1, 0, .{ .inf = true });
    try f.newMemtable(1);

    // History of posting "p": base state at t10, then deltas layering
    // adds and removes on top at t20/t30.
    try roar.putState(f, alloc, "p", 10, &.{ 1, 2, 3 });
    try roar.putDelta(f, alloc, "p", 20, &.{ 40, 50 }, &.{2});
    try roar.putDelta(f, alloc, "p", 30, &.{2}, &.{ 1, 40 });

    // Whole chain still in the memtable.
    try roar.expectBitmap(f, alloc, "p", std.math.maxInt(u64), &.{ 2, 3, 50 });

    // Historical reads reconstruct each point in time.
    try roar.expectBitmap(f, alloc, "p", 25, &.{ 1, 3, 40, 50 });
    try roar.expectBitmap(f, alloc, "p", 10, &.{ 1, 2, 3 });
    // Before the base existed: empty.
    try roar.expectBitmap(f, alloc, "p", 5, &.{});

    // Flush chain to L0; answers must not change.
    try roar.flush(f, alloc, 2);
    try roar.expectBitmap(f, alloc, "p", std.math.maxInt(u64), &.{ 2, 3, 50 });
    try roar.expectBitmap(f, alloc, "p", 25, &.{ 1, 3, 40, 50 });

    // New deltas in the fresh memtable fold onto the flushed history.
    try roar.putDelta(f, alloc, "p", 40, &.{100}, &.{3});
    try roar.expectBitmap(f, alloc, "p", std.math.maxInt(u64), &.{ 2, 50, 100 });
}

test "forest: getBitmap tombstone resets, absent key is empty" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    _ = try f.createTree(1, 0, .{ .inf = true });
    try f.newMemtable(1);

    // State, then a tombstone (the index key was cleared), then a new
    // delta. The tombstone terminates the fold: only t30 survives.
    try roar.putState(f, alloc, "p", 10, &.{ 1, 2, 3 });
    const del = [_]zdb.kv.Item{.{
        .key = roar.key("p"),
        .versions = .{ .one = .{
            .ts = 20,
            .meta = .{ .kind = .deleted },
            .value = "",
        } },
    }};
    try f.put(&del);
    try roar.putDelta(f, alloc, "p", 30, &.{7}, &.{});

    try roar.expectBitmap(f, alloc, "p", std.math.maxInt(u64), &.{7});
    // As of the tombstone: empty. Before it: the old state.
    try roar.expectBitmap(f, alloc, "p", 25, &.{});
    try roar.expectBitmap(f, alloc, "p", 15, &.{ 1, 2, 3 });

    // A key never written reads as an empty posting, not an error.
    try roar.expectBitmap(f, alloc, "absent", std.math.maxInt(u64), &.{});
}

test "forest: rollup is read-invariant across flush and compactions" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    _ = try f.createTree(1, 0, .{ .inf = true });
    try f.newMemtable(1);

    // "p" has a full chain (base in inputs → rolls up at L0→L1);
    // "q" is a baseless delta (stays verbatim until read-below lands).
    try roar.putState(f, alloc, "p", 10, &.{ 1, 2 });
    try roar.putDelta(f, alloc, "p", 20, &.{40}, &.{2});
    try roar.putDelta(f, alloc, "p", 30, &.{2}, &.{1});
    try roar.putDelta(f, alloc, "q", 15, &.{7}, &.{});

    // The oracle: every (key, read_ts) answer, captured while the
    // whole history is still in the memtable. Rollup may reshape the
    // physical chain at will; these answers must never change.
    const Case = struct { key: String, ts: u64, want: []const u64 };
    const cases = [_]Case{
        .{ .key = "p", .ts = 5, .want = &.{} },
        .{ .key = "p", .ts = 10, .want = &.{ 1, 2 } },
        .{ .key = "p", .ts = 25, .want = &.{ 1, 40 } },
        .{ .key = "p", .ts = 35, .want = &.{ 2, 40 } },
        .{ .key = "p", .ts = std.math.maxInt(u64), .want = &.{ 2, 40 } },
        .{ .key = "q", .ts = 5, .want = &.{} },
        .{ .key = "q", .ts = std.math.maxInt(u64), .want = &.{7} },
    };
    for (cases) |c| try roar.expectBitmap(f, alloc, c.key, c.ts, c.want);

    // Flush to L0, then push the data down two levels; re-check the
    // oracle after every reshape.
    try roar.flush(f, alloc, 2);
    for (cases) |c| try roar.expectBitmap(f, alloc, c.key, c.ts, c.want);

    const tree = f.getTree(1, 0) orelse return error.TestExpectedTree;
    defer tree.release();
    for ([_]u8{ 0, 1 }) |src| {
        var comp = try tree.buildCompaction(src, 0);
        defer comp.deinit(alloc);
        try std.testing.expect(comp.items.len > 0);
        try tree.applyCompaction(1, comp.items);
        for (cases) |c| try roar.expectBitmap(f, alloc, c.key, c.ts, c.want);
    }

    // Sanity that the rollup actually happened (this test must not
    // pass vacuously): "p"'s newest version is now a synthetic state.
    const top = try f.getRaw(alloc, .{ .key = roar.key("p") }) orelse
        return error.TestExpectedItem;
    defer top.free(alloc);
    try std.testing.expect(top.newest().meta.isState());
    try std.testing.expect(top.newest().meta.isRoaring());
    try std.testing.expectEqual(@as(u64, 30), top.newest().ts);
}

test "forest: getBitmap rejects non-roaring chains and old reads" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    _ = try f.createTree(1, 0, .{ .inf = true });
    try f.newMemtable(1);

    const raw = [_]zdb.kv.Item{.{
        .key = roar.key("r"),
        .versions = .{ .one = .{ .ts = 10, .meta = .{}, .value = "plain" } },
    }};
    try f.put(&raw);
    try std.testing.expectError(
        error.NotRoaring,
        f.getBitmap(alloc, .{ .key = roar.key("r") }),
    );

    // Undecodable delta bytes (only structure is checked on reads —
    // content is validated at write time) surface as corruption.
    const junk = [_]zdb.kv.Item{.{
        .key = roar.key("c"),
        .versions = .{ .one = .{
            .ts = 10,
            .meta = roar.delta_meta,
            .value = "junk",
        } },
    }};
    try f.put(&junk);
    try std.testing.expectError(
        error.CorruptPosting,
        f.getBitmap(alloc, .{ .key = roar.key("c") }),
    );

    _ = f.setGlobalMinRead(100);
    try std.testing.expectError(
        error.ReadBelowMinRead,
        f.getBitmap(alloc, .{ .key = roar.key("r"), .read_ts = 99 }),
    );
}

test "forest: index backfill installs bottom states below live deltas" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    _ = try f.createTree(1, 0, .{ .inf = true });
    try f.newMemtable(1);

    // The backfill product: one roaring STATE per index key at the
    // snapshot ts, installed straight into the bottom level — the
    // oldest data the tree will ever hold.
    const snap: u64 = 500;
    var blue = try zroar.Bitmap.fromSortedList(alloc, &.{ 1, 3 });
    defer blue.deinit();
    var red = try zroar.Bitmap.fromSortedList(alloc, &.{2});
    defer red.deinit();
    const items = [_]zdb.kv.Item{
        .{ .key = roar.key("blue"), .versions = .{ .one = .{
            .ts = snap,
            .meta = roar.state_meta,
            .value = blue.toBuffer(),
        } } },
        .{ .key = roar.key("red"), .versions = .{ .one = .{
            .ts = snap,
            .meta = roar.state_meta,
            .value = red.toBuffer(),
        } } },
    };
    var changes = try f.buildBottomTable(alloc, 1, 0, &items);
    defer changes.deinit(alloc);
    try std.testing.expectEqual(1, changes.items.len);
    const tree = f.getTree(1, zdb.kv.makeTreeId(0, 0)).?;
    defer tree.release();
    try tree.applyBackfill(1, changes.items);
    try std.testing.expect(try f.bottomLevelHas(1, 0, changes.items));

    // Post-snapshot DML: a delta strictly above the states.
    try roar.putDelta(f, alloc, "blue", 600, &.{5}, &.{});

    // At/above the delta the fold layers it over the bottom state;
    // between snap and the delta the state answers alone; other keys
    // are untouched.
    try roar.expectBitmap(f, alloc, "blue", 700, &.{ 1, 3, 5 });
    try roar.expectBitmap(f, alloc, "blue", 550, &.{ 1, 3 });
    try roar.expectBitmap(f, alloc, "red", 700, &.{2});

    // A second install must skip, deterministically: the bottom level
    // is no longer empty, so the sorted run (and any rolled-up
    // terminal) cannot be undercut.
    var again = try f.buildBottomTable(alloc, 1, 0, &items);
    defer again.deinit(alloc);
    try tree.applyBackfill(1, again.items);
    // The first install's files are there, the second's are not —
    // which a table count could not tell apart.
    try std.testing.expect(try f.bottomLevelHas(1, 0, changes.items));
    try std.testing.expect(!try f.bottomLevelHas(1, 0, again.items));
    try roar.expectBitmap(f, alloc, "blue", 700, &.{ 1, 3, 5 });
}

// ---------------------------------------------------------------------------
// File GC: retire on the last unlist, unlink once the snapshot index passes
// ---------------------------------------------------------------------------

/// Put one row into the current memtable.
fn putRow(f: *Forest, primary: String, ts: u64, value: String) !void {
    const items = [_]zdb.kv.Item{.{
        .key = .{ .namespace = 1, .table_id = 0, .primary = primary },
        .versions = .{ .one = .{ .ts = ts, .meta = .{}, .value = value } },
    }};
    try f.put(&items);
}

/// Whether `id`'s SST file is still on disk.
fn sstExists(alloc: std.mem.Allocator, dir: String, id: u64) !bool {
    const path = try zdb.sstPath(alloc, dir, id);
    defer alloc.free(path);
    vfs.access(path) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}

test "forest: deleteFiles unlinks compaction inputs at the retire index" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();

    _ = try f.createTree(1, 0, .{ .inf = true });

    // Two flushes → two L0 SSTs, so the compaction below has real inputs.
    try f.newMemtable(1);
    try putRow(f, "a", 1, "one");
    try f.newMemtable(2);
    var flush_a = (try f.buildFlush()) orelse return error.TestExpectedFlush;
    defer flush_a.changes.deinit(alloc);
    try f.applyFlush(3, flush_a.changes.items, flush_a.memtable_id);

    try putRow(f, "b", 2, "two");
    try f.newMemtable(4);
    var flush_b = (try f.buildFlush()) orelse return error.TestExpectedFlush;
    defer flush_b.changes.deinit(alloc);
    try f.applyFlush(5, flush_b.changes.items, flush_b.memtable_id);

    const tree = f.getTree(1, 0) orelse return error.TestExpectedTree;
    defer tree.release();

    // Pin one input before the compaction takes it out of the level: the
    // pin has to keep the table readable across the unlink below.
    const pinned = blk: {
        const held = tree.mu.hold(.shared, @src());
        defer held.release();
        const ents = tree.levels[0].entries(held);
        try std.testing.expectEqual(@as(usize, 2), ents.len);
        ents[0].table.incrRef();
        break :blk ents[0].table;
    };
    defer pinned.decrRef();

    var comp = try tree.buildCompaction(0, 0);
    defer comp.deinit(alloc);
    try std.testing.expect(comp.items.len > 0);

    var inputs: std.ArrayList(u64) = .empty;
    defer inputs.deinit(alloc);
    var outputs: std.ArrayList(u64) = .empty;
    defer outputs.deinit(alloc);
    for (comp.items) |c| {
        if (c.op == .delete)
            try inputs.append(alloc, c.sst_id)
        else
            try outputs.append(alloc, c.sst_id);
    }
    try std.testing.expectEqual(@as(usize, 2), inputs.items.len);
    try std.testing.expectEqual(@as(usize, 1), outputs.items.len);

    const k: u64 = 20;
    try tree.applyCompaction(k, comp.items);
    // What the engine runs after every apply: the unlisted inputs leave
    // the table cache.
    try f.evictTables();

    // Applying the compaction retires the inputs but leaves them on disk.
    for (inputs.items) |id|
        try std.testing.expect(try sstExists(alloc, dir_path, id));

    // A snapshot index one short of the retire index unlinks nothing.
    try f.deleteFiles(k - 1);
    for (inputs.items) |id|
        try std.testing.expect(try sstExists(alloc, dir_path, id));

    // At the retire index the inputs go; the output stays.
    try f.deleteFiles(k);
    for (inputs.items) |id|
        try std.testing.expect(!try sstExists(alloc, dir_path, id));
    for (outputs.items) |id|
        try std.testing.expect(try sstExists(alloc, dir_path, id));

    // The compaction output still reads back through the forest.
    {
        const got = try f.getRaw(alloc, .{
            .key = .{ .namespace = 1, .table_id = 0, .primary = "a" },
        }) orelse return error.TestExpectedItem;
        defer got.free(alloc);
        try std.testing.expectEqualStrings("one", got.newest().value);
    }

    // And the pinned input still serves a read off its open descriptor,
    // although the cache dropped it and the file is gone.
    try std.testing.expect(!f.table_cache.contains(pinned.file_id));
    {
        var it = zdb.table.TableIterator.init(alloc, pinned, .p0);
        defer it.deinit();
        try it.seekRange(pinned.smallest, .{});
        try std.testing.expect(it.itemRef() != null);
    }
}

/// Backdate a file's mtime so the orphan scan sees it as old, without a
/// test ever sleeping. Direct std.Io: setting times is a test-only need,
/// so vfs carries no wrapper for it.
fn backdate(path: String, secs: i64) !void {
    try std.Io.Dir.cwd().setTimestamps(utils.context.g().io, path, .{
        .modify_timestamp = .{ .new = .{ .nanoseconds = @as(i96, secs) * std.time.ns_per_s } },
    });
}

test "table cache: a table loaded for an install survives an evict before it is installed" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();

    _ = try f.createTree(1, 0, .{ .inf = true });
    try f.newMemtable(1);
    try putRow(f, "a", 1, "one");
    try f.newMemtable(2);
    var flush = (try f.buildFlush()) orelse return error.TestExpectedFlush;
    defer flush.changes.deinit(alloc);
    try f.applyFlush(3, flush.changes.items, flush.memtable_id);
    const listed = flush.changes.items[0].sst_id;

    // A real SST under an id the manifest has not heard of yet, pre-opened
    // the way a follower's prefetch does: the cache's ref is its only one.
    const pending: u64 = 0x5eed;
    {
        const read = f.readFile(alloc, listed) orelse return error.TestExpectedFile;
        defer read.free();
        f.writeFile(pending, read.data);
    }
    try std.testing.expect(f.table_cache.contains(pending));

    // An apply looks the table up for its install...
    var needs = [_]zdb.tree.TableNeed{.{ .id = pending, .level = 0 }};
    try f.table_cache.loadTables(&needs);
    const t = needs[0].table orelse return error.TestExpectedTable;

    // ...and before it installs it, the orphan scan evicts that id: the
    // manifest does not list it yet. The cache's ref goes.
    f.table_cache.evict(pending);
    try std.testing.expect(!f.table_cache.contains(pending));

    // The loader's own ref keeps the table open and readable.
    try std.testing.expectEqual(@as(i32, 1), t.ref.load(.acquire));
    {
        var it = zdb.table.TableIterator.init(alloc, t, .p0);
        defer it.deinit();
        try it.seekRange(t.smallest, .{});
        try std.testing.expect(it.itemRef() != null);
    }

    // Releasing it is the last ref: the table is freed here, which the
    // testing allocator checks.
    zdb.tree.TableCache.decrRefs(&needs);
    try std.testing.expect(needs[0].table == null);
}

test "forest: removeOrphans unlinks only unknown files at or before the cutoff" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();

    _ = try f.createTree(1, 0, .{ .inf = true });
    try f.newMemtable(1);
    try putRow(f, "a", 1, "one");
    try f.newMemtable(2);
    var flush = (try f.buildFlush()) orelse return error.TestExpectedFlush;
    defer flush.changes.deinit(alloc);
    try f.applyFlush(3, flush.changes.items, flush.memtable_id);
    const listed = flush.changes.items[0].sst_id;

    // Four SST-named files the manifest never heard of. Three are backdated
    // two hours, one a minute — the young one stands in for a file written
    // after the cutoff: some writer may still be working on it.
    const now = fiber.timestampNow(.realtime, .secs);
    const old_junk = [_]u64{ 0xdead, 0xbeef, 0xcafe };
    const young_junk: u64 = 0xf00d;
    for (old_junk ++ [_]u64{young_junk}, 0..) |id, i| {
        const path = try zdb.sstPath(alloc, dir_path, id);
        defer alloc.free(path);
        try vfs.writeFile(path, "not a real sst");
        const age_s: i64 = if (i < old_junk.len)
            2 * std.time.s_per_hour
        else
            std.time.s_per_min;
        try backdate(path, now - age_s);
    }

    // A file whose name is not an SST name at all.
    const other = try std.fmt.allocPrint(alloc, "{s}/NOTES.txt", .{dir_path});
    defer alloc.free(other);
    try vfs.writeFile(other, "keep me");
    try backdate(other, now - 2 * std.time.s_per_hour);

    // A real SST pre-opened into the table cache for a create that never
    // applies (the follower prefetch path): the scan must evict it, or
    // its open fd keeps the unlinked file's blocks.
    const prewarmed: u64 = 0xabcd;
    {
        const read = f.readFile(alloc, listed) orelse return error.TestExpectedFile;
        defer read.free();
        f.writeFile(prewarmed, read.data);
        try std.testing.expect(f.table_cache.contains(prewarmed));
        const path = try zdb.sstPath(alloc, dir_path, prewarmed);
        defer alloc.free(path);
        try backdate(path, now - 2 * std.time.s_per_hour);
    }

    // No snapshot yet (cutoff 0), or a cutoff older than every file:
    // nothing is an orphan.
    for ([_]i64{ 0, now - 3 * std.time.s_per_hour }) |cutoff_s| {
        try f.removeOrphans(cutoff_s);
        for (old_junk) |id|
            try std.testing.expect(try sstExists(alloc, dir_path, id));
        try std.testing.expect(try sstExists(alloc, dir_path, prewarmed));
        try std.testing.expect(try sstExists(alloc, dir_path, young_junk));
    }

    // A cutoff between the old files and the young one.
    try f.removeOrphans(now - std.time.s_per_hour);

    // Only the old unknown SSTs go, the pre-opened one with its cache entry.
    for (old_junk) |id|
        try std.testing.expect(!try sstExists(alloc, dir_path, id));
    try std.testing.expect(!try sstExists(alloc, dir_path, prewarmed));
    try std.testing.expect(!f.table_cache.contains(prewarmed));
    try std.testing.expect(try sstExists(alloc, dir_path, young_junk));
    try std.testing.expect(try sstExists(alloc, dir_path, listed));
    try vfs.access(other);

    // A listed file is left alone however old it is.
    {
        const path = try zdb.sstPath(alloc, dir_path, listed);
        defer alloc.free(path);
        try backdate(path, now - 2 * std.time.s_per_hour);
    }
    try f.removeOrphans(now - std.time.s_per_hour);
    try std.testing.expect(try sstExists(alloc, dir_path, listed));
    try std.testing.expect(try sstExists(alloc, dir_path, young_junk));

    // Once the cutoff passes the young file it goes too, and still
    // nothing the manifest lists.
    try f.removeOrphans(now);
    try std.testing.expect(!try sstExists(alloc, dir_path, young_junk));
    try std.testing.expect(try sstExists(alloc, dir_path, listed));
    try vfs.access(other);
}

test "forest: evictTables evicts a compacted-away file; its blocks go with the table" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // A private block cache: never installed in the process-wide slot,
    // since the runner executes tests in parallel and they would share
    // it. Tables are asked to prefetch into it explicitly.
    var cache = try zdb.cache.ShardedCache(u128).init(alloc, 64 << 20, 4);
    defer cache.deinit();

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    _ = try f.createTree(1, 0, .{ .inf = true });

    try f.newMemtable(1);
    try putRow(f, "a", 1, "one");
    try f.newMemtable(2);
    var flush_a = (try f.buildFlush()) orelse return error.TestExpectedFlush;
    defer flush_a.changes.deinit(alloc);
    try f.applyFlush(3, flush_a.changes.items, flush_a.memtable_id);
    try putRow(f, "b", 2, "two");
    try f.newMemtable(4);
    var flush_b = (try f.buildFlush()) orelse return error.TestExpectedFlush;
    defer flush_b.changes.deinit(alloc);
    try f.applyFlush(5, flush_b.changes.items, flush_b.memtable_id);

    const tree = f.getTree(1, 0) orelse return error.TestExpectedTree;
    defer tree.release();

    // Both L0 inputs prefetch their first block into the cache.
    var in_ids: [2]u64 = undefined;
    {
        const held = tree.mu.hold(.shared, @src());
        defer held.release();
        const ents = tree.levels[0].entries(held);
        try std.testing.expectEqual(@as(usize, 2), ents.len);
        for (ents, 0..) |e, i| {
            in_ids[i] = e.id;
            const n = try e.table.prefetchBlocks(&cache, alloc, &.{0}, .p0);
            try std.testing.expectEqual(@as(usize, 1), n);
        }
    }
    for (in_ids) |id| {
        const v = cache.get(@as(u128, id) << 64) orelse return error.TestExpectedCached;
        v.decrRef();
    }

    // Pin the inputs the way a reader would, so they outlive the evict
    // and the test can purge their blocks by hand. A freed table purges
    // against the process-wide cache, which no test may install.
    var pinned: [2]*zdb.table.Table = undefined;
    {
        const held = tree.mu.hold(.shared, @src());
        defer held.release();
        for (tree.levels[0].entries(held), 0..) |e, i| {
            e.table.incrRef();
            pinned[i] = e.table;
        }
    }
    defer for (pinned) |t| t.decrRef();

    var comp = try tree.buildCompaction(0, 0);
    defer comp.deinit(alloc);
    try tree.applyCompaction(20, comp.items);

    // The apply unlisted both inputs; the purge evicts them.
    for (in_ids) |id| try std.testing.expect(f.table_cache.contains(id));
    try f.evictTables();
    for (in_ids) |id| try std.testing.expect(!f.table_cache.contains(id));

    // What the last `decrRef` does: drop exactly this table's blocks.
    for (pinned) |t| try std.testing.expectEqual(@as(usize, 1), t.purgeCachedBlocks(&cache));
    for (in_ids) |id|
        try std.testing.expectEqual(@as(?*zdb.cache.Value, null), cache.get(@as(u128, id) << 64));
    try std.testing.expectEqual(@as(usize, 0), cache.count());

    // The output is listed and untouched by a second purge.
    var out_id: u64 = 0;
    for (comp.items) |c| if (c.op == .create) {
        out_id = c.sst_id;
    };
    try f.evictTables();
    try std.testing.expect(f.table_cache.contains(out_id));
}

// ---------------------------------------------------------------------------
// Multi-memtable flush and L0→L0 merges
// ---------------------------------------------------------------------------

/// The newest value of `primary` visible at `read_ts`, checked against
/// `want`.
fn expectRowAt(f: *Forest, primary: String, read_ts: u64, want: String) !void {
    const alloc = std.testing.allocator;
    const got = try f.getRaw(alloc, .{
        .key = .{ .namespace = 1, .table_id = 0, .primary = primary },
        .read_ts = read_ts,
    }) orelse return error.TestExpectedItem;
    defer got.free(alloc);
    try std.testing.expectEqualStrings(want, got.newest().value);
}

test "forest: one flush covers every waiting memtable" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path });

    defer f_live.destroy();

    const f = try Forest.open(alloc, f_live);
    defer f.close();
    _ = try f.createTree(1, 0, .{ .inf = true });

    // Three frozen memtables (ids 1, 2, 3); "a" is written in two of them.
    try f.newMemtable(1);
    try putRow(f, "a", 1, "a1");
    try f.newMemtable(2);
    try putRow(f, "a", 2, "a2");
    try putRow(f, "b", 3, "b1");
    try f.newMemtable(3);
    try putRow(f, "c", 4, "c1");
    try f.newMemtable(4);
    try std.testing.expectEqual(@as(usize, 3), f.memtables.immutableCount());

    var flush = (try f.buildFlush()) orelse return error.TestExpectedFlush;
    defer flush.changes.deinit(alloc);
    // One table for the one tree, named after the newest memtable covered.
    try std.testing.expectEqual(@as(usize, 1), flush.changes.items.len);
    try std.testing.expectEqual(@as(u64, 3), flush.memtable_id);

    try f.applyFlush(5, flush.changes.items, flush.memtable_id);
    // The apply released all three, so nothing is left to flush.
    try std.testing.expectEqual(@as(usize, 0), f.memtables.immutableCount());
    try std.testing.expect((try f.buildFlush()) == null);

    const tree = f.getTree(1, 0) orelse return error.TestExpectedTree;
    defer tree.release();
    {
        const held = tree.mu.hold(.shared, @src());
        defer held.release();
        const ents = tree.levels[0].entries(held);
        try std.testing.expectEqual(@as(usize, 1), ents.len);
        // Both versions of "a", and "b" and "c".
        try std.testing.expectEqual(@as(u64, 4), ents[0].table.index.key_count);
    }
    try expectRowAt(f, "a", std.math.maxInt(u64), "a2");
    try expectRowAt(f, "a", 1, "a1");
    try expectRowAt(f, "b", std.math.maxInt(u64), "b1");
    try expectRowAt(f, "c", std.math.maxInt(u64), "c1");
}

test "forest: an L0 to L0 merge folds small tables into one and keeps L0's read order" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // Four tables to a merge, so three ask for nothing below.
    const f_live = try flags.Live(Config).create(alloc, .{ .dir = dir_path, .l0_merge_min_tables = 4 });
    defer f_live.destroy();
    const f = try Forest.open(alloc, f_live);
    defer f.close();
    _ = try f.createTree(1, 0, .{ .inf = true });

    // Five flushes, one small L0 table each. "k" is rewritten in every
    // one; each flush also brings a key of its own.
    const own = [_]String{ "r1", "r2", "r3", "r4", "r5" };
    const vals = [_]String{ "k1", "k2", "k3", "k4", "k5" };
    var index: u64 = 1;
    try f.newMemtable(index);
    for (own, vals, 0..) |own_key, val, i| {
        try putRow(f, "k", 10 * (i + 1), val);
        try putRow(f, own_key, 10 * (i + 1) + 1, val);
        index += 1;
        try f.newMemtable(index);
        var flush = (try f.buildFlush()) orelse return error.TestExpectedFlush;
        defer flush.changes.deinit(alloc);
        index += 1;
        try f.applyFlush(index, flush.changes.items, flush.memtable_id);
        if (i == 2) {
            // Three small tables: below l0_merge_min_tables, and L0 is
            // within its bounds, so the tree asks for nothing.
            const tree = f.getTree(1, 0) orelse return error.TestExpectedTree;
            defer tree.release();
            try std.testing.expect(tree.compact_candidate == null);
            var none = try tree.compactL0(0);
            defer none.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 0), none.items.len);
        }
    }

    const tree = f.getTree(1, 0) orelse return error.TestExpectedTree;
    defer tree.release();

    // Five small tables: the tree asks for an L0→L0 job.
    const cand = tree.compact_candidate orelse return error.TestExpectedCandidate;
    try std.testing.expectEqual(@as(u8, 0), cand.level);

    var newest_index: u64 = 0;
    {
        const held = tree.mu.hold(.shared, @src());
        defer held.release();
        const ents = tree.levels[0].entries(held);
        try std.testing.expectEqual(@as(usize, 5), ents.len);
        newest_index = ents[ents.len - 1].raft_index;
    }

    var merge = try tree.compactL0(0);
    defer merge.deinit(alloc);
    // Five L0 deletes and ONE L0 create, listed under the newest input's
    // index so it stands where its inputs stood.
    var n_delete: usize = 0;
    var n_create: usize = 0;
    for (merge.items) |c| {
        try std.testing.expectEqual(@as(u32, 0), c.level);
        if (c.op == .delete) n_delete += 1;
        if (c.op == .create) {
            n_create += 1;
            try std.testing.expectEqual(newest_index, c.raft_index);
        }
    }
    try std.testing.expectEqual(@as(usize, 5), n_delete);
    try std.testing.expectEqual(@as(usize, 1), n_create);

    // A flush lands between the build and the apply: newer than every
    // input, so it must keep shadowing the merged table.
    try putRow(f, "k", 100, "k-late");
    index += 1;
    try f.newMemtable(index);
    var late = (try f.buildFlush()) orelse return error.TestExpectedFlush;
    defer late.changes.deinit(alloc);
    index += 1;
    try f.applyFlush(index, late.changes.items, late.memtable_id);

    index += 1;
    try tree.applyCompaction(index, merge.items);
    {
        const held = tree.mu.hold(.shared, @src());
        defer held.release();
        const ents = tree.levels[0].entries(held);
        try std.testing.expectEqual(@as(usize, 2), ents.len);
        // Oldest first: the merged table, then the late flush.
        try std.testing.expectEqual(newest_index, ents[0].raft_index);
        try std.testing.expect(ents[1].raft_index > newest_index);
        // L1 was not touched.
        try std.testing.expectEqual(@as(usize, 0), tree.levels[1].numTables(held));
    }

    // Reads: the late flush wins for "k"; below it, the merged table
    // serves every version the five inputs held.
    try expectRowAt(f, "k", std.math.maxInt(u64), "k-late");
    try expectRowAt(f, "k", 99, "k5");
    try expectRowAt(f, "k", 35, "k3");
    try expectRowAt(f, "k", 10, "k1");
    for (own, vals) |own_key, val|
        try expectRowAt(f, own_key, std.math.maxInt(u64), val);

    // The manifest stored the same index, so a restart rebuilds the
    // same L0 order.
    var merged_id: u64 = 0;
    for (merge.items) |c| if (c.op == .create) {
        merged_id = c.sst_id;
    };
    try std.testing.expectEqual(newest_index, f.manifest.ssts.get(merged_id).?.raft_index);
}
