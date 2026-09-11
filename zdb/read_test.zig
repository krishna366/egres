//! Tests for the batch read path (`Forest.multiGet`, read.zig).

const std = @import("std");
const utils = @import("utils");
const zdb = @import("zdb");
const vfs = zdb.vfs;
const zroar = zdb.zroar;

const Key = zdb.kv.Key;
const Item = zdb.kv.Item;
const Meta = zdb.kv.Meta;
const Forest = zdb.forest.Forest;
const Builder = zdb.table.builder.Builder;
const manifest = zdb.manifest;

const ns: u64 = 1;
const tid: u32 = 0;

fn tmpPath(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir) ![:0]u8 {
    return tmp.dir.realPathFileAlloc(utils.context.g().io, ".", alloc);
}

/// A forest with one table (`ns`, `tid`) and a live memtable, plus the
/// moves that put rows where a test wants them: frozen into an
/// immutable memtable, flushed to L0, compacted to L1.
const Fixture = struct {
    alloc: std.mem.Allocator,
    tmp: std.testing.TmpDir,
    dir: [:0]u8,
    f: *Forest,
    tree_id: u64,
    next_memtable: u64,
    lease_ctx: u8 = 0,

    const Lease = struct {
        fn call(_: *anyopaque, _: u64) anyerror!void {}
    };

    /// On the fixture's final address: the lease callback keeps a
    /// pointer into it.
    fn setup(self: *Fixture, alloc: std.mem.Allocator) !void {
        self.* = .{
            .alloc = alloc,
            .tmp = std.testing.tmpDir(.{}),
            .dir = undefined,
            .f = undefined,
            .tree_id = undefined,
            .next_memtable = 2,
        };
        self.dir = try tmpPath(alloc, &self.tmp);
        self.f = try Forest.open(alloc, .{ .dir = self.dir });
        self.tree_id = try self.f.createTree(ns, tid, .{ .inf = true });
        self.f.file_ids.bind(1, 0, @ptrCast(&self.lease_ctx), Lease.call);
        // Forest.open leaves an id-0 placeholder memtable that a swap
        // discards rather than freezes; start on a real one.
        try self.f.newMemtable(1, 1);
    }

    fn teardown(self: *Fixture) void {
        self.f.close();
        self.alloc.free(self.dir);
        self.tmp.cleanup();
    }

    fn key(primary: []const u8, ts: u64) Key {
        return .{
            .namespace = ns,
            .table_id = tid,
            .primary = primary,
            .timestamp = ts,
        };
    }

    fn put(
        self: *Fixture,
        primary: []const u8,
        ts: u64,
        value: []const u8,
    ) !void {
        try self.putMeta(primary, ts, value, .{});
    }

    fn putMeta(
        self: *Fixture,
        primary: []const u8,
        ts: u64,
        value: []const u8,
        meta: Meta,
    ) !void {
        const items = [_]Item{.{
            .key = key(primary, ts),
            .value = value,
            .meta = meta,
        }};
        try self.f.put(&items);
    }

    /// Freeze the current memtable into an immutable one; nothing is
    /// flushed.
    fn freeze(self: *Fixture) !void {
        const id = self.next_memtable;
        self.next_memtable += 1;
        try self.f.newMemtable(id, id);
    }

    /// Freeze and flush to an L0 SST.
    fn flush(self: *Fixture) !void {
        try self.freeze();
        var fl = (try self.f.buildFlush()) orelse
            return error.TestExpectedFlush;
        defer fl.changes.deinit(self.alloc);
        try self.f.applyFlush(fl.changes.items, fl.memtable_id);
    }

    /// Compact every L0 table into L1.
    fn compactL0(self: *Fixture) !void {
        const tree = self.f.getTree(ns, self.tree_id) orelse
            return error.TestExpectedTree;
        defer tree.release();
        var comp = try tree.buildCompaction(0, &self.f.file_ids, 0);
        defer comp.deinit(self.alloc);
        try std.testing.expect(comp.items.len > 0);
        try tree.applyCompaction(comp.items);
    }
};

/// `mg.item(key)` against `Forest.get` of the same key at `read_ts`,
/// and against the expected value and timestamp (null = absent).
fn expectRow(
    fx: *Fixture,
    mg: *const Forest.MultiGet,
    read_ts: u64,
    primary: []const u8,
    want: ?struct { value: []const u8, ts: u64 },
) !void {
    const got = mg.item(primary);
    const single = try fx.f.get(fx.alloc, Fixture.key(primary, read_ts), .p0);
    defer if (single) |s| s.free(fx.alloc);
    if (want) |w| {
        const it = got orelse return error.TestExpectedItem;
        try std.testing.expectEqualStrings(primary, it.key.primary);
        try std.testing.expectEqualStrings(w.value, it.value);
        try std.testing.expectEqual(w.ts, it.key.timestamp);
        const s = single orelse return error.TestExpectedItem;
        try std.testing.expectEqualStrings(w.value, s.value);
        try std.testing.expectEqual(w.ts, s.key.timestamp);
    } else {
        try std.testing.expect(got == null);
        try std.testing.expect(single == null);
    }
}

test "multiget: rows resolve across pending writes, memtables, L0 and L1" {
    const alloc = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.setup(alloc);
    defer fx.teardown();

    // k01..k10 at ts 10, down in L1.
    const base = [_][]const u8{
        "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10",
    };
    for (base) |k| try fx.put(k, 10, "v1");
    try fx.flush();
    try fx.compactL0();

    // Two overlapping L0 tables: k03 and k04 at ts 20, then k04 again
    // at ts 30 in the newer one.
    try fx.put("k03", 20, "v2");
    try fx.put("k04", 20, "v2");
    try fx.flush();
    try fx.put("k04", 30, "v3");
    try fx.flush();

    // An immutable memtable (k05), then the current one (k06 and a
    // committed tombstone for k02).
    try fx.put("k05", 40, "v4");
    try fx.freeze();
    try fx.put("k06", 50, "v5");
    try fx.putMeta("k02", 60, "", .{ .kind = .deleted });

    // The txn's own writes at read_ts: a row and a tombstone over a
    // committed row.
    const read_ts: u64 = 100;
    const pending = try zdb.skl.SkipList.initCapacity(alloc, 0);
    defer pending.release();
    try pending.putItem(.{
        .key = Fixture.key("k07", read_ts),
        .value = "pend",
        .meta = .{},
    });
    try pending.putItem(.{
        .key = Fixture.key("k08", read_ts),
        .value = "",
        .meta = .{ .kind = .deleted },
    });

    // Unsorted, with a duplicate and an absent key.
    const keys = [_][]const u8{
        "k09", "k03", "k01", "k04", "k05", "k06", "k07", "k08", "k02", "k99",
        "k03",
    };
    var mg = try fx.f.multiGet(alloc, ns, tid, &keys, .{
        .read_ts = read_ts,
        .pending = pending,
    });
    defer mg.deinit();
    try std.testing.expectEqual(@as(usize, 10), mg.nodes.len);
    try std.testing.expectEqual(@as(usize, 1), mg.pending.items.len);

    try expectRow(&fx, &mg, read_ts, "k01", .{ .value = "v1", .ts = 10 });
    try expectRow(&fx, &mg, read_ts, "k02", null);
    try expectRow(&fx, &mg, read_ts, "k03", .{ .value = "v2", .ts = 20 });
    try expectRow(&fx, &mg, read_ts, "k04", .{ .value = "v3", .ts = 30 });
    try expectRow(&fx, &mg, read_ts, "k05", .{ .value = "v4", .ts = 40 });
    try expectRow(&fx, &mg, read_ts, "k06", .{ .value = "v5", .ts = 50 });
    try std.testing.expectEqualStrings("pend", mg.item("k07").?.value);
    try std.testing.expect(mg.item("k08") == null);
    try expectRow(&fx, &mg, read_ts, "k09", .{ .value = "v1", .ts = 10 });
    try expectRow(&fx, &mg, read_ts, "k99", null);

    // include_deleted hands the tombstone back.
    var raw = try fx.f.multiGet(alloc, ns, tid, &.{"k02"}, .{
        .read_ts = read_ts,
        .include_deleted = true,
    });
    defer raw.deinit();
    const tomb = raw.item("k02") orelse return error.TestExpectedItem;
    try std.testing.expect(tomb.meta.isDeleted());
    try std.testing.expectEqual(@as(u64, 60), tomb.key.timestamp);
}

/// A key of table 7, the neighbour of `tid` in the namespace-wide
/// memtable.
fn otherKey(primary: []const u8, ts: u64) Key {
    return .{
        .namespace = ns,
        .table_id = 7,
        .primary = primary,
        .timestamp = ts,
    };
}

test "multiget: other tables in the memtables and pending writes are skipped" {
    const alloc = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.setup(alloc);
    defer fx.teardown();

    // The memtable spans the namespace: the same primary in table 7
    // sits next to table 0's rows.
    _ = try fx.f.createTree(ns, 7, .{ .inf = true });
    try fx.put("a", 10, "a-t0");
    try fx.put("z", 1, "z-ts1");
    const other = [_]Item{
        .{ .key = otherKey("a", 20), .value = "a-t7", .meta = .{} },
        .{ .key = otherKey("b", 20), .value = "b-t7", .meta = .{} },
    };
    try fx.f.put(&other);

    // A txn's pending writes to table 7 while table 0 is probed: the
    // seek window must not let the merge land on them.
    const pending = try zdb.skl.SkipList.initCapacity(alloc, 0);
    defer pending.release();
    try pending.putItem(.{
        .key = otherKey("b", 100),
        .value = "pend-t7",
        .meta = .{},
    });

    var mg = try fx.f.multiGet(alloc, ns, tid, &.{ "a", "b", "z" }, .{
        .read_ts = 100,
        .pending = pending,
    });
    defer mg.deinit();
    try std.testing.expectEqualStrings("a-t0", mg.item("a").?.value);
    try std.testing.expect(mg.item("b") == null);
    try std.testing.expectEqualStrings("z-ts1", mg.item("z").?.value);
}

test "multiget: read_ts hides the versions above it" {
    const alloc = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.setup(alloc);
    defer fx.teardown();

    try fx.put("a", 10, "a10");
    try fx.put("b", 10, "b10");
    try fx.flush();
    try fx.put("a", 30, "a30");
    try fx.putMeta("b", 30, "", .{ .kind = .deleted });
    try fx.put("c", 30, "c30");

    var mg = try fx.f.multiGet(alloc, ns, tid, &.{ "a", "b", "c" }, .{
        .read_ts = 20,
    });
    defer mg.deinit();
    try expectRow(&fx, &mg, 20, "a", .{ .value = "a10", .ts = 10 });
    try expectRow(&fx, &mg, 20, "b", .{ .value = "b10", .ts = 10 });
    try expectRow(&fx, &mg, 20, "c", null);

    var now = try fx.f.multiGet(alloc, ns, tid, &.{ "a", "b", "c" }, .{});
    defer now.deinit();
    const latest = std.math.maxInt(u64);
    try expectRow(&fx, &now, latest, "a", .{ .value = "a30", .ts = 30 });
    try expectRow(&fx, &now, latest, "b", null);
    try expectRow(&fx, &now, latest, "c", .{ .value = "c30", .ts = 30 });
}

test "multiget: rejects a read_ts below globalMinRead" {
    const alloc = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.setup(alloc);
    defer fx.teardown();

    _ = fx.f.setGlobalMinRead(50);
    try std.testing.expectError(
        error.ReadBelowMinRead,
        fx.f.multiGet(alloc, ns, tid, &.{"a"}, .{ .read_ts = 10 }),
    );
}

fn writeOneKeyTable(
    alloc: std.mem.Allocator,
    dir: []const u8,
    file_id: u64,
    k: Key,
    value: []const u8,
) !void {
    const path = try zdb.file_ids.sstPath(alloc, dir, file_id);
    defer alloc.free(path);
    var bld = Builder.init(alloc, .{ .target_block_size = 4 * 1024 });
    defer bld.deinit();
    try bld.add(k, value);
    var tb = try bld.finish();
    defer tb.deinit();
    try vfs.writeFile(path, tb.bytes);
}

test "multiget: keys across two trees and several bottom tables" {
    const alloc = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.setup(alloc);
    defer fx.teardown();

    // Ten one-key L3 SSTs, value = key, then a split into two trees.
    const keys = [_][]const u8{
        "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10",
    };
    for (keys, 1..) |k, file_id| {
        try writeOneKeyTable(alloc, fx.dir, file_id, Fixture.key(k, 1), k);
    }
    {
        const tree = fx.f.getTree(ns, fx.tree_id) orelse
            return error.TestExpectedTree;
        defer tree.release();
        var changes: [keys.len]manifest.SSTableChange = undefined;
        for (&changes, 1..) |*c, file_id| {
            c.* = manifest.createSSTableChange(
                @intCast(file_id),
                ns,
                fx.tree_id,
                3,
            );
        }
        try tree.applyCompaction(&changes);
    }
    try fx.f.splitTreeForTests(ns, fx.tree_id);

    const asked = [_][]const u8{
        "k10", "k01", "k05", "k06", "k00", "k11", "k03",
    };
    var mg = try fx.f.multiGet(alloc, ns, tid, &asked, .{});
    defer mg.deinit();
    for (asked) |k| {
        if (std.mem.eql(u8, k, "k00") or std.mem.eql(u8, k, "k11")) {
            try std.testing.expect(mg.item(k) == null);
            continue;
        }
        const it = mg.item(k) orelse return error.TestExpectedItem;
        try std.testing.expectEqualStrings(k, it.value);
    }
}

test "multiget: prefetch reads the blocks the seeks land in, in one batch" {
    const alloc = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.setup(alloc);
    defer fx.teardown();

    // 300 rows of 1 KiB: one L0 table of several 64 KiB blocks.
    const wide = try alloc.alloc(u8, 1024);
    defer alloc.free(wide);
    @memset(wide, 'w');
    var name: [4]u8 = undefined;
    for (0..300) |i| {
        _ = try std.fmt.bufPrint(&name, "r{d:0>3}", .{i});
        try fx.put(&name, 10, wide);
    }
    try fx.flush();

    const tree = fx.f.getTree(ns, fx.tree_id) orelse
        return error.TestExpectedTree;
    defer tree.release();
    const tbl = blk: {
        const held = tree.mu.hold(.shared, @src());
        defer held.release();
        const ents = tree.levels[0].entries(held);
        try std.testing.expectEqual(@as(usize, 1), ents.len);
        ents[0].table.incrRef();
        break :blk ents[0].table;
    };
    defer tbl.decrRef();
    const nblocks = tbl.blockCount();
    try std.testing.expect(nblocks >= 3);

    // blockFor names the block a seek opens.
    for ([_][]const u8{ "r000", "r150", "r299" }) |k| {
        var it = zdb.table.TableIterator.init(alloc, tbl, .p0);
        defer it.deinit();
        try it.seekRange(Fixture.key(k, 10), .{});
        try std.testing.expect(it.itemRef() != null);
        const want = tbl.blockFor(Fixture.key(k, 10)).?;
        try std.testing.expectEqual(want, it.bpos);
    }

    // Every block in one batch into a private cache; a second call
    // finds them all cached.
    var cache = try zdb.cache.ShardedCache(u128).init(alloc, 64 << 20, 2);
    defer cache.deinit();
    const idxs = try alloc.alloc(usize, nblocks);
    defer alloc.free(idxs);
    for (idxs, 0..) |*idx, i| idx.* = i;
    const read = try tbl.prefetchBlocks(&cache, alloc, idxs, .p0);
    try std.testing.expectEqual(nblocks, read);
    try std.testing.expectEqual(nblocks, cache.count());
    const misses = cache.misses.load(.monotonic);
    const again = try tbl.prefetchBlocks(&cache, alloc, idxs, .p0);
    try std.testing.expectEqual(@as(usize, 0), again);
    try std.testing.expectEqual(misses, cache.misses.load(.monotonic));
    try std.testing.expectEqual(@as(u64, nblocks), cache.hits.load(.monotonic));

    // A cache too small to admit the whole batch takes a prefix of it
    // and evicts nothing: every block read is still there.
    var small = try zdb.cache.ShardedCache(u128).init(alloc, 4 << 20, 2);
    defer small.deinit();
    const part = try tbl.prefetchBlocks(&small, alloc, idxs, .p0);
    try std.testing.expect(part >= 1 and part < nblocks);
    try std.testing.expectEqual(part, small.count());

    // A cached block decodes: block 0 starts at the first row.
    const key0: u128 = @as(u128, tbl.file_id) << 64;
    const val = cache.get(key0) orelse return error.TestExpectedBlock;
    defer val.decrRef();
    var bi = try zdb.table.BlockIterator.init(
        alloc,
        val.data,
        try tbl.blockBaseKey(0),
    );
    defer bi.deinit();
    try bi.seekToFirst();
    const first = bi.itemRef() orelse return error.TestExpectedItem;
    try std.testing.expectEqualStrings("r000", first.key.primary);
}

const roar = struct {
    const delta_meta: Meta = .{ .kind = .delta, .encoding = .roaring };
    const state_meta: Meta = .{ .kind = .state, .encoding = .roaring };
    const tomb_meta: Meta = .{ .kind = .deleted, .encoding = .roaring };

    fn putDelta(
        fx: *Fixture,
        primary: []const u8,
        ts: u64,
        adds: []const u64,
        rems: []const u64,
    ) !void {
        var list: std.ArrayList(u8) = .empty;
        defer list.deinit(fx.alloc);
        var w = try zdb.buffers.RoaringDeltaWriter.initWithBuf(fx.alloc, &list);
        if (adds.len > 0) try w.setAdds(adds);
        if (rems.len > 0) try w.setRemoves(rems);
        _ = try w.finish();
        try fx.putMeta(primary, ts, list.items, delta_meta);
    }

    fn putState(
        fx: *Fixture,
        primary: []const u8,
        ts: u64,
        vals: []const u64,
    ) !void {
        var bm = try zroar.Bitmap.fromSortedList(fx.alloc, vals);
        defer bm.deinit();
        try fx.putMeta(primary, ts, bm.toBuffer(), state_meta);
    }

    /// `mg.bitmap(key)` against `Forest.getBitmap` and the expected set.
    fn expectBitmap(
        fx: *Fixture,
        mg: *const Forest.MultiGet,
        read_ts: u64,
        primary: []const u8,
        expected: []const u64,
    ) !void {
        var bm = try mg.bitmap(fx.alloc, primary);
        defer bm.deinit();
        var single = try fx.f.getBitmap(
            fx.alloc,
            Fixture.key(primary, read_ts),
            .p0,
        );
        defer single.deinit();
        try std.testing.expectEqual(expected.len, bm.getCardinality());
        try std.testing.expectEqual(expected.len, single.getCardinality());
        for (expected) |v| {
            try std.testing.expect(bm.contains(v));
            try std.testing.expect(single.contains(v));
        }
    }
};

test "multiget: postings fold across memtable, L0, tombstones and history" {
    const alloc = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.setup(alloc);
    defer fx.teardown();

    // p: a state in L0, deltas above it in the memtable.
    // q: a state, then a tombstone.
    // r: deltas only, no terminal anywhere.
    // s: absent.
    try roar.putState(&fx, "p", 10, &.{ 1, 2, 3 });
    try roar.putState(&fx, "q", 10, &.{ 7, 8 });
    try roar.putDelta(&fx, "r", 10, &.{ 1, 2 }, &.{});
    try fx.flush();
    try roar.putDelta(&fx, "p", 20, &.{ 40, 50 }, &.{2});
    try roar.putDelta(&fx, "p", 30, &.{2}, &.{ 1, 40 });
    try fx.putMeta("q", 15, "", roar.tomb_meta);
    try roar.putDelta(&fx, "r", 20, &.{3}, &.{1});

    const latest = std.math.maxInt(u64);
    var mg = try fx.f.multiGet(alloc, ns, tid, &.{ "s", "r", "q", "p" }, .{});
    defer mg.deinit();
    try roar.expectBitmap(&fx, &mg, latest, "p", &.{ 2, 3, 50 });
    try roar.expectBitmap(&fx, &mg, latest, "q", &.{});
    try roar.expectBitmap(&fx, &mg, latest, "r", &.{ 2, 3 });
    try roar.expectBitmap(&fx, &mg, latest, "s", &.{});

    // History: p before its second delta, q before its tombstone.
    var old = try fx.f.multiGet(alloc, ns, tid, &.{ "p", "q" }, .{
        .read_ts = 12,
    });
    defer old.deinit();
    try roar.expectBitmap(&fx, &old, 12, "p", &.{ 1, 2, 3 });
    try roar.expectBitmap(&fx, &old, 12, "q", &.{ 7, 8 });

    // A row is read like any key; only its bitmap is refused.
    try fx.put("t", 10, "row");
    var row = try fx.f.multiGet(alloc, ns, tid, &.{"t"}, .{});
    defer row.deinit();
    try std.testing.expectEqualStrings("row", row.item("t").?.value);
    try std.testing.expectError(error.NotRoaring, row.bitmap(alloc, "t"));
}
