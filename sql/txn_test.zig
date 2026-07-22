const std = @import("std");
const testing = std.testing;
const fiber = @import("zfiber");
const sql = @import("sql");
const utils = @import("utils");
const zdb = @import("zdb");

const Txn = sql.txn.Txn;
const Oracle = sql.oracle.Oracle;
const Forest = zdb.forest.Forest;
const Key = zdb.kv.Key;
const Meta = zdb.kv.Meta;

const ns: u64 = 16;
const tid: u32 = 16;
const read_ts: u64 = 500;

/// A Forest in a tmpdir with one tree for (ns, tid) and the given
/// committed rows, plus the lock map — everything a Txn needs.
const Fixture = struct {
    tmp: std.testing.TmpDir,
    dir_path: [:0]u8,
    forest: *Forest,
    sm: fiber.SemaphoreMap,
    oracle: Oracle = undefined,

    fn setup(rows: []const zdb.kv.Item) !Fixture {
        var tmp = std.testing.tmpDir(.{});
        errdefer tmp.cleanup();
        const dir_path = try tmp.dir.realPathFileAlloc(
            utils.context.g().io,
            ".",
            testing.allocator,
        );
        errdefer testing.allocator.free(dir_path);

        const f = try Forest.open(testing.allocator, .{ .dir = dir_path });
        errdefer f.close();
        _ = try f.createTree(ns, tid, .{ .inf = true });
        try f.put(rows);

        return .{
            .tmp = tmp,
            .dir_path = dir_path,
            .forest = f,
            .sm = try fiber.SemaphoreMap.init(testing.allocator),
            .oracle = Oracle.init(testing.allocator),
        };
    }

    fn teardown(self: *Fixture) void {
        self.oracle.deinit();
        self.sm.deinit();
        self.forest.close();
        testing.allocator.free(self.dir_path);
        self.tmp.cleanup();
    }

    fn txn(self: *Fixture) Txn {
        return self.txnAs(1);
    }

    fn txnAs(self: *Fixture, id: u64) Txn {
        return Txn.init(
            testing.allocator,
            id,
            ns,
            read_ts,
            1,
            &self.sm,
            &self.oracle,
            self.forest,
        );
    }

    /// Record a commit of `primary` at `ts` in the oracle, the way apply
    /// does — a one-item run through updateItems.
    fn commit(self: *Fixture, primary: []const u8, ts: u64) !void {
        try self.oracle.updateItems(&.{
            .{ .key = rowKey(primary, ts), .value = "", .meta = .{} },
        });
    }
};

fn rowKey(primary: []const u8, ts: u64) Key {
    return .{ .namespace = ns, .table_id = tid, .primary = primary, .timestamp = ts };
}

test "txn: lookup reads pending first, then the forest at the key's ts" {
    var fx = try Fixture.setup(&.{
        .{ .key = rowKey("k1", 100), .value = "old", .meta = .{} },
    });
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    // Nothing pending: the committed row, owned by the caller.
    {
        const found = (try tx.lookup(rowKey("k1", read_ts))) orelse
            return error.TestExpectedRow;
        defer found.release();
        try testing.expect(found.owned);
        try testing.expectEqualSlices(u8, "old", found.item.value);
    }
    // Absent key: null either view.
    try testing.expectEqual(null, try tx.lookup(rowKey("nope", read_ts)));

    // Pending write shadows the committed row at both views; the item
    // borrows from the txn arena (release is a no-op).
    try tx.put(tid, "k1", "new", .{});
    inline for (.{ read_ts, std.math.maxInt(u64) }) |ts| {
        const found = (try tx.lookup(rowKey("k1", ts))) orelse
            return error.TestExpectedRow;
        defer found.release();
        try testing.expect(!found.owned);
        try testing.expectEqualSlices(u8, "new", found.item.value);
        try testing.expectEqual(read_ts, found.item.key.timestamp);
    }
}

test "txn: pending tombstone hides the committed row, no resurrection" {
    var fx = try Fixture.setup(&.{
        .{ .key = rowKey("k1", 100), .value = "old", .meta = .{} },
    });
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    try tx.put(tid, "k1", "", .{ .kind = .deleted });
    // The pending tombstone answers "absent" — the forest fallback must
    // NOT run, or the committed "old" would resurrect.
    try testing.expectEqual(null, try tx.lookup(rowKey("k1", std.math.maxInt(u64))));
}

test "txn: same-key re-put is last-write-wins" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    try tx.put(tid, "k1", "v1", .{});
    try tx.put(tid, "k1", "v2", .{});

    // One version, not two: the commit payload carries the final state.
    const kvs = try tx.wireKvs(testing.allocator);
    defer {
        for (kvs) |kv| testing.allocator.free(kv.key);
        testing.allocator.free(kvs);
    }
    try testing.expectEqual(1, kvs.len);
    try testing.expectEqualSlices(u8, "v2", kvs[0].value);
    try testing.expect(!Meta.fromByte(kvs[0].meta).isDeleted());
}

test "txn: lockRow is re-entrant and deinit releases every held lock" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();

    try tx.lockRow(tid, "r1");
    try tx.lockRow(tid, "r2");
    // Re-lock of a held row: the map answers "already yours", no second
    // hold is recorded.
    try tx.lockRow(tid, "r1");
    try testing.expectEqual(2, tx.held.items.len);
    try testing.expectEqual(2, fx.sm.entries.count());

    // Txn end releases everything — the table drains.
    tx.deinit();
    try testing.expectEqual(0, fx.sm.entries.count());
}

test "txn: fresh lock on a newer-committed row bumps and restarts" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    // The row committed above our snapshot (read_ts 500).
    try fx.commit("r1", 600);
    try testing.expectError(error.Restart, tx.lockRow(tid, "r1"));
    // The bump happened AND the lock is held — a restart re-runs the
    // statement with both intact; the re-lock is a re-entrant no-op
    // with no second bump check.
    try testing.expectEqual(600, tx.read_ts);
    try tx.lockRow(tid, "r1");
    try testing.expectEqual(1, tx.held.items.len);
}

test "txn: bump after a prior statement's scan is a conflict" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    tx.recordScan();
    try tx.beginStatement(); // the scan is now a PRIOR statement's
    try fx.commit("r1", 600);
    try testing.expectError(error.Conflict, tx.lockRow(tid, "r1"));
    try testing.expectEqual(read_ts, tx.read_ts); // no bump happened
}

test "txn: bump revalidates prior point reads" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    // Statement 1 point-reads k1 (absent — still recorded).
    try testing.expectEqual(null, try tx.lookup(rowKey("k1", read_ts)));
    try tx.beginStatement();

    // k1 then commits above our snapshot; a bump for r1 must notice
    // the prior read of k1 went stale → conflict, not a silent bump.
    try fx.commit("k1", 550);
    try fx.commit("r1", 600);
    try testing.expectError(error.Conflict, tx.lockRow(tid, "r1"));
}

test "txn: bump with clean prior reads succeeds" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    // Statement 1 point-reads k2; nothing ever commits to it.
    try testing.expectEqual(null, try tx.lookup(rowKey("k2", read_ts)));
    try tx.beginStatement();

    try fx.commit("r1", 600);
    try testing.expectError(error.Restart, tx.lockRow(tid, "r1"));
    try testing.expectEqual(600, tx.read_ts);
}

test "txn: restartStatement rewinds the statement's read tracking" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    _ = try tx.lookup(rowKey("a", read_ts));
    try tx.beginStatement();
    _ = try tx.lookup(rowKey("b", read_ts));
    _ = try tx.lookup(rowKey("c", read_ts));
    tx.recordScan();
    // 'a' settled into prev at beginStatement; 'b'/'c' are cur.
    try testing.expectEqual(1, tx.prev.reads.items.len);
    try testing.expectEqual(2, tx.cur.reads.items.len);

    tx.restartStatement();
    // Statement-local reads and scan flag are gone; the prior
    // statement's read survives.
    try testing.expectEqual(1, tx.prev.reads.items.len);
    try testing.expectEqual(0, tx.cur.reads.items.len);
    try testing.expect(!tx.cur.ran_scan);
    try testing.expect(!tx.prev.ran_scan);
}

test "txn: wireKvs dedups a key re-put across a bump" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    try tx.put(tid, "k1", "v1", .{});
    tx.read_ts = 600; // as a lockRow bump would
    try tx.put(tid, "k1", "v2", .{});

    // Two pending versions (ts 500 and 600) — the wire payload carries
    // only the txn's final state.
    const kvs = try tx.wireKvs(testing.allocator);
    defer {
        for (kvs) |kv| testing.allocator.free(kv.key);
        testing.allocator.free(kvs);
    }
    try testing.expectEqual(1, kvs.len);
    try testing.expectEqualSlices(u8, "v2", kvs[0].value);
    try testing.expectEqual(600, Key.init(kvs[0].key).timestamp);
}

test "txn: lockTable coexists with row locks and is re-entrant" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();

    // Shared intent + a row lock in the same table: distinct entries
    // (TableKey and Key identity hashes use different seeds).
    try tx.lockTable(tid, .shared);
    try tx.lockRow(tid, "r1");
    try testing.expectEqual(2, tx.held.items.len);
    try testing.expectEqual(2, fx.sm.entries.count());

    // Per-statement re-acquire inside one txn is a no-op.
    try tx.lockTable(tid, .shared);
    try testing.expectEqual(2, tx.held.items.len);

    // A second txn shares the intent lock but blocks... deadlock and
    // blocking behavior is the map's own test surface; here just check
    // shared coexistence.
    var tx2 = fx.txnAs(2);
    try tx2.lockTable(tid, .shared);
    tx2.deinit();

    tx.deinit();
    try testing.expectEqual(0, fx.sm.entries.count());
}

test "txn: wireKvs emits key order regardless of insert order" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    // Out of order, across two tables; empty txn emits nothing first.
    try testing.expectEqual(0, (try tx.wireKvs(testing.allocator)).len);
    try tx.put(tid + 1, "a", "t17-a", .{});
    try tx.put(tid, "c", "vc", .{});
    try tx.put(tid, "a", "va", .{});
    try tx.put(tid, "b", "vb", .{});

    const kvs = try tx.wireKvs(testing.allocator);
    defer {
        for (kvs) |kv| testing.allocator.free(kv.key);
        testing.allocator.free(kvs);
    }
    try testing.expectEqual(4, kvs.len);
    // (tid, "a") < (tid, "b") < (tid, "c") < (tid+1, "a"): table_id
    // orders before primary, and every wire key carries read_ts.
    const expect_primary = [_][]const u8{ "a", "b", "c", "a" };
    const expect_tid = [_]u32{ tid, tid, tid, tid + 1 };
    for (kvs, expect_primary, expect_tid) |kv, p, t| {
        const k = Key.init(kv.key);
        try testing.expectEqual(ns, k.namespace);
        try testing.expectEqual(t, k.table_id);
        try testing.expectEqualSlices(u8, p, k.primary);
        try testing.expectEqual(read_ts, k.timestamp);
    }
}
