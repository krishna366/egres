const std = @import("std");
const testing = std.testing;
const sql = @import("sql");
const String = sql.value.String;
const zdb = @import("zdb");

const Oracle = sql.oracle.Oracle;

// Test keys share one (ns, table); `h(p)` is the identity hash that
// `updateItems` records for a commit of `p`.
fn key(primary: String) zdb.kv.Key {
    return .{ .namespace = 7, .table_id = 16, .primary = primary };
}

/// Record a commit of `primary` at `ts`, the way apply does — a
/// one-item, one-version run through updateItems.
fn commit(o: *Oracle, primary: String, ts: u64) !void {
    try o.updateItems(&.{.{
        .key = key(primary),
        .versions = .{ .one = .{ .ts = ts, .meta = .{}, .value = "" } },
    }});
}

fn h(primary: String) u64 {
    return key(primary).identityHash();
}

test "oracle: unknown keys answer the horizon; updates answer exactly" {
    var o = try Oracle.init(testing.allocator, 1024);
    defer o.deinit();

    // Empty oracle: nothing ever committed — horizon 0 for any key.
    try testing.expectEqual(0, o.commitTs(h("a")));

    try commit(&o, "a", 100);
    try testing.expectEqual(100, o.commitTs(h("a")));
    try testing.expectEqual(0, o.commitTs(h("b")));

    // Per-key max-fold: an older ts never regresses a newer one.
    try commit(&o, "a", 90);
    try testing.expectEqual(100, o.commitTs(h("a")));
    try commit(&o, "a", 150);
    try testing.expectEqual(150, o.commitTs(h("a")));
}

test "oracle: a tick within the period is skipped" {
    var o = try Oracle.init(testing.allocator, 1024);
    defer o.deinit();

    // init stamps the clock, so a period-long tick is too soon and
    // the entry stays in `current`; a forced rotation moves it.
    try commit(&o, "a", 100);
    o.rotate(std.math.maxInt(u64));
    try testing.expectEqual(1, o.current.map.count());
    o.rotate(0);
    try testing.expectEqual(0, o.current.map.count());
    try testing.expectEqual(1, o.previous.map.count());
}

test "oracle: entries survive one rotation and die at the second" {
    var o = try Oracle.init(testing.allocator, 1024);
    defer o.deinit();

    try commit(&o, "a", 100);
    o.rotate(0); // h("a") now in `previous`
    try testing.expectEqual(100, o.commitTs(h("a")));

    o.rotate(0); // dropped; the horizon absorbs its max
    try testing.expectEqual(100, o.horizon_ts);
    // Absent keys — h("a") included — answer the horizon now:
    // conservative, but the old commit can never hide.
    try testing.expectEqual(100, o.commitTs(h("a")));
    try testing.expectEqual(100, o.commitTs(h("b")));
}

test "oracle: a full generation rotates by itself, maps keep capacity" {
    // Three keys per generation. The fourth commit ends the first
    // generation, the seventh the second; nothing is ever freed.
    var o = try Oracle.init(testing.allocator, 3);
    defer o.deinit();
    const cap = o.current.map.capacity();
    try testing.expect(cap >= 3);

    try commit(&o, "a", 10);
    try commit(&o, "b", 11);
    try commit(&o, "c", 12);
    try commit(&o, "d", 13); // rotation: a, b, c in previous
    try testing.expectEqual(1, o.current.map.count());
    try testing.expectEqual(3, o.previous.map.count());
    try testing.expectEqual(10, o.commitTs(h("a")));
    try testing.expectEqual(0, o.horizon_ts);

    try commit(&o, "e", 14);
    try commit(&o, "f", 15);
    try commit(&o, "g", 16); // rotation: a, b, c retired into the horizon
    try testing.expectEqual(12, o.horizon_ts);
    try testing.expectEqual(12, o.commitTs(h("a")));
    try testing.expectEqual(13, o.commitTs(h("d")));
    try testing.expectEqual(16, o.commitTs(h("g")));
    try testing.expectEqual(cap, o.current.map.capacity());
    try testing.expectEqual(cap, o.previous.map.capacity());
}

test "oracle: a re-committed key reads from current, not previous" {
    var o = try Oracle.init(testing.allocator, 1024);
    defer o.deinit();

    try commit(&o, "a", 100);
    o.rotate(0);
    try commit(&o, "a", 150); // lives in both generations now
    try testing.expectEqual(150, o.commitTs(h("a")));

    o.rotate(0); // drops the generation holding the 100 copy
    try testing.expectEqual(100, o.horizon_ts);
    try testing.expectEqual(150, o.commitTs(h("a")));
    o.rotate(0); // drops the 150 copy too
    try testing.expectEqual(150, o.horizon_ts);
}

test "oracle: maxCommitTs over a read set" {
    var o = try Oracle.init(testing.allocator, 1024);
    defer o.deinit();

    // One batched apply records the whole write set under a single hold.
    try o.updateItems(&.{
        .{ .key = key("k1"), .versions = .{
            .one = .{ .ts = 100, .meta = .{}, .value = "" },
        } },
        .{ .key = key("k2"), .versions = .{
            .one = .{ .ts = 300, .meta = .{}, .value = "" },
        } },
        .{ .key = key("k3"), .versions = .{
            .one = .{ .ts = 200, .meta = .{}, .value = "" },
        } },
    });

    try testing.expectEqual(0, o.maxCommitTs(&.{}));
    try testing.expectEqual(300, o.maxCommitTs(&.{ h("k1"), h("k2"), h("k3") }));
    try testing.expectEqual(200, o.maxCommitTs(&.{ h("k1"), h("k3") }));
    // An absent key contributes the horizon (0 here).
    try testing.expectEqual(100, o.maxCommitTs(&.{ h("k1"), h("k99") }));

    // Once every key has aged out, the horizon speaks for them all.
    o.rotate(0);
    o.rotate(0);
    try testing.expectEqual(300, o.horizon_ts);
    try testing.expectEqual(300, o.maxCommitTs(&.{ h("k1"), h("k99") }));
}
