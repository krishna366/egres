const std = @import("std");
const zdb = @import("zdb");

const compaction = zdb.compaction;
const TableEntry = compaction.TableEntry;
const Config = zdb.config.Config;

const kib: u64 = 1024;
const mib: u64 = 1024 * 1024;
const gib: u64 = 1024 * mib;

/// L0 entries of the given sizes, oldest first, each with its own id
/// (100, 101, ...) and raft_index (10, 20, ...). The picker reads sizes
/// and indexes only, so no table stands behind them.
fn tablesOf(comptime sizes: []const u64) [sizes.len]TableEntry {
    var out: [sizes.len]TableEntry = undefined;
    for (sizes, 0..) |sz, i| out[i] = .{
        .id = 100 + i,
        .raft_index = (i + 1) * 10,
        .size = sz,
        .table = undefined,
    };
    return out;
}

/// The run is exactly the entries with these ids, in this order.
fn expectIds(run: []const TableEntry, want: []const u64) !void {
    try std.testing.expectEqual(want.len, run.len);
    for (want, run) |id, e| try std.testing.expectEqual(id, e.id);
}

const no_run: ?[]const TableEntry = null;

/// The merge run `pickL0Job` picks, null when it picks a push or nothing.
fn mergeRun(opts: Config, tables: []const TableEntry) ?[]const TableEntry {
    const job = compaction.pickL0Job(opts, tables) orelse return null;
    return switch (job) {
        .merge => |run| run,
        .push => null,
    };
}

test "pickL0Job merge: needs l0_merge_min_tables unfinished tables" {
    const opts: Config = .{ .l0_merge_min_tables = 4 };
    const three = tablesOf(&.{ 30 * kib, 30 * kib, 30 * kib });
    try std.testing.expectEqual(no_run, mergeRun(opts, &three));

    const four = tablesOf(&.{ 30 * kib, 30 * kib, 30 * kib, 30 * kib });
    const run = mergeRun(opts, &four) orelse return error.TestExpectedRun;
    try expectIds(run, &.{ 100, 101, 102, 103 });
}

test "pickL0Job merge: an unfinished earlier output is merged again with the flushes after it" {
    const opts: Config = .{ .l0_merge_min_tables = 4 };
    // Oldest first: an earlier merge's 3 MiB output, then four flushes.
    const tables = tablesOf(&.{ 3 * mib, 30 * kib, 50 * kib, 20 * kib, 45 * kib });
    const run = mergeRun(opts, &tables) orelse return error.TestExpectedRun;
    try expectIds(run, &.{ 100, 101, 102, 103, 104 });
}

test "pickL0Job merge: the run starts after the newest finished table" {
    const opts: Config = .{ .l0_merge_min_tables = 4 };
    try std.testing.expectEqual(@as(usize, 64 * mib), opts.memtable_bytes);

    // A finished table between small ones: only the newer ones are
    // taken, whatever sits before it.
    const split = tablesOf(&.{ 30 * kib, 30 * kib, 64 * mib, 30 * kib, 30 * kib, 30 * kib, 30 * kib });
    const newer = mergeRun(opts, &split) orelse return error.TestExpectedRun;
    try expectIds(newer, &.{ 103, 104, 105, 106 });

    // Too few after it: nothing, although there are enough before it.
    const short = tablesOf(&.{ 30 * kib, 30 * kib, 30 * kib, 64 * mib, 30 * kib, 30 * kib, 30 * kib });
    try std.testing.expectEqual(no_run, mergeRun(opts, &short));

    // Finished tables only: nothing to merge.
    const done = tablesOf(&.{ 64 * mib, 70 * mib, 64 * mib, 80 * mib, 64 * mib });
    try std.testing.expectEqual(no_run, mergeRun(opts, &done));

    // The output itself may pass the L0 file size: four 20 MiB tables
    // make one 80 MiB table. The 80 MiB output of an earlier merge, next
    // to them, stays out.
    const tiers = tablesOf(&.{ 80 * mib, 20 * mib, 20 * mib, 20 * mib, 20 * mib });
    const run = mergeRun(opts, &tiers) orelse return error.TestExpectedRun;
    try expectIds(run, &.{ 101, 102, 103, 104 });
}

test "pickL0Job merge: the run stops once it holds twice the L0 file size" {
    const opts: Config = .{ .l0_merge_min_tables = 4 };
    // Eight 20 MiB tables: the seventh brings the run to 140 MiB, past
    // 128 MiB, and ends it; the eighth waits for the next merge.
    const bulk = tablesOf(&(.{20 * mib} ** 8));
    const run = mergeRun(opts, &bulk) orelse return error.TestExpectedRun;
    try expectIds(run, &.{ 100, 101, 102, 103, 104, 105, 106 });
}

test "pickL0Job merge: 0 turns L0 to L0 compaction off" {
    const opts: Config = .{ .l0_merge_min_tables = 0 };
    const tables = tablesOf(&.{ 30 * kib, 30 * kib, 30 * kib, 30 * kib, 30 * kib });
    try std.testing.expectEqual(no_run, mergeRun(opts, &tables));
}

test "pickL0Job: push once L0 holds L1's target, else merge, else wait" {
    // A 4 GiB Lmax puts L1's target at 164 MiB.
    const opts: Config = .{ .l0_merge_min_tables = 4, .lmax_target_bytes = 4 * gib };
    try std.testing.expectEqual(@as(i64, 4 * gib / 25), compaction.levelTarget(opts, 1));

    // Nothing in L0.
    try std.testing.expectEqual(@as(?compaction.L0Job, null), compaction.pickL0Job(opts, &.{}));

    // Below L1's target with too few tables to merge: L0 is left to fill.
    const few = tablesOf(&.{ 40 * mib, 40 * mib, 40 * mib });
    try std.testing.expectEqual(@as(?compaction.L0Job, null), compaction.pickL0Job(opts, &few));

    // Small tables merge, however many there are: no table count pushes.
    const small = tablesOf(&(.{30 * kib} ** 40));
    try std.testing.expect(compaction.pickL0Job(opts, &small).? == .merge);

    // Too few unfinished tables after the newest finished one, below
    // L1's target: left alone, although there are four tables.
    const waiting = tablesOf(&.{ 64 * mib, 40 * mib, 10 * mib, 10 * mib });
    try std.testing.expectEqual(no_run, mergeRun(opts, &waiting));
    try std.testing.expectEqual(@as(?compaction.L0Job, null), compaction.pickL0Job(opts, &waiting));

    // L1's target reached: push, even though six unfinished tables
    // would merge. Merging them first would write them twice.
    const full = tablesOf(&(.{30 * mib} ** 6));
    try std.testing.expect(compaction.pickL0Job(opts, &full).? == .push);
    // Three finished tables do it too.
    const done = tablesOf(&.{ 64 * mib, 64 * mib, 64 * mib });
    try std.testing.expect(compaction.pickL0Job(opts, &done).? == .push);
}

test "config: a merge needs at least two tables" {
    const ok: Config = .{};
    try ok.validate();
    const off: Config = .{ .l0_merge_min_tables = 0 };
    try off.validate();
    const one: Config = .{ .l0_merge_min_tables = 1 };
    try std.testing.expectError(error.InvalidL0MergeMinTables, one.validate());
}

test "config: the backpressure start count cannot be zero" {
    const zero: Config = .{ .l0_backpressure_start_count = 0 };
    try std.testing.expectError(error.InvalidL0BackpressureStartCount, zero.validate());
}

test "levelTarget: fixed targets cascade up from the Lmax target" {
    const opts: Config = .{};
    const lmax: i64 = opts.lmax_target_bytes;
    try std.testing.expectEqual(@as(i64, 10 * gib), lmax);
    // The split point is a tree with every level at target.
    try std.testing.expectEqual(@as(i64, 12800 * mib), opts.splitBytes());
    try std.testing.expectEqual(lmax, compaction.levelTarget(opts, 3));
    try std.testing.expectEqual(@divTrunc(lmax, 5), compaction.levelTarget(opts, 2));
    try std.testing.expectEqual(@divTrunc(lmax, 25), compaction.levelTarget(opts, 1));
    // L0 has no target; a level past Lmax neither.
    try std.testing.expectEqual(@as(i64, 0), compaction.levelTarget(opts, 0));
    try std.testing.expectEqual(@as(i64, 0), compaction.levelTarget(opts, 4));
}
