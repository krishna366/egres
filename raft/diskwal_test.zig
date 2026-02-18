const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const raft = @import("raft");
const zen = raft.buffers;

fn testPrint(comptime fmt: []const u8, args: anytype) void {
    if (!builtin.is_test) return;
    _ = fmt;
    _ = args;
    // std.debug.print(fmt, args);
    return;
}

fn makeTempWalDir(alloc: std.mem.Allocator) ![]u8 {
    var path_buf: [160]u8 = undefined;
    const ts = std.time.nanoTimestamp();
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/egres-diskwal-{d}", .{ts});
    try std.fs.cwd().makePath(path);
    return alloc.dupe(u8, path);
}

fn makeLogEntry(
    alloc: std.mem.Allocator,
    index: u64,
    term: u64,
    data: []const u8,
) !zen.LogEntryBuffer {
    var writer = try zen.LogEntryWriter.init(alloc);
    defer writer.deinit();
    try writer.set(.{
        .index = index,
        .term = term,
        .data = data,
        .is_normal = true,
        .is_conf_change = false,
        .conf_change = zen.NullOffset,
    });
    return writer.finishWithTail();
}

fn appendEntry(
    alloc: std.mem.Allocator,
    log: raft.RaftWAL,
    index: u64,
    term: u64,
    data: []const u8,
) !void {
    var entry = try makeLogEntry(alloc, index, term, data);
    defer entry.deinit();
    try log.append(&[_]zen.LogEntryBuffer{entry});
}

fn appendRange(
    alloc: std.mem.Allocator,
    log: raft.RaftWAL,
    start: u64,
    end: u64,
    term: u64,
) !void {
    var idx = start;
    var appended: u64 = 0;
    while (idx <= end) {
        var batch = std.ArrayList(zen.LogEntryBuffer).empty;
        defer {
            for (batch.items) |*entry| entry.deinit();
            batch.deinit(alloc);
        }

        var n: usize = 0;
        while (idx <= end and n < 256) : (n += 1) {
            const payload = if ((idx & 1) == 0) "p0" else "p1";
            const entry = try makeLogEntry(alloc, idx, term, payload);
            try batch.append(alloc, entry);
            idx += 1;
            appended += 1;
        }
        if (appended % 100 == 0 or idx > end) {
            testPrint(
                "[diskwal_test] appendRange progress start={d} end={d} next_idx={d} appended={d}\n",
                .{ start, end, idx, appended },
            );
        }
        try log.append(batch.items);
    }
}

fn checkRange(log: raft.RaftWAL, lo: u64, hi: u64) !void {
    const entries = try log.entriesRangeRef(lo, hi, std.math.maxInt(u64));
    try std.testing.expectEqual(@as(usize, @intCast(hi - lo)), entries.len);
    for (entries, 0..) |stored, i| {
        const idx = lo + @as(u64, @intCast(i));
        const reader = try zen.LogEntryReader.parseRoot(stored.bytes);
        try std.testing.expectEqual(idx, reader.index());
        const expected = if ((idx & 1) == 0) "p0" else "p1";
        try std.testing.expectEqualSlices(u8, expected, reader.data());
    }
}

fn fillLargePayload(buf: []u8, index: u64) void {
    // Use a deterministic pattern so we can validate payload integrity
    // after rotation and reopen without storing all expected entries.
    @memset(buf, @intCast(index % 251));
    const le = std.mem.toBytes(index);
    @memcpy(buf[0..8], &le);
}

fn checkLargePayload(reader: zen.LogEntryReader, index: u64, expected_len: usize) !void {
    const data = reader.data();
    try std.testing.expectEqual(expected_len, data.len);

    const expected_fill: u8 = @intCast(index % 251);
    // Spot-check beginning/middle/end, and verify the encoded index prefix.
    try std.testing.expectEqual(expected_fill, data[data.len / 2]);
    try std.testing.expectEqual(expected_fill, data[data.len - 1]);

    const got_index = std.mem.readInt(u64, data[0..8], .little);
    try std.testing.expectEqual(index, got_index);
}

fn countWalFiles(path: []const u8) !usize {
    var dir = try std.fs.cwd().openDir(path, .{ .iterate = true });
    defer dir.close();

    var it = dir.iterate();
    var count: usize = 0;
    while (try it.next()) |entry_dir| {
        if (entry_dir.kind != .file) continue;
        if (std.mem.endsWith(u8, entry_dir.name, ".wal")) count += 1;
    }
    return count;
}

test "diskwal term and index basics" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    var ds = try raft.DiskWAL.openPath(wal_dir, alloc);
    defer ds.deinit();
    const log = ds.asRaftWAL();

    try std.testing.expectEqual(@as(u64, 1), log.firstIndex());
    try std.testing.expectEqual(@as(u64, 0), log.lastIndex());
    try std.testing.expectEqual(@as(?u64, null), try log.term(1));

    try appendEntry(alloc, log, 1, 1, "a");
    try appendEntry(alloc, log, 2, 2, "bb");
    try appendEntry(alloc, log, 3, 2, "ccc");
    try log.sync();

    try std.testing.expectEqual(@as(u64, 1), log.firstIndex());
    try std.testing.expectEqual(@as(u64, 3), log.lastIndex());
    try std.testing.expectEqual(@as(u64, 2), log.lastTerm());
    try std.testing.expectEqual(@as(?u64, 2), try log.term(3));
}

test "diskwal entriesRangeRef max_bytes keeps at least one" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    var ds = try raft.DiskWAL.openPath(wal_dir, alloc);
    defer ds.deinit();
    const log = ds.asRaftWAL();

    try appendEntry(alloc, log, 1, 1, "1111");
    try appendEntry(alloc, log, 2, 1, "2222");
    try appendEntry(alloc, log, 3, 1, "3333");
    try log.sync();

    const all = try log.entriesRangeRef(1, 4, std.math.maxInt(u64));
    try std.testing.expectEqual(@as(usize, 3), all.len);

    // Even when max_bytes is tiny, API should return at least one entry.
    const limited = try log.entriesRangeRef(1, 4, 1);
    try std.testing.expectEqual(@as(usize, 1), limited.len);
    const reader = try zen.LogEntryReader.parseRoot(limited[0].bytes);
    try std.testing.expectEqualSlices(u8, "1111", reader.data());
}

test "diskwal append overwrites conflicting suffix" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    var ds = try raft.DiskWAL.openPath(wal_dir, alloc);
    defer ds.deinit();
    const log = ds.asRaftWAL();

    try appendEntry(alloc, log, 1, 1, "1");
    try appendEntry(alloc, log, 2, 1, "2");
    try appendEntry(alloc, log, 3, 1, "3");

    try appendEntry(alloc, log, 2, 9, "2x");
    try appendEntry(alloc, log, 3, 9, "3x");
    try log.sync();

    try std.testing.expectEqual(@as(u64, 3), log.lastIndex());
    try std.testing.expectEqual(@as(?u64, 1), try log.term(1));
    try std.testing.expectEqual(@as(?u64, 9), try log.term(2));
    try std.testing.expectEqual(@as(?u64, 9), try log.term(3));

    const entries = try log.entriesRangeRef(1, 4, std.math.maxInt(u64));
    try std.testing.expectEqual(@as(usize, 3), entries.len);
    const reader = try zen.LogEntryReader.parseRoot(entries[2].bytes);
    try std.testing.expectEqualSlices(u8, "3x", reader.data());
}

test "diskwal truncateFrom drops suffix" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    var ds = try raft.DiskWAL.openPath(wal_dir, alloc);
    defer ds.deinit();
    const log = ds.asRaftWAL();

    try appendEntry(alloc, log, 1, 1, "a");
    try appendEntry(alloc, log, 2, 1, "b");
    try appendEntry(alloc, log, 3, 1, "c");
    try appendEntry(alloc, log, 4, 1, "d");
    log.truncateFrom(3);

    try std.testing.expectEqual(@as(u64, 2), log.lastIndex());
    try std.testing.expectEqual(@as(?u64, null), try log.term(4));
}

test "diskwal persists hardstate and entries across reopen" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc);
        defer ds.deinit();
        const log = ds.asRaftWAL();
        try log.setHardState(.{
            .current_term = 7,
            .voted_for = 2,
            .commit_index = 3,
        });
        try appendEntry(alloc, log, 1, 7, "a1");
        try appendEntry(alloc, log, 2, 7, "a2");
        try log.sync();
    }

    {
        var ds2 = try raft.DiskWAL.openPath(wal_dir, alloc);
        defer ds2.deinit();
        const log2 = ds2.asRaftWAL();
        try std.testing.expectEqual(@as(u64, 2), log2.lastIndex());
        try std.testing.expectEqual(@as(?u64, 7), try log2.term(2));
        const hs = log2.hardState();
        try std.testing.expectEqual(@as(u64, 7), hs.current_term);
        try std.testing.expectEqual(@as(u64, 2), hs.voted_for);
        try std.testing.expectEqual(@as(u64, 3), hs.commit_index);
    }
}

test "diskwal drops malformed trailing wal files on reopen" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc);
        defer ds.deinit();
        const log = ds.asRaftWAL();
        try appendEntry(alloc, log, 1, 1, "x");
        try log.sync();
    }

    const bad_name = try std.fs.path.join(alloc, &[_][]const u8{ wal_dir, "00002.wal" });
    defer alloc.free(bad_name);
    {
        const bad = try std.fs.cwd().createFile(bad_name, .{ .truncate = true, .read = true });
        bad.close();
    }

    {
        var ds2 = try raft.DiskWAL.openPath(wal_dir, alloc);
        defer ds2.deinit();
        try std.testing.expectEqual(@as(u64, 1), ds2.asRaftWAL().lastIndex());
    }

    try std.testing.expectError(error.FileNotFound, std.fs.cwd().access(bad_name, .{}));
}

test "diskwal rejects snapshot-log firstIndex mismatch on reopen" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc);
        defer ds.deinit();
        const log = ds.asRaftWAL();
        try log.setSnapshot(.{ .index = 5, .term = 2, .data = "snap" });
        try log.sync();
    }

    try std.testing.expectError(
        error.CorruptLogFile,
        raft.DiskWAL.openPath(wal_dir, alloc),
    );
}

test "BIGTEST diskwal storage" {
    if (!build_options.enable_bigtests) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    // Keep this large enough to cross a segment boundary.
    const N: u64 = 62_000;

    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc);
        defer ds.deinit();
        const log = ds.asRaftWAL();

        try appendRange(alloc, log, 1, N, 1);
        try std.testing.expectEqual(N, log.lastIndex());

        try checkRange(log, 1, N + 1);

        // Jump back by 2001 to cross the 60k boundary, then append 100 entries.
        const jump_start: u64 = N - 2001; // 59999 when N=62000
        const jump_end: u64 = jump_start + 100; // 60099
        try appendRange(alloc, log, jump_start, jump_start, 3);
        // try appendEntry(alloc, log, jump_start, 3, "tail");
        try std.testing.expectEqual(jump_start, log.lastIndex());

        try appendRange(alloc, log, jump_start + 1, jump_end, 3);
        try std.testing.expectEqual(jump_end, log.lastIndex());

        try checkRange(log, jump_start, jump_end + 1);
        try std.testing.expectEqual(@as(?u64, 3), try log.term(jump_start));
        try std.testing.expectEqual(@as(?u64, 3), try log.term(jump_end));

        // Jump back again to 60000 and overwrite the same 100-entry window.
        const overwrite_start: u64 = 60_000;
        const overwrite_end: u64 = overwrite_start + 99;
        try appendRange(alloc, log, overwrite_start, overwrite_end, 4);
        try std.testing.expectEqual(overwrite_end, log.lastIndex());
        try checkRange(log, overwrite_start, overwrite_end + 1);
        try std.testing.expectEqual(@as(?u64, 4), try log.term(overwrite_start));
        try std.testing.expectEqual(@as(?u64, 4), try log.term(overwrite_end));
        try log.sync();
    }

    {
        var ds2 = try raft.DiskWAL.openPath(wal_dir, alloc);
        defer ds2.deinit();
        const log2 = ds2.asRaftWAL();
        try std.testing.expectEqual(@as(u64, 60_099), log2.lastIndex());
        try checkRange(log2, 1, 60_100);
        try std.testing.expectEqual(@as(?u64, 4), try log2.term(60_000));
        try std.testing.expectEqual(@as(?u64, 4), try log2.term(60_099));
    }
}

test "diskwal meta file adapted from dgraph" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    var mf = try raft.diskwal.metaFile.openPath(wal_dir, alloc);
    defer mf.deinit();

    const hs0 = try mf.hardState();
    try std.testing.expectEqual(@as(u64, 0), hs0.current_term);
    try std.testing.expectEqual(@as(u64, 0), hs0.voted_for);
    try std.testing.expectEqual(@as(u64, 0), hs0.commit_index);

    try mf.storeHardState(.{
        .current_term = 10,
        .voted_for = 20,
        .commit_index = 30,
    });
    const hs1 = try mf.hardState();
    try std.testing.expectEqual(@as(u64, 10), hs1.current_term);
    try std.testing.expectEqual(@as(u64, 20), hs1.voted_for);
    try std.testing.expectEqual(@as(u64, 30), hs1.commit_index);

    const snap0 = try mf.snapshot();
    defer if (snap0.data.len != 0) alloc.free(@constCast(snap0.data));
    try std.testing.expectEqual(@as(u64, 0), snap0.index);
    try std.testing.expectEqual(@as(u64, 0), snap0.term);
    try std.testing.expectEqual(@as(usize, 0), snap0.data.len);

    try mf.storeSnapshot(.{
        .index = 12,
        .term = 200,
        .data = "foo",
    });
    const snap1 = try mf.snapshot();
    defer if (snap1.data.len != 0) alloc.free(@constCast(snap1.data));
    try std.testing.expectEqual(@as(u64, 12), snap1.index);
    try std.testing.expectEqual(@as(u64, 200), snap1.term);
    try std.testing.expectEqualSlices(u8, "foo", snap1.data);
}

test "BIGTEST diskwal truncate storage" {
    if (!build_options.enable_bigtests) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    var ds = try raft.DiskWAL.openPath(wal_dir, alloc);
    defer ds.deinit();
    const log = ds.asRaftWAL();

    const num_entries: u64 = (raft.diskwal.max_num_entries * 3) / 2;
    const compact_to: u64 = raft.diskwal.max_num_entries + 10;

    // Fill across segment boundaries.
    try appendRange(alloc, log, 1, num_entries, 1);
    try std.testing.expectEqual(num_entries, log.lastIndex());
    try checkRange(log, 1, num_entries + 1);

    // Compaction here is segment-granular. Compact beyond first segment so
    // early indices become unavailable.
    log.compactUntil(compact_to);
    try std.testing.expectEqual(@as(?u64, null), try log.term(1));
    const first_after_compact = log.firstIndex();
    try checkRange(log, first_after_compact, num_entries + 1);

    // Append one more full-segment worth and verify full retained range.
    try appendRange(alloc, log, num_entries + 1, num_entries + raft.diskwal.max_num_entries, 2);
    const new_last = num_entries + raft.diskwal.max_num_entries;
    try std.testing.expectEqual(new_last, log.lastIndex());
    try std.testing.expectEqual(@as(?u64, 2), try log.term(new_last));
    try checkRange(log, first_after_compact, new_last + 1);
    try checkRange(log, new_last - 100, new_last + 1);
}

test "BIGTEST diskwal large payload rotation" {
    if (!build_options.enable_bigtests) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    const payload_size = 4 * 1024 * 1024;
    const total_entries: u64 = 150;
    const payload = try alloc.alloc(u8, payload_size);
    defer alloc.free(payload);

    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc);
        defer ds.deinit();
        const log = ds.asRaftWAL();

        var idx: u64 = 1;
        while (idx <= total_entries) : (idx += 1) {
            fillLargePayload(payload, idx);
            // Use term=index to exercise varying term persistence across segments.
            try appendEntry(alloc, log, idx, idx, payload);
        }
        try log.sync();

        try std.testing.expectEqual(total_entries, log.lastIndex());

        // 150 x 4MiB should span exactly 3 WAL segment files with current limits.
        try std.testing.expectEqual(@as(usize, 3), try countWalFiles(wal_dir));

        // Validate reads that cross file boundaries.
        const cross_1 = try log.entriesRangeRef(60, 67, std.math.maxInt(u64));
        try std.testing.expectEqual(@as(usize, 7), cross_1.len);
        for (cross_1, 0..) |stored, i| {
            const expected_index = @as(u64, 60) + @as(u64, @intCast(i));
            const reader = try zen.LogEntryReader.parseRoot(stored.bytes);
            try std.testing.expectEqual(expected_index, reader.index());
            try std.testing.expectEqual(expected_index, reader.term());
            try checkLargePayload(reader, expected_index, payload_size);
        }

        const cross_2 = try log.entriesRangeRef(124, 131, std.math.maxInt(u64));
        try std.testing.expectEqual(@as(usize, 7), cross_2.len);
        for (cross_2, 0..) |stored, i| {
            const expected_index = @as(u64, 124) + @as(u64, @intCast(i));
            const reader = try zen.LogEntryReader.parseRoot(stored.bytes);
            try std.testing.expectEqual(expected_index, reader.index());
            try std.testing.expectEqual(expected_index, reader.term());
            try checkLargePayload(reader, expected_index, payload_size);
        }

        // Even with a very small max_bytes, API should return at least one entry.
        const limited = try log.entriesRangeRef(1, 10, 1024 * 1024);
        try std.testing.expectEqual(@as(usize, 1), limited.len);
        const limited_reader = try zen.LogEntryReader.parseRoot(limited[0].bytes);
        try std.testing.expectEqual(@as(u64, 1), limited_reader.index());
        try checkLargePayload(limited_reader, 1, payload_size);
    }

    // Reopen and verify recovery paths over the same boundary ranges.
    {
        var ds2 = try raft.DiskWAL.openPath(wal_dir, alloc);
        defer ds2.deinit();
        const log2 = ds2.asRaftWAL();

        try std.testing.expectEqual(total_entries, log2.lastIndex());
        try std.testing.expectEqual(@as(?u64, 1), try log2.term(1));
        try std.testing.expectEqual(@as(?u64, 63), try log2.term(63));
        try std.testing.expectEqual(@as(?u64, 64), try log2.term(64));
        try std.testing.expectEqual(@as(?u64, 126), try log2.term(126));
        try std.testing.expectEqual(@as(?u64, 127), try log2.term(127));
        try std.testing.expectEqual(@as(?u64, 150), try log2.term(150));

        const cross = try log2.entriesRangeRef(124, 131, std.math.maxInt(u64));
        try std.testing.expectEqual(@as(usize, 7), cross.len);
        for (cross, 0..) |stored, i| {
            const expected_index = @as(u64, 124) + @as(u64, @intCast(i));
            const reader = try zen.LogEntryReader.parseRoot(stored.bytes);
            try std.testing.expectEqual(expected_index, reader.index());
            try std.testing.expectEqual(expected_index, reader.term());
            try checkLargePayload(reader, expected_index, payload_size);
        }

        // Overwrite from an older index after reopen. This should truncate the
        // suffix and drop the highest segment file, leaving two WAL files.
        fillLargePayload(payload, 100);
        try appendEntry(alloc, log2, 100, 100, payload);
        try log2.sync();

        try std.testing.expectEqual(@as(u64, 100), log2.lastIndex());
        try std.testing.expectEqual(@as(?u64, null), try log2.term(150));
        try std.testing.expectEqual(@as(usize, 2), try countWalFiles(wal_dir));
    }
}
