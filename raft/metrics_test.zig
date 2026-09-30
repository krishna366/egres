// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const raft = @import("raft");
const CommitTimeline = raft.metrics.CommitTimeline;

test "CommitTimeline: stages accumulate per index; a range covers every index in it" {
    // Too big for a fiber stack at this ring size; the real one is a global.
    const tl = try std.testing.allocator.create(CommitTimeline);
    defer std.testing.allocator.destroy(tl);
    tl.* = .{};
    tl.record(3, .proposed, 100);
    tl.record(3, .buffered, 120);
    tl.record(4, .buffered, 130);
    tl.recordRange(3, 4, .write_start, 200);
    tl.recordRange(3, 4, .write_end, 220);
    tl.recordRange(1, 4, .commit, 300);

    const s3 = tl.lookup(3).?;
    try std.testing.expectEqual(@as(i64, 100), s3.at[@intFromEnum(CommitTimeline.Stage.proposed)]);
    try std.testing.expectEqual(@as(i64, 120), s3.at[@intFromEnum(CommitTimeline.Stage.buffered)]);
    try std.testing.expectEqual(@as(i64, 200), s3.at[@intFromEnum(CommitTimeline.Stage.write_start)]);
    try std.testing.expectEqual(@as(i64, 300), s3.at[@intFromEnum(CommitTimeline.Stage.commit)]);
    // A follower-style index: no proposed, the rest present.
    const s4 = tl.lookup(4).?;
    try std.testing.expectEqual(@as(i64, 0), s4.at[@intFromEnum(CommitTimeline.Stage.proposed)]);
    try std.testing.expectEqual(@as(i64, 220), s4.at[@intFromEnum(CommitTimeline.Stage.write_end)]);
    // The commit range reached indexes the slots knew nothing about.
    try std.testing.expectEqual(@as(i64, 300), tl.lookup(1).?.at[@intFromEnum(CommitTimeline.Stage.commit)]);
    try std.testing.expect(tl.lookup(5) == null);
}

test "CommitTimeline: the ring forgets old indexes, and a range fills only what fits" {
    // Too big for a fiber stack at this ring size; the real one is a global.
    const tl = try std.testing.allocator.create(CommitTimeline);
    defer std.testing.allocator.destroy(tl);
    tl.* = .{};
    const ring = CommitTimeline.ring_len;
    tl.record(10, .buffered, 1);
    const far: u64 = 10 + 10 * ring;
    tl.recordRange(11, far, .commit, 3);
    try std.testing.expect(tl.lookup(10) == null);
    try std.testing.expectEqual(@as(i64, 3), tl.lookup(far).?.at[@intFromEnum(CommitTimeline.Stage.commit)]);
    // The oldest index the fill kept is a ring's worth back from `far`.
    try std.testing.expectEqual(@as(i64, 3), tl.lookup(far - (ring - 1)).?.at[@intFromEnum(CommitTimeline.Stage.commit)]);
    try std.testing.expect(tl.lookup(far - ring) == null);
}

test "CommitTimeline: a stage recorded twice (re-appended after truncation) starts the slot over" {
    // Too big for a fiber stack at this ring size; the real one is a global.
    const tl = try std.testing.allocator.create(CommitTimeline);
    defer std.testing.allocator.destroy(tl);
    tl.* = .{};
    tl.record(50, .proposed, 1);
    tl.record(50, .buffered, 2);
    tl.recordRange(50, 50, .commit, 3);
    // Truncated and re-appended: buffered again, the old marks go.
    tl.record(50, .buffered, 10);
    const s = tl.lookup(50).?;
    try std.testing.expectEqual(@as(i64, 10), s.at[@intFromEnum(CommitTimeline.Stage.buffered)]);
    try std.testing.expectEqual(@as(i64, 0), s.at[@intFromEnum(CommitTimeline.Stage.proposed)]);
    try std.testing.expectEqual(@as(i64, 0), s.at[@intFromEnum(CommitTimeline.Stage.commit)]);
}

test "CommitTimeline: a late record for an index the ring has lapped is dropped" {
    const tl = try std.testing.allocator.create(CommitTimeline);
    defer std.testing.allocator.destroy(tl);
    tl.* = .{};
    const ring = CommitTimeline.ring_len;
    tl.record(7 + ring, .commit, 5); // the slot for 7 now belongs to 7 + ring
    tl.record(7, .woken, 9); // a proposer of 7 running very late
    const s = tl.lookup(7 + ring).?;
    try std.testing.expectEqual(@as(i64, 5), s.at[@intFromEnum(CommitTimeline.Stage.commit)]);
    try std.testing.expect(tl.lookup(7) == null);
}

test "CommitTimeline: a slot prints its stages as offsets from the first" {
    const tl = try std.testing.allocator.create(CommitTimeline);
    defer std.testing.allocator.destroy(tl);
    tl.* = .{};
    tl.record(9, .buffered, 1000);
    tl.recordRange(9, 9, .commit, 1400);
    tl.record(9, .apply, 2100);
    var buf: [128]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try w.print("{f}", .{tl.lookup(9).?});
    try std.testing.expectEqualStrings("buffered+0us commit+400us apply+1100us", w.buffered());
}
