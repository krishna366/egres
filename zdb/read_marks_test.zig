// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const utils = @import("utils");
const zdb = @import("zdb");

const HLC = utils.hlc.HLC;
const ReadMarks = zdb.forest.ReadMarks;

fn at(seconds: u32, logical: u32) u64 {
    return (HLC{ .seconds = seconds, .logical = logical }).asU64();
}

// A whole minute, so offsets from it name minutes plainly.
const base: u32 = 1_800_000_000 - (1_800_000_000 % 60);
const hour_secs: u64 = 3600;
const no_peers = ReadMarks.no_peers;

test "ReadMarks: min read ts is the oldest open minute, or the snapshot" {
    var marks = ReadMarks{};
    const snap = at(base + 600, 5);
    // Nothing open: the snapshot's own timestamp.
    try testing.expectEqual(snap, marks.minReadTs(snap, hour_secs, no_peers));

    // Three reads: two in one minute, one two minutes later.
    marks.begin(at(base + 7, 3));
    marks.begin(at(base + 59, 1));
    marks.begin(at(base + 125, 9));

    // A minute's first timestamp: below every read in it.
    try testing.expectEqual(
        at(base, 0),
        marks.minReadTs(snap, hour_secs, no_peers),
    );

    // One of the two leaves: the minute still has a read.
    marks.end(at(base + 7, 3));
    try testing.expectEqual(
        at(base, 0),
        marks.minReadTs(snap, hour_secs, no_peers),
    );
    // The other leaves: the oldest is now the later minute.
    marks.end(at(base + 59, 1));
    try testing.expectEqual(
        at(base + 120, 0),
        marks.minReadTs(snap, hour_secs, no_peers),
    );
    marks.end(at(base + 125, 9));
    try testing.expectEqual(snap, marks.minReadTs(snap, hour_secs, no_peers));
}

test "ReadMarks: a read newer than the snapshot is not the min read ts" {
    var marks = ReadMarks{};
    const snap = at(base + 600, 5);
    // In a later minute, and in the snapshot's own minute but after it.
    marks.begin(at(base + 700, 1));
    try testing.expectEqual(snap, marks.minReadTs(snap, hour_secs, no_peers));
    marks.begin(at(base + 600, 9));
    // The snapshot's minute has a read: its start is below the
    // snapshot, and the result is never above the snapshot either way.
    try testing.expectEqual(
        at(base + 600, 0),
        marks.minReadTs(snap, hour_secs, no_peers),
    );
    marks.end(at(base + 600, 9));
    marks.end(at(base + 700, 1));
}

test "ReadMarks: a read older than the given age is not counted" {
    var marks = ReadMarks{};
    const old = at(base, 1);
    marks.begin(old);
    defer marks.end(old);

    // An hour on, to the minute: still inside the look-back.
    const edge = at(base + 3600, 0);
    try testing.expectEqual(
        at(base, 0),
        marks.minReadTs(edge, hour_secs, no_peers),
    );
    // A minute past that: no longer looked at.
    const past = at(base + 3660, 0);
    try testing.expectEqual(past, marks.minReadTs(past, hour_secs, no_peers));
    // Unless the age allowed is longer.
    try testing.expectEqual(
        at(base, 0),
        marks.minReadTs(past, 2 * hour_secs, no_peers),
    );
    // The look-back is capped at half the ring, whatever is asked.
    const lookback: u32 = @intCast(ReadMarks.max_lookback_secs);
    const far = at(base + lookback + 60, 0);
    try testing.expectEqual(
        far,
        marks.minReadTs(far, 100 * hour_secs, no_peers),
    );
}

test "ReadMarks: what other nodes report counts, within the age" {
    var marks = ReadMarks{};
    const snap = at(base + 600, 5);

    // Another node reads four minutes back: the result is its report.
    const theirs = at(base + 360, 0);
    try testing.expectEqual(theirs, marks.minReadTs(snap, hour_secs, theirs));
    // A read here that is older still wins, and a newer one does not.
    marks.begin(at(base + 130, 2));
    try testing.expectEqual(
        at(base + 120, 0),
        marks.minReadTs(snap, hour_secs, theirs),
    );
    marks.end(at(base + 130, 2));
    marks.begin(at(base + 500, 2));
    try testing.expectEqual(theirs, marks.minReadTs(snap, hour_secs, theirs));
    marks.end(at(base + 500, 2));

    // A report above the snapshot changes nothing.
    try testing.expectEqual(
        snap,
        marks.minReadTs(snap, hour_secs, at(base + 900, 0)),
    );
    // A report older than the age allowed counts only as far
    // back as that age: a node stuck behind does not stop the group.
    const late = at(base + 7200, 0);
    try testing.expectEqual(
        at(base + 3600, 0),
        marks.minReadTs(late, hour_secs, at(base, 0)),
    );
}

test "ReadMarks: a read inside the age counts at any second" {
    var marks = ReadMarks{};
    // An age that is not whole minutes, and a snapshot five seconds
    // into its minute: the read is 75 seconds older, two minutes back
    // on the clock, and inside the age.
    const snap = at(base + 125, 0);
    const read = at(base + 50, 0);
    marks.begin(read);
    defer marks.end(read);
    try testing.expectEqual(at(base, 0), marks.minReadTs(snap, 90, no_peers));
    // An age the read is outside of.
    try testing.expectEqual(snap, marks.minReadTs(snap, 60, no_peers));
    // The age is counted from the snapshot, not from the clock: a read
    // newer than the snapshot is never below the result, however short
    // the age.
    const later = at(base + 200, 0);
    marks.begin(later);
    defer marks.end(later);
    try testing.expectEqual(snap, marks.minReadTs(snap, 1, no_peers));
}
