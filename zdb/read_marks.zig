// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The reads open on this node, counted by the minute they read at.
//!
//! `Forest.snapshot_ts` must stay at or below every open read. A
//! read is one count in the slot of its minute: `begin` increments,
//! `end` decrements, nothing is locked or allocated. `minReadTs` is
//! the start of the oldest minute with a count, so it is below that
//! read by at most a minute.
//!
//! The slots are a ring of one day of minutes. `minReadTs` looks back
//! `max_age_secs` at most: a read open for longer is passed, and its
//! next statement is refused.

const std = @import("std");
const utils = @import("utils");
const HLC = utils.hlc.HLC;

pub const ReadMarks = struct {
    /// One slot per minute, a day's worth.
    pub const num_slots: usize = 24 * 60;
    /// The furthest `minReadTs` looks back: half the ring, so a slot
    /// it reads never holds a minute newer than `upper_ts`.
    pub const max_lookback_secs: u64 = (num_slots / 2) * std.time.s_per_min;

    counts: [num_slots]std.atomic.Value(u32) =
        [_]std.atomic.Value(u32){.init(0)} ** num_slots,

    /// The minute `ts` is in, counted from the epoch.
    fn minuteOf(ts: u64) u64 {
        return HLC.fromU64(ts).seconds / std.time.s_per_min;
    }

    /// The first timestamp of a minute.
    fn startOf(minute: u64) u64 {
        const start = HLC{
            .logical = 0,
            .seconds = @intCast(minute * std.time.s_per_min),
        };
        return start.asU64();
    }

    /// A read at `ts` begins. Counting does not make it valid: one
    /// already below `snapshot_ts` is still refused.
    pub fn begin(self: *ReadMarks, ts: u64) void {
        _ = self.counts[minuteOf(ts) % num_slots].fetchAdd(1, .monotonic);
    }

    /// The read counted at `ts` is over. Same `ts` as at `begin`.
    pub fn end(self: *ReadMarks, ts: u64) void {
        const slot = minuteOf(ts) % num_slots;
        const before = self.counts[slot].fetchSub(1, .monotonic);
        std.debug.assert(before > 0);
    }

    /// `peers_min_ts` when no other node has reported a read.
    pub const no_peers: u64 = std.math.maxInt(u64);

    /// The start of the oldest minute in [first, last] with a read
    /// open here, or null when there is none.
    fn minOpenTs(self: *const ReadMarks, first: u64, last: u64) ?u64 {
        var minute = first;
        while (minute <= last) : (minute += 1) {
            const slot = minute % num_slots;
            if (self.counts[slot].load(.monotonic) != 0)
                return startOf(minute);
        }
        return null;
    }

    /// The lowest timestamp still read at, to the minute, here or on
    /// the other voters (`peers_min_ts`). At most `upper_ts`, where
    /// the next read begins, and at most `max_age_secs` before it.
    pub fn minReadTs(
        self: *const ReadMarks,
        upper_ts: u64,
        max_age_secs: u64,
        peers_min_ts: u64,
    ) u64 {
        // Both minutes found from seconds: counting back in whole
        // minutes would drop a read just inside the age.
        const upper_secs: u64 = HLC.fromU64(upper_ts).seconds;
        const lookback_secs = @min(max_age_secs, max_lookback_secs);
        const first = (upper_secs -| lookback_secs) / std.time.s_per_min;
        const last = upper_secs / std.time.s_per_min;

        const here = self.minOpenTs(first, last) orelse upper_ts;
        const min_ts = @min(here, peers_min_ts);
        return std.math.clamp(min_ts, startOf(first), upper_ts);
    }
};
