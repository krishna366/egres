const std = @import("std");
const m = @import("metrics");
const utils = @import("utils");
const String = utils.String;
const fiber = @import("zfiber");

// Raft-layer metrics. Same lifecycle pattern as zdb/metrics.zig: an
// application creates a `Metrics` once, library code reaches it via the
// `g()` global. Default is noop so unit tests + embedders that don't call
// initGlobal still compile and run safely.

pub const PhaseLabels = struct {
    phase: String,
};

// Millisecond bucket boundaries. fiber.coarseMs ticks ~every 1 ms, so any
// sub-ms phase lands in the lowest bucket — a tail above ~10 ms points
// at fsync stalls or scheduling hazards. Last bucket = 1 s.
const phase_buckets: []const u64 = &.{ 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000 };

// Powers-of-two bucket boundaries for proposal-batch sizes. The propose
// pipeline drains the entire pending queue per call, so this histogram
// shows the actual group-commit width — the key driver of WAL amortization.
const batch_buckets: []const u64 = &.{ 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 4096 };

pub const PhaseHist = m.HistogramVec(u64, PhaseLabels, phase_buckets);
// Microsecond buckets for the per-commit timeline (see Node.Timeline):
// stages are 50 us to a few ms, and the interesting tail is 5 to 50 ms.
const stage_buckets_us: []const u64 = &.{
    50,   100,  200,  300,  500,   750,   1000,
    1500, 2000, 3000, 5000, 10000, 50000,
};
pub const StageHist = m.HistogramVec(u64, PhaseLabels, stage_buckets_us);
pub const BatchHist = m.Histogram(u64, batch_buckets);

pub const Metrics = struct {
    /// Wall-clock duration of each phase of the leader's mainLoop iteration.
    /// Phase taxonomy:
    ///   inbound          drain inbound_ch (handle AE replies, etc.)
    ///   propose          processProposalRequests total (queue + build + persist)
    ///   propose_build    sub-phase: walk requests, build LogEntry buffers
    ///   propose_persist  sub-phase: log.append + log.sync
    ///   advance          advanceCommitIndex + applyConfChanges + notify
    ///   broadcast        sendAppendEntries to all peers
    propose_phase_ms: PhaseHist,
    /// How many proposals were drained out of the queue per
    /// `processProposalRequests` call. Higher = more group-commit
    /// amortization across the WAL fsync. Recorded only when the call had
    /// at least one proposal to handle (no zero samples — the propose
    /// phase histogram already encodes idle iterations).
    propose_batch_size: BatchHist,
    /// Per-commit timeline: one sample per entry per stage it passes,
    /// labelled "<previous stage>_to_<stage>" by `CommitTimeline`
    /// (proposed, buffered, write_start, write_end, commit, picked,
    /// apply, woken), plus `entries_range` and the apply call itself as
    /// `apply_data` / `apply_manifest`.
    commit_stage_us: StageHist,

    pub fn init(allocator: std.mem.Allocator) !Metrics {
        const io = utils.context.g().io;
        return .{
            .propose_phase_ms = try PhaseHist.init(
                allocator,
                io,
                "raft_propose_phase_ms",
                .{},
                .{},
            ),
            // Histogram (no labels) is a comptime construction — no allocator,
            // no error union. Storage is a fixed [N]u64 array of bucket counts.
            .propose_batch_size = BatchHist.init(
                "raft_propose_batch_size",
                .{},
                .{},
            ),
            .commit_stage_us = try StageHist.init(
                allocator,
                io,
                "raft_commit_stage_us",
                .{},
                .{},
            ),
        };
    }

    pub fn deinit(self: *Metrics) void {
        self.propose_phase_ms.deinit();
        self.commit_stage_us.deinit();
        // propose_batch_size has no dynamic state, no deinit needed.
    }

    /// noop returns a Metrics where every metric is a no-op. Library code can
    /// always call observe() unconditionally without checking for init.
    pub fn noop() Metrics {
        return m.initializeNoop(Metrics);
    }

    /// Observe `fiber.coarseMs() - start_ms` on the named phase. Errors
    /// from the underlying allocator (only possible the first time a new
    /// label is seen) are swallowed — losing one sample beats taking down
    /// the leader.
    pub fn observePhase(self: *Metrics, phase: String, start_ms: i64) void {
        const elapsed_ms: u64 = @intCast(fiber.coarseMs() - start_ms);
        self.propose_phase_ms.observe(.{ .phase = phase }, elapsed_ms) catch {};
    }

    /// Record the drain depth — number of proposal requests pulled out of
    /// the queue in one `processProposalRequests` call. Skip the call
    /// entirely if `count == 0` so the histogram only describes active
    /// drains (matches `propose_build`'s sample count).
    /// One timeline stage, in microseconds (negative = the marks were
    /// taken out of order, recorded as 0).
    pub fn observeStage(self: *Metrics, stage: String, us: i64) void {
        const v: u64 = if (us <= 0) 0 else @intCast(us);
        self.commit_stage_us.observe(.{ .phase = stage }, v) catch {};
    }

    pub fn observeBatchSize(self: *Metrics, count: usize) void {
        if (count == 0) return;
        self.propose_batch_size.observe(@intCast(count));
    }
};

var g_metrics: Metrics = Metrics.noop();

pub fn g() *Metrics {
    return &g_metrics;
}

pub fn initGlobal(allocator: std.mem.Allocator) !void {
    g_metrics = try Metrics.init(allocator);
}

pub fn deinitGlobal() void {
    g_metrics.deinit();
    g_metrics = Metrics.noop();
}

/// Per-commit timeline (`raft_commit_stage_us`): the moments a log
/// index passes through, recorded as it goes. `record(index, stage)`
/// observes the time since the slot's latest earlier stage as one
/// histogram sample labelled "<that>_to_<stage>", so a sample is taken
/// where the stage happens and nothing is gathered up later. A ring by
/// index: an index older than the ring's span is gone, and its later
/// stages start a fresh slot with nothing earlier to measure from; that
/// entry's samples are lost, nothing else.
pub const CommitTimeline = struct {
    /// In the order a leader's entry passes through them. A follower's
    /// entry starts at `buffered`. `woken` is the proposer running again
    /// after its wait, which for a commit waiter comes before `apply`.
    pub const Stage = enum(u8) {
        proposed,
        buffered,
        write_start,
        write_end,
        commit,
        picked,
        apply,
        woken,
    };
    const stage_count = @typeInfo(Stage).@"enum".fields.len;

    pub const Slot = struct {
        index: u64 = 0,
        /// When each stage was reached; 0 = not yet.
        at: [stage_count]i64 = @splat(0),

        /// Every recorded stage, as its offset from the first one:
        /// "buffered+0us write_start+412us ... woken+5013204us".
        pub fn format(self: Slot, w: *std.Io.Writer) std.Io.Writer.Error!void {
            var base: ?i64 = null;
            for (self.at, 0..) |at, i| {
                if (at == 0) continue;
                const b = base orelse at;
                base = b;
                const stage: Stage = @enumFromInt(i);
                try w.print("{s}{s}+{d}us", .{ if (b == at) "" else " ", @tagName(stage), at - b });
            }
        }
    };
    /// 72 bytes a slot: at 10k entries/s this holds ~1.6 s, against an
    /// apply that follows its commit within milliseconds.
    pub const ring_len = 16 * 1024;
    sl: fiber.Spinlock = .{},
    slots: [ring_len]Slot = [_]Slot{.{}} ** ring_len,

    /// "<stage>_to_<next>" for each consecutive pair: stages are
    /// reached in order, so a sample is the step from the one before.
    const step_names: [stage_count - 1]String = blk: {
        const fields = @typeInfo(Stage).@"enum".fields;
        var names: [stage_count - 1]String = undefined;
        for (0..stage_count - 1) |i| {
            names[i] = fields[i].name ++ "_to_" ++ fields[i + 1].name;
        }
        break :blk names;
    };
    /// The one skip: a commit waiter wakes at commit, before this node
    /// has picked and applied the entry.
    const commit_to_woken = "commit_to_woken";

    /// `index` reached `stage` at `at_us`: one sample, the step from the
    /// stage before it (none if that stage was not recorded, e.g. a
    /// follower's `write_start` with no `buffered`, or a slot that
    /// started mid-way). A stage the slot already has means the index
    /// was re-appended after a truncation: it starts over. A slot held
    /// by a higher index keeps it: the ring has moved a lap past
    /// `index`, and a late record for it is dropped.
    pub fn record(self: *CommitTimeline, index: u64, stage: Stage, at_us: i64) void {
        self.sl.lock();
        defer self.sl.unlock();
        self.recordLocked(index, stage, at_us);
    }

    /// The per-batch stages, a batch write or a commit advance: every
    /// index in [first, last], at most a ring's worth back from `last`.
    pub fn recordRange(self: *CommitTimeline, first: u64, last: u64, stage: Stage, at_us: i64) void {
        self.sl.lock();
        defer self.sl.unlock();
        var i = @max(first, last -| (ring_len - 1));
        while (i <= last) : (i += 1) self.recordLocked(i, stage, at_us);
    }

    /// The slot for `index`, if the ring still holds it.
    pub fn lookup(self: *CommitTimeline, index: u64) ?Slot {
        self.sl.lock();
        defer self.sl.unlock();
        const slot = self.slots[index % ring_len];
        return if (slot.index == index) slot else null;
    }

    fn recordLocked(self: *CommitTimeline, index: u64, stage: Stage, at_us: i64) void {
        const slot = &self.slots[index % ring_len];
        const s = @intFromEnum(stage);
        if (slot.index > index) return;
        if (slot.index != index or slot.at[s] != 0) slot.* = .{ .index = index };

        slot.at[s] = at_us;
        if (s == 0) return;
        if (slot.at[s - 1] != 0) {
            g().observeStage(step_names[s - 1], at_us - slot.at[s - 1]);
            return;
        }
        if (stage == .woken) {
            const c = @intFromEnum(Stage.commit);
            if (slot.at[c] != 0) g().observeStage(commit_to_woken, at_us - slot.at[c]);
        }
    }
};

var g_timeline: CommitTimeline = .{};

/// The process's commit timeline (one raft group per process).
pub fn timeline() *CommitTimeline {
    return &g_timeline;
}
