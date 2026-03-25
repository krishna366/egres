const std = @import("std");
const zio = @import("zio");
const raftwal_mod = @import("raftwal.zig");
const zen = @import("buffers_gen");
const utils = @import("utils");
const raft_log = std.log.scoped(.raft);

pub const NodeId = raftwal_mod.NodeId;
pub const State = enum {
    follower,
    candidate,
    leader,
    learner,
};
pub const ConfChange = raftwal_mod.ConfChange;
pub const ConfChangeType = raftwal_mod.ConfChangeType;

pub const PeerRole = enum {
    voter,
    learner,
};

pub const Config = struct {
    id: NodeId,
    cluster_id: u64 = 0,
    listen_addr: []const u8 = "",
    tick_interval_ms: u32 = 100,
    heartbeat_ticks: u32 = 2,
    election_timeout_min_ticks: u32 = 10,
    election_timeout_max_ticks: u32 = 20,
    max_entries_per_msg: u32 = 64,
    max_msg_size: u32 = 1_048_576,
    enable_prevote: bool = true,

    pub fn validate(self: Config) !void {
        if (self.election_timeout_min_ticks == 0) return error.InvalidConfig;
        if (self.election_timeout_max_ticks < self.election_timeout_min_ticks) return error.InvalidConfig;
        if (self.heartbeat_ticks == 0) return error.InvalidConfig;
        if (self.heartbeat_ticks >= self.election_timeout_min_ticks) return error.InvalidConfig;
    }
};

pub const ConfigError = error{
    InvalidConfig,
};
pub const RaftWAL = raftwal_mod.RaftWAL;
pub const RaftLogError = raftwal_mod.RaftLogError;

pub const RaftError = error{
    NotLeader,
    InvalidMessage,
    ConfChangeInProgress,
    LastVoter,
    InvalidBytes,
    QueueClosed,
} || RaftLogError || zen.Error;

pub const Peer = struct {
    id: NodeId,
    next_index: u64,
    // Highest log index known to be replicated on this peer (leader view).
    match_index: u64,
    role: PeerRole,
    addr: []const u8 = "",
};

/// fn(ctx, from, to, payload)
pub const SendFn = *const fn (*anyopaque, NodeId, NodeId, []const u8) void;

const serial_mod = @import("serial");
pub const msg_type_raft = serial_mod.msg_type_raft;
pub const msg_type_join_request = serial_mod.msg_type_join_request;

pub const InboundMsg = struct {
    message_type: u16,
    payload: []u8, // owned copy
};

pub const Inbound = []const u8;
pub const ProposalEntry = struct {
    term: u64,
    index: u64,
};

pub const CommitStatus = enum {
    committed,
    not_leader,
    step_down,
    shutdown,
    pending_conf_change,
    last_voter,
    unknown_node,
    existing_node,
    invalid_addr,
    invalid_req,
};

pub const ApplyCallback = *const fn (
    ctx: *anyopaque,
    entry: ProposalEntry,
    status: CommitStatus,
) void;

pub const Node = struct {
    const TimeoutResetReason = enum {
        init,
        grant_vote,
        start_prevote,
        start_election,
        append_entries,
        step_down,
    };

    fn dbg(comptime format: []const u8, args: anytype) void {
        raft_log.debug(format, args);
    }

    id: NodeId,
    peers: std.AutoHashMap(NodeId, Peer),

    current_term: u64,
    voted_for: ?NodeId,
    log: RaftWAL,

    state: State,
    commit_index: std.atomic.Value(u64),
    apply_index: std.atomic.Value(u64),
    conf_applied_index: u64,
    leader_id: ?NodeId,
    pending_conf_change: bool,
    quorum_size: usize,

    votes_received: std.AutoHashMap(NodeId, void),
    prevotes_received: std.AutoHashMap(NodeId, void),
    // peers map stores both membership role and replication state.

    elapsed_ticks: u32,
    election_timeout: u32,
    tick_count: u64,
    config: Config,

    allocator: std.mem.Allocator,

    prng: std.Random.DefaultPrng,
    proposal_requests: ProposalQueue,
    apply_callbacks: zio.Channel(PendingApply) = undefined,
    event_ch: zio.Channel(u8) = undefined,
    commit_ch: zio.Channel(u8) = undefined,
    inbound_ch: zio.Channel(InboundMsg) = undefined,
    shutting_down: std.atomic.Value(bool),

    on_conf_applied: ?*const fn (*anyopaque, ConfChange) void = null,
    on_conf_applied_ctx: ?*anyopaque = null,

    on_apply: ?*const fn (*anyopaque, u64, u64, []const u8) void = null,
    on_apply_ctx: ?*anyopaque = null,

    on_send: ?SendFn = null,
    on_send_ctx: ?*anyopaque = null,

    pub const ProposalType = enum { data, conf };
    const ProposalQueue = utils.Queue(ProposalRequest, 4096);
    const ProposalRequest = struct {
        data: []u8,
        apply_cb: ApplyCallback,
        ctx: *anyopaque,
        proposal_type: ProposalType = .data,
    };
    const PendingApply = struct {
        entry: ProposalEntry,
        cb: ApplyCallback,
        ctx: *anyopaque,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        peers: []const Peer,
        log: RaftWAL,
        config: Config,
        self_role: PeerRole,
    ) !*Node {
        try config.validate();
        const seed = @as(u64, @intCast(std.time.nanoTimestamp())) ^ config.id;
        const prng = std.Random.DefaultPrng.init(seed);
        const node = try allocator.create(Node);
        node.* = .{
            .id = config.id,
            .peers = std.AutoHashMap(NodeId, Peer).init(allocator),
            .current_term = 0,
            .voted_for = null,
            .log = log,
            .state = .follower,
            .commit_index = std.atomic.Value(u64).init(0),
            .apply_index = std.atomic.Value(u64).init(0),
            .conf_applied_index = 0,
            .leader_id = null,
            .pending_conf_change = false,
            .quorum_size = 1,
            .votes_received = std.AutoHashMap(NodeId, void).init(allocator),
            .prevotes_received = std.AutoHashMap(NodeId, void).init(allocator),
            .elapsed_ticks = 0,
            .election_timeout = config.election_timeout_min_ticks,
            .tick_count = 0,
            .config = config,
            .allocator = allocator,
            .prng = prng,
            .proposal_requests = .{},
            .shutting_down = std.atomic.Value(bool).init(false),
        };
        node.apply_callbacks = try zio.Channel(PendingApply).initAlloc(allocator, 1024);
        node.event_ch = try zio.Channel(u8).initAlloc(allocator, 1);
        node.commit_ch = try zio.Channel(u8).initAlloc(allocator, 1);
        node.inbound_ch = try zio.Channel(InboundMsg).initAlloc(allocator, 1024);
        errdefer allocator.destroy(node);
        for (peers) |peer| {
            try node.peers.put(peer.id, peer);
        }
        if (self_role == .learner) {
            node.state = .learner;
        }
        node.recomputeQuorum();
        node.resetElectionTimeout(.init);
        dbg(
            "init node={d} state={s} term={d} election_timeout={d} quorum={d} seed=0x{x}",
            .{
                node.id,
                @tagName(node.state),
                node.current_term,
                node.election_timeout,
                node.quorum_size,
                seed,
            },
        );
        return node;
    }

    pub fn deinit(self: *Node) void {
        self.proposal_requests.close();
        while (self.proposal_requests.tryPop()) |req| {
            self.allocator.free(req.data);
            req.apply_cb(req.ctx, .{ .term = self.current_term, .index = 0 }, .shutdown);
        }
        self.failAllPendingCallbacks(.shutdown);
        self.apply_callbacks.close(.immediate);
        self.apply_callbacks.deinit();
        self.event_ch.close(.immediate);
        self.event_ch.deinit();
        self.commit_ch.close(.immediate);
        self.commit_ch.deinit();

        self.inbound_ch.close(.graceful);
        while (self.inbound_ch.tryReceive()) |msg| {
            self.allocator.free(msg.payload);
        } else |_| {}
        self.inbound_ch.deinit();
        self.votes_received.deinit();
        self.prevotes_received.deinit();
        self.peers.deinit();
        self.allocator.destroy(self);
    }

    /// Restore hard state (term, voted_for, commit_index) from WAL on restart.
    pub fn restoreState(self: *Node) void {
        const hs = self.log.hardState();
        self.current_term = hs.current_term;
        self.voted_for = if (hs.voted_for == 0) null else hs.voted_for;
        self.commit_index.store(hs.commit_index, .seq_cst);
        dbg("restoreState node={d} term={d} voted_for={?d} commit={d}", .{
            self.id, self.current_term, self.voted_for, hs.commit_index,
        });
    }

    /// Restore cluster membership from snapshot conf_state + replay conf change entries.
    /// Updates the node's peers map in place. After this, the caller can iterate
    /// node.peers to hand addresses to the transport.
    pub fn restoreConfState(self: *Node) void {
        const snap = self.log.snapshot();
        if (snap.conf_state) |cs| {
            for (cs.peers) |p| {
                const role: PeerRole = if (p.is_voter) .voter else .learner;
                if (p.id == self.id) {
                    // Restore our own role from conf state.
                    self.state = if (role == .learner) .learner else .follower;
                } else {
                    self.addPeer(p.id, role, p.addr);
                }
            }
        }

        // Replay conf changes from snapshot to commit_index.
        // TODO: pass a flag to entriesRangeRef to only return conf change entries.
        const ci = self.commit_index.load(.seq_cst);
        const start = if (snap.index > 0) snap.index + 1 else self.log.firstIndex();
        if (start > ci) {
            self.conf_applied_index = ci;
            self.recomputeQuorum();
            return;
        }
        const entries = self.log.entriesRangeRef(start, ci + 1, 0) catch {
            self.conf_applied_index = ci;
            self.recomputeQuorum();
            return;
        };
        for (entries) |entry| {
            // TODO: Also apply the data entries.
            const reader = zen.LogEntryReader.parseRoot(entry.bytes) catch continue;
            if (reader.confChange()) |cc_reader| {
                const cc = self.decodeConfChange(cc_reader) catch continue;
                self.applyConfChange(cc);
            }
        }
        self.conf_applied_index = ci;
        self.recomputeQuorum();
        dbg("restoreConfState node={d} peers={d} quorum={d}", .{
            self.id, self.peers.count(), self.quorum_size,
        });
        var peer_it = self.peers.iterator();
        while (peer_it.next()) |kv| {
            const p = kv.value_ptr.*;
            dbg("  peer id={d} role={s} addr={s} match={d} next={d}", .{
                p.id, @tagName(p.role), p.addr, p.match_index, p.next_index,
            });
        }
    }

    /// Build the current conf state from the peers map (including self).
    /// Caller must deinit the returned ConfState.
    pub fn confState(self: *Node) !raftwal_mod.ConfState {
        const total = self.peers.count() + 1; // +1 for self
        var cs_peers = try self.allocator.alloc(raftwal_mod.ConfStatePeer, total);
        errdefer self.allocator.free(cs_peers);
        @memset(cs_peers, .{});

        // Add self.
        const self_is_voter = self.state != .learner;
        cs_peers[0] = .{
            .id = self.id,
            .is_voter = self_is_voter,
            .is_learner = !self_is_voter,
        };

        var i: usize = 1;
        var it = self.peers.valueIterator();
        while (it.next()) |peer| {
            cs_peers[i] = .{
                .id = peer.id,
                .addr = try self.allocator.dupe(u8, peer.addr),
                .is_voter = peer.role == .voter,
                .is_learner = peer.role == .learner,
            };
            i += 1;
        }
        return .{ .peers = cs_peers };
    }

    pub fn startShutdown(self: *Node) void {
        self.shutting_down.store(true, .seq_cst);
        self.proposal_requests.close();
        self.event_ch.close(.graceful);
        self.commit_ch.close(.graceful);
    }

    pub fn isShuttingDown(self: *const Node) bool {
        return self.shutting_down.load(.seq_cst);
    }

    /// Returns true if a heartbeat is due (leader only).
    pub fn tick(self: *Node) RaftError!bool {
        self.tick_count += 1;
        self.elapsed_ticks += 1;
        switch (self.state) {
            .leader => if (self.elapsed_ticks >= self.config.heartbeat_ticks) {
                self.elapsed_ticks = 0;
                return true;
            },
            .follower, .candidate => if (self.elapsed_ticks >= self.election_timeout) {
                dbg(
                    "election-timeout tick={d} node={d} state={s} term={d} elapsed={d} timeout={d}",
                    .{
                        self.tick_count,
                        self.id,
                        @tagName(self.state),
                        self.current_term,
                        self.elapsed_ticks,
                        self.election_timeout,
                    },
                );
                if (self.config.enable_prevote) {
                    try self.startPreVote();
                } else {
                    try self.startElection();
                }
            },
            .learner => {},
        }
        return false;
    }

    // handleInbound would be called concurrently. It's up to us to serialize them.
    pub fn handleInbound(self: *Node, buf: Inbound) RaftError!void {
        switch (zen.parseMessageWithTail(buf) catch return RaftError.InvalidMessage) {
            .RequestVote => |rv| try self.handleRequestVote(rv),
            .RequestVoteReply => |rvr| try self.handleRequestVoteReply(rvr),
            .AppendEntries => |ae| try self.handleAppendEntries(ae),
            .AppendEntriesReply => |aer| try self.handleAppendEntriesReply(aer),
            else => return RaftError.InvalidMessage,
        }
    }

    pub fn startPreVote(self: *Node) RaftError!void {
        if (self.state == .learner) return;
        self.elapsed_ticks = 0;
        self.resetElectionTimeout(.start_prevote);
        dbg(
            "start-prevote tick={d} node={d} term={d} last_idx={d} last_term={d}",
            .{ self.tick_count, self.id, self.current_term, self.log.lastIndex(), self.log.lastTerm() },
        );

        self.prevotes_received.clearRetainingCapacity();
        try self.prevotes_received.put(self.id, {});

        // Single-node cluster: self-vote is already quorum.
        if (self.prevotes_received.count() >= self.quorumSize()) {
            return try self.startElection();
        }

        var it = self.peers.iterator();
        while (it.next()) |kv| {
            try self.sendRequestVote(kv.key_ptr.*, true);
        }
    }

    pub fn startElection(self: *Node) RaftError!void {
        if (self.state == .learner) return;
        self.state = .candidate;
        self.current_term += 1;
        self.voted_for = self.id;
        self.leader_id = null;
        self.elapsed_ticks = 0;
        self.resetElectionTimeout(.start_election);
        dbg(
            "start-election tick={d} node={d} term={d} timeout={d}",
            .{ self.tick_count, self.id, self.current_term, self.election_timeout },
        );

        self.votes_received.clearRetainingCapacity();
        try self.votes_received.put(self.id, {});

        try self.persistHardState();

        // Single-node cluster: self-vote is already quorum.
        if (self.votes_received.count() >= self.quorumSize()) {
            return try self.becomeLeader();
        }

        var it = self.peers.iterator();
        while (it.next()) |kv| {
            try self.sendRequestVote(kv.key_ptr.*, false);
        }
    }

    pub fn becomeLeader(self: *Node) RaftError!void {
        if (self.state == .learner) return;
        self.state = .leader;
        self.leader_id = self.id;
        self.elapsed_ticks = 0;
        dbg("become-leader tick={d} node={d} term={d}", .{ self.tick_count, self.id, self.current_term });

        var it = self.peers.iterator();
        while (it.next()) |kv| {
            kv.value_ptr.next_index = 1;
            kv.value_ptr.match_index = 0;
        }

        self.maybeBroadcast(true);
    }

    /// Send AppendEntries only to peers that are behind (need entries or commit update).
    pub fn maybeBroadcast(self: *Node, do_send: bool) void {
        if (self.state != .leader) return;
        const last = self.log.lastIndex();
        var it = self.peers.iterator();
        while (it.next()) |kv| {
            const peer = kv.value_ptr.*;
            // Send if peer needs entries or doesn't know about our commit.
            if (do_send or peer.match_index < last or peer.next_index <= last) {
                self.sendAppendEntries(kv.key_ptr.*) catch {};
            }
        }
    }

    pub fn sendAppendEntries(self: *Node, peer: NodeId) RaftError!void {
        if (self.state != .leader) return;
        const state = self.peers.getPtr(peer) orelse return;
        const prev_index = if (state.next_index == 0) 0 else state.next_index - 1;
        const prev_term: u64 = if (prev_index == 0) 0 else (try self.log.term(prev_index)) orelse 0;
        const entries = try self.log.entriesFromRef(state.next_index, self.config.max_entries_per_msg);

        var buf = std.ArrayList(u8).empty;
        defer buf.deinit(self.allocator);

        var ae_writer = try zen.AppendEntriesWriter.initWithBuf(self.allocator, &buf);
        defer ae_writer.deinit();

        var entry_offsets = std.ArrayList(u32).empty;
        defer entry_offsets.deinit(self.allocator);
        try entry_offsets.ensureTotalCapacity(self.allocator, entries.len);

        for (entries) |entry_ref| {
            var entry_buf = try zen.LogEntryBuffer.initWithTail(
                self.allocator,
                @constCast(entry_ref.bytes),
                .borrowed,
            );
            defer entry_buf.deinit();

            const entry_off = try entry_buf.cloneInto(self.allocator, &buf);
            try entry_offsets.append(self.allocator, entry_off);
        }

        try ae_writer.set(.{
            .term = self.current_term,
            .leader_id = self.id,
            .prev_log_index = prev_index,
            .prev_log_term = prev_term,
            .entries = entry_offsets.items,
            .leader_commit = self.commit_index.load(.seq_cst),
        });
        _ = try ae_writer.finishWithTail();
        const bytes = try buf.toOwnedSlice(self.allocator);
        self.sendEncoded(peer, bytes);
    }

    fn handleRequestVote(self: *Node, rv: zen.RequestVoteReader) RaftError!void {
        const rv_term = rv.term();
        const is_prevote = rv.isPrevote();
        const candidate_id = rv.candidateId();
        const last_log_index = rv.lastLogIndex();
        const last_log_term = rv.lastLogTerm();
        if (!is_prevote and rv_term > self.current_term) {
            try self.stepDown(rv_term);
        }

        if (rv_term < self.current_term) {
            dbg(
                "vote-reject tick={d} node={d} candidate={d} reason=stale_term rv_term={d} cur_term={d}",
                .{ self.tick_count, self.id, candidate_id, rv_term, self.current_term },
            );
            try self.sendRequestVoteReply(candidate_id, false, is_prevote);
            return;
        }

        if (self.state == .learner) {
            dbg(
                "vote-reject tick={d} node={d} candidate={d} reason=learner",
                .{ self.tick_count, self.id, candidate_id },
            );
            try self.sendRequestVoteReply(candidate_id, false, is_prevote);
            return;
        }

        const up_to_date = self.isLogUpToDate(last_log_term, last_log_index);
        const can_vote = self.voted_for == null or self.voted_for.? == candidate_id;

        var granted = false;
        if (is_prevote) {
            granted = up_to_date;
        } else if (can_vote and up_to_date) {
            self.voted_for = candidate_id;
            self.elapsed_ticks = 0;
            self.resetElectionTimeout(.grant_vote);
            try self.persistHardState();
            granted = true;
        }
        if (is_prevote) {
            dbg(
                "prevote tick={d} node={d} candidate={d} term={d} granted={} up_to_date={} cand_last=({d},{d}) self_last=({d},{d})",
                .{
                    self.tick_count,
                    self.id,
                    candidate_id,
                    rv_term,
                    granted,
                    up_to_date,
                    last_log_term,
                    last_log_index,
                    self.log.lastTerm(),
                    self.log.lastIndex(),
                },
            );
        } else {
            dbg(
                "vote tick={d} node={d} candidate={d} term={d} granted={} can_vote={} up_to_date={} voted_for={any} cand_last=({d},{d}) self_last=({d},{d})",
                .{
                    self.tick_count,
                    self.id,
                    candidate_id,
                    rv_term,
                    granted,
                    can_vote,
                    up_to_date,
                    self.voted_for,
                    last_log_term,
                    last_log_index,
                    self.log.lastTerm(),
                    self.log.lastIndex(),
                },
            );
        }

        try self.sendRequestVoteReply(candidate_id, granted, is_prevote);
    }

    fn handleRequestVoteReply(self: *Node, rvr: zen.RequestVoteReplyReader) RaftError!void {
        const is_prevote = rvr.isPrevote();
        const term = rvr.term();
        if (!is_prevote and term > self.current_term) {
            try self.stepDown(term);
            return;
        }
        if (term < self.current_term) return;
        if (!rvr.voteGranted()) return;

        const from = rvr.from();
        if (is_prevote) {
            if (self.state == .learner) return;
            _ = try self.prevotes_received.put(from, {});
            dbg(
                "prevote-reply tick={d} node={d} from={d} term={d} count={d} quorum={d}",
                .{
                    self.tick_count,
                    self.id,
                    from,
                    self.current_term,
                    self.prevotes_received.count(),
                    self.quorumSize(),
                },
            );
            if (self.prevotes_received.count() >= self.quorumSize()) {
                try self.startElection();
            }
            return;
        }

        if (self.state != .candidate) return;
        _ = try self.votes_received.put(from, {});
        dbg(
            "vote-reply tick={d} node={d} from={d} term={d} count={d} quorum={d}",
            .{
                self.tick_count,
                self.id,
                from,
                self.current_term,
                self.votes_received.count(),
                self.quorumSize(),
            },
        );
        if (self.votes_received.count() >= self.quorumSize()) {
            try self.becomeLeader();
        }
    }

    pub fn handleAppendEntries(self: *Node, ae: zen.AppendEntriesReader) RaftError!void {
        const ae_term = ae.term();
        const leader_id = ae.leaderId();
        if (ae_term > self.current_term) {
            try self.stepDown(ae_term);
        }
        if (ae_term < self.current_term) {
            try self.sendAppendEntriesReply(leader_id, .{
                .success = false,
                .match_index = 0,
                .conflict_term = 0,
                .conflict_index = self.log.lastIndex() + 1,
            });
            return;
        }

        if (self.state != .follower and self.state != .learner) {
            self.state = .follower;
        }
        self.leader_id = leader_id;
        self.elapsed_ticks = 0;
        self.resetElectionTimeout(.append_entries);

        if (ae.prevLogIndex() > 0) {
            const prev_term_opt = try self.log.term(ae.prevLogIndex());
            if (prev_term_opt == null or prev_term_opt.? != ae.prevLogTerm()) {
                try self.sendAppendEntriesReply(leader_id, .{
                    .success = false,
                    .match_index = 0,
                    .conflict_term = prev_term_opt orelse 0,
                    .conflict_index = if (prev_term_opt) |t| self.log.firstIndexOfTerm(t) else self.log.lastIndex() + 1,
                });
                return;
            }
        }

        var append_from: ?usize = null;
        const ae_entries_len = ae.entriesLen();
        if (ae_entries_len > 0) {
            var idx: u32 = 0;
            while (idx < ae_entries_len) : (idx += 1) {
                const log_entry = try ae.entriesAt(idx);
                const term_opt = try self.log.term(log_entry.index());
                if (term_opt == null or term_opt.? != log_entry.term()) {
                    append_from = @intCast(idx);
                    break;
                }
            }

            var entries_to_append = std.ArrayList(zen.LogEntryBuffer).empty;
            defer {
                for (entries_to_append.items) |*entry| {
                    entry.deinit();
                }
                entries_to_append.deinit(self.allocator);
            }

            const start_idx: usize = if (append_from) |v| v else ae_entries_len;
            for (start_idx..ae_entries_len) |append_idx| {
                const entry = try ae.entriesAtClone(self.allocator, @intCast(append_idx));
                try entries_to_append.append(self.allocator, entry);
            }
            if (entries_to_append.items.len > 0) {
                dbg("node {d}: appending {d} entries to log", .{ self.id, entries_to_append.items.len });
                try self.log.append(entries_to_append.items);
                try self.log.sync();
            }
        }

        // utils.logEvery(10, "node {d}: handleAE entries={d} leaderCommit={d} ci={d} lastIndex={d}", .{
        //     self.id, ae_entries_len, ae.leaderCommit(), self.commit_index.load(.seq_cst), self.log.lastIndex(),
        // });

        if (ae.leaderCommit() > self.commit_index.load(.seq_cst)) {
            const last_index = self.log.lastIndex();
            const new_ci = if (ae.leaderCommit() < last_index) ae.leaderCommit() else last_index;
            dbg("node {d}: advancing commit_index {d} -> {d}", .{ self.id, self.commit_index.load(.seq_cst), new_ci });
            self.commit_index.store(new_ci, .seq_cst);
        }
        try self.sendAppendEntriesReply(leader_id, .{
            .success = true,
            .match_index = self.log.lastIndex(),
            .conflict_term = 0,
            .conflict_index = 0,
        });
    }

    pub fn handleAppendEntriesReply(self: *Node, aer: zen.AppendEntriesReplyReader) RaftError!void {
        if (self.state != .leader) return;

        const term = aer.term();
        if (term < self.current_term) return;
        if (term > self.current_term) {
            try self.stepDown(term);
            return;
        }

        const from = aer.from();
        if (!aer.success()) {
            if (self.peers.getPtr(from)) |state| {
                const conflict_index = aer.conflictIndex();
                if (conflict_index > 0) {
                    state.next_index = conflict_index;
                } else if (state.next_index > 1) {
                    state.next_index -= 1;
                }
            }
            return;
        }

        if (self.peers.getPtr(from)) |state| {
            const match_index = aer.matchIndex();
            state.match_index = match_index;
            state.next_index = match_index + 1;
        }
    }

    /// Encode a ConfChange and propose it through the normal proposal queue.
    /// For fire-and-forget (e.g., join requests), use a no-op callback.
    pub fn proposeConfChange(
        self: *Node,
        cc: ConfChange,
        cb: ApplyCallback,
        ctx: *anyopaque,
    ) RaftError!void {
        var buf = std.ArrayList(u8).empty;
        defer buf.deinit(self.allocator);

        dbg("proposeConfChange: node_id={d} type={s} addr_len={d} addr={s}", .{
            cc.node_id, @tagName(cc.change_type), cc.addr.len, cc.addr,
        });

        var cc_writer = try zen.ConfChangeWriter.initWithBuf(self.allocator, &buf);
        defer cc_writer.deinit();
        try cc_writer.set(.{
            .add_voter = cc.change_type == .add_voter,
            .add_learner = cc.change_type == .add_learner,
            .to_voter = cc.change_type == .to_voter,
            .to_learner = cc.change_type == .to_learner,
            .remove_node = cc.change_type == .remove_node,
            .node_id = cc.node_id,
            .addr = cc.addr,
        });
        _ = try cc_writer.finishWithTail();
        try self.proposeWithCallback(buf.items, cb, ctx, .conf);
    }

    /// Propose a data change (normal mutation) through the raft proposal queue.
    pub fn proposeDataChange(
        self: *Node,
        data: []const u8,
        cb: ApplyCallback,
        ctx: *anyopaque,
    ) RaftError!void {
        return self.proposeWithCallback(data, cb, ctx, .data);
    }

    fn proposeWithCallback(
        self: *Node,
        data: []const u8,
        cb: ApplyCallback,
        ctx: *anyopaque,
        proposal_type: ProposalType,
    ) RaftError!void {
        if (self.isShuttingDown()) return error.QueueClosed;
        const owned = try self.allocator.dupe(u8, data);
        errdefer self.allocator.free(owned);
        try self.proposal_requests.pushOrClosed(.{
            .data = owned,
            .apply_cb = cb,
            .ctx = ctx,
            .proposal_type = proposal_type,
        });
        self.event_ch.trySend(1) catch {};
    }

    fn appendProposalsToLeaderLog(self: *Node, requests: []const ProposalRequest) RaftError!u64 {
        if (self.state != .leader) return RaftError.NotLeader;
        if (requests.len == 0) return self.log.lastIndex() + 1;

        var entries = std.ArrayList(zen.LogEntryBuffer).empty;
        defer {
            for (entries.items) |*entry| entry.deinit();
            entries.deinit(self.allocator);
        }
        try entries.ensureTotalCapacity(self.allocator, requests.len);

        const first_index = self.log.lastIndex() + 1;
        var index = first_index;
        var has_conf = false;
        for (requests) |req| {
            const is_conf = req.proposal_type == .conf;
            if (is_conf) {
                if (self.pending_conf_change or has_conf) {
                    req.apply_cb(
                        req.ctx,
                        .{ .term = self.current_term, .index = 0 },
                        .pending_conf_change,
                    );
                    continue;
                }

                const cc = try zen.ConfChangeReader.init(req.data, 0);
                // TODO: Switch to enum here.
                if (!cc.addVoter() and !cc.addLearner() and !cc.toVoter() and !cc.toLearner() and !cc.removeNode()) {
                    req.apply_cb(req.ctx, .{ .term = self.current_term, .index = 0 }, .invalid_req);
                    continue;
                }
                // Reject add if addr is missing or unparseable.
                if (cc.addVoter() or cc.addLearner()) {
                    _ = zio.net.IpAddress.parseIpAndPort(cc.addr()) catch {
                        req.apply_cb(req.ctx, .{ .term = self.current_term, .index = 0 }, .invalid_addr);
                        continue;
                    };
                    if (cc.nodeId() == self.id or self.peers.contains(cc.nodeId())) {
                        // This node is already part of the group.
                        req.apply_cb(req.ctx, .{ .term = self.current_term, .index = 0 }, .existing_node);
                        continue;
                    }
                }

                // Reject if the target peer doesn't exist for changes that require it.
                if (cc.toVoter() or cc.toLearner() or cc.removeNode()) {
                    if (cc.nodeId() != self.id and !self.peers.contains(cc.nodeId())) {
                        req.apply_cb(req.ctx, .{ .term = self.current_term, .index = 0 }, .unknown_node);
                        continue;
                    }
                }

                // Reject if this would remove the last voter.
                if (self.quorum_size <= 1 and (cc.toLearner() or cc.removeNode())) {
                    req.apply_cb(req.ctx, .{ .term = self.current_term, .index = 0 }, .last_voter);
                    continue;
                }
                has_conf = true;
            }

            var buf = std.ArrayList(u8).empty;
            defer buf.deinit(self.allocator);

            var writer = try zen.LogEntryWriter.initWithBuf(self.allocator, &buf);
            defer writer.deinit();

            var cc_offset: u32 = zen.NullOffset;
            if (is_conf) {
                // req.data is a serialized ConfChange. Clone it into buf and get the offset.
                var cc_buf = try zen.ConfChangeBuffer.initWithTail(
                    self.allocator,
                    req.data,
                    .borrowed,
                );
                defer cc_buf.deinit();
                cc_offset = try cc_buf.cloneInto(self.allocator, &buf);
            }

            try writer.set(.{
                .index = index,
                .term = self.current_term,
                .data = if (is_conf) &[_]u8{} else req.data,
                .is_normal = !is_conf,
                .is_conf_change = is_conf,
                .conf_change = cc_offset,
            });
            _ = try writer.finishWithTail();

            const data = try buf.toOwnedSlice(self.allocator);
            var entry = try zen.LogEntryBuffer.initWithTail(self.allocator, data, .owned);
            errdefer entry.deinit();

            try entries.append(self.allocator, entry);
            index += 1;
        }

        try self.log.append(entries.items);
        try self.log.sync();
        if (has_conf) self.pending_conf_change = true;
        return first_index;
    }

    // processProposalRequests is just checking for proposals in proposal_requests queue, and
    // proposing them. This is only relevant to the leader node. Followers won't have this.
    pub fn processProposalRequests(self: *Node, max_batch: usize) RaftError!usize {
        if (self.isShuttingDown()) {
            while (self.proposal_requests.tryPop()) |req| {
                self.allocator.free(req.data);
                req.apply_cb(req.ctx, .{ .term = self.current_term, .index = 0 }, .shutdown);
            }
            return 0;
        }
        var batch = std.ArrayList(ProposalRequest).empty;
        defer batch.deinit(self.allocator);

        while (batch.items.len < max_batch) {
            const req = self.proposal_requests.tryPop() orelse break;
            try batch.append(self.allocator, req);
        }
        if (batch.items.len == 0) return 0;

        defer for (batch.items) |req| self.allocator.free(req.data);

        const term = self.current_term;
        const first_index = self.appendProposalsToLeaderLog(batch.items) catch |err| switch (err) {
            error.NotLeader => {
                for (batch.items) |req| {
                    req.apply_cb(req.ctx, .{ .term = term, .index = 0 }, .not_leader);
                }
                return batch.items.len;
            },
            else => return err,
        };

        for (batch.items, 0..) |req, i| {
            const entry = ProposalEntry{
                .term = term,
                .index = first_index + @as(u64, @intCast(i)),
            };
            self.apply_callbacks.trySend(.{
                .entry = entry,
                .cb = req.apply_cb,
                .ctx = req.ctx,
            }) catch {
                req.apply_cb(req.ctx, entry, .shutdown);
            };
        }

        return batch.items.len;
    }

    fn notifyApplyCallbacks(self: *Node) void {
        // DO NOT BLOCK
        self.commit_ch.trySend(1) catch {};
    }

    pub fn applyLoop(self: *Node) void {
        while (!self.isShuttingDown()) {
            _ = self.commit_ch.receive() catch return;
            self.applyCommits();
        }
    }

    const max_apply_bytes: u64 = 16 * 1024 * 1024; // 16 MiB per batch

    pub fn applyCommits(self: *Node) void {
        const ci = self.commit_index.load(.seq_cst);
        var ai = self.apply_index.load(.seq_cst);
        if (ai >= ci) {
            return;
        }
        dbg(
            "applyCommits.1 ticks: {d} node {d}: applyCommits commit_index={d} apply_index={d}",
            .{ self.tick_count, self.id, ci, ai },
        );

        while (ai < ci) {
            const entries = self.log.entriesRangeRef(ai + 1, ci + 1, max_apply_bytes) catch return;
            if (entries.len == 0) return;

            for (entries) |entry| {
                const reader = zen.LogEntryReader.parseRoot(entry.bytes) catch continue;

                if (reader.isNormal()) {
                    if (self.on_apply) |cb| {
                        if (reader.data().len > 0) {
                            cb(self.on_apply_ctx.?, entry.index, entry.term, reader.data());
                        }
                    }
                }
                ai = entry.index;
                self.apply_index.store(ai, .seq_cst);

                // Fire proposal callback if one exists for this index (leader only).
                const head = self.apply_callbacks.peek() catch continue;
                if (head.entry.index == entry.index) {
                    const ready = self.apply_callbacks.tryReceive() catch continue;
                    ready.cb(ready.ctx, ready.entry, .committed);
                }
            }
        }
        dbg(
            "applyCommits.2 ticks: {d} node {d}: applyCommits ai={d} ci={d}",
            .{ self.tick_count, self.id, ai, ci },
        );
    }

    /// Tick loop: periodically signals event_ch to trigger raft processing.
    pub fn tickLoop(self: *Node, tick_interval_ms: u32) void {
        const tick_dur = zio.Duration.fromMilliseconds(tick_interval_ms);
        while (!self.isShuttingDown()) {
            zio.sleep(tick_dur) catch return;
            self.event_ch.trySend(1) catch {};
        }
    }

    /// Main processing loop. Blocks on event_ch, drains inbound messages,
    /// processes proposals, advances state, and broadcasts to peers.
    pub fn mainLoop(self: *Node, tick_interval_ms: u32) void {
        const tick_us: i64 = @as(i64, tick_interval_ms) * 1_000;
        var last_tick: i64 = std.time.microTimestamp();

        while (!self.isShuttingDown()) {
            _ = self.event_ch.receive() catch return;

            // Drain all queued inbound messages.
            while (self.inbound_ch.tryReceive()) |msg| {
                defer self.allocator.free(msg.payload);
                switch (msg.message_type) {
                    msg_type_raft => {
                        self.handleInbound(msg.payload) catch {};
                    },
                    else => {},
                }
            } else |_| {}

            const ci_before = self.commit_index.load(.seq_cst);
            // TODO: What happens to the rest of the requests?
            _ = self.processProposalRequests(64) catch {};
            self.advance();
            const ci_advanced = self.commit_index.load(.seq_cst) > ci_before;

            var heartbeat_due = false;
            const now = std.time.microTimestamp();
            if (now - last_tick >= tick_us) {
                heartbeat_due = self.tick() catch false;
                last_tick = now;
            }
            self.maybeBroadcast(heartbeat_due or ci_advanced);
        }
    }

    pub fn isLeader(self: *const Node) bool {
        return self.state == .leader;
    }

    pub fn getLeaderId(self: *const Node) ?NodeId {
        return self.leader_id;
    }

    pub fn clusterSize(self: *const Node) u32 {
        return @intCast(self.peers.count() + 1);
    }

    pub fn persistHardState(self: *Node) RaftError!void {
        try self.log.setHardState(.{
            .current_term = self.current_term,
            .voted_for = self.voted_for orelse 0,
            .commit_index = self.commit_index.load(.seq_cst),
        });
        try self.log.sync();
    }

    pub fn persistLog(self: *Node, entries: []const zen.LogEntryBuffer) RaftError!void {
        try self.log.append(entries);
        try self.log.sync();
    }

    pub fn isLogUpToDate(self: *const Node, candidate_term: u64, candidate_index: u64) bool {
        const last_term = self.log.lastTerm();
        if (candidate_term > last_term) return true;
        if (candidate_term < last_term) return false;
        return candidate_index >= self.log.lastIndex();
    }

    fn sendEncoded(self: *Node, to: NodeId, payload: []u8) void {
        if (self.on_send) |send_fn| {
            send_fn(self.on_send_ctx.?, self.id, to, payload);
        }
        self.allocator.free(payload);
    }

    fn decodeConfChange(self: *Node, reader: zen.ConfChangeReader) RaftError!ConfChange {
        _ = self;
        const change_type: ConfChangeType = if (reader.addVoter())
            .add_voter
        else if (reader.addLearner())
            .add_learner
        else if (reader.toVoter())
            .to_voter
        else if (reader.toLearner())
            .to_learner
        else if (reader.removeNode())
            .remove_node
        else
            return RaftError.InvalidMessage;
        return .{
            .change_type = change_type,
            .node_id = reader.nodeId(),
            .addr = reader.addr(),
        };
    }

    fn sendRequestVote(self: *Node, to: NodeId, is_prevote: bool) !void {
        var writer = try zen.RequestVoteWriter.init(self.allocator);
        defer writer.deinit();

        try writer.set(.{
            .term = self.current_term,
            .candidate_id = self.id,
            .last_log_index = self.log.lastIndex(),
            .last_log_term = self.log.lastTerm(),
            .is_prevote = is_prevote,
        });
        const out = try writer.finishWithTail();
        const bytes = out.bytes orelse return zen.Error.Invalid;
        self.sendEncoded(to, bytes);
    }

    fn sendRequestVoteReply(self: *Node, to: NodeId, vote_granted: bool, is_prevote: bool) !void {
        var writer = try zen.RequestVoteReplyWriter.init(self.allocator);
        defer writer.deinit();

        try writer.set(.{
            .term = self.current_term,
            .vote_granted = vote_granted,
            .from = self.id,
            .is_prevote = is_prevote,
        });
        const buf = try writer.finishWithTail();
        const bytes = buf.bytes orelse return zen.Error.Invalid;
        self.sendEncoded(to, bytes);
    }

    fn sendAppendEntriesReply(self: *Node, to: NodeId, opts: struct {
        success: bool,
        match_index: u64,
        conflict_term: u64,
        conflict_index: u64,
    }) !void {
        var writer = try zen.AppendEntriesReplyWriter.init(self.allocator);
        defer writer.deinit();

        try writer.set(.{
            .term = self.current_term,
            .success = opts.success,
            .from = self.id,
            .match_index = opts.match_index,
            .conflict_term = opts.conflict_term,
            .conflict_index = opts.conflict_index,
        });
        const buf = try writer.finishWithTail();
        const bytes = buf.bytes orelse return zen.Error.Invalid;
        self.sendEncoded(to, bytes);
    }

    pub fn quorumSize(self: *const Node) usize {
        return self.quorum_size;
    }

    fn advanceCommitIndex(self: *Node) void {
        if (self.state != .leader) return;

        var matches = std.ArrayList(u64).empty;
        defer matches.deinit(self.allocator);

        // Leader always has its own log up to lastIndex.
        matches.append(self.allocator, self.log.lastIndex()) catch return;

        var it = self.peers.valueIterator();
        while (it.next()) |peer| {
            if (peer.role != .voter) continue;
            matches.append(self.allocator, peer.match_index) catch return;
        }

        if (matches.items.len == 0) return;

        std.mem.sort(u64, matches.items, {}, comptime std.sort.asc(u64));

        const quorum_pos = matches.items.len - self.quorum_size;
        const candidate = matches.items[quorum_pos];
        if (candidate <= self.commit_index.load(.seq_cst)) return;

        const term = self.log.term(candidate) catch return orelse return;
        if (term != self.current_term) return;
        self.commit_index.store(candidate, .seq_cst);
    }

    // commit_index udvances when leader sees a quorum match.
    // apply_index advances when entries are applied to the state machine (via callbacks).
    // conf_applied_index advances when conf changes are applied to raft membership.
    pub fn advance(self: *Node) void {
        self.advanceCommitIndex();
        self.applyConfChanges();
        self.notifyApplyCallbacks();
    }

    fn applyConfChanges(self: *Node) void {
        const ci = self.commit_index.load(.seq_cst);
        if (self.conf_applied_index < ci) {
            dbg("applyConfChanges node={d} conf_applied={d} ci={d}", .{ self.id, self.conf_applied_index, ci });
        }
        while (self.conf_applied_index < ci) {
            self.conf_applied_index += 1;
            // TODO: Quickly find all the conf change requests between conf_applied_index and ci.
            // And just loop over them, instead of fetching entry for every index.
            const entry = (self.log.entryRef(self.conf_applied_index) catch continue) orelse continue;
            const reader = zen.LogEntryReader.parseRoot(entry.bytes) catch continue;
            if (reader.confChange()) |cc_reader| {
                const cc = self.decodeConfChange(cc_reader) catch continue;
                self.applyConfChange(cc);
                self.pending_conf_change = false;
            }
        }
    }

    fn applyConfChange(self: *Node, cc: ConfChange) void {
        if (cc.node_id == self.id) {
            // Update our own role.
            switch (cc.change_type) {
                .add_voter, .to_voter => {
                    if (self.state == .learner) self.state = .follower;
                },
                .add_learner, .to_learner => self.state = .learner,
                .remove_node => {
                    // We've been removed from the cluster. Shut down.
                    dbg("node {d} removed from cluster, shutting down", .{self.id});
                    self.startShutdown();
                },
            }
        } else {
            switch (cc.change_type) {
                .add_voter => self.addPeer(cc.node_id, .voter, cc.addr),
                .add_learner => self.addPeer(cc.node_id, .learner, cc.addr),
                .to_voter => self.setPeerRole(cc.node_id, .voter),
                .to_learner => self.setPeerRole(cc.node_id, .learner),
                .remove_node => self.removePeer(cc.node_id),
            }
        }
        if (self.on_conf_applied) |cb| {
            cb(self.on_conf_applied_ctx.?, cc);
        }
    }

    fn hasPeer(self: *Node, id: NodeId) bool {
        return self.peers.contains(id);
    }

    fn addPeer(self: *Node, id: NodeId, role: PeerRole, addr: []const u8) void {
        if (self.hasPeer(id)) return;
        _ = self.peers.put(id, .{
            .id = id,
            .next_index = self.log.lastIndex() + 1,
            .match_index = 0,
            .role = role,
            .addr = addr,
        }) catch {
            @panic("raft: addPeer out of memory");
        };
        self.recomputeQuorum();
    }

    fn removePeer(self: *Node, id: NodeId) void {
        _ = self.peers.remove(id);
        self.recomputeQuorum();
        if (id == self.id) {
            self.state = .learner;
        }
    }

    fn setPeerRole(self: *Node, id: NodeId, role: PeerRole) void {
        if (self.peers.getPtr(id)) |peer| {
            peer.role = role;
            self.recomputeQuorum();
        } else {
            // If it's not a peer, treat as add with desired role.
            self.addPeer(id, role, "");
            return;
        }
        if (id == self.id) {
            if (role == .learner) {
                self.state = .learner;
            } else if (self.state == .learner) {
                self.state = .follower;
            }
        }
    }

    fn recomputeQuorum(self: *Node) void {
        var voters: usize = 0;
        if (self.state != .learner) voters += 1;
        var it = self.peers.valueIterator();
        while (it.next()) |peer| {
            if (peer.role == .voter) voters += 1;
        }
        self.quorum_size = (voters / 2) + 1;
        dbg("node={d} quorum_size={d}", .{ self.id, self.quorum_size });
    }

    fn resetElectionTimeout(self: *Node, reason: TimeoutResetReason) void {
        const old_timeout = self.election_timeout;
        const rand = self.prng.random();
        const draw = rand.int(u32);
        const min = self.config.election_timeout_min_ticks;
        const max = self.config.election_timeout_max_ticks;
        const span = (max - min) + 1;
        self.election_timeout = min + (draw % span);

        _ = old_timeout;
        _ = reason;
        // utils.logEvery(5, "timeout-reset tick={d} node={d} reason={s} old={d} new={d} range=[{d},{d}] elapsed={d}", .{
        //     self.tick_count,
        //     self.id,
        //     @tagName(reason),
        //     old_timeout,
        //     self.election_timeout,
        //     min,
        //     max,
        //     self.elapsed_ticks,
        // });
    }

    fn stepDown(self: *Node, new_term: u64) RaftError!void {
        const prev_state = self.state;
        const prev_term = self.current_term;
        if (self.state == .learner) {
            self.state = .learner;
        } else {
            self.state = .follower;
        }
        self.current_term = new_term;
        self.voted_for = null;
        self.leader_id = null;
        self.elapsed_ticks = 0;
        self.resetElectionTimeout(.step_down);
        self.failAllPendingCallbacks(.step_down);
        dbg(
            "step-down tick={d} node={d} {s}/term={d} -> {s}/term={d}",
            .{ self.tick_count, self.id, @tagName(prev_state), prev_term, @tagName(self.state), self.current_term },
        );
        try self.persistHardState();
    }

    fn failAllPendingCallbacks(self: *Node, status: CommitStatus) void {
        while (true) {
            const head = self.apply_callbacks.tryReceive() catch break;
            head.cb(head.ctx, head.entry, status);
        }
    }

    // TESTING ONLY
    // _propose is and should only being used by tests.
    pub fn _propose(self: *Node, data: []const u8) RaftError!u64 {
        if (self.state != .leader) return RaftError.NotLeader;

        const new_index = self.log.lastIndex() + 1;
        var writer = try zen.LogEntryWriter.init(self.allocator);
        defer writer.deinit();
        try writer.set(.{
            .index = new_index,
            .term = self.current_term,
            .data = data,
            .is_normal = true,
            .is_conf_change = false,
            .conf_change = zen.NullOffset,
        });
        var entry = try writer.finishWithTail();
        defer entry.deinit();

        try self.log.append(&[_]zen.LogEntryBuffer{entry});
        try self.log.sync();
        self.advance();
        self.maybeBroadcast(true);
        return new_index;
    }
};
