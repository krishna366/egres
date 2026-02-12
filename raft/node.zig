const std = @import("std");
const msg = @import("message.zig");
const raftlog_mod = @import("raftlog.zig");
const config_mod = @import("config.zig");
const raft_log = std.log.scoped(.raft);

pub const NodeId = msg.NodeId;
pub const Message = msg.Message;
pub const MessageType = msg.MessageType;
pub const RequestVote = msg.RequestVote;
pub const RequestVoteReply = msg.RequestVoteReply;
pub const PreVote = msg.PreVote;
pub const PreVoteReply = msg.PreVoteReply;
pub const AppendEntries = msg.AppendEntries;
pub const AppendEntriesReply = msg.AppendEntriesReply;
pub const LogEntry = msg.LogEntry;
pub const State = msg.State;
pub const EntryType = msg.EntryType;
pub const ConfChange = msg.ConfChange;
pub const ConfChangeType = msg.ConfChangeType;

pub const PeerRole = enum {
    voter,
    learner,
};

pub const Config = config_mod.Config;
pub const RaftLog = raftlog_mod.RaftLog;
pub const RaftLogError = raftlog_mod.RaftLogError;

pub const RaftError = error{
    NotLeader,
    InvalidMessage,
    ConfChangeInProgress,
} || RaftLogError;

pub const Peer = struct {
    id: NodeId,
    next_index: u64,
    // Highest log index known to be replicated on this peer (leader view).
    match_index: u64,
    role: PeerRole,
};

pub const Outbound = struct {
    to: NodeId,
    msg: Message,
};

pub const Node = struct {
    pub const DebugOutput = enum {
        debug_print,
        none,
    };

    const TimeoutResetReason = enum {
        init,
        grant_vote,
        start_prevote,
        start_election,
        append_entries,
        step_down,
    };

    pub var debug_output_mode: DebugOutput = .none;

    fn dbg(comptime format: []const u8, args: anytype) void {
        switch (debug_output_mode) {
            .debug_print => std.debug.print("[raft] " ++ format ++ "\n", args),
            .none => return,
        }
    }

    id: NodeId,
    peers: std.AutoHashMap(NodeId, Peer),

    current_term: u64,
    voted_for: ?NodeId,
    log: RaftLog,

    state: State,
    commit_index: u64,
    last_applied: u64,
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

    outbox: std.ArrayList(Outbound),
    prng: std.Random.DefaultPrng,

    pub fn init(
        allocator: std.mem.Allocator,
        id: NodeId,
        peers: []const Peer,
        log: RaftLog,
        config: Config,
        self_role: PeerRole,
    ) !Node {
        try config.validate();
        const seed = @as(u64, @intCast(std.time.nanoTimestamp())) ^ id;
        const prng = std.Random.DefaultPrng.init(seed);
        var node = Node{
            .id = id,
            .peers = std.AutoHashMap(NodeId, Peer).init(allocator),
            .current_term = 0,
            .voted_for = null,
            .log = log,
            .state = .follower,
            .commit_index = 0,
            .last_applied = 0,
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
            .outbox = std.ArrayList(Outbound).empty,
            .prng = prng,
        };
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
        self.votes_received.deinit();
        self.prevotes_received.deinit();
        self.peers.deinit();
        self.outbox.deinit(self.allocator);
    }

    pub fn tick(self: *Node) RaftError!void {
        self.tick_count += 1;
        self.elapsed_ticks += 1;
        switch (self.state) {
            .leader => if (self.elapsed_ticks >= self.config.heartbeat_ticks) {
                self.elapsed_ticks = 0;
                try self.broadcastHeartbeat();
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
    }

    pub fn handleMessage(self: *Node, message: Message) RaftError!void {
        const msg_term = message.term();
        if (msg_term > self.current_term) {
            try self.stepDown(msg_term);
        }

        switch (message) {
            .request_vote => |rv| try self.handleRequestVote(rv),
            .request_vote_reply => |rvr| try self.handleRequestVoteReply(rvr),
            .append_entries => |ae| try self.handleAppendEntries(ae),
            .append_entries_reply => |aer| try self.handleAppendEntriesReply(aer),
            .pre_vote => |pv| try self.handlePreVote(pv),
            .pre_vote_reply => |pvr| try self.handlePreVoteReply(pvr),
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

        const pv = PreVote{
            .term = self.current_term,
            .candidate_id = self.id,
            .last_log_index = self.log.lastIndex(),
            .last_log_term = self.log.lastTerm(),
        };

        var it = self.peers.iterator();
        while (it.next()) |kv| {
            self.send(kv.key_ptr.*, Message{ .pre_vote = pv });
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

        const rv = RequestVote{
            .term = self.current_term,
            .candidate_id = self.id,
            .last_log_index = self.log.lastIndex(),
            .last_log_term = self.log.lastTerm(),
        };

        var it = self.peers.iterator();
        while (it.next()) |kv| {
            self.send(kv.key_ptr.*, Message{ .request_vote = rv });
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

        try self.broadcastHeartbeat();
    }

    pub fn broadcastHeartbeat(self: *Node) RaftError!void {
        var it = self.peers.iterator();
        while (it.next()) |kv| {
            try self.sendAppendEntries(kv.key_ptr.*);
        }
    }

    pub fn sendAppendEntries(self: *Node, peer: NodeId) RaftError!void {
        if (self.state != .leader) return;
        const state = self.peers.getPtr(peer) orelse return;
        const next_index = state.next_index;
        const prev_index = if (next_index == 0) 0 else next_index - 1;
        var prev_term: u64 = 0;
        if (prev_index != 0) {
            if (self.log.entry(prev_index)) |log_entry| {
                prev_term = log_entry.term;
            }
        }
        const entries = self.log.entriesFrom(next_index, self.config.max_entries_per_msg);
        const ae = AppendEntries{
            .term = self.current_term,
            .leader_id = self.id,
            .prev_log_index = prev_index,
            .prev_log_term = prev_term,
            .entries = entries,
            .leader_commit = self.commit_index,
        };
        self.send(peer, Message{ .append_entries = ae });
    }

    pub fn handleRequestVote(self: *Node, rv: RequestVote) RaftError!void {
        if (rv.term < self.current_term) {
            dbg(
                "vote-reject tick={d} node={d} candidate={d} reason=stale_term rv_term={d} cur_term={d}",
                .{ self.tick_count, self.id, rv.candidate_id, rv.term, self.current_term },
            );
            self.send(rv.candidate_id, Message{ .request_vote_reply = .{
                .term = self.current_term,
                .vote_granted = false,
                .from = self.id,
            } });
            return;
        }

        if (self.state == .learner) {
            dbg(
                "vote-reject tick={d} node={d} candidate={d} reason=learner",
                .{ self.tick_count, self.id, rv.candidate_id },
            );
            self.send(rv.candidate_id, Message{ .request_vote_reply = .{
                .term = self.current_term,
                .vote_granted = false,
                .from = self.id,
            } });
            return;
        }

        const can_vote = self.voted_for == null or self.voted_for.? == rv.candidate_id;
        const up_to_date = self.isLogUpToDate(rv.last_log_term, rv.last_log_index);

        var granted = false;
        if (can_vote and up_to_date) {
            self.voted_for = rv.candidate_id;
            self.elapsed_ticks = 0;
            self.resetElectionTimeout(.grant_vote);
            try self.persistHardState();
            granted = true;
        }
        const self_last_term = self.log.lastTerm();
        const self_last_index = self.log.lastIndex();
        dbg(
            "vote tick={d} node={d} candidate={d} term={d} granted={} can_vote={} up_to_date={} voted_for={any} cand_last=({d},{d}) self_last=({d},{d})",
            .{
                self.tick_count,
                self.id,
                rv.candidate_id,
                rv.term,
                granted,
                can_vote,
                up_to_date,
                self.voted_for,
                rv.last_log_term,
                rv.last_log_index,
                self_last_term,
                self_last_index,
            },
        );

        self.send(rv.candidate_id, Message{ .request_vote_reply = .{
            .term = self.current_term,
            .vote_granted = granted,
            .from = self.id,
        } });
    }

    pub fn handlePreVote(self: *Node, pv: PreVote) RaftError!void {
        if (self.state == .learner) {
            dbg(
                "prevote-reject tick={d} node={d} candidate={d} reason=learner",
                .{ self.tick_count, self.id, pv.candidate_id },
            );
            self.send(pv.candidate_id, Message{ .pre_vote_reply = .{
                .term = self.current_term,
                .vote_granted = false,
                .from = self.id,
            } });
            return;
        }
        if (pv.term < self.current_term) {
            dbg(
                "prevote-reject tick={d} node={d} candidate={d} reason=stale_term pv_term={d} cur_term={d}",
                .{ self.tick_count, self.id, pv.candidate_id, pv.term, self.current_term },
            );
            self.send(pv.candidate_id, Message{ .pre_vote_reply = .{
                .term = self.current_term,
                .vote_granted = false,
                .from = self.id,
            } });
            return;
        }

        const up_to_date = self.isLogUpToDate(pv.last_log_term, pv.last_log_index);
        dbg(
            "prevote tick={d} node={d} candidate={d} term={d} granted={} cand_last=({d},{d}) self_last=({d},{d})",
            .{
                self.tick_count,
                self.id,
                pv.candidate_id,
                pv.term,
                up_to_date,
                pv.last_log_term,
                pv.last_log_index,
                self.log.lastTerm(),
                self.log.lastIndex(),
            },
        );
        self.send(pv.candidate_id, Message{ .pre_vote_reply = .{
            .term = self.current_term,
            .vote_granted = up_to_date,
            .from = self.id,
        } });
    }

    pub fn handlePreVoteReply(self: *Node, pvr: PreVoteReply) RaftError!void {
        if (self.state == .learner) return;
        if (pvr.term < self.current_term) return;
        if (!pvr.vote_granted) return;

        _ = try self.prevotes_received.put(pvr.from, {});
        dbg(
            "prevote-reply tick={d} node={d} from={d} term={d} count={d} quorum={d}",
            .{
                self.tick_count,
                self.id,
                pvr.from,
                self.current_term,
                self.prevotes_received.count(),
                self.quorumSize(),
            },
        );
        if (self.prevotes_received.count() >= self.quorumSize()) {
            try self.startElection();
        }
    }

    pub fn handleRequestVoteReply(self: *Node, rvr: RequestVoteReply) RaftError!void {
        if (self.state != .candidate) return;
        if (rvr.term < self.current_term) return; // candidate should have +1 term.
        if (!rvr.vote_granted) return;

        _ = try self.votes_received.put(rvr.from, {});
        dbg(
            "vote-reply tick={d} node={d} from={d} term={d} count={d} quorum={d}",
            .{
                self.tick_count,
                self.id,
                rvr.from,
                self.current_term,
                self.votes_received.count(),
                self.quorumSize(),
            },
        );
        if (self.votes_received.count() >= self.quorumSize()) {
            try self.becomeLeader();
        }
    }

    pub fn handleAppendEntries(self: *Node, ae: AppendEntries) RaftError!void {
        if (ae.term < self.current_term) {
            self.send(ae.leader_id, Message{ .append_entries_reply = .{
                .term = self.current_term,
                .success = false,
                .from = self.id,
                .match_index = 0,
                .conflict_term = 0,
                .conflict_index = self.log.lastIndex() + 1,
            } });
            return;
        }

        if (self.state != .follower and self.state != .learner) {
            self.state = .follower;
        }
        self.leader_id = ae.leader_id;
        self.elapsed_ticks = 0;
        self.resetElectionTimeout(.append_entries);

        if (ae.prev_log_index > 0) {
            const prev = self.log.entry(ae.prev_log_index);
            if (prev == null or prev.?.term != ae.prev_log_term) {
                self.send(ae.leader_id, Message{ .append_entries_reply = .{
                    .term = self.current_term,
                    .success = false,
                    .from = self.id,
                    .match_index = 0,
                    .conflict_term = if (prev) |p| p.term else 0,
                    .conflict_index = if (prev) |p| self.log.firstIndexOfTerm(p.term) else self.log.lastIndex() + 1,
                } });
                return;
            }
        }

        if (ae.entries.len > 0) {
            var append_from: ?usize = null;
            for (ae.entries, 0..) |log_entry, i| {
                if (self.log.entry(log_entry.index)) |existing| {
                    if (existing.term != log_entry.term) {
                        self.log.truncateFrom(log_entry.index);
                        append_from = i;
                        break;
                    }
                } else {
                    append_from = i;
                    break;
                }
            }

            if (append_from) |start| {
                const slice = ae.entries[start..];
                try self.log.append(slice);
                try self.log.sync();
            }
        }

        if (ae.leader_commit > self.commit_index) {
            const last_index = self.log.lastIndex();
            self.commit_index = if (ae.leader_commit < last_index) ae.leader_commit else last_index;
        }
        self.applyCommitted();

        self.send(ae.leader_id, Message{ .append_entries_reply = .{
            .term = self.current_term,
            .success = true,
            .from = self.id,
            .match_index = self.log.lastIndex(),
            .conflict_term = 0,
            .conflict_index = 0,
        } });
    }

    pub fn handleAppendEntriesReply(self: *Node, aer: AppendEntriesReply) RaftError!void {
        if (self.state != .leader) return;
        if (aer.term < self.current_term) return;

        if (!aer.success) {
            if (self.peers.getPtr(aer.from)) |state| {
                if (aer.conflict_index > 0) {
                    state.next_index = aer.conflict_index;
                } else if (state.next_index > 1) {
                    state.next_index -= 1;
                }
            }
            return;
        }

        if (self.peers.getPtr(aer.from)) |state| {
            state.match_index = aer.match_index;
            state.next_index = aer.match_index + 1;
        }
        self.advanceCommitIndex();
        self.applyCommitted();
    }

    pub fn propose(self: *Node, data: []const u8) RaftError!u64 {
        if (self.state != .leader) return RaftError.NotLeader;

        const new_index = self.log.lastIndex() + 1;
        const entry = LogEntry{ .index = new_index, .term = self.current_term, .data = data };
        try self.log.append(&[_]LogEntry{entry});
        try self.log.sync();
        var it = self.peers.iterator();
        while (it.next()) |kv| {
            try self.sendAppendEntries(kv.key_ptr.*);
        }
        return new_index;
    }

    pub fn proposeConfChange(self: *Node, cc: ConfChange) RaftError!u64 {
        if (self.state != .leader) return RaftError.NotLeader;
        if (self.pending_conf_change) return RaftError.ConfChangeInProgress;

        self.pending_conf_change = true;
        const new_index = self.log.lastIndex() + 1;
        const entry = LogEntry{
            .index = new_index,
            .term = self.current_term,
            .ty = .conf_change,
            .conf_change = cc,
            .data = &[_]u8{},
        };
        try self.log.append(&[_]LogEntry{entry});
        try self.log.sync();
        var it = self.peers.iterator();
        while (it.next()) |kv| {
            try self.sendAppendEntries(kv.key_ptr.*);
        }
        return new_index;
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
        try self.log.setHardState(.{ .current_term = self.current_term, .voted_for = self.voted_for });
        try self.log.sync();
    }

    pub fn persistLog(self: *Node, entries: []const LogEntry) RaftError!void {
        try self.log.append(entries);
        try self.log.sync();
    }

    pub fn isLogUpToDate(self: *const Node, candidate_term: u64, candidate_index: u64) bool {
        const last_term = self.log.lastTerm();
        if (candidate_term > last_term) return true;
        if (candidate_term < last_term) return false;
        return candidate_index >= self.log.lastIndex();
    }

    pub fn send(self: *Node, to: NodeId, message: Message) void {
        _ = self.outbox.append(self.allocator, .{ .to = to, .msg = message }) catch {
            @panic("raft: outbox append failed");
        };
    }

    pub fn takeOutbox(self: *Node) []Outbound {
        return self.outbox.items;
    }

    pub fn clearOutbox(self: *Node) void {
        self.outbox.clearRetainingCapacity();
    }

    pub fn quorumSize(self: *const Node) usize {
        return self.quorum_size;
    }

    pub fn applyCommitted(self: *Node) void {
        while (self.last_applied < self.commit_index) {
            self.last_applied += 1;
            const entry = self.log.entry(self.last_applied) orelse continue;
            if (entry.ty == .conf_change and entry.conf_change != null) {
                self.applyConfChange(entry.conf_change.?);
                self.pending_conf_change = false;
            } else {
                // normal entry -> apply to state machine (not yet wired)
            }
        }
    }

    fn applyConfChange(self: *Node, cc: ConfChange) void {
        switch (cc.change_type) {
            .add_voter => self.addPeer(cc.node_id, .voter),
            .add_learner => self.addPeer(cc.node_id, .learner),
            .to_voter => self.setPeerRole(cc.node_id, .voter),
            .to_learner => self.setPeerRole(cc.node_id, .learner),
            .remove_node => self.removePeer(cc.node_id),
        }
    }

    fn hasPeer(self: *Node, id: NodeId) bool {
        return self.peers.contains(id);
    }

    fn addPeer(self: *Node, id: NodeId, role: PeerRole) void {
        if (self.hasPeer(id)) return;
        _ = self.peers.put(id, .{
            .id = id,
            .next_index = self.log.lastIndex() + 1,
            .match_index = 0,
            .role = role,
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
            self.addPeer(id, role);
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
        dbg(
            "timeout-reset tick={d} node={d} reason={s} old={d} new={d} draw={d} range=[{d},{d}] elapsed={d}",
            .{
                self.tick_count,
                self.id,
                @tagName(reason),
                old_timeout,
                self.election_timeout,
                draw,
                min,
                max,
                self.elapsed_ticks,
            },
        );
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
        dbg(
            "step-down tick={d} node={d} {s}/term={d} -> {s}/term={d}",
            .{ self.tick_count, self.id, @tagName(prev_state), prev_term, @tagName(self.state), self.current_term },
        );
        try self.persistHardState();
    }

    fn advanceCommitIndex(self: *Node) void {
        _ = self;
        // TODO: compute majority match index and advance commit_index safely.
    }
};
