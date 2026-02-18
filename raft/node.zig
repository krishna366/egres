const std = @import("std");
const raftwal_mod = @import("raftwal.zig");
const zen = @import("buffers_gen");
const utils = @import("utils");
const linux = std.os.linux;
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
    InvalidBytes,
    QueueClosed,
} || RaftLogError || zen.Error;

pub const Peer = struct {
    id: NodeId,
    next_index: u64,
    // Highest log index known to be replicated on this peer (leader view).
    match_index: u64,
    role: PeerRole,
};

pub const Outbound = struct {
    to: NodeId,
    payload: []u8,
};

pub const Inbound = []u8;
pub const ProposalEntry = struct {
    term: u64,
    index: u64,
};

pub const CommitStatus = enum {
    committed,
    not_leader,
    step_down,
    shutdown,
};

pub const CommitCallback = *const fn (
    ctx: *anyopaque,
    entry: ProposalEntry,
    status: CommitStatus,
) void;

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
    log: RaftWAL,

    state: State,
    commit_index: u64,
    last_applied: std.atomic.Value(u64),
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
    proposal_wake_fd: std.posix.fd_t,
    proposal_requests: ProposalQueue,
    pending_commit_callbacks: PendingCommitQueue,
    shutting_down: std.atomic.Value(bool),
    commit_wait_mutex: std.Thread.Mutex,
    commit_wait_cond: std.Thread.Condition,

    const ProposalQueue = utils.Queue(ProposalRequest, 4096);
    const PendingCommitQueue = utils.Queue(PendingCommit, 4096);
    const ProposalRequest = struct {
        data: []u8,
        cb: CommitCallback,
        ctx: *anyopaque,
    };
    const PendingCommit = struct {
        entry: ProposalEntry,
        cb: CommitCallback,
        ctx: *anyopaque,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        id: NodeId,
        peers: []const Peer,
        log: RaftWAL,
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
            .last_applied = std.atomic.Value(u64).init(0),
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
            .proposal_wake_fd = try std.posix.eventfd(0, linux.EFD.CLOEXEC | linux.EFD.NONBLOCK),
            .proposal_requests = .{},
            .pending_commit_callbacks = .{},
            .shutting_down = std.atomic.Value(bool).init(false),
            .commit_wait_mutex = .{},
            .commit_wait_cond = .{},
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
        self.proposal_requests.close();
        while (self.proposal_requests.tryPop()) |req| {
            self.allocator.free(req.data);
            req.cb(req.ctx, .{ .term = self.current_term, .index = 0 }, .shutdown);
        }
        self.failAllPendingCallbacks(.shutdown);
        self.pending_commit_callbacks.close();
        std.posix.close(self.proposal_wake_fd);
        self.clearOutbox();
        self.votes_received.deinit();
        self.prevotes_received.deinit();
        self.peers.deinit();
        self.outbox.deinit(self.allocator);
    }

    pub fn proposalWakeFd(self: *const Node) std.posix.fd_t {
        return self.proposal_wake_fd;
    }

    pub fn startShutdown(self: *Node) void {
        self.shutting_down.store(true, .seq_cst);
        self.proposal_requests.close();
        self.notifyCommitCallbacks();
        var one: u64 = 1;
        _ = std.posix.write(self.proposal_wake_fd, std.mem.asBytes(&one)) catch {};
    }

    pub fn isShuttingDown(self: *const Node) bool {
        return self.shutting_down.load(.seq_cst);
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
            .leader_commit = self.commit_index,
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
                try self.log.append(entries_to_append.items);
                try self.log.sync();
            }
        }

        if (ae.leaderCommit() > self.commit_index) {
            const last_index = self.log.lastIndex();
            self.commit_index = if (ae.leaderCommit() < last_index) ae.leaderCommit() else last_index;
        }
        self.applyCommitted();

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
        self.advance();
    }

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
        var it = self.peers.iterator();
        while (it.next()) |kv| {
            try self.sendAppendEntries(kv.key_ptr.*);
        }
        return new_index;
    }

    /// Preferred external proposal API.
    /// Callers should enqueue through this method; direct `propose` calls are
    /// intended for the single-threaded raft runtime loop.
    pub fn proposeWithCallback(
        self: *Node,
        data: []const u8,
        cb: CommitCallback,
        ctx: *anyopaque,
    ) RaftError!void {
        if (self.isShuttingDown()) return error.QueueClosed;
        const owned = try self.allocator.dupe(u8, data);
        errdefer self.allocator.free(owned);
        try self.proposal_requests.pushOrClosed(.{
            .data = owned,
            .cb = cb,
            .ctx = ctx,
        });
        var one: u64 = 1;
        _ = std.posix.write(self.proposal_wake_fd, std.mem.asBytes(&one)) catch {};
    }

    pub fn processProposalRequests(self: *Node, max_batch: usize) RaftError!usize {
        if (self.isShuttingDown()) {
            while (self.proposal_requests.tryPop()) |req| {
                self.allocator.free(req.data);
                req.cb(req.ctx, .{ .term = self.current_term, .index = 0 }, .shutdown);
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
        const first_index = self.appendNormalEntriesBatch(batch.items) catch |err| switch (err) {
            error.NotLeader => {
                for (batch.items) |req| {
                    req.cb(req.ctx, .{ .term = term, .index = 0 }, .not_leader);
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
            self.pending_commit_callbacks.pushOrClosed(.{
                .entry = entry,
                .cb = req.cb,
                .ctx = req.ctx,
            }) catch |err| switch (err) {
                error.QueueClosed => req.cb(req.ctx, entry, .shutdown),
            };
        }
        self.notifyCommitCallbacks();

        return batch.items.len;
    }

    pub fn processCommitCallbacks(self: *Node, max_callbacks: usize) usize {
        var count: usize = 0;
        while (count < max_callbacks) : (count += 1) {
            const head = self.pending_commit_callbacks.peek() orelse break;
            if (head.entry.index > self.last_applied.load(.seq_cst)) break;

            const ready = self.pending_commit_callbacks.tryPop() orelse break;
            ready.cb(ready.ctx, ready.entry, .committed);
        }
        return count;
    }

    fn hasReadyCommitCallback(self: *Node) bool {
        const head = self.pending_commit_callbacks.peek() orelse return false;
        return head.entry.index <= self.last_applied.load(.seq_cst);
    }

    fn notifyCommitCallbacks(self: *Node) void {
        self.commit_wait_mutex.lock();
        defer self.commit_wait_mutex.unlock();
        self.commit_wait_cond.broadcast();
    }

    pub fn waitForCommitCallbacks(self: *Node) void {
        self.commit_wait_mutex.lock();
        defer self.commit_wait_mutex.unlock();
        while (!self.isShuttingDown() and !self.hasReadyCommitCallback()) {
            self.commit_wait_cond.wait(&self.commit_wait_mutex);
        }
    }

    pub fn proposeConfChange(self: *Node, cc: ConfChange) RaftError!u64 {
        if (self.state != .leader) return RaftError.NotLeader;
        if (self.pending_conf_change) return RaftError.ConfChangeInProgress;

        self.pending_conf_change = true;
        const new_index = self.log.lastIndex() + 1;
        var buf = std.ArrayList(u8).empty;
        errdefer buf.deinit(self.allocator);

        var cc_writer = try zen.ConfChangeWriter.initWithBuf(self.allocator, &buf);
        try cc_writer.set(.{
            .add_voter = cc.change_type == .add_voter,
            .add_learner = cc.change_type == .add_learner,
            .to_voter = cc.change_type == .to_voter,
            .to_learner = cc.change_type == .to_learner,
            .remove_node = cc.change_type == .remove_node,
            .node_id = cc.node_id,
        });
        const cc_res = try cc_writer.finish();

        var le_writer = try zen.LogEntryWriter.initWithBuf(self.allocator, &buf);
        try le_writer.set(.{
            .index = new_index,
            .term = self.current_term,
            .data = &[_]u8{},
            .is_normal = false,
            .is_conf_change = true,
            .conf_change = cc_res.offset,
        });
        const le_res = try le_writer.finish();
        try zen.addTail(self.allocator, &buf, zen.LogEntryReader, le_res.offset);

        const bytes = try buf.toOwnedSlice(self.allocator);
        var entry = try zen.LogEntryBuffer.initWithTail(self.allocator, bytes, .owned);
        defer entry.deinit();

        try self.log.append(&[_]zen.LogEntryBuffer{entry});
        try self.log.sync();
        self.advance();

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
        try self.log.setHardState(.{
            .current_term = self.current_term,
            .voted_for = self.voted_for orelse 0,
            .commit_index = self.commit_index,
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
        _ = self.outbox.append(self.allocator, .{
            .to = to,
            .payload = payload,
        }) catch {
            self.allocator.free(payload);
            @panic("raft: outbox append failed");
        };
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

    pub fn takeOutbox(self: *Node) []Outbound {
        return self.outbox.items;
    }

    pub fn clearOutbox(self: *Node) void {
        for (self.outbox.items) |outbound| {
            self.allocator.free(outbound.payload);
        }
        self.outbox.clearRetainingCapacity();
    }

    pub fn quorumSize(self: *const Node) usize {
        return self.quorum_size;
    }

    pub fn advance(self: *Node) void {
        self.advanceCommitIndex();
        self.applyCommitted();
    }

    pub fn applyCommitted(self: *Node) void {
        var applied = self.last_applied.load(.seq_cst);
        var advanced = false;
        while (applied < self.commit_index) {
            applied += 1;
            self.last_applied.store(applied, .seq_cst);
            advanced = true;
            const entry = (self.log.entryRef(applied) catch continue) orelse continue;
            const reader = zen.LogEntryReader.parseRoot(entry.bytes) catch continue;
            if (reader.confChange()) |cc_reader| {
                const cc = self.decodeConfChange(cc_reader) catch continue;
                self.applyConfChange(cc);
                self.pending_conf_change = false;
            }
        }
        if (advanced) self.notifyCommitCallbacks();
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
        self.failAllPendingCallbacks(.step_down);
        dbg(
            "step-down tick={d} node={d} {s}/term={d} -> {s}/term={d}",
            .{ self.tick_count, self.id, @tagName(prev_state), prev_term, @tagName(self.state), self.current_term },
        );
        try self.persistHardState();
    }

    fn failAllPendingCallbacks(self: *Node, status: CommitStatus) void {
        while (self.pending_commit_callbacks.tryPop()) |head| {
            head.cb(head.ctx, head.entry, status);
        }
    }

    fn appendNormalEntriesBatch(self: *Node, requests: []const ProposalRequest) RaftError!u64 {
        if (self.state != .leader) return RaftError.NotLeader;
        if (requests.len == 0) return self.log.lastIndex() + 1;

        var entries = std.ArrayList(zen.LogEntryBuffer).empty;
        defer {
            for (entries.items) |*entry| entry.deinit();
            entries.deinit(self.allocator);
        }
        try entries.ensureTotalCapacity(self.allocator, requests.len);

        // TODO: Need to figure how to deal with conf changes.
        const first_index = self.log.lastIndex() + 1;
        var index = first_index;
        for (requests) |req| {
            var writer = try zen.LogEntryWriter.init(self.allocator);
            defer writer.deinit();
            try writer.set(.{
                .index = index,
                .term = self.current_term,
                .data = req.data,
                .is_normal = true,
                .is_conf_change = false,
                .conf_change = zen.NullOffset,
            });
            const entry = try writer.finishWithTail();
            try entries.append(self.allocator, entry);
            index += 1;
        }

        try self.log.append(entries.items);
        // TODO: If leader, this sync can be run concurrently with sendAppendEntries.
        try self.log.sync();
        self.advance();

        var it = self.peers.iterator();
        while (it.next()) |kv| {
            try self.sendAppendEntries(kv.key_ptr.*);
        }
        return first_index;
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
        if (candidate <= self.commit_index) return;

        const term = self.log.term(candidate) catch return orelse return;
        if (term != self.current_term) return;
        self.commit_index = candidate;
    }
};
