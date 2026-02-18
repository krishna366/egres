const std = @import("std");
const zen = @import("buffers_gen");

pub const NodeId = u64;
pub const ConfChangeType = enum(u8) {
    add_voter,
    add_learner,
    to_voter,
    to_learner,
    remove_node,
};
pub const ConfChange = struct {
    change_type: ConfChangeType,
    node_id: NodeId,
};
pub const LogEntryBuffer = zen.LogEntryBuffer;
pub const HardState = struct {
    current_term: u64,
    voted_for: u64,
    commit_index: u64,
};
pub const StoredEntry = struct {
    index: u64,
    term: u64,
    data_len: u32,
    // Borrowed/owned payload bytes. Callers must treat this as read-only.
    bytes: []const u8,
};

pub const Snapshot = struct {
    index: u64,
    term: u64,
    data: []const u8,
};

pub const RaftLogError = error{
    Compacted,
    Unavailable,
} || std.mem.Allocator.Error || zen.Error;

pub const RaftWAL = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        hardState: *const fn (*anyopaque) HardState,
        setHardState: *const fn (*anyopaque, HardState) RaftLogError!void,
        snapshot: *const fn (*anyopaque) Snapshot,
        setSnapshot: *const fn (*anyopaque, Snapshot) RaftLogError!void,
        firstIndex: *const fn (*anyopaque) u64,
        lastIndex: *const fn (*anyopaque) u64,
        lastTerm: *const fn (*anyopaque) u64,
        term: *const fn (*anyopaque, u64) RaftLogError!?u64,
        // `Ref` APIs return borrowed views. Implementations may invalidate these
        // views after the next ref call or after any mutating log operation.
        entryRef: *const fn (*anyopaque, u64) RaftLogError!?StoredEntry,
        entriesFromRef: *const fn (*anyopaque, u64, u32) RaftLogError![]const StoredEntry,
        entriesRangeRef: *const fn (*anyopaque, u64, u64, u64) RaftLogError![]const StoredEntry,
        firstIndexOfTerm: *const fn (*anyopaque, u64) u64,
        lastIndexOfTerm: *const fn (*anyopaque, u64) u64,
        append: *const fn (*anyopaque, []const LogEntryBuffer) RaftLogError!void,
        truncateFrom: *const fn (*anyopaque, u64) void,
        compactUntil: *const fn (*anyopaque, u64) void,
        sync: *const fn (*anyopaque) RaftLogError!void,
    };

    pub fn hardState(self: RaftWAL) HardState {
        return self.vtable.hardState(self.ctx);
    }

    pub fn setHardState(self: RaftWAL, hs: HardState) RaftLogError!void {
        return self.vtable.setHardState(self.ctx, hs);
    }

    pub fn snapshot(self: RaftWAL) Snapshot {
        return self.vtable.snapshot(self.ctx);
    }

    pub fn setSnapshot(self: RaftWAL, snap: Snapshot) RaftLogError!void {
        return self.vtable.setSnapshot(self.ctx, snap);
    }

    pub fn firstIndex(self: RaftWAL) u64 {
        return self.vtable.firstIndex(self.ctx);
    }

    pub fn lastIndex(self: RaftWAL) u64 {
        return self.vtable.lastIndex(self.ctx);
    }

    pub fn lastTerm(self: RaftWAL) u64 {
        return self.vtable.lastTerm(self.ctx);
    }

    pub fn term(self: RaftWAL, index: u64) RaftLogError!?u64 {
        return self.vtable.term(self.ctx, index);
    }

    pub fn entryRef(self: RaftWAL, index: u64) RaftLogError!?StoredEntry {
        return self.vtable.entryRef(self.ctx, index);
    }

    // Borrowed refs, valid only per implementation contract (typically until
    // the next ref call or mutating operation on the same log instance).
    pub fn entriesFromRef(self: RaftWAL, start_index: u64, max_count: u32) RaftLogError![]const StoredEntry {
        return self.vtable.entriesFromRef(self.ctx, start_index, max_count);
    }

    pub fn entriesRangeRef(self: RaftWAL, lo: u64, hi: u64, max_bytes: u64) RaftLogError![]const StoredEntry {
        return self.vtable.entriesRangeRef(self.ctx, lo, hi, max_bytes);
    }

    pub fn firstIndexOfTerm(self: RaftWAL, term_value: u64) u64 {
        return self.vtable.firstIndexOfTerm(self.ctx, term_value);
    }

    pub fn lastIndexOfTerm(self: RaftWAL, term_value: u64) u64 {
        return self.vtable.lastIndexOfTerm(self.ctx, term_value);
    }

    pub fn append(self: RaftWAL, entries: []const LogEntryBuffer) RaftLogError!void {
        return self.vtable.append(self.ctx, entries);
    }

    pub fn truncateFrom(self: RaftWAL, from_index: u64) void {
        return self.vtable.truncateFrom(self.ctx, from_index);
    }

    pub fn compactUntil(self: RaftWAL, index: u64) void {
        return self.vtable.compactUntil(self.ctx, index);
    }

    pub fn sync(self: RaftWAL) RaftLogError!void {
        return self.vtable.sync(self.ctx);
    }
};

pub fn deinitOwnedEntries(alloc: std.mem.Allocator, entries: []LogEntryBuffer) void {
    for (entries) |*entry_buf| {
        entry_buf.deinit();
    }
    alloc.free(entries);
}

pub const MemWAL = struct {
    allocator: std.mem.Allocator,
    entries: std.ArrayList(StoredEntry),
    hard_state: HardState,
    snapshot_info: Snapshot,

    pub fn init(allocator: std.mem.Allocator) MemWAL {
        return .{
            .allocator = allocator,
            .entries = std.ArrayList(StoredEntry).empty,
            .hard_state = .{ .current_term = 0, .voted_for = 0, .commit_index = 0 },
            .snapshot_info = .{ .index = 0, .term = 0, .data = &[_]u8{} },
        };
    }

    pub fn deinit(self: *MemWAL) void {
        self.clearEntries();
        if (self.snapshot_info.data.len != 0) {
            self.allocator.free(self.snapshot_info.data);
        }
        self.entries.deinit(self.allocator);
    }

    pub fn asRaftWAL(self: *MemWAL) RaftWAL {
        return .{ .ctx = self, .vtable = &vtable };
    }

    fn fromCtx(ctx: *anyopaque) *MemWAL {
        return @ptrCast(@alignCast(ctx));
    }

    fn hardState(ctx: *anyopaque) HardState {
        const self = fromCtx(ctx);
        return self.hard_state;
    }

    fn setHardState(ctx: *anyopaque, hs: HardState) RaftLogError!void {
        const self = fromCtx(ctx);
        self.hard_state = hs;
    }

    fn snapshot(ctx: *anyopaque) Snapshot {
        const self = fromCtx(ctx);
        return self.snapshot_info;
    }

    fn setSnapshot(ctx: *anyopaque, snap: Snapshot) RaftLogError!void {
        const self = fromCtx(ctx);
        if (self.snapshot_info.data.len != 0) {
            self.allocator.free(self.snapshot_info.data);
        }
        const data_copy = if (snap.data.len == 0) &[_]u8{} else try self.allocator.dupe(u8, snap.data);
        self.snapshot_info = .{ .index = snap.index, .term = snap.term, .data = data_copy };
    }

    fn firstIndex(ctx: *anyopaque) u64 {
        const self = fromCtx(ctx);
        if (self.snapshot_info.index != 0) return self.snapshot_info.index + 1;
        if (self.entries.items.len == 0) return 1;
        return self.entries.items[0].index;
    }

    fn lastIndex(ctx: *anyopaque) u64 {
        const self = fromCtx(ctx);
        if (self.entries.items.len == 0) return self.snapshot_info.index;
        return self.entries.items[self.entries.items.len - 1].index;
    }

    fn lastTerm(ctx: *anyopaque) u64 {
        const self = fromCtx(ctx);
        if (self.entries.items.len == 0) return self.snapshot_info.term;
        return self.entries.items[self.entries.items.len - 1].term;
    }

    fn term(ctx: *anyopaque, index: u64) RaftLogError!?u64 {
        if (index == 0) return 0;
        const self = fromCtx(ctx);
        if (self.snapshot_info.index != 0 and index == self.snapshot_info.index) return self.snapshot_info.term;
        const found = self.findStored(index) orelse return null;
        return found.term;
    }

    fn entryRef(ctx: *anyopaque, index: u64) RaftLogError!?StoredEntry {
        const self = fromCtx(ctx);
        const stored = self.findStored(index) orelse return null;
        return stored;
    }

    fn entriesFromRef(ctx: *anyopaque, start_index: u64, max_count: u32) RaftLogError![]const StoredEntry {
        const self = fromCtx(ctx);
        if (self.entries.items.len == 0) return &[_]StoredEntry{};

        const base = self.entries.items[0].index;
        if (start_index < base) return &[_]StoredEntry{};

        const offset: usize = @intCast(start_index - base);
        if (offset >= self.entries.items.len) return &[_]StoredEntry{};

        const start = offset;
        const max: usize = @intCast(max_count);
        const end = if (start + max <= self.entries.items.len) start + max else self.entries.items.len;
        return self.entries.items[start..end];
    }

    fn entriesRangeRef(ctx: *anyopaque, lo: u64, hi: u64, max_bytes: u64) RaftLogError![]const StoredEntry {
        const self = fromCtx(ctx);
        if (lo >= hi) return &[_]StoredEntry{};
        if (self.entries.items.len == 0) return &[_]StoredEntry{};
        const base = self.entries.items[0].index;
        if (hi <= base) return &[_]StoredEntry{};
        const start_index = if (lo > base) lo else base;
        const start_offset: usize = @intCast(start_index - base);
        if (start_offset >= self.entries.items.len) return &[_]StoredEntry{};

        var end_offset: usize = start_offset;
        var size: u64 = 0;
        while (end_offset < self.entries.items.len) : (end_offset += 1) {
            const stored = self.entries.items[end_offset];
            if (stored.index >= hi) break;
            size += stored.data_len;
            if (max_bytes > 0 and end_offset > start_offset and size > max_bytes) break;
        }

        if (end_offset <= start_offset) return &[_]StoredEntry{};
        return self.entries.items[start_offset..end_offset];
    }

    fn firstIndexOfTerm(ctx: *anyopaque, term_value: u64) u64 {
        const self = fromCtx(ctx);
        for (self.entries.items) |log_entry| {
            if (log_entry.term == term_value) return log_entry.index;
        }
        return 0;
    }

    fn lastIndexOfTerm(ctx: *anyopaque, term_value: u64) u64 {
        const self = fromCtx(ctx);
        var i: usize = self.entries.items.len;
        while (i > 0) : (i -= 1) {
            const log_entry = self.entries.items[i - 1];
            if (log_entry.term == term_value) return log_entry.index;
        }
        return 0;
    }

    fn append(ctx: *anyopaque, new_entries: []const LogEntryBuffer) RaftLogError!void {
        const self = fromCtx(ctx);
        if (new_entries.len == 0) return;

        const first_reader = try new_entries[0].initReader();
        const first = first_reader.index();
        if (self.entries.items.len > 0) {
            const base = self.entries.items[0].index;
            const last = self.entries.items[self.entries.items.len - 1].index;
            if (first <= last and first >= base) {
                truncateFrom(ctx, first);
            } else if (first < base) {
                self.clearEntries();
            }
        }

        for (new_entries) |entry_buf| {
            const bytes = entry_buf.bytes orelse return RaftLogError.Invalid;
            const reader = try entry_buf.initReader();
            const encoded = try self.allocator.dupe(u8, bytes);
            try self.entries.append(self.allocator, .{
                .index = reader.index(),
                .term = reader.term(),
                .data_len = @as(u32, @intCast(reader.data().len)),
                .bytes = encoded,
            });
        }
    }

    fn truncateFrom(ctx: *anyopaque, from_index: u64) void {
        const self = fromCtx(ctx);
        if (self.entries.items.len == 0) return;
        const base = self.entries.items[0].index;
        if (from_index <= base) {
            self.clearEntries();
            return;
        }
        const offset: usize = @intCast(from_index - base);
        if (offset >= self.entries.items.len) return;
        for (self.entries.items[offset..]) |stored| {
            self.allocator.free(@constCast(stored.bytes));
        }
        self.entries.shrinkRetainingCapacity(offset);
    }

    fn compactUntil(ctx: *anyopaque, index: u64) void {
        const self = fromCtx(ctx);
        if (self.entries.items.len == 0) return;
        const base = self.entries.items[0].index;
        if (index <= base) return;
        const offset: usize = @intCast(index - base);
        if (offset >= self.entries.items.len) {
            self.clearEntries();
            return;
        }
        for (self.entries.items[0..offset]) |stored| {
            self.allocator.free(@constCast(stored.bytes));
        }
        self.entries.items = self.entries.items[offset..];
    }

    fn sync(ctx: *anyopaque) RaftLogError!void {
        _ = ctx;
    }

    fn clearEntries(self: *MemWAL) void {
        for (self.entries.items) |stored| {
            self.allocator.free(@constCast(stored.bytes));
        }
        self.entries.shrinkRetainingCapacity(0);
    }

    fn findStored(self: *MemWAL, index: u64) ?StoredEntry {
        if (index == 0) return null;
        if (self.entries.items.len == 0) return null;
        const base = self.entries.items[0].index;
        if (index < base) return null;
        const offset: usize = @intCast(index - base);
        if (offset >= self.entries.items.len) return null;
        return self.entries.items[offset];
    }

    const vtable = RaftWAL.VTable{
        .hardState = hardState,
        .setHardState = setHardState,
        .snapshot = snapshot,
        .setSnapshot = setSnapshot,
        .firstIndex = firstIndex,
        .lastIndex = lastIndex,
        .lastTerm = lastTerm,
        .term = term,
        .entryRef = entryRef,
        .entriesFromRef = entriesFromRef,
        .entriesRangeRef = entriesRangeRef,
        .firstIndexOfTerm = firstIndexOfTerm,
        .lastIndexOfTerm = lastIndexOfTerm,
        .append = append,
        .truncateFrom = truncateFrom,
        .compactUntil = compactUntil,
        .sync = sync,
    };
};
