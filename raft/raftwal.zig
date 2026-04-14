const std = @import("std");
const zen = @import("buffers_gen");

pub const NodeId = u64;
pub const ConfChangeType = enum(u8) {
    add_voter,
    add_learner,
    to_voter,
    to_learner,
    remove_node,
    update_addr,
};
pub const ConfChange = struct {
    change_type: ConfChangeType,
    node_id: NodeId,
    addr: []const u8 = "",
};
pub const LogEntryBuffer = zen.LogEntryBuffer;
pub const HardState = zen.HardStateStruct;
pub const Snapshot = zen.SnapshotStruct;
pub const ConfState = zen.ConfStateStruct;
pub const ConfStatePeer = zen.ConfStatePeerStruct;

pub const EntryType = enum(u16) {
    any = 0,
    data = 1,
    conf = 2,
};

pub const StoredEntry = struct {
    index: u64,
    term: u64,
    bytes: []const u8 = "",
};

pub const RaftLogError = error{
    Compacted,
    Unavailable,
    Canceled,
} || std.mem.Allocator.Error || zen.Error;

pub const EntriesRangeOpts = struct {
    lo: u64,
    hi: u64 = std.math.maxInt(u64),
    max_bytes: u64 = 64 * 1024 * 1024, // 64 MiB
    ty: EntryType = .any,
};

/// Result of an entries range query. All entry bytes live in the arena.
/// Caller must call deinit() when done.
pub const EntriesResult = struct {
    entries: []StoredEntry,
    arena: std.heap.ArenaAllocator,
    /// True if all matching entries in the range were returned (not truncated by max_bytes).
    eof: bool = true,

    pub fn deinit(self: *EntriesResult) void {
        self.arena.deinit();
    }
};

pub const RaftWAL = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        hardState: *const fn (*anyopaque) *const HardState,
        setHardState: *const fn (*anyopaque, HardState) RaftLogError!void,
        snapshot: *const fn (*anyopaque) *const Snapshot,
        setSnapshot: *const fn (*anyopaque, Snapshot) RaftLogError!void,
        firstIndex: *const fn (*anyopaque) u64,
        lastIndex: *const fn (*anyopaque) u64,
        lastTerm: *const fn (*anyopaque) u64,
        term: *const fn (*anyopaque, u64) RaftLogError!?u64,
        // Returns entries in an arena. Caller must call result.deinit() when done.
        entriesRange: *const fn (*anyopaque, EntriesRangeOpts) RaftLogError!EntriesResult,
        firstIndexOfTerm: *const fn (*anyopaque, u64) u64,
        append: *const fn (*anyopaque, []const LogEntryBuffer) RaftLogError!void,
        truncateFrom: *const fn (*anyopaque, u64) RaftLogError!void,
        compactUntil: *const fn (*anyopaque, u64) RaftLogError!void,
        sync: *const fn (*anyopaque) RaftLogError!void,
    };

    pub fn hardState(self: RaftWAL) *const HardState {
        return self.vtable.hardState(self.ctx);
    }

    pub fn setHardState(self: RaftWAL, hs: HardState) RaftLogError!void {
        return self.vtable.setHardState(self.ctx, hs);
    }

    pub fn snapshot(self: RaftWAL) *const Snapshot {
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

    // Returns entries in an arena. Caller must call result.deinit() when done.
    pub fn entriesRange(self: RaftWAL, opts: EntriesRangeOpts) RaftLogError!EntriesResult {
        return self.vtable.entriesRange(self.ctx, opts);
    }

    pub fn firstIndexOfTerm(self: RaftWAL, term_value: u64) u64 {
        return self.vtable.firstIndexOfTerm(self.ctx, term_value);
    }

    pub fn append(self: RaftWAL, entries: []const LogEntryBuffer) RaftLogError!void {
        return self.vtable.append(self.ctx, entries);
    }

    pub fn truncateFrom(self: RaftWAL, from_index: u64) RaftLogError!void {
        return self.vtable.truncateFrom(self.ctx, from_index);
    }

    pub fn compactUntil(self: RaftWAL, index: u64) RaftLogError!void {
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
    hard_state: *HardState,
    snapshot_info: *Snapshot,

    pub fn init(allocator: std.mem.Allocator) !MemWAL {
        const hs = try allocator.create(HardState);
        hs.* = .{};
        const snap = try allocator.create(Snapshot);
        snap.* = .{};
        return .{
            .allocator = allocator,
            .entries = std.ArrayList(StoredEntry).empty,
            .hard_state = hs,
            .snapshot_info = snap,
        };
    }

    pub fn deinit(self: *MemWAL) void {
        self.clearEntries();
        self.snapshot_info.deinit(self.allocator);
        self.allocator.destroy(self.snapshot_info);
        self.allocator.destroy(self.hard_state);
        self.entries.deinit(self.allocator);
    }

    pub fn asRaftWAL(self: *MemWAL) RaftWAL {
        return .{ .ctx = self, .vtable = &vtable };
    }

    fn fromCtx(ctx: *anyopaque) *MemWAL {
        return @ptrCast(@alignCast(ctx));
    }

    fn hardState(ctx: *anyopaque) *const HardState {
        const self = fromCtx(ctx);
        return self.hard_state;
    }

    fn setHardState(ctx: *anyopaque, hs: HardState) RaftLogError!void {
        const self = fromCtx(ctx);
        const copy = hs.clone(self.allocator) catch return error.OutOfMemory;
        self.hard_state.destroy(self.allocator);
        self.hard_state = copy;
    }

    fn snapshot(ctx: *anyopaque) *const Snapshot {
        const self = fromCtx(ctx);
        return self.snapshot_info;
    }

    fn setSnapshot(ctx: *anyopaque, snap: Snapshot) RaftLogError!void {
        const self = fromCtx(ctx);
        const copy = snap.clone(self.allocator) catch return error.OutOfMemory;
        self.snapshot_info.destroy(self.allocator);
        self.snapshot_info = copy;
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

    fn entriesRange(ctx: *anyopaque, opts: EntriesRangeOpts) RaftLogError!EntriesResult {
        const self = fromCtx(ctx);
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        errdefer arena.deinit();
        const alloc = arena.allocator();

        if (opts.lo >= opts.hi or self.entries.items.len == 0) {
            return .{ .entries = try alloc.alloc(StoredEntry, 0), .arena = arena };
        }
        const base = self.entries.items[0].index;
        if (opts.hi <= base) {
            return .{ .entries = try alloc.alloc(StoredEntry, 0), .arena = arena };
        }
        const start_index = if (opts.lo > base) opts.lo else base;
        const start_offset: usize = @intCast(start_index - base);
        if (start_offset >= self.entries.items.len) {
            return .{ .entries = try alloc.alloc(StoredEntry, 0), .arena = arena };
        }

        // MemWAL doesn't track entry types, so ty filter is ignored here.
        var end_offset: usize = start_offset;
        var size: u64 = 0;
        while (end_offset < self.entries.items.len) : (end_offset += 1) {
            const stored = self.entries.items[end_offset];
            if (stored.index >= opts.hi) break;
            size += stored.bytes.len;
            if (opts.max_bytes > 0 and end_offset > start_offset and size > opts.max_bytes) break;
        }

        if (end_offset <= start_offset) {
            return .{ .entries = try alloc.alloc(StoredEntry, 0), .arena = arena };
        }

        // Copy entries into arena so they have uniform lifetime.
        const src = self.entries.items[start_offset..end_offset];
        const entries = try alloc.alloc(StoredEntry, src.len);
        for (src, 0..) |se, i| {
            entries[i] = .{
                .index = se.index,
                .term = se.term,
                .bytes = try alloc.dupe(u8, se.bytes),
            };
        }
        return .{ .entries = entries, .arena = arena };
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
        .entriesRange = entriesRange,
        .firstIndexOfTerm = firstIndexOfTerm,
        .append = append,
        .truncateFrom = truncateFrom,
        .compactUntil = compactUntil,
        .sync = sync,
    };
};
