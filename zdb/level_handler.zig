const std = @import("std");
const key = @import("key.zig");
const table = @import("table.zig");
const vfs = @import("vfs.zig");
const utils = @import("utils");

const Table = table.Table;
const TableIterator = table.TableIterator;
const Item = table.table.Item;

pub const Error = table.table.Error;

// ---------------------------------------------------------------------------
// KeyRange
// ---------------------------------------------------------------------------

pub const KeyRange = struct {
    left: []const u8 = "",
    right: []const u8 = "",
    inf: bool = false,

    pub fn isEmpty(self: KeyRange) bool {
        return self.left.len == 0 and self.right.len == 0 and !self.inf;
    }

    pub fn overlapsWith(self: KeyRange, other: KeyRange) bool {
        if (self.isEmpty()) return true;
        if (other.isEmpty()) return false;
        if (self.inf or other.inf) return true;
        // [other.left, other.right] ... [self.left, self.right]
        if (key.compareWithTs(other.right, self.left) == .lt) return false;
        // [self.left, self.right] ... [other.left, other.right]
        if (key.compareWithTs(self.right, other.left) == .lt) return false;
        return true;
    }

    pub fn extend(self: *KeyRange, other: KeyRange) void {
        if (other.isEmpty()) return;
        if (self.isEmpty()) {
            self.* = other;
            return;
        }
        if (self.left.len == 0 or key.compareWithTs(other.left, self.left) == .lt) {
            self.left = other.left;
        }
        if (self.right.len == 0 or key.compareWithTs(other.right, self.right) == .gt) {
            self.right = other.right;
        }
        if (other.inf) self.inf = true;
    }
};

// ---------------------------------------------------------------------------
// TableEntry
// ---------------------------------------------------------------------------

pub const TableEntry = struct {
    id: u64,
    table: *Table,
};

// ---------------------------------------------------------------------------
// LevelHandler
// ---------------------------------------------------------------------------

pub const LevelHandler = struct {
    mu: std.Thread.RwLock,
    entries: std.ArrayList(TableEntry),
    total_size: i64, // sum of each table's buf.len
    level: u8,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, level: u8) LevelHandler {
        return .{
            .mu = .{},
            .entries = .{},
            .total_size = 0,
            .level = level,
            .allocator = allocator,
        };
    }

    /// deinit releases each table's reference without deleting SST files.
    /// Used by LevelsController.close(); detaches the path so decrRef at
    /// zero only frees memory.
    pub fn deinit(self: *LevelHandler) void {
        for (self.entries.items) |e| {
            e.table.detach(); // clear path → decrRef won't delete the file
            e.table.decrRef();
        }
        self.entries.deinit(self.allocator);
    }

    /// initEntries bulk-sets entries, sorts them, and recomputes total_size.
    pub fn initEntries(self: *LevelHandler, new_entries: []const TableEntry) !void {
        self.mu.lock();
        defer self.mu.unlock();

        self.entries.clearRetainingCapacity();
        try self.entries.appendSlice(self.allocator, new_entries);
        self.sortEntries();

        self.total_size = 0;
        for (self.entries.items) |e| {
            self.total_size += @intCast(e.table.fileSize());
        }
    }

    /// sortEntries: L0 → by id ascending; L≥1 → by table.smallest key.
    /// L0 tables would overlap. Newer tables would have higher ids and would be at the end.
    /// L1 and onwards would have non-overlapping tables.
    fn sortEntries(self: *LevelHandler) void {
        if (self.level == 0) {
            std.mem.sort(TableEntry, self.entries.items, {}, struct {
                fn lt(_: void, a: TableEntry, b: TableEntry) bool {
                    return a.id < b.id;
                }
            }.lt);
        } else {
            std.mem.sort(TableEntry, self.entries.items, {}, struct {
                fn lt(_: void, a: TableEntry, b: TableEntry) bool {
                    return key.compareWithTs(a.table.smallest(), b.table.smallest()) == .lt;
                }
            }.lt);
        }
    }

    /// addEntry appends an entry, sorts, and increments the table's ref count.
    pub fn addEntry(self: *LevelHandler, id: u64, t: *Table) !void {
        self.mu.lock();
        defer self.mu.unlock();

        try self.entries.append(self.allocator, .{ .id = id, .table = t });
        t.incrRef();
        self.total_size += @intCast(t.fileSize());
        self.sortEntries();
    }

    /// deleteEntries removes entries by id and decrements each removed table's ref.
    pub fn deleteEntries(self: *LevelHandler, to_del: []const TableEntry) void {
        self.mu.lock();
        defer self.mu.unlock();

        return self.deleteEntriesLocked(to_del);
    }

    fn deleteEntriesLocked(self: *LevelHandler, to_del: []const TableEntry) void {
        // Iterate over the full list of tables we own, and see for each table whether
        // it's id is present in the ids list provided.
        var i: usize = self.entries.items.len;
        while (i > 0) {
            i -= 1;
            const e = self.entries.items[i];
            // I think looping here is fine, given we expect ids to be small.
            // We can consider storing ids in a map as a micro-optimization.
            const found = for (to_del) |entry| {
                if (e.id == entry.id) break true;
            } else false;

            if (found) {
                // TableEntry e is on the chopping board.
                self.total_size -= @intCast(e.table.fileSize());
                e.table.decrRef();
                _ = self.entries.orderedRemove(i);
            }
        }
    }

    /// replaceEntries is write-locked: deletes del_ids, appends to_add, then sorts.
    pub fn replaceEntries(self: *LevelHandler, to_del: []const TableEntry, to_add: []const TableEntry) !void {
        self.mu.lock();
        defer self.mu.unlock();

        self.deleteEntriesLocked(to_del);

        // Add new entries.
        for (to_add) |e| {
            try self.entries.append(self.allocator, e);
            e.table.incrRef();
            self.total_size += @intCast(e.table.fileSize());
        }

        self.sortEntries();
    }

    /// overlappingTables returns a heap-allocated slice of TableEntry whose key
    /// range overlaps kr. The caller must free the slice when done.
    /// L0: always all tables; L≥1: binary search on both ends.
    pub fn overlappingTables(self: *LevelHandler, allocator: std.mem.Allocator, kr: KeyRange) ![]TableEntry {
        self.mu.lockShared();
        defer self.mu.unlockShared();

        const items = self.entries.items;
        const slice: []TableEntry = if (self.level == 0)
            items
        else if (kr.isEmpty())
            items[0..0]
        else blk: {
            // First table where kr.left <= biggest (could overlap on the left).
            const left = utils.search(TableEntry, items, kr.left, struct {
                fn f(kr_left: []const u8, e: TableEntry) bool {
                    return key.compareWithTs(kr_left, e.table.biggest()) != .gt;
                }
            }.f);
            // First table where kr.right < smallest (no longer overlapping).
            const right = utils.search(TableEntry, items, kr.right, struct {
                fn f(kr_right: []const u8, e: TableEntry) bool {
                    return key.compareWithTs(kr_right, e.table.smallest()) == .lt;
                }
            }.f);
            break :blk items[left..right];
        };

        return allocator.dupe(TableEntry, slice);
    }

    pub fn getTotalSize(self: *LevelHandler) i64 {
        self.mu.lockShared();
        defer self.mu.unlockShared();
        return self.total_size;
    }

    pub fn numTables(self: *LevelHandler) usize {
        self.mu.lockShared();
        defer self.mu.unlockShared();
        return self.entries.items.len;
    }

    /// get seeks search_key within this level's tables.
    /// Returns a heap-allocated Item (key + value duped with self.allocator) on hit,
    /// or null on miss. Caller must free item.key and item.value with self.allocator.
    ///
    /// L0: back-to-front (highest id = newest); first match wins.
    /// L≥1: binary search for the one table that could contain the key.
    pub fn get(self: *LevelHandler, search_key: []const u8, priority: vfs.Priority) Error!?Item {
        const searchKeyNoTs = key.parseKey(search_key);
        if (self.level == 0) {
            self.mu.lockShared();
            defer self.mu.unlockShared();

            var ri: usize = self.entries.items.len;
            while (ri > 0) {
                ri -= 1;
                const e = self.entries.items[ri];
                if (!e.table.containsKey(searchKeyNoTs)) continue;
                var it = TableIterator.init(self.allocator, e.table, priority);
                defer it.deinit();

                try it.seek(search_key);
                const found = it.itemRef() orelse continue;
                if (!std.mem.eql(u8, key.parseKey(found.key), searchKeyNoTs)) continue;

                const k = try self.allocator.dupe(u8, found.key);
                errdefer self.allocator.free(k);
                const v = try self.allocator.dupe(u8, found.value);
                return .{ .key = k, .value = v };
            }
            return null;
        } else {
            self.mu.lockShared();
            defer self.mu.unlockShared();

            // First table whose biggest key >= search_key.
            const lo = utils.search(TableEntry, self.entries.items, search_key, struct {
                fn f(sk: []const u8, e: TableEntry) bool {
                    return key.compareWithTs(e.table.biggest(), sk) != .lt;
                }
            }.f);
            if (lo >= self.entries.items.len) return null;

            const e = self.entries.items[lo];
            // Verify the key can actually be in this table.
            // Compare by user key only: a table whose smallest user key is
            // greater than the search user key cannot contain the key at any
            // timestamp, so skip it.
            if (std.mem.order(u8, key.parseKey(e.table.smallest()), searchKeyNoTs) == .gt) return null;
            if (!e.table.containsKey(searchKeyNoTs)) return null;

            var it = TableIterator.init(self.allocator, e.table, priority);
            defer it.deinit();

            try it.seek(search_key);
            const found = it.itemRef() orelse return null;
            if (!std.mem.eql(u8, key.parseKey(found.key), searchKeyNoTs)) return null;

            const k = try self.allocator.dupe(u8, found.key);
            errdefer self.allocator.free(k);
            const v = try self.allocator.dupe(u8, found.value);
            return .{ .key = k, .value = v };
        }
    }
};
