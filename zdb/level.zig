const std = @import("std");
const Key = @import("kv.zig").Key;
const _table = @import("table.zig");
const vfs = @import("vfs");
const utils = @import("utils");
const fiber = @import("zfiber");

const Table = _table.Table;
const TableIterator = _table.TableIterator;
const Item = _table.table.Item;

pub const Error = _table.table.Error;

// ---------------------------------------------------------------------------
// KeyRange
// ---------------------------------------------------------------------------

// Low sentinel for a table's start: an EMPTY primary reads as fully-open, so
// table-scoped bounds use this non-empty minimum instead. Static so
// `limitToTable` can hand out slices into it.
const table_lo = [_]u8{0};

pub const KeyRange = struct {
    left: Key = .{},
    right: Key = .{},
    inf: bool = false,

    /// This range CLAMPED to one table's key window `[table_start, table_end)`
    /// (table_end = the next table_id's start, exclusive) — i.e. intersect with
    /// the table range. A left below the table (a lower namespace/table, or
    /// empty) is raised to table_start; a right at/above the end (or empty =
    /// open) is lowered to table_end; a left/right genuinely inside the table is
    /// kept. So a scan over the namespace-wide memtable stays within the table
    /// instead of grafting one key's primary onto another.
    pub fn limitToTable(self: KeyRange, ns: u64, table_id: u32) KeyRange {
        const mt = std.math.maxInt(u64);
        return self.intersect(.{
            .left = .{ .namespace = ns, .table_id = table_id, .primary = &table_lo, .timestamp = mt },
            .right = .{ .namespace = ns, .table_id = table_id +| 1, .primary = &table_lo, .timestamp = mt },
        });
    }

    pub fn isEmpty(self: KeyRange) bool {
        return self.left.primary.len == 0 and self.right.primary.len == 0 and !self.inf;
    }

    /// The range spanning one key's versions at or below its timestamp.
    /// Left keeps `key.timestamp` — ts sorts descending, so a seek
    /// lands directly on the newest version visible at that ts, never
    /// touching newer ones (pass maxInt for every version). Right is
    /// `Key.end`: exclusive, at ts 0, which closes the range without
    /// allocating a successor primary. Borrows `key.primary`.
    pub fn versionsAt(key: Key) KeyRange {
        return .{ .left = key, .right = key.end() };
    }

    /// Deep copy: dupes the underlying primary bytes for both bounds
    /// using `allocator`. The original Keys may borrow from
    /// short-lived storage (e.g. manifest entries protected by a
    /// lock); duping into an allocator the caller controls keeps the
    /// range usable after that storage is released.
    pub fn dupe(self: KeyRange, allocator: std.mem.Allocator) !KeyRange {
        return .{
            .left = try self.left.dupe(allocator),
            .right = try self.right.dupe(allocator),
            .inf = self.inf,
        };
    }

    pub fn overlapsWith(self: KeyRange, other: KeyRange) bool {
        if (self.isEmpty()) return true;
        if (other.isEmpty()) return false;
        if (self.inf or other.inf) return true;
        // [other.left, other.right] ... [self.left, self.right]
        if (other.right.order(self.left) == .lt) return false;
        // [self.left, self.right] ... [other.left, other.right]
        if (self.right.order(other.left) == .lt) return false;
        return true;
    }

    pub fn extend(self: *KeyRange, other: KeyRange) void {
        if (other.isEmpty()) return;
        if (self.isEmpty()) {
            self.* = other;
            return;
        }
        if (self.left.primary.len == 0 or other.left.order(self.left) == .lt) {
            self.left = other.left;
        }
        if (self.right.primary.len == 0 or other.right.order(self.right) == .gt) {
            self.right = other.right;
        }
        if (other.inf) self.inf = true;
    }

    /// Tighten `self` against `other` and return the overlap. Empty
    /// primary on either bound is treated as "open" (no constraint
    /// from that side), so this composes naturally with caller-supplied
    /// open scan bounds and tree-stored ranges. `inf` survives only
    /// when BOTH sides are unbounded after tightening.
    pub fn intersect(self: KeyRange, other: KeyRange) KeyRange {
        const lhs = tighter_left(self.left, other.left);
        const rhs = tighter_right(self.right, other.right);
        return .{
            .left = lhs,
            .right = rhs,
            .inf = lhs.primary.len == 0 and rhs.primary.len == 0,
        };
    }

    /// Pick the "rightmost" lower bound (= max). An empty primary
    /// means "unbounded on the left" — the OTHER bound wins.
    fn tighter_left(a: Key, b: Key) Key {
        if (a.primary.len == 0) return b;
        if (b.primary.len == 0) return a;
        return if (a.order(b) == .gt) a else b;
    }

    /// Pick the "leftmost" upper bound (= min). An empty primary means
    /// "unbounded on the right" — the OTHER bound wins.
    fn tighter_right(a: Key, b: Key) Key {
        if (a.primary.len == 0) return b;
        if (b.primary.len == 0) return a;
        return if (a.order(b) == .lt) a else b;
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
// Level
// ---------------------------------------------------------------------------

/// The lock a tree keeps over its levels. Shared to read a level's list,
/// exclusive to change it. Every `Level` method takes a token minted by
/// it (`Lock.Shared` / `Lock.Exclusive`), so a level cannot be touched
/// without the tree's lock in hand, and the parameter says which mode.
pub const Lock = fiber.RWMutex(.untracked);

/// One level of a tree's LSM. No lock of its own: `tables` and
/// `total_size` are guarded by the owning tree's `Lock`, and every
/// method here takes its token. Readers that go on to touch a table's
/// bytes pin it (a ref) while they hold the lock and read after releasing
/// it, so the lock is never held across I/O: `overlappingTables`,
/// `acquireTables`, and the compaction builder's snapshots all work that
/// way.
pub const Level = struct {
    tables: std.ArrayList(TableEntry),
    total_size: i64, // sum of each table's buf.len
    level: u8,
    allocator: std.mem.Allocator,
    /// The owning tree's lock — what every method's token must be for.
    mu: *const Lock,

    pub fn init(allocator: std.mem.Allocator, level: u8, mu: *const Lock) Level {
        return .{
            .tables = .empty,
            .total_size = 0,
            .level = level,
            .allocator = allocator,
            .mu = mu,
        };
    }

    /// deinit releases each table's reference without deleting SST files.
    /// Used by Tree.close(); detaches the path so decrRef at
    /// zero only frees memory.
    pub fn deinit(self: *Level) void {
        for (self.tables.items) |e| {
            e.table.detach(); // clear path → decrRef won't delete the file
            e.table.decrRef();
        }
        self.tables.deinit(self.allocator);
    }

    /// Number of tables currently in this level.
    pub fn tableCount(self: *Level, held: Lock.Shared) usize {
        self.mu.assertHeld(held);
        return self.tables.items.len;
    }

    /// The level's table list, borrowed for as long as `held` is.
    pub fn entries(self: *const Level, held: Lock.Shared) []const TableEntry {
        self.mu.assertHeld(held);
        return self.tables.items;
    }

    /// initEntries bulk-sets entries, sorts them, and recomputes total_size.
    pub fn initEntries(self: *Level, held: Lock.Exclusive, new_entries: []const TableEntry) !void {
        self.mu.assertHeld(held);
        self.tables.clearRetainingCapacity();
        try self.tables.appendSlice(self.allocator, new_entries);
        self.sortEntries();

        self.total_size = 0;
        for (self.tables.items) |e| {
            self.total_size += @intCast(e.table.fileSize());
        }
    }

    /// sortEntries: L0 → by id ascending; L≥1 → by table.smallest key.
    /// L0 tables would overlap. Newer tables would have higher ids and would be at the end.
    /// L1 and onwards would have non-overlapping tables.
    fn sortEntries(self: *Level) void {
        if (self.level == 0) {
            std.mem.sort(TableEntry, self.tables.items, {}, struct {
                fn lt(_: void, a: TableEntry, b: TableEntry) bool {
                    return a.id < b.id;
                }
            }.lt);
        } else {
            std.mem.sort(TableEntry, self.tables.items, {}, struct {
                fn lt(_: void, a: TableEntry, b: TableEntry) bool {
                    return a.table.smallest.order(b.table.smallest) == .lt;
                }
            }.lt);
        }
    }

    /// addEntry appends an entry, sorts, and increments the table's ref count.
    pub fn addEntry(self: *Level, held: Lock.Exclusive, id: u64, t: *Table) !void {
        self.mu.assertHeld(held);
        try self.tables.append(self.allocator, .{ .id = id, .table = t });
        t.incrRef();
        self.total_size += @intCast(t.fileSize());
        self.sortEntries();
    }

    /// deleteEntry removes a single entry by id and decrements the removed
    /// table's ref.
    pub fn deleteEntry(self: *Level, held: Lock.Exclusive, id: u64) void {
        self.mu.assertHeld(held);
        return self.deleteEntries(&.{.{ .id = id, .table = undefined }});
    }

    fn deleteEntries(self: *Level, to_del: []const TableEntry) void {
        // Iterate over the full list of tables we own, and see for each table whether
        // it's id is present in the ids list provided.
        var i: usize = self.tables.items.len;
        while (i > 0) {
            i -= 1;
            const e = self.tables.items[i];
            // I think looping here is fine, given we expect ids to be small.
            // We can consider storing ids in a map as a micro-optimization.
            const found = for (to_del) |entry| {
                if (e.id == entry.id) break true;
            } else false;

            if (found) {
                // TableEntry e is on the chopping board.
                self.total_size -= @intCast(e.table.fileSize());
                e.table.decrRef();
                _ = self.tables.orderedRemove(i);
            }
        }
    }

    /// replaceEntries deletes del_ids, appends to_add, then sorts.
    pub fn replaceEntries(self: *Level, held: Lock.Exclusive, to_del: []const TableEntry, to_add: []const TableEntry) !void {
        self.mu.assertHeld(held);
        self.deleteEntries(to_del);

        // Add new entries.
        for (to_add) |e| {
            try self.tables.append(self.allocator, e);
            e.table.incrRef();
            self.total_size += @intCast(e.table.fileSize());
        }

        self.sortEntries();
    }

    /// overlappingTables returns a heap-allocated slice of TableEntry whose key
    /// range overlaps kr. The caller must free the slice when done.
    /// L0: always all tables; L≥1: binary search on both ends.
    ///
    /// Bounds compare on the PRIMARY only (orderWithoutTs). A full Key.order
    /// would be wrong here: it sorts timestamps descending, so a table whose
    /// smallest key is an older version of kr.right's primary compares as
    /// "starts after kr.right" and gets excluded — even though it holds that
    /// very primary. Callers use this to answer "which tables could hold keys
    /// in this primary range", and every one of them (compaction src/dest
    /// selection, openMerge) needs every version of a boundary primary, not
    /// the subset that happens to sort inside a versioned bound.
    ///
    /// The returned tables are pinned, so the caller can release the lock
    /// and read them after.
    pub fn overlappingTables(self: *Level, held: Lock.Shared, allocator: std.mem.Allocator, kr: KeyRange) ![]TableEntry {
        self.mu.assertHeld(held);
        const items = self.tables.items;
        const slice: []TableEntry = if (self.level == 0)
            items
        else if (kr.isEmpty())
            items[0..0]
        else blk: {
            // First table where kr.left <= biggest (could overlap on the left).
            const left = utils.search(TableEntry, items, kr.left, struct {
                fn f(kl: Key, e: TableEntry) bool {
                    return kl.orderWithoutTs(e.table.biggest) != .gt;
                }
            }.f);
            // First table where kr.right < smallest (no longer overlapping).
            // An empty kr.right means "no upper bound" — sweep to end.
            // Without this, the binary search would treat the empty primary
            // as smaller than every table's smallest key and return 0,
            // making the slice empty even when the left bound is well-formed.
            const right = if (kr.right.primary.len == 0)
                items.len
            else
                utils.search(TableEntry, items, kr.right, struct {
                    fn f(right_key: Key, e: TableEntry) bool {
                        return right_key.orderWithoutTs(e.table.smallest) == .lt;
                    }
                }.f);
            break :blk items[left..right];
        };

        // Pin every returned table while the caller's lock is still held.
        // The *Table pointers must survive a concurrent applyCompaction
        // deleting these entries (decrRef at zero frees the Table; the
        // file stays). Callers release with `releaseTables` once done —
        // TableIterator.init takes its own ref, so iterators outlive the
        // release.
        const out = try allocator.dupe(TableEntry, slice);
        for (out) |e| e.table.incrRef();
        return out;
    }

    /// Release a slice returned by `overlappingTables`: drop the pins
    /// and free the slice.
    pub fn releaseTables(
        allocator: std.mem.Allocator,
        tables: []TableEntry,
    ) void {
        for (tables) |e| e.table.decrRef();
        allocator.free(tables);
    }

    pub fn getTotalSize(self: *Level, held: Lock.Shared) i64 {
        self.mu.assertHeld(held);
        return self.total_size;
    }

    pub fn numTables(self: *Level, held: Lock.Shared) usize {
        self.mu.assertHeld(held);
        return self.tables.items.len;
    }

    /// Pin the tables that could hold `search_key`, appended to `out` in
    /// read order: L0 back-to-front (highest id = newest) so the first hit
    /// wins; L≥1 the one table whose range covers the key. Each pin is a
    /// ref, so the caller releases the lock and reads afterwards — an
    /// install that drops one of these tables from the level meanwhile
    /// frees nothing the reader holds. Hand the list to `toOwnedSlice` and
    /// release with `releaseTables`.
    pub fn acquireTables(self: *Level, held: Lock.Shared, allocator: std.mem.Allocator, search_key: Key, out: *std.ArrayList(TableEntry)) !void {
        self.mu.assertHeld(held);
        if (self.level == 0) {
            var ri: usize = self.tables.items.len;
            while (ri > 0) {
                ri -= 1;
                const e = self.tables.items[ri];
                if (!e.table.containsKey(search_key)) continue;
                try out.append(allocator, e);
                e.table.incrRef();
            }
            return;
        }

        // First table whose biggest key >= search_key.
        const lo = utils.search(TableEntry, self.tables.items, search_key, struct {
            fn f(sk: Key, e: TableEntry) bool {
                return e.table.biggest.order(sk) != .lt;
            }
        }.f);
        if (lo >= self.tables.items.len) return;

        const e = self.tables.items[lo];

        // Verify the key can actually be in this table. Compare without considering timestamp.
        // A table whose smallest primary is greater than the search primary cannot contain the
        // key at any timestamp. Note that full Key.order includes reversed timestamp which
        // would give wrong results when the search key uses timestamp = maxInt as a "find any
        // version" sentinel. So, we don't use "order". Instead use "orderWithoutTs".
        if (e.table.smallest.orderWithoutTs(search_key) == .gt) return;
        if (!e.table.containsKey(search_key)) return;

        try out.append(allocator, e);
        e.table.incrRef();
    }

    /// Seek `search_key` in one pinned table: the item on a hit (owned —
    /// primary and value duped with `allocator`), null on a miss. No lock:
    /// the pin keeps the table readable.
    pub fn readKey(allocator: std.mem.Allocator, t: *Table, search_key: Key, priority: vfs.Priority) Error!?Item {
        var it = TableIterator.init(allocator, t, priority);
        defer it.deinit();

        try it.seekRange(search_key, .{});
        const found = it.itemRef() orelse return null;
        if (!search_key.equalPrimary(found.key)) return null;

        return try found.dupe(allocator);
    }

    /// `acquireTables` then `readKey` over this one level, under the
    /// caller's hold for both. `Tree.get` is the production path: it pins
    /// across every level under one hold and reads after releasing.
    pub fn get(self: *Level, held: Lock.Shared, search_key: Key, priority: vfs.Priority) !?Item {
        var list: std.ArrayList(TableEntry) = .empty;
        errdefer {
            for (list.items) |e| e.table.decrRef();
            list.deinit(self.allocator);
        }
        try self.acquireTables(held, self.allocator, search_key, &list);
        const tables = try list.toOwnedSlice(self.allocator);
        defer releaseTables(self.allocator, tables);
        for (tables) |e| {
            if (try readKey(self.allocator, e.table, search_key, priority)) |item| return item;
        }
        return null;
    }
};
