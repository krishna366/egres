const std = @import("std");
const zio = @import("zio");
const utils = @import("utils");
const options = @import("options.zig");
const manifest = @import("manifest.zig");
const level = @import("level.zig");
const table_mod = @import("table.zig");
const vfs = @import("vfs.zig");
const tree_mod = @import("tree.zig");

const Key = @import("key.zig").Key;
const KeyRange = level.KeyRange;
const Tree = tree_mod.Tree;
const TableCache = tree_mod.TableCache;
const Table = table_mod.Table;
const Item = table_mod.table.Item;

// ---------------------------------------------------------------------------
// RouteTable
// ---------------------------------------------------------------------------

/// RouteTable maps keys to tree_ids within a single namespace.
/// Entries are kept sorted by range.left so we can binary-search for the
/// tree that owns a given key.
pub const RouteTable = struct {
    allocator: std.mem.Allocator,
    namespace_id: u32,
    entries: std.ArrayList(RouteEntry),

    pub const RouteEntry = struct {
        tree_id: u64,
        range: KeyRange,
    };

    pub fn init(allocator: std.mem.Allocator, namespace_id: u32) RouteTable {
        return .{
            .allocator = allocator,
            .namespace_id = namespace_id,
            .entries = .{},
        };
    }

    pub fn deinit(self: *RouteTable) void {
        self.entries.deinit(self.allocator);
    }

    /// Add a route entry and keep sorted by range.left.
    pub fn addRoute(self: *RouteTable, tree_id: u64, range: KeyRange) !void {
        try self.entries.append(self.allocator, .{ .tree_id = tree_id, .range = range });
        std.mem.sort(RouteEntry, self.entries.items, {}, struct {
            fn lt(_: void, a: RouteEntry, b: RouteEntry) bool {
                return a.range.left.order(b.range.left) == .lt;
            }
        }.lt);
    }

    /// Remove a route entry by tree_id.
    pub fn removeRoute(self: *RouteTable, tree_id: u64) void {
        var i: usize = self.entries.items.len;
        while (i > 0) {
            i -= 1;
            if (self.entries.items[i].tree_id == tree_id) {
                _ = self.entries.orderedRemove(i);
                return;
            }
        }
    }

    /// Update the range for an existing route.
    pub fn updateRange(self: *RouteTable, tree_id: u64, range: KeyRange) void {
        for (self.entries.items) |*e| {
            if (e.tree_id == tree_id) {
                e.range = range;
                break;
            }
        }
        std.mem.sort(RouteEntry, self.entries.items, {}, struct {
            fn lt(_: void, a: RouteEntry, b: RouteEntry) bool {
                return a.range.left.order(b.range.left) == .lt;
            }
        }.lt);
    }

    /// Find the tree_id that owns this key, or null if none.
    pub fn lookup(self: *const RouteTable, search_key: Key) ?u64 {
        if (self.entries.items.len == 1) return self.entries.items[0].tree_id;
        if (self.entries.items.len == 0) return null;

        var lo: usize = 0;
        var hi: usize = self.entries.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.entries.items[mid].range.left.order(search_key) != .gt) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo == 0) return null;
        const candidate = &self.entries.items[lo - 1];
        if (candidate.range.inf) return candidate.tree_id;
        if (candidate.range.right.primary.len > 0 and search_key.order(candidate.range.right) == .gt) {
            return null;
        }
        return candidate.tree_id;
    }
};

// ---------------------------------------------------------------------------
// Forest
// ---------------------------------------------------------------------------

pub const Forest = struct {
    allocator: std.mem.Allocator,
    opts: options.Options,
    ms: manifest.ForestState,
    next_tree_id: std.atomic.Value(u64),
    next_file_id: std.atomic.Value(u64),
    trees: std.AutoHashMap(u64, *Tree),
    ns_routes: std.AutoHashMap(u32, *RouteTable),
    table_cache: TableCache,
    mu: std.Thread.RwLock,

    /// Open or create a forest from a directory.
    /// Rebuilds trees from the manifest state.
    /// Note: namespace routing is not persisted yet — routes must be
    /// re-established after open via createTree or addRoute.
    pub fn open(allocator: std.mem.Allocator, opts: options.Options) !*Forest {
        var ms = try manifest.openOrCreate(allocator, opts.dir);
        errdefer ms.close();

        const self = try allocator.create(Forest);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .opts = opts,
            .ms = ms,
            .next_tree_id = std.atomic.Value(u64).init(1),
            .next_file_id = std.atomic.Value(u64).init(1),
            .trees = std.AutoHashMap(u64, *Tree).init(allocator),
            .ns_routes = std.AutoHashMap(u32, *RouteTable).init(allocator),
            .table_cache = TableCache.init(allocator, opts.dir),
            .mu = .{},
        };
        errdefer self.trees.deinit();
        errdefer self.ns_routes.deinit();
        errdefer self.table_cache.deinit();

        var max_tid: u64 = 0;
        var max_fid: u64 = 0;
        {
            var it = self.ms.trees.iterator();
            while (it.next()) |kv| {
                const tree_id = kv.key_ptr.*;
                const tree_state = kv.value_ptr.*;
                if (tree_id > max_tid) max_tid = tree_id;

                // Track the highest table ID to seed next_file_id.
                for (tree_state.levels.items) |lm| {
                    var fit = lm.tables.keyIterator();
                    while (fit.next()) |fid| {
                        if (fid.* > max_fid) max_fid = fid.*;
                    }
                }

                var tree = try Tree.open(allocator, opts, tree_id, &self.ms, &self.table_cache);
                errdefer tree.close();

                const tree_ptr = try allocator.create(Tree);
                tree_ptr.* = tree;
                try self.trees.put(tree_id, tree_ptr);

                // Rebuild namespace routing from persisted tree state.
                if (tree_state.namespace_id != 0) {
                    const rt = try self.getOrCreateRouteTable(tree_state.namespace_id);
                    try rt.addRoute(tree_id, .{ .left = tree_state.left, .right = tree_state.right });
                }
            }
        }

        self.next_tree_id = std.atomic.Value(u64).init(max_tid + 1);
        self.next_file_id = std.atomic.Value(u64).init(max_fid + 1);
        return self;
    }

    pub fn close(self: *Forest) void {
        var tree_it = self.trees.iterator();
        while (tree_it.next()) |kv| {
            kv.value_ptr.*.close();
            self.allocator.destroy(kv.value_ptr.*);
        }
        self.trees.deinit();

        var rt_it = self.ns_routes.iterator();
        while (rt_it.next()) |kv| {
            kv.value_ptr.*.deinit();
            self.allocator.destroy(kv.value_ptr.*);
        }
        self.ns_routes.deinit();

        self.ms.close();
        self.table_cache.deinit();
        self.allocator.destroy(self);
    }

    /// Get a value by key, routing to the correct tree by namespace.
    pub fn get(self: *Forest, search_key: Key, priority: vfs.Priority) !?Item {
        self.mu.lockShared();
        defer self.mu.unlockShared();

        const rt = self.ns_routes.get(search_key.namespace) orelse return null;
        const tree_id = rt.lookup(search_key) orelse return null;
        const tree = self.trees.get(tree_id) orelse return null;
        return tree.get(search_key, priority);
    }

    /// Create a new tree for the given namespace and key range.
    /// Returns the new tree_id.
    pub fn createTree(self: *Forest, namespace_id: u32, range: KeyRange) !u64 {
        self.mu.lock();
        defer self.mu.unlock();

        const tree_id = self.next_tree_id.fetchAdd(1, .monotonic);

        // Commit tree creation to manifest.
        const left_raw = try range.left.marshal(self.allocator);
        defer self.allocator.free(left_raw);

        const right_raw = try range.right.marshal(self.allocator);
        defer self.allocator.free(right_raw);

        try self.ms.commit(&.{}, &.{manifest.createTreeChange(tree_id, namespace_id, left_raw, right_raw)});

        // Create in-memory tree (empty — no tables yet).
        var tree = try Tree.open(self.allocator, self.opts, tree_id, &self.ms, &self.table_cache);
        errdefer tree.close();

        const tree_ptr = try self.allocator.create(Tree);
        tree_ptr.* = tree;
        try self.trees.put(tree_id, tree_ptr);

        // Set up namespace routing.
        const rt = try self.getOrCreateRouteTable(namespace_id);
        try rt.addRoute(tree_id, range);

        return tree_id;
    }

    /// Get a tree by its id.
    pub fn getTree(self: *Forest, tree_id: u64) ?*Tree {
        self.mu.lockShared();
        defer self.mu.unlockShared();
        return self.trees.get(tree_id);
    }

    /// Compact finds the tree with the highest compaction score and runs one
    /// compaction on it. Returns true if a compaction was executed, false if
    /// no tree needs compaction.
    pub fn compact(self: *Forest) !bool {
        const Best = struct { tree: *Tree, tree_id: u64, candidate: tree_mod.CompactCandidate };
        var best: ?Best = null;

        {
            self.mu.lockShared();
            defer self.mu.unlockShared();

            var it = self.trees.iterator();
            while (it.next()) |kv| {
                const tree = kv.value_ptr.*;
                const candidate = tree.compact_candidate orelse continue;
                if (best == null or candidate.score > best.?.candidate.score) {
                    best = .{ .tree = tree, .tree_id = kv.key_ptr.*, .candidate = candidate };
                }
            }
        }

        const b = best orelse return false;
        try b.tree.doCompact(b.candidate.level, &self.next_file_id);
        self.table_cache.clearStale();

        const linfo = b.tree.lowestLevelSize();
        if (linfo.size > self.opts.tree_split_size) {
            self.mu.lock();
            defer self.mu.unlock();
            try self.splitTree(b.tree_id);
        }

        return true;
    }

    /// splitTree splits the given tree at the median key of its bottommost level.
    /// Caller must hold the write lock.
    pub fn splitTree(self: *Forest, tree_id: u64) !void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        const left_tree = self.trees.get(tree_id) orelse return;

        // Find split key from the lowest level in old tree.
        const split_key = try left_tree.findSplitKey(alloc);
        if (split_key.len() == 0) {
            return;
        }

        // Capture old tree state before the manifest commit.
        const left_tree_state = self.ms.trees.get(tree_id) orelse return;
        const new_tree_id = self.next_tree_id.fetchAdd(1, .monotonic);

        // Classify tables per level and build manifest changes.
        var table_changes: std.ArrayList(manifest.TableChange) = .{};

        // Track tables which go entirely to right tree. These tables would be removed from
        // left tree. We will rearrange the tables after MANIFEST commit.
        const TablesAtLevel = struct { level_idx: usize, entries: []level.TableEntry };
        var left_tree_remove: std.ArrayList(TablesAtLevel) = .{};

        for (left_tree.levels, 0..) |*llv, llv_idx| {
            var remove_at_level: std.ArrayList(level.TableEntry) = .{};

            {
                llv.mu.lockShared();
                defer llv.mu.unlockShared();

                for (llv.tables.items) |tbe| {
                    if (tbe.table.smallest.order(split_key) != .lt) { // .eq or .gt
                        // Right-only: remove from old tree, add to new tree.
                        try remove_at_level.append(alloc, tbe);
                        try table_changes.append(alloc, manifest.deleteTableChange(
                            tbe.id,
                            tree_id,
                        ));
                        try table_changes.append(alloc, manifest.createTableChange(
                            tbe.id,
                            new_tree_id,
                            @intCast(llv_idx),
                        ));
                    } else if (tbe.table.biggest.order(split_key) != .lt) { // .eq or .gt
                        // order: smallest key < split key <= biggest key
                        // Therefore, the split key falls within the table.
                        // Shared: keep in old tree, also add to new tree.
                        try table_changes.append(alloc, manifest.createTableChange(tbe.id, new_tree_id, @intCast(llv_idx)));
                    }
                    // Left-only: no manifest change needed.
                }
            }

            const owned = try remove_at_level.toOwnedSlice(alloc);
            try left_tree_remove.append(alloc, .{ .level_idx = llv_idx, .entries = owned });
        }

        const left_left_raw_key = try left_tree_state.left.marshal(alloc);
        const right_right_raw_key = try left_tree_state.right.marshal(alloc);
        const split_raw = try split_key.marshal(alloc);
        const namespace_id = left_tree_state.namespace_id;
        std.debug.assert(namespace_id > 0);

        const tree_changes = [2]manifest.TreeChange{
            manifest.updateTreeChange(tree_id, left_left_raw_key, split_raw),
            manifest.createTreeChange(new_tree_id, namespace_id, split_raw, right_right_raw_key),
        };
        // Commit MANIFEST so we can recover from a crash.
        try self.ms.commit(table_changes.items, &tree_changes);

        // Open the new right tree BEFORE deleting from old tree, so that
        // loadTables finds the shared tables in the cache with ref > 0.
        // It would use MANIFEST state to figure out which tables it owns.
        var new_tree = try Tree.open(self.allocator, self.opts, new_tree_id, &self.ms, &self.table_cache);
        errdefer new_tree.close();

        // Update old key range and remove tables.
        const spkey = try split_key.dupe(left_tree.allocator);
        errdefer spkey.free(left_tree.allocator);

        left_tree.key_range.right.free(left_tree.allocator);
        left_tree.key_range.right = spkey;
        left_tree.key_range.inf = false;

        for (left_tree_remove.items) |lr| {
            left_tree.levels[lr.level_idx].deleteEntries(lr.entries);
        }

        // Add new tree to the forest.
        const new_ptr = try self.allocator.create(Tree);
        errdefer self.allocator.destroy(new_ptr);
        new_ptr.* = new_tree;
        try self.trees.put(new_tree_id, new_ptr);

        // Update route table using manifest-owned Keys (post-commit).
        const rt = try self.getOrCreateRouteTable(namespace_id);
        const ms_tree_left = self.ms.trees.getPtr(tree_id).?;
        const ms_tree_right = self.ms.trees.getPtr(new_tree_id).?;
        rt.updateRange(tree_id, .{ .left = ms_tree_left.left, .right = ms_tree_left.right });
        try rt.addRoute(new_tree_id, .{ .left = ms_tree_right.left, .right = ms_tree_right.right });
    }

    fn getOrCreateRouteTable(self: *Forest, namespace_id: u32) !*RouteTable {
        if (self.ns_routes.get(namespace_id)) |rt| return rt;

        const rt = try self.allocator.create(RouteTable);
        rt.* = RouteTable.init(self.allocator, namespace_id);
        try self.ns_routes.put(namespace_id, rt);
        return rt;
    }
};
