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
    trees: std.AutoHashMap(u64, *Tree),
    ns_routes: std.AutoHashMap(u32, *RouteTable),
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
            .trees = std.AutoHashMap(u64, *Tree).init(allocator),
            .ns_routes = std.AutoHashMap(u32, *RouteTable).init(allocator),
            .mu = .{},
        };
        errdefer self.trees.deinit();
        errdefer self.ns_routes.deinit();

        var max_tid: u64 = 0;
        {
            var it = self.ms.trees.iterator();
            while (it.next()) |kv| {
                const tree_id = kv.key_ptr.*;
                const tree_state = kv.value_ptr.*;
                if (tree_id > max_tid) max_tid = tree_id;

                var tree = try Tree.open(allocator, opts, tree_id, &self.ms);
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
        var tree = try Tree.open(self.allocator, self.opts, tree_id, &self.ms);
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
        const Best = struct { tree: *Tree, candidate: tree_mod.CompactCandidate };
        var best: ?Best = null;

        {
            self.mu.lockShared();
            defer self.mu.unlockShared();

            var it = self.trees.iterator();
            while (it.next()) |kv| {
                const tree = kv.value_ptr.*;
                const candidate = tree.compact_candidate orelse continue;
                if (best == null or candidate.score > best.?.candidate.score) {
                    best = .{ .tree = tree, .candidate = candidate };
                }
            }
        }

        const b = best orelse return false;
        try b.tree.doCompact(b.candidate.level);
        return true;
    }

    fn getOrCreateRouteTable(self: *Forest, namespace_id: u32) !*RouteTable {
        if (self.ns_routes.get(namespace_id)) |rt| return rt;

        const rt = try self.allocator.create(RouteTable);
        rt.* = RouteTable.init(self.allocator, namespace_id);
        try self.ns_routes.put(namespace_id, rt);
        return rt;
    }
};
