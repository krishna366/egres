const std = @import("std");
const zen = @import("buffers.gen.zig");
const keymod = @import("key.zig");
const Key = keymod.Key;
const TreeKey = keymod.TreeKey;
const vfs = @import("vfs");
const zbuf = @import("zbuf").zbuf;
const fiber = @import("zfiber");

// -------------------------------------------------------------------------
// Error set
// -------------------------------------------------------------------------

pub const ManifestError = error{
    TreeNotFound,
};

// -------------------------------------------------------------------------
// State types
// -------------------------------------------------------------------------

pub const LevelState = struct {
    tables: std.AutoHashMap(u64, void), // set of sst_ids at this level
};

pub const TreeState = struct {
    namespace_id: u64 = 0,
    // The SQL table is derived from the tree_id (Key.treeTableId), not
    // stored — the tree_id is the map key alongside namespace_id (Key.TreeKey).
    levels: std.ArrayList(LevelState),
    left: Key = .{},
    right: Key = .{},
};

/// table_ids 0–15 are reserved per namespace (0 = raw-KV / default table,
/// 1 = the namespace's SQL catalog, the rest for future system tables); user
/// tables start here. nextTableId floors every namespace's counter at this.
pub const first_table_id: u32 = 16;

/// namespaces 1–15 are reserved for future engine use. User databases start
/// here. nextNamespace floors next_ns_id at this value.
pub const first_namespace: u64 = 16;

/// In-memory manifest state. Durability comes from the Raft log + periodic
/// snapshots — this struct is never persisted to its own file. `serialize`
/// produces the bytes that go into a Raft snapshot's `data` field;
/// `restoreFrom` is the inverse, used by recovery to install the manifest
/// before WAL replay.
pub const ForestState = struct {
    allocator: std.mem.Allocator,
    trees: std.AutoHashMap(TreeKey, TreeState),
    /// Per-namespace manifest state (see `Namespace`), snapshot-persisted and
    /// maintained by the apply paths on every node — so a re-elected leader
    /// never re-issues a table_id, and database-name resolution is native
    /// manifest state. A namespace with no entry allocates from
    /// `first_table_id`.
    namespaces: std.AutoHashMap(u64, Namespace),
    /// Reverse index for name → namespace resolution (per-connection hot
    /// path). Keys borrow the name strings owned by `namespaces`.
    names: std.StringHashMap(u64),
    /// Next-free namespace id (global). Same allocate-on-leader / advance-on-
    /// apply / snapshot-persist discipline as the per-namespace table_id
    /// counter, one level up.
    next_ns_id: u64 = first_namespace,
    mu: fiber.Mutex(.untracked) = .{},

    pub const Namespace = struct {
        /// Next-free user table_id: the only authoritative source of
        /// table_id allocation — bumped on apply of every create
        /// (advanceTableId), reserved by the leader (nextTableId).
        next_table_id: u32 = first_table_id,
        /// Database name (SQL: CREATE DATABASE), owned by the map. Empty for
        /// unnamed namespaces (legacy admin KV path) — those never resolve
        /// by name.
        name: []const u8 = "",
    };

    pub fn init(allocator: std.mem.Allocator) ForestState {
        return .{
            .allocator = allocator,
            .trees = std.AutoHashMap(TreeKey, TreeState).init(allocator),
            .namespaces = std.AutoHashMap(u64, Namespace).init(allocator),
            .names = std.StringHashMap(u64).init(allocator),
        };
    }

    /// Reserve the next namespace id (leader path). Floors at `first_namespace`.
    /// Like table_ids, a burned id (failed propose) is never reused.
    pub fn nextNamespace(self: *ForestState) u64 {
        self.mu.lock(@src());
        defer self.mu.unlock();
        if (self.next_ns_id < first_namespace) self.next_ns_id = first_namespace;
        const id = self.next_ns_id;
        self.next_ns_id = id + 1;
        return id;
    }

    /// Raise the namespace counter past `namespace` (apply path, all nodes).
    /// Idempotent; never lowers it; floors at first_namespace.
    pub fn advanceNamespace(self: *ForestState, namespace: u64) void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        if (self.next_ns_id < first_namespace) self.next_ns_id = first_namespace;
        const next = namespace +| 1;
        if (next > self.next_ns_id) self.next_ns_id = next;
    }

    /// Reserve the next table_id for `namespace` (leader path). Floors at
    /// `first_table_id` for a namespace's first user table. The reserved id is
    /// only a hint until the create_tree commits — a burned id (failed propose)
    /// is never reused because advanceTableId only ever raises the counter.
    pub fn nextTableId(self: *ForestState, namespace: u64) !u32 {
        self.mu.lock(@src());
        defer self.mu.unlock();

        const slot = try self.namespaces.getOrPut(namespace);
        if (!slot.found_existing) slot.value_ptr.* = .{};

        // Hand out the current counter (never below the floor), reserve next.
        const id = @max(slot.value_ptr.next_table_id, first_table_id);
        slot.value_ptr.next_table_id = id + 1;
        return id;
    }

    /// Raise `namespace`'s counter past `table_id` (apply path, all nodes).
    /// Idempotent; never lowers it (so re-applying or applying a low system
    /// table leaves a higher user counter untouched). Floors at first_table_id.
    pub fn advanceTableId(self: *ForestState, namespace: u64, table_id: u32) !void {
        self.mu.lock(@src());
        defer self.mu.unlock();

        const slot = try self.namespaces.getOrPut(namespace);
        if (!slot.found_existing) slot.value_ptr.* = .{};

        // Floor the current counter, then raise it past table_id; never lowers.
        const floor = @max(slot.value_ptr.next_table_id, first_table_id);
        slot.value_ptr.next_table_id = @max(floor, table_id +| 1);
    }

    /// Record `namespace`'s database name (ns_create apply path, all nodes).
    /// Idempotent for re-applies; an empty name (legacy unnamed namespace)
    /// records nothing. A name held by ANOTHER namespace is a build-side bug
    /// (uniqueness is checked before propose) — logged and skipped, never
    /// clobbered.
    pub fn registerNamespaceName(self: *ForestState, namespace: u64, name: []const u8) !void {
        if (name.len == 0) return;

        self.mu.lock(@src());
        defer self.mu.unlock();

        if (self.names.get(name)) |owner| {
            if (owner != namespace)
                std.log.err("namespace name '{s}' already owned by ns={d}, not renaming ns={d}", .{ name, owner, namespace });
            return;
        }
        const slot = try self.namespaces.getOrPut(namespace);
        if (!slot.found_existing) slot.value_ptr.* = .{};
        if (slot.value_ptr.name.len > 0) return; // re-apply; name is immutable

        const owned = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned);
        try self.names.put(owned, namespace);
        slot.value_ptr.name = owned;
    }

    /// Resolve a database name to its namespace.
    pub fn lookupNamespace(self: *ForestState, name: []const u8) ?u64 {
        self.mu.lock(@src());
        defer self.mu.unlock();
        return self.names.get(name);
    }

    /// Forget `namespace`'s state — counter and name (drop_ns apply path).
    /// Safe because namespace ids are never reused: no later create can see
    /// the missing entry as a lowered floor, and freeing the name is by
    /// design (DROP DATABASE then CREATE DATABASE of the same name works).
    pub fn dropNamespace(self: *ForestState, namespace: u64) void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        if (self.namespaces.fetchRemove(namespace)) |kv| {
            if (kv.value.name.len > 0) {
                _ = self.names.remove(kv.value.name);
                self.allocator.free(kv.value.name);
            }
        }
    }

    fn freeTree(self: *ForestState, tree: *TreeState) void {
        for (tree.levels.items) |*lm| lm.tables.deinit();
        tree.levels.deinit(self.allocator);
        if (tree.left.primary.len > 0) self.allocator.free(tree.left.primary);
        if (tree.right.primary.len > 0) self.allocator.free(tree.right.primary);
    }

    pub fn deinit(self: *ForestState) void {
        var it = self.trees.iterator();
        while (it.next()) |kv| self.freeTree(kv.value_ptr);
        self.trees.deinit();
        self.names.deinit(); // keys borrow the names owned below
        var nit = self.namespaces.valueIterator();
        while (nit.next()) |n| {
            if (n.name.len > 0) self.allocator.free(n.name);
        }
        self.namespaces.deinit();
    }

    /// Applies a single table CREATE or DELETE to the tree's levels.
    pub fn applySSTableChange(self: *ForestState, tc: SSTableChange) !void {
        const key = TreeKey{ .namespace = tc.namespace_id, .tree_id = tc.tree_id };
        if (tc.op == .create) {
            const tree = self.trees.getPtr(key) orelse return ManifestError.TreeNotFound;
            while (tree.levels.items.len <= @as(usize, tc.level)) {
                const lm = LevelState{ .tables = std.AutoHashMap(u64, void).init(self.allocator) };
                try tree.levels.append(self.allocator, lm);
            }
            try tree.levels.items[tc.level].tables.put(tc.sst_id, {});
        } else if (tc.op == .delete) {
            const tree = self.trees.getPtr(key) orelse return ManifestError.TreeNotFound;
            // Scan levels to find and remove the table.
            for (tree.levels.items) |*lm| {
                if (lm.tables.remove(tc.sst_id)) {
                    break;
                }
            }
        }
    }

    /// Applies a tree CREATE, DELETE, or UPDATE.
    /// DELETE automatically removes all tables owned by the tree.
    /// UPDATE changes the tree's key range.
    pub fn applyTreeChange(self: *ForestState, tc: TreeChange) !void {
        const key = TreeKey{ .namespace = tc.namespace_id, .tree_id = tc.tree_id };
        if (tc.op == .create) {
            try self.trees.put(key, .{
                .namespace_id = tc.namespace_id,
                .levels = std.ArrayList(LevelState).empty,
                .left = try Key.initAlloc(self.allocator, tc.left_key),
                .right = try Key.initAlloc(self.allocator, tc.right_key),
            });
        } else if (tc.op == .delete) {
            if (self.trees.fetchRemove(key)) |kv| {
                var tree = kv.value;
                self.freeTree(&tree);
            }
        } else if (tc.op == .update) {
            const tree = self.trees.getPtr(key) orelse return ManifestError.TreeNotFound;
            tree.left.free(self.allocator);
            tree.right.free(self.allocator);

            tree.left = try Key.initAlloc(self.allocator, tc.left_key);
            tree.right = try Key.initAlloc(self.allocator, tc.right_key);
        }
    }

    /// Apply a batch of tree + table changes under the mutex. In-memory only:
    /// durability comes from the surrounding Raft log entry, not from any
    /// disk write here.
    pub fn commit(
        self: *ForestState,
        table_changes: []const SSTableChange,
        tree_changes: []const TreeChange,
    ) !void {
        if (table_changes.len == 0 and tree_changes.len == 0) return;

        self.mu.lock(@src());
        defer self.mu.unlock();

        for (tree_changes) |tc| try self.applyTreeChange(tc);
        for (table_changes) |tc| try self.applySSTableChange(tc);
    }

    /// Count total table entries across all trees and levels.
    pub fn tableCount(self: *ForestState) u32 {
        var count: u32 = 0;
        var it = self.trees.iterator();
        while (it.next()) |kv| {
            for (kv.value_ptr.levels.items) |lm| {
                count += @intCast(lm.tables.count());
            }
        }
        return count;
    }

    /// Serialize the entire ForestState into a self-describing byte blob
    /// suitable for embedding in a Raft snapshot's `data` field. Captures
    /// every tree, every per-level table list, the tree key ranges + namespace
    /// assignments, and the per-namespace `next_table_id` high-water-marks. The
    /// `next_file_id` counter is *not* persisted — it is restored from the
    /// per-node HardState lease by the Forest layer. Tree ids are derived
    /// (table_id<<32 | split_id), so there is no tree-id counter to persist.
    ///
    /// Wire format reuses the existing `ManifestChange` zbuf shape: a list of
    /// `tree_changes` (CREATE per tree), `sstable_changes` (CREATE per SST), and
    /// `next_table_ids` (one per namespace).
    pub fn serialize(self: *const ForestState, allocator: std.mem.Allocator) ![]u8 {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const a = arena.allocator();

        var tree_changes: std.ArrayList(TreeChange) = .empty;
        var table_changes: std.ArrayList(SSTableChange) = .empty;

        var it = self.trees.iterator();
        while (it.next()) |kv| {
            const tk = kv.key_ptr.*;
            const tree = kv.value_ptr.*;
            const left_raw = try tree.left.marshal(a);
            const right_raw = try tree.right.marshal(a);
            try tree_changes.append(a, createTreeChange(tk.tree_id, tk.namespace, left_raw, right_raw));
            for (tree.levels.items, 0..) |lm, level_idx| {
                var tit = lm.tables.iterator();
                while (tit.next()) |tkv| {
                    try table_changes.append(a, createSSTableChange(
                        tkv.key_ptr.*,
                        tk.namespace,
                        tk.tree_id,
                        @intCast(level_idx),
                    ));
                }
            }
        }

        var namespaces: std.ArrayList(zen.NamespaceStruct) = .empty;
        var cit = self.namespaces.iterator();
        while (cit.next()) |kv| {
            try namespaces.append(a, .{
                .id = kv.key_ptr.*,
                .next_table_id = kv.value_ptr.next_table_id,
                .name = @constCast(kv.value_ptr.name),
            });
        }

        // The arena holds the source changes; marshalRoot copies them into a
        // fresh `allocator`-owned payload, so the arena can be torn down after.
        return zen.marshalRoot(allocator, ManifestChange{
            .tree_changes = tree_changes.items,
            .sstable_changes = table_changes.items,
            .namespaces = namespaces.items,
            .next_ns_id = self.next_ns_id,
        });
    }

    /// Replace the receiver's tree contents with the deserialized state from
    /// `bytes` (as produced by `serialize`). Failure mid-decode leaves the
    /// state empty, not partial.
    pub fn restoreFrom(self: *ForestState, bytes: []const u8) !void {
        // Drop existing trees + namespace state first; restore is a full
        // replace.
        var it = self.trees.iterator();
        while (it.next()) |kv| self.freeTree(kv.value_ptr);
        self.trees.clearRetainingCapacity();
        self.names.clearRetainingCapacity(); // keys borrow the names below
        var nit = self.namespaces.valueIterator();
        while (nit.next()) |n| {
            if (n.name.len > 0) self.allocator.free(n.name);
        }
        self.namespaces.clearRetainingCapacity();

        const reader = try zen.ManifestChangeReader.parseRoot(bytes);

        // Order matters: trees must exist before tables can attach to them.
        const tc_len = reader.treeChangesLen();
        var i: u32 = 0;
        while (i < tc_len) : (i += 1) {
            const tc = try reader.treeChangesAt(i);
            try self.applyTreeChange(.{
                .op = tc.op(),
                .tree_id = tc.treeId(),
                .namespace_id = tc.namespaceId(),
                .left_key = @constCast(tc.leftKey()),
                .right_key = @constCast(tc.rightKey()),
            });
        }

        const tbl_len = reader.sstableChangesLen();
        i = 0;
        while (i < tbl_len) : (i += 1) {
            const tc = try reader.sstableChangesAt(i);
            try self.applySSTableChange(.{
                .op = tc.op(),
                .sst_id = tc.sstId(),
                .namespace_id = tc.namespaceId(),
                .tree_id = tc.treeId(),
                .level = tc.level(),
            });
        }

        const ns_len = reader.namespacesLen();
        i = 0;
        while (i < ns_len) : (i += 1) {
            const c = try reader.namespacesAt(i);
            try self.namespaces.put(c.id(), .{ .next_table_id = c.nextTableId() });
            try self.registerNamespaceName(c.id(), c.name());
        }

        self.next_ns_id = @max(reader.nextNsId(), first_namespace);
    }

    /// Deep-copy this ForestState into a fresh standalone copy. Used when
    /// building a snapshot draft so the live state is never mutated by the
    /// snapshot pipeline.
    pub fn clone(self: *const ForestState, allocator: std.mem.Allocator) !ForestState {
        const bytes = try self.serialize(allocator);
        defer allocator.free(bytes);
        var out = ForestState.init(allocator);
        errdefer out.deinit();
        try out.restoreFrom(bytes);
        return out;
    }
};

// -------------------------------------------------------------------------
// Change types — aliases to generated structs
// -------------------------------------------------------------------------

pub const SSTableChange = zen.SSTableChangeStruct;
pub const TreeChange = zen.TreeChangeStruct;
pub const ManifestChange = zen.ManifestChangeStruct;
/// A namespace's persisted next_table_id (one per namespace in a snapshot).
pub const TableId = zen.TableIdStruct;

pub fn createSSTableChange(sst_id: u64, namespace_id: u64, tree_id: u64, level: u32) SSTableChange {
    return .{ .op = .create, .sst_id = sst_id, .namespace_id = namespace_id, .tree_id = tree_id, .level = level };
}

pub fn deleteSSTableChange(sst_id: u64, namespace_id: u64, tree_id: u64, level: u32) SSTableChange {
    return .{ .op = .delete, .sst_id = sst_id, .namespace_id = namespace_id, .tree_id = tree_id, .level = level };
}

pub fn createTreeChange(tree_id: u64, namespace_id: u64, left_key: []const u8, right_key: []const u8) TreeChange {
    return .{ .op = .create, .tree_id = tree_id, .namespace_id = namespace_id, .left_key = @constCast(left_key), .right_key = @constCast(right_key) };
}

pub fn deleteTreeChange(tree_id: u64, namespace_id: u64) TreeChange {
    return .{ .op = .delete, .tree_id = tree_id, .namespace_id = namespace_id };
}

pub fn updateTreeChange(tree_id: u64, namespace_id: u64, left_key: []const u8, right_key: []const u8) TreeChange {
    return .{ .op = .update, .tree_id = tree_id, .namespace_id = namespace_id, .left_key = @constCast(left_key), .right_key = @constCast(right_key) };
}

// -------------------------------------------------------------------------
// Encoding helpers
// -------------------------------------------------------------------------
