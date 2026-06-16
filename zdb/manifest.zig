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
    // The SQL table bucket is derived from the tree_id (Key.treeTableId), not
    // stored — the tree_id is the map key alongside namespace_id (Key.TreeKey).
    levels: std.ArrayList(LevelState),
    left: Key = .{},
    right: Key = .{},
};

/// table_id 0 is reserved for a namespace's raw-KV / default bucket; user tables
/// start here. nextTableId floors every namespace's counter at this value.
pub const first_table_id: u32 = 1;

/// namespaces 1–15 are reserved (1 = catalog; the rest for future engine use).
/// User databases start here. nextNamespace floors next_ns_id at this value.
pub const first_namespace: u64 = 16;

/// In-memory manifest state. Durability comes from the Raft log + periodic
/// snapshots — this struct is never persisted to its own file. `serialize`
/// produces the bytes that go into a Raft snapshot's `data` field;
/// `restoreFrom` is the inverse, used by recovery to install the manifest
/// before WAL replay.
pub const ForestState = struct {
    allocator: std.mem.Allocator,
    trees: std.AutoHashMap(TreeKey, TreeState),
    /// Per-namespace next-free user table_id. The only authoritative source of
    /// table_id allocation: bumped on apply of every create (advanceTableId) and
    /// reserved by the leader (nextTableId), and snapshot-persisted so a
    /// re-elected leader never re-issues an id even after a DROP removed the
    /// tree. A namespace with no entry is treated as `first_table_id`.
    next_table_id: std.AutoHashMap(u64, u32),
    /// Next-free namespace id (global). Same allocate-on-leader / advance-on-
    /// apply / snapshot-persist discipline as next_table_id, one level up.
    next_ns_id: u64 = first_namespace,
    mu: fiber.Mutex(.untracked) = .{},

    pub fn init(allocator: std.mem.Allocator) ForestState {
        return .{
            .allocator = allocator,
            .trees = std.AutoHashMap(TreeKey, TreeState).init(allocator),
            .next_table_id = std.AutoHashMap(u64, u32).init(allocator),
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
        const slot = try self.next_table_id.getOrPut(namespace);
        // The id to hand out: the current counter (never below the floor), or
        // first_table_id for a namespace's first table. Then reserve the next.
        const id = if (slot.found_existing) @max(slot.value_ptr.*, first_table_id) else first_table_id;
        slot.value_ptr.* = id + 1;
        return id;
    }

    /// Raise `namespace`'s counter past `table_id` (apply path, all nodes).
    /// Idempotent; never lowers it (so re-applying or applying a low system
    /// bucket leaves a higher user counter untouched). Floors at first_table_id.
    pub fn advanceTableId(self: *ForestState, namespace: u64, table_id: u32) !void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        const slot = try self.next_table_id.getOrPut(namespace);
        // Floor the current counter, then raise it past table_id; never lowers.
        const floor = if (slot.found_existing) @max(slot.value_ptr.*, first_table_id) else first_table_id;
        slot.value_ptr.* = @max(floor, table_id +| 1);
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
        self.next_table_id.deinit();
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

        var next_table_ids: std.ArrayList(TableId) = .empty;
        var cit = self.next_table_id.iterator();
        while (cit.next()) |kv| {
            try next_table_ids.append(a, .{ .namespace = kv.key_ptr.*, .next_table_id = kv.value_ptr.* });
        }

        return encodeChangeSet(allocator, table_changes.items, tree_changes.items, next_table_ids.items, self.next_ns_id);
    }

    /// Replace the receiver's tree contents with the deserialized state from
    /// `bytes` (as produced by `serialize`). Failure mid-decode leaves the
    /// state empty, not partial.
    pub fn restoreFrom(self: *ForestState, bytes: []const u8) !void {
        // Drop existing trees + counters first; restore is a full replace.
        var it = self.trees.iterator();
        while (it.next()) |kv| self.freeTree(kv.value_ptr);
        self.trees.clearRetainingCapacity();
        self.next_table_id.clearRetainingCapacity();

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

        const ctr_len = reader.nextTableIdsLen();
        i = 0;
        while (i < ctr_len) : (i += 1) {
            const c = try reader.nextTableIdsAt(i);
            try self.next_table_id.put(c.namespace(), c.nextTableId());
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

fn encodeChangeSet(
    allocator: std.mem.Allocator,
    table_changes: []const SSTableChange,
    tree_changes: []const TreeChange,
    next_table_ids: []const TableId,
    next_ns_id: u64,
) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);

    var mw = try zen.ManifestChangeWriter.initWithBuf(allocator, &buf);
    if (next_ns_id != 0) try mw.setNextNsId(next_ns_id);

    if (tree_changes.len > 0) {
        const offsets = try allocator.alloc(u32, tree_changes.len);
        defer allocator.free(offsets);

        for (tree_changes, 0..) |tc, i| {
            var tw = try zen.TreeChangeWriter.initWithBuf(allocator, &buf);
            try tw.setOp(tc.op);
            try tw.setTreeId(tc.tree_id);
            if (tc.namespace_id != 0) try tw.setNamespaceId(tc.namespace_id);
            if (tc.left_key.len > 0) try tw.setLeftKey(tc.left_key);
            if (tc.right_key.len > 0) try tw.setRightKey(tc.right_key);
            const result = try tw.finish();
            offsets[i] = result.offset;
        }

        try mw.setTreeChanges(offsets);
    }

    if (table_changes.len > 0) {
        const offsets = try allocator.alloc(u32, table_changes.len);
        defer allocator.free(offsets);

        for (table_changes, 0..) |tc, i| {
            var tw = try zen.SSTableChangeWriter.initWithBuf(allocator, &buf);
            try tw.setOp(tc.op);
            try tw.setLevel(tc.level);
            try tw.setSstId(tc.sst_id);
            try tw.setTreeId(tc.tree_id);
            if (tc.namespace_id != 0) try tw.setNamespaceId(tc.namespace_id);
            const result = try tw.finish();
            offsets[i] = result.offset;
        }

        try mw.setSstableChanges(offsets);
    }

    if (next_table_ids.len > 0) {
        const offsets = try allocator.alloc(u32, next_table_ids.len);
        defer allocator.free(offsets);

        for (next_table_ids, 0..) |c, i| {
            var cw = try zen.TableIdWriter.initWithBuf(allocator, &buf);
            try cw.setNamespace(c.namespace);
            try cw.setNextTableId(c.next_table_id);
            const result = try cw.finish();
            offsets[i] = result.offset;
        }

        try mw.setNextTableIds(offsets);
    }

    _ = try mw.finishWithTail();
    return buf.toOwnedSlice(allocator);
}
