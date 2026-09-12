const std = @import("std");
const _zen = @import("buffers.gen.zig");
const Key = @import("kv.zig").Key;
const KeyRange = @import("level.zig").KeyRange;
const TreeKey = @import("kv.zig").TreeKey;
const TableKey = @import("kv.zig").TableKey;
const treeTableId = @import("kv.zig").treeTableId;
const makeTreeId = @import("kv.zig").makeTreeId;
const vfs = @import("vfs");
const zbuf = @import("zbuf").zbuf;
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;
const Forest = @import("forest.zig").Forest;
const Tree = @import("tree.zig").Tree;
/// This file, so the Forest functions below keep their `_manifest.` prefix.
const _manifest = @This();

/// The manifest lock. Shared for the name lookups on the query path and
/// the copy-out readers, exclusive for the apply path's commits.
pub const Lock = fiber.RWMutex(.untracked);

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

/// The namespace's SQL catalog table (schema rows for its tables). Its tree
/// is created with the namespace itself (see forest.buildNamespaceCreate).
pub const catalog_table_id: u32 = 1;

/// The namespace's SQL index catalog (one row per index). Like the catalog,
/// its tree is born with the namespace.
pub const index_catalog_table_id: u32 = 2;

/// Every tree a namespace is born with, by table_id: the default raw-KV
/// table plus the SQL catalogs. forest.buildNamespaceCreate and the genesis
/// seeder both loop over this — extend the list to add a system tree.
pub const namespace_system_table_ids =
    [_]u32{ 0, catalog_table_id, index_catalog_table_id };

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
    mu: Lock = .{},

    pub const Namespace = struct {
        /// Next-free user table_id: the only authoritative source of
        /// table_id allocation — bumped on apply of every create
        /// (advanceTableId), reserved by the leader (nextTableId).
        next_table_id: u32 = first_table_id,
        /// Database name (SQL: CREATE DATABASE), owned by the map. Empty for
        /// unnamed namespaces (legacy admin KV path) — those never resolve
        /// by name.
        name: String = "",
        /// SQL table name → table_id for this namespace's tables. Keys are
        /// allocator-owned dupes. Registered on create_tree apply (the
        /// TreeChange carries the name), unregistered on drop_tree apply;
        /// freed wholesale by deinit.
        tables: std.StringHashMapUnmanaged(u32) = .empty,

        /// Free the owned strings (database name + table-name keys) and the
        /// map. The caller removes `name` from the ForestState.names reverse
        /// index first — its key borrows the string freed here.
        pub fn deinit(self: *Namespace, allocator: std.mem.Allocator) void {
            if (self.name.len > 0) allocator.free(self.name);
            var it = self.tables.keyIterator();
            while (it.next()) |k| allocator.free(k.*);
            self.tables.deinit(allocator);
        }
    };

    pub fn init(allocator: std.mem.Allocator) ForestState {
        return .{
            .allocator = allocator,
            .trees = std.AutoHashMap(TreeKey, TreeState).init(allocator),
            .namespaces = std.AutoHashMap(u64, Namespace).init(allocator),
            .names = std.StringHashMap(u64).init(allocator),
        };
    }

    /// Reserve the next namespace id (leader path). Floors at
    /// `first_namespace`. Like table_ids, a burned id (failed propose) is never
    /// reused.
    pub fn nextNamespace(self: *ForestState) u64 {
        self.mu.lock(@src());
        defer self.mu.unlock();
        if (self.next_ns_id < first_namespace)
            self.next_ns_id = first_namespace;
        const id = self.next_ns_id;
        self.next_ns_id = id + 1;
        return id;
    }

    /// Raise the namespace counter past `namespace` (apply path, all nodes).
    /// Idempotent; never lowers it; floors at first_namespace.
    pub fn advanceNamespace(self: *ForestState, namespace: u64) void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        if (self.next_ns_id < first_namespace)
            self.next_ns_id = first_namespace;
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
    pub fn advanceTableId(
        self: *ForestState,
        namespace: u64,
        table_id: u32,
    ) !void {
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
    pub fn registerNamespaceName(
        self: *ForestState,
        namespace: u64,
        name: String,
    ) !void {
        if (name.len == 0) return;

        self.mu.lock(@src());
        defer self.mu.unlock();

        if (self.names.get(name)) |owner| {
            if (owner != namespace)
                std.log.err(
                    "namespace name '{s}' already owned by ns={d}, not " ++
                        "renaming ns={d}",
                    .{ name, owner, namespace },
                );
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
    pub fn lookupNamespace(self: *ForestState, name: String) ?u64 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        return self.names.get(name);
    }

    /// Record a table's name (create_tree apply path, all nodes). Idempotent
    /// for re-applies (same name → same table_id). A name held by a DIFFERENT
    /// table is a build-side bug (uniqueness is checked before propose) —
    /// logged and skipped, never clobbered. Empty name records nothing
    /// (system trees, splits).
    pub fn registerTableName(
        self: *ForestState,
        namespace: u64,
        table_id: u32,
        name: String,
    ) !void {
        if (name.len == 0) return;

        self.mu.lock(@src());
        defer self.mu.unlock();

        const slot = try self.namespaces.getOrPut(namespace);
        if (!slot.found_existing) slot.value_ptr.* = .{};

        if (slot.value_ptr.tables.get(name)) |owner| {
            if (owner != table_id)
                std.log.err(
                    "table name '{s}' in ns={d} already owned by " ++
                        "table_id={d}, not renaming table_id={d}",
                    .{ name, namespace, owner, table_id },
                );
            return;
        }
        const owned = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned);
        try slot.value_ptr.tables.put(self.allocator, owned, table_id);
    }

    /// Resolve a table name to its table_id within a namespace (per-query
    /// hot path).
    pub fn lookupTable(
        self: *ForestState,
        namespace: u64,
        name: String,
    ) ?u32 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        const ns = self.namespaces.getPtr(namespace) orelse return null;
        return ns.tables.get(name);
    }

    /// Forget a table's manifest state — today just its name entry
    /// (drop_tree apply path, all nodes). Safe because table_ids are never
    /// reused; freeing the name is by design (DROP TABLE then CREATE TABLE
    /// of the same name works). Reverse lookup walks the map — drops are
    /// rare, name resolution is the hot path. Idempotent.
    pub fn dropTable(self: *ForestState, namespace: u64, table_id: u32) void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        const ns = self.namespaces.getPtr(namespace) orelse return;
        var it = ns.tables.iterator();
        while (it.next()) |kv| {
            if (kv.value_ptr.* != table_id) continue;
            const owned = kv.key_ptr.*;
            ns.tables.removeByPtr(kv.key_ptr);
            self.allocator.free(owned);
            return;
        }
    }

    /// Forget `namespace`'s state — counter and names (drop_ns apply path).
    /// Safe because namespace ids are never reused: no later create can see
    /// the missing entry as a lowered floor, and freeing the names is by
    /// design (DROP DATABASE then CREATE DATABASE of the same name works).
    pub fn dropNamespace(self: *ForestState, namespace: u64) void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        if (self.namespaces.fetchRemove(namespace)) |kv| {
            if (kv.value.name.len > 0) _ = self.names.remove(kv.value.name);
            var v = kv.value;
            v.deinit(self.allocator);
        }
    }

    /// A tree's key range, duped into `alloc` under the manifest lock
    /// (`inf` set as `Tree.open` sets it: both bounds empty). `commit` on
    /// the apply loop rehashes `trees` and, on a delete or a range update,
    /// frees the very bytes a stored `TreeState` borrows — so a caller off
    /// that loop copies under the lock instead of holding a `TreeState`
    /// across its own work. Null if the tree is not listed.
    pub fn treeRange(
        self: *ForestState,
        alloc: std.mem.Allocator,
        key: TreeKey,
    ) !?KeyRange {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        const tree = self.trees.getPtr(key) orelse return null;
        const range = KeyRange{
            .left = tree.left,
            .right = tree.right,
            .inf = tree.left.len() == 0 and tree.right.len() == 0,
        };
        return try range.dupe(alloc);
    }

    /// Every listed tree's key — one namespace's when `namespace` is
    /// given — copied out under the lock. Caller frees the slice.
    pub fn treeKeys(
        self: *ForestState,
        alloc: std.mem.Allocator,
        namespace: ?u64,
    ) ![]TreeKey {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        var out: std.ArrayList(TreeKey) = .empty;
        errdefer out.deinit(alloc);
        var it = self.trees.keyIterator();
        while (it.next()) |tk| {
            if (namespace) |ns| if (tk.namespace != ns) continue;
            try out.append(alloc, tk.*);
        }
        return out.toOwnedSlice(alloc);
    }

    /// Every SST id any tree lists (a table shared by two trees appears
    /// twice), copied out under the lock. Caller frees the slice.
    pub fn listedFileIds(self: *ForestState, alloc: std.mem.Allocator) ![]u64 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        var out: std.ArrayList(u64) = .empty;
        errdefer out.deinit(alloc);
        var it = self.trees.valueIterator();
        while (it.next()) |tree| {
            for (tree.levels.items) |lm| {
                var tit = lm.tables.keyIterator();
                while (tit.next()) |fid| try out.append(alloc, fid.*);
            }
        }
        return out.toOwnedSlice(alloc);
    }

    /// A tree's manifest record, borrowed for as long as `held` is: the
    /// record's key bytes are freed by a delete or range update, so this
    /// exists only under a caller's hold. Null if the tree is not listed.
    pub fn treeState(
        self: *ForestState,
        held: Lock.Shared,
        key: TreeKey,
    ) ?*const TreeState {
        self.mu.assertHeld(held);
        return self.trees.getPtr(key);
    }

    fn freeTree(self: *ForestState, tree_state: *TreeState) void {
        for (tree_state.levels.items) |*lm| lm.tables.deinit();
        tree_state.levels.deinit(self.allocator);
        if (tree_state.left.primary.len > 0)
            self.allocator.free(tree_state.left.primary);
        if (tree_state.right.primary.len > 0)
            self.allocator.free(tree_state.right.primary);
    }

    pub fn deinit(self: *ForestState) void {
        var it = self.trees.iterator();
        while (it.next()) |kv| self.freeTree(kv.value_ptr);
        self.trees.deinit();
        self.names.deinit(); // keys borrow the names owned below
        var nit = self.namespaces.valueIterator();
        while (nit.next()) |n| n.deinit(self.allocator);
        self.namespaces.deinit();
    }

    /// Applies a single table CREATE or DELETE to the tree's levels. Caller
    /// holds the manifest lock exclusively — `held` is the proof.
    pub fn applySSTableChange(
        self: *ForestState,
        held: Lock.Exclusive,
        tc: SSTableChange,
    ) !void {
        self.mu.assertHeld(held);
        const key = TreeKey{
            .namespace = tc.namespace_id,
            .tree_id = tc.tree_id,
        };
        if (tc.op == .create) {
            const tree = self.trees.getPtr(key) orelse
                return ManifestError.TreeNotFound;
            while (tree.levels.items.len <= @as(usize, tc.level)) {
                const lm = LevelState{
                    .tables = std.AutoHashMap(u64, void).init(self.allocator),
                };
                try tree.levels.append(self.allocator, lm);
            }
            try tree.levels.items[tc.level].tables.put(tc.sst_id, {});
        } else if (tc.op == .delete) {
            const tree = self.trees.getPtr(key) orelse
                return ManifestError.TreeNotFound;
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
    /// UPDATE changes the tree's key range. Caller holds the manifest lock
    /// exclusively — `held` is the proof.
    pub fn applyTreeChange(
        self: *ForestState,
        held: Lock.Exclusive,
        tc: TreeChange,
    ) !void {
        self.mu.assertHeld(held);
        const key = TreeKey{
            .namespace = tc.namespace_id,
            .tree_id = tc.tree_id,
        };
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
            const tree = self.trees.getPtr(key) orelse
                return ManifestError.TreeNotFound;
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

        const held = self.mu.hold(.exclusive, @src());
        defer held.release();

        for (tree_changes) |tc| try self.applyTreeChange(held, tc);
        for (table_changes) |tc| try self.applySSTableChange(held, tc);
    }

    /// Count total table entries across all trees and levels.
    pub fn tableCount(self: *ForestState) u32 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
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
    /// `tree_changes` (CREATE per tree), `sstable_changes` (CREATE per SST),
    /// and `next_table_ids` (one per namespace).
    pub fn serialize(self: *ForestState, allocator: std.mem.Allocator) ![]u8 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const a = arena.allocator();

        var tree_changes: std.ArrayList(TreeChange) = .empty;
        var table_changes: std.ArrayList(SSTableChange) = .empty;

        // Reverse map (ns, table_id) → table name, so a tree's CREATE change
        // below can carry its table's name — the same vehicle the live
        // create_tree apply uses (TreeChange.name).
        var tnames = std.AutoHashMap(TableKey, String).init(a);
        var nit = self.namespaces.iterator();
        while (nit.next()) |kv| {
            var tit = kv.value_ptr.tables.iterator();
            while (tit.next()) |t| {
                // t.key = table name. t.value = uint32 table id.
                try tnames.put(.{
                    .namespace = kv.key_ptr.*,
                    .table_id = t.value_ptr.*,
                }, t.key_ptr.*);
            }
        }

        var it = self.trees.iterator();
        while (it.next()) |kv| {
            const tk = kv.key_ptr.*;
            const tree = kv.value_ptr.*;
            const left_raw = try tree.left.marshal(a);
            const right_raw = try tree.right.marshal(a);
            var change = createTreeChange(
                tk.tree_id,
                tk.namespace,
                left_raw,
                right_raw,
            );
            // Only the table's FIRST tree carries the name — exactly one
            // tree per table has the open left bound (splits leave it with
            // the leftmost child), so a heavily-split table doesn't copy
            // its name into every TreeChange.
            if (tree.left.primary.len == 0) {
                if (tnames.get(.{
                    .namespace = tk.namespace,
                    .table_id = treeTableId(tk.tree_id),
                })) |n|
                    change.name = @constCast(n);
            }
            try tree_changes.append(a, change);
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

        var namespaces: std.ArrayList(_zen.NamespaceStruct) = .empty;
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
        return _zen.marshalRoot(allocator, ManifestChange{
            .tree_changes = tree_changes.items,
            .sstable_changes = table_changes.items,
            .namespaces = namespaces.items,
            .next_ns_id = self.next_ns_id,
        });
    }

    /// Replace the receiver's tree contents with the deserialized state from
    /// `bytes` (as produced by `serialize`). Failure mid-decode leaves the
    /// state empty, not partial. Recovery-time, before the apply barrier
    /// lifts: nothing else touches the manifest meanwhile. Two phases —
    /// the map rebuild under an exclusive hold, then the name registry
    /// through the same one-shot calls the apply path uses.
    pub fn restoreFrom(self: *ForestState, bytes: String) !void {
        const reader = try _zen.ManifestChangeReader.parseRoot(bytes);
        {
            const held = self.mu.hold(.exclusive, @src());
            defer held.release();

            // Drop existing trees + namespace state first; restore is a full
            // replace.
            var it = self.trees.iterator();
            while (it.next()) |kv| self.freeTree(kv.value_ptr);
            self.trees.clearRetainingCapacity();
            self.names.clearRetainingCapacity(); // keys borrow the names below
            var nit = self.namespaces.valueIterator();
            while (nit.next()) |n| n.deinit(self.allocator);
            self.namespaces.clearRetainingCapacity();

            // Order matters: trees must exist before tables can attach to them.
            const tc_len = reader.treeChangesLen();
            var i: u32 = 0;
            while (i < tc_len) : (i += 1) {
                const tc = try reader.treeChangesAt(i);
                try self.applyTreeChange(held, .{
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
                try self.applySSTableChange(held, .{
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
                const slot = try self.namespaces.getOrPut(c.id());
                if (!slot.found_existing) slot.value_ptr.* = .{};
                slot.value_ptr.next_table_id = c.nextTableId();
            }

            self.next_ns_id = @max(reader.nextNsId(), first_namespace);
        }

        // Names: serialize stamps a table's name onto its tree's change
        // (only user tables carry one; empty is a no-op), and each
        // namespace record carries the database name.
        const tc_len = reader.treeChangesLen();
        var i: u32 = 0;
        while (i < tc_len) : (i += 1) {
            const tc = try reader.treeChangesAt(i);
            if (tc.op() == .create)
                try self.registerTableName(
                    tc.namespaceId(),
                    treeTableId(tc.treeId()),
                    tc.name(),
                );
        }
        const ns_len = reader.namespacesLen();
        i = 0;
        while (i < ns_len) : (i += 1) {
            const c = try reader.namespacesAt(i);
            try self.registerNamespaceName(c.id(), c.name());
        }
    }

    /// Deep-copy this ForestState into a fresh standalone copy. Used when
    /// building a snapshot draft so the live state is never mutated by the
    /// snapshot pipeline.
    pub fn clone(
        self: *ForestState,
        allocator: std.mem.Allocator,
    ) !ForestState {
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

pub const SSTableChange = _zen.SSTableChangeStruct;
pub const TreeChange = _zen.TreeChangeStruct;
pub const ManifestChange = _zen.ManifestChangeStruct;
pub const Command = _zen.CommandEnum;

pub fn createSSTableChange(
    sst_id: u64,
    namespace_id: u64,
    tree_id: u64,
    level: u32,
) SSTableChange {
    return .{
        .op = .create,
        .sst_id = sst_id,
        .namespace_id = namespace_id,
        .tree_id = tree_id,
        .level = level,
    };
}

pub fn deleteSSTableChange(
    sst_id: u64,
    namespace_id: u64,
    tree_id: u64,
    level: u32,
) SSTableChange {
    return .{
        .op = .delete,
        .sst_id = sst_id,
        .namespace_id = namespace_id,
        .tree_id = tree_id,
        .level = level,
    };
}

pub fn createTreeChange(
    tree_id: u64,
    namespace_id: u64,
    left_key: String,
    right_key: String,
) TreeChange {
    return .{
        .op = .create,
        .tree_id = tree_id,
        .namespace_id = namespace_id,
        .left_key = @constCast(left_key),
        .right_key = @constCast(right_key),
    };
}

pub fn deleteTreeChange(tree_id: u64, namespace_id: u64) TreeChange {
    return .{ .op = .delete, .tree_id = tree_id, .namespace_id = namespace_id };
}

pub fn updateTreeChange(
    tree_id: u64,
    namespace_id: u64,
    left_key: String,
    right_key: String,
) TreeChange {
    return .{
        .op = .update,
        .tree_id = tree_id,
        .namespace_id = namespace_id,
        .left_key = @constCast(left_key),
        .right_key = @constCast(right_key),
    };
}

// -------------------------------------------------------------------------
// Forest: building and applying manifest changes
// -------------------------------------------------------------------------

/// installTree opens the in-memory Tree for the given id and adds it to its
/// table's TableTrees (keyed by the high 32 bits of tree_id). Tree.open
/// reads the tree's key range from the manifest, so the caller must have
/// already committed the TreeChange. The open — which loads the tree's
/// SSTs, file I/O — holds nothing; the publish is the map's own insert
/// plus one list append under the table's lock, and the tree is
/// invisible until that append.
pub fn installTree(self: *Forest, namespace_id: u64, tree_id: u64) !void {
    const tree = try Tree.open(
        self.allocator,
        self.opts,
        namespace_id,
        tree_id,
        &self.ms,
        &self.table_cache,
    );
    errdefer tree.release();

    const tbl_trees = try self.getOrCreateTable(
        namespace_id,
        treeTableId(tree_id),
    );
    const held = tbl_trees.mu.hold(.exclusive, @src());
    defer held.release();
    try tbl_trees.addTree(held, tree);
}

/// Walk every tree's level table list and append the file_ids of any SST
/// not currently on disk. The returned slice is owned by the caller.
pub fn collectMissingFileIds(
    self: *Forest,
    allocator: std.mem.Allocator,
) ![]u64 {
    // The ids come out under the manifest lock; the disk check (a stat
    // per file) runs after it is released.
    const listed = try self.ms.listedFileIds(allocator);
    defer allocator.free(listed);
    var out: std.ArrayList(u64) = .empty;
    errdefer out.deinit(allocator);
    for (listed) |fid| {
        if (!self.hasFile(fid)) try out.append(allocator, fid);
    }
    return out.toOwnedSlice(allocator);
}

/// Walk `self.ms.trees` and install each tree (Tree + TableTrees entries).
/// Used after `ForestState.restoreFrom(snapshot.data)` at recovery — the
/// manifest already names every tree and table, but the in-memory Tree
/// objects and TableTrees need to be rebuilt from it. Tree ids are
/// derived (table_id<<32 | split_id), so there is no counter to advance.
///
/// File ids are NOT scanned here: each node's local counter is
/// restored from HardState's `max_file_id` via `forest.file_ids.bind`,
/// which is independent of (and immune to) ids in other nodes'
/// namespaces that show up in the manifest.
pub fn openTablesFromManifest(self: *Forest) !void {
    // installTree opens each Tree (which dupes its key range from the
    // manifest), so we only need the keys here.
    const keys = try self.ms.treeKeys(self.allocator, null);
    defer self.allocator.free(keys);
    for (keys) |tk| try self.installTree(tk.namespace, tk.tree_id);
}

/// Create a new tree for the given namespace, table, and key range.
/// Single-node path (no Raft) — used by tests. Returns the new tree_id,
/// derived from the table's next free split index.
pub fn createTree(
    self: *Forest,
    namespace_id: u64,
    table_id: u32,
    range: KeyRange,
) !u64 {
    const tree_id = self.nextTreeId(namespace_id, table_id);

    const left_raw = try range.left.marshal(self.allocator);
    defer self.allocator.free(left_raw);
    const right_raw = try range.right.marshal(self.allocator);
    defer self.allocator.free(right_raw);

    try self.ms.commit(&.{}, &.{
        _manifest.createTreeChange(
            tree_id,
            namespace_id,
            left_raw,
            right_raw,
        ),
    });
    try self.installTree(namespace_id, tree_id);
    return tree_id;
}

/// buildNamespaceCreate is Phase A of the Raft-replicated namespace create:
/// returns a ManifestChange with one TreeChange per system table
/// (manifest.namespace_system_table_ids) — the namespace's default
/// (raw-KV) table (table_id 0, tree_id 0, carrying the namespace's `name`:
/// a SQL database name; empty for unnamed namespaces) and its SQL
/// catalogs — all with an infinite range (empty left+right). No state
/// mutates, so a failed propose costs nothing. Caller owns the returned
/// slice; `name` must outlive the change (it is marshalled at propose).
///
/// Errors with `NamespaceAlreadyExists` if `namespace_id`'s default table
/// already exists, `NameExists` if another namespace holds `name`.
pub fn buildNamespaceCreate(
    self: *Forest,
    alloc: std.mem.Allocator,
    namespace_id: u64,
    name: String,
) !_manifest.ManifestChange {
    std.debug.assert(namespace_id > 0);
    if (self.hasNamespace(namespace_id)) return error.NamespaceAlreadyExists;
    if (self.ms.lookupNamespace(name)) |_| return error.NameExists;

    // One tree per system table: the default (raw-KV) table plus the SQL
    // catalogs, born with the namespace — catalog rows must never race
    // tree creation, because buildFlush silently drops keys whose
    // (ns, table_id) has no tree.
    const ids = _manifest.namespace_system_table_ids;
    const tree_changes = try alloc.alloc(_manifest.TreeChange, ids.len);
    for (ids, tree_changes) |table_id, *tc|
        tc.* = _manifest.createTreeChange(
            makeTreeId(table_id, 0),
            namespace_id,
            "",
            "",
        );
    // table_id 0 carries the namespace's name.
    tree_changes[0].name = @constCast(name);

    return .{
        .tree_changes = tree_changes,
        .sstable_changes = &.{},
        .cmd = .create_ns,
        .memtable_id = 0,
    };
}

/// Result of buildCreateNamespace: the allocated namespace id plus the
/// ns_create ManifestChange carrying the database name.
pub const CreateNamespaceResult = struct {
    namespace: u64,
    change: _manifest.ManifestChange,
};

/// buildCreateNamespace is the allocating namespace create (SQL: CREATE
/// DATABASE `name`): reserve the next namespace id and build its ns_create
/// change. applyNamespaceCreate advances the same counter and records the
/// name on every node, so a re-elected leader never re-issues a namespace
/// and name → namespace resolution is manifest state everywhere. Caller
/// owns `change.tree_changes`.
pub fn buildCreateNamespace(
    self: *Forest,
    alloc: std.mem.Allocator,
    name: String,
) !CreateNamespaceResult {
    const namespace = self.ms.nextNamespace();
    return .{
        .namespace = namespace,
        .change = try self.buildNamespaceCreate(alloc, namespace, name),
    };
}

/// Resolve a database name to its namespace id (manifest state, current
/// on every node via the apply path).
pub fn lookupNamespace(self: *Forest, name: String) ?u64 {
    return self.ms.lookupNamespace(name);
}

/// Resolve a table name to its table_id within a namespace (manifest
/// state, current on every node via the apply path).
pub fn lookupTable(self: *Forest, namespace_id: u64, name: String) ?u32 {
    return self.ms.lookupTable(namespace_id, name);
}

/// applyNamespaceCreate is Phase B: commits each TreeChange to the manifest,
/// installs the in-memory Tree + namespace route, records the namespace's
/// name, and advances the global namespace counter (idempotent — keeps a
/// re-elected leader current). Called on all nodes after Raft commit
/// (create_ns).
///
/// Skips any TreeChange whose tree already exists (re-apply).
pub fn applyNamespaceCreate(self: *Forest, mc: _manifest.ManifestChange) !void {
    for (mc.tree_changes) |tc| {
        if (tc.op != .create) continue; // only CREATE for ns_create
        self.ms.advanceNamespace(tc.namespace_id);
        try self.ms.registerNamespaceName(tc.namespace_id, tc.name);
        // Per-tree, not per-namespace: an ns_create carries the default
        // AND catalog trees — checking only table 0 would skip the rest.
        if (self.hasTree(tc.namespace_id, tc.tree_id)) {
            std.log.warn(
                "applyNamespaceCreate: ns={d} tree_id={d} already exists, " ++
                    "skipping",
                .{ tc.namespace_id, tc.tree_id },
            );
            continue;
        }
        try self.ms.commit(&.{}, &.{tc});
        // Empty left/right encodes the infinite range; each tree lands in
        // its table derived from the tree_id (0 = raw-KV, 1 = catalog).
        try self.installTree(tc.namespace_id, tc.tree_id);
    }
}

/// buildCreateTree is Phase A of a per-table tree create: derive the tree_id
/// for a NEW table (Key.table_id) in an existing namespace — a fresh
/// table starts at split 0, so tree_id = (table_id << 32). The tree is
/// infinite-range *within* its table — routing isolates by
/// (namespace, table_id), so no key-range bound is needed. No state mutates, so
/// a failed propose costs nothing. create_tree. Caller owns the returned slice.
pub fn buildCreateTree(
    self: *Forest,
    alloc: std.mem.Allocator,
    namespace_id: u64,
    table_id: u32,
) !_manifest.ManifestChange {
    const tree_id = self.nextTreeId(namespace_id, table_id);
    const tree_changes = try alloc.alloc(_manifest.TreeChange, 1);
    tree_changes[0] = _manifest.createTreeChange(tree_id, namespace_id, "", "");
    return .{
        .tree_changes = tree_changes,
        .sstable_changes = &.{},
        .cmd = .create_tree,
        .memtable_id = 0,
    };
}

/// Result of buildCreateTables: one allocated table_id per requested name
/// (same order) plus the single create_tree ManifestChange creating them
/// all. Caller owns `table_ids` and `change.tree_changes`.
pub const CreateTablesResult = struct {
    table_ids: []u32,
    change: _manifest.ManifestChange,
};

/// buildCreateTables creates N sibling tables in ONE change — a
/// SQL table plus its index tables, so they commit atomically. Each
/// name gets its own freshly allocated table_id (at split 0) and a create
/// TreeChange carrying that name; apply registers every name → table_id on
/// all nodes and advances the id counter past each. A non-empty name
/// already taken in the namespace errors `NameExists` (sound while the
/// caller serializes DDL); an empty name registers nothing (system trees).
/// `names` must be pairwise distinct — an intra-batch duplicate is the
/// caller's bug (the second would install unnamed). Caller owns both
/// returned slices; each name must outlive the change (marshalled at
/// propose).
pub fn buildCreateTables(
    self: *Forest,
    alloc: std.mem.Allocator,
    namespace_id: u64,
    names: []const String,
) !CreateTablesResult {
    for (names) |name|
        if (name.len > 0 and self.ms.lookupTable(namespace_id, name) != null)
            return error.NameExists;

    const table_ids = try alloc.alloc(u32, names.len);
    errdefer alloc.free(table_ids);
    const tree_changes = try alloc.alloc(_manifest.TreeChange, names.len);
    errdefer alloc.free(tree_changes);

    for (names, 0..) |name, i| {
        const table_id = try self.ms.nextTableId(namespace_id);
        table_ids[i] = table_id;
        const tree_id = self.nextTreeId(namespace_id, table_id);
        tree_changes[i] = _manifest.createTreeChange(
            tree_id,
            namespace_id,
            "",
            "",
        );
        tree_changes[i].name = @constCast(name);
    }
    return .{
        .table_ids = table_ids,
        .change = .{
            .tree_changes = tree_changes,
            .sstable_changes = &.{},
            .cmd = .create_tree,
            .memtable_id = 0,
        },
    };
}

/// applyCreateTree is Phase B: commit each TreeChange and install an empty
/// infinite-range tree in its (namespace, table_id) table, registering the
/// namespace/table if absent. Also advances the namespace's table_id
/// counter past the created table (idempotent; this is what keeps a
/// re-elected leader's allocator current). Idempotent on (namespace,
/// tree_id). Called on all nodes after Raft commit (create_tree).
pub fn applyCreateTree(self: *Forest, mc: _manifest.ManifestChange) !void {
    for (mc.tree_changes) |tc| {
        if (tc.op != .create) continue;
        try self.ms.advanceTableId(tc.namespace_id, treeTableId(tc.tree_id));
        // Before the idempotency skip, like advanceTableId: a re-apply
        // (WAL replay over snapshot-restored state) re-registers as a
        // no-op.
        try self.ms.registerTableName(
            tc.namespace_id,
            treeTableId(tc.tree_id),
            tc.name,
        );
        // already applied
        if (self.hasTree(tc.namespace_id, tc.tree_id)) continue;
        try self.ms.commit(&.{}, &.{tc});
        try self.installTree(tc.namespace_id, tc.tree_id);
    }
}

/// buildDropTables is Phase A of DROP TABLE: emit a delete change for
/// every tree of every given table_id — a table can span >1 tree after
/// splits, and a SQL table drops alongside its index tables in one atomic
/// change. table_id counters are NOT rewound (ids are never reused, since
/// a dropped SST may still be clone-shared). drop_tree. Returns an empty
/// change set if none of the tables have trees. Caller owns the slice.
///
/// The dropped tables' rows in the namespace's CATALOG trees are NOT
/// touched — with their names gone and table_ids never reused they are
/// unreachable garbage. TODO: issue tombstones for them.
pub fn buildDropTables(
    self: *Forest,
    alloc: std.mem.Allocator,
    namespace_id: u64,
    table_ids: []const u32,
) !_manifest.ManifestChange {
    var ids: std.ArrayList(u64) = .empty;
    defer ids.deinit(alloc);
    for (table_ids) |table_id|
        if (self.getTable(namespace_id, table_id)) |trees| {
            const held = trees.mu.hold(.shared, @src());
            defer held.release();
            for (trees.list(held)) |e| try ids.append(alloc, e.tree_id);
        };

    const tree_changes = try alloc.alloc(_manifest.TreeChange, ids.items.len);
    for (ids.items, 0..) |tree_id, i| {
        tree_changes[i] = _manifest.deleteTreeChange(tree_id, namespace_id);
    }
    return .{
        .tree_changes = tree_changes,
        .sstable_changes = &.{},
        .cmd = .drop_tree,
        .memtable_id = 0,
    };
}

/// applyDropTree is Phase B of DROP: for each delete change, remove the route
/// entry (dropping the table once empty), tear down the in-memory tree
/// (Tree.close releases its level refs on its SSTs — SSTs shared with another
/// tree survive via refcount), and remove the tree from the manifest.
/// Idempotent. drop_tree.
///
/// NOTE: dropped SST *files* are not eagerly unlinked from disk (Tree.close
/// detaches, and the table cache holds its own ref). The data is immediately
/// unqueryable (route + manifest gone); physical reclaim of orphaned SSTs is
/// left to snapshot GC — a follow-up.
pub fn applyDropTree(self: *Forest, mc: _manifest.ManifestChange) !void {
    try self.applyTreeDeletes(mc);
    // Forget each dropped table's name (first delete of a multi-tree
    // table removes it; the rest are no-ops). drop_ns doesn't
    // come here — dropNamespace frees the whole map.
    for (mc.tree_changes) |tc| {
        if (tc.op != .delete) continue;
        self.ms.dropTable(tc.namespace_id, treeTableId(tc.tree_id));
    }
}

/// The delete-changes walk shared by applyDropTree and applyDropNamespace.
/// Runs on the apply loop, like every other create and drop.
pub fn applyTreeDeletes(self: *Forest, mc: _manifest.ManifestChange) !void {
    for (mc.tree_changes) |tc| {
        if (tc.op != .delete) continue;

        // Remove the tree from its table (derived from the tree_id's
        // high 32 bits) and tear it down (releases its level refs on its
        // SSTs). Drop the table once it has no trees left.
        // The tree leaves the list under the table lock, so no reader can
        // find it from here on; dropping the list's ref then closes it
        // as soon as the readers that already hold it let go. The
        // TableTrees itself stays in the map, empty: table ids are never
        // reused, and a reader may still be holding its pointer.
        if (self.getTable(tc.namespace_id, treeTableId(tc.tree_id))) |trees| {
            const gone = blk: {
                const held = trees.mu.hold(.exclusive, @src());
                defer held.release();
                break :blk trees.removeTree(held, tc.tree_id);
            };
            if (gone) |tree| tree.release();
        }

        // Remove from the manifest (frees the TreeState + its table set).
        try self.ms.commit(&.{}, &.{tc});
    }
}

/// buildDropNamespace is Phase A of DROP DATABASE: emit a delete change for
/// every tree in every table of the namespace — the default table, system
/// tables (catalog etc.), and user tables alike. drop_ns. Namespace ids are
/// never reused, so no counter rewind is needed; applyDropNamespace forgets
/// the counter entirely. Returns an empty change set for an unknown
/// namespace (idempotent). Caller owns the returned slice.
pub fn buildDropNamespace(
    self: *Forest,
    alloc: std.mem.Allocator,
    namespace_id: u64,
) !_manifest.ManifestChange {
    var changes: std.ArrayList(_manifest.TreeChange) = .empty;
    defer changes.deinit(alloc);
    {
        var it = self.tables.iterator();
        defer it.deinit();
        while (it.next()) |kv| {
            if (kv.key_ptr.namespace != namespace_id) continue;
            const trees = kv.value_ptr.*;
            const held = trees.mu.hold(.shared, @src());
            defer held.release();
            for (trees.list(held)) |e| {
                try changes.append(
                    alloc,
                    _manifest.deleteTreeChange(e.tree_id, namespace_id),
                );
            }
        }
    }
    return .{
        .tree_changes = try changes.toOwnedSlice(alloc),
        .sstable_changes = &.{},
        .cmd = .drop_ns,
        .memtable_id = 0,
    };
}

/// applyDropNamespace is Phase B of DROP DATABASE: tear down every tree of
/// the namespace (same per-tree semantics as applyDropTree — SSTs shared via
/// refcount survive; physical reclaim is snapshot GC's job), then forget the
/// namespace's manifest state (table_id counter + name, freeing the name
/// for re-CREATE). Leftover memtable entries for the namespace are dropped
/// at flush (buildFlush skips items with no route). Idempotent. drop_ns.
pub fn applyDropNamespace(self: *Forest, mc: _manifest.ManifestChange) !void {
    try self.applyTreeDeletes(mc);
    if (mc.tree_changes.len > 0) {
        self.ms.dropNamespace(mc.tree_changes[0].namespace_id);
    }
}

/// buildSplit is Phase A: finds the split key, classifies tables, and returns
/// a ManifestChange with the table and tree changes. All allocations go on
/// the provided allocator (typically an arena). The caller proposes the
/// ManifestChange through Raft. The new half stays in the parent's table
/// table, so its tree_id is the table's next free split index.
pub fn buildSplit(
    self: *Forest,
    alloc: std.mem.Allocator,
    namespace_id: u64,
    tree_id: u64,
) !?_manifest.ManifestChange {
    std.debug.assert(namespace_id > 0);
    const left_tree = self.getTree(namespace_id, tree_id) orelse return null;
    defer left_tree.release();

    // Find split key from the lowest level in old tree.
    const split_key = try left_tree.findSplitKey(alloc);
    if (split_key.len() == 0) {
        return null;
    }

    // The old tree's bounds, copied out under the manifest lock: this
    // runs on the split-propose fiber while the apply loop may commit
    // (rehashing the map, freeing a dropped tree's keys).
    const old_range = try self.ms.treeRange(alloc, .{
        .namespace = namespace_id,
        .tree_id = tree_id,
    }) orelse return null;
    const new_tree_id = self.nextTreeId(namespace_id, treeTableId(tree_id));

    // Classify tables per level and build manifest changes.
    var table_changes: std.ArrayList(_manifest.SSTableChange) = .empty;

    const held = left_tree.mu.hold(.shared, @src());
    defer held.release();
    for (left_tree.levels, 0..) |*llv, llv_idx| {
        for (llv.entries(held)) |tbe| {
            if (tbe.table.smallest.order(split_key) != .lt) { // .eq or .gt
                // Right-only: remove from old tree, add to new tree.
                try table_changes.append(alloc, _manifest.deleteSSTableChange(
                    tbe.id,
                    namespace_id,
                    tree_id,
                    @intCast(llv_idx),
                ));
                try table_changes.append(alloc, _manifest.createSSTableChange(
                    tbe.id,
                    namespace_id,
                    new_tree_id,
                    @intCast(llv_idx),
                ));
            } else if (tbe.table.biggest.order(
                split_key,
            ) != .lt) { // .eq or .gt
                // order: smallest key < split key <= biggest key
                // Therefore, the split key falls within the table.
                // Shared: keep in old tree, also add to new tree.
                try table_changes.append(alloc, _manifest.createSSTableChange(
                    tbe.id,
                    namespace_id,
                    new_tree_id,
                    @intCast(llv_idx),
                ));
            }
            // Left-only: no manifest change needed.
        }
    }

    const left_left_raw_key = try old_range.left.marshal(alloc);
    const right_right_raw_key = try old_range.right.marshal(alloc);
    const split_raw = try split_key.marshal(alloc);

    const tree_changes = try alloc.alloc(_manifest.TreeChange, 2);
    tree_changes[0] = _manifest.updateTreeChange(
        tree_id,
        namespace_id,
        left_left_raw_key,
        split_raw,
    );
    tree_changes[1] = _manifest.createTreeChange(
        new_tree_id,
        namespace_id,
        split_raw,
        right_right_raw_key,
    );

    return .{
        .sstable_changes = try table_changes.toOwnedSlice(alloc),
        .tree_changes = tree_changes,
        .cmd = .tree_split,
    };
}

/// applySplit is Phase B: commits the manifest changes, opens FRESH tree
/// objects for both halves from the post-commit manifest — the left
/// keeps its id with the narrowed range and only its tables, the right
/// is new — swaps them into the table's list, and retires the old
/// object. The old one is never mutated: whoever still holds a ref on
/// it (a reader mid-selection, a proposer) keeps seeing its full range
/// and full table set until they let go, which is what makes a split
/// safe against readers without any of them taking a lock across their
/// use of a tree. Called on all nodes after Raft commit. The new tree_id
/// is derived deterministically (table's next split index), so there is
/// no counter for a new leader to re-issue.
pub fn applySplit(self: *Forest, mc: _manifest.ManifestChange) !void {
    // Commit MANIFEST so we can recover from a crash.
    try self.ms.commit(mc.sstable_changes, mc.tree_changes);

    // Find the old tree_id (UPDATE) and new tree_id (CREATE) from tree_changes.
    // Both changes carry the same namespace (a split stays within one).
    var old_tree_id: u64 = 0;
    var new_tree_id: u64 = 0;
    var namespace_id: u64 = 0;
    for (mc.tree_changes) |tc| {
        if (tc.op == .update) { // UPDATE
            old_tree_id = tc.tree_id;
        } else if (tc.op == .create) { // CREATE
            new_tree_id = tc.tree_id;
        }
        namespace_id = tc.namespace_id;
    }

    // Both halves are opened from the post-commit manifest while the old
    // object still lists every table, so loadTables finds the shared
    // ones in the cache with ref > 0. Their key ranges come from the
    // manifest too: the left's right edge is the split key.
    // Refs start at 1, so releasing a half that never gets published
    // closes and frees it.
    const left = try Tree.open(
        self.allocator,
        self.opts,
        namespace_id,
        old_tree_id,
        &self.ms,
        &self.table_cache,
    );
    errdefer left.release();
    const right = try Tree.open(
        self.allocator,
        self.opts,
        namespace_id,
        new_tree_id,
        &self.ms,
        &self.table_cache,
    );
    errdefer right.release();

    // Fresh candidates against the post-split state. Without this the
    // left half would carry the old tree's `{needs_split=true}` verdict
    // even though half its tables just moved out, and the next
    // levels-loop iteration would fire another split.
    try left.updateCompactCandidate();
    try right.updateCompactCandidate();

    // The swap: both halves in, the old object out, under one exclusive
    // hold of the table's lock (same table — a split shares the high
    // 32 bits of the tree_id). Capacity is reserved first so the two
    // adds cannot fail between the old tree leaving and them landing.
    std.debug.assert(treeTableId(new_tree_id) == treeTableId(old_tree_id));
    const trees = self.getTable(namespace_id, treeTableId(old_tree_id)) orelse {
        left.release();
        right.release();
        return;
    };
    const old = blk: {
        const held = trees.mu.hold(.exclusive, @src());
        defer held.release();
        // gone (dropped) — nothing to split
        if (trees.byId(held.shared(), old_tree_id) == null) {
            left.release();
            right.release();
            return;
        }
        try trees.entries.ensureUnusedCapacity(trees.allocator, 2);
        const old = trees.removeTree(held, old_tree_id).?;
        trees.addTree(held, left) catch unreachable; // capacity reserved above
        trees.addTree(held, right) catch unreachable;
        break :blk old;
    };

    // The list's ref on the old object. Readers that still hold it keep
    // its full range and tables; the last of them closes it.
    old.release();
}

/// splitTreeForTests splits the given tree at the median key of its
/// bottommost level, locally, without going through Raft. **Test-only**
/// — production must propose splits through the manifest pipeline so
/// all nodes apply the same split deterministically; see
/// `proposeSplit` in the engine. Takes the locks it needs itself, like
/// applySplit on the apply path.
pub fn splitTreeForTests(self: *Forest, namespace_id: u64, tree_id: u64) !void {
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();
    const mc = try self.buildSplit(
        arena.allocator(),
        namespace_id,
        tree_id,
    ) orelse return;
    try self.applySplit(mc);
}
