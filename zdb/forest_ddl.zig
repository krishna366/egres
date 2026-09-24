//! Forest's DDL methods for the namespace, table and split commands: the
//! leader-side `build*` functions, which read the manifest and the live
//! levels and return a ManifestChange for the caller to propose, and the
//! node-side `apply*` functions, which run on every node after Raft commits
//! that change — they load the files, apply the change to the manifest, and
//! update the live trees (install, close, swap) to match. Tree installation
//! and the manifest-driven recovery walks are here too. Nothing in this file
//! mutates the manifest except through `Manifest.apply` (or, on the two
//! index-0 paths with no Raft entry behind them, `Manifest.commit`).

const std = @import("std");
const utils = @import("utils");
const String = utils.String;
const _manifest = @import("manifest.zig");
const KeyRange = @import("kv.zig").KeyRange;
const TableKey = @import("kv.zig").TableKey;
const makeTreeId = @import("kv.zig").makeTreeId;
const treeTableId = @import("kv.zig").treeTableId;
const Forest = @import("forest.zig").Forest;
const Tree = @import("tree.zig").Tree;

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
        self.live,
        namespace_id,
        tree_id,
        &self.manifest,
        &self.table_cache,
    );
    errdefer tree.release();

    const tbl_trees = try self.getOrCreateTable(.{
        .namespace = namespace_id,
        .table_id = treeTableId(tree_id),
    });
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
    const listed = try self.manifest.listedFileIds(allocator);
    defer allocator.free(listed);
    var out: std.ArrayList(u64) = .empty;
    errdefer out.deinit(allocator);
    for (listed) |fid| {
        if (!self.hasFile(fid)) try out.append(allocator, fid);
    }
    return out.toOwnedSlice(allocator);
}

/// Walk `self.manifest.trees` and install each tree (Tree + TableTrees entries).
/// Used after `Manifest.restoreFrom(snapshot.data)` at recovery — the
/// manifest already names every tree and table, but the in-memory Tree
/// objects and TableTrees need to be rebuilt from it. Tree ids are
/// derived (table_id<<32 | split_id), so there is no counter to advance.
///
/// File ids need no recovery either: they are drawn at random by
/// `Manifest.newFileId`, so there is no counter or lease to restore.
pub fn openTablesFromManifest(self: *Forest) !void {
    // installTree opens each Tree (which dupes its key range from the
    // manifest), so we only need the keys here.
    const keys = try self.manifest.treeKeys(self.allocator, null);
    defer self.allocator.free(keys);
    for (keys) |tk| try self.installTree(tk.namespace, tk.tree_id);
}

/// Create a new tree for the given namespace, table, and key range.
/// Single-node path (no Raft) — used by tests. Returns the new tree_id,
/// derived from the table's next free split index.
pub fn createTree(
    self: *Forest,
    table_key: TableKey,
    range: KeyRange,
) !u64 {
    const tree_id = self.nextTreeId(table_key);

    const left_raw = try range.left.marshal(self.allocator);
    defer self.allocator.free(left_raw);
    const right_raw = try range.right.marshal(self.allocator);
    defer self.allocator.free(right_raw);

    // Single-node path: no raft entry behind this, so index 0.
    try self.manifest.commit(0, &.{}, &.{
        _manifest.createTreeChange(
            tree_id,
            table_key.namespace,
            left_raw,
            right_raw,
        ),
    });
    try self.installTree(table_key.namespace, tree_id);
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
    if (self.manifest.lookupNamespace(name)) |_| return error.NameExists;

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
    const namespace = self.manifest.nextNamespace();
    return .{
        .namespace = namespace,
        .change = try self.buildNamespaceCreate(alloc, namespace, name),
    };
}

/// applyNamespaceCreate is Phase B: applies the change to the manifest —
/// which records the namespace's name and advances the global namespace
/// counter — then installs the in-memory Tree + namespace route for each
/// created tree. Called on all nodes after Raft commit (create_ns).
///
/// Skips installing any tree the forest already holds (re-apply).
pub fn applyNamespaceCreate(
    self: *Forest,
    index: u64,
    mc: _manifest.ManifestChange,
) !void {
    try self.manifest.apply(index, mc);
    for (mc.tree_changes) |tc| {
        if (tc.op != .create) continue; // only CREATE for ns_create
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
    table_key: TableKey,
) !_manifest.ManifestChange {
    const tree_id = self.nextTreeId(table_key);
    const tree_changes = try alloc.alloc(_manifest.TreeChange, 1);
    tree_changes[0] = _manifest.createTreeChange(
        tree_id,
        table_key.namespace,
        "",
        "",
    );
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
    /// One per name: what that tree's keys are (`buffers.KeyKind`). A SQL
    /// base table's rows are keyed by egid; everything else is bytes.
    kinds: []const _manifest.KeyKind,
) !CreateTablesResult {
    std.debug.assert(kinds.len == names.len);
    for (names) |name|
        if (name.len > 0 and self.manifest.lookupTable(namespace_id, name) != null)
            return error.NameExists;

    const table_ids = try alloc.alloc(u32, names.len);
    errdefer alloc.free(table_ids);
    const tree_changes = try alloc.alloc(_manifest.TreeChange, names.len);
    errdefer alloc.free(tree_changes);

    for (names, 0..) |name, i| {
        const table_id = try self.manifest.nextTableId(namespace_id);
        table_ids[i] = table_id;
        const tree_id = self.nextTreeId(.{
            .namespace = namespace_id,
            .table_id = table_id,
        });
        tree_changes[i] = _manifest.createTreeChange(
            tree_id,
            namespace_id,
            "",
            "",
        );
        tree_changes[i].name = @constCast(name);
        tree_changes[i].key_kind = kinds[i];
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

/// applyCreateTree is Phase B: apply the change to the manifest — which
/// registers each table's name and advances the namespace's table_id
/// counter past it (this is what keeps a re-elected leader's allocator
/// current) — then install an empty infinite-range tree in its
/// (namespace, table_id) table, registering the namespace/table if absent.
/// Idempotent on (namespace, tree_id). Called on all nodes after Raft
/// commit (create_tree).
pub fn applyCreateTree(
    self: *Forest,
    index: u64,
    mc: _manifest.ManifestChange,
) !void {
    try self.manifest.apply(index, mc);
    for (mc.tree_changes) |tc| {
        if (tc.op != .create) continue;
        // already applied
        if (self.hasTree(tc.namespace_id, tc.tree_id)) continue;
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
        if (self.getTable(.{
            .namespace = namespace_id,
            .table_id = table_id,
        })) |trees| {
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
/// unqueryable (route + manifest gone); the manifest apply below retires
/// every file whose last listing the drop removes, and `Forest.deleteFiles`
/// unlinks it once this node's snapshot index passes `index`.
pub fn applyDropTree(
    self: *Forest,
    index: u64,
    mc: _manifest.ManifestChange,
) !void {
    // The manifest apply removes each tree's record (and, for drop_tree,
    // each dropped table's name); the walk below only tears down the live
    // trees, reading nothing the freed records owned.
    try self.manifest.apply(index, mc);
    applyTreeDeletes(self, mc);
}

/// The live-tree teardown shared by applyDropTree and applyNamespaceDrop.
/// Runs on the apply loop, like every other create and drop, and after the
/// manifest apply that removed the same trees' records.
fn applyTreeDeletes(self: *Forest, mc: _manifest.ManifestChange) void {
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
        if (self.getTable(.{
            .namespace = tc.namespace_id,
            .table_id = treeTableId(tc.tree_id),
        })) |trees| {
            const gone = blk: {
                const held = trees.mu.hold(.exclusive, @src());
                defer held.release();
                break :blk trees.removeTree(held, tc.tree_id);
            };
            if (gone) |tree| tree.release();
        }
    }
}

/// buildDropNamespace is Phase A of DROP DATABASE: emit a delete change for
/// every tree in every table of the namespace — the default table, system
/// tables (catalog etc.), and user tables alike. drop_ns. Namespace ids are
/// never reused, so no counter rewind is needed; applyNamespaceDrop forgets
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

/// applyNamespaceDrop is Phase B of DROP DATABASE: tear down every tree of
/// the namespace (same per-tree semantics as applyDropTree — SSTs shared via
/// refcount survive; the rest are retired here and unlinked by
/// `Forest.deleteFiles`), then forget the
/// namespace's manifest state (table_id counter + name, freeing the name
/// for re-CREATE). Leftover memtable entries for the namespace are dropped
/// at flush (buildFlush skips items with no route). Idempotent. drop_ns.
pub fn applyNamespaceDrop(
    self: *Forest,
    index: u64,
    mc: _manifest.ManifestChange,
) !void {
    // The manifest apply removes every tree record and forgets the
    // namespace's counter and names; the walk below tears down the live
    // trees.
    try self.manifest.apply(index, mc);
    applyTreeDeletes(self, mc);
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
    const split_key = (try left_tree.findSplitKey(alloc)) orelse return null;

    // The old tree's bounds, copied out under the manifest lock: this
    // runs on the split-propose fiber while the apply loop may commit
    // (rehashing the map, freeing a dropped tree's keys).
    const old_range = try self.manifest.treeRange(alloc, .{
        .namespace = namespace_id,
        .tree_id = tree_id,
    }) orelse return null;
    const old_kind = self.manifest.treeKeyKind(.{
        .namespace = namespace_id,
        .tree_id = tree_id,
    }) orelse return null;
    const new_tree_id = self.nextTreeId(.{
        .namespace = namespace_id,
        .table_id = treeTableId(tree_id),
    });

    // Classify tables per level and build manifest changes.
    var table_changes: std.ArrayList(_manifest.SSTableChange) = .empty;

    const held = left_tree.mu.hold(.shared, @src());
    defer held.release();
    for (left_tree.levels, 0..) |*llv, llv_idx| {
        for (llv.entries(held)) |tbe| {
            if (tbe.table.smallest.orderPrimary(split_key) != .lt) { // .eq or .gt
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
            } else if (tbe.table.biggest.orderPrimary(
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
    tree_changes[1].key_kind = old_kind;

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
pub fn applySplit(
    self: *Forest,
    index: u64,
    mc: _manifest.ManifestChange,
) !void {
    // Commit MANIFEST so we can recover from a crash.
    try self.manifest.commit(index, mc.sstable_changes, mc.tree_changes);

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
        self.live,
        namespace_id,
        old_tree_id,
        &self.manifest,
        &self.table_cache,
    );
    errdefer left.release();
    const right = try Tree.open(
        self.allocator,
        self.live,
        namespace_id,
        new_tree_id,
        &self.manifest,
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
    const trees = self.getTable(.{
        .namespace = namespace_id,
        .table_id = treeTableId(old_tree_id),
    }) orelse {
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
    // Test-only path: no raft entry behind it, so index 0.
    try self.applySplit(0, mc);
}
