const std = @import("std");
const manifest = @import("manifest.zig");
const forest_mod = @import("forest.zig");
const Forest = forest_mod.Forest;
const Item = @import("key.zig").Item;

// ---------------------------------------------------------------------------
// StorageApplier
// ---------------------------------------------------------------------------

/// StorageApplier bridges Raft committed entries to Forest operations.
/// It handles two kinds of entries:
///   - Mutations (cmd=0): applied immediately to the current memtable.
///   - Structural changes (cmd=1 flush, cmd=2 compaction, cmd=3 split):
///     queued until all referenced files are locally available, then
///     finalized in order.
///
/// Two progress cursors:
///   - last_applied: advances for every entry (mutations are instant).
///   - last_structural_applied: tracks which structural changes have been
///     fully finalized (files fetched + manifest updated).
pub const StorageApplier = struct {
    forest: *Forest,
    allocator: std.mem.Allocator,
    last_applied: u64,
    last_structural_applied: u64,
    pending: std.ArrayList(PendingChange),
    mu: std.Thread.Mutex,

    pub const PendingChange = struct {
        index: u64,
        mc: manifest.ManifestChange,
        ready: bool, // true when all referenced files are locally available
    };

    pub fn init(allocator: std.mem.Allocator, forest: *Forest) StorageApplier {
        return .{
            .forest = forest,
            .allocator = allocator,
            .last_applied = 0,
            .last_structural_applied = 0,
            .pending = .{},
            .mu = .{},
        };
    }

    pub fn deinit(self: *StorageApplier) void {
        for (self.pending.items) |*pc| {
            pc.mc.deinit(self.allocator);
        }
        self.pending.deinit(self.allocator);
    }

    /// Apply a batch of key-value mutations to the current memtable.
    /// Returns true if the memtable should be frozen (size threshold exceeded).
    pub fn applyMutations(self: *StorageApplier, index: u64, items: []const Item) !bool {
        self.mu.lock();
        defer self.mu.unlock();

        const should_freeze = try self.forest.put(items);
        self.last_applied = index;
        return should_freeze;
    }

    /// Apply a memtable freeze. Moves the current memtable to immutables.
    pub fn applyFreeze(self: *StorageApplier, index: u64) !void {
        self.mu.lock();
        defer self.mu.unlock();

        try self.forest.freezeMemtable();
        self.last_applied = index;
    }

    /// Queue a structural change (flush, compaction, or split) for deferred
    /// application. On the leader, files are already local so ready=true.
    /// Followers set ready=false and mark ready after file fetch completes.
    pub fn queueStructural(self: *StorageApplier, index: u64, mc: manifest.ManifestChange, ready: bool) !void {
        self.mu.lock();
        defer self.mu.unlock();

        try self.pending.append(self.allocator, .{
            .index = index,
            .mc = mc,
            .ready = ready,
        });
        self.last_applied = index;

        self.finalizePendingLocked();
    }

    /// Mark a pending structural change as ready (files fetched).
    /// Called by the file fetch subsystem when all files for the given
    /// index are locally available.
    pub fn markReady(self: *StorageApplier, index: u64) void {
        self.mu.lock();
        defer self.mu.unlock();

        for (self.pending.items) |*pc| {
            if (pc.index == index) {
                pc.ready = true;
                break;
            }
        }
        self.finalizePendingLocked();
    }

    /// Finalize pending structural changes in order. Each change is applied
    /// only when all prior changes have been applied and the change is ready.
    /// Caller must hold self.mu.
    fn finalizePendingLocked(self: *StorageApplier) void {
        while (self.pending.items.len > 0) {
            const pc = &self.pending.items[0];
            if (!pc.ready) break;

            self.applyStructural(pc) catch break;
            self.last_structural_applied = pc.index;
            pc.mc.deinit(self.allocator);
            _ = self.pending.orderedRemove(0);
        }
    }

    fn applyStructural(self: *StorageApplier, pc: *const PendingChange) !void {
        switch (pc.mc.cmd) {
            1 => { // flush
                try self.forest.applyFlush(pc.mc.table_changes, pc.mc.memtable_id);
            },
            2 => { // compaction
                // Route to the correct tree. All table_changes in a compaction
                // belong to a single tree — use the first one's tree_id.
                if (pc.mc.table_changes.len > 0) {
                    const tree_id = pc.mc.table_changes[0].tree_id;
                    if (self.forest.trees.get(tree_id)) |tree| {
                        try tree.applyCompaction(pc.mc.table_changes);
                    }
                }
            },
            3 => { // tree_split
                try self.forest.applySplit(.{
                    .table_changes = pc.mc.table_changes,
                    .tree_changes = pc.mc.tree_changes,
                    .cmd = pc.mc.cmd,
                    .memtable_id = pc.mc.memtable_id,
                });
            },
            else => {},
        }
    }

    /// Returns the list of file IDs referenced by pending structural changes
    /// that are not yet ready. Used by the file fetch subsystem to know what
    /// to fetch.
    pub fn pendingFileIds(self: *StorageApplier, alloc: std.mem.Allocator) !std.ArrayList(u64) {
        self.mu.lock();
        defer self.mu.unlock();

        var ids: std.ArrayList(u64) = .{};
        for (self.pending.items) |pc| {
            if (pc.ready) continue;
            for (pc.mc.table_changes) |tc| {
                if (tc.op == 0) { // CREATE — file needs to be fetched
                    try ids.append(alloc, tc.table_id);
                }
            }
        }
        return ids;
    }
};
