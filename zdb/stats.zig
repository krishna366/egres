//! The human-readable stats dump of a Forest: a top-level summary, one
//! tree, or one namespace. Every function here takes a `*Forest` and
//! leaves it unchanged; `Forest` aliases each declaration back in, so
//! callers keep using method syntax.

const std = @import("std");
const _table = @import("table.zig");
const _tree = @import("tree.zig");

const Forest = @import("forest.zig").Forest;
const TreeKey = @import("kv.zig").TreeKey;

/// What slice of the forest the caller wants to see. Default is a
/// top-level summary that aggregates across all trees.
pub const StatsScope = union(enum) {
    forest,
    tree: TreeKey,
    namespace: u64,
};

pub const StatsOpts = struct {
    scope: StatsScope = .forest,
};

/// writeStats produces a hierarchical, human-readable text dump of
/// current LSM state to `w`. Unlike the Prometheus metrics endpoint,
/// this is for ops drill-down: per-tree shape, per-level sizes,
/// pending compactions. Snapshot is best-effort — locks are taken
/// briefly per section, so a concurrent flush/compaction may show
/// itself half-applied. Acceptable for a debug endpoint.
pub fn writeStats(self: *Forest, w: *std.Io.Writer, opts: StatsOpts) !void {
    switch (opts.scope) {
        .forest => try self.writeForestStats(w),
        .tree => |id| try self.writeTreeStats(w, id),
        .namespace => |ns| try self.writeNamespaceStats(w, ns),
    }
}

pub fn writeForestStats(self: *Forest, w: *std.Io.Writer) !void {
    const mt = self.memtables.stats();
    try w.print("forest:\n", .{});
    try w.print("  memtables: active=", .{});
    try writeBytes(w, mt.active_bytes);
    try w.print(" immutable={d} (", .{mt.immutable_count});
    try writeBytes(w, mt.immutable_bytes);
    try w.print(" pending flush)\n", .{});
    try w.print("  flush_pending: {d}\n", .{mt.immutable_count});
    if (_table.table.block_cache) |bc| {
        try w.print("  block_cache: entries={d} bytes=", .{bc.count()});
        try writeBytes(w, bc.byteSize());
        try w.print(" hits={d} misses={d}\n", .{
            bc.hits.load(.monotonic),
            bc.misses.load(.monotonic),
        });
    }

    // Per-level aggregate (sum + max-with-tree).
    const max_levels = self.opts.max_levels;
    const Agg = struct {
        tables_total: u64 = 0,
        tables_max: u64 = 0,
        tables_max_tree: u64 = 0,
        bytes_total: u64 = 0,
        bytes_max: u64 = 0,
        bytes_max_tree: u64 = 0,
    };
    const aggs = try self.allocator.alloc(Agg, max_levels);
    defer self.allocator.free(aggs);
    for (aggs) |*a| a.* = .{};

    // Per-level "compactions wanted" rollup.
    const Wanted = struct {
        count: u64 = 0,
        score_max: f64 = 0,
        score_max_tree: u64 = 0,
    };
    const wanted = try self.allocator.alloc(Wanted, max_levels);
    defer self.allocator.free(wanted);
    for (wanted) |*x| x.* = .{};

    // Largest tree (sum across levels).
    var largest_tree_id: u64 = 0;
    var largest_tree_size: i64 = 0;
    var tree_count: usize = 0;

    // Walk under the map lock, each table's lock shared while its list
    // is read, but only briefly: collect the numbers we need, release.
    // Each level access takes its own lock inside numTables/getTotalSize.
    {
        var tables_it = self.tables.iterator();
        defer tables_it.deinit();
        while (tables_it.next()) |kv| {
            const trees = kv.value_ptr.*;
            const held = trees.mu.hold(.shared, @src());
            defer held.release();
            for (trees.list(held)) |tree| {
                tree_count += 1;
                const tree_id = tree.tree_id;

                const th = tree.mu.hold(.shared, @src());
                defer th.release();
                var tree_total: i64 = 0;
                for (tree.levels, 0..) |*lv, idx| {
                    if (idx >= max_levels) break;
                    const tcount: u64 = @intCast(lv.numTables(th));
                    const tbytes: u64 = @intCast(lv.getTotalSize(th));
                    tree_total += @intCast(tbytes);

                    aggs[idx].tables_total += tcount;
                    if (tcount > aggs[idx].tables_max) {
                        aggs[idx].tables_max = tcount;
                        aggs[idx].tables_max_tree = tree_id;
                    }
                    aggs[idx].bytes_total += tbytes;
                    if (tbytes > aggs[idx].bytes_max) {
                        aggs[idx].bytes_max = tbytes;
                        aggs[idx].bytes_max_tree = tree_id;
                    }
                }
                if (tree_total > largest_tree_size) {
                    largest_tree_size = tree_total;
                    largest_tree_id = tree_id;
                }

                if (tree.compact_candidate) |c| {
                    if (c.level < max_levels) {
                        wanted[c.level].count += 1;
                        if (c.score > wanted[c.level].score_max) {
                            wanted[c.level].score_max = c.score;
                            wanted[c.level].score_max_tree = tree_id;
                        }
                    }
                }
            }
        }
    }

    try w.print("  trees: {d}\n", .{tree_count});
    try w.print("  largest_tree: id={d} size=", .{largest_tree_id});
    try writeBytes(w, @intCast(largest_tree_size));
    try w.print("\n\n", .{});

    try w.print("  level summary (across all trees):\n", .{});
    for (aggs, 0..) |a, idx| {
        // Skip levels that have nothing — shows max_levels=7 dumps cleanly.
        if (a.tables_total == 0 and a.bytes_total == 0) continue;
        try w.print("    L{d}: tables={d} (max={d} in tree {d})  bytes=", .{
            idx, a.tables_total, a.tables_max, a.tables_max_tree,
        });
        try writeBytes(w, a.bytes_total);
        try w.print(" (max=", .{});
        try writeBytes(w, a.bytes_max);
        try w.print(" in tree {d})\n", .{a.bytes_max_tree});
    }

    var any_wanted = false;
    for (wanted) |x| if (x.count > 0) {
        any_wanted = true;
        break;
    };
    if (any_wanted) {
        try w.print("\n  compactions wanted:\n", .{});
        for (wanted, 0..) |x, idx| {
            if (x.count == 0) continue;
            try w.print("    L{d}: trees={d} score_max={d:.2} (tree {d})\n", .{
                idx, x.count, x.score_max, x.score_max_tree,
            });
        }
    }
}

pub fn writeTreeStats(self: *Forest, w: *std.Io.Writer, tk: TreeKey) !void {
    const tree = self.getTree(tk.namespace, tk.tree_id) orelse {
        try w.print("tree id={d}: not found\n", .{tk.tree_id});
        return;
    };
    defer tree.release();

    // Numbers first, under the lock; printing after — the writer may be
    // a socket, and a slow client must not hold up installs into this
    // tree.
    const LevelStat = struct { tables: usize, bytes: u64 };
    const per_level = try self.allocator.alloc(LevelStat, tree.levels.len);
    defer self.allocator.free(per_level);
    const total: i64, const candidate: ?_tree.CompactCandidate = blk: {
        const held = tree.mu.hold(.shared, @src());
        defer held.release();
        for (tree.levels, per_level) |*lv, *st| {
            st.* = .{
                .tables = lv.numTables(held),
                .bytes = @intCast(lv.getTotalSize(held)),
            };
        }
        break :blk .{ tree.totalSize(held), tree.compact_candidate };
    };

    try w.print(
        "tree id={d} namespace={d} size=",
        .{ tk.tree_id, tk.namespace },
    );
    try writeBytes(w, @intCast(total));
    try w.print("\n", .{});

    for (per_level, 0..) |st, idx| {
        if (st.tables == 0 and st.bytes == 0) continue;
        try w.print("  L{d}: {d} tables, ", .{ idx, st.tables });
        try writeBytes(w, st.bytes);
        try w.print("\n", .{});
    }
    if (candidate) |c| {
        try w.print(
            "  compact_candidate: L{d} score={d:.2}\n",
            .{ c.level, c.score },
        );
    }
}

pub fn writeNamespaceStats(self: *Forest, w: *std.Io.Writer, ns_id: u64) !void {
    try w.print("namespace={d}:\n", .{ns_id});
    var any = false;
    // Tree keys come out under the manifest lock (the map they live in
    // is the apply loop's to rehash); writeTreeStats then takes each
    // tree's own lock.
    const keys = try self.ms.treeKeys(self.allocator, ns_id);
    defer self.allocator.free(keys);
    for (keys) |tk| {
        any = true;
        try writeTreeStats(self, w, tk);
    }
    if (!any) try w.print("  (no trees)\n", .{});
}

/// Pretty-print a byte count: KiB / MiB / GiB / TiB. Two decimal places
/// of precision past the smallest meaningful unit.
pub fn writeBytes(w: *std.Io.Writer, b: u64) !void {
    const kib: u64 = 1024;
    const mib: u64 = 1024 * kib;
    const gib: u64 = 1024 * mib;
    const tib: u64 = 1024 * gib;
    if (b >= tib) {
        try w.print(
            "{d:.2}TiB",
            .{@as(f64, @floatFromInt(b)) / @as(f64, @floatFromInt(tib))},
        );
    } else if (b >= gib) {
        try w.print(
            "{d:.2}GiB",
            .{@as(f64, @floatFromInt(b)) / @as(f64, @floatFromInt(gib))},
        );
    } else if (b >= mib) {
        try w.print(
            "{d:.2}MiB",
            .{@as(f64, @floatFromInt(b)) / @as(f64, @floatFromInt(mib))},
        );
    } else if (b >= kib) {
        try w.print(
            "{d:.2}KiB",
            .{@as(f64, @floatFromInt(b)) / @as(f64, @floatFromInt(kib))},
        );
    } else {
        try w.print("{d}B", .{b});
    }
}
