const std = @import("std");
const table = @import("table.zig");
const Key = @import("../kv.zig").Key;
const skl = @import("../skl/skiplist.zig");
const utils = @import("utils");

pub const Error = table.Error;

/// Iterator is a tagged union that lets MergeIterator accept a TableIterator
/// (single SST), ConcatIterator (multiple non-overlapping SSTs), or a
/// memtable scan (`skl.Iterator`). All three expose the same
/// `seekRange` / `itemRef` / `next` / `deinit` shape.
pub const Iterator = union(enum) {
    table: table.TableIterator,
    concat: ConcatIterator,
    skiplist: skl.Iterator,

    pub fn seekRange(self: *Iterator, start: Key, end: Key) !void {
        switch (self.*) {
            inline else => |*it| try it.seekRange(start, end),
        }
    }

    pub fn itemRef(self: *const Iterator) ?table.Item {
        return switch (self.*) {
            inline else => |*it| it.itemRef(),
        };
    }

    pub fn next(self: *Iterator) Error!void {
        switch (self.*) {
            inline else => |*it| try it.next(),
        }
    }

    pub fn deinit(self: *Iterator) void {
        switch (self.*) {
            inline else => |*it| it.deinit(),
        }
    }
};

/// Heap slot: a source index. A merge has one source per memtable plus
/// one per overlapping L0 table plus one per deeper level, so u16 is
/// three orders of magnitude of headroom; seekRange asserts the bound.
const Src = u16;

pub const MergeIterator = struct {
    allocator: std.mem.Allocator,
    iters: []Iterator, // caller-owned slice
    /// Each source's current item, indexed BY SOURCE and refreshed once
    /// per advance. Comparisons read keys from here instead of
    /// dispatching through the Iterator union and re-parsing the bytes
    /// every time. An item stays valid exactly until its own source
    /// advances (its key points into that source's key_buf or arena, its
    /// value into a block the source holds a ref on) — which is when we
    /// overwrite it.
    items: []table.Item = &.{},
    /// Min-heap (by winsOver) of the sources that still hold an item.
    /// Source indices rather than the items themselves: sift swaps stay
    /// 2 bytes wide and the live heap sits in a single cache line.
    heap: []Src = &.{},
    heap_len: usize = 0,

    pub fn init(allocator: std.mem.Allocator, iters: []Iterator) MergeIterator {
        return .{ .allocator = allocator, .iters = iters };
    }

    pub fn deinit(self: *MergeIterator) void {
        if (self.items.len > 0) self.allocator.free(self.items);
        if (self.heap.len > 0) self.allocator.free(self.heap);
        for (self.iters) |*it| it.deinit();
    }

    /// True when source `a` should reach the caller before source `b`.
    ///
    /// Every source in one merge shares a single (namespace, table_id):
    /// scans run on table-limited ranges (KeyRange.limitToTable) and a
    /// compaction merge never leaves its tree. Those fields are equal by
    /// construction, so orderPrimary skips them; the asserts catch a
    /// future caller that breaks the assumption.
    ///
    /// Equal full keys fall back to source index for a deterministic
    /// order; they are NOT collapsed. A key present in two sources
    /// surfaces once per source — compaction's keys_in accounting reads
    /// a missing one as iterator loss.
    fn winsOver(self: *const MergeIterator, a: Src, b: Src) bool {
        const ka = self.items[a].key;
        const kb = self.items[b].key;
        std.debug.assert(ka.namespace == kb.namespace);
        std.debug.assert(ka.table_id == kb.table_id);
        return switch (ka.orderPrimary(kb)) {
            .lt => true,
            .gt => false,
            .eq => a < b,
        };
    }

    pub fn seekRange(self: *MergeIterator, start: Key, end: Key) !void {
        std.debug.assert(self.iters.len <= std.math.maxInt(Src));
        if (self.heap.len == 0 and self.iters.len > 0) {
            self.items = try self.allocator.alloc(table.Item, self.iters.len);
            self.heap = try self.allocator.alloc(Src, self.iters.len);
        }
        self.heap_len = 0;
        for (self.iters, 0..) |*it, i| {
            try it.seekRange(start, end);
            self.items[i] = it.itemRef() orelse continue;
            self.heap[self.heap_len] = @intCast(i);
            self.heap_len += 1;
        }
        // Floyd heapify: O(n), against O(n log n) for n separate pushes.
        var i = self.heap_len / 2;
        while (i > 0) {
            i -= 1;
            self.siftDown(i);
        }
    }

    pub fn itemRef(self: *MergeIterator) ?table.Item {
        if (self.heap_len == 0) return null;
        return self.items[self.heap[0]];
    }

    pub fn next(self: *MergeIterator) Error!void {
        if (self.heap_len == 0) return;
        const src = self.heap[0];
        try self.iters[src].next();
        if (self.iters[src].itemRef()) |item| {
            // Replace-top rather than pop + push: the source that just
            // advanced often still wins (runs from one source are the
            // common case), so one sift replaces two.
            self.items[src] = item;
        } else {
            self.heap_len -= 1;
            if (self.heap_len == 0) return;
            self.heap[0] = self.heap[self.heap_len];
        }
        self.siftDown(0);
    }

    fn siftDown(self: *MergeIterator, start: usize) void {
        var i = start;
        while (true) {
            const l = 2 * i + 1;
            if (l >= self.heap_len) return; // also the single-source exit
            var c = l;
            const r = l + 1;
            if (r < self.heap_len and
                self.winsOver(self.heap[r], self.heap[l])) c = r;
            if (!self.winsOver(self.heap[c], self.heap[i])) return;
            std.mem.swap(Src, &self.heap[c], &self.heap[i]);
            i = c;
        }
    }
};

pub const ConcatIterator = struct {
    iters: []table.TableIterator, // caller-owned slice; sorted by smallest key
    current: usize, // == iters.len when exhausted
    end: Key = .{}, // upper bound carried over from `seekRange` for lazy seeking

    pub fn init(iters: []table.TableIterator) ConcatIterator {
        return .{ .iters = iters, .current = 0 };
    }

    pub fn deinit(self: *ConcatIterator) void {
        for (self.iters) |*it| it.deinit();
    }

    /// Seek the FIRST relevant iter only — subsequent iters get seeked
    /// lazily by `next()` as it advances `current`. Avoids paying the
    /// per-table `setBlock` + decompress cost for tables we may never
    /// reach (e.g., when the merge upstream stops early).
    pub fn seekRange(self: *ConcatIterator, start: Key, end: Key) !void {
        self.end = end;
        if (self.iters.len == 0) return;

        if (start.len() == 0) {
            self.current = 0;
        } else {
            // First table whose biggest key >= start.
            self.current = utils.search(table.TableIterator, self.iters, start, struct {
                fn f(sk: Key, it: table.TableIterator) bool {
                    return it.table.biggest.order(sk) != .lt;
                }
            }.f);
            if (self.current >= self.iters.len) return;
        }
        try self.iters[self.current].seekRange(start, end);
    }

    pub fn itemRef(self: *const ConcatIterator) ?table.Item {
        if (self.current >= self.iters.len) return null;
        return self.iters[self.current].itemRef();
    }

    pub fn next(self: *ConcatIterator) Error!void {
        if (self.current >= self.iters.len) return;
        try self.iters[self.current].next();
        // When the current iter exhausts, advance and lazy-seek the next.
        // The handoff key is empty: every subsequent iter is by
        // definition entirely above the current iter's range, so there's
        // no lower bound to apply — only the upper bound from `end`.
        while (self.iters[self.current].itemRef() == null) {
            self.current += 1;
            if (self.current >= self.iters.len) return;
            try self.iters[self.current].seekRange(.{}, self.end);
        }
    }
};
