const std = @import("std");
const _table = @import("table.zig");
const _cache = @import("../cache.zig");
const Key = @import("../kv.zig").Key;
const Version = @import("../kv.zig").Version;
const _skl = @import("../skl/skiplist.zig");
const utils = @import("utils");

pub const Error = _table.Error;

/// A refcounted hold on the storage backing an iterator item's values:
/// a decompressed block (SST sources) or a memtable's skiplist arena.
/// Every positioned source is pinnable — `Iterator.currentPins` is
/// empty only when there is no item, and a merged item names one pin
/// per source it drew from. `acquire` takes the ref and returns the pin
/// for append-style use; `release` drops it (the skiplist arena
/// outlives its Memtables slot while held; a block outlives cache
/// eviction while held).
///
/// A held slice is stable AND immutable, even against the active
/// memtable: skiplist arenas are append-only — an overwrite dupes new
/// bytes and publishes a new version node, never touching old bytes (see
/// SkipList.putItem) — and blocks are immutable by construction. So
/// borrowing until a fold completes carries exactly the guarantees
/// copying at capture time had.
pub const Pin = union(enum) {
    block: *_cache.Value,
    skiplist: *_skl.SkipList,

    pub fn acquire(self: Pin) Pin {
        switch (self) {
            .block => |b| _ = b.incrRef(),
            .skiplist => |sl| sl.acquire(),
        }
        return self;
    }

    pub fn release(self: Pin) void {
        switch (self) {
            .block => |b| b.decrRef(),
            .skiplist => |sl| sl.release(),
        }
    }
};

/// Iterator is a tagged union over every source shape: a TableIterator
/// (single SST), a ConcatIterator (multiple non-overlapping SSTs), a
/// memtable scan (`skl.Iterator`), or a MergeIterator over other
/// sources. All expose the same `seekRange` / `itemRef` / `next` /
/// `currentPins` / `deinit` shape, so a reader can take one `*Iterator`
/// whatever is behind it.
pub const Iterator = union(enum) {
    table: _table.TableIterator,
    concat: ConcatIterator,
    skiplist: _skl.Iterator,
    merge: MergeIterator,

    // `Error`, not inferred: a merge's seek calls back into this one.
    pub fn seekRange(self: *Iterator, start: Key, end: Key) Error!void {
        switch (self.*) {
            inline else => |*it| try it.seekRange(start, end),
        }
    }

    /// The pins on the storage backing itemRef()'s values; empty only
    /// when the source holds no item. A leaf source has one pin and
    /// returns it through `buf`; a merge returns its own list, one pin
    /// per source the item drew from.
    pub fn currentPins(self: *Iterator, buf: *[1]Pin) []const Pin {
        switch (self.*) {
            .table => |*t| {
                const b = t.currentBlock() orelse return &.{};
                buf[0] = .{ .block = b };
                return buf[0..1];
            },
            .concat => |*c| {
                const b = c.currentBlock() orelse return &.{};
                buf[0] = .{ .block = b };
                return buf[0..1];
            },
            .skiplist => |*s| {
                if (s.node == null) return &.{};
                buf[0] = .{ .skiplist = s.skiplist };
                return buf[0..1];
            },
            .merge => |*m| return m.currentPins(),
        }
    }

    pub fn itemRef(self: *const Iterator) ?_table.Item {
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

/// One item per key, drawn from every source that holds it.
///
/// `live` is the sources that still hold an item, sorted by that item's
/// key (ties by the source's position among the inputs, so a txn's
/// pending writes, source 0, come first). The sources holding the
/// smallest key are therefore the first `n_cur` entries, and the item
/// is built from them: one source's versions as they are, or several
/// sources' versions sorted by timestamp descending with a repeated
/// timestamp dropped (a version a flushed memtable and its L0 table
/// both hold, or a txn's pending write over the committed version it
/// read; the lower source wins). Source order is not timestamp order —
/// L0 tables arrive oldest first, and a txn's pending version can be
/// older than committed memtable versions — which is why the versions
/// are sorted. `next` takes those sources out, advances them, and puts
/// each back at its place with a scan from the front: a source in the
/// middle of a run of keys costs one compare, one that jumped past the
/// others costs as many as it passed.
///
/// Nothing is held: the item's key and values point into the sources,
/// which keep their blocks and arenas pinned until they are advanced,
/// so the item is valid until the following `next`, `seekRange` or
/// `deinit`. `currentPins` names those pins for a caller that keeps a
/// value longer.
pub const MergeIterator = struct {
    /// One input: its iterator, its current item (refreshed on every
    /// advance, null once exhausted), and its position among the inputs
    /// as given to `init`, the tie-break between equal keys.
    const Source = struct {
        it: Iterator,
        item: ?_table.Item = null,
        prio: usize,
    };

    allocator: std.mem.Allocator,
    /// Every input, for the merge's lifetime: a source that ran out
    /// leaves `live` but comes back on the next `seekRange`.
    srcs: []*Source,
    /// The sources holding an item, sorted by (key, prio).
    live: std.ArrayList(*Source) = .empty,
    /// How many sources at the front of `live` hold the current key.
    n_cur: usize = 0,
    /// The current item: the first source's own item, or its key over
    /// `vers` when the key is shared.
    cur: ?_table.Item = null,
    vers: std.ArrayList(Version) = .empty,
    /// Scratch for `currentPins`.
    pins: std.ArrayList(Pin) = .empty,

    /// Takes the iterators over: `deinit` releases them. `iters` itself
    /// stays the caller's; on error nothing was taken.
    pub fn init(
        allocator: std.mem.Allocator,
        iters: []const Iterator,
    ) !MergeIterator {
        const srcs = try allocator.alloc(*Source, iters.len);
        errdefer allocator.free(srcs);
        var made: usize = 0;
        errdefer for (srcs[0..made]) |src| allocator.destroy(src);
        for (srcs, iters, 0..) |*slot, it, prio| {
            const src = try allocator.create(Source);
            src.* = .{ .it = it, .prio = prio };
            slot.* = src;
            made += 1;
        }
        var self: MergeIterator = .{ .allocator = allocator, .srcs = srcs };
        errdefer self.live.deinit(allocator);
        try self.live.ensureTotalCapacity(allocator, iters.len);
        // One pin per source at most.
        try self.pins.ensureTotalCapacity(allocator, iters.len);
        return self;
    }

    pub fn deinit(self: *MergeIterator) void {
        for (self.srcs) |src| {
            src.it.deinit();
            self.allocator.destroy(src);
        }
        self.allocator.free(self.srcs);
        self.live.deinit(self.allocator);
        self.vers.deinit(self.allocator);
        self.pins.deinit(self.allocator);
    }

    /// The order of `live`: by key (the full key, since a MultiGet seek
    /// over the memtables has no end bound and its sources can settle on
    /// keys of different tables or namespaces), then by prio. The first
    /// parameter is the context std.sort passes; there is none.
    fn earlier(_: void, a: *Source, b: *Source) bool {
        return switch (a.item.?.key.order(b.item.?.key)) {
            .lt => true,
            .gt => false,
            .eq => a.prio < b.prio,
        };
    }

    pub fn seekRange(self: *MergeIterator, start: Key, end: Key) Error!void {
        utils.clearRetainCap(&self.live);
        for (self.srcs) |src| {
            try src.it.seekRange(start, end);
            src.item = src.it.itemRef();
            if (src.item != null) self.live.appendAssumeCapacity(src);
        }
        std.mem.sortUnstable(*Source, self.live.items, {}, earlier);
        try self.build();
    }

    /// The pins on the storage backing itemRef()'s values, one per
    /// source the item was built from; empty when the merge is
    /// exhausted. The pins stay borrowed: the sources hold them.
    pub fn currentPins(self: *MergeIterator) []const Pin {
        utils.clearRetainCap(&self.pins);
        for (self.live.items[0..self.n_cur]) |src| {
            var buf: [1]Pin = undefined;
            for (src.it.currentPins(&buf)) |pin| {
                const covered = self.pins.items.len > 0 and
                    std.meta.eql(self.pins.getLast(), pin);
                if (!covered) self.pins.appendAssumeCapacity(pin);
            }
        }
        return self.pins.items;
    }

    pub fn itemRef(self: *const MergeIterator) ?_table.Item {
        return self.cur;
    }

    /// Step to the next key: advance the sources the item was built
    /// from, move each to its place, and build again. The sources are
    /// the first `n_cur` entries, and each pass handles the one at the
    /// front: the ones not yet advanced still hold the smallest key, so
    /// the advanced one lands behind them, and the next pass finds the
    /// next of them at the front.
    pub fn next(self: *MergeIterator) Error!void {
        if (self.cur == null) return;
        for (0..self.n_cur) |_| {
            const src = self.live.items[0];
            try src.it.next();
            src.item = src.it.itemRef();
            if (src.item == null) {
                _ = self.live.orderedRemove(0);
                continue;
            }
            // Its place among the rest: past every source that sorts
            // before it. One compare and no move when it still holds
            // the smallest key.
            var pos: usize = 0;
            for (self.live.items[1..]) |other| {
                if (!earlier({}, other, src)) break;
                pos += 1;
            }
            const items = self.live.items;
            std.mem.copyForwards(*Source, items[0..pos], items[1 .. pos + 1]);
            items[pos] = src;
        }
        try self.build();
    }

    /// Build the item from the sources at the front of `live`, the ones
    /// holding the smallest key.
    fn build(self: *MergeIterator) Error!void {
        self.n_cur = 0;
        self.cur = null;
        if (self.live.items.len == 0) return;

        const first = self.live.items[0].item.?;
        self.n_cur = 1;
        while (self.n_cur < self.live.items.len) : (self.n_cur += 1) {
            const item = self.live.items[self.n_cur].item.?;
            if (item.key.order(first.key) != .eq) break;
        }
        if (self.n_cur == 1) {
            self.cur = first;
            return;
        }

        // The sources are in prio order, so a stable sort keeps the
        // lower source's copy of an equal timestamp for the dedup to
        // keep. Insertion sort: stable, in place, and right for the
        // few versions a key has — `mem.sort` carries a 512-element
        // scratch cache that a safe build poison-fills on every call,
        // which was a third of a full-table scan once rows had versions
        // in more than one source.
        utils.clearRetainCap(&self.vers);
        for (self.live.items[0..self.n_cur]) |src| {
            const vers = src.item.?.versions.slice();
            try self.vers.appendSlice(self.allocator, vers);
        }
        std.sort.insertion(Version, self.vers.items, {}, newerFirst);
        var kept: usize = 1;
        for (self.vers.items[1..]) |ver| {
            if (ver.ts == self.vers.items[kept - 1].ts) continue;
            self.vers.items[kept] = ver;
            kept += 1;
        }
        self.vers.items.len = kept;
        self.cur = .{
            .key = first.key,
            .versions = .{ .many = self.vers.items },
        };
    }

    fn newerFirst(_: void, a: Version, b: Version) bool {
        return a.ts > b.ts;
    }
};

pub const ConcatIterator = struct {
    iters: []_table.TableIterator, // caller-owned slice; sorted by smallest key
    current: usize, // == iters.len when exhausted
    end: Key = .{}, // upper bound carried over from `seekRange` for lazy seeking

    pub fn init(iters: []_table.TableIterator) ConcatIterator {
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
            self.current = utils.search(_table.TableIterator, self.iters, start, struct {
                fn f(sk: Key, it: _table.TableIterator) bool {
                    return it.table.biggest.order(sk) != .lt;
                }
            }.f);
            if (self.current >= self.iters.len) return;
        }
        try self.iters[self.current].seekRange(start, end);
    }

    pub fn itemRef(self: *const ConcatIterator) ?_table.Item {
        if (self.current >= self.iters.len) return null;
        return self.iters[self.current].itemRef();
    }

    pub fn currentBlock(self: *const ConcatIterator) ?*_cache.Value {
        if (self.current >= self.iters.len) return null;
        return self.iters[self.current].currentBlock();
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
