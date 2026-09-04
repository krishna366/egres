//! The statement executor: a pull pipeline of RowNodes exchanging ONE
//! Batch — rows the source copied once, plus a bit set saying which of
//! them survive — and BitmapNodes that turn index reads into egid sets.
//! The source copies; operators after it only clear bits; sinks
//! (Materialize, the wire emitter) are the only ones to touch bytes
//! again. plan.zig builds the trees; dml.zig drives them.

const std = @import("std");
const zdb = @import("zdb");

const _key = @import("key.zig");
const _pred = @import("pred.zig");
const _table = @import("table.zig");
const _txn = @import("txn.zig");

const log = std.log.scoped(.sql);
const Forest = zdb.forest.Forest;
const Item = zdb.kv.Item;
const Key = zdb.kv.Key;
const KeyRange = zdb.level.KeyRange;
const Pred = _pred.Pred;
const Txn = _txn.Txn;
const zroar = zdb.zroar;

/// Everything a node needs from the statement it runs in.
pub const ExecCtx = struct {
    /// Statement arena: captured rows (Materialize), sorted key lists.
    arloc: std.mem.Allocator,
    /// read_ts, pending writes, the forest, the locks; its allocator
    /// backs bitmaps and the scan cursor.
    tx: *Txn,
    tbl: *const _table.Table,
    table_id: u32,
    /// Lock rows: sources BEFORE each read (the lock's bump makes the
    /// read the newest version), Materialize after its drain. lockRow
    /// may raise Restart, so a locking plan never streams.
    lock: bool,
};

pub const row_cap = 1024;
pub const byte_cap: usize = 256 << 10;
/// Bit i set = row i of the batch survives. std.StaticBitSet, not
/// zroar: 16 words inline, no allocation — zroar stays for u64 id sets
/// (postings, BitmapNode.egids), where the universe really is u64.
pub const Selection = std.StaticBitSet(row_cap);

/// The rows in flight between operators. A row is either COPIED in by
/// `append` — its key and value slice into `bytes`, which is
/// capacity-pinned while non-empty and grows only to admit ONE
/// oversized row into an EMPTY batch — or REFERENCED by `appendRefs`
/// from memory that outlives the fill (the statement arena; pinned
/// blocks later), the caller's promise. Sources fill it and set the
/// bits; filters clear bits; consumers walk `selected()` (ascending).
pub const Batch = struct {
    allocator: std.mem.Allocator,
    /// Reserved to row_cap once; never grows past it.
    rows: std.ArrayList(Item),
    sel: Selection = Selection.initEmpty(),
    bytes: std.ArrayList(u8),
    /// How many rows a fill may place — row_cap unless the consumer
    /// lowers it (a LIMIT, a portal honouring Execute's max_rows).
    /// `reset` leaves it alone; sources read it through `full`.
    row_limit: usize = row_cap,
    /// Set when `append` turned a row away for lack of bytes: the fill
    /// is over even though rows remain below the limit. Sources stop at
    /// the first refusal and stash that row (rows are sequential, so a
    /// smaller one is never offered in its place). Cleared by `reset`.
    no_space: bool = false,

    pub fn init(allocator: std.mem.Allocator) !Batch {
        var rows: std.ArrayList(Item) = .empty;
        try rows.ensureTotalCapacityPrecise(allocator, row_cap);
        errdefer rows.deinit(allocator);

        var bytes: std.ArrayList(u8) = .empty;
        try bytes.ensureTotalCapacityPrecise(allocator, byte_cap);
        return .{ .allocator = allocator, .rows = rows, .bytes = bytes };
    }

    pub fn deinit(self: *Batch) void {
        self.rows.deinit(self.allocator);
        self.bytes.deinit(self.allocator);
    }

    /// Empty the batch; capacity stays (it never shrinks).
    pub fn reset(self: *Batch) void {
        self.rows.clearRetainingCapacity();
        self.bytes.clearRetainingCapacity();
        self.sel = Selection.initEmpty();
        self.no_space = false;
    }

    pub fn numRows(self: *const Batch) usize {
        return self.rows.items.len;
    }

    /// This fill accepts no more rows: the row limit is reached, or a
    /// row was already refused for lack of bytes (`no_space`).
    pub fn full(self: *const Batch) bool {
        return self.no_space or self.numRows() >= self.row_limit;
    }

    /// Copy `item` in (its key, and its value when `keep_value`) and
    /// select it. false = no room — the caller keeps the row for the
    /// next fill, and `full` is true from here on. A row larger than the
    /// whole byte capacity is admitted only into an EMPTY batch: nothing
    /// slices into `bytes` then, so growing it is safe.
    pub fn append(self: *Batch, item: Item, keep_value: bool) !bool {
        if (self.full()) return false;

        const vlen = if (keep_value) item.value.len else 0;
        const need = item.key.primary.len + vlen;
        if (self.bytes.items.len + need > self.bytes.capacity) {
            if (self.numRows() > 0) {
                self.no_space = true;
                return false;
            }
            std.debug.assert(self.bytes.items.len == 0);
            try self.bytes.ensureTotalCapacity(self.allocator, need);
        }

        const kstart = self.bytes.items.len;
        self.bytes.appendSliceAssumeCapacity(item.key.primary);
        const primary = self.bytes.items[kstart..][0..item.key.primary.len];

        var value: []const u8 = "";
        if (keep_value) {
            const vstart = self.bytes.items.len;
            self.bytes.appendSliceAssumeCapacity(item.value);
            value = self.bytes.items[vstart..][0..item.value.len];
        }

        self.sel.set(self.numRows());
        self.rows.appendAssumeCapacity(.{
            .key = .{
                .namespace = item.key.namespace,
                .table_id = item.key.table_id,
                .primary = primary,
                .timestamp = item.key.timestamp,
            },
            .value = value,
            .meta = item.meta,
        });
        return true;
    }

    /// Place rows that live elsewhere — no copy, `bytes` untouched —
    /// and select them. Only the row limit bounds them. Returns how
    /// many were placed; the caller offers the rest on the next fill.
    pub fn appendRefs(self: *Batch, rows: []const Item) usize {
        const start = self.numRows();
        const n = @min(rows.len, self.row_limit -| start);
        self.rows.appendSliceAssumeCapacity(rows[0..n]);
        self.sel.setRangeValue(.{ .start = start, .end = start + n }, true);
        return n;
    }

    /// The selected rows, ascending. The batch must not move or be
    /// refilled while an iterator is live.
    pub const Selected = struct {
        batch: *const Batch,
        it: Selection.Iterator(.{}),

        pub fn next(self: *Selected) ?Item {
            const i = self.it.next() orelse return null;
            return self.batch.rows.items[i];
        }
    };

    pub fn selected(self: *const Batch) Selected {
        return .{ .batch = self, .it = self.sel.iterator(.{}) };
    }
};

/// A point read of an index tree: which tree + the memcomparable key.
/// Unique: the entry's value IS the one row key. Non-unique: the entry
/// is a roaring bitmap — every matching egid.
pub const IndexLookup = struct {
    index_table_id: u32,
    key: []const u8,
    unique: bool,
};

/// Bitmap nodes produce a WHOLE egid set — a bitmap already is the
/// batch, and every variant evaluates to one (an egid list touches no
/// index and and_/or_ combine bitmaps, hence not "IndexNode").
pub const BitmapNode = union(enum) {
    index_lookup: IndexLookup,
    /// Literal egids (`egid = N`; IN lists later), built by the planner
    /// and owned by the plan; eval hands out a clone.
    egids: zroar.Bitmap,
    and_: []BitmapNode,
    or_: []BitmapNode,

    /// Frees this node's own bitmaps, recursively.
    pub fn deinit(self: *BitmapNode) void {
        switch (self.*) {
            .egids => |*bm| bm.deinit(),
            .and_, .or_ => |kids| for (kids) |*kid| kid.deinit(),
            .index_lookup => {},
        }
    }

    /// Whether any egid in the result came from an index — a missing
    /// row is then corruption (rows and index entries commit
    /// atomically), not an absent literal id.
    pub fn fromIndex(self: *const BitmapNode) bool {
        return switch (self.*) {
            .index_lookup => true,
            .egids => false,
            .and_, .or_ => |kids| for (kids) |*kid| {
                if (kid.fromIndex()) break true;
            } else false,
        };
    }

    /// Lock every unique-index key in the subtree — even an absent key
    /// is the phantom-insert guard (a racing INSERT of it blocks) — in
    /// (table_id, key) order so concurrent statements cannot cycle.
    /// No-op unless ctx.lock. Call BEFORE eval.
    pub fn lockKeys(self: *const BitmapNode, ctx: *ExecCtx) !void {
        if (!ctx.lock) return;
        var keys: std.ArrayList(IndexLookup) = .empty;
        try self.collectUniqueIndexKeys(ctx.arloc, &keys);
        std.mem.sort(IndexLookup, keys.items, {}, lessThanKey);
        for (keys.items) |lu| try ctx.tx.lockRow(lu.index_table_id, lu.key);
    }

    fn collectUniqueIndexKeys(
        self: *const BitmapNode,
        arloc: std.mem.Allocator,
        out: *std.ArrayList(IndexLookup),
    ) !void {
        switch (self.*) {
            .index_lookup => |lu| if (lu.unique) try out.append(arloc, lu),
            .egids => {},
            .and_, .or_ => |kids| for (kids) |*kid| {
                try kid.collectUniqueIndexKeys(arloc, out);
            },
        }
    }

    fn lessThanKey(_: void, a: IndexLookup, b: IndexLookup) bool {
        if (a.index_table_id != b.index_table_id)
            return a.index_table_id < b.index_table_id;
        return std.mem.order(u8, a.key, b.key) == .lt;
    }

    /// The egid set this node names, OWNED by the caller (tx.allocator).
    /// AND folds in place and stops at the first empty result; OR
    /// evaluates every child and unions them in one pass.
    pub fn eval(self: *const BitmapNode, ctx: *ExecCtx) !zroar.Bitmap {
        const alloc = ctx.tx.allocator;
        switch (self.*) {
            .egids => |*bm| return bm.clone(),
            .index_lookup => |lu| return if (lu.unique)
                uniqueLookup(ctx, lu)
            else
                postingLookup(ctx, lu),
            .and_ => |kids| {
                std.debug.assert(kids.len > 0);
                var acc = try kids[0].eval(ctx);
                errdefer acc.deinit();

                for (kids[1..]) |*kid| {
                    if (acc.isEmpty()) break;
                    var other = try kid.eval(ctx);
                    defer other.deinit();
                    acc.andInPlace(&other);
                }
                return acc;
            },
            .or_ => |kids| {
                std.debug.assert(kids.len > 0);
                var parts: std.ArrayList(zroar.Bitmap) = .empty;
                defer {
                    for (parts.items) |*p| p.deinit();
                    parts.deinit(alloc);
                }

                try parts.ensureTotalCapacity(alloc, kids.len);
                for (kids) |*kid| parts.appendAssumeCapacity(try kid.eval(ctx));
                // Pointers only once the list is complete — appends
                // may have moved it.
                const ptrs = try alloc.alloc(
                    *const zroar.Bitmap,
                    parts.items.len,
                );
                defer alloc.free(ptrs);
                for (parts.items, ptrs) |*p, *q| q.* = p;
                return zroar.Bitmap.fastOr(alloc, ptrs);
            },
        }
    }

    /// `WHERE <unique col> = v`: the entry under the key names the one
    /// row. The key is locked already (lockKeys). Absent → empty. The
    /// value is the 8 BE egid bytes INSERT wrote — anything else is
    /// corruption, failed loudly rather than read as "0 rows".
    fn uniqueLookup(ctx: *ExecCtx, lu: IndexLookup) !zroar.Bitmap {
        var bm = try zroar.Bitmap.init(ctx.tx.allocator);
        errdefer bm.deinit();

        const entry = (try ctx.tx.lookup(.{
            .namespace = ctx.tx.ns,
            .table_id = lu.index_table_id,
            .primary = lu.key,
            .timestamp = ctx.tx.read_ts,
        })) orelse return bm;
        defer entry.release();

        const egid = _key.decodeEgid(entry.item.value) orelse
            return error.CorruptIndexEntry;
        _ = try bm.set(egid);
        return bm;
    }

    /// `WHERE <indexed col> = v` through a posting bitmap. Recorded as a
    /// point read of the INDEX key, not a scan: any commit changing
    /// this key's membership writes a posting delta under this exact
    /// key, so one identity hash revalidates the whole result on a
    /// later read_ts bump — phantom-free. Own pending posting ops are
    /// overlaid. A corrupt posting fails the statement loudly.
    fn postingLookup(ctx: *ExecCtx, lu: IndexLookup) !zroar.Bitmap {
        const pk = Key{
            .namespace = ctx.tx.ns,
            .table_id = lu.index_table_id,
            .primary = lu.key,
            .timestamp = ctx.tx.read_ts,
        };
        try ctx.tx.recordRead(pk);
        var bm = try ctx.tx.forest.getBitmap(ctx.tx.allocator, pk, .p0);
        errdefer bm.deinit();

        try ctx.tx.overlayPostings(lu.index_table_id, lu.key, &bm);
        return bm;
    }
};

/// One operator. `next` either fills `b` (sources) or narrows `b.sel`
/// (filters): true = selected rows are in `b` (a partial batch only
/// when the source ran dry); false = exhausted, and sticky. ONE Batch
/// per pipeline, owned by the plan root and passed down. `deinit`
/// frees this node's own resources only — the plan deinits every node.
pub const RowNode = union(enum) {
    seq_scan: SeqScan,
    fetch: Fetch,
    filter: Filter,
    materialize: Materialize,

    // Explicit error set: Filter and Materialize recurse into RowNode.next,
    // which an inferred set cannot express (dependency loop).
    pub fn next(self: *RowNode, ctx: *ExecCtx, b: *Batch) anyerror!bool {
        return switch (self.*) {
            .seq_scan => |*n| n.next(ctx, b),
            .fetch => |*n| n.next(ctx, b),
            .filter => |*n| n.next(ctx, b),
            .materialize => |*n| n.next(ctx, b),
        };
    }

    /// Frees what the subtree holds — cursors, bitmaps, held rows —
    /// children included: a node owns its child, the plan owns the
    /// root. Node memory itself belongs to the statement arena.
    pub fn deinit(self: *RowNode) void {
        switch (self.*) {
            .seq_scan => |*n| n.deinit(),
            .fetch => |*n| n.deinit(),
            .filter => |*n| n.child.deinit(),
            .materialize => |*n| n.child.deinit(),
        }
    }
};

/// Snapshot scan of the whole table at read_ts, own pending writes
/// merged in. Recorded on the txn as a scan (SI: a later read_ts bump
/// cannot revalidate it). The residual predicate runs on the cursor's
/// BORROW, before the copy, so a non-matching row is never copied.
pub const SeqScan = struct {
    pred: ?*const Pred = null,
    /// false = keys only (a DELETE that never reads the row back).
    keep_value: bool = true,
    cursor: ?Forest.Cursor = null,
    /// A pulled row that did not fit: the borrow stays valid until the
    /// next cursor.next, which happens only after it is appended.
    stash: ?Item = null,
    done: bool = false,

    pub fn next(self: *SeqScan, ctx: *ExecCtx, b: *Batch) !bool {
        b.reset();
        if (self.done) return false;

        if (self.cursor == null) {
            ctx.tx.recordScan();
            self.cursor = try ctx.tx.forest.openCursor(
                ctx.tx.allocator,
                (KeyRange{}).limitToTable(ctx.tx.ns, ctx.table_id),
                .{ .read_ts = ctx.tx.read_ts, .pending = ctx.tx.writes },
            );
        }
        const cursor = &self.cursor.?;
        if (self.stash) |item| {
            // An empty batch admits any single row.
            const ok = try b.append(item, self.keep_value);
            std.debug.assert(ok);
            self.stash = null;
        }
        while (!b.full()) {
            const item = (try cursor.next()) orelse {
                self.done = true;
                break;
            };
            if (self.pred) |pred| {
                if (!_pred.matches(ctx.tbl, item, pred)) continue;
            }

            if (!try b.append(item, self.keep_value)) {
                self.stash = item;
                break;
            }
        }
        return b.numRows() > 0;
    }

    pub fn deinit(self: *SeqScan) void {
        if (self.cursor) |*c| c.deinit();
        self.cursor = null;
    }
};

/// Point lookups of every egid a BitmapNode names, egid-ascending —
/// the same order a table scan yields. With ctx.lock each row is
/// locked BEFORE its read.
pub const Fetch = struct {
    bnode: *BitmapNode,
    pred: ?*const Pred = null,
    keep_value: bool = true,
    /// The evaluated set and a bookmark into it (the bitmap is
    /// unbounded and rows go out a batch at a time; zroar has no seek).
    /// The iterator points at `bm`: the node is NOT movable after its
    /// first next.
    bm: ?zroar.Bitmap = null,
    it: ?zroar.Iterator = null,
    /// A fetched row that did not fit — held (owned or borrowed from the
    /// pending skiplist) until the next fill, then appended and released.
    held: ?Txn.Found = null,
    from_index: bool = false,
    done: bool = false,

    pub fn next(self: *Fetch, ctx: *ExecCtx, batch: *Batch) !bool {
        batch.reset();
        if (self.done) return false;

        if (self.bm == null) {
            try self.bnode.lockKeys(ctx);
            self.bm = try self.bnode.eval(ctx);

            self.it = self.bm.?.iterator();
            self.from_index = self.bnode.fromIndex(); // For debug assert
        }

        if (self.held) |held| {
            const ok = try batch.append(held.item, self.keep_value);
            std.debug.assert(ok);
            held.release();
            self.held = null;
        }

        while (!batch.full()) {
            const egid = self.it.?.next() orelse {
                self.done = true;
                break;
            };
            const found = (try self.lookupEgid(ctx, egid)) orelse continue;
            if (self.pred) |p| if (!_pred.matches(ctx.tbl, found.item, p)) {
                found.release();
                continue;
            };
            if (!try batch.append(found.item, self.keep_value)) {
                self.held = found;
                break;
            }
            found.release();
        }
        return batch.numRows() > 0;
    }

    /// One base row by egid. Locked BEFORE the read when ctx.lock: the
    /// lock's bump has already advanced read_ts, so the value read is
    /// the newest. An absent row is a legit empty result when a literal
    /// named it, but CORRUPTION when an index did (rows and entries
    /// commit atomically) — logged loudly, then still skipped so the
    /// statement answers from the rows that do exist.
    fn lookupEgid(self: *const Fetch, ctx: *ExecCtx, egid: u64) !?Txn.Found {
        const k = _key.encodeEgid(egid);
        if (ctx.lock) try ctx.tx.lockRow(ctx.table_id, &k);
        const found = (try ctx.tx.lookup(.{
            .namespace = ctx.tx.ns,
            .table_id = ctx.table_id,
            .primary = &k,
            .timestamp = ctx.tx.read_ts,
        })) orelse {
            if (self.from_index) log.err(
                "corrupt index entry: an index names egid={d}, " ++
                    "no such row in table={d}",
                .{ egid, ctx.table_id },
            );
            return null;
        };
        return found;
    }

    /// Also frees the bitmap tree: the planner built it for this node.
    pub fn deinit(self: *Fetch) void {
        if (self.held) |found| found.release();
        self.held = null;
        if (self.bm) |*bm| bm.deinit();
        self.bm = null;
        self.it = null;
        self.bnode.deinit();
    }
};

/// Narrows a batch to the rows that satisfy `pred` — clears bits, copies
/// nothing. For a predicate that cannot sit on a source (post-join);
/// the planner pushes the residual into the source otherwise.
pub const Filter = struct {
    child: *RowNode,
    pred: *const Pred,

    pub fn next(self: *Filter, ctx: *ExecCtx, batch: *Batch) anyerror!bool {
        // A batch whose every row fails yields nothing — pull the next
        // rather than hand back an empty selection.
        while (try self.child.next(ctx, batch)) {
            // Clearing bit i mid-walk is safe: the iterator has already
            // copied the word it is handing bits out of.
            var it = batch.sel.iterator(.{});
            while (it.next()) |i| {
                const row = batch.rows.items[i];
                if (!_pred.matches(ctx.tbl, row, self.pred)) batch.sel.unset(i);
            }
            if (batch.sel.count() > 0) return true;
        }
        return false;
    }
};

/// Drain fully, then lock, then act: the DML write bodies call `rows`
/// before their first tx.put; FOR UPDATE streams the same rows back
/// through `next` after the locks.
pub const Materialize = struct {
    child: *RowNode,
    rows: ?[]const Item = null,
    pos: usize = 0,

    /// Every selected row of the child, copied into the statement arena
    /// (keys and whatever values the source kept), in the order the
    /// child yielded them — an ORDER BY below survives. With ctx.lock
    /// every key is then locked in KEY order — the order that keeps
    /// concurrent statements from cycling, taken from a separate sorted
    /// key list so the rows keep their order — and this returns only
    /// once EVERY lock is held (lockRow may Restart the statement, which
    /// is safe exactly because nothing was written or sent yet). `batch`
    /// is scratch to drive the child.
    pub fn gatherAndLockRows(
        self: *Materialize,
        ctx: *ExecCtx,
        batch: *Batch,
    ) anyerror![]const Item {
        if (self.rows) |rows| return rows;

        var lrows: std.ArrayList(Item) = .empty;
        while (try self.child.next(ctx, batch)) {
            var it = batch.selected();
            while (it.next()) |row| {
                try lrows.append(ctx.arloc, try row.dupe(ctx.arloc));
            }
        }
        const rows = try lrows.toOwnedSlice(ctx.arloc);
        if (ctx.lock) try lockInKeyOrder(ctx, rows);
        self.rows = rows;
        return rows;
    }

    /// Lock every row's key ascending, whatever order `rows` is in.
    /// Both sources already yield ascending keys, so the sort is one
    /// pass in practice; it makes the lock order this node's own
    /// property rather than the child's.
    fn lockInKeyOrder(ctx: *ExecCtx, rows: []const Item) !void {
        const keys = try ctx.arloc.alloc([]const u8, rows.len);
        for (rows, keys) |row, *key| key.* = row.key.primary;
        std.mem.sort([]const u8, keys, {}, lessThanBytes);
        for (keys) |key| try ctx.tx.lockRow(ctx.table_id, key);
    }

    fn lessThanBytes(_: void, a: []const u8, b: []const u8) bool {
        return std.mem.order(u8, a, b) == .lt;
    }

    /// Serve the captured rows back a batch at a time, by reference:
    /// they already live in the arena, so nothing is copied.
    pub fn next(
        self: *Materialize,
        ctx: *ExecCtx,
        batch: *Batch,
    ) anyerror!bool {
        const rows = try self.gatherAndLockRows(ctx, batch);
        batch.reset();

        const n = batch.appendRefs(rows[self.pos..]);
        self.pos += n;
        return n > 0;
    }
};
