//! The SQL read machinery: source selection (resolveSource — the
//! planner) and the Scanner that feeds every statement its rows
//! (snapshot scan, egid point lookups, unique-index lookup, posting
//! bitmap) filtered through its predicate (pred.zig), plus the
//! RowEmitter that projects matches onto the wire and UPDATE's pure
//! row rewrite.

const std = @import("std");
const engine = @import("engine");
const pgwire = @import("pgwire");
const zdb = @import("zdb");

const _key = @import("key.zig");
const _pred = @import("pred.zig");
const _row = @import("row.zig");
const _schema = @import("schema.zig");
const _table = @import("table.zig");
const _txn = @import("txn.zig");
const _value = @import("value.zig");

const log = std.log.scoped(.sql);
const Pred = _pred.Pred;

/// A resolved index point lookup: which index tree + the memcomparable key.
pub const IndexLookup = struct { index_table_id: u32, key: []const u8 };

/// Pick where the statement's rows come from. The FIRST indexable
/// top-level AND term drives the read: `egid = N` is a point lookup;
/// `col = v` on a valid indexed column an index read (unique: the one
/// row under the key; non-unique: the key's posting bitmap — every
/// matching row); `col IS NULL` on a valid non-unique index the bitmap
/// under the NULL tag key. Anything else scans the table. Choosing is
/// only about which rows are READ — the whole predicate is re-checked
/// per row (Scanner.residual), so a term the source already decided
/// costs one more cell compare, never a wrong answer. A comparison
/// against NULL can never be true (3VL), so the source is empty and
/// nothing is read or recorded. `a` (the statement arena) backs the
/// encoded keys.
pub fn resolveSource(
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    p: *const Pred,
) !Scanner.Source {
    var pick: ?Scanner.Source = null;
    for (p.topLevelAnds()) |*c| {
        switch (c.*) {
            .eq => |cmp| {
                // If query asks for col == null, then just return empty set,
                // even if we are holding a legit pick.
                if (cmp.val == .null_value) return .{ .egids = &.{} };

                if (pick != null) continue;
                if (cmp.col.isEgid()) {
                    pick = .{ .egids = try arloc.dupe(u64, &.{cmp.val.uint8}) };
                } else {
                    pick = try indexSource(arloc, tbl, cmp.col, cmp.val);
                }
            },
            .is_null => |nt| {
                if (nt.negate or pick != null) continue;
                // A unique index holds no NULL entries; only a posting
                // bitmap can answer IS NULL.
                if (try indexSource(arloc, tbl, nt.col, .null_value)) |s| {
                    if (s == .index_bitmap) pick = s;
                }
            },
            .and_, .or_, .not => {},
        }
    }
    return pick orelse .table;
}

/// The index read answering `col = val`, or null when the column has
/// no valid index or the value has no memcomparable encoding
/// (interval) — then the rows are found by scanning. NULL encodes (its
/// own tag-byte key). A mid-backfill index (catalog.IndexDef.valid =
/// false) is maintained by DML but must not be read yet.
fn indexSource(
    a: std.mem.Allocator,
    tbl: *const _table.Table,
    col: *const _table.Column,
    val: _value.Value,
) !?Scanner.Source {
    for (tbl.indexes) |idx| {
        if (idx.column != col or !idx.valid) continue;
        const ik = _key.encodeIndexKey(a, val, null) catch |err| {
            if (err == error.OutOfMemory) return err;
            return null;
        };
        const lu = IndexLookup{ .index_table_id = idx.index_table_id, .key = ik };
        // Unique: the entry's value IS the one row key. Non-unique:
        // the entry is a roaring bitmap — every matching egid.
        return if (idx.unique) .{ .index_eq = lu } else .{ .index_bitmap = lu };
    }
    return null;
}

/// Turns each row into a projected DataRow on the wire — Scanner's
/// emitter for streaming (emit_value), and driven directly to emit a
/// FOR UPDATE capture post-lock (filtering is Scanner's job either
/// way). Columns format straight into the frame (pgwire DataRowWriter),
/// which the connection's buffered writer batches to the socket.
pub const RowEmitter = struct {
    state: *pgwire.ConnState,
    t: *const _table.Table,
    /// The projection — egid is a column too (table.egid_column).
    outs: []const *const _table.Column,
    count: u64 = 0,

    /// Project and send one row — each output cell decoded on its own
    /// (O(1) each, Table.read), no full-row decode.
    pub fn emit(self: *RowEmitter, item: zdb.kv.Item) !void {
        var dw = try self.state.beginDataRow(@intCast(self.outs.len));
        for (self.outs) |col| {
            const v = self.t.read(item, col);
            if (v == .null_value) {
                try dw.writeNull();
            } else {
                try _value.formatValue(try dw.beginColumn(), v);
                dw.endColumn();
            }
        }
        try dw.finish();
        self.count += 1;
    }
};

/// The statement scanner — the one forest.scan callback for DML. Filters
/// each row through the statement's predicate (reading only the cells
/// it names — no full-row decode), then handles the match per `mode` —
/// streamed to the wire, or captured into `items` for a post-lock pass
/// (duped into the statement arena; scan Items don't outlive the
/// callback). Default ScanOptions skip tombstones, so only live rows
/// are seen — re-running a DELETE reports 0.
pub const Scanner = struct {
    a: std.mem.Allocator, // statement arena — backs items
    t: *const _table.Table,
    /// The WHERE, whole — checked on every row the source yields, even
    /// rows an index already selected (see resolveSource).
    residual: ?*const Pred = null,
    source: Source = .table,
    mode: Mode,
    /// Lock every matched row (exclusive, held to txn end) before run()
    /// returns: point keys BEFORE their read (the lock's bump makes the
    /// read the newest version — the lost-update guard without a
    /// latest-read), scan captures after the scan, in key order so
    /// concurrent statements can't cycle. lockRow may raise Restart
    /// mid-run — safe exactly because run() has buffered no writes and
    /// sent no wire bytes; the re-run finds the locks still held
    /// (re-entrant) and takes a fresh capture. Streaming (emit_value)
    /// must NOT lock: its rows are already on the wire and a Restart
    /// would resend them.
    lock: bool = false,
    decoded: _value.Row = .{}, // rewrite decode scratch, arena-backed
    /// Captured matches. The post-lock pass tombstones/puts/emits
    /// straight from these: reaching it means no candidate committed
    /// above read_ts (that lockRow would have bumped → restarted →
    /// re-scanned), so the scan-time source IS the newest version and
    /// no second forest read is needed.
    items: std.ArrayList(zdb.kv.Item) = .empty,
    err: ?anyerror = null,

    /// Where the statement's rows come from (chosen by resolveSource).
    pub const Source = union(enum) {
        /// Snapshot scan of the whole table; residual prunes rows.
        table,
        /// Point lookups by row id — `WHERE egid = N` today (one
        /// entry), IN-lists later. With `lock` set, each key is locked
        /// BEFORE its read: the lock's bump has already advanced
        /// read_ts, so the value read is the newest. An absent row
        /// still leaves its key locked — harmless today, and the
        /// phantom-insert guard once explicit row ids exist.
        egids: []const u64,
        /// Point lookup through a unique index — `WHERE <indexed col> =
        /// v`: read the row id in `index_table_id` under `key`, then the
        /// base row. With `lock` set, the INDEX key is locked FIRST (even
        /// when absent — the phantom-insert guard: a concurrent INSERT of
        /// the same key blocks), then the row.
        index_eq: IndexLookup,
        /// Every row matching `WHERE <indexed col> = v` through a
        /// non-unique index: the posting bitmap under `key` in
        /// `index_table_id` at read_ts, own pending posting ops
        /// overlaid, then one point lookup per egid. With `lock` set
        /// each MATCHED row is locked before its read — the lost-update
        /// guard. The posting key itself is never locked: posting
        /// writers don't lock (commuting deltas), so it would guard
        /// nothing — no phantom-insert guard, same as a `.table` scan.
        index_bitmap: IndexLookup,
    };

    /// What each match's value gets used for.
    const Mode = union(enum) {
        /// DELETE: key-only capture — tombstoned by key, bytes unused.
        no_value,
        /// SELECT FOR UPDATE: capture the row verbatim; the post-lock
        /// pass emits it.
        copy_value,
        /// Plain SELECT: straight to the wire; nothing is captured.
        emit_value: *RowEmitter,
        /// Capture the REWRITTEN row (built here, while the scan's
        /// source bytes are alive), so the old value is never copied
        /// and the post-lock pass just buffers it verbatim.
        /// TODO: currently unused — UPDATE always captures the
        /// pre-image (.copy_value) since index maintenance needs the
        /// old cell values; revisit as the no-index fast path.
        update_value: []const SetVal,
    };

    fn collect(self: *Scanner, item: zdb.kv.Item) !void {
        // Only the cells the predicate names are read — no full-row
        // decode here. Unknown (a NULL on either side) rejects like false.
        if (self.residual) |p| {
            if (!_pred.matches(self.t, item, p)) return;
        }
        const captured: zdb.kv.Item = switch (self.mode) {
            .emit_value => |em| return em.emit(item),
            .no_value => try item.dupeNoValue(self.a),
            .copy_value => try item.dupe(self.a),
            .update_value => |sets| rewritten: {
                var dup = try item.dupeNoValue(self.a);
                // Rewrite now, borrowing the scan's transient row
                // bytes: pure computation into the arena — no txn state
                // touched, so a later Restart just discards it with the
                // statement arena.
                var buf: std.ArrayList(u8) = .empty; // arena; persists
                dup.value = try rewriteRowBytes(
                    self.a,
                    &self.t.ps,
                    sets,
                    &self.decoded,
                    item.value,
                    &buf,
                );
                break :rewritten dup;
            },
        };
        try self.items.append(self.a, captured);
    }

    /// forest.scan callback: collect, capturing any error so the scan stops.
    fn scanCb(self: *Scanner, item: zdb.kv.Item) anyerror!bool {
        self.collect(item) catch |e| {
            self.err = e;
            return false;
        };
        return true;
    }

    /// One base row by egid — the shared body of the `.egids` and
    /// `.index_bitmap` arms. With `lock` set the row is locked BEFORE
    /// its read: the lock's bump has already advanced read_ts, so the
    /// value read is the newest. An absent row is skipped: a legit
    /// empty result when the caller named the id (`WHERE egid = N`),
    /// but CORRUPTION when `index_tid` says a posting named it (deltas
    /// commit atomically with their row) — logged loudly, then still
    /// skipped so the statement answers from the rows that do exist. A
    /// collect error lands in `err` and returns false so the source
    /// loop stops; lock/lookup errors propagate.
    fn lookupEgid(
        self: *Scanner,
        ns: u64,
        table_id: u32,
        tx: *_txn.Txn,
        egid: u64,
        index_tid: ?u32,
    ) !bool {
        const k = _key.encodeEgid(egid);
        if (self.lock) try tx.lockRow(table_id, &k);

        const found = (try tx.lookup(.{
            .namespace = ns,
            .table_id = table_id,
            .primary = &k,
            .timestamp = tx.read_ts,
        })) orelse {
            if (index_tid) |itid| log.err(
                "corrupt index entry: index={d} posting names egid={d}, no such row in table={d}",
                .{ itid, egid, table_id },
            );
            return true;
        };
        defer found.release();

        self.collect(found.item) catch |e| {
            self.err = e;
            return false;
        };
        return true;
    }

    /// Feed the statement's rows through `collect`, per `source` — and
    /// when `lock` is set, return only once every match is locked.
    /// Point lookups read at the txn's read_ts (pending write first —
    /// read-your-own-writes; tombstones read as absent). The table scan
    /// is recorded on the txn first (SI: prior-scan revalidation on a
    /// later bump); limitToTable confines it across the namespace-wide
    /// memtable. Collect errors land in `err`; lock errors propagate —
    /// callers map them via replyRunErr.
    pub fn run(
        self: *Scanner,
        eng: *engine.Engine,
        ns: u64,
        table_id: u32,
        tx: *_txn.Txn,
    ) !void {
        std.debug.assert(!(self.lock and self.mode == .emit_value));
        switch (self.source) {
            .egids => |egids| for (egids) |egid| {
                if (!try self.lookupEgid(ns, table_id, tx, egid, null)) return;
            },
            .index_bitmap => |ix| {
                // Recorded as a point read of the INDEX key, not a
                // scan: any commit changing this key's membership
                // writes a posting delta under this exact key (bumping
                // its oracle floor), so one identity hash revalidates
                // the whole result set on a later read_ts bump —
                // phantom-free, where a scan would have to Conflict.
                const pk = zdb.kv.Key{
                    .namespace = ns,
                    .table_id = ix.index_table_id,
                    .primary = ix.key,
                    .timestamp = tx.read_ts,
                };
                try tx.recordRead(pk);
                // A corrupt posting (CorruptPosting/NotRoaring) fails
                // the statement loudly — never a silent scan fallback.
                var bm = try eng.forest.getBitmap(eng.allocator, pk, .p0);
                defer bm.deinit();
                try tx.overlayPostings(ix.index_table_id, ix.key, &bm);
                // Egid-ascending — the same output order a table scan
                // yields.
                var it = bm.iterator();
                while (it.next()) |egid| {
                    if (!try self.lookupEgid(
                        ns,
                        table_id,
                        tx,
                        egid,
                        ix.index_table_id,
                    )) return;
                }
            },
            .index_eq => |ix| {
                // Lock the index key FIRST — even absent, it is the
                // phantom-insert guard (a racing INSERT of this key blocks).
                if (self.lock) try tx.lockRow(ix.index_table_id, ix.key);

                const entry = (try tx.lookup(.{
                    .namespace = ns,
                    .table_id = ix.index_table_id,
                    .primary = ix.key,
                    .timestamp = tx.read_ts,
                })) orelse return; // no such key (index key stays locked)
                defer entry.release();

                // The entry's value IS the row key (8 BE egid bytes,
                // written by INSERT) — use it directly, after the shape
                // check that keeps corruption from reading as "0 rows".
                const rk = entry.item.value;
                if (rk.len != 8) {
                    self.err = error.CorruptIndexEntry;
                    return;
                }
                if (self.lock) try tx.lockRow(table_id, rk);
                const found = (try tx.lookup(.{
                    .namespace = ns,
                    .table_id = table_id,
                    .primary = rk,
                    .timestamp = tx.read_ts,
                })) orelse {
                    // Entry and row commit atomically — an entry
                    // without its row is corruption. Log; answer
                    // "no match" rather than failing the statement.
                    log.err(
                        "corrupt index entry: index={d} names egid={d}, no such row in table={d}",
                        .{ ix.index_table_id, _key.decodeEgid(rk) orelse 0, table_id },
                    );
                    return;
                };
                defer found.release();
                self.collect(found.item) catch |e| {
                    self.err = e;
                    return;
                };
            },
            .table => {
                tx.recordScan();
                _ = try eng.forest.scan(
                    eng.allocator,
                    (zdb.level.KeyRange{}).limitToTable(ns, table_id),
                    .{ .read_ts = tx.read_ts, .pending = tx.writes },
                    self,
                    Scanner.scanCb,
                );
                // A failed collect ends the statement — don't bother
                // locking a partial capture.
                if (self.lock and self.err == null)
                    for (self.items.items) |item|
                        try tx.lockRow(table_id, item.key.primary);
            },
        }
    }
};

/// One resolved SET assignment: the column + its typed replacement value.
pub const SetVal = struct { col: *const _table.Column, val: _value.Value };

/// UPDATE's rewrite: decode `src` (its cells borrow from src — the call
/// must finish while src is alive), overlay the SET values, marshal the
/// new row into `buf`, and return its bytes (a view into buf). Pure —
/// no txn state touched. The WHERE filter was already applied to these
/// same bytes when the row was found, so there is nothing to re-verify.
pub fn rewriteRowBytes(
    a: std.mem.Allocator,
    ps: *const _schema.PhysicalSchema,
    sets: []const SetVal,
    dec: *_value.Row,
    src: []const u8,
    buf: *std.ArrayList(u8),
) ![]const u8 {
    try _row.unmarshalRow(a, ps, src, &.{}, dec);
    // Overlaying into the codec's backing store is fine: the next
    // unmarshalRow refills every cell.
    for (sets) |sv| dec.cells.items[sv.col.pos] = sv.val;
    return _row.marshalRow(a, ps, dec.items(), buf);
}
