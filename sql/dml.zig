//! The four DML statement bodies — INSERT, SELECT, DELETE, UPDATE.
//! Each resolves its table and WHERE, plans the read (plan.zig) and
//! runs it (exec.zig) for the rows it acts on, maintains the PK index
//! entry and any posting deltas, and buffers the result into the
//! statement's txn. The restart loop that re-runs these on a read_ts
//! bump lives in server.runDml.
//!
//! Free functions over `*Handler` — see the note in ddl.zig.

const std = @import("std");
const engine = @import("engine");
const pgwire = @import("pgwire");
const zdb = @import("zdb");

const _exec = @import("exec.zig");
const _parse = @import("parse.zig");
const _key = @import("key.zig");
const _plan = @import("plan.zig");
const _pred = @import("pred.zig");
const _row = @import("row.zig");
const _schema = @import("schema.zig");
const _table = @import("table.zig");
const _txn = @import("txn.zig");
const _value = @import("value.zig");
const _server = @import("server.zig");

const ExecCtx = _exec.ExecCtx;
const Handler = _server.Handler;
const Pred = _pred.Pred;
const SetVal = _row.SetVal;
const String = _value.String;
const litToValue = _parse.litToValue;
const rewriteRowBytes = _row.rewriteRowBytes;
const log = std.log.scoped(.sql);
const replyError = _server.replyError;
const replyErrorCode = _server.replyErrorCode;
const replyLockErr = _server.replyLockErr;

pub fn insert(self: *Handler, state: *pgwire.ConnState, ins: _parse.Insert) !void {
    const eng = self.srv.engine;
    const ns = try self.sessionNamespace(state) orelse return;
    const table_id = eng.forest.lookupTable(ns, ins.table) orelse
        return replyErrorCode(state, pgwire.sqlstate.undefined_table, "table does not exist");

    var arena = std.heap.ArenaAllocator.init(eng.allocator);
    defer arena.deinit();
    const arloc = arena.allocator();

    const tx = (try self.dmlTxn(state, ns, table_id)) orelse return;

    // Open AFTER the intent locks: a statement that parked behind
    // DDL holding the table lock exclusively (CREATE/DROP INDEX)
    // must see the reloaded schema, not a stale cached Table.
    const tbl = self.srv.openTable(ns, table_id) catch |err| {
        log.err("insert into '{s}': load schema: {s}", .{ ins.table, @errorName(err) });
        return replyError(state, "failed to load table schema");
    };

    // The column each VALUES position writes (its cell in `values` is
    // Column.pos). Positional insert is the live columns themselves;
    // an explicit list resolves names. Inserting an explicit `egid`
    // isn't supported yet.
    // TODO: allow user-provided egids once unique-index enforcement exists
    // — a below-counter egid needs a data-tree existence check, and
    // concurrent inserts of the same egid need apply-time validation.
    const values = try arloc.alloc(_value.Value, tbl.columns.len);
    const target_cols: []const _table.Column = blk: {
        const cols = ins.columns orelse break :blk tbl.columns;

        const list = try arloc.alloc(_table.Column, cols.len);
        for (cols, list) |name, *target| {
            const col = tbl.column(name) orelse
                return replyErrorCode(state, pgwire.sqlstate.undefined_column, "column does not exist");
            if (col.isEgid())
                return replyErrorCode(state, pgwire.sqlstate.feature_not_supported, "inserting an explicit egid is not supported yet");
            target.* = col.*;
        }
        break :blk list;
    };

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(eng.allocator);

    // One VALUES row, fully derived in Phase 0 — parse + marshal +
    // encode every key BEFORE any lock or write: a Restart re-runs
    // the whole statement, so nothing may be buffered until every
    // PK lock is held (§Phase 1). `values` is reused per row, so
    // anything derived from a row's cells is captured here.
    const Row = struct {
        bytes: String = "", // marshaled row value
        pkey: String = "", // PK index key, when t.pkey != null
        /// Per posting index (same order as `posting_idx`): the
        /// key this row lands under. Always present — NULL cells
        /// key on the NULL tag byte, and unencodable column types
        /// (interval) never carry an index (rejected at CREATE
        /// INDEX).
        bitmap_keys: []String = &.{},

        fn lessByPkey(rows: []@This(), x: usize, y: usize) bool {
            return std.mem.lessThan(u8, rows[x].pkey, rows[y].pkey);
        }
    };
    const num_rows = ins.rows.len;
    const bitmap_idxs = try bitmapIndexes(arloc, tbl);
    const rows = try arloc.alloc(Row, num_rows);
    for (ins.rows, rows) |row_lits, *prow| {
        prow.* = .{};
        if (row_lits.len != target_cols.len)
            return replyErrorCode(state, pgwire.sqlstate.syntax_error, "INSERT has the wrong number of values");

        for (values) |*v| v.* = .null_value; // reset all to null

        for (row_lits, target_cols) |lit, col| {
            values[col.pos] =
                (try coerceLiteral(state, arloc, lit, col.ty)) orelse
                return;
        }

        if (tbl.primary_index) |pk| {
            const val = values[pk.column.pos];
            if (val == .null_value)
                return replyErrorCode(
                    state,
                    pgwire.sqlstate.not_null_violation,
                    "null value in primary key column violates not-null constraint",
                );
            prow.pkey = _key.encodeIndexKey(arloc, val, null) catch |err|
                switch (err) {
                    error.OutOfMemory => return err,
                    error.Unencodable => return replyErrorCode(
                        state,
                        pgwire.sqlstate.feature_not_supported,
                        "PRIMARY KEY on this column type is not supported",
                    ),
                };
        }
        if (bitmap_idxs.len > 0) {
            prow.bitmap_keys = try arloc.alloc(String, bitmap_idxs.len);
            for (bitmap_idxs, prow.bitmap_keys) |ix, *bm_key| {
                bm_key.* = _key.encodeIndexKey(
                    arloc,
                    values[ix.column.pos],
                    null,
                ) catch |err| switch (err) {
                    error.Unencodable => unreachable,
                    else => return err,
                };
            }
        }
        // marshalRow reuses `buf`; dupe out before the next row clobbers it.
        const bytes = try _row.marshalRow(eng.allocator, &tbl.ps, values, &buf);
        prow.bytes = try arloc.dupe(u8, bytes);
    }

    // Phase 1 (PK tables): reject duplicates, then lock every PK index
    // key — all before any write. Sorted order gives deadlock-free
    // multi-row locking AND an adjacency check for an in-statement
    // duplicate. lockRow may Restart (bump) / Conflict / Deadlock; on
    // Restart the driver re-runs from Phase 0 with nothing buffered.
    if (tbl.primary_index) |pk| {
        const row_order = try arloc.alloc(usize, num_rows);
        for (row_order, 0..) |*o, j| o.* = j;
        std.mem.sort(usize, row_order, rows, Row.lessByPkey);

        for (1..num_rows) |j| {
            if (std.mem.eql(u8, rows[row_order[j - 1]].pkey, rows[row_order[j]].pkey))
                return replyErrorCode(state, pgwire.sqlstate.unique_violation, "duplicate key value violates unique constraint");
        }
        for (row_order) |j| tx.lockRow(pk.index_table_id, rows[j].pkey) catch |err|
            return replyLockErr(state, err);
        // Post-lock, read_ts covers the key's newest commit: a live
        // index entry means the PK value already exists.
        for (row_order) |j| {
            const found = tx.lookup(.{
                .namespace = ns,
                .table_id = pk.index_table_id,
                .primary = rows[j].pkey,
                .timestamp = tx.read_ts,
            }) catch |err| {
                log.err("insert into '{s}': pk probe: {s}", .{ ins.table, @errorName(err) });
                return replyError(state, "insert failed");
            };
            if (found) |f| {
                f.release();
                return replyErrorCode(state, pgwire.sqlstate.unique_violation, "duplicate key value violates unique constraint");
            }
        }
    }

    // Phase 2: every lock is held — allocate egids and buffer each row
    // and its index entries. No Restart can occur here (puts don't
    // bump). The whole VALUES list commits atomically as one .data
    // entry; an implicit txn waits for apply so an immediate SELECT
    // on this session sees the rows.
    for (rows) |prow| {
        const egid = self.srv.allocEgid(ns, table_id) catch |err| switch (err) {
            error.NotLeader => return replyError(state, "not the leader"),
            else => {
                log.err("insert into '{s}': alloc egid: {s}", .{ ins.table, @errorName(err) });
                return replyError(state, "row id allocation failed");
            },
        };
        const ekey = _key.encodeEgid(egid);
        try tx.put(table_id, &ekey, prow.bytes, .{});
        // The index entry's value is the row key: the same 8 BE bytes.
        if (tbl.primary_index) |pk|
            try tx.put(pk.index_table_id, prow.pkey, &ekey, .{});
        // Non-unique indexes: commuting posting deltas, NO locks —
        // the row lock (here, a fresh egid) serializes add/remove
        // of one egid (docs/transactions.md, lock classes).
        for (bitmap_idxs, prow.bitmap_keys) |ix, posting_key|
            try tx.putPosting(ix.index_table_id, posting_key, egid, .add);
    }

    if (tx.implicit) {
        if (!try self.commitOrReply(state, arloc, tx, "insert", ins.table)) return;
    }

    try state.sendCommandComplete(try std.fmt.allocPrint(arloc, "INSERT 0 {d}", .{ins.rows.len}));
}

pub fn select(self: *Handler, state: *pgwire.ConnState, sel: _parse.Select) !void {
    const eng = self.srv.engine;
    const ns = try self.sessionNamespace(state) orelse return;
    const table_id = eng.forest.lookupTable(ns, sel.table) orelse
        return replyErrorCode(state, pgwire.sqlstate.undefined_table, "table does not exist");

    var arena = std.heap.ArenaAllocator.init(eng.allocator);
    defer arena.deinit();
    const arloc = arena.allocator();

    const tx = (try self.dmlTxn(state, ns, table_id)) orelse return;

    // Open AFTER the intent locks — see insert's comment.
    const tbl = self.srv.openTable(ns, table_id) catch |err| {
        log.err("select from '{s}': load schema: {s}", .{ sel.table, @errorName(err) });
        return replyError(state, "failed to load table schema");
    };

    // Resolve outputs: `*` is the user columns; by name, egid resolves
    // like any other column (Table.column).
    var cols: std.ArrayList(*const _table.Column) = .empty;
    switch (sel.projection) {
        .star => for (tbl.columns) |*c| try cols.append(arloc, c),
        .columns => |names| for (names) |nm| {
            const c = tbl.column(nm) orelse
                return replyErrorCode(
                    state,
                    pgwire.sqlstate.undefined_column,
                    "column does not exist",
                );
            try cols.append(arloc, c);
        },
    }

    const fields = try arloc.alloc(pgwire.FieldDescription, cols.items.len);
    for (cols.items, fields) |c, *fd| fd.* = .{
        .name = c.name,
        .type_oid = _value.pgTypeOid(c.ty),
        .format_code = 0,
    };
    var emitter = RowEmitter{
        .state = state,
        .tbl = tbl,
        .outs = cols.items,
    };

    // One loop for both kinds of plan. Plain SELECT streams each fill
    // to the wire; under FOR UPDATE the root's first `next` gathers
    // every match (values included) and locks every one — none
    // committed above read_ts (that lockRow would have bumped →
    // restarted), so the rows it then serves are the newest versions,
    // frozen by our locks.
    var plan = (try planFor(state, arloc, tbl, sel.where, .{
        .limit = sel.limit,
    })) orelse return;
    defer plan.deinit();
    var ctx = ExecCtx{
        .arloc = arloc,
        .tx = tx,
        .tbl = tbl,
        .table_id = table_id,
        .lock = sel.for_update,
    };
    const batch = try self.batch();
    // The row description goes out after the first fill — after
    // whatever the root had to do to produce it. For a locking plan
    // that is the whole gather and every lock, so nothing reaches the
    // wire before the last lockRow and a Restart re-runs a statement
    // that has sent nothing. An empty result still gets its description.
    var first = true;
    while (true) {
        // An executor error is a read failure — replied; an emit
        // error is a wire error — surfaced to the serve loop.
        const more = plan.root.next(&ctx, batch) catch |err|
            return replyRunErr(state, "select from", sel.table, err);
        if (first) {
            try state.sendRowDescription(fields);
            first = false;
        }
        if (!more) break;
        var it = batch.selected();
        while (it.next()) |row| try emitter.emit(row);
    }

    try state.sendCommandComplete(try std.fmt.allocPrint(arloc, "SELECT {d}", .{emitter.count}));
}

pub fn delete(self: *Handler, state: *pgwire.ConnState, del: _parse.Delete) !void {
    const eng = self.srv.engine;

    const ns = try self.sessionNamespace(state) orelse return;
    const table_id = eng.forest.lookupTable(ns, del.table) orelse
        return replyErrorCode(
            state,
            pgwire.sqlstate.undefined_table,
            "table does not exist",
        );
    var arena = std.heap.ArenaAllocator.init(eng.allocator);
    defer arena.deinit();
    const arloc = arena.allocator();

    const tx = (try self.dmlTxn(state, ns, table_id)) orelse return;

    // Open AFTER the intent locks — see insert's comment.
    const tbl = self.srv.openTable(ns, table_id) catch |err| {
        logDeleteErr(del.table, "load schema", err);
        return replyError(state, "failed to load table schema");
    };

    // Plan the WHERE: an egid or indexed term drives point reads,
    // anything else scans; no WHERE → tombstone every live row.
    // Materialize gathers the matches and locks every one; then they
    // are tombstoned from the gathered rows.
    const posting_idx = try bitmapIndexes(arloc, tbl);
    var plan = (try planFor(state, arloc, tbl, del.where, .{
        // Indexed tables keep the old bytes so every index entry —
        // the PK's (PK value → egid) and each posting's remove —
        // can be derived from the row being tombstoned.
        .keep_value = tbl.indexes.len > 0,
    })) orelse return;
    defer plan.deinit();

    var ctx = ExecCtx{
        .arloc = arloc,
        .tx = tx,
        .tbl = tbl,
        .table_id = table_id,
        .lock = true,
    };
    const batch = try self.batch();
    const mat = &plan.root.materialize;
    const rows = mat.gatherAndLockRows(&ctx, batch) catch |err|
        return replyRunErr(state, "delete from", del.table, err);

    // Lock and tombstone each gathered row's PK index entry BEFORE the
    // row tombstones: the index-key lock serializes against a concurrent
    // re-insert of the same key (whose fresh entry a late tombstone would
    // otherwise shadow). Locking may Restart — safe, nothing buffered yet.
    if (tbl.primary_index) |pk| {
        const index_keys = try arloc.alloc([]const u8, rows.len);

        for (rows, index_keys) |item, *ik| {
            const pkv = tbl.parseCol(item, pk.column);
            ik.* = _key.encodeIndexKey(arloc, pkv, null) catch |err|
                switch (err) {
                    error.OutOfMemory => return err,
                    error.Unencodable => {
                        logDeleteErr(del.table, "encode pk", err);
                        return replyError(state, "delete failed");
                    },
                };
        }
        // The item↔key pairing is not needed past this point, so sort the
        // keys themselves (lock order = the deadlock-avoidance order).
        std.mem.sort([]const u8, index_keys, {}, lessThanBytes);
        for (index_keys) |ik| {
            tx.lockRow(pk.index_table_id, ik) catch |err|
                return replyLockErr(state, err);
        }
        for (index_keys) |ik|
            try tx.put(pk.index_table_id, ik, "", .{ .kind = .deleted });
    }

    // Non-unique indexes: remove each row's egid from its postings
    // — commuting deltas, NO locks (docs/transactions.md, lock
    // classes). All locking is done (gatherAndLockRows' row locks, the PK
    // block above), so no Restart can strand these.
    for (rows) |item| {
        const egid = _key.decodeEgid(item.key.primary).?;
        for (posting_idx) |ix| {
            const cell = tbl.parseCol(item, ix.column);
            const old_key = _key.encodeIndexKey(arloc, cell, null) catch |err|
                switch (err) {
                    error.Unencodable => unreachable,
                    else => return err,
                };
            try tx.putPosting(ix.index_table_id, old_key, egid, .remove);
        }
    }

    // Past gatherAndLockRows' locks no candidate committed above read_ts —
    // the gathered rows are the newest versions, frozen by our locks,
    // so they are tombstoned directly with no re-read.
    for (rows) |item|
        try tx.put(table_id, item.key.primary, "", .{ .kind = .deleted });
    const n = rows.len;
    if (n > 0 and tx.implicit) {
        if (!try self.commitOrReply(state, arloc, tx, "delete", del.table)) return;
    }
    const tag = try std.fmt.allocPrint(arloc, "DELETE {d}", .{n});
    try state.sendCommandComplete(tag);
}

pub fn update(self: *Handler, state: *pgwire.ConnState, up: _parse.Update) !void {
    const eng = self.srv.engine;

    const ns = try self.sessionNamespace(state) orelse return;
    const table_id = eng.forest.lookupTable(ns, up.table) orelse
        return replyErrorCode(
            state,
            pgwire.sqlstate.undefined_table,
            "table does not exist",
        );
    var arena = std.heap.ArenaAllocator.init(eng.allocator);
    defer arena.deinit();
    const arloc = arena.allocator();

    const tx = (try self.dmlTxn(state, ns, table_id)) orelse return;

    // Open AFTER the intent locks — see insert's comment.
    const tbl = self.srv.openTable(ns, table_id) catch |err| {
        log.err("update '{s}': load schema: {s}", .{ up.table, @errorName(err) });
        return replyError(state, "failed to load table schema");
    };
    const posting_idx = try bitmapIndexes(arloc, tbl);

    // A SET on an indexed column moves each matched row's egid
    // from its old posting to a statement-constant new one (the
    // SET value is one literal for every row): the index (whose
    // column reads the old key out of the pre-image) plus the
    // encoded new key. Parse rejects duplicate SET columns, so
    // one entry per column.
    const MovedCol = struct {
        index: *const _table.Index,
        new_key: String,
    };
    var moved_cols: std.ArrayList(MovedCol) = .empty;

    // sets is used to create the updated row. It tells us which cols have
    // been updated and what their new value is.
    const set_cols = try arloc.alloc(SetVal, up.sets.len);
    for (up.sets, set_cols) |clause, *sv| {
        const col = tbl.column(clause.column) orelse
            return replyErrorCode(
                state,
                pgwire.sqlstate.undefined_column,
                "column does not exist",
            );
        if (col.isEgid())
            return replyErrorCode(
                state,
                pgwire.sqlstate.feature_not_supported,
                "egid cannot be updated",
            );
        // A changed PK would move the row's index entry; not supported
        // this arc (the index stays valid only while the key is fixed).
        if (col.pkey_position != null)
            return replyErrorCode(
                state,
                pgwire.sqlstate.feature_not_supported,
                "PRIMARY KEY column cannot be updated",
            );
        const val =
            (try coerceLiteral(state, arloc, clause.literal, col.ty)) orelse
            return;
        sv.* = .{ .pos = col.pos, .val = val };

        for (posting_idx) |ix| {
            if (ix.column != col) continue;
            try moved_cols.append(arloc, .{
                .index = ix,
                .new_key = _key.encodeIndexKey(arloc, val, null) catch |err|
                    switch (err) {
                        error.Unencodable => unreachable,
                        else => return err,
                    },
            });
        }
    }

    // Plan the WHERE: an egid or indexed term drives point reads,
    // anything else scans; no WHERE → rewrite every live row.
    // Materialize gathers each match's PRE-image and locks every
    // one; the rewrite (and any posting moves, whose old keys come
    // from that pre-image) happens post-lock.
    var plan = (try planFor(state, arloc, tbl, up.where, .{})) orelse
        return;
    defer plan.deinit();
    var ctx = ExecCtx{
        .arloc = arloc,
        .tx = tx,
        .tbl = tbl,
        .table_id = table_id,
        .lock = true,
    };
    const batch = try self.batch();
    const mat = &plan.root.materialize;
    const rows = mat.gatherAndLockRows(&ctx, batch) catch |err|
        return replyRunErr(state, "update", up.table, err);
    // Past gatherAndLockRows' locks no candidate committed above read_ts —
    // the gathered pre-images are the newest versions, frozen by our
    // locks. Move each row's egid between postings when an indexed
    // column changed (equal keys move nothing — commuting deltas,
    // NO locks per docs/transactions.md; all locking is done, so
    // no Restart can strand these), then rewrite and buffer the
    // row.
    var dec: _value.Row = .{};
    // Reused across items: marshalRow resets it, and tx.put
    // copies the bytes into the txn arena before returning.
    var rbuf: std.ArrayList(u8) = .empty;
    for (rows) |item| {
        const egid = _key.decodeEgid(item.key.primary).?;
        for (moved_cols.items) |mv| {
            const cell = tbl.parseCol(item, mv.index.column);
            const old_key = _key.encodeIndexKey(arloc, cell, null) catch |err|
                switch (err) {
                    error.Unencodable => unreachable,
                    else => return err,
                };
            // If old_key and new_key match, there's no change. Skip.
            if (std.mem.eql(u8, old_key, mv.new_key)) continue;

            try tx.putPosting(mv.index.index_table_id, //
                old_key, egid, .remove);
            try tx.putPosting(mv.index.index_table_id, //
                mv.new_key, egid, .add);
        }
        const rewritten = try rewriteRowBytes(arloc, //
            &tbl.ps, set_cols, &dec, item.value, &rbuf);
        try tx.put(table_id, item.key.primary, rewritten, .{});
    }
    const n = rows.len;

    if (tx.implicit) {
        if (!try self.commitOrReply(state, arloc, tx, "update", up.table)) return;
    }
    try state.sendCommandComplete(try std.fmt.allocPrint(arloc, "UPDATE {d}", .{n}));
}

/// Turns each row into a projected DataRow on the wire — plain
/// SELECT's sink per batch, and driven over a FOR UPDATE capture
/// post-lock. Columns format straight into the frame (pgwire
/// DataRowWriter), which the connection's buffered writer batches to
/// the socket.
pub const RowEmitter = struct {
    state: *pgwire.ConnState,
    tbl: *const _table.Table,
    /// The projection — egid is a column too (table.egid_column).
    outs: []const *const _table.Column,
    count: u64 = 0,

    /// Project and send one row — each output cell decoded on its own
    /// (O(1) each, Table.read), no full-row decode.
    pub fn emit(self: *RowEmitter, item: zdb.kv.Item) !void {
        var dw = try self.state.beginDataRow(@intCast(self.outs.len));
        for (self.outs) |col| {
            const v = self.tbl.parseCol(item, col);
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

/// Bind the WHERE (null: every row) and plan it. null = bind already
/// sent the error reply (the pred.bind convention).
fn planFor(
    state: *pgwire.ConnState,
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    where: ?_parse.Where,
    opts: _plan.Options,
) !?_plan.Plan {
    var pred: ?*const Pred = null;
    if (where) |tree| {
        pred = (try _pred.bind(state, arloc, tbl, tree)) orelse return null;
    }
    return try _plan.plan(arloc, tbl, pred, opts);
}

/// litToValue plus the client-facing error mapping shared by INSERT
/// and UPDATE. null = the error reply was already sent (the
/// pred.bind convention).
fn coerceLiteral(
    state: *pgwire.ConnState,
    a: std.mem.Allocator,
    lit: _parse.Literal,
    ty: _value.Type,
) !?_value.Value {
    return litToValue(a, lit, ty) catch |err| switch (err) {
        error.OutOfMemory => return err,
        error.UnsupportedLiteral, error.UnsupportedType => {
            try replyErrorCode(
                state,
                pgwire.sqlstate.feature_not_supported,
                "unsupported value or column type",
            );
            return null;
        },
        else => {
            try replyErrorCode(
                state,
                pgwire.sqlstate.invalid_text_representation,
                "invalid value for column type",
            );
            return null;
        },
    };
}

/// Order row indices by their encoded PK index key (memcomparable, so byte
/// order is value order) — used to lock a multi-row INSERT deadlock-free and
/// to spot an in-statement duplicate as an adjacent pair.
fn lessThanBytes(_: void, x: []const u8, y: []const u8) bool {
    return std.mem.lessThan(u8, x, y);
}

/// The indexes maintained as roaring postings — every non-unique index
/// of `tbl`, at most one per column (CREATE INDEX rejects seconds). Their
/// entries are commuting deltas (Txn.putPosting); unique indexes (the
/// pkey) keep their single-egid lock-then-check path. Pointers into
/// tbl.indexes, in a stable order callers zip parallel slices against.
fn bitmapIndexes(
    a: std.mem.Allocator,
    tbl: *const _table.Table,
) ![]const *const _table.Index {
    var list: std.ArrayList(*const _table.Index) = .empty;
    for (tbl.indexes) |*idx| {
        if (idx.unique) continue;
        try list.append(a, idx);
    }
    return list.toOwnedSlice(a);
}

fn logDeleteErr(table_name: []const u8, what: []const u8, err: anyerror) void {
    log.err(
        "delete from '{s}': {s}: {s}",
        .{ table_name, what, @errorName(err) },
    );
}

/// Map an executor failure (root.next / gatherAndLockRows) onto the wire:
/// row-lock errors (a locking plan locks the matches) ride
/// replyLockErr — Restart propagates to the statement driver — and
/// anything else is an internal read failure.
fn replyRunErr(
    state: *pgwire.ConnState,
    comptime verb: []const u8,
    table_name: []const u8,
    err: anyerror,
) !void {
    return switch (err) {
        error.Restart,
        error.Conflict,
        error.Deadlock,
        error.Upgrade,
        => replyLockErr(state, err),
        else => {
            log.err(
                verb ++ " '{s}': scan: {s}",
                .{ table_name, @errorName(err) },
            );
            return replyError(state, "scan failed");
        },
    };
}
