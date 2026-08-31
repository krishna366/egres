//! The four DML statement bodies — INSERT, SELECT, DELETE, UPDATE.
//! Each resolves its table and WHERE, drives a Scanner for the rows it
//! acts on (scanner.zig), maintains the PK index entry and any posting
//! deltas, and buffers the result into the statement's txn. The restart
//! loop that re-runs these on a read_ts bump lives in server.runDml.
//!
//! Free functions over `*Handler` — see the note in ddl.zig.

const std = @import("std");
const engine = @import("engine");
const pgwire = @import("pgwire");
const zdb = @import("zdb");

const parse = @import("parse.zig");
const key = @import("key.zig");
const row = @import("row.zig");
const scanner = @import("scanner.zig");
const schema = @import("schema.zig");
const table = @import("table.zig");
const txn = @import("txn.zig");
const value = @import("value.zig");
const server = @import("server.zig");

const Handler = server.Handler;
const RowEmitter = scanner.RowEmitter;
const Scanner = scanner.Scanner;
const SelectOut = scanner.SelectOut;
const SetVal = scanner.SetVal;
const String = value.String;
const litToValue = scanner.litToValue;
const resolveFilter = scanner.resolveFilter;
const rewriteRowBytes = scanner.rewriteRowBytes;
const log = std.log.scoped(.sql);
const replyError = server.replyError;
const replyErrorCode = server.replyErrorCode;
const replyLockErr = server.replyLockErr;

pub fn insert(self: *Handler, state: *pgwire.ConnState, ins: parse.Insert) !void {
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
    const t = self.srv.openTable(ns, table_id) catch |err| {
        log.err("insert into '{s}': load schema: {s}", .{ ins.table, @errorName(err) });
        return replyError(state, "failed to load table schema");
    };

    // Map each VALUES position → the column it writes: `col` carries the
    // name and coercion type, `slot` is that column's cell in `values`
    // (parallel to t.columns, == ps order). Positional insert maps 1:1
    // to the live columns; an explicit list resolves names. Inserting an
    // explicit `egid` isn't supported yet.
    // TODO: allow user-provided egids once unique-index enforcement exists
    // — a below-counter egid needs a data-tree existence check, and
    // concurrent inserts of the same egid need apply-time validation.
    const values = try arloc.alloc(value.Value, t.columns.len);
    const Target = struct {
        col: *const table.Column,
        dst: *value.Value,
    };
    const targets: []Target = blk: {
        if (ins.columns) |cols| {
            const list = try arloc.alloc(Target, cols.len);
            for (cols, list) |name, *target| {
                if (std.ascii.eqlIgnoreCase(name, "egid"))
                    return replyErrorCode(state, pgwire.sqlstate.feature_not_supported, "inserting an explicit egid is not supported yet");
                const ci = t.columnIndex(name) orelse
                    return replyErrorCode(state, pgwire.sqlstate.undefined_column, "column does not exist");
                target.* = .{ .col = &t.columns[ci], .dst = &values[ci] };
            }
            break :blk list;
        }
        const list = try arloc.alloc(Target, t.columns.len);
        for (list, t.columns, values) |*dst, *col, *slot|
            dst.* = .{ .col = col, .dst = slot };
        break :blk list;
    };

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(eng.allocator);

    // One VALUES row, fully derived in Phase 0 — parse + marshal +
    // encode every key BEFORE any lock or write: a Restart re-runs
    // the whole statement, so nothing may be buffered until every
    // PK lock is held (§Phase 1). `values` is reused per row, so
    // anything derived from a row's cells is captured here.
    const PendingRow = struct {
        bytes: String = "", // marshaled row value
        pkey: String = "", // PK index key, when t.pkey != null
        /// Per idx_cols entry (same order): the index key this
        /// row lands under. Always present — NULL cells key on
        /// the NULL tag byte, and unencodable column types
        /// (interval) never carry an index (rejected at CREATE
        /// INDEX).
        postings: []String = &.{},

        fn lessByPkey(rows: []@This(), x: usize, y: usize) bool {
            return std.mem.lessThan(u8, rows[x].pkey, rows[y].pkey);
        }
    };
    const num_rows = ins.rows.len;
    const idx_cols = try indexCols(arloc, t);
    const pending = try arloc.alloc(PendingRow, num_rows);
    for (ins.rows, pending) |row_lits, *pr| {
        pr.* = .{};
        if (row_lits.len != targets.len)
            return replyErrorCode(state, pgwire.sqlstate.syntax_error, "INSERT has the wrong number of values");

        for (values) |*v| v.* = .null_value; // reset all to null

        for (row_lits, targets) |lit, target| {
            target.dst.* =
                (try coerceLiteral(state, arloc, lit, target.col.ty)) orelse
                return;
        }

        if (t.pkey) |pk| {
            const pkv = values[pk.column_index];
            if (pkv == .null_value)
                return replyErrorCode(
                    state,
                    pgwire.sqlstate.not_null_violation,
                    "null value in primary key column violates not-null constraint",
                );
            pr.pkey = key.encodeIndexKey(arloc, pkv, null) catch |err|
                switch (err) {
                    error.OutOfMemory => return err,
                    error.Unencodable => return replyErrorCode(
                        state,
                        pgwire.sqlstate.feature_not_supported,
                        "PRIMARY KEY on this column type is not supported",
                    ),
                };
        }
        if (idx_cols.count() > 0) {
            pr.postings = try arloc.alloc(String, idx_cols.count());
            for (idx_cols.keys(), pr.postings) |col_idx, *posting_key| {
                posting_key.* = key.encodeIndexKey(
                    arloc,
                    values[col_idx],
                    null,
                ) catch |err| switch (err) {
                    error.Unencodable => unreachable,
                    else => return err,
                };
            }
        }
        // marshalRow reuses `buf`; dupe out before the next row clobbers it.
        const bytes = try row.marshalRow(eng.allocator, &t.ps, values, &buf);
        pr.bytes = try arloc.dupe(u8, bytes);
    }

    // Phase 1 (PK tables): reject duplicates, then lock every PK index
    // key — all before any write. Sorted order gives deadlock-free
    // multi-row locking AND an adjacency check for an in-statement
    // duplicate. lockRow may Restart (bump) / Conflict / Deadlock; on
    // Restart the driver re-runs from Phase 0 with nothing buffered.
    if (t.pkey) |pk| {
        const order = try arloc.alloc(usize, num_rows);
        for (order, 0..) |*o, j| o.* = j;
        std.mem.sort(usize, order, pending, PendingRow.lessByPkey);

        for (1..num_rows) |j| {
            if (std.mem.eql(u8, pending[order[j - 1]].pkey, pending[order[j]].pkey))
                return replyErrorCode(state, pgwire.sqlstate.unique_violation, "duplicate key value violates unique constraint");
        }
        for (order) |j| tx.lockRow(pk.index_table_id, pending[j].pkey) catch |err|
            return replyLockErr(state, err);
        // Post-lock, read_ts covers the key's newest commit: a live
        // index entry means the PK value already exists.
        for (order) |j| {
            const found = tx.lookup(.{
                .namespace = ns,
                .table_id = pk.index_table_id,
                .primary = pending[j].pkey,
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
    for (pending) |pr| {
        const egid = self.srv.allocEgid(ns, table_id) catch |err| switch (err) {
            error.NotLeader => return replyError(state, "not the leader"),
            else => {
                log.err("insert into '{s}': alloc egid: {s}", .{ ins.table, @errorName(err) });
                return replyError(state, "row id allocation failed");
            },
        };
        const ekey = key.encodeEgid(egid);
        try tx.put(table_id, &ekey, pr.bytes, .{});
        // The index entry's value is the row key: the same 8 BE bytes.
        if (t.pkey) |pk|
            try tx.put(pk.index_table_id, pr.pkey, &ekey, .{});
        // Non-unique indexes: commuting posting deltas, NO locks —
        // the row lock (here, a fresh egid) serializes add/remove
        // of one egid (docs/transactions.md, lock classes).
        for (idx_cols.values(), pr.postings) |index_tid, posting_key|
            try tx.putPosting(index_tid, posting_key, egid, .add);
    }

    if (tx.implicit) {
        if (!try self.commitOrReply(state, arloc, tx, "insert", ins.table)) return;
    }

    try state.sendCommandComplete(try std.fmt.allocPrint(arloc, "INSERT 0 {d}", .{ins.rows.len}));
}

pub fn select(self: *Handler, state: *pgwire.ConnState, sel: parse.Select) !void {
    const eng = self.srv.engine;
    const ns = try self.sessionNamespace(state) orelse return;
    const table_id = eng.forest.lookupTable(ns, sel.table) orelse
        return replyErrorCode(state, pgwire.sqlstate.undefined_table, "table does not exist");
    var arena = std.heap.ArenaAllocator.init(eng.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const tx = (try self.dmlTxn(state, ns, table_id)) orelse return;

    // Open AFTER the intent locks — see insert's comment.
    const t = self.srv.openTable(ns, table_id) catch |err| {
        log.err("select from '{s}': load schema: {s}", .{ sel.table, @errorName(err) });
        return replyError(state, "failed to load table schema");
    };

    // Resolve outputs: `egid` (from the key) or a payload column (its index
    // into ps.columns == the empty-projection decode order).
    var outs: std.ArrayList(SelectOut) = .empty;
    switch (sel.projection) {
        .star => for (t.columns) |c| {
            try outs.append(a, .{ .name = c.name, .ty = c.ty, .source = .{ .cell = t.ps.index(c.column_id) } });
        },
        .columns => |names| for (names) |nm| {
            if (std.ascii.eqlIgnoreCase(nm, "egid")) {
                try outs.append(a, .{ .name = "egid", .ty = .int8, .source = .egid });
            } else if (t.column(nm)) |c| {
                try outs.append(a, .{ .name = c.name, .ty = c.ty, .source = .{ .cell = t.ps.index(c.column_id) } });
            } else return replyErrorCode(state, pgwire.sqlstate.undefined_column, "column does not exist");
        },
    }

    const fields = try a.alloc(pgwire.FieldDescription, outs.items.len);
    for (outs.items, fields) |o, *fd| fd.* = .{ .name = o.name, .type_oid = value.pgTypeOid(o.ty), .format_code = 0 };
    // FOR UPDATE sends NOTHING until every lock is held: lockRow may
    // Restart the statement, and bytes already on the wire (the row
    // description, emitted rows) would repeat on the re-run.
    if (!sel.for_update) try state.sendRowDescription(fields);

    var emitter = RowEmitter{
        .state = state,
        .ps = &t.ps,
        .outs = outs.items,
        .stmt_alloc = a,
    };
    defer emitter.deinit();

    // Plain SELECT streams matches to the wire as run() visits
    // them; FOR UPDATE captures them (values included) to lock
    // every one BEFORE the first byte is sent — the UPDATE/DELETE
    // lock-all shape with "act" = emission.
    var sc = Scanner{
        .a = a,
        .ps = &t.ps,
        .mode = if (sel.for_update)
            .copy_value
        else
            .{ .emit_value = &emitter },
        .lock = sel.for_update,
    };
    if (sel.filter) |f| {
        switch ((try resolveFilter(state, a, t, f)) orelse return) {
            .egids => |es| sc.source = .{ .egids = es },
            .index_eq => |ix| sc.source = .{ .index_eq = ix },
            .index_bitmap => |ix| sc.source = .{ .index_bitmap = ix },
            .cell => |c| sc.cell_filter = c,
        }
    }
    sc.run(eng, ns, table_id, tx) catch |err|
        return replyRunErr(state, "select from", sel.table, err);
    if (sc.err) |err| {
        // Streaming: an emit error is a wire error — surface it
        // to the serve loop. Capture: arena OOM — reply.
        if (!sel.for_update) return err;
        log.err("select from '{s}': collect: {s}", .{
            sel.table, @errorName(err),
        });
        return replyError(state, "select failed");
    }
    if (sel.for_update) {
        // run() returned with every match locked: none committed
        // above read_ts (that lockRow would have bumped →
        // restarted), so the captured values are the newest
        // versions, frozen by our locks — emit them now.
        try state.sendRowDescription(fields);
        for (sc.items.items) |item| try emitter.emit(item);
    }

    try state.sendCommandComplete(try std.fmt.allocPrint(a, "SELECT {d}", .{emitter.count}));
}

pub fn delete(self: *Handler, state: *pgwire.ConnState, del: parse.Delete) !void {
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

    // Resolve WHERE: `egid = N` → a point lookup; a payload column
    // → scan + compare; no WHERE → tombstone every live row. Either
    // way run() captures the matches; every one is locked, then
    // tombstoned from the capture.
    const idx_cols = try indexCols(arloc, tbl);
    var sc = Scanner{
        .a = arloc,
        .ps = &tbl.ps,
        // Indexed tables keep the old bytes so every index entry —
        // the PK's (PK value → egid) and each posting's remove —
        // can be derived from the row being tombstoned.
        .mode = if (tbl.pkey != null or idx_cols.count() > 0)
            .copy_value
        else
            .no_value,
        .lock = true,
    };
    if (del.filter) |f| {
        switch ((try resolveFilter(state, arloc, tbl, f)) orelse return) {
            .egids => |es| sc.source = .{ .egids = es },
            .index_eq => |ix| sc.source = .{ .index_eq = ix },
            .index_bitmap => |ix| sc.source = .{ .index_bitmap = ix },
            .cell => |c| sc.cell_filter = c,
        }
    }
    sc.run(eng, ns, table_id, tx) catch |err|
        return replyRunErr(state, "delete from", del.table, err);
    if (sc.err) |err| {
        logDeleteErr(del.table, "collect", err);
        return replyError(state, "delete failed");
    }

    // Lock and tombstone each captured row's PK index entry BEFORE the
    // row tombstones: the index-key lock serializes against a concurrent
    // re-insert of the same key (whose fresh entry a late tombstone would
    // otherwise shadow). Locking may Restart — safe, nothing buffered yet.
    if (tbl.pkey) |pk| {
        const items = sc.items.items;
        const index_keys = try arloc.alloc([]const u8, items.len);

        for (items, index_keys) |item, *ik| {
            const pkv = row.readCell(&tbl.ps, item.value, pk.column_index);
            ik.* = key.encodeIndexKey(arloc, pkv, null) catch |err|
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
    // classes). All locking is done (run()'s row locks, the PK
    // block above), so no Restart can strand these.
    for (sc.items.items) |item| {
        const egid = key.decodeEgid(item.key.primary).?;
        for (idx_cols.keys(), idx_cols.values()) |col_idx, index_tid| {
            const cell = row.readCell(&tbl.ps, item.value, col_idx);
            const old_key = key.encodeIndexKey(arloc, cell, null) catch |err|
                switch (err) {
                    error.Unencodable => unreachable,
                    else => return err,
                };
            try tx.putPosting(index_tid, old_key, egid, .remove);
        }
    }

    // Past run()'s locks no candidate committed above read_ts — the
    // captured rows are the newest versions, frozen by our locks, so
    // they are tombstoned directly with no re-read.
    for (sc.items.items) |item|
        try tx.put(table_id, item.key.primary, "", .{ .kind = .deleted });
    const n = sc.items.items.len;
    if (n > 0 and tx.implicit) {
        if (!try self.commitOrReply(state, arloc, tx, "delete", del.table)) return;
    }
    const tag = try std.fmt.allocPrint(arloc, "DELETE {d}", .{n});
    try state.sendCommandComplete(tag);
}

pub fn update(self: *Handler, state: *pgwire.ConnState, up: parse.Update) !void {
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
    const t = self.srv.openTable(ns, table_id) catch |err| {
        log.err("update '{s}': load schema: {s}", .{ up.table, @errorName(err) });
        return replyError(state, "failed to load table schema");
    };
    const idx_cols = try indexCols(arloc, t);

    // A SET on an indexed column moves each matched row's egid
    // from its old posting to a statement-constant new one (the
    // SET value is one literal for every row): the encoded new
    // key plus where the old one comes from (col_idx into the
    // pre-image). Parse rejects duplicate SET columns, so one
    // entry per column.
    const MovedCol = struct {
        table_id: u32,
        col_idx: usize,
        new_key: String,
    };
    var moved_cols: std.ArrayList(MovedCol) = .empty;

    // sets is used to create the updated row. It tells us which cols have
    // been updated and what their new value is.
    const set_cols = try arloc.alloc(SetVal, up.sets.len);
    for (up.sets, set_cols) |clause, *sv| {
        if (std.ascii.eqlIgnoreCase(clause.column, "egid"))
            return replyErrorCode(
                state,
                pgwire.sqlstate.feature_not_supported,
                "egid cannot be updated",
            );
        const col_idx = t.columnIndex(clause.column) orelse
            return replyErrorCode(
                state,
                pgwire.sqlstate.undefined_column,
                "column does not exist",
            );
        // A changed PK would move the row's index entry; not supported
        // this arc (the index stays valid only while the key is fixed).
        if (t.columns[col_idx].pkey_position != null)
            return replyErrorCode(
                state,
                pgwire.sqlstate.feature_not_supported,
                "PRIMARY KEY column cannot be updated",
            );
        const col = t.columns[col_idx];
        const val =
            (try coerceLiteral(state, arloc, clause.literal, col.ty)) orelse
            return;
        sv.* = .{
            .idx = t.ps.index(col.column_id),
            .val = val,
        };

        if (idx_cols.get(sv.idx)) |index_tid| {
            try moved_cols.append(arloc, .{
                .table_id = index_tid,
                .col_idx = sv.idx,
                .new_key = key.encodeIndexKey(arloc, sv.val, null) catch |err|
                    switch (err) {
                        error.Unencodable => unreachable,
                        else => return err,
                    },
            });
        }
    }

    // Resolve WHERE: `egid = N` → a point lookup; a payload column
    // → scan + compare; no WHERE → rewrite every live row. run()
    // captures each match's PRE-image; the rewrite (and any
    // posting moves, whose old keys come from that pre-image)
    // happens post-lock.
    var sc = Scanner{
        .a = arloc,
        .ps = &t.ps,
        .mode = .copy_value,
        .lock = true,
    };
    if (up.filter) |f| {
        switch ((try resolveFilter(state, arloc, t, f)) orelse return) {
            .egids => |es| sc.source = .{ .egids = es },
            .index_eq => |ix| sc.source = .{ .index_eq = ix },
            .index_bitmap => |ix| sc.source = .{ .index_bitmap = ix },
            .cell => |c| sc.cell_filter = c,
        }
    }
    sc.run(eng, ns, table_id, tx) catch |err|
        return replyRunErr(state, "update", up.table, err);
    if (sc.err) |err| {
        log.err("update '{s}': collect: {s}", .{ up.table, @errorName(err) });
        return replyError(state, "update failed");
    }
    // Past run()'s locks no candidate committed above read_ts — the
    // captured pre-images are the newest versions, frozen by our
    // locks. Move each row's egid between postings when an indexed
    // column changed (equal keys move nothing — commuting deltas,
    // NO locks per docs/transactions.md; all locking is done, so
    // no Restart can strand these), then rewrite and buffer the
    // row.
    var dec: value.Row = .{};
    // Reused across items: marshalRow resets it, and tx.put
    // copies the bytes into the txn arena before returning.
    var rbuf: std.ArrayList(u8) = .empty;
    for (sc.items.items) |item| {
        const egid = key.decodeEgid(item.key.primary).?;
        for (moved_cols.items) |mv| {
            const cell = row.readCell(&t.ps, item.value, mv.col_idx);
            const old_key = key.encodeIndexKey(arloc, cell, null) catch |err|
                switch (err) {
                    error.Unencodable => unreachable,
                    else => return err,
                };
            // If old_key and new_key match, there's no change. Skip.
            if (std.mem.eql(u8, old_key, mv.new_key)) continue;

            try tx.putPosting(mv.table_id, old_key, egid, .remove);
            try tx.putPosting(mv.table_id, mv.new_key, egid, .add);
        }
        const rewritten = try rewriteRowBytes(
            arloc,
            &t.ps,
            set_cols,
            &dec,
            item.value,
            &rbuf,
        );
        try tx.put(table_id, item.key.primary, rewritten, .{});
    }
    const n = sc.items.items.len;

    if (tx.implicit) {
        if (!try self.commitOrReply(state, arloc, tx, "update", up.table)) return;
    }
    try state.sendCommandComplete(try std.fmt.allocPrint(arloc, "UPDATE {d}", .{n}));
}

/// litToValue plus the client-facing error mapping shared by INSERT
/// and UPDATE. null = the error reply was already sent (the
/// resolveFilter convention).
fn coerceLiteral(
    state: *pgwire.ConnState,
    a: std.mem.Allocator,
    lit: parse.Literal,
    ty: value.Type,
) !?value.Value {
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

/// key indexes t.columns, t.ps and a row's values alike — same
/// order): O(1) "is this column indexed?", and — ArrayHashMap —
/// stable order, so callers can zip keys()/values() against parallel
/// slices. At most one non-unique index per column (CREATE INDEX
/// rejects seconds; the assert enforces it here). These indexes carry
/// roaring postings maintained as commuting deltas (Txn.putPosting);
/// unique indexes (the pkey) keep their single-egid lock-then-check
/// path.
fn indexCols(
    a: std.mem.Allocator,
    t: *const table.Table,
) !std.AutoArrayHashMapUnmanaged(usize, u32) {
    var map: std.AutoArrayHashMapUnmanaged(usize, u32) = .empty;
    for (t.indexes) |idx| {
        if (idx.unique) continue;
        const gop = try map.getOrPut(a, t.ps.index(idx.column_id));
        std.debug.assert(!gop.found_existing);
        gop.value_ptr.* = idx.index_table_id;
    }
    return map;
}

fn logDeleteErr(table_name: []const u8, what: []const u8, err: anyerror) void {
    log.err(
        "delete from '{s}': {s}: {s}",
        .{ table_name, what, @errorName(err) },
    );
}

/// Map a Scanner.run failure onto the wire: row-lock errors (run locks
/// the matches when Scanner.lock is set) ride replyLockErr — Restart
/// propagates to the statement driver — and anything else is an
/// internal scan failure.
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
