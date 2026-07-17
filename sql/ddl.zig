//! DDL parsing: SQL text → a small typed `DdlStmt`, via libpg_query (reused
//! from pgwire). Only the statements the store can execute are recognized;
//! everything else (SELECT/INSERT/…) maps to `.unsupported`. A statement we
//! recognize but must reject (unsupported type, constraint, …) maps to
//! `.invalid` carrying the client-facing reason — parse succeeded, this is
//! a semantic rejection. Syntax errors propagate as ParseError so the
//! endpoint can answer with a 42601.

const std = @import("std");
const pgwire = @import("pgwire");
const parser = pgwire.parser_mod;
const value = @import("value.zig");

/// One parsed `name type [PRIMARY KEY]` column. `name` is owned.
pub const ColumnDef = struct {
    name: []const u8,
    ty: value.Type,
    pkey: bool = false,
};

pub const CreateTable = struct {
    name: []const u8, // table name (owned)
    if_not_exists: bool = false,
    columns: []ColumnDef, // owned (and each column name)
};

pub const DropTable = struct {
    name: []const u8, // table name (owned)
    missing_ok: bool = false, // IF EXISTS
};

/// One VALUES cell, still untyped: coerced to its column's Type at execute
/// (value.parseValue). `str`/`float` own their bytes. `unsupported` is a
/// non-literal item (DEFAULT, an expression) the executor rejects.
pub const Literal = union(enum) {
    int: i64,
    float: []const u8, // owned; libpg_query keeps floats as text
    str: []const u8, // owned
    bool_: bool,
    null_value,
    unsupported,
};

pub const Insert = struct {
    table: []const u8, // owned
    columns: ?[][]const u8, // owned target columns; null = positional (all columns)
    rows: [][]Literal, // owned rows of literals
};

/// What a SELECT projects: `*` (all live columns) or a named list (which may
/// include the `egid` pseudo-column). Names are owned.
pub const Projection = union(enum) {
    star,
    columns: [][]const u8,
};

/// A single `column = literal` filter (the only WHERE this slice supports).
pub const Filter = struct {
    column: []const u8, // owned
    literal: Literal,
};

pub const Select = struct {
    table: []const u8, // owned
    projection: Projection,
    filter: ?Filter,
};

pub const DdlStmt = union(enum) {
    create_database: []const u8, // database name (owned)
    drop_database: []const u8, // database name (owned)
    create_table: CreateTable,
    drop_table: DropTable,
    insert: Insert,
    select: Select,
    invalid: []const u8, // recognized but rejected; reason (owned)
    unsupported,

    pub fn deinit(self: *DdlStmt, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create_database, .drop_database, .invalid => |n| alloc.free(n),
            .create_table => |ct| {
                for (ct.columns) |c| alloc.free(c.name);
                alloc.free(ct.columns);
                alloc.free(ct.name);
            },
            .drop_table => |dt| alloc.free(dt.name),
            .insert => |ins| {
                alloc.free(ins.table);
                if (ins.columns) |cols| {
                    for (cols) |c| alloc.free(c);
                    alloc.free(cols);
                }
                for (ins.rows) |r| {
                    for (r) |lit| freeLit(alloc, lit);
                    alloc.free(r);
                }
                alloc.free(ins.rows);
            },
            .select => |sel| {
                alloc.free(sel.table);
                switch (sel.projection) {
                    .star => {},
                    .columns => |cols| {
                        for (cols) |c| alloc.free(c);
                        alloc.free(cols);
                    },
                }
                if (sel.filter) |f| {
                    alloc.free(f.column);
                    freeLit(alloc, f.literal);
                }
            },
            .unsupported => {},
        }
    }
};

fn freeLit(alloc: std.mem.Allocator, lit: Literal) void {
    switch (lit) {
        .str, .float => |s| alloc.free(s),
        else => {},
    }
}

pub fn parseDdl(alloc: std.mem.Allocator, sql: []const u8) parser.ParseError!DdlStmt {
    var tree = try parser.parse(alloc, sql, null);
    defer tree.deinit(alloc);

    const stmts = tree.statements() orelse return .unsupported;
    if (stmts.len == 0) return .unsupported;
    const wrapper = asObject(stmts[0]).?.get("stmt") orelse return .unsupported;
    const wrapper_obj = asObject(wrapper) orelse return .unsupported;

    // The statement node is a single-entry map keyed by its kind. CREATE and
    // DROP DATABASE are their own kinds (CreatedbStmt / DropdbStmt), both
    // carrying the name in `dbname`.
    var it = wrapper_obj.iterator();
    const entry = it.next() orelse return .unsupported;
    const kind = entry.key_ptr.*;
    const body = asObject(entry.value_ptr.*) orelse return .unsupported;

    if (std.mem.eql(u8, kind, "CreatedbStmt") or std.mem.eql(u8, kind, "DropdbStmt")) {
        const name = asString(body.get("dbname") orelse return .unsupported) orelse return .unsupported;
        const owned = try alloc.dupe(u8, name);
        return if (kind[0] == 'C')
            .{ .create_database = owned }
        else
            .{ .drop_database = owned };
    }
    if (std.mem.eql(u8, kind, "CreateStmt")) return parseCreateTable(alloc, entry.value_ptr.*);
    if (std.mem.eql(u8, kind, "DropStmt")) return parseDropTable(alloc, entry.value_ptr.*);
    if (std.mem.eql(u8, kind, "InsertStmt")) return parseInsert(alloc, entry.value_ptr.*);
    if (std.mem.eql(u8, kind, "SelectStmt")) return parseSelect(alloc, entry.value_ptr.*);
    return .unsupported;
}

// ---------------------------------------------------------------------------
// Typed mirrors of the libpg_query JSON we consume, decoded with
// std.json.parseFromValue. libpg_query encodes every AST node as a
// single-key object keyed by its kind — {"ColumnDef": {...}} — which is
// exactly std.json's tagged-union convention, and omits zero-valued/false
// fields, which land on the struct defaults (absent if_not_exists ⇒ false).
// ignore_unknown_fields skips the location/typemod/oncommit noise. The
// declarations double as the shape documentation.
// ---------------------------------------------------------------------------

/// One part of a (possibly qualified) name: `public.t` arrives as
/// [{"String": {"sval": "public"}}, {"String": {"sval": "t"}}] — the LAST
/// part is the object itself.
const NameNode = struct { String: struct { sval: []const u8 = "" } = .{} };

/// CREATE TABLE t (id int PRIMARY KEY, name text)
const CreateStmtJson = struct {
    relation: struct { relname: []const u8 = "" } = .{},
    if_not_exists: bool = false,
    tableElts: []const TableElt = &.{},

    const TableElt = union(enum) {
        ColumnDef: ColumnDefJson,
        /// Table-level constraint (PRIMARY KEY (a, b), CHECK, …) — rejected.
        Constraint: std.json.Value,
    };
    const ColumnDefJson = struct {
        colname: []const u8 = "",
        /// `int` arrives pg_catalog-qualified: names = [pg_catalog, int4].
        typeName: struct { names: []const NameNode = &.{} } = .{},
        constraints: []const ConstraintNode = &.{},
    };
    const ConstraintNode = struct {
        Constraint: struct { contype: []const u8 = "" } = .{},
    };
};

/// DROP TABLE IF EXISTS public.t
const DropStmtJson = struct {
    removeType: []const u8 = "", // OBJECT_TABLE (vs INDEX, VIEW, …)
    missing_ok: bool = false, // IF EXISTS
    objects: []const Obj = &.{}, // one per dropped table

    const Obj = struct { List: struct { items: []const NameNode = &.{} } = .{} };
};

/// INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')
const InsertStmtJson = struct {
    relation: struct { relname: []const u8 = "" } = .{},
    cols: []const ResTargetNode = &.{}, // empty = positional
    selectStmt: ?SelectWrap = null,
    onConflictClause: ?std.json.Value = null, // presence ⇒ ON CONFLICT
    returningList: []const std.json.Value = &.{}, // non-empty ⇒ RETURNING

    const ResTargetNode = struct { ResTarget: struct { name: []const u8 = "" } = .{} };
    // INSERT ... VALUES nests rows under a SelectStmt with `valuesLists`;
    // INSERT ... SELECT has a SelectStmt WITHOUT valuesLists (→ rejected).
    const SelectWrap = struct { SelectStmt: SelectValues = .{} };
    const SelectValues = struct { valuesLists: []const ListNode = &.{} };
    const ListNode = struct { List: struct { items: []const AConstNode = &.{} } = .{} };
    const AConstNode = struct { A_Const: AConst = .{} };
    // A_Const encodes its value under a type-named key; the key's presence is
    // the discriminant (libpg_query omits zero inner values, so `ival: {}`
    // still means integer 0).
    pub const AConst = struct {
        ival: ?struct { ival: i64 = 0 } = null,
        fval: ?struct { fval: []const u8 = "" } = null,
        sval: ?struct { sval: []const u8 = "" } = null,
        boolval: ?struct { boolval: bool = false } = null,
        isnull: bool = false,
    };
};

/// SELECT [cols|*] FROM t [WHERE col = const]
const SelectStmtJson = struct {
    targetList: []const ResTargetNode = &.{},
    fromClause: []const FromNode = &.{},
    whereClause: ?WhereNode = null,
    // Clauses this slice doesn't support — presence ⇒ rejection.
    sortClause: []const std.json.Value = &.{},
    groupClause: []const std.json.Value = &.{},
    distinctClause: []const std.json.Value = &.{},
    havingClause: ?std.json.Value = null,
    limitCount: ?std.json.Value = null,
    limitOffset: ?std.json.Value = null,

    const ResTargetNode = struct { ResTarget: struct { val: ColumnRefWrap = .{} } = .{} };
    // A target / WHERE operand must be a column reference; anything else
    // (function, arithmetic) leaves ColumnRef null → rejected.
    const ColumnRefWrap = struct { ColumnRef: ?ColumnRef = null };
    const ColumnRef = struct { fields: []const FieldNode = &.{} };
    const FieldNode = struct {
        String: ?struct { sval: []const u8 = "" } = null,
        A_Star: ?struct {} = null,
    };
    const FromNode = struct { RangeVar: ?struct { relname: []const u8 = "" } = null };
    const WhereNode = struct {
        A_Expr: ?AExpr = null,
        const AExpr = struct {
            kind: []const u8 = "",
            name: []const NameNode = &.{},
            lexpr: ColumnRefWrap = .{},
            rexpr: struct { A_Const: InsertStmtJson.AConst = .{} } = .{},
        };
    };
};

/// Decode one statement body into its typed mirror. A shape we don't
/// recognize (unknown node kind, wrong field type) is null → .unsupported;
/// only OOM propagates.
fn fromValue(comptime T: type, alloc: std.mem.Allocator, body: std.json.Value) parser.ParseError!?std.json.Parsed(T) {
    return std.json.parseFromValue(T, alloc, body, .{ .ignore_unknown_fields = true }) catch |err| switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => null,
    };
}

/// CREATE TABLE. Only bare `name type` columns with an optional
/// single-column PRIMARY KEY parse; any other constraint (NOT NULL, UNIQUE,
/// FK, DEFAULT, table-level) is a clean `.invalid`.
fn parseCreateTable(alloc: std.mem.Allocator, body: std.json.Value) parser.ParseError!DdlStmt {
    const parsed = try fromValue(CreateStmtJson, alloc, body) orelse return .unsupported;
    defer parsed.deinit();
    const cs = parsed.value;

    var columns: std.ArrayList(ColumnDef) = .empty;
    errdefer {
        for (columns.items) |c| alloc.free(c.name);
        columns.deinit(alloc);
    }

    var pkey_seen = false;
    for (cs.tableElts) |elt| {
        const cd = switch (elt) {
            .ColumnDef => |col| col,
            .Constraint => return invalidMsg(alloc, &columns, "table-level constraints are not supported", .{}),
        };
        if (std.ascii.eqlIgnoreCase(cd.colname, "egid"))
            return invalidMsg(alloc, &columns, "column name \"egid\" is reserved", .{});
        for (columns.items) |c| {
            if (std.mem.eql(u8, c.name, cd.colname))
                return invalidMsg(alloc, &columns, "duplicate column \"{s}\"", .{cd.colname});
        }

        const names = cd.typeName.names;
        if (names.len == 0) return .unsupported;
        const tyname = names[names.len - 1].String.sval;
        const ty = value.Type.fromName(tyname) orelse
            return invalidMsg(alloc, &columns, "unsupported type \"{s}\"", .{tyname});

        var pkey = false;
        for (cd.constraints) |con| {
            if (!std.mem.eql(u8, con.Constraint.contype, "CONSTR_PRIMARY"))
                return invalidMsg(alloc, &columns, "only PRIMARY KEY column constraints are supported", .{});
            pkey = true;
        }
        if (pkey) {
            if (pkey_seen)
                return invalidMsg(alloc, &columns, "multiple PRIMARY KEY columns are not supported", .{});
            pkey_seen = true;
        }

        const owned = try alloc.dupe(u8, cd.colname);
        errdefer alloc.free(owned);
        try columns.append(alloc, .{ .name = owned, .ty = ty, .pkey = pkey });
    }

    const owned_name = try alloc.dupe(u8, cs.relation.relname);
    errdefer alloc.free(owned_name);
    return .{ .create_table = .{
        .name = owned_name,
        .if_not_exists = cs.if_not_exists,
        .columns = try columns.toOwnedSlice(alloc),
    } };
}

/// DROP TABLE. Non-table DROPs (INDEX, VIEW, …) are `.unsupported`;
/// multi-table DROPs are rejected.
fn parseDropTable(alloc: std.mem.Allocator, body: std.json.Value) parser.ParseError!DdlStmt {
    const parsed = try fromValue(DropStmtJson, alloc, body) orelse return .unsupported;
    defer parsed.deinit();
    const ds = parsed.value;

    if (!std.mem.eql(u8, ds.removeType, "OBJECT_TABLE")) return .unsupported;
    if (ds.objects.len != 1)
        return .{ .invalid = try alloc.dupe(u8, "DROP TABLE with multiple tables is not supported") };
    const items = ds.objects[0].List.items;
    if (items.len == 0) return .unsupported;

    return .{ .drop_table = .{
        .name = try alloc.dupe(u8, items[items.len - 1].String.sval),
        .missing_ok = ds.missing_ok,
    } };
}

/// INSERT ... VALUES. ON CONFLICT / RETURNING / INSERT…SELECT are rejected
/// (before any allocation, so no cleanup); arity, unknown columns, and literal
/// coercion are the executor's job. The `errdefer`s only cover OOM.
fn parseInsert(alloc: std.mem.Allocator, body: std.json.Value) parser.ParseError!DdlStmt {
    const parsed = try fromValue(InsertStmtJson, alloc, body) orelse return .unsupported;
    defer parsed.deinit();
    const is = parsed.value;

    if (is.onConflictClause != null) return .{ .invalid = try alloc.dupe(u8, "ON CONFLICT is not supported") };
    if (is.returningList.len > 0) return .{ .invalid = try alloc.dupe(u8, "RETURNING is not supported") };
    const sel = is.selectStmt orelse return .{ .invalid = try alloc.dupe(u8, "INSERT requires VALUES") };
    if (sel.SelectStmt.valuesLists.len == 0)
        return .{ .invalid = try alloc.dupe(u8, "only INSERT ... VALUES is supported") };

    var columns: ?[][]const u8 = null;
    errdefer if (columns) |cols| {
        for (cols) |c| alloc.free(c);
        alloc.free(cols);
    };
    if (is.cols.len > 0) {
        const cols = try alloc.alloc([]const u8, is.cols.len);
        var filled: usize = 0;
        errdefer {
            for (cols[0..filled]) |c| alloc.free(c);
            alloc.free(cols);
        }
        for (is.cols, cols) |ct, *dst| {
            dst.* = try alloc.dupe(u8, ct.ResTarget.name);
            filled += 1;
        }
        columns = cols;
    }

    var rows: std.ArrayList([]Literal) = .empty;
    errdefer {
        for (rows.items) |r| {
            freeLits(alloc, r);
            alloc.free(r);
        }
        rows.deinit(alloc);
    }
    for (sel.SelectStmt.valuesLists) |ln| {
        const lits = try alloc.alloc(Literal, ln.List.items.len);
        var filled: usize = 0;
        errdefer {
            freeLits(alloc, lits[0..filled]);
            alloc.free(lits);
        }
        for (ln.List.items, lits) |ac, *lit| {
            lit.* = try literalFrom(alloc, ac.A_Const);
            filled += 1;
        }
        try rows.append(alloc, lits);
    }

    const table = try alloc.dupe(u8, is.relation.relname);
    errdefer alloc.free(table);
    return .{ .insert = .{
        .table = table,
        .columns = columns,
        .rows = try rows.toOwnedSlice(alloc),
    } };
}

fn literalFrom(alloc: std.mem.Allocator, ac: InsertStmtJson.AConst) parser.ParseError!Literal {
    if (ac.isnull) return .null_value;
    if (ac.ival) |iv| return .{ .int = iv.ival };
    if (ac.fval) |fv| return .{ .float = try alloc.dupe(u8, fv.fval) };
    if (ac.sval) |sv| return .{ .str = try alloc.dupe(u8, sv.sval) };
    if (ac.boolval) |bv| return .{ .bool_ = bv.boolval };
    return .unsupported;
}

fn freeLits(alloc: std.mem.Allocator, lits: []const Literal) void {
    for (lits) |lit| switch (lit) {
        .str, .float => |s| alloc.free(s),
        else => {},
    };
}

/// SELECT [cols|*] FROM one_table [WHERE col = literal]. Joins, subqueries,
/// ORDER BY / GROUP BY / LIMIT / DISTINCT and expression targets are rejected.
fn parseSelect(alloc: std.mem.Allocator, body: std.json.Value) parser.ParseError!DdlStmt {
    const parsed = try fromValue(SelectStmtJson, alloc, body) orelse return .unsupported;
    defer parsed.deinit();
    const ss = parsed.value;

    if (ss.sortClause.len > 0 or ss.groupClause.len > 0 or ss.distinctClause.len > 0 or
        ss.havingClause != null or ss.limitCount != null or ss.limitOffset != null)
        return .{ .invalid = try alloc.dupe(u8, "ORDER BY / GROUP BY / LIMIT / DISTINCT are not supported") };

    if (ss.fromClause.len != 1)
        return .{ .invalid = try alloc.dupe(u8, "SELECT must read exactly one table") };
    const rv = ss.fromClause[0].RangeVar orelse
        return .{ .invalid = try alloc.dupe(u8, "only table sources are supported") };

    // Every target must be a column reference; a `*` anywhere makes it a
    // star projection.
    var star = false;
    for (ss.targetList) |tn| {
        const cr = tn.ResTarget.val.ColumnRef orelse
            return .{ .invalid = try alloc.dupe(u8, "only column references are supported in SELECT") };
        for (cr.fields) |f| {
            if (f.A_Star != null) star = true;
        }
    }

    var cols: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (cols.items) |c| alloc.free(c);
        cols.deinit(alloc);
    }
    if (!star) {
        for (ss.targetList) |tn| {
            const fields = tn.ResTarget.val.ColumnRef.?.fields;
            if (fields.len == 0 or fields[fields.len - 1].String == null)
                return invalidSel(alloc, &cols, "unsupported column reference", .{});
            try cols.append(alloc, try alloc.dupe(u8, fields[fields.len - 1].String.?.sval));
        }
    }

    var filter: ?Filter = null;
    errdefer if (filter) |f| {
        alloc.free(f.column);
        freeLit(alloc, f.literal);
    };
    if (ss.whereClause) |w| {
        const ae = w.A_Expr orelse return invalidSel(alloc, &cols, "unsupported WHERE clause", .{});
        if (!std.mem.eql(u8, ae.kind, "AEXPR_OP") or ae.name.len == 0 or
            !std.mem.eql(u8, ae.name[ae.name.len - 1].String.sval, "="))
            return invalidSel(alloc, &cols, "only `column = value` is supported in WHERE", .{});
        const lfields = (ae.lexpr.ColumnRef orelse return invalidSel(alloc, &cols, "WHERE left side must be a column", .{})).fields;
        if (lfields.len == 0 or lfields[lfields.len - 1].String == null)
            return invalidSel(alloc, &cols, "WHERE left side must be a column", .{});
        const lit = try literalFrom(alloc, ae.rexpr.A_Const);
        if (lit == .unsupported) return invalidSel(alloc, &cols, "WHERE right side must be a literal", .{});
        const wcol = alloc.dupe(u8, lfields[lfields.len - 1].String.?.sval) catch |e| {
            freeLit(alloc, lit);
            return e;
        };
        filter = .{ .column = wcol, .literal = lit };
    }

    const table = try alloc.dupe(u8, rv.relname);
    errdefer alloc.free(table);
    return .{ .select = .{
        .table = table,
        .projection = if (star) .star else .{ .columns = try cols.toOwnedSlice(alloc) },
        .filter = filter,
    } };
}

/// Reject a recognized SELECT, freeing the columns accumulated so far. Message
/// first — on OOM the caller's errdefer owns the cleanup.
fn invalidSel(alloc: std.mem.Allocator, cols: *std.ArrayList([]const u8), comptime fmt: []const u8, args: anytype) parser.ParseError!DdlStmt {
    const msg = try std.fmt.allocPrint(alloc, fmt, args);
    for (cols.items) |c| alloc.free(c);
    cols.deinit(alloc);
    return .{ .invalid = msg };
}

/// Reject a recognized statement: build the owned `.invalid` reason, then
/// release the columns accumulated so far. Message first — on OOM the
/// caller's errdefer still owns the cleanup.
fn invalidMsg(alloc: std.mem.Allocator, columns: *std.ArrayList(ColumnDef), comptime fmt: []const u8, args: anytype) parser.ParseError!DdlStmt {
    const msg = try std.fmt.allocPrint(alloc, fmt, args);
    for (columns.items) |c| alloc.free(c.name);
    columns.deinit(alloc);
    return .{ .invalid = msg };
}

fn asObject(v: std.json.Value) ?std.json.ObjectMap {
    return switch (v) {
        .object => |o| o,
        else => null,
    };
}

fn asString(v: std.json.Value) ?[]const u8 {
    return switch (v) {
        .string => |s| s,
        else => null,
    };
}
