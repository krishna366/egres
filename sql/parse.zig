//! Statement parsing: SQL text → a small typed `Stmt`, via libpg_query
//! (reused from pgwire). Only the statements the server executes are
//! recognized — CREATE/DROP DATABASE, TABLE, INDEX; INSERT, SELECT,
//! DELETE, UPDATE; BEGIN, COMMIT, ROLLBACK — everything else maps to
//! `.unsupported`. A statement we recognize but must reject (unsupported
//! type, constraint, clause, …) maps to `.invalid` carrying the
//! client-facing reason — parse succeeded, this is a semantic rejection.
//! Syntax errors propagate as ParseError so the endpoint can answer with
//! a 42601.
//!
//! `statement` takes an arena and returns a `Stmt` whose every string
//! and slice BORROWS from it — parse allocates the libpg_query tree into
//! the arena and the statement points straight into it; nothing is duped
//! or individually freed. The statement is valid only while the arena
//! lives, and the caller drops the arena when the statement is done.

const std = @import("std");
const pgwire = @import("pgwire");
const parser = pgwire.parser_mod;
const value = @import("value.zig");

const String = value.String;

/// One parsed `name type [PRIMARY KEY]` column. `name` is borrowed.
pub const ColumnDef = struct {
    name: []const u8,
    ty: value.Type,
    pkey: bool = false,
};

pub const CreateTable = struct {
    name: []const u8, // table name (borrowed)
    if_not_exists: bool = false,
    columns: []ColumnDef, // borrowed (and each column name)
};

pub const DropTable = struct {
    name: []const u8, // table name (borrowed)
    missing_ok: bool = false, // IF EXISTS
};

pub const CreateIndex = struct {
    name: ?String, // borrowed; null = derive "{table}_{column}_idx"
    table: String, // borrowed
    column: String, // borrowed
    if_not_exists: bool = false,
};

pub const DropIndex = struct {
    name: String, // index name (borrowed)
    missing_ok: bool = false, // IF EXISTS
};

/// One VALUES cell, still untyped: coerced to its column's Type at execute
/// (value.parseValue). `str`/`float` borrow their bytes. `unsupported` is a
/// non-literal item (DEFAULT, an expression) the executor rejects.
pub const Literal = union(enum) {
    int: i64,
    float: []const u8, // borrowed; libpg_query keeps floats as text
    str: []const u8, // borrowed
    bool_: bool,
    null_value,
    unsupported,
};

pub const Insert = struct {
    table: []const u8, // borrowed
    columns: ?[][]const u8, // borrowed target columns; null = positional (all columns)
    rows: [][]Literal, // borrowed rows of literals
};

/// What a SELECT projects: `*` (all live columns) or a named list (which may
/// include the `egid` pseudo-column). Names are borrowed.
pub const Projection = union(enum) {
    star,
    columns: [][]const u8,
};

/// A single `column = literal` filter (the only WHERE this slice supports).
pub const Filter = struct {
    column: []const u8, // borrowed
    literal: Literal,
};

pub const Select = struct {
    table: []const u8, // borrowed
    projection: Projection,
    filter: ?Filter,
    /// FOR UPDATE: lock every matched row exclusively (held to txn end).
    for_update: bool = false,
};

pub const Delete = struct {
    table: []const u8, // borrowed
    filter: ?Filter, // null = delete every row
};

/// One SET assignment: `column = literal`.
pub const SetClause = struct {
    column: []const u8, // borrowed
    literal: Literal,
};

pub const Update = struct {
    table: []const u8, // borrowed
    sets: []SetClause, // borrowed
    filter: ?Filter, // null = update every row
};

pub const Stmt = union(enum) {
    create_database: []const u8, // database name (borrowed)
    drop_database: []const u8, // database name (borrowed)
    create_table: CreateTable,
    drop_table: DropTable,
    create_index: CreateIndex,
    drop_index: DropIndex,
    insert: Insert,
    select: Select,
    delete: Delete,
    update: Update,
    begin_txn,
    commit_txn,
    rollback_txn,
    invalid: []const u8, // recognized but rejected; reason
    unsupported,
};

pub fn statement(alloc: std.mem.Allocator, sql: []const u8) parser.ParseError!Stmt {
    // `alloc` must be an arena: parse allocates the whole libpg_query JSON
    // tree into it, and the returned Stmt borrows strings straight out
    // of that tree. Nothing is freed here — the caller drops the arena.
    const tree = try parser.parse(alloc, sql, null);

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
        return if (kind[0] == 'C')
            .{ .create_database = name }
        else
            .{ .drop_database = name };
    }
    if (std.mem.eql(u8, kind, "CreateStmt")) return parseCreateTable(alloc, entry.value_ptr.*);
    if (std.mem.eql(u8, kind, "DropStmt")) return parseDropTable(alloc, entry.value_ptr.*);
    if (std.mem.eql(u8, kind, "IndexStmt")) return parseCreateIndex(alloc, entry.value_ptr.*);
    if (std.mem.eql(u8, kind, "InsertStmt")) return parseInsert(alloc, entry.value_ptr.*);
    if (std.mem.eql(u8, kind, "SelectStmt")) return parseSelect(alloc, entry.value_ptr.*);
    if (std.mem.eql(u8, kind, "DeleteStmt")) return parseDelete(alloc, entry.value_ptr.*);
    if (std.mem.eql(u8, kind, "UpdateStmt")) return parseUpdate(alloc, entry.value_ptr.*);
    if (std.mem.eql(u8, kind, "TransactionStmt")) return parseTransaction(alloc, entry.value_ptr.*);
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

/// DROP TABLE IF EXISTS public.t / DROP INDEX IF EXISTS i
const DropStmtJson = struct {
    removeType: String = "", // OBJECT_TABLE / OBJECT_INDEX (vs VIEW, …)
    missing_ok: bool = false, // IF EXISTS
    concurrent: bool = false, // DROP INDEX CONCURRENTLY — rejected
    objects: []const Obj = &.{}, // one per dropped object

    const Obj = struct { List: struct { items: []const NameNode = &.{} } = .{} };
};

/// CREATE INDEX i ON t (col)
const IndexStmtJson = struct {
    idxname: String = "", // empty = unnamed (executor derives one)
    relation: struct { relname: String = "" } = .{},
    accessMethod: String = "", // the parser fills "btree" by default
    tableSpace: String = "",
    indexParams: []const IndexElemNode = &.{},
    indexIncludingParams: []const std.json.Value = &.{},
    whereClause: ?std.json.Value = null, // presence ⇒ partial index
    unique: bool = false,
    concurrent: bool = false,
    if_not_exists: bool = false,

    const IndexElemNode = struct {
        IndexElem: struct {
            name: String = "", // empty ⇒ expression element
            ordering: String = "", // SORTBY_* ; "" = default (ASC)
            nulls_ordering: String = "", // SORTBY_NULLS_* ; "" = LAST
        } = .{},
    };
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
    lockingClause: []const LockingNode = &.{},
    // Clauses this slice doesn't support — presence ⇒ rejection.
    sortClause: []const std.json.Value = &.{},
    groupClause: []const std.json.Value = &.{},
    distinctClause: []const std.json.Value = &.{},
    havingClause: ?std.json.Value = null,
    limitCount: ?std.json.Value = null,
    limitOffset: ?std.json.Value = null,

    // FOR UPDATE / FOR SHARE / NOWAIT / SKIP LOCKED. Unlike zero-valued
    // ints (omitted), enum strings are always emitted: plain FOR UPDATE
    // arrives as strength="LCS_FORUPDATE", waitPolicy="LockWaitBlock".
    const LockingNode = struct {
        LockingClause: ?struct {
            strength: []const u8 = "",
            waitPolicy: []const u8 = "",
            lockedRels: []const std.json.Value = &.{},
        } = null,
    };

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

/// DELETE FROM t [WHERE col = const]. `relation` is inline (a RangeVar body,
/// like INSERT); the last three clauses are decoded only to reject them.
const DeleteStmtJson = struct {
    relation: struct { relname: []const u8 = "" } = .{},
    whereClause: ?SelectStmtJson.WhereNode = null,
    usingClause: []const std.json.Value = &.{}, // DELETE ... USING
    returningList: []const std.json.Value = &.{}, // RETURNING
    withClause: ?std.json.Value = null, // WITH ... DELETE
};

/// UPDATE t SET col = const [, ...] [WHERE col = const]. Each targetList
/// entry is one SET assignment; a non-literal value (expression, DEFAULT,
/// sub-select, multi-assign) leaves `A_Const` null → rejected. The last
/// three clauses are decoded only to reject them.
const UpdateStmtJson = struct {
    relation: struct { relname: []const u8 = "" } = .{},
    targetList: []const ResTargetNode = &.{},
    whereClause: ?SelectStmtJson.WhereNode = null,
    fromClause: []const std.json.Value = &.{}, // UPDATE ... FROM
    returningList: []const std.json.Value = &.{}, // RETURNING
    withClause: ?std.json.Value = null, // WITH ... UPDATE

    const ResTargetNode = struct { ResTarget: ResTarget = .{} };
    const ResTarget = struct {
        name: []const u8 = "",
        val: struct { A_Const: ?InsertStmtJson.AConst = null } = .{},
    };
};

/// Decode one statement body into its typed mirror. A shape we don't
/// recognize (unknown node kind, wrong field type) is null → .unsupported;
/// only OOM propagates.
fn fromValue(comptime T: type, alloc: std.mem.Allocator, body: std.json.Value) parser.ParseError!?T {
    return std.json.parseFromValueLeaky(T, alloc, body, .{ .ignore_unknown_fields = true }) catch |err| switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => null,
    };
}

/// CREATE TABLE. Only bare `name type` columns with an optional
/// single-column PRIMARY KEY parse; any other constraint (NOT NULL, UNIQUE,
/// FK, DEFAULT, table-level) is a clean `.invalid`.
fn parseCreateTable(alloc: std.mem.Allocator, body: std.json.Value) parser.ParseError!Stmt {
    const cs = try fromValue(CreateStmtJson, alloc, body) orelse return .unsupported;

    var columns: std.ArrayList(ColumnDef) = .empty;
    var pkey_seen = false;
    for (cs.tableElts) |elt| {
        const cd = switch (elt) {
            .ColumnDef => |col| col,
            .Constraint => return invalid("table-level constraints are not supported"),
        };
        if (std.ascii.eqlIgnoreCase(cd.colname, "egid"))
            return invalid("column name \"egid\" is reserved");
        for (columns.items) |c| {
            if (std.mem.eql(u8, c.name, cd.colname))
                return invalidFmt(alloc, "duplicate column \"{s}\"", .{cd.colname});
        }

        const names = cd.typeName.names;
        if (names.len == 0) return .unsupported;
        const tyname = names[names.len - 1].String.sval;
        const ty = value.Type.fromName(tyname) orelse
            return invalidFmt(alloc, "unsupported type \"{s}\"", .{tyname});

        var pkey = false;
        for (cd.constraints) |con| {
            if (!std.mem.eql(u8, con.Constraint.contype, "CONSTR_PRIMARY"))
                return invalid("only PRIMARY KEY column constraints are supported");
            pkey = true;
        }
        if (pkey) {
            if (pkey_seen)
                return invalid("multiple PRIMARY KEY columns are not supported");
            pkey_seen = true;
        }

        try columns.append(alloc, .{ .name = cd.colname, .ty = ty, .pkey = pkey });
    }

    return .{ .create_table = .{
        .name = cs.relation.relname,
        .if_not_exists = cs.if_not_exists,
        .columns = try columns.toOwnedSlice(alloc),
    } };
}

/// DROP TABLE / DROP INDEX (libpg_query reuses DropStmt for both,
/// discriminated by removeType). Other object kinds (VIEW, …) are
/// `.unsupported`; multi-object DROPs are rejected.
fn parseDropTable(alloc: std.mem.Allocator, body: std.json.Value) parser.ParseError!Stmt {
    const ds = try fromValue(DropStmtJson, alloc, body) orelse return .unsupported;

    const is_index = std.mem.eql(u8, ds.removeType, "OBJECT_INDEX");
    if (!is_index and !std.mem.eql(u8, ds.removeType, "OBJECT_TABLE"))
        return .unsupported;
    if (ds.objects.len != 1) {
        return if (is_index)
            invalid("DROP INDEX with multiple indexes is not supported")
        else
            invalid("DROP TABLE with multiple tables is not supported");
    }
    if (is_index and ds.concurrent)
        return invalid("DROP INDEX CONCURRENTLY is not supported");
    const items = ds.objects[0].List.items;
    if (items.len == 0) return .unsupported;
    const name = items[items.len - 1].String.sval;

    if (is_index) {
        return .{ .drop_index = .{
            .name = name,
            .missing_ok = ds.missing_ok,
        } };
    }
    return .{ .drop_table = .{
        .name = name,
        .missing_ok = ds.missing_ok,
    } };
}

/// CREATE INDEX. v1: a single plain column, non-unique, whole-table,
/// default btree — every narrowing (UNIQUE, CONCURRENTLY, partial,
/// multi-column, expression, INCLUDE, TABLESPACE, other access
/// methods, DESC / NULLS FIRST ordering) is a clean `.invalid`.
/// ASC / NULLS LAST are accepted: they are this index's semantics
/// already (the NULL tag byte sorts last).
fn parseCreateIndex(alloc: std.mem.Allocator, body: std.json.Value) parser.ParseError!Stmt {
    const is = try fromValue(IndexStmtJson, alloc, body) orelse return .unsupported;

    if (is.unique) return invalid("UNIQUE indexes are not supported");
    if (is.concurrent)
        return invalid("CREATE INDEX CONCURRENTLY is not supported");
    if (is.whereClause != null)
        return invalid("partial indexes are not supported");
    if (is.indexIncludingParams.len > 0)
        return invalid("INCLUDE columns are not supported");
    if (is.tableSpace.len > 0) return invalid("TABLESPACE is not supported");
    if (is.accessMethod.len > 0 and !std.mem.eql(u8, is.accessMethod, "btree"))
        return invalidFmt(
            alloc,
            "access method \"{s}\" is not supported",
            .{is.accessMethod},
        );
    if (is.indexParams.len != 1)
        return invalid("only single-column indexes are supported");
    const elem = is.indexParams[0].IndexElem;
    if (elem.name.len == 0)
        return invalid("expression indexes are not supported");
    if (std.mem.eql(u8, elem.ordering, "SORTBY_DESC"))
        return invalid("DESC index ordering is not supported");
    if (std.mem.eql(u8, elem.nulls_ordering, "SORTBY_NULLS_FIRST"))
        return invalid("NULLS FIRST index ordering is not supported");

    return .{ .create_index = .{
        .name = if (is.idxname.len > 0) is.idxname else null,
        .table = is.relation.relname,
        .column = elem.name,
        .if_not_exists = is.if_not_exists,
    } };
}

/// INSERT ... VALUES. ON CONFLICT / RETURNING / INSERT…SELECT are rejected
/// (before any allocation, so no cleanup); arity, unknown columns, and literal
/// coercion are the executor's job. The `errdefer`s only cover OOM.
fn parseInsert(alloc: std.mem.Allocator, body: std.json.Value) parser.ParseError!Stmt {
    const is = try fromValue(InsertStmtJson, alloc, body) orelse return .unsupported;

    if (is.onConflictClause != null) return invalid("ON CONFLICT is not supported");
    if (is.returningList.len > 0) return invalid("RETURNING is not supported");
    const sel = is.selectStmt orelse return invalid("INSERT requires VALUES");
    if (sel.SelectStmt.valuesLists.len == 0)
        return invalid("only INSERT ... VALUES is supported");

    var columns: ?[][]const u8 = null;
    if (is.cols.len > 0) {
        const cols = try alloc.alloc([]const u8, is.cols.len);
        for (is.cols, cols) |ct, *dst| dst.* = ct.ResTarget.name;
        columns = cols;
    }

    var rows: std.ArrayList([]Literal) = .empty;
    for (sel.SelectStmt.valuesLists) |ln| {
        const lits = try alloc.alloc(Literal, ln.List.items.len);
        for (ln.List.items, lits) |ac, *lit| lit.* = literalFrom(ac.A_Const);
        try rows.append(alloc, lits);
    }

    return .{ .insert = .{
        .table = is.relation.relname,
        .columns = columns,
        .rows = try rows.toOwnedSlice(alloc),
    } };
}

fn literalFrom(ac: InsertStmtJson.AConst) Literal {
    if (ac.isnull) return .null_value;
    if (ac.ival) |iv| return .{ .int = iv.ival };
    if (ac.fval) |fv| return .{ .float = fv.fval };
    if (ac.sval) |sv| return .{ .str = sv.sval };
    if (ac.boolval) |bv| return .{ .bool_ = bv.boolval };
    return .unsupported;
}

/// A parsed WHERE: a `col = literal` filter or a rejection reason (borrowed).
const ParsedWhere = union(enum) {
    filter: Filter,
    invalid: []const u8,

    fn reject(msg: []const u8) ParsedWhere {
        return .{ .invalid = msg };
    }
};

/// Parse the one WHERE shape this slice supports — `column = literal` —
/// shared by SELECT and DELETE. Anything else (non-equality op, AND/OR,
/// subquery, column-vs-column) yields a clean `.invalid` reason; only OOM
/// propagates.
fn parseWhere(w: SelectStmtJson.WhereNode) ParsedWhere {
    const ae = w.A_Expr orelse
        return ParsedWhere.reject("unsupported WHERE clause");
    if (!std.mem.eql(u8, ae.kind, "AEXPR_OP") or ae.name.len == 0 or
        !std.mem.eql(u8, ae.name[ae.name.len - 1].String.sval, "="))
        return ParsedWhere.reject(
            "only `column = value` is supported in WHERE",
        );

    const lcol = ae.lexpr.ColumnRef orelse
        return ParsedWhere.reject("WHERE left side must be a column");
    const lfields = lcol.fields;
    if (lfields.len == 0 or lfields[lfields.len - 1].String == null)
        return ParsedWhere.reject("WHERE left side must be a column");

    const lit = literalFrom(ae.rexpr.A_Const);
    if (lit == .unsupported)
        return ParsedWhere.reject("WHERE right side must be a literal");
    return .{ .filter = .{
        .column = lfields[lfields.len - 1].String.?.sval,
        .literal = lit,
    } };
}

/// SELECT [cols|*] FROM one_table [WHERE col = literal]. Joins, subqueries,
/// ORDER BY / GROUP BY / LIMIT / DISTINCT and expression targets are rejected.
fn parseSelect(alloc: std.mem.Allocator, body: std.json.Value) parser.ParseError!Stmt {
    const ss = try fromValue(SelectStmtJson, alloc, body) orelse return .unsupported;

    if (ss.sortClause.len > 0 or ss.groupClause.len > 0 or ss.distinctClause.len > 0 or
        ss.havingClause != null or ss.limitCount != null or ss.limitOffset != null)
        return invalid("ORDER BY / GROUP BY / LIMIT / DISTINCT are not supported");

    if (ss.fromClause.len != 1)
        return invalid("SELECT must read exactly one table");
    const rv = ss.fromClause[0].RangeVar orelse
        return invalid("only table sources are supported");

    // Every target must be a column reference; a `*` anywhere makes it a
    // star projection.
    var star = false;
    for (ss.targetList) |tn| {
        const cr = tn.ResTarget.val.ColumnRef orelse
            return invalid("only column references are supported in SELECT");
        for (cr.fields) |f| {
            if (f.A_Star != null) star = true;
        }
    }

    var cols: std.ArrayList([]const u8) = .empty;
    if (!star) {
        for (ss.targetList) |tn| {
            const fields = tn.ResTarget.val.ColumnRef.?.fields;
            if (fields.len == 0 or fields[fields.len - 1].String == null)
                return invalid("unsupported column reference");
            try cols.append(alloc, fields[fields.len - 1].String.?.sval);
        }
    }

    var filter: ?Filter = null;
    if (ss.whereClause) |w| switch (parseWhere(w)) {
        .filter => |f| filter = f,
        .invalid => |msg| return .{ .invalid = msg },
    };

    var for_update = false;
    if (ss.lockingClause.len > 0) {
        if (ss.lockingClause.len > 1)
            return invalid("only a single FOR UPDATE clause is supported");
        const lc = ss.lockingClause[0].LockingClause orelse
            return invalid("unsupported locking clause");
        if (!std.mem.eql(u8, lc.strength, "LCS_FORUPDATE"))
            return invalid("only FOR UPDATE is supported");
        if (!std.mem.eql(u8, lc.waitPolicy, "LockWaitBlock"))
            return invalid("NOWAIT / SKIP LOCKED are not supported");
        if (lc.lockedRels.len > 0)
            return invalid("FOR UPDATE OF is not supported");
        for_update = true;
    }

    return .{ .select = .{
        .table = rv.relname,
        .projection = if (star) .star else .{ .columns = try cols.toOwnedSlice(alloc) },
        .filter = filter,
        .for_update = for_update,
    } };
}

/// Reject a recognized statement with a static client-facing reason —
/// borrowed straight from .rodata, so no allocation.
fn invalid(msg: []const u8) Stmt {
    return .{ .invalid = msg };
}

/// Reject with a formatted reason, allocated into the parse arena.
fn invalidFmt(
    alloc: std.mem.Allocator,
    comptime fmt: []const u8,
    args: anytype,
) parser.ParseError!Stmt {
    return .{ .invalid = try std.fmt.allocPrint(alloc, fmt, args) };
}

/// BEGIN / START TRANSACTION / COMMIT / ROLLBACK. GOTCHA: TRANS_STMT_BEGIN
/// is protobuf tag 0, so libpg_query OMITS `kind` for a plain BEGIN — the
/// default must be begin. Transaction modes (ISOLATION LEVEL …) are
/// ignored; savepoints and two-phase commit are rejected with a reason.
const TransactionStmtJson = struct {
    kind: []const u8 = "TRANS_STMT_BEGIN",
};

fn parseTransaction(
    alloc: std.mem.Allocator,
    body: std.json.Value,
) parser.ParseError!Stmt {
    const ts = try fromValue(TransactionStmtJson, alloc, body) orelse
        return .unsupported;
    const k = ts.kind;
    if (std.mem.eql(u8, k, "TRANS_STMT_BEGIN") or
        std.mem.eql(u8, k, "TRANS_STMT_START"))
        return .begin_txn;
    if (std.mem.eql(u8, k, "TRANS_STMT_COMMIT")) return .commit_txn;
    if (std.mem.eql(u8, k, "TRANS_STMT_ROLLBACK")) return .rollback_txn;
    return invalid("unsupported transaction statement");
}

/// DELETE FROM one_table [WHERE col = literal]. USING, RETURNING, WITH, and
/// any non-equality WHERE are rejected with a reason. `WHERE egid = N` parses
/// as an ordinary column filter — egid resolution stays server-side.
fn parseDelete(
    alloc: std.mem.Allocator,
    body: std.json.Value,
) parser.ParseError!Stmt {
    const ds = try fromValue(DeleteStmtJson, alloc, body) orelse
        return .unsupported;

    if (ds.usingClause.len > 0)
        return invalid("DELETE ... USING is not supported");
    if (ds.returningList.len > 0)
        return invalid("RETURNING is not supported");
    if (ds.withClause != null)
        return invalid("WITH is not supported");

    var filter: ?Filter = null;
    if (ds.whereClause) |w| switch (parseWhere(w)) {
        .filter => |f| filter = f,
        .invalid => |msg| return .{ .invalid = msg },
    };

    return .{ .delete = .{ .table = ds.relation.relname, .filter = filter } };
}

/// UPDATE one_table SET col = literal [, ...] [WHERE col = literal].
/// FROM, RETURNING, WITH, non-literal SET values, and duplicate SET
/// columns are rejected with a reason.
fn parseUpdate(
    alloc: std.mem.Allocator,
    body: std.json.Value,
) parser.ParseError!Stmt {
    const us = try fromValue(UpdateStmtJson, alloc, body) orelse
        return .unsupported;

    if (us.fromClause.len > 0)
        return invalid("UPDATE ... FROM is not supported");
    if (us.returningList.len > 0)
        return invalid("RETURNING is not supported");
    if (us.withClause != null)
        return invalid("WITH is not supported");
    if (us.targetList.len == 0) return .unsupported;

    var filter: ?Filter = null;
    if (us.whereClause) |w| switch (parseWhere(w)) {
        .filter => |f| filter = f,
        .invalid => |msg| return .{ .invalid = msg },
    };

    var sets: std.ArrayList(SetClause) = .empty;
    for (us.targetList) |tn| {
        const rt = tn.ResTarget;
        if (rt.name.len == 0) return .unsupported;
        const ac = rt.val.A_Const orelse
            return invalid("SET value must be a literal");
        for (sets.items) |s| {
            if (std.mem.eql(u8, s.column, rt.name))
                return invalid("duplicate column in SET");
        }
        const lit = literalFrom(ac);
        if (lit == .unsupported)
            return invalid("SET value must be a literal");
        try sets.append(alloc, .{ .column = rt.name, .literal = lit });
    }

    return .{ .update = .{
        .table = us.relation.relname,
        .sets = try sets.toOwnedSlice(alloc),
        .filter = filter,
    } };
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
