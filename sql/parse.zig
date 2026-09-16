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
const parser = pgwire.parser;
const _value = @import("value.zig");
const Column = @import("table.zig").Column;

const String = _value.String;

/// One parsed `name type [PRIMARY KEY | UNIQUE]` column. `name` is
/// borrowed. A PRIMARY KEY column is unique too, but `unique` marks
/// only a bare UNIQUE constraint: the PK gets its own index.
pub const ColumnDef = struct {
    name: String,
    ty: _value.Type,
    /// `char(n)` / `varchar(n)` length; null for every other type.
    type_mod: ?u32 = null,
    pkey: bool = false,
    unique: bool = false,
};

pub const CreateTable = struct {
    name: String, // table name (borrowed)
    if_not_exists: bool = false,
    columns: []ColumnDef, // borrowed (and each column name)
};

pub const DropTable = struct {
    name: String, // table name (borrowed)
    missing_ok: bool = false, // IF EXISTS
};

pub const CreateIndex = struct {
    name: ?String, // borrowed; null = derive "{table}_{column}_idx"
    table: String, // borrowed
    column: String, // borrowed
    unique: bool = false,
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
    float: String, // borrowed; libpg_query keeps floats as text
    str: String, // borrowed
    bool_: bool,
    null_value,
    /// CURRENT_TIMESTAMP / LOCALTIMESTAMP: the transaction's start
    /// time, resolved by the executor (dml.coerceLiteral) — the one
    /// literal that is not a constant.
    current_timestamp,
    unsupported,
};

pub const Insert = struct {
    table: String, // borrowed
    columns: ?[]String, // borrowed target columns; null = positional (all columns)
    rows: [][]Literal, // borrowed rows of literals
};

/// What a SELECT projects: `*` (all live columns), a named list (which
/// may include the `egid` pseudo-column), or `count(*)` — one int8 cell,
/// the number of rows the plan yields. Names are borrowed.
pub const Projection = union(enum) {
    star,
    columns: []String,
    count_star,
};

/// A single `column = literal` comparison — the equality leaf of a WHERE.
pub const Eq = struct {
    column: String, // borrowed
    literal: Literal,
};

/// A parsed WHERE tree. Leaves name one column; AND/OR hold their
/// children inline (a slice is already an indirection, and children
/// are never shared between parents); NOT is a pointer only because a
/// union cannot contain itself by value. Everything borrows the arena.
pub const Where = union(enum) {
    eq: Eq,
    is_null: NullTest,
    and_: []const Where,
    or_: []const Where,
    not: *const Where,

    /// `column IS NULL` (negate = false) / `column IS NOT NULL`.
    pub const NullTest = struct {
        column: String, // borrowed
        negate: bool,
    };
};

pub const Select = struct {
    table: String, // borrowed
    projection: Projection,
    where: ?Where,
    /// FOR UPDATE: lock every matched row exclusively (held to txn end).
    for_update: bool = false,
    /// LIMIT n: stop after n rows. null = every row (LIMIT ALL too).
    limit: ?usize = null,
};

pub const Delete = struct {
    table: String, // borrowed
    where: ?Where, // null = delete every row
};

/// A SET value as the grammar built it: precedence and parentheses
/// are already in the tree's shape, and a minus before a number is
/// already folded into a negative literal (gram.y's doNegate), so
/// `-col` is the only unary form — it parses as `0 - col`; `+x` is
/// `x`. Everything borrows the arena; children are pointers only
/// because a union cannot hold itself by value.
pub const Expr = union(enum) {
    literal: Literal,
    column: String, // borrowed
    binop: BinOp,

    pub const Op = enum { add, sub, mul, div, mod, concat };
    pub const BinOp = struct { op: Op, lhs: *const Expr, rhs: *const Expr };
};

/// One SET assignment: `column = expr`.
pub const SetClause = struct {
    column: String, // borrowed
    value: Expr,
};

pub const Update = struct {
    table: String, // borrowed
    sets: []SetClause, // borrowed
    where: ?Where, // null = update every row
};

pub const Stmt = union(enum) {
    create_database: String, // database name (borrowed)
    drop_database: String, // database name (borrowed)
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
    invalid: String, // recognized but rejected; reason
    unsupported,
};

pub fn statement(alloc: std.mem.Allocator, sql: String) parser.ParseError!Stmt {
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
const NameNode = struct { String: struct { sval: String = "" } = .{} };

/// CREATE TABLE t (id int PRIMARY KEY, name text)
const CreateStmtJson = struct {
    relation: struct { relname: String = "" } = .{},
    if_not_exists: bool = false,
    tableElts: []const TableElt = &.{},

    const TableElt = union(enum) {
        ColumnDef: ColumnDefJson,
        /// Table-level constraint (PRIMARY KEY (a, b), CHECK, …) — rejected.
        Constraint: std.json.Value,
    };
    const ColumnDefJson = struct {
        colname: String = "",
        /// `int` arrives pg_catalog-qualified: names = [pg_catalog, int4].
        /// `char(84)` is names = [pg_catalog, bpchar] with typmods =
        /// [{A_Const: {ival: {ival: 84}}}]; `varchar(20)` the same shape
        /// under varchar. A bare `char` arrives as `bpchar` with typmods
        /// [1] (the grammar fills it), a bare `varchar` with none.
        typeName: struct {
            names: []const NameNode = &.{},
            typmods: []const TypmodNode = &.{},
        } = .{},
        constraints: []const ConstraintNode = &.{},
    };
    const TypmodNode = struct { A_Const: ?InsertStmtJson.AConst = null };
    const ConstraintNode = struct {
        Constraint: struct { contype: String = "" } = .{},
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
    relation: struct { relname: String = "" } = .{},
    cols: []const ResTargetNode = &.{}, // empty = positional
    selectStmt: ?SelectWrap = null,
    onConflictClause: ?std.json.Value = null, // presence ⇒ ON CONFLICT
    returningList: []const std.json.Value = &.{}, // non-empty ⇒ RETURNING

    const ResTargetNode = struct { ResTarget: struct { name: String = "" } = .{} };
    // INSERT ... VALUES nests rows under a SelectStmt with `valuesLists`;
    // INSERT ... SELECT has a SelectStmt WITHOUT valuesLists (→ rejected).
    const SelectWrap = struct { SelectStmt: SelectValues = .{} };
    const SelectValues = struct { valuesLists: []const ListNode = &.{} };
    const ListNode = struct {
        List: struct { items: []const ValueNode = &.{} } = .{},
    };
    // One VALUES item: a constant, or a SQL value function
    // (CURRENT_TIMESTAMP and kin). Anything else leaves both null →
    // `.unsupported`.
    const ValueNode = struct {
        A_Const: ?AConst = null,
        SQLValueFunction: ?struct { op: String = "" } = null,
    };
    // A_Const encodes its value under a type-named key; the key's presence is
    // the discriminant (libpg_query omits zero inner values, so `ival: {}`
    // still means integer 0).
    pub const AConst = struct {
        ival: ?struct { ival: i64 = 0 } = null,
        fval: ?struct { fval: String = "" } = null,
        sval: ?struct { sval: String = "" } = null,
        boolval: ?struct { boolval: bool = false } = null,
        isnull: bool = false,
    };
};

/// SELECT [cols|*] FROM t [WHERE ...]
const SelectStmtJson = struct {
    targetList: []const ResTargetNode = &.{},
    fromClause: []const FromNode = &.{},
    whereClause: ?WhereNode = null,
    lockingClause: []const LockingNode = &.{},
    // LIMIT n: a literal (A_Const null ⇒ a parameter or expression);
    // LIMIT ALL arrives as a NULL constant. limitOption tells COUNT
    // from FETCH ... WITH TIES.
    limitCount: ?LimitNode = null,
    limitOption: String = "",
    // Clauses this slice doesn't support — presence ⇒ rejection.
    sortClause: []const std.json.Value = &.{},
    groupClause: []const std.json.Value = &.{},
    distinctClause: []const std.json.Value = &.{},
    havingClause: ?std.json.Value = null,
    limitOffset: ?std.json.Value = null,

    const LimitNode = struct { A_Const: ?InsertStmtJson.AConst = null };

    // FOR UPDATE / FOR SHARE / NOWAIT / SKIP LOCKED. Unlike zero-valued
    // ints (omitted), enum strings are always emitted: plain FOR UPDATE
    // arrives as strength="LCS_FORUPDATE", waitPolicy="LockWaitBlock".
    const LockingNode = struct {
        LockingClause: ?struct {
            strength: String = "",
            waitPolicy: String = "",
            lockedRels: []const std.json.Value = &.{},
        } = null,
    };

    const ResTargetNode = struct {
        ResTarget: struct { val: TargetVal = .{} } = .{},
    };
    // A projection item: a column reference, or a function call — of
    // which only `count(*)` is accepted. Anything else (arithmetic, a
    // cast) leaves both null → rejected.
    const TargetVal = struct {
        ColumnRef: ?ColumnRef = null,
        FuncCall: ?FuncCallNode = null,
    };
    // count(*) is a FuncCall with agg_star and no args; the other
    // fields are decoded only to reject what they mark.
    const FuncCallNode = struct {
        funcname: []const NameNode = &.{},
        agg_star: bool = false,
        args: []const std.json.Value = &.{},
        agg_distinct: bool = false,
        agg_filter: ?std.json.Value = null, // FILTER (WHERE ...)
        over: ?std.json.Value = null, // a window function
    };
    // A WHERE operand must be a column reference; anything else
    // (function, arithmetic) leaves ColumnRef null → rejected.
    const ColumnRefWrap = struct { ColumnRef: ?ColumnRef = null };
    const ColumnRef = struct { fields: []const FieldNode = &.{} };
    const FieldNode = struct {
        String: ?struct { sval: String = "" } = null,
        A_Star: ?struct {} = null,
    };
    const FromNode = struct { RangeVar: ?struct { relname: String = "" } = null };
    // One WHERE node: exactly one of the three is present. The mirror
    // is recursive through `args` (a slice, so the type has a size).
    const WhereNode = struct {
        A_Expr: ?AExpr = null,
        BoolExpr: ?BoolExprNode = null,
        NullTest: ?NullTestNode = null,
        const AExpr = struct {
            kind: String = "",
            name: []const NameNode = &.{},
            lexpr: ColumnRefWrap = .{},
            rexpr: struct { A_Const: InsertStmtJson.AConst = .{} } = .{},
        };
        // AND_EXPR / OR_EXPR / NOT_EXPR. libpg_query flattens `a AND b
        // AND c` into ONE node with three args (parse_test asserts it);
        // NOT always carries exactly one.
        const BoolExprNode = struct {
            boolop: String = "",
            args: []const WhereNode = &.{},
        };
        // `x IS NULL` / `x IS NOT NULL`: nulltesttype = IS_NULL | IS_NOT_NULL.
        const NullTestNode = struct {
            arg: ColumnRefWrap = .{},
            nulltesttype: String = "",
        };
    };
};

/// DELETE FROM t [WHERE ...]. `relation` is inline (a RangeVar body,
/// like INSERT); the last three clauses are decoded only to reject them.
const DeleteStmtJson = struct {
    relation: struct { relname: String = "" } = .{},
    whereClause: ?SelectStmtJson.WhereNode = null,
    usingClause: []const std.json.Value = &.{}, // DELETE ... USING
    returningList: []const std.json.Value = &.{}, // RETURNING
    withClause: ?std.json.Value = null, // WITH ... DELETE
};

/// One value-expression node: exactly one of the three is present.
/// Any other node kind (FuncCall, TypeCast, ParamRef, SubLink,
/// SetToDefault, MultiAssignRef) leaves all three null → rejected.
/// Recursive through pointers: std.json allocates a `*T` field.
const ExprNode = struct {
    A_Const: ?InsertStmtJson.AConst = null,
    ColumnRef: ?SelectStmtJson.ColumnRef = null,
    A_Expr: ?AExpr = null,

    const AExpr = struct {
        kind: String = "",
        name: []const NameNode = &.{},
        lexpr: ?*const ExprNode = null, // absent: unary `-x` / `+x`
        rexpr: ?*const ExprNode = null,
    };
};

/// UPDATE t SET col = expr [, ...] [WHERE ...]. Each targetList entry
/// is one SET assignment. The last three clauses are decoded only to
/// reject them.
const UpdateStmtJson = struct {
    relation: struct { relname: String = "" } = .{},
    targetList: []const ResTargetNode = &.{},
    whereClause: ?SelectStmtJson.WhereNode = null,
    fromClause: []const std.json.Value = &.{}, // UPDATE ... FROM
    returningList: []const std.json.Value = &.{}, // RETURNING
    withClause: ?std.json.Value = null, // WITH ... UPDATE

    const ResTargetNode = struct { ResTarget: ResTarget = .{} };
    const ResTarget = struct {
        name: String = "",
        val: ExprNode = .{},
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
/// single-column PRIMARY KEY or UNIQUE parse; any other constraint (NOT
/// NULL, FK, DEFAULT, table-level) is a clean `.invalid`.
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
        var ty = _value.Type.fromName(tyname) orelse
            return invalidFmt(alloc, "unsupported type \"{s}\"", .{tyname});

        // A length modifier: only `char(n)` / `varchar(n)` take one, a
        // single integer in 1..max_type_mod. A lengthless `char` is
        // `char(1)`; a lengthless `varchar` stays `.text`.
        const typmods = cd.typeName.typmods;
        var type_mod: ?u32 = null;
        if (typmods.len > 0) {
            const is_varchar = std.mem.eql(u8, tyname, "varchar");
            if (ty != .bpchar and !is_varchar)
                return invalidFmt(alloc, "type modifier is not allowed for type \"{s}\"", .{tyname});
            if (typmods.len != 1)
                return invalidFmt(alloc, "invalid type modifier for type \"{s}\"", .{tyname});
            const ac = typmods[0].A_Const orelse
                return invalidFmt(alloc, "invalid type modifier for type \"{s}\"", .{tyname});
            const iv = ac.ival orelse
                return invalidFmt(alloc, "invalid type modifier for type \"{s}\"", .{tyname});
            if (iv.ival < 1)
                return invalidFmt(alloc, "length for type {s} must be at least 1", .{tyname});
            if (iv.ival > _value.Type.max_type_mod)
                return invalidFmt(alloc, "length for type {s} cannot exceed {d}", //
                    .{ tyname, _value.Type.max_type_mod });
            type_mod = @intCast(iv.ival);
            if (is_varchar) ty = .varchar;
        } else if (ty == .bpchar) {
            type_mod = 1;
        }

        var pkey = false;
        var unique = false;
        for (cd.constraints) |con| {
            const contype = con.Constraint.contype;
            if (std.mem.eql(u8, contype, "CONSTR_PRIMARY")) {
                pkey = true;
            } else if (std.mem.eql(u8, contype, "CONSTR_UNIQUE")) {
                unique = true;
            } else {
                return invalid(
                    "only PRIMARY KEY and UNIQUE column constraints are supported",
                );
            }
        }
        if (pkey) {
            if (pkey_seen)
                return invalid("multiple PRIMARY KEY columns are not supported");
            pkey_seen = true;
            unique = false; // the PK index already enforces it
        }

        try columns.append(alloc, .{
            .name = cd.colname,
            .ty = ty,
            .type_mod = type_mod,
            .pkey = pkey,
            .unique = unique,
        });
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

/// CREATE [UNIQUE] INDEX. v1: a single plain column, whole-table,
/// default btree — every narrowing (CONCURRENTLY, partial,
/// multi-column, expression, INCLUDE, TABLESPACE, other access
/// methods, DESC / NULLS FIRST ordering) is a clean `.invalid`.
/// ASC / NULLS LAST are accepted: they are this index's semantics
/// already (the NULL tag byte sorts last).
fn parseCreateIndex(alloc: std.mem.Allocator, body: std.json.Value) parser.ParseError!Stmt {
    const is = try fromValue(IndexStmtJson, alloc, body) orelse return .unsupported;

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
        .unique = is.unique,
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

    var columns: ?[]String = null;
    if (is.cols.len > 0) {
        const cols = try alloc.alloc(String, is.cols.len);
        for (is.cols, cols) |ct, *dst| dst.* = ct.ResTarget.name;
        columns = cols;
    }

    var rows: std.ArrayList([]Literal) = .empty;
    for (sel.SelectStmt.valuesLists) |ln| {
        const lits = try alloc.alloc(Literal, ln.List.items.len);
        for (ln.List.items, lits) |item, *lit| lit.* = literalFromValue(item);
        try rows.append(alloc, lits);
    }

    return .{ .insert = .{
        .table = is.relation.relname,
        .columns = columns,
        .rows = try rows.toOwnedSlice(alloc),
    } };
}

/// A VALUES item as a Literal: the constant, or the value function.
fn literalFromValue(node: InsertStmtJson.ValueNode) Literal {
    if (node.A_Const) |ac| return literalFrom(ac);
    const svf = node.SQLValueFunction orelse return .unsupported;
    // The precision forms (CURRENT_TIMESTAMP(3)) keep the full µs;
    // LOCALTIMESTAMP is the same instant on a UTC-only server.
    const ops = [_]String{
        "SVFOP_CURRENT_TIMESTAMP", "SVFOP_CURRENT_TIMESTAMP_N",
        "SVFOP_LOCALTIMESTAMP",    "SVFOP_LOCALTIMESTAMP_N",
    };
    for (ops) |op| if (std.mem.eql(u8, svf.op, op)) return .current_timestamp;
    return .unsupported;
}

fn literalFrom(ac: InsertStmtJson.AConst) Literal {
    if (ac.isnull) return .null_value;
    if (ac.ival) |iv| return .{ .int = iv.ival };
    if (ac.fval) |fv| return .{ .float = fv.fval };
    if (ac.sval) |sv| return .{ .str = sv.sval };
    if (ac.boolval) |bv| return .{ .bool_ = bv.boolval };
    return .unsupported;
}

/// Coerce a parsed literal to a cell of `col`, reusing value.parseValue
/// by rendering non-string literals to text first (int/bool stringified
/// into `a`, so a text-typed column can borrow it). A string column goes
/// through value.coerceString with its length. NULL passes through; a
/// non-literal item is `error.UnsupportedLiteral`.
pub fn litToValue(a: std.mem.Allocator, lit: Literal, col: *const Column) !_value.Value {
    const text: String = switch (lit) {
        .null_value => return .null_value,
        .unsupported => return error.UnsupportedLiteral,
        // Resolved by the executor before coercion (dml.coerceLiteral);
        // a position without a transaction time (SET, WHERE) never
        // produces it.
        .current_timestamp => return error.UnsupportedLiteral,
        .str, .float => |s| s,
        .int => |n| try std.fmt.allocPrint(a, "{d}", .{n}),
        .bool_ => |b| if (b) "t" else "f",
    };
    if (col.ty.isString()) return .{ .text = try _value.coerceString(text, col.ty, col.type_mod) };
    return _value.parseValue(col.ty, text);
}

/// A parsed WHERE tree or a rejection reason (borrowed).
const ParsedWhere = union(enum) {
    where: Where,
    invalid: String,

    fn reject(msg: String) ParsedWhere {
        return .{ .invalid = msg };
    }
};

/// The column a WHERE operand names — the last field of a qualified
/// reference — or null when the operand is not a plain column.
fn columnOf(ref: SelectStmtJson.ColumnRefWrap) ?String {
    const cr = ref.ColumnRef orelse return null;
    if (cr.fields.len == 0) return null;
    const last = cr.fields[cr.fields.len - 1].String orelse return null;
    return last.sval;
}

/// Parse a WHERE tree, shared by SELECT, DELETE, and UPDATE: `column =
/// literal` leaves, `column IS [NOT] NULL`, and AND / OR / NOT over
/// them. Anything else (another operator, a subquery, column-vs-column,
/// a bare boolean column) yields a clean `.invalid` reason; only OOM
/// propagates. AND/OR children land in one arena slice, NOT's in one
/// arena cell.
fn parseWhere(
    alloc: std.mem.Allocator,
    where: SelectStmtJson.WhereNode,
) parser.ParseError!ParsedWhere {
    if (where.BoolExpr) |be| {
        if (std.mem.eql(u8, be.boolop, "NOT_EXPR")) {
            if (be.args.len != 1)
                return ParsedWhere.reject("NOT takes exactly one operand");
            const child = try alloc.create(Where);
            child.* = switch (try parseWhere(alloc, be.args[0])) {
                .where => |x| x,
                .invalid => |msg| return .{ .invalid = msg },
            };
            return .{ .where = .{ .not = child } };
        }
        const is_and = std.mem.eql(u8, be.boolop, "AND_EXPR");
        if (!is_and and !std.mem.eql(u8, be.boolop, "OR_EXPR"))
            return ParsedWhere.reject("unsupported WHERE clause");
        const kids = try alloc.alloc(Where, be.args.len);
        for (be.args, kids) |arg, *kid| {
            kid.* = switch (try parseWhere(alloc, arg)) {
                .where => |x| x,
                .invalid => |msg| return .{ .invalid = msg },
            };
        }
        return .{ .where = if (is_and) .{ .and_ = kids } else .{ .or_ = kids } };
    }

    if (where.NullTest) |nt| {
        const column = columnOf(nt.arg) orelse
            return ParsedWhere.reject("IS NULL operand must be a column");
        const negate = if (std.mem.eql(u8, nt.nulltesttype, "IS_NULL"))
            false
        else if (std.mem.eql(u8, nt.nulltesttype, "IS_NOT_NULL"))
            true
        else
            return ParsedWhere.reject("unsupported WHERE clause");
        return .{ .where = .{ .is_null = .{ .column = column, .negate = negate } } };
    }

    const ae = where.A_Expr orelse
        return ParsedWhere.reject("unsupported WHERE clause");
    if (!std.mem.eql(u8, ae.kind, "AEXPR_OP") or ae.name.len == 0 or
        !std.mem.eql(u8, ae.name[ae.name.len - 1].String.sval, "="))
        return ParsedWhere.reject(
            "only `column = value` comparisons are supported in WHERE",
        );

    const column = columnOf(ae.lexpr) orelse
        return ParsedWhere.reject("WHERE left side must be a column");
    const lit = literalFrom(ae.rexpr.A_Const);
    if (lit == .unsupported)
        return ParsedWhere.reject("WHERE right side must be a literal");
    return .{ .where = .{ .eq = .{ .column = column, .literal = lit } } };
}

/// A parsed SET expression or a rejection reason (borrowed).
const ParsedExpr = union(enum) {
    expr: Expr,
    invalid: String,

    fn reject(msg: String) ParsedExpr {
        return .{ .invalid = msg };
    }
};

/// The operator an A_Expr names, or null for one SET does not do.
fn opFrom(name: String) ?Expr.Op {
    if (std.mem.eql(u8, name, "||")) return .concat;
    if (name.len != 1) return null;
    return switch (name[0]) {
        '+' => .add,
        '-' => .sub,
        '*' => .mul,
        '/' => .div,
        '%' => .mod,
        else => null,
    };
}

/// Parse a SET value: a literal, a column, or `+ - * / % ||` over
/// them, recursively. Anything else (a function, a cast, a
/// parameter, a subquery, DEFAULT, another operator) yields a clean
/// `.invalid` reason; only OOM propagates. Each child lands in one
/// arena cell.
fn parseExpr(
    arloc: std.mem.Allocator,
    node: ExprNode,
) parser.ParseError!ParsedExpr {
    if (node.A_Const) |ac| {
        const lit = literalFrom(ac);
        if (lit == .unsupported)
            return ParsedExpr.reject("unsupported literal in SET");
        return .{ .expr = .{ .literal = lit } };
    }
    if (node.ColumnRef != null) {
        const column = columnOf(.{ .ColumnRef = node.ColumnRef }) orelse
            return ParsedExpr.reject("unsupported column reference in SET");
        return .{ .expr = .{ .column = column } };
    }
    const ae = node.A_Expr orelse return ParsedExpr.reject(
        "SET value must be a literal, a column, or an operator " ++
            "expression over them",
    );
    const bad_op = "only + - * / % || are supported in SET";
    if (!std.mem.eql(u8, ae.kind, "AEXPR_OP") or ae.name.len == 0)
        return ParsedExpr.reject(bad_op);
    const op = opFrom(ae.name[ae.name.len - 1].String.sval) orelse
        return ParsedExpr.reject(bad_op);

    const rhs_node = ae.rexpr orelse return ParsedExpr.reject(bad_op);
    const rhs = try arloc.create(Expr);
    rhs.* = switch (try parseExpr(arloc, rhs_node.*)) {
        .expr => |e| e,
        .invalid => |msg| return .{ .invalid = msg },
    };
    const lhs = try arloc.create(Expr);
    if (ae.lexpr) |lhs_node| {
        lhs.* = switch (try parseExpr(arloc, lhs_node.*)) {
            .expr => |e| e,
            .invalid => |msg| return .{ .invalid = msg },
        };
    } else switch (op) {
        // Unary: `-x` is `0 - x`, `+x` is `x`. (`-1` never gets
        // here — the grammar folds it into the literal.)
        .sub => lhs.* = .{ .literal = .{ .int = 0 } },
        .add => return .{ .expr = rhs.* },
        else => return ParsedExpr.reject(bad_op),
    }
    return .{ .expr = .{ .binop = .{ .op = op, .lhs = lhs, .rhs = rhs } } };
}

/// SELECT [cols|*|count(*)] FROM one_table [WHERE ...] [FOR UPDATE]
/// [LIMIT n]. Joins, subqueries, ORDER BY / GROUP BY / OFFSET /
/// DISTINCT and expression targets are rejected.
fn parseSelect(alloc: std.mem.Allocator, body: std.json.Value) parser.ParseError!Stmt {
    const ss = try fromValue(SelectStmtJson, alloc, body) orelse return .unsupported;

    if (ss.sortClause.len > 0 or ss.groupClause.len > 0 or ss.distinctClause.len > 0 or
        ss.havingClause != null or ss.limitOffset != null)
        return invalid(
            "ORDER BY / GROUP BY / OFFSET / DISTINCT are not supported",
        );

    // LIMIT: an integer literal, or ALL (a NULL constant: no limit).
    // A parameter, an expression, or WITH TIES is refused (the vendored
    // grammar rejects WITH TIES itself today; the check is for one that
    // does not, so it can never pass as a plain LIMIT).
    var limit: ?usize = null;
    if (ss.limitCount) |lc| {
        if (std.mem.eql(u8, ss.limitOption, "LIMIT_OPTION_WITH_TIES"))
            return invalid("FETCH ... WITH TIES is not supported");
        const ac = lc.A_Const orelse
            return invalid("only an integer literal LIMIT is supported");
        if (!ac.isnull) {
            const iv = ac.ival orelse
                return invalid("only an integer literal LIMIT is supported");
            if (iv.ival < 0) return invalid("LIMIT must not be negative");
            limit = @intCast(iv.ival);
        }
    }

    if (ss.fromClause.len != 1)
        return invalid("SELECT must read exactly one table");
    const rv = ss.fromClause[0].RangeVar orelse
        return invalid("only table sources are supported");

    // Every target must be a column reference, except the one aggregate,
    // a lone `count(*)`; a `*` anywhere makes it a star projection.
    var star = false;
    var count_star = false;
    for (ss.targetList) |tn| {
        if (tn.ResTarget.val.FuncCall) |fc| {
            if (!isCountStar(fc))
                return invalid("only count(*) is supported in SELECT");
            count_star = true;
            continue;
        }
        const cr = tn.ResTarget.val.ColumnRef orelse
            return invalid("only column references are supported in SELECT");
        for (cr.fields) |f| {
            if (f.A_Star != null) star = true;
        }
    }
    if (count_star and ss.targetList.len != 1)
        return invalid("count(*) cannot be combined with other targets");
    if (count_star and ss.lockingClause.len > 0)
        return invalid("FOR UPDATE is not allowed with count(*)");

    var cols: std.ArrayList(String) = .empty;
    if (!star and !count_star) {
        for (ss.targetList) |tn| {
            const fields = tn.ResTarget.val.ColumnRef.?.fields;
            if (fields.len == 0 or fields[fields.len - 1].String == null)
                return invalid("unsupported column reference");
            try cols.append(alloc, fields[fields.len - 1].String.?.sval);
        }
    }

    var where: ?Where = null;
    if (ss.whereClause) |node| switch (try parseWhere(alloc, node)) {
        .where => |x| where = x,
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

    const projection: Projection = if (count_star)
        .count_star
    else if (star)
        .star
    else
        .{ .columns = try cols.toOwnedSlice(alloc) };
    return .{ .select = .{
        .table = rv.relname,
        .projection = projection,
        .where = where,
        .for_update = for_update,
        .limit = limit,
    } };
}

/// Whether a target's function call is exactly `count(*)`: no
/// arguments, no DISTINCT, no FILTER, no OVER.
fn isCountStar(fc: SelectStmtJson.FuncCallNode) bool {
    if (fc.funcname.len == 0) return false;
    const name = fc.funcname[fc.funcname.len - 1].String.sval;
    return std.mem.eql(u8, name, "count") and fc.agg_star and
        fc.args.len == 0 and !fc.agg_distinct and
        fc.agg_filter == null and fc.over == null;
}

/// Reject a recognized statement with a static client-facing reason —
/// borrowed straight from .rodata, so no allocation.
fn invalid(msg: String) Stmt {
    return .{ .invalid = msg };
}

/// Reject with a formatted reason, allocated into the parse arena.
fn invalidFmt(
    alloc: std.mem.Allocator,
    comptime fmt: String,
    args: anytype,
) parser.ParseError!Stmt {
    return .{ .invalid = try std.fmt.allocPrint(alloc, fmt, args) };
}

/// BEGIN / START TRANSACTION / COMMIT / ROLLBACK. GOTCHA: TRANS_STMT_BEGIN
/// is protobuf tag 0, so libpg_query OMITS `kind` for a plain BEGIN — the
/// default must be begin. Transaction modes (ISOLATION LEVEL …) are
/// ignored; savepoints and two-phase commit are rejected with a reason.
const TransactionStmtJson = struct {
    kind: String = "TRANS_STMT_BEGIN",
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

/// DELETE FROM one_table [WHERE ...]. USING, RETURNING, WITH, and any
/// WHERE shape parseWhere rejects carry a reason. `WHERE egid = N` parses
/// as an ordinary column comparison — egid resolution stays server-side.
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

    var where: ?Where = null;
    if (ds.whereClause) |node| switch (try parseWhere(alloc, node)) {
        .where => |x| where = x,
        .invalid => |msg| return .{ .invalid = msg },
    };

    return .{ .delete = .{ .table = ds.relation.relname, .where = where } };
}

/// UPDATE one_table SET col = expr [, ...] [WHERE ...]. FROM,
/// RETURNING, WITH, SET values parseExpr does not take, and duplicate
/// SET columns are rejected with a reason.
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

    var where: ?Where = null;
    if (us.whereClause) |node| switch (try parseWhere(alloc, node)) {
        .where => |x| where = x,
        .invalid => |msg| return .{ .invalid = msg },
    };

    var sets: std.ArrayList(SetClause) = .empty;
    for (us.targetList) |tn| {
        const rt = tn.ResTarget;
        if (rt.name.len == 0) return .unsupported;
        for (sets.items) |s| {
            if (std.mem.eql(u8, s.column, rt.name))
                return invalid("duplicate column in SET");
        }
        const value = switch (try parseExpr(alloc, rt.val)) {
            .expr => |e| e,
            .invalid => |msg| return .{ .invalid = msg },
        };
        try sets.append(alloc, .{ .column = rt.name, .value = value });
    }

    return .{ .update = .{
        .table = us.relation.relname,
        .sets = try sets.toOwnedSlice(alloc),
        .where = where,
    } };
}

fn asObject(v: std.json.Value) ?std.json.ObjectMap {
    return switch (v) {
        .object => |o| o,
        else => null,
    };
}

fn asString(v: std.json.Value) ?String {
    return switch (v) {
        .string => |s| s,
        else => null,
    };
}
