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

pub const DdlStmt = union(enum) {
    create_database: []const u8, // database name (owned)
    drop_database: []const u8, // database name (owned)
    create_table: CreateTable,
    drop_table: DropTable,
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
            .unsupported => {},
        }
    }
};

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
