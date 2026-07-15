//! DDL parsing: SQL text → a small typed `DdlStmt`, via libpg_query (reused
//! from pgwire). Only the statements the store can execute are recognized;
//! everything else (SELECT/INSERT/…) maps to `.unsupported`. Syntax errors
//! propagate as ParseError so the endpoint can answer with a 42601.

const std = @import("std");
const pgwire = @import("pgwire");
const parser = pgwire.parser_mod;

pub const DdlStmt = union(enum) {
    create_database: []const u8, // database name (owned)
    drop_database: []const u8, // database name (owned)
    unsupported,

    pub fn deinit(self: *DdlStmt, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create_database, .drop_database => |n| alloc.free(n),
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
    return .unsupported;
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
