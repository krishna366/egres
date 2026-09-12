//! pgwire-shakeout: a minimal Postgres-protocol server that runs the
//! pgwire module against a real client (e.g. psql, asyncpg). The
//! handler is intentionally dumb — for any incoming SQL, it parses
//! via libpg_query and emits one row that echoes what it saw. The
//! point is to surface protocol gaps (extended query, binary format,
//! auth handshake details) without needing a real executor.

const std = @import("std");
const fiber = @import("zfiber");
const pgwire = @import("pgwire");
const utils = @import("utils");
const String = utils.String;

const log = std.log.scoped(.pgwire_shakeout);

const default_addr = "127.0.0.1:5433";

/// Parses each query via libpg_query and emits a single text row
/// describing what it found. On parse error, sends an ErrorResponse
/// in the Postgres SQLSTATE 42601 (syntax_error) class.
const ShakeoutHandler = struct {
    alloc: std.mem.Allocator,

    pub fn onQuery(
        self: *ShakeoutHandler,
        state: *pgwire.ConnState,
        sql: String,
    ) !void {
        log.info("query: {s}", .{sql});
        try self.runQuery(state, sql);
    }

    /// Extended-query Execute. Same shape as onQuery; the params get
    /// logged but otherwise ignored — shakeout doesn't actually run
    /// SQL.
    pub fn onExecute(
        self: *ShakeoutHandler,
        state: *pgwire.ConnState,
        ctx: pgwire.ExecuteCtx,
    ) !void {
        log.info("execute: {s} ({d} params)", .{ ctx.sql, ctx.params.len });
        // Surface what each param looks like on the wire — useful
        // when probing whether a client is sending binary defaults
        // for primitives. The shakeout doesn't actually execute the
        // SQL, so format is informational only.
        for (ctx.params, ctx.param_formats, 0..) |maybe, fmt, i| {
            if (maybe) |bytes| {
                log.info("  param[{d}] fmt={d} ({s}) {d}B", .{
                    i, fmt,
                    if (fmt == 1) "binary" else "text",
                    bytes.len,
                });
            } else {
                log.info("  param[{d}] = NULL", .{i});
            }
        }
        try self.runQuery(state, ctx.sql);
    }

    /// Extended-query Describe. We always emit a `kind` text-column
    /// RowDescription so pgx (and similar clients) know what to
    /// expect from the upcoming Execute. A real executor would parse
    /// the SQL and emit the actual schema.
    pub fn onDescribe(
        self: *ShakeoutHandler,
        state: *pgwire.ConnState,
        ctx: pgwire.DescribeCtx,
    ) !void {
        _ = self;
        log.info("describe ({s}): {s}", .{ @tagName(ctx.kind), ctx.sql });
        const fields = [_]pgwire.FieldDescription{
            .{ .name = "kind", .type_oid = pgwire.oid.text },
        };
        try state.sendRowDescription(&fields);
    }

    fn runQuery(
        self: *ShakeoutHandler,
        state: *pgwire.ConnState,
        sql: String,
    ) !void {
        // Empty / whitespace-only queries get the EmptyQueryResponse
        // tag — psql sends this on bare semicolons.
        const trimmed = std.mem.trim(u8, sql, " \t\r\n;");
        if (trimmed.len == 0) {
            try state.sendEmptyQueryResponse();
            return;
        }

        var info: ?pgwire.ErrorInfo = null;
        var tree = pgwire.parse(self.alloc, sql, &info) catch |err| {
            switch (err) {
                error.SyntaxError => {
                    var details = info.?;
                    defer details.deinit(self.alloc);
                    try state.sendError(.{
                        .code = pgwire.sqlstate.syntax_error,
                        .message = details.message,
                        .position = details.cursor_pos,
                    });
                    return;
                },
                else => return err,
            }
        };
        defer tree.deinit(self.alloc);

        // Walk into stmt[0].stmt and use the first key as the kind.
        const stmts = tree.statements() orelse {
            try state.sendCommandComplete("EMPTY");
            return;
        };
        const kind = firstKind(stmts) orelse "Unknown";

        // Emit one row, one column ("kind"), text format. Real
        // executors would route on kind here; we just echo it back.
        const fields = [_]pgwire.FieldDescription{
            .{ .name = "kind", .type_oid = pgwire.oid.text },
        };
        try state.sendRowDescription(&fields);

        const cols = [_]pgwire.DataValue{
            .{ .text = kind },
        };
        try state.sendDataRow(&cols);

        // Postgres CommandComplete tags are statement-kind specific
        // ("SELECT 1", "INSERT 0 5"). We hardcode the SELECT shape
        // since shakeout never executes anything for real.
        try state.sendCommandComplete("SELECT 1");
    }
};

fn firstKind(stmts: []std.json.Value) ?String {
    if (stmts.len == 0) return null;
    const obj = switch (stmts[0]) {
        .object => |o| o,
        else => return null,
    };
    const wrapper = obj.get("stmt") orelse return null;
    const wrapper_obj = switch (wrapper) {
        .object => |o| o,
        else => return null,
    };
    var it = wrapper_obj.iterator();
    if (it.next()) |entry| return entry.key_ptr.*;
    return null;
}

// Process-wide allocator handle. Set once in `main`, read from spawned
// fibers. Using a shared global is the simplest way to keep the spawn
// arg list to just `(stream, conn_id)` — passing a context struct by
// pointer crashed mid-fiber-cleanup, suggesting a lifetime mismatch
// between the fiber and the listener stack frame.
var g_alloc: std.mem.Allocator = undefined;

fn handleConn(stream: fiber.net.Stream, conn_id: u64) void {
    var s = stream;
    defer s.close();

    log.info("conn #{d} accepted", .{conn_id});
    defer log.info("conn #{d} closed", .{conn_id});

    var handler = ShakeoutHandler{ .alloc = g_alloc };
    pgwire.serve(g_alloc, &s, &handler, .{}) catch |err| {
        log.warn("conn #{d} serve error: {s}", .{ conn_id, @errorName(err) });
    };
}

fn runListener(listen_addr: fiber.net.IpAddress) void {
    const listener = listen_addr.listen(.{ .reuse_address = true }) catch |err| {
        log.err("listen failed: {s}", .{@errorName(err)});
        return;
    };
    defer listener.close();

    const port = listener.port();
    log.info(
        "listening on port {d} (try: psql -h 127.0.0.1 -p {d} -U you -d db)",
        .{ port, port },
    );

    // Per-connection handlers run as fibers; NODELAY is applied to each
    // accepted stream inside fiber.net.accept, so no manual setsockopt.
    var group = fiber.Group(void).init(g_alloc);
    defer group.deinit();

    var conn_id: u64 = 0;
    while (true) {
        const stream = listener.accept(.{}) catch |err| {
            log.err("accept failed: {s}", .{@errorName(err)});
            return;
        };
        conn_id += 1;
        group.spawn(.{}, handleConn, .{ stream, conn_id }) catch |err| {
            log.err("conn #{d}: spawn failed: {s}", .{ conn_id, @errorName(err) });
            stream.close();
        };
    }
}

pub fn main(init: std.process.Init) !void {
    utils.context.initGlobal(init);
    g_alloc = utils.context.g().gpa;

    // Optional positional arg: host:port. Default to loopback:5433
    // (avoiding 5432 in case real Postgres is up).
    const argv = try utils.context.g().argv();
    const addr_str = if (argv.len >= 2) argv[1] else default_addr;

    const listen_addr = try fiber.net.IpAddress.parseIpAndPort(addr_str);

    var rt: fiber.Runtime = undefined;
    try rt.init(g_alloc, .{ .num_workers = 1 });
    defer rt.deinit();

    // runListener loops on accept forever; rt.run() blocks until stop()
    // (which nothing calls here — the server runs until killed).
    _ = try fiber.spawn(.{}, runListener, .{listen_addr});
    rt.run();
}
