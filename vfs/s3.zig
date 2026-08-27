//! S3-compatible object store client — the `s3` FileStore backend. A
//! minimal HTTP/1.1 client over fiber.net (connection per request,
//! `Connection: close`) with SigV4-signed PUT/GET/HEAD/DELETE and
//! path-style URLs (`/bucket/prefix/key`). Deliberate v1 limits, each a
//! recorded follow-up: plain HTTP only (no TLS in the runtime; covers
//! SeaweedFS / in-cluster / terminating proxies), endpoint must be an
//! `ip:port` literal (no DNS in the fiber runtime), Content-Length bodies
//! only (chunked → error), single PUT ≤ 5 GiB (no multipart).
//!
//! Credentials come from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY at
//! init. Local testing: SeaweedFS — `weed server -s3` (see
//! scripts/cluster_e2e.sh).

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const sigv4 = @import("sigv4.zig");

const log = std.log.scoped(.s3);

const max_put_bytes: u64 = 5 << 30; // single-PUT ceiling (S3 hard limit)
const attempts_max = 3; // connect/5xx retries per request
const pool_cap = 8; // idle keep-alive connections held per store
const idle_ttl_ms = 30_000; // drop idle conns older than this at checkout

pub const S3Config = struct {
    /// `ip:port` literal, e.g. "127.0.0.1:8333".
    endpoint: []const u8,
    bucket: []const u8,
    region: []const u8 = "us-east-1",
    /// Optional key prefix inside the bucket. Unreserved URI characters
    /// only — keys are used verbatim in the request path (no
    /// percent-encoding pass).
    prefix: []const u8 = "",
};

pub const S3Store = struct {
    a: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    endpoint: []const u8,
    bucket: []const u8,
    region: []const u8,
    prefix: []const u8,
    // Environ-backed slices: process lifetime, never freed.
    access_key: []const u8,
    secret_key: []const u8,
    // Keep-alive connection pool, shared by every caller (prefetch, apply,
    // recovery, uploads, Object reads). A conn is reusable only after its
    // response was fully drained — parseResponse guarantees that.
    pool_mu: fiber.Mutex(.untracked) = .{},
    idle: std.ArrayList(IdleConn) = .empty,

    const IdleConn = struct {
        stream: fiber.net.Stream,
        since_ms: i64,
    };

    pub fn init(a: std.mem.Allocator, cfg: S3Config) !S3Store {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const arloc = arena.allocator();

        const ctx = utils.context.g();
        const access = ctx.env("AWS_ACCESS_KEY_ID") orelse "";
        const secret = ctx.env("AWS_SECRET_ACCESS_KEY") orelse "";
        if (access.len == 0 or secret.len == 0) {
            log.err("s3 store requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the environment", .{});
            return error.MissingCredentials;
        }
        // Fail fast on a bad endpoint instead of erroring per request.
        _ = fiber.net.IpAddress.parseIpAndPort(cfg.endpoint) catch {
            log.err("s3 endpoint must be an ip:port literal, got '{s}'", .{cfg.endpoint});
            return error.BadEndpoint;
        };
        var ret: S3Store = .{
            .a = a,
            // Placeholder — the arena is moved in AFTER the literal, once
            // every arloc allocation above has mutated its state; copying
            // it earlier would snapshot a state missing those buffers.
            .arena = undefined,
            .endpoint = try arloc.dupe(u8, cfg.endpoint),
            .bucket = try arloc.dupe(u8, cfg.bucket),
            .region = try arloc.dupe(u8, cfg.region),
            .prefix = try arloc.dupe(u8, cfg.prefix),
            .access_key = access,
            .secret_key = secret,
            .idle = try std.ArrayList(IdleConn).initCapacity(arloc, pool_cap),
        };
        ret.arena = arena;
        return ret;
    }

    pub fn deinit(self: *S3Store) void {
        for (self.idle.items) |c| c.stream.close();
        self.arena.deinit();
        self.pool_mu.deinit();
    }

    /// Get a connection: a pooled keep-alive one when available, else a
    /// fresh dial. Pooled conns that idled past the TTL are discarded
    /// (servers close quiet keep-alives; a dead one would waste a retry).
    /// `fresh` skips the pool entirely — retries pass true so a batch of
    /// stale idle conns can never exhaust the retry budget.
    fn getConn(self: *S3Store, fresh: bool) !fiber.net.Stream {
        if (!fresh) {
            // The block-scoped defer unlocks before the dial below.
            self.pool_mu.lock(@src());
            defer self.pool_mu.unlock();

            const now = fiber.coarseMs();
            while (self.idle.pop()) |c| {
                if (now - c.since_ms > idle_ttl_ms) {
                    c.stream.close();
                    continue;
                }
                return c.stream;
            }
        }
        const addr = fiber.net.IpAddress.parseIpAndPort(self.endpoint) catch
            return error.BadEndpoint;
        return addr.connect(.{});
    }

    /// Return a healthy, fully-drained connection to the pool (or close it
    /// when the pool is full).
    fn putConn(self: *S3Store, stream: fiber.net.Stream) void {
        self.pool_mu.lock(@src());
        defer self.pool_mu.unlock();

        // A pooled conn that has spent 70% of its idle TTL is a fair
        // displacement victim — it would likely be discarded at checkout
        // anyway. Comptime integer math (7/10 of 30s = 21s exactly), so
        // the i64 time comparison never touches floats.
        const displace_age_ms = idle_ttl_ms * 7 / 10;
        const now = fiber.coarseMs();
        if (self.idle.items.len >= pool_cap) {
            for (self.idle.items, 0..) |prev, i| {
                if (now - prev.since_ms < displace_age_ms) {
                    continue;
                }
                // Displace a stale or soon to be stale connection.
                prev.stream.close();
                self.idle.items[i] = .{
                    .stream = stream,
                    .since_ms = now,
                };
                return;
            }
            stream.close();
            return;
        }
        self.idle.appendAssumeCapacity(.{
            .stream = stream,
            .since_ms = now,
        });
    }

    pub fn put(self: *S3Store, name: []const u8, data: []const u8) !void {
        if (data.len > max_put_bytes) return error.ObjectTooLarge;
        var resp = try self.request("PUT", name, data, null, false);
        defer resp.deinit(self.a);
        if (resp.status != 200) return self.statusErr("PUT", name, &resp);
    }

    pub fn exists(self: *S3Store, name: []const u8) !bool {
        var resp = try self.request("HEAD", name, null, null, false);
        defer resp.deinit(self.a);

        return switch (resp.status) {
            200 => true,
            404 => false,
            else => self.statusErr("HEAD", name, &resp),
        };
    }

    pub fn delete(self: *S3Store, name: []const u8) !void {
        var resp = try self.request("DELETE", name, null, null, false);
        defer resp.deinit(self.a);
        // S3 DELETE is idempotent: 204 whether or not the key existed.
        switch (resp.status) {
            200, 204, 404 => {},
            else => return self.statusErr("DELETE", name, &resp),
        }
    }

    /// HEAD for size + existence; ranged GETs happen per read().
    pub fn head(self: *S3Store, name: []const u8) !u64 {
        var resp = try self.request("HEAD", name, null, null, false);
        defer resp.deinit(self.a);

        return switch (resp.status) {
            200 => resp.content_length,
            404 => error.FileNotFound,
            else => self.statusErr("HEAD", name, &resp),
        };
    }

    /// Ranged GET of [offset, offset+len). Caller owns the bytes.
    pub fn readRange(self: *S3Store, a: std.mem.Allocator, name: []const u8, offset: u64, len: u64) ![]u8 {
        var resp = try self.request("GET", name, null, .{ offset, len }, true);
        errdefer resp.deinit(self.a);
        switch (resp.status) {
            // 206 for a partial range; some servers reply 200 when the
            // range covers the whole object.
            200, 206 => {},
            404 => {
                resp.deinit(self.a);
                return error.FileNotFound;
            },
            else => {
                const e = self.statusErr("GET", name, &resp);
                resp.deinit(self.a);
                return e;
            },
        }
        const body = resp.body orelse return error.MissingBody;
        if (body.len != len) {
            log.err("s3 GET {s}: range asked {d} bytes, got {d}", .{ name, len, body.len });
            resp.deinit(self.a);
            return error.ShortRead;
        }
        resp.body = null; // ownership moves to the caller
        defer resp.deinit(self.a);

        if (a.ptr == self.a.ptr) return body;
        defer self.a.free(body);
        return a.dupe(u8, body);
    }

    fn statusErr(self: *S3Store, method: []const u8, name: []const u8, resp: *const Response) error{S3Error} {
        _ = self;
        log.err("s3 {s} {s}: status={d} body={s}", .{
            method, name, resp.status, if (resp.body) |b| b else "",
        });
        return error.S3Error;
    }

    pub const Response = struct {
        status: u16,
        content_length: u64 = 0,
        body: ?[]u8 = null,
        /// Server refused keep-alive (Connection: close / HTTP/1.0) —
        /// the conn must not go back to the pool.
        conn_close: bool = false,

        pub fn deinit(self: *Response, a: std.mem.Allocator) void {
            if (self.body) |b| a.free(b);
            self.body = null;
        }
    };

    /// One signed HTTP request over a pooled keep-alive connection.
    /// Bounded retries on connect failures and 5xx; definitive statuses
    /// (2xx/4xx) return immediately. Only the FIRST attempt may use a
    /// pooled conn — a failure closes it and every retry dials fresh, so a
    /// batch of stale idle conns can never exhaust the retry budget.
    /// `range` rides unsigned (SigV4 requires host + x-amz-* only).
    fn request(
        self: *S3Store,
        method: []const u8,
        name: []const u8,
        body: ?[]const u8,
        range: ?[2]u64,
        want_body: bool,
    ) !Response {
        var attempt: u32 = 0;
        while (true) : (attempt += 1) {
            const stream = self.getConn(attempt > 0) catch |err| {
                if (attempt + 1 >= attempts_max) return err;
                log.warn("s3 {s} {s}: connect: {s}, retrying", .{ method, name, @errorName(err) });
                fiber.sleepMs(100 * (attempt + 1));
                continue;
            };
            const resp = self.requestOnce(stream, method, name, body, range, want_body) catch |err| {
                stream.close();
                if (attempt + 1 >= attempts_max) return err;
                log.warn("s3 {s} {s}: {s}, retrying", .{ method, name, @errorName(err) });
                fiber.sleepMs(100 * (attempt + 1));
                continue;
            };
            // The response is fully drained — the conn is reusable unless
            // the server said otherwise.
            if (resp.conn_close) stream.close() else self.putConn(stream);
            if (resp.status >= 500 and attempt + 1 < attempts_max) {
                var r = resp;
                r.deinit(self.a);
                log.warn("s3 {s} {s}: status={d}, retrying", .{ method, name, resp.status });
                fiber.sleepMs(100 * (attempt + 1));
                continue;
            }
            return resp;
        }
    }

    fn requestOnce(
        self: *S3Store,
        stream: fiber.net.Stream,
        method: []const u8,
        name: []const u8,
        body: ?[]const u8,
        range: ?[2]u64,
        want_body: bool,
    ) !Response {
        const a = self.a;
        const path = if (self.prefix.len > 0)
            try std.fmt.allocPrint(a, "/{s}/{s}/{s}", .{ self.bucket, self.prefix, name })
        else
            try std.fmt.allocPrint(a, "/{s}/{s}", .{ self.bucket, name });
        defer a.free(path);

        const amz = sigv4.AmzDate.fromEpochSeconds(@intCast(@divTrunc(fiber.coarseMs(), 1000)));
        const payload_hash = sigv4.hashHex(body orelse "");
        const auth = try sigv4.authorization(
            a,
            self.access_key,
            self.secret_key,
            self.region,
            method,
            path,
            self.endpoint,
            &payload_hash,
            &amz,
        );
        defer a.free(auth);

        var head_buf: std.ArrayList(u8) = .empty;
        defer head_buf.deinit(a);
        // Keep-alive (the HTTP/1.1 default — no Connection header): the
        // caller pools the conn after the response is fully drained.
        try head_buf.print(a, "{s} {s} HTTP/1.1\r\n" ++
            "Host: {s}\r\nx-amz-date: {s}\r\nx-amz-content-sha256: {s}\r\n" ++
            "Authorization: {s}\r\nContent-Length: {d}\r\n", .{
            method,                     path,          self.endpoint,
            &amz.stamp,                 &payload_hash, auth,
            if (body) |b| b.len else 0,
        });
        if (range) |r| {
            try head_buf.print(a, "Range: bytes={d}-{d}\r\n", .{ r[0], r[0] + r[1] - 1 });
        }
        try head_buf.appendSlice(a, "\r\n");

        var wbuf: [4096]u8 = undefined;
        var w = stream.writer(&wbuf);
        w.interface.writeAll(head_buf.items) catch return error.BrokenPipe;

        if (body) |b| w.interface.writeAll(b) catch return error.BrokenPipe;
        w.interface.flush() catch return error.BrokenPipe;

        var rbuf: [4096]u8 = undefined;
        var r = stream.reader(&rbuf);
        return parseResponse(&r.interface, a, method, want_body);
    }
};

/// Parse one HTTP/1.1 response: status line, headers, then the body when
/// `want_body` (or on an error status, whose XML body we log) — HEAD
/// advertises Content-Length but never carries a body. ALWAYS consumes
/// the response through its final body byte (unwanted bodies are
/// discarded), so on return the connection is clean for the next request.
/// Pub for tests (canned buffers via std.Io.Reader.fixed).
pub fn parseResponse(
    reader: *std.Io.Reader,
    a: std.mem.Allocator,
    method: []const u8,
    want_body: bool,
) !S3Store.Response {
    // Status line: "HTTP/1.1 200 OK".
    const status_line = reader.takeDelimiterInclusive('\n') catch return error.BadResponse;
    if (status_line.len < 12 or !std.mem.startsWith(
        u8,
        status_line,
        "HTTP/1.",
    )) return error.BadResponse;
    const status = std.fmt.parseInt(
        u16,
        status_line[9..12],
        10,
    ) catch return error.BadResponse;

    var resp = S3Store.Response{ .status = status };
    // HTTP/1.0 has no keep-alive by default.
    var chunked = false;
    resp.conn_close = std.mem.startsWith(u8, status_line, "HTTP/1.0");

    while (true) {
        const line_raw = reader.takeDelimiterInclusive('\n') catch return error.BadResponse;
        const line = std.mem.trimEnd(u8, line_raw, "\r\n");
        if (line.len == 0) break;

        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const hname = line[0..colon];
        const hval = std.mem.trim(u8, line[colon + 1 ..], " ");

        if (std.ascii.eqlIgnoreCase(hname, "content-length")) {
            resp.content_length = std.fmt.parseInt(u64, hval, 10) catch return error.BadResponse;
        } else if (std.ascii.eqlIgnoreCase(hname, "transfer-encoding")) {
            chunked = true;
        } else if (std.ascii.eqlIgnoreCase(hname, "connection")) {
            if (std.ascii.eqlIgnoreCase(hval, "close")) resp.conn_close = true;
        }
    }
    if (chunked) return error.UnsupportedChunkedEncoding;

    const has_body = resp.content_length > 0 and !std.mem.eql(u8, method, "HEAD");
    if (has_body) {
        if (want_body or resp.status >= 300) {
            const buf = try a.alloc(u8, @intCast(resp.content_length));
            errdefer a.free(buf);
            reader.readSliceAll(buf) catch return error.BadResponse;
            resp.body = buf;
        } else {
            // Drain the unwanted body so the connection stays reusable.
            reader.discardAll64(resp.content_length) catch return error.BadResponse;
        }
    }
    return resp;
}
