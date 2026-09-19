const std = @import("std");
const testing = std.testing;
const s3 = @import("vfs").s3;
const utils = @import("utils");
const String = utils.String;

fn parse(canned: String, method: String, want_body: bool) !s3.S3Store.Response {
    var r = std.Io.Reader.fixed(canned);
    return s3.parseResponse(&r, testing.allocator, method, want_body);
}

test "s3: parse 200 GET with body" {
    var resp = try parse(
        "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nServer: test\r\n\r\nhello",
        "GET",
        true,
    );
    defer resp.deinit(testing.allocator);
    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expectEqualStrings("hello", resp.body.?);
}

test "s3: parse HEAD advertises length but has no body" {
    var resp = try parse(
        "HTTP/1.1 200 OK\r\nContent-Length: 12345\r\n\r\n",
        "HEAD",
        false,
    );
    defer resp.deinit(testing.allocator);
    try testing.expectEqual(@as(u64, 12345), resp.content_length);
    try testing.expect(resp.body == null);
}

test "s3: parse 404 reads the error body for logging" {
    var resp = try parse(
        "HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n\r\n<error/>x",
        "GET",
        false,
    );
    defer resp.deinit(testing.allocator);
    try testing.expectEqual(@as(u16, 404), resp.status);
    try testing.expectEqualStrings("<error/>x", resp.body.?);
}

test "s3: parse 206 partial content" {
    var resp = try parse(
        "HTTP/1.1 206 Partial Content\r\ncontent-length: 3\r\n\r\nabc",
        "GET",
        true,
    );
    defer resp.deinit(testing.allocator);
    try testing.expectEqual(@as(u16, 206), resp.status);
    try testing.expectEqualStrings("abc", resp.body.?);
}

test "s3: chunked responses are rejected" {
    try testing.expectError(error.UnsupportedChunkedEncoding, parse(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
        "GET",
        true,
    ));
}

test "s3: garbage status line is a BadResponse" {
    try testing.expectError(error.BadResponse, parse("nonsense\r\n\r\n", "GET", false));
}

// Keep-alive contract: an unwanted body is DRAINED, so the next response
// on the same connection parses cleanly.
test "s3: unwanted body is drained, next response parses" {
    const canned =
        "HTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\nignored" ++
        "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
    var r = std.Io.Reader.fixed(canned);
    var first = try s3.parseResponse(&r, testing.allocator, "PUT", false);
    defer first.deinit(testing.allocator);
    try testing.expect(first.body == null);
    var second = try s3.parseResponse(&r, testing.allocator, "GET", true);
    defer second.deinit(testing.allocator);
    try testing.expectEqualStrings("hello", second.body.?);
}

test "s3: Connection close and HTTP/1.0 forbid pooling" {
    var resp = try parse(
        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
        "PUT",
        false,
    );
    defer resp.deinit(testing.allocator);
    try testing.expect(resp.conn_close);

    var old = try parse("HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n", "PUT", false);
    defer old.deinit(testing.allocator);
    try testing.expect(old.conn_close);

    var ka = try parse("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n", "PUT", false);
    defer ka.deinit(testing.allocator);
    try testing.expect(!ka.conn_close);
}

// ───────────────────────── ListObjectsV2 ─────────────────────────
//
// The network half of `S3Store.list` needs a server; the parsing half is
// what can go wrong silently, so it is exercised against canned bodies.

const vfs = @import("vfs");
const parsing = s3.list_parsing_for_tests;

fn page(items: String, truncated: bool, next: String) ![]u8 {
    return std.fmt.allocPrint(testing.allocator,
        \\<?xml version="1.0" encoding="UTF-8"?>
        \\<ListBucketResult><Name>depot</Name><IsTruncated>{s}</IsTruncated>
        \\{s}<NextContinuationToken>{s}</NextContinuationToken>
        \\</ListBucketResult>
    , .{ if (truncated) "true" else "false", items, next });
}

fn object(key: String, size: u64, when: String) ![]u8 {
    return std.fmt.allocPrint(
        testing.allocator,
        "<Contents><Key>{s}</Key><LastModified>{s}</LastModified>" ++
            "<Size>{d}</Size><StorageClass>STANDARD</StorageClass></Contents>",
        .{ key, when, size },
    );
}

test "s3: ListObjectsV2 pages concatenate with names, sizes and mtimes" {
    const alloc = testing.allocator;

    const c1 = try object("0000000000000001.sst", 4096, "2026-09-18T12:00:00.000Z");
    defer alloc.free(c1);
    const c2 = try object("0000000000000002.sst", 8192, "2026-09-18T12:00:01.500Z");
    defer alloc.free(c2);
    const c3 = try object("0000000000000003.sst", 16384, "1970-01-01T00:00:00Z");
    defer alloc.free(c3);

    const both = try std.mem.concat(alloc, u8, &.{ c1, c2 });
    defer alloc.free(both);
    const first = try page(both, true, "tok/1+2=");
    defer alloc.free(first);
    const second = try page(c3, false, "");
    defer alloc.free(second);

    var out: std.ArrayList(vfs.ObjectInfo) = .empty;
    defer vfs.freeObjectInfos(alloc, out.toOwnedSlice(alloc) catch unreachable);
    try parsing.appendPage(alloc, first, "", .stat, &out);
    try parsing.appendPage(alloc, second, "", .stat, &out);

    try testing.expectEqual(@as(usize, 3), out.items.len);
    try testing.expectEqualStrings("0000000000000001.sst", out.items[0].name);
    try testing.expectEqual(@as(u64, 4096), out.items[0].stat.?.size);
    try testing.expectEqual(@as(u64, 8192), out.items[1].stat.?.size);
    try testing.expectEqual(@as(u64, 16384), out.items[2].stat.?.size);

    // 2026-09-18T12:00:00Z; the second object is 1.5 s later.
    const t0 = out.items[0].stat.?.mtime_s;
    try testing.expectEqual(t0 + 1, out.items[1].stat.?.mtime_s); // 1.5 s later, whole seconds
    try testing.expectEqual(@as(i64, 0), out.items[2].stat.?.mtime_s);
}

test "s3: a prefix is stripped from listed keys" {
    const alloc = testing.allocator;
    const c1 = try object("node1/0000000000000001.sst", 1, "2026-09-18T12:00:00.000Z");
    defer alloc.free(c1);
    // A key outside the prefix, and the prefix folder marker itself.
    const c2 = try object("other/0000000000000002.sst", 1, "2026-09-18T12:00:00.000Z");
    defer alloc.free(c2);
    const c3 = try object("node1/", 0, "2026-09-18T12:00:00.000Z");
    defer alloc.free(c3);

    const all = try std.mem.concat(alloc, u8, &.{ c1, c2, c3 });
    defer alloc.free(all);
    const body = try page(all, false, "");
    defer alloc.free(body);

    var out: std.ArrayList(vfs.ObjectInfo) = .empty;
    defer vfs.freeObjectInfos(alloc, out.toOwnedSlice(alloc) catch unreachable);
    try parsing.appendPage(alloc, body, "node1", .names, &out);

    try testing.expectEqual(@as(usize, 1), out.items.len);
    try testing.expectEqualStrings("0000000000000001.sst", out.items[0].name);
    // ListObjectsV2 carries size and mtime whatever was asked, so a
    // `.names` listing still reports them.
    try testing.expectEqual(@as(u64, 1), out.items[0].stat.?.size);
}

test "s3: query values are percent-encoded the way SigV4 signs them" {
    const alloc = testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);
    try parsing.encode(alloc, &buf, "tok/1+2=~a_b.c-d");
    try testing.expectEqualStrings("tok%2F1%2B2%3D~a_b.c-d", buf.items);
}
