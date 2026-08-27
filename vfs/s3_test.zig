const std = @import("std");
const testing = std.testing;
const s3 = @import("vfs").s3;

fn parse(canned: []const u8, method: []const u8, want_body: bool) !s3.S3Store.Response {
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
