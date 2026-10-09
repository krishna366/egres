// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const s3 = @import("vfs").s3;
const utils = @import("utils");
const String = utils.String;

fn parse(
    canned: String,
    method: s3.S3Store.Method,
    sink: s3.BodySink,
) !s3.S3Store.Response {
    var r = std.Io.Reader.fixed(canned);
    return s3.parseResponse(&r, testing.allocator, method, sink);
}

test "s3: parse 200 GET with body" {
    var resp = try parse(
        "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nServer: test\r\n\r\nhello",
        .GET,
        .alloc,
    );
    defer resp.deinit(testing.allocator);
    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expectEqualStrings("hello", resp.body.?);
}

test "s3: parse HEAD advertises length but has no body" {
    var resp = try parse(
        "HTTP/1.1 200 OK\r\nContent-Length: 12345\r\n\r\n",
        .HEAD,
        .discard,
    );
    defer resp.deinit(testing.allocator);
    try testing.expectEqual(@as(u64, 12345), resp.content_length);
    try testing.expect(resp.body == null);
}

test "s3: parse 404 reads the error body for logging" {
    var resp = try parse(
        "HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n\r\n<error/>x",
        .GET,
        .discard,
    );
    defer resp.deinit(testing.allocator);
    try testing.expectEqual(@as(u16, 404), resp.status);
    try testing.expectEqualStrings("<error/>x", resp.body.?);
}

test "s3: parse 206 partial content" {
    var resp = try parse(
        "HTTP/1.1 206 Partial Content\r\ncontent-length: 3\r\n\r\nabc",
        .GET,
        .alloc,
    );
    defer resp.deinit(testing.allocator);
    try testing.expectEqual(@as(u16, 206), resp.status);
    try testing.expectEqualStrings("abc", resp.body.?);
}

test "s3: chunked responses are rejected" {
    try testing.expectError(error.UnsupportedChunkedEncoding, parse(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
        .GET,
        .alloc,
    ));
}

test "s3: garbage status line is a BadResponse" {
    try testing.expectError(
        error.BadResponse,
        parse("nonsense\r\n\r\n", .GET, .discard),
    );
}

// Keep-alive contract: an unwanted body is DRAINED, so the next response
// on the same connection parses cleanly.
test "s3: unwanted body is drained, next response parses" {
    const canned =
        "HTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\nignored" ++
        "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
    var r = std.Io.Reader.fixed(canned);
    var first = try s3.parseResponse(&r, testing.allocator, .PUT, .discard);
    defer first.deinit(testing.allocator);
    try testing.expect(first.body == null);
    var second = try s3.parseResponse(&r, testing.allocator, .GET, .alloc);
    defer second.deinit(testing.allocator);
    try testing.expectEqualStrings("hello", second.body.?);
}

test "s3: Connection close and HTTP/1.0 forbid pooling" {
    var resp = try parse(
        "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
        .PUT,
        .discard,
    );
    defer resp.deinit(testing.allocator);
    try testing.expect(resp.conn_close);

    var old = try parse(
        "HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n",
        .PUT,
        .discard,
    );
    defer old.deinit(testing.allocator);
    try testing.expect(old.conn_close);

    var ka = try parse(
        "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
        .PUT,
        .discard,
    );
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

test "s3: signing region comes from the endpoint host" {
    const eq = testing.expectEqualStrings;
    const region = s3.regionFromHost;
    try eq("us-west-2", region("s3.us-west-2.amazonaws.com"));
    try eq("us-west-2", region("s3-us-west-2.amazonaws.com"));
    try eq("us-east-005", region("s3.us-east-005.backblazeb2.com"));
    try eq("us-east-1", region("s3.amazonaws.com"));
    try eq("us-east-1", region("127.0.0.1"));
    try eq("us-east-1", region("minio.internal"));
}

test "s3: into sink lands the body in the caller's buffer" {
    var buf: [5]u8 = undefined;
    var resp = try parse(
        "HTTP/1.1 206 Partial Content\r\nContent-Length: 5\r\n\r\nhello",
        .GET,
        .{ .into = &buf },
    );
    defer resp.deinit(testing.allocator);
    try testing.expect(resp.body == null);
    try testing.expectEqualStrings("hello", &buf);
}

// A body of the wrong length is drained (the conn stays clean) and
// reported, never partially copied as if it fit.
test "s3: into sink rejects a body of another length" {
    const canned =
        "HTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\nignored" ++
        "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
    var r = std.Io.Reader.fixed(canned);
    var small: [3]u8 = undefined;
    try testing.expectError(
        error.ShortRead,
        s3.parseResponse(&r, testing.allocator, .GET, .{ .into = &small }),
    );
    var buf: [5]u8 = undefined;
    var second = try s3.parseResponse(
        &r,
        testing.allocator,
        .GET,
        .{ .into = &buf },
    );
    defer second.deinit(testing.allocator);
    try testing.expectEqualStrings("hello", &buf);
}

test "s3: Ranges walks a byte range across the chunk grid" {
    const Ranges = s3.S3Store.Ranges;
    // Inside one chunk.
    var one = Ranges.init(100, 120, 30);
    const p = one.next().?;
    try testing.expectEqual(@as(u64, 1), p.idx);
    try testing.expectEqual(@as(u64, 20), p.lo);
    try testing.expectEqual(@as(u64, 50), p.hi);
    try testing.expectEqual(@as(u64, 0), p.at);
    try testing.expect(one.next() == null);

    // Straddling three chunks: 180..320.
    var three = Ranges.init(100, 180, 140);
    const a = three.next().?;
    try testing.expectEqual(@as(u64, 1), a.idx);
    try testing.expectEqual(@as(u64, 80), a.lo);
    try testing.expectEqual(@as(u64, 100), a.hi);
    try testing.expectEqual(@as(u64, 0), a.at);
    const b = three.next().?;
    try testing.expectEqual(@as(u64, 2), b.idx);
    try testing.expectEqual(@as(u64, 0), b.lo);
    try testing.expectEqual(@as(u64, 100), b.hi);
    try testing.expectEqual(@as(u64, 20), b.at);
    const d = three.next().?;
    try testing.expectEqual(@as(u64, 3), d.idx);
    try testing.expectEqual(@as(u64, 0), d.lo);
    try testing.expectEqual(@as(u64, 20), d.hi);
    try testing.expectEqual(@as(u64, 120), d.at);
    try testing.expect(three.next() == null);

    // Empty range: nothing.
    var none = Ranges.init(100, 50, 0);
    try testing.expect(none.next() == null);
}

test "s3: Ranges.gather slices one chunk, stitches across chunks" {
    const S = s3.S3Store;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arloc = arena.allocator();
    // Chunks of 100 bytes; byte i of the object holds i % 251.
    var chunks: S.ChunkMap = .empty;
    for (0..3) |idx| {
        const buf = try arloc.alloc(u8, 100);
        for (buf, 0..) |*b, i| b.* = @intCast((idx * 100 + i) % 251);
        try chunks.put(arloc, idx, .{
            .idx = idx,
            .lo = 0,
            .hi = 100,
            .buf = buf,
        });
    }
    const check = struct {
        fn run(data: []const u8, off: usize) !void {
            for (data, 0..) |b, i| {
                try testing.expectEqual(@as(u8, @intCast((off + i) % 251)), b);
            }
        }
    };

    // Inside chunk 1: a slice into its buffer, no copy.
    var one = S.Ranges.init(100, 120, 30);
    const in_one = try one.gather(arloc, &chunks);
    try testing.expectEqual(@as(usize, 30), in_one.len);
    try testing.expectEqual(chunks.get(1).?.buf.ptr + 20, in_one.ptr);
    try check.run(in_one, 120);

    // 180..300 crosses chunks 1 and 2: stitched into a fresh buffer.
    var two = S.Ranges.init(100, 180, 120);
    const across = try two.gather(arloc, &chunks);
    try testing.expectEqual(@as(usize, 120), across.len);
    try check.run(across, 180);

    var none = S.Ranges.init(100, 50, 0);
    const empty = try none.gather(arloc, &chunks);
    try testing.expectEqual(@as(usize, 0), empty.len);
}

test "s3: planChunks wants spans, or whole chunks for the cache" {
    const S = s3.S3Store;
    const Req = @import("vfs").FileHandle.ReadRequest;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    // Chunks of 100 over a 250-byte object. Two requests in chunk 0,
    // one straddling chunks 1 and 2 (the tail chunk is 50 bytes).
    const reqs = [_]Req{
        .{ .offset = 70, .len = 10 },
        .{ .offset = 10, .len = 20 },
        .{ .offset = 190, .len = 30 },
    };

    const spans = try S.planChunks(arena.allocator(), 100, 250, &reqs, false);
    try testing.expectEqual(@as(usize, 3), spans.count());
    try testing.expectEqual(@as(u64, 10), spans.get(0).?.lo);
    try testing.expectEqual(@as(u64, 80), spans.get(0).?.hi);
    try testing.expectEqual(@as(usize, 70), spans.get(0).?.buf.len);
    try testing.expectEqual(@as(u64, 90), spans.get(1).?.lo);
    try testing.expectEqual(@as(u64, 100), spans.get(1).?.hi);
    try testing.expectEqual(@as(u64, 0), spans.get(2).?.lo);
    try testing.expectEqual(@as(u64, 20), spans.get(2).?.hi);

    const whole = try S.planChunks(arena.allocator(), 100, 250, &reqs, true);
    try testing.expectEqual(@as(u64, 0), whole.get(0).?.lo);
    try testing.expectEqual(@as(u64, 100), whole.get(0).?.hi);
    try testing.expectEqual(@as(u64, 0), whole.get(1).?.lo);
    try testing.expectEqual(@as(u64, 100), whole.get(1).?.hi);
    try testing.expectEqual(@as(u64, 50), whole.get(2).?.hi); // tail
}
