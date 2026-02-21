const std = @import("std");
const compress = @import("compress.zig");
const utils = @import("utils.zig");

fn makePayload(alloc: std.mem.Allocator, len: usize) ![]u8 {
    var out = try alloc.alloc(u8, len);
    var prng = std.Random.DefaultPrng.init(0xBADC0FFEE0DDF00D);
    prng.random().bytes(out);

    // Add repetition so real codecs can compress something meaningful.
    const pat = "egres-egres-egres";
    var i: usize = 0;
    while (i + pat.len <= out.len) : (i += 64) {
        @memcpy(out[i .. i + pat.len], pat);
    }
    return out;
}

fn expectRoundTrip(
    alloc: std.mem.Allocator,
    codec: compress.Codec,
    data: []const u8,
) !void {
    var enc_buf = std.ArrayList(u8).empty;
    defer enc_buf.deinit(alloc);
    const encoded = try compress.compress(alloc, codec, data, &enc_buf);

    var dec_buf = std.ArrayList(u8).empty;
    defer dec_buf.deinit(alloc);
    const decoded = try compress.decompress(alloc, codec, encoded, &dec_buf);

    utils.testPrint(
        "[u.compress] codec={s} input_len={} compressed_len={} output_len={}\n",
        .{ @tagName(codec), data.len, encoded.len, decoded.len },
    );

    try std.testing.expectEqualSlices(u8, data, decoded);
}

test "none compression roundtrip" {
    const data = try makePayload(std.testing.allocator, 128 * 1024);
    defer std.testing.allocator.free(data);
    try expectRoundTrip(std.testing.allocator, .none, data);
}

test "zstd compression roundtrip" {
    const data = try makePayload(std.testing.allocator, 256 * 1024);
    defer std.testing.allocator.free(data);
    try expectRoundTrip(std.testing.allocator, .zstd, data);
}

test "lz4 compression roundtrip" {
    const data = try makePayload(std.testing.allocator, 256 * 1024);
    defer std.testing.allocator.free(data);
    try expectRoundTrip(std.testing.allocator, .lz4, data);
}

test "lz4 detects truncated frame" {
    const data = "hello world hello world hello world";
    var enc_buf = std.ArrayList(u8).empty;
    defer enc_buf.deinit(std.testing.allocator);
    const encoded = try compress.compress(std.testing.allocator, .lz4, data, &enc_buf);

    const truncated = encoded[0 .. encoded.len - 1];
    var dec_buf = std.ArrayList(u8).empty;
    defer dec_buf.deinit(std.testing.allocator);
    try std.testing.expectError(
        error.DecompressionFailed,
        compress.decompress(std.testing.allocator, .lz4, truncated, &dec_buf),
    );
}

test "zstd rejects garbage bytes" {
    const garbage = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };
    var dec_buf = std.ArrayList(u8).empty;
    defer dec_buf.deinit(std.testing.allocator);
    try std.testing.expectError(
        error.DecompressionFailed,
        compress.decompress(std.testing.allocator, .zstd, &garbage, &dec_buf),
    );
}
