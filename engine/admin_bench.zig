//! Protocol parser benchmarks. Measures `proto.readToken` in isolation —
//! no sockets, no allocator contention beyond what the real handler does,
//! no Raft. Goal is to verify the parser can move bytes far faster than
//! the end-to-end pipeline (~24 MiB/s observed in zdb-bench), so we can
//! rule it out as the bottleneck.

const std = @import("std");
const proto = @import("admin.zig");
const utils = @import("utils");
const bench = utils.bench;
const Bench = bench.Bench;

/// Build a set-batch body in memory: `count` records of (key + ' ' + value
/// + '\n'), where key is 24 bytes zero-padded and value is `value_size`
/// bytes of 'v'. Returned slice is heap-allocated; caller owns it.
fn buildBatchBody(alloc: std.mem.Allocator, count: u32, value_size: usize) ![]u8 {
    const key_size: usize = 24;
    const per_pair = key_size + 1 + value_size + 1;
    const body = try alloc.alloc(u8, count * per_pair);
    var off: usize = 0;
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        _ = std.fmt.bufPrint(body[off .. off + key_size], "user{d:0>20}", .{i}) catch unreachable;
        off += key_size;
        body[off] = ' ';
        off += 1;
        @memset(body[off .. off + value_size], 'v');
        off += value_size;
        body[off] = '\n';
        off += 1;
    }
    return body;
}

fn benchReadToken(b: *Bench, count: u32, value_size: usize) !void {
    const body = try buildBatchBody(b.alloc, count, value_size);
    defer b.alloc.free(body);

    b.resetTimer();

    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        var r = std.Io.Reader.fixed(body);
        // Fresh ArrayList per iter — mirrors handleSetBatch's per-request
        // scratch lifecycle, including the realloc chain as it grows.
        var input: std.ArrayList(u8) = .empty;
        defer input.deinit(b.alloc);
        try input.ensureTotalCapacity(b.alloc, 16 * 1024);

        var k: u32 = 0;
        while (k < count * 2) : (k += 1) {
            try input.ensureUnusedCapacity(b.alloc, proto.max_token_size);
            try proto.readToken(&r, &input);
        }
        b.addBytes(body.len);
    }
}

// 256 pairs per batch. Sweep value sizes to expose how the parser scales
// with payload — small values stress per-token overhead; large values are
// dominated by memcpy and the realloc chain.

test "BENCH: protocol readToken 64B values" {
    try benchReadToken(bench.current(), 256, 64);
}

test "BENCH: protocol readToken 1KiB values" {
    try benchReadToken(bench.current(), 256, 1024);
}

test "BENCH: protocol readToken 4KiB values" {
    try benchReadToken(bench.current(), 256, 4096);
}
