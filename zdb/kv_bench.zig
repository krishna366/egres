// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Key ordering: parsed struct vs marshaled bytes.
//!
//! Three ways to answer "which of these two keys sorts first":
//!
//!   Key pre-parsed — both operands are already Key structs, so ordering
//!                    touches fields only. What level.zig bounds checks
//!                    and checkEndKey do.
//!   Key parse-each — Key.init both operands, then order. What the
//!                    block-index binary search does per probe.
//!   SerialKey      — order the marshaled bytes directly, no parse. See
//!                    the struct below for why that is even legal.
//!
//! Keys are shaped like the SQL row keys the engine actually stores: a
//! fixed namespace and table_id, and a primary that shares a long prefix
//! and differs near its END (egid-style ids). Adjacent pairs are
//! compared, so every comparison walks the shared prefix before the
//! primary decides it.

const std = @import("std");
const utils = @import("utils");
const fiber = @import("zfiber");
const String = utils.String;
const zdb = @import("zdb");
const bench = fiber.bench;
const Bench = bench.Bench;
const Key = zdb.kv.Key;

/// A marshaled key ordered without ever being parsed.
///
/// `[ns|tid|primary]` compares bytewise because ns and tid are
/// fixed-width big-endian — byte order IS field order, and a length
/// difference can only come from primary, where shorter-is-less is what
/// std.mem.order gives. So the whole ordering is one memcmp and no
/// integer parsing at all.
const SerialKey = struct {
    raw: String,

    fn order(a: SerialKey, b: SerialKey) std.math.Order {
        return std.mem.order(u8, a.raw, b.raw);
    }
};

const gpa = std.heap.smp_allocator;
const num_keys: usize = 256; // small enough to stay cache-resident

/// Marshal `num_keys` keys whose primaries are `primary_len` bytes and
/// differ only in their last 8 — the egid-shaped case, where a compare
/// must walk the whole common prefix before deciding.
fn buildKeys(a: std.mem.Allocator, primary_len: usize) ![]const String {
    const out = try a.alloc(String, num_keys);
    const buf = try a.alloc(u8, primary_len);
    defer a.free(buf);

    for (out, 0..) |*slot, i| {
        @memset(buf, 'p');
        var nb: [8]u8 = undefined;
        _ = try std.fmt.bufPrint(&nb, "{d:0>8}", .{i});
        @memcpy(buf[buf.len - 8 ..], &nb);
        const k = Key{
            .namespace = 16,
            .table_id = 17,
            .primary = buf,
        };
        slot.* = try k.marshal(a);
    }
    return out;
}

fn freeKeys(a: std.mem.Allocator, keys: []const String) void {
    for (keys) |k| a.free(k);
    a.free(keys);
}

fn benchPreParsed(b: *Bench, primary_len: usize) !void {
    const a = gpa;
    const raws = try buildKeys(a, primary_len);
    defer freeKeys(a, raws);

    const keys = try a.alloc(Key, num_keys);
    defer a.free(keys);
    for (raws, 0..) |raw, i| keys[i] = Key.init(raw);

    b.resetTimer();
    var i: u64 = 0;
    var sink: u64 = 0;
    while (i < b.n) : (i += 1) {
        const x = i % num_keys;
        const y = (i + 1) % num_keys;
        sink +%= @intFromEnum(keys[x].order(keys[y]));
    }
    std.mem.doNotOptimizeAway(sink);
}

fn benchParseEach(b: *Bench, primary_len: usize) !void {
    const a = gpa;
    const raws = try buildKeys(a, primary_len);
    defer freeKeys(a, raws);

    b.resetTimer();
    var i: u64 = 0;
    var sink: u64 = 0;
    while (i < b.n) : (i += 1) {
        const x = Key.init(raws[i % num_keys]);
        const y = Key.init(raws[(i + 1) % num_keys]);
        sink +%= @intFromEnum(x.order(y));
    }
    std.mem.doNotOptimizeAway(sink);
}

fn benchSerial(b: *Bench, primary_len: usize) !void {
    const a = gpa;
    const raws = try buildKeys(a, primary_len);
    defer freeKeys(a, raws);

    b.resetTimer();
    var i: u64 = 0;
    var sink: u64 = 0;
    while (i < b.n) : (i += 1) {
        const x = SerialKey{ .raw = raws[i % num_keys] };
        const y = SerialKey{ .raw = raws[(i + 1) % num_keys] };
        sink +%= @intFromEnum(x.order(y));
    }
    std.mem.doNotOptimizeAway(sink);
}

// 8-byte primary = an egid row key; 32-byte = a wider composite key.
test "BENCH: order Key pre-parsed 8B primary" {
    try benchPreParsed(bench.current(), 8);
}
test "BENCH: order Key parse-each 8B primary" {
    try benchParseEach(bench.current(), 8);
}
test "BENCH: order SerialKey bytes 8B primary" {
    try benchSerial(bench.current(), 8);
}

test "BENCH: order Key pre-parsed 32B primary" {
    try benchPreParsed(bench.current(), 32);
}
test "BENCH: order Key parse-each 32B primary" {
    try benchParseEach(bench.current(), 32);
}
test "BENCH: order SerialKey bytes 32B primary" {
    try benchSerial(bench.current(), 32);
}
