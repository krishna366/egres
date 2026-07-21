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
//! fixed namespace and table_id, a primary that shares a long prefix and
//! differs near its END (egid-style ids), and two versions per primary.
//! Adjacent pairs are compared, so about half the comparisons are decided
//! by the primary and half fall through to the timestamp.

const std = @import("std");
const utils = @import("utils");
const zdb = @import("zdb");
const bench = utils.bench;
const Bench = bench.Bench;
const Key = zdb.kv.Key;

/// A marshaled key ordered without ever being parsed.
///
/// `[ns|tid|primary]` compares bytewise because ns and tid are
/// fixed-width big-endian — byte order IS field order, and a length
/// difference can only come from primary, where shorter-is-less is what
/// std.mem.order gives. The trailing timestamp also compares bytewise,
/// with the result INVERTED, since newer versions sort first. So the
/// whole ordering is two memcmps and no integer parsing at all.
const SerialKey = struct {
    raw: []const u8,

    fn order(a: SerialKey, b: SerialKey) std.math.Order {
        const ap = a.raw[0 .. a.raw.len - 8];
        const bp = b.raw[0 .. b.raw.len - 8];
        switch (std.mem.order(u8, ap, bp)) {
            .eq => {},
            else => |c| return c,
        }
        const at = a.raw[a.raw.len - 8 ..];
        const bt = b.raw[b.raw.len - 8 ..];
        return switch (std.mem.order(u8, at, bt)) {
            .lt => .gt,
            .gt => .lt,
            .eq => .eq,
        };
    }
};

const gpa = std.heap.smp_allocator;
const num_keys: usize = 256; // small enough to stay cache-resident
const versions: usize = 2; // versions per primary

/// Marshal `num_keys` keys whose primaries are `primary_len` bytes and
/// differ only in their last 8 — the egid-shaped case, where a compare
/// must walk the whole common prefix before deciding.
fn buildKeys(a: std.mem.Allocator, primary_len: usize) ![]const []const u8 {
    const out = try a.alloc([]const u8, num_keys);
    const buf = try a.alloc(u8, primary_len);
    defer a.free(buf);

    for (out, 0..) |*slot, i| {
        @memset(buf, 'p');
        var nb: [8]u8 = undefined;
        _ = try std.fmt.bufPrint(&nb, "{d:0>8}", .{i / versions});
        @memcpy(buf[buf.len - 8 ..], &nb);
        const k = Key{
            .namespace = 16,
            .table_id = 17,
            .primary = buf,
            .timestamp = 100 - @as(u64, @intCast(i % versions)),
        };
        slot.* = try k.marshal(a);
    }
    return out;
}

fn freeKeys(a: std.mem.Allocator, keys: []const []const u8) void {
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
