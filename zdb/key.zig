const std = @import("std");

pub const ts_len: usize = @sizeOf(u64);

pub const Error = error{
    BufferTooSmall,
};

pub fn withTs(allocator: std.mem.Allocator, key: []const u8, ts: u64) ![]u8 {
    var out = try allocator.alloc(u8, key.len + ts_len);
    @memcpy(out[0..key.len], key);
    setTs(out, ts) catch unreachable;
    return out;
}

pub fn setTs(key_with_ts: []u8, ts: u64) Error!void {
    if (key_with_ts.len < ts_len) return error.BufferTooSmall;
    const start = key_with_ts.len - ts_len;
    std.mem.writeInt(u64, key_with_ts[start..][0..ts_len], ~ts, .big);
}

pub fn parseTs(key_with_ts: []const u8) u64 {
    std.debug.assert(key_with_ts.len > ts_len);
    const start = key_with_ts.len - ts_len;
    const raw = std.mem.readInt(u64, key_with_ts[start..][0..ts_len], .big);
    return ~raw;
}

pub fn parseKey(key_with_ts: []const u8) []const u8 {
    std.debug.assert(key_with_ts.len > ts_len);
    return key_with_ts[0 .. key_with_ts.len - ts_len];
}

pub fn sameKeyNoTs(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    return std.mem.eql(u8, parseKey(a), parseKey(b));
}

pub fn compareWithTs(a: []const u8, b: []const u8) std.math.Order {
    std.debug.assert(a.len >= ts_len and b.len >= ts_len);
    const key_order = std.mem.order(u8, parseKey(a), parseKey(b));
    if (key_order != .eq) return key_order;
    return std.mem.order(u8, a[a.len - ts_len ..], b[b.len - ts_len ..]);
}

pub fn hashWithTs(key_with_ts: []const u8) u64 {
    std.debug.assert(key_with_ts.len > ts_len);
    return std.hash.Wyhash.hash(0, parseKey(key_with_ts));
}

pub fn hashNoTs(key_no_ts: []const u8) u64 {
    std.debug.assert(key_no_ts.len > 0);
    return std.hash.Wyhash.hash(0, key_no_ts);
}
