const std = @import("std");

/// Version of the per-entry block encoding, stored in TableIndex.format_version
/// and checked when a table is opened. Bump this whenever the entry layout in
/// table/builder.zig changes — including a change to what the meta byte's bits
/// MEAN (kv.zig Meta), since that mis-decodes just as silently as a shifted
/// field would.
///
/// 0 is never legal: an SST written before the field existed reads back 0 (zbuf
/// readers return 0 for slots past size_bytes), and that's exactly the case we
/// need to reject.
///
/// 1: [u16 key_overlap][u16 key_diff_len][u8 meta][key diff][value]
pub const format_version: u32 = 1;

pub const trailer_len: usize = 12;

pub const Error = error{
    CorruptTable,
    OutOfBounds,
};

pub const BlockMeta = struct {
    offset: u32,
    len: u32,
};

pub fn readU16LE(buf: []const u8, off: usize) Error!u16 {
    if (off + 2 > buf.len) return error.OutOfBounds;
    const bytes: *const [2]u8 = @ptrCast(buf[off .. off + 2].ptr);
    return std.mem.readInt(u16, bytes, .little);
}

pub fn readU32LE(buf: []const u8, off: usize) Error!u32 {
    if (off + 4 > buf.len) return error.OutOfBounds;
    const bytes: *const [4]u8 = @ptrCast(buf[off .. off + 4].ptr);
    return std.mem.readInt(u32, bytes, .little);
}

pub fn readU64LE(buf: []const u8, off: usize) Error!u64 {
    if (off + 8 > buf.len) return error.OutOfBounds;
    const bytes: *const [8]u8 = @ptrCast(buf[off .. off + 8].ptr);
    return std.mem.readInt(u64, bytes, .little);
}

pub fn writeU16LE(
    out: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    value: u16,
) !void {
    var bytes: [2]u8 = undefined;
    std.mem.writeInt(u16, &bytes, value, .little);
    try out.appendSlice(allocator, bytes[0..]);
}

pub fn writeU32LE(
    out: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    value: u32,
) !void {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &bytes, value, .little);
    try out.appendSlice(allocator, bytes[0..]);
}

pub fn writeU64LE(
    out: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    value: u64,
) !void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, value, .little);
    try out.appendSlice(allocator, bytes[0..]);
}
