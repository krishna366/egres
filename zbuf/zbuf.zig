const std = @import("std");

pub const Error = error{
    OutOfBounds,
    Invalid,
};

pub const Ownership = enum {
    borrowed,
    owned,
};

pub const NullOffset: u32 = 0xFFFF_FFFF;

pub const MessageTail = struct {
    table_id: u16,
};

// 2-byte magic ("ZB" little-endian) — enough to discriminate zbuf bytes from
// arbitrary noise without paying for a 4-byte signature on every message.
pub const message_magic: u16 = 0x425A;
// 4-byte tail: u16 table_id + u16 magic. The root table is required to live
// at payload offset 0. appendMessageTail accepts the caller's offset only to
// validate that invariant; the offset is not stored.
pub const message_tail_size: u16 = 4;
const payload_alignment: usize = 4;

pub fn appendMessageTail(
    list: *std.ArrayList(u8),
    alloc: std.mem.Allocator,
    table_id: u16,
    offset: u32,
) !void {
    if (offset != 0) return Error.Invalid;
    const start = list.items.len;
    try appendZeros(list, alloc, message_tail_size);
    try writeU16At(list.items, start + 0, table_id);
    try writeU16At(list.items, start + 2, message_magic);
}

pub fn readMessageTail(buf: []const u8) !MessageTail {
    if (buf.len < message_tail_size) return Error.OutOfBounds;
    const start = buf.len - message_tail_size;
    const magic = try readU16(buf, start + 2);
    if (magic != message_magic) return Error.Invalid;
    const table_id = try readU16(buf, start + 0);
    return .{ .table_id = table_id };
}

// The single hash function used for ALL framing and integrity checks in
// this project. Centralizing here means one decision, one comment, one
// place to swap implementations.
//
// Choice: std.hash.XxHash3 (Zig stdlib, AVX2-vectorized via @Vector(8, u64)).
// ~67 GiB/s on Zen 3 — ~5× faster than scalar SSE4.2 CRC32C, ~115× faster
// than software CRC. Full u64 output gives a 2⁻⁶⁴ false-negative rate on
// random/torn-write corruption (vs. 2⁻³² for a 32-bit CRC). CRC32C retains
// a theoretical edge on single-bit and small-burst errors (provably 100%
// vs. xxhash3's 1 − 2⁻⁶⁴) but the gap is ~10⁻¹⁰ — irrelevant at NVMe
// error rates.
pub fn hash(payload: []const u8) u64 {
    return std.hash.XxHash3.hash(0, payload);
}

/// 12-byte self-describing prefix for an arbitrary serialized payload:
/// `[u64 hash][u32 length][payload bytes]`. Lets the reader bound the
/// payload inside a larger fixed-size region (e.g. a zero-padded WAL
/// header) and verify it with one hash pass — no magic number / separate
/// length file needed.
///
/// All four items below (hdr_size, write, append, read) operate on the
/// same wire format; they're co-located so a future format tweak only
/// needs to touch one block.
pub const Frame = struct {
    pub const hdr_size: u16 = 12;

    /// In-place writer: place the [hash | len | payload] frame at the
    /// start of `dest`. `dest` must be at least `hdr_size + payload.len`
    /// bytes long. Bytes after the payload are left untouched, so callers
    /// using a fixed-size region should @memset the region first.
    ///
    /// Zero-copy fast path: if `payload` already aliases the destination
    /// slot (e.g. the caller wrapped `dest[hdr_size..]` as an ArrayList
    /// build buffer and wrote the payload into it directly), the copy is
    /// skipped. One API serves both "build elsewhere, copy in" and "build
    /// in place, just stamp the prefix" callers.
    pub fn write(dest: []u8, payload: []const u8) !void {
        if (payload.len > std.math.maxInt(u32)) return Error.OutOfBounds;
        if (dest.len < @as(usize, hdr_size) + payload.len) return Error.OutOfBounds;
        try writeU64At(dest, 0, hash(payload));
        try writeU32At(dest, 8, @intCast(payload.len));
        const slot = dest[hdr_size .. hdr_size + payload.len];
        if (payload.ptr != slot.ptr) @memcpy(slot, payload);
    }

    /// Streaming writer: append a framed payload onto an ArrayList.
    /// Mirrors `write` for callers that build their output incrementally.
    /// Returns the total number of bytes appended (prefix + payload).
    pub fn append(
        list: *std.ArrayList(u8),
        alloc: std.mem.Allocator,
        payload: []const u8,
    ) !usize {
        if (payload.len > std.math.maxInt(u32)) return Error.OutOfBounds;
        try list.ensureUnusedCapacity(alloc, hdr_size + payload.len);
        const start = list.items.len;
        const sz = hdr_size + payload.len;
        list.items.len += sz;
        try write(list.items[start..], payload);
        return sz;
    }

    /// The decoded [hash | len] prefix of a frame.
    pub const Prefix = struct {
        hash: u64,
        len: u32,

        /// Bytes the whole frame takes: prefix and payload.
        pub fn frameLen(self: Prefix) usize {
            return @as(usize, hdr_size) + @as(usize, self.len);
        }
    };

    /// Decode the prefix at the start of `buf`. Touches only the first
    /// `hdr_size` bytes, so `buf` need not hold the payload: a caller
    /// that has read only the start of a frame learns from `frameLen`
    /// how much to read for `read`.
    pub fn readPrefix(buf: []const u8) !Prefix {
        if (buf.len < hdr_size) return Error.OutOfBounds;
        return .{ .hash = try readU64(buf, 0), .len = try readU32(buf, 8) };
    }

    /// Reads the frame prefix from `buf`, validates the encoded length fits
    /// within `buf`, and verifies the hash against the payload bytes.
    /// Returns a slice into `buf` covering just the payload (no copy).
    pub fn read(buf: []const u8) ![]const u8 {
        const prefix = try readPrefix(buf);
        const end = prefix.frameLen();
        if (end > buf.len) return Error.OutOfBounds;

        const payload = buf[hdr_size..end];
        if (hash(payload) != prefix.hash) return Error.Invalid;

        return payload;
    }
};
pub fn readU8(buf: []const u8, off: usize) !u8 {
    if (off + 1 > buf.len) return Error.OutOfBounds;
    return buf[off];
}

pub fn readU16(buf: []const u8, off: usize) !u16 {
    if (off + 2 > buf.len) return Error.OutOfBounds;
    return readIntLittle(u16, buf, off);
}

pub fn readU32(buf: []const u8, off: usize) !u32 {
    if (off + 4 > buf.len) return Error.OutOfBounds;
    return readIntLittle(u32, buf, off);
}

pub fn readU64(buf: []const u8, off: usize) !u64 {
    if (off + 8 > buf.len) return Error.OutOfBounds;
    return readIntLittle(u64, buf, off);
}

pub fn writeU8At(buf: []u8, off: usize, v: u8) !void {
    if (off + 1 > buf.len) return Error.OutOfBounds;
    buf[off] = v;
}

pub fn writeU16At(buf: []u8, off: usize, v: u16) !void {
    if (off + 2 > buf.len) return Error.OutOfBounds;
    writeIntLittle(u16, buf, off, v);
}

pub fn writeU32At(buf: []u8, off: usize, v: u32) !void {
    if (off + 4 > buf.len) return Error.OutOfBounds;
    writeIntLittle(u32, buf, off, v);
}

pub fn writeU64At(buf: []u8, off: usize, v: u64) !void {
    if (off + 8 > buf.len) return Error.OutOfBounds;
    writeIntLittle(u64, buf, off, v);
}

pub fn alignForward(value: usize, alignment: usize) usize {
    return (value + alignment - 1) & ~(alignment - 1);
}

pub fn appendZeros(list: *std.ArrayList(u8), alloc: std.mem.Allocator, count: usize) !void {
    try list.ensureUnusedCapacity(alloc, count);
    const start = list.items.len;
    list.items.len += count;
    @memset(list.items[start..], 0);
}

pub fn reserveHeader(list: *std.ArrayList(u8), alloc: std.mem.Allocator, size_bytes: u16) !u32 {
    const base = @as(u32, @intCast(list.items.len));
    try appendZeros(list, alloc, size_bytes);
    return base;
}

pub fn appendBytes(list: *std.ArrayList(u8), alloc: std.mem.Allocator, data: []const u8) !u32 {
    const aligned = alignForward(list.items.len, payload_alignment);
    if (aligned > list.items.len) {
        try appendZeros(list, alloc, aligned - list.items.len);
    }
    const off = @as(u32, @intCast(list.items.len));
    try list.appendSlice(alloc, data);
    return off;
}

pub fn appendU32Slice(list: *std.ArrayList(u8), alloc: std.mem.Allocator, values: []const u32) !u32 {
    const aligned = alignForward(list.items.len, payload_alignment);
    if (aligned > list.items.len) {
        try appendZeros(list, alloc, aligned - list.items.len);
    }
    const off = @as(u32, @intCast(list.items.len));
    try list.ensureUnusedCapacity(alloc, values.len * 4);
    for (values) |v| {
        const start = list.items.len;
        list.items.len += 4;
        writeIntLittle(u32, list.items, start, v);
    }
    return off;
}

pub fn appendU8Slice(list: *std.ArrayList(u8), alloc: std.mem.Allocator, values: []const u8) !u32 {
    const aligned = alignForward(list.items.len, payload_alignment);
    if (aligned > list.items.len) {
        try appendZeros(list, alloc, aligned - list.items.len);
    }
    const off = @as(u32, @intCast(list.items.len));
    try list.appendSlice(alloc, values);
    return off;
}

pub fn appendU16Slice(list: *std.ArrayList(u8), alloc: std.mem.Allocator, values: []const u16) !u32 {
    const aligned = alignForward(list.items.len, payload_alignment);
    if (aligned > list.items.len) {
        try appendZeros(list, alloc, aligned - list.items.len);
    }
    const off = @as(u32, @intCast(list.items.len));
    try list.ensureUnusedCapacity(alloc, values.len * 2);
    for (values) |v| {
        const start = list.items.len;
        list.items.len += 2;
        writeIntLittle(u16, list.items, start, v);
    }
    return off;
}

pub fn appendU64Slice(list: *std.ArrayList(u8), alloc: std.mem.Allocator, values: []const u64) !u32 {
    const aligned = alignForward(list.items.len, payload_alignment);
    if (aligned > list.items.len) {
        try appendZeros(list, alloc, aligned - list.items.len);
    }
    const off = @as(u32, @intCast(list.items.len));
    try list.ensureUnusedCapacity(alloc, values.len * 8);
    for (values) |v| {
        const start = list.items.len;
        list.items.len += 8;
        writeIntLittle(u64, list.items, start, v);
    }
    return off;
}

pub fn sliceAt(buf: []const u8, off: u32, len: u32) ![]const u8 {
    const start: usize = off;
    const end: usize = start + len;
    if (end > buf.len) return Error.OutOfBounds;
    return buf[start..end];
}

pub fn patchU8(buf: []u8, off: u32, v: u8) !void {
    try writeU8At(buf, off, v);
}

pub fn patchU32(buf: []u8, off: u32, v: u32) !void {
    try writeU32At(buf, off, v);
}

pub fn patchU16(buf: []u8, off: u32, v: u16) !void {
    try writeU16At(buf, off, v);
}

pub fn patchU64(buf: []u8, off: u32, v: u64) !void {
    try writeU64At(buf, off, v);
}

fn readIntLittle(comptime T: type, buf: []const u8, off: usize) T {
    const size = @sizeOf(T);
    const slice = buf[off .. off + size];
    const ptr = @as(*const [size]u8, @ptrCast(slice.ptr));
    return std.mem.readInt(T, ptr, .little);
}

fn writeIntLittle(comptime T: type, buf: []u8, off: usize, v: T) void {
    const size = @sizeOf(T);
    const slice = buf[off .. off + size];
    const ptr = @as(*[size]u8, @ptrCast(slice.ptr));
    std.mem.writeInt(T, ptr, v, .little);
}

// =========================================================================
// The runtime the generated code is written against. A generated reader
// or writer holds one of the views below and reads or patches each of
// its fields through it in one line; the layout rules (absent slots,
// alignment, null children, length prefixes) live here, once. See the
// README for the wire format.
// =========================================================================

/// An integer read little-endian, bounds-checked.
pub fn readInt(comptime T: type, buf: []const u8, off: usize) Error!T {
    if (off + @sizeOf(T) > buf.len) return Error.OutOfBounds;
    return readIntLittle(T, buf, off);
}

/// An integer patched little-endian into a header slot.
pub fn patchInt(comptime T: type, buf: []u8, off: usize, v: T) Error!void {
    if (off + @sizeOf(T) > buf.len) return Error.OutOfBounds;
    writeIntLittle(T, buf, off, v);
}

/// A run of integers appended little-endian, 4-byte aligned like every
/// payload region; returns where it starts.
pub fn appendIntSlice(comptime T: type, list: *std.ArrayList(u8), alloc: std.mem.Allocator, values: []const T) !u32 {
    const aligned = alignForward(list.items.len, payload_alignment);
    if (aligned > list.items.len) {
        try appendZeros(list, alloc, aligned - list.items.len);
    }
    const off: u32 = @intCast(list.items.len);
    try list.ensureUnusedCapacity(alloc, values.len * @sizeOf(T));
    for (values) |v| {
        const start = list.items.len;
        list.items.len += @sizeOf(T);
        writeIntLittle(T, list.items, start, v);
    }
    return off;
}

/// A table as a reader sees it: the payload, where the table's header
/// starts, and the header size the record was written with. A slot
/// past that header is absent, and reads as its zero: 0, false, empty,
/// null. Every accessor is bounds-checked and never fails; `verify`
/// is the one that reports a corrupt payload.
pub const Table = struct {
    buf: []const u8,
    base: u32,
    size_bytes: u16,

    pub fn init(buf: []const u8, base: u32) Error!Table {
        if (@as(usize, base) + 2 > buf.len) return Error.OutOfBounds;
        const size_bytes = try readU16(buf, base);
        if (@as(usize, base) + size_bytes > buf.len) return Error.OutOfBounds;
        return .{ .buf = buf, .base = base, .size_bytes = size_bytes };
    }

    /// The root table of a message whose tail names `table_id`: the
    /// root is always at payload offset 0.
    pub fn parseRoot(buf: []const u8, table_id: u16) Error!Table {
        const tail = try readMessageTail(buf);
        if (tail.table_id != table_id) return Error.Invalid;
        return init(buf[0 .. buf.len - message_tail_size], 0);
    }

    /// Whether a slot of `size` bytes at `offset` lies inside the header.
    fn present(self: Table, offset: u16, size: u16) bool {
        return @as(u32, offset) + size <= self.size_bytes;
    }

    fn at(self: Table, offset: u16) usize {
        return @as(usize, self.base) + offset;
    }

    pub fn scalar(self: Table, comptime T: type, offset: u16) T {
        if (!self.present(offset, @sizeOf(T))) return 0;
        return readInt(T, self.buf, self.at(offset)) catch 0;
    }

    /// Bit `bit_index` of the scalar at `offset`; false past its width.
    pub fn hasBit(self: Table, comptime T: type, offset: u16, bit_index: u32) bool {
        if (bit_index >= @bitSizeOf(T)) return false;
        const word = self.scalar(T, offset);
        return ((word >> @intCast(bit_index)) & 1) == 1;
    }

    /// A bytes field: `{off, len}` in the slot, the data elsewhere.
    pub fn bytes(self: Table, offset: u16) []const u8 {
        if (!self.present(offset, 8)) return &.{};
        const off = readU32(self.buf, self.at(offset)) catch 0;
        const len = readU32(self.buf, self.at(offset + 4)) catch 0;
        if (len == 0) return &.{};
        return sliceAt(self.buf, off, len) catch &.{};
    }

    /// An array's elements as bytes: `{index_off, count}` in the slot,
    /// `count` elements of `elem_size` at `index_off`.
    pub fn array(self: Table, offset: u16, elem_size: u16) []const u8 {
        if (!self.present(offset, 8)) return &.{};
        const index_off = readU32(self.buf, self.at(offset)) catch return &.{};
        const count = readU32(self.buf, self.at(offset + 4)) catch return &.{};
        if (count == 0) return &.{};
        const end = @as(usize, index_off) + @as(usize, count) * elem_size;
        if (end > self.buf.len) return &.{};
        return self.buf[index_off..end];
    }

    pub fn arrayLen(self: Table, offset: u16, elem_size: u16) u32 {
        return @intCast(self.array(offset, elem_size).len / elem_size);
    }

    /// Element `i` of a scalar array.
    pub fn arrayScalar(self: Table, comptime T: type, offset: u16, i: u32) Error!T {
        const s = self.array(offset, @sizeOf(T));
        const off = @as(usize, i) * @sizeOf(T);
        if (off + @sizeOf(T) > s.len) return Error.OutOfBounds;
        return readInt(T, s, off) catch unreachable;
    }

    /// Element `i` of a table array: the child's offset in the payload.
    pub fn arrayChild(self: Table, offset: u16, i: u32) Error!u32 {
        return self.arrayScalar(u32, offset, i);
    }

    pub fn arrayToSliceAlloc(self: Table, comptime T: type, offset: u16, alloc: std.mem.Allocator) ![]T {
        const raw = self.array(offset, @sizeOf(T));
        const out = try alloc.alloc(T, raw.len / @sizeOf(T));
        errdefer alloc.free(out);
        for (out, 0..) |*v, i| {
            v.* = readInt(T, raw, i * @sizeOf(T)) catch unreachable;
        }
        return out;
    }

    /// A nested table's offset, or null when absent or unset.
    pub fn child(self: Table, offset: u16) ?u32 {
        if (!self.present(offset, 4)) return null;
        const off = readU32(self.buf, self.at(offset)) catch 0;
        if (off == NullOffset) return null;
        return off;
    }

    pub fn verifyHeader(self: Table) Error!void {
        if (@as(usize, self.base) + @as(usize, self.size_bytes) > self.buf.len) return Error.OutOfBounds;
    }

    /// The bytes field at `offset` lies within the payload.
    pub fn verifyBytes(self: Table, offset: u16) Error!void {
        if (!self.present(offset, 8)) return;
        const off = try readU32(self.buf, self.at(offset));
        const len = try readU32(self.buf, self.at(offset + 4));
        if (len > 0) _ = try sliceAt(self.buf, off, len);
    }

    /// The array at `offset` lies within the payload.
    pub fn verifyArray(self: Table, offset: u16, elem_size: u16) Error!void {
        if (!self.present(offset, 8)) return;
        const index_off = try readU32(self.buf, self.at(offset));
        const count = try readU32(self.buf, self.at(offset + 4));
        if (count == 0) return;
        const end = @as(usize, index_off) + @as(usize, count) * elem_size;
        if (end > self.buf.len) return Error.OutOfBounds;
    }
};

/// What a finished table writer hands back: the bytes, owned or
/// borrowed, and where the root sits in them. `Reader` is the table's
/// generated reader, whose TABLE_ID a tail is checked against.
pub fn Buffer(comptime Reader: type) type {
    return struct {
        alloc: std.mem.Allocator,
        offset: u32,
        bytes: ?[]u8,
        ownership: Ownership,
        has_tail: bool,

        /// A message with a tail: the root is at payload offset 0.
        pub fn initWithTail(alloc: std.mem.Allocator, bytes: []u8, ownership: Ownership) !@This() {
            const tail = try readMessageTail(bytes);
            if (tail.table_id != Reader.TABLE_ID) return Error.Invalid;
            return .{ .alloc = alloc, .offset = 0, .bytes = bytes, .ownership = ownership, .has_tail = true };
        }

        pub fn initWithOffset(alloc: std.mem.Allocator, bytes: []u8, offset: u32, ownership: Ownership) !@This() {
            return .{ .alloc = alloc, .offset = offset, .bytes = bytes, .ownership = ownership, .has_tail = false };
        }

        pub fn initReader(self: @This()) !Reader {
            const payload = self.bytes orelse return Error.Invalid;
            return Reader.init(payload, self.offset);
        }

        pub fn deinit(self: *@This()) void {
            if (self.bytes) |bytes| {
                if (self.ownership == .owned) self.alloc.free(bytes);
                self.bytes = null;
                self.ownership = .borrowed;
            }
        }
    };
}

/// A table being written: the header is reserved up front, with every
/// nested table slot null, and each setter patches its slot, appending
/// bytes and arrays after the header. Either into a caller's list or
/// into one of its own that `finish` hands over.
pub const TableWriter = struct {
    buf: ?*std.ArrayList(u8),
    owns_buf: bool,
    alloc: std.mem.Allocator,
    base: u32,

    pub fn initWithBuf(alloc: std.mem.Allocator, buf: *std.ArrayList(u8), size_bytes: u16, child_slots: []const u16) !TableWriter {
        const base = try reserveHeader(buf, alloc, size_bytes);
        try writeU16At(buf.items, base, size_bytes);
        for (child_slots) |slot| try patchU32(buf.items, base + slot, NullOffset);
        return .{ .buf = buf, .owns_buf = false, .alloc = alloc, .base = base };
    }

    pub fn init(alloc: std.mem.Allocator, size_bytes: u16, child_slots: []const u16) !TableWriter {
        const owned = try alloc.create(std.ArrayList(u8));
        owned.* = .empty;
        errdefer alloc.destroy(owned);
        var w = try initWithBuf(alloc, owned, size_bytes, child_slots);
        w.owns_buf = true;
        return w;
    }

    fn out(self: *TableWriter) *std.ArrayList(u8) {
        return self.buf orelse @panic("zbuf writer missing buffer");
    }

    fn at(self: *TableWriter, offset: u16) u32 {
        return self.base + offset;
    }

    pub fn setScalar(self: *TableWriter, comptime T: type, offset: u16, v: T) Error!void {
        try patchInt(T, self.out().items, self.at(offset), v);
    }

    /// Sets or clears one bit of the scalar at `offset`.
    pub fn setBit(self: *TableWriter, comptime T: type, offset: u16, bit: u16, v: bool) Error!void {
        const items = self.out().items;
        var word = try readInt(T, items, self.at(offset));
        const mask = @as(T, 1) << @intCast(bit);
        if (v) word |= mask else word &= ~mask;
        try patchInt(T, items, self.at(offset), word);
    }

    /// The runtime-indexed bit helpers a flags field exports: a bit
    /// past the scalar's width is OutOfBounds.
    pub fn bitSet(self: *TableWriter, comptime T: type, offset: u16, bit_index: u32) Error!void {
        if (bit_index >= @bitSizeOf(T)) return Error.OutOfBounds;
        try self.setBit(T, offset, @intCast(bit_index), true);
    }

    pub fn bitClear(self: *TableWriter, comptime T: type, offset: u16, bit_index: u32) Error!void {
        if (bit_index >= @bitSizeOf(T)) return Error.OutOfBounds;
        try self.setBit(T, offset, @intCast(bit_index), false);
    }

    pub fn bitHas(self: *TableWriter, comptime T: type, offset: u16, bit_index: u32) Error!bool {
        if (bit_index >= @bitSizeOf(T)) return Error.OutOfBounds;
        const word = try readInt(T, self.out().items, self.at(offset));
        return ((word >> @intCast(bit_index)) & 1) == 1;
    }

    /// Appends the data and points the slot at it; empty data leaves
    /// the slot zero.
    pub fn setBytes(self: *TableWriter, offset: u16, data: []const u8) !void {
        const list = self.out();
        if (data.len == 0) {
            try patchU32(list.items, self.at(offset), 0);
            try patchU32(list.items, self.at(offset + 4), 0);
            return;
        }
        const off = try appendBytes(list, self.alloc, data);
        try patchU32(list.items, self.at(offset), off);
        try patchU32(list.items, self.at(offset + 4), @intCast(data.len));
    }

    /// Appends the elements and points the slot at them; a table array
    /// is its children's u32 offsets.
    pub fn setArray(self: *TableWriter, comptime T: type, offset: u16, values: []const T) !void {
        const list = self.out();
        if (values.len == 0) {
            try patchU32(list.items, self.at(offset), 0);
            try patchU32(list.items, self.at(offset + 4), 0);
            return;
        }
        const off = try appendIntSlice(T, list, self.alloc, values);
        try patchU32(list.items, self.at(offset), off);
        try patchU32(list.items, self.at(offset + 4), @intCast(values.len));
    }

    pub fn setChild(self: *TableWriter, offset: u16, child_off: u32) Error!void {
        try patchU32(self.out().items, self.at(offset), child_off);
    }

    pub fn finish(self: *TableWriter, comptime B: type) !B {
        if (self.owns_buf) {
            const owned = self.buf orelse return Error.Invalid;
            const bytes = try owned.toOwnedSlice(self.alloc);
            self.alloc.destroy(owned);
            self.buf = null;
            return .{ .alloc = self.alloc, .offset = self.base, .bytes = bytes, .ownership = .owned, .has_tail = false };
        }
        return .{ .alloc = self.alloc, .offset = self.base, .bytes = null, .ownership = .borrowed, .has_tail = false };
    }

    /// Root must be at payload offset 0. Initialize the root writer first
    /// on an empty buffer; children built later land at higher offsets.
    pub fn finishWithTail(self: *TableWriter, comptime B: type, table_id: u16) !B {
        if (self.base != 0) return Error.Invalid;
        try appendMessageTail(self.out(), self.alloc, table_id, self.base);
        var result = try self.finish(B);
        result.has_tail = true;
        return result;
    }

    pub fn deinit(self: *TableWriter) void {
        if (self.owns_buf) {
            const owned = self.buf orelse return;
            owned.deinit(self.alloc);
            self.alloc.destroy(owned);
            self.buf = null;
            self.base = 0;
            self.owns_buf = false;
        }
    }
};

/// Width of a packed length or count prefix.
pub const LenWidth = enum(u8) {
    u8 = 1,
    u16 = 2,
    u32 = 4,

    /// The prefix at `off`, trusted (see `load`).
    pub fn load(self: LenWidth, buf: []const u8, off: u32) u32 {
        return switch (self) {
            .u8 => zbuf_load(u8, buf, off),
            .u16 => zbuf_load(u16, buf, off),
            .u32 => zbuf_load(u32, buf, off),
        };
    }

    pub fn max(self: LenWidth) u64 {
        return switch (self) {
            .u8 => std.math.maxInt(u8),
            .u16 => std.math.maxInt(u16),
            .u32 => std.math.maxInt(u32),
        };
    }

    pub fn append(self: LenWidth, list: *std.ArrayList(u8), alloc: std.mem.Allocator, n: u64) !void {
        if (n > self.max()) return Error.Invalid;
        switch (self) {
            .u8 => try appendInt(u8, list, alloc, @intCast(n)),
            .u16 => try appendInt(u16, list, alloc, @intCast(n)),
            .u32 => try appendInt(u32, list, alloc, @intCast(n)),
        }
    }
};

/// A packed record is TRUSTED: its container vouches for it (a block
/// is checksummed before it is decoded), so its fields are read where
/// the layout says with no walk and no per-field bounds test. A record
/// that lies about a length is a bug upstream, caught by the slice
/// bounds check in a safe build.
pub fn load(comptime T: type, buf: []const u8, off: u32) T {
    return readIntLittle(T, buf, off);
}

const zbuf_load = load;

/// A `[len][bytes]` field at `off`.
pub fn packedBytes(buf: []const u8, off: u32, width: LenWidth) []const u8 {
    const len = width.load(buf, off);
    const start = off + @intFromEnum(width);
    return buf[start .. start + len];
}

/// The count of a `[count][elements]` field at `off`.
pub fn packedArrayLen(buf: []const u8, off: u32, width: LenWidth) u32 {
    return width.load(buf, off);
}

/// The elements of a `[count][elements]` field at `off`, count × elem
/// bytes.
pub fn packedArray(buf: []const u8, off: u32, width: LenWidth, elem: u32) []const u8 {
    const count = packedArrayLen(buf, off, width);
    const start = off + @intFromEnum(width);
    return buf[start .. start + count * elem];
}

/// Element `i` of a `[count][elements]` field: its bytes. The index is
/// the caller's, so it is checked.
pub fn packedElem(buf: []const u8, off: u32, width: LenWidth, elem: u32, i: u32) Error![]const u8 {
    if (i >= packedArrayLen(buf, off, width)) return Error.OutOfBounds;
    const start = off + @intFromEnum(width) + i * elem;
    return buf[start .. start + elem];
}

/// A `[count][count × u32 end][records]` field at `off`: records of
/// varying size, indexed by where each ENDS within the region, so
/// record `i` is `[end(i-1), end(i))` with `end(-1) = 0`. Ends rather
/// than starts, so the table is its own check: the last end is the
/// region's length, and every record's bounds come from two table
/// entries. Empty when the count is 0.
pub fn packedRecordsLen(buf: []const u8, off: u32, width: LenWidth) u32 {
    return width.load(buf, off);
}

/// The byte after the records region: the field's end.
pub fn packedRecordsEnd(buf: []const u8, off: u32, width: LenWidth) u32 {
    const count = packedRecordsLen(buf, off, width);
    const table = off + @intFromEnum(width);
    const region = table + count * 4;
    if (count == 0) return region;
    return region + load(u32, buf, table + (count - 1) * 4);
}

/// Record `i`'s bytes. The index is the caller's and is checked; a
/// table whose ends run backwards or past the region is Invalid.
pub fn packedRecord(buf: []const u8, off: u32, width: LenWidth, i: u32) Error![]const u8 {
    const count = packedRecordsLen(buf, off, width);
    if (i >= count) return Error.OutOfBounds;
    const table = off + @intFromEnum(width);
    const region = table + count * 4;
    const start: u32 = if (i == 0) 0 else load(u32, buf, table + (i - 1) * 4);
    const end = load(u32, buf, table + i * 4);
    const last = load(u32, buf, table + (count - 1) * 4);
    if (end < start or end > last) return Error.Invalid;
    return buf[region + start .. region + end];
}

pub fn packedToSliceAlloc(comptime T: type, buf: []const u8, off: u32, width: LenWidth, alloc: std.mem.Allocator) ![]T {
    const raw = packedArray(buf, off, width, @sizeOf(T));
    const out = try alloc.alloc(T, raw.len / @sizeOf(T));
    for (out, 0..) |*v, i| {
        v.* = readIntLittle(T, raw, i * @sizeOf(T));
    }
    return out;
}

/// An integer appended little-endian.
pub fn appendInt(comptime T: type, list: *std.ArrayList(u8), alloc: std.mem.Allocator, v: T) !void {
    var tmp: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &tmp, v, .little);
    try list.appendSlice(alloc, &tmp);
}

/// A packed record being written: fields appended in order from where
/// the list ended.
pub const PackedWriter = struct {
    buf: *std.ArrayList(u8),
    alloc: std.mem.Allocator,
    base: u32,

    pub fn initWithBuf(alloc: std.mem.Allocator, buf: *std.ArrayList(u8)) PackedWriter {
        return .{ .buf = buf, .alloc = alloc, .base = @intCast(buf.items.len) };
    }

    pub fn int(self: *PackedWriter, comptime T: type, v: T) !void {
        try appendInt(T, self.buf, self.alloc, v);
    }

    /// `[len][bytes]`; data longer than the prefix can say is Invalid.
    pub fn bytes(self: *PackedWriter, width: LenWidth, data: []const u8) !void {
        try width.append(self.buf, self.alloc, data.len);
        try self.buf.appendSlice(self.alloc, data);
    }

    pub fn rest(self: *PackedWriter, data: []const u8) !void {
        try self.buf.appendSlice(self.alloc, data);
    }

    /// `[count][count × element]` of scalars.
    pub fn ints(self: *PackedWriter, comptime T: type, width: LenWidth, values: []const T) !void {
        try width.append(self.buf, self.alloc, values.len);
        for (values) |v| try appendInt(T, self.buf, self.alloc, v);
    }

    /// The count prefix of an array whose elements the caller appends.
    pub fn count(self: *PackedWriter, width: LenWidth, n: usize) !void {
        try width.append(self.buf, self.alloc, n);
    }

    /// Begins a records field of `n` variable-size records: the count,
    /// then a table of `n` end offsets left for `RecordTable.endRecord`
    /// to fill as each record is appended.
    pub fn records(self: *PackedWriter, width: LenWidth, n: usize) !RecordTable {
        try width.append(self.buf, self.alloc, n);
        const table: u32 = @intCast(self.buf.items.len);
        try appendZeros(self.buf, self.alloc, n * 4);
        return .{ .table = table, .region = @intCast(self.buf.items.len), .next = 0 };
    }

    pub fn finish(self: *PackedWriter) u32 {
        return self.base;
    }
};

/// The offset table of a records field being written: after each
/// record is appended, `endRecord` notes where it ended.
pub const RecordTable = struct {
    table: u32,
    region: u32,
    next: u32,

    pub fn endRecord(self: *RecordTable, w: *PackedWriter) void {
        const end: u32 = @intCast(w.buf.items.len - self.region);
        patchU32(w.buf.items, self.table + self.next * 4, end) catch unreachable;
        self.next += 1;
    }
};
