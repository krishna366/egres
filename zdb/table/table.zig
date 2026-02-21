const std = @import("std");
const key_mod = @import("../key.zig");
const idx = @import("index.zig");
const buffers = @import("../buffers.gen.zig");
const compress = @import("utils").compression;

pub const Error = idx.Error || buffers.Error || compress.Error || key_mod.Error ||
    std.mem.Allocator.Error || error{
    ChecksumMismatch,
    CorruptTable,
};

pub const Item = struct {
    key: []const u8,
    value: []const u8,
};

pub const BlockLayout = struct {
    offsets_off: usize,
    entry_count: usize,
};

pub const Table = struct {
    allocator: std.mem.Allocator,
    buf: []const u8,
    index: buffers.TableIndexReader,
    key_count: u32,
    max_version: u64,
    codec: compress.Codec,
    ref: std.atomic.Value(i32),
    smallest: []const u8, // owned; empty until openTable() is called
    biggest: []const u8, // owned; empty until openTable() is called

    /// open parses the table and initialises smallest/biggest by reading the
    /// first and last entries. Caller gets one reference (ref = 1).
    pub fn open(allocator: std.mem.Allocator, buf: []const u8) Error!Table {
        var t = try parse(allocator, buf);
        errdefer t.deinit();

        if (t.blockCount() == 0) return t;

        // smallest = first key of block 0 (its base key IS the first entry).
        const first_base = try t.blockBaseKey(0);
        t.smallest = try allocator.dupe(u8, first_base);
        errdefer {
            allocator.free(t.smallest);
            t.smallest = &[_]u8{};
        }

        // biggest = last_key stored in the TableIndex by the builder.
        const last = t.index.lastKey();
        if (last.len > 0) {
            t.biggest = try allocator.dupe(u8, last);
        }
        return t;
    }

    fn parse(allocator: std.mem.Allocator, buf: []const u8) Error!Table {
        if (buf.len < 8) return error.CorruptTable;

        var read_pos = buf.len;
        const stored_checksum = try idx.readU32LE(buf, read_pos - 4);
        read_pos -= 4;

        const index_len = try idx.readU32LE(buf, read_pos - 4);
        read_pos -= 4;
        if (index_len > read_pos) return error.CorruptTable;

        const index_start = read_pos - index_len;
        const index_bytes = buf[index_start..read_pos];
        const got_checksum = std.hash.crc.Crc32Iscsi.hash(index_bytes);
        if (got_checksum != stored_checksum) return error.ChecksumMismatch;

        const index_reader = try buffers.TableIndexReader.init(index_bytes, 0);
        const codec: compress.Codec = if (index_reader.isLz4())
            .lz4
        else if (index_reader.isZstd())
            .zstd
        else
            .none;

        return .{
            .allocator = allocator,
            .buf = buf,
            .index = index_reader,
            .key_count = index_reader.keyCount(),
            .max_version = index_reader.maxVersion(),
            .codec = codec,
            .ref = std.atomic.Value(i32).init(1),
            .smallest = &[_]u8{},
            .biggest = &[_]u8{},
        };
    }

    pub fn deinit(self: *Table) void {
        if (self.smallest.len > 0) self.allocator.free(self.smallest);
        if (self.biggest.len > 0) self.allocator.free(self.biggest);
    }

    /// Increment the reference count. Call this when sharing a Table pointer.
    pub fn incrRef(self: *Table) void {
        _ = self.ref.fetchAdd(1, .monotonic);
    }

    /// Decrement the reference count. When it reaches zero the table's resources are freed.
    /// TODO: when file-backed, unmap/close the backing file and evict cached blocks here.
    pub fn decrRef(self: *Table) void {
        const prev = self.ref.fetchSub(1, .acq_rel);
        std.debug.assert(prev > 0);
    }

    pub fn getIndex(self: Table) buffers.TableIndexReader {
        return self.index;
    }

    pub fn blockCount(self: Table) usize {
        return @intCast(self.index.blocksLen());
    }

    pub fn blockBaseKey(self: Table, block_idx: usize) Error![]const u8 {
        const bi = try self.index.blocksAt(@intCast(block_idx));
        return bi.key();
    }

    pub fn readBlock(self: Table, alloc: std.mem.Allocator, block_idx: usize, out: *std.ArrayList(u8)) Error![]const u8 {
        const bi = try self.index.blocksAt(@intCast(block_idx));

        const start: usize = bi.offset();
        const end = start + bi.len();
        if (end > self.buf.len) return error.CorruptTable;
        if (bi.len() < 4) return error.CorruptTable;

        const raw = self.buf[start..end];

        // Decompress into out (noop for .none), then verify checksum.
        // CRC was computed before compression in the builder.
        const block = try compress.decompress(alloc, self.codec, raw, out);
        if (block.len < 4) return error.CorruptTable;

        const checksum_off = block.len - 4;
        const stored_checksum = try idx.readU32LE(block, checksum_off);
        const got_checksum = std.hash.crc.Crc32Iscsi.hash(block[0..checksum_off]);
        if (stored_checksum != got_checksum) return error.ChecksumMismatch;
        return block;
    }
};

/// BlockIterator decodes entries from a single block lazily, one at a time.
/// Block data is caller-owned; key_buf is self-owned.
pub const BlockIterator = struct {
    alloc: std.mem.Allocator,
    key_buf: std.ArrayList(u8),
    block: []const u8,
    entries_region: []const u8,
    layout: BlockLayout,
    base_key: []const u8,
    entry_idx: usize,
    val: []const u8,
    is_valid: bool,

    pub fn deinit(self: *BlockIterator) void {
        self.key_buf.deinit(self.alloc);
    }

    fn parseBlockLayout(block: []const u8) Error!BlockLayout {
        if (block.len < 8) return error.CorruptTable;

        const checksum_off = block.len - 4;
        if (checksum_off < 4) return error.CorruptTable;

        const count_off = checksum_off - 4;
        const entry_count_u32 = try idx.readU32LE(block, count_off);
        const entry_count: usize = @intCast(entry_count_u32);
        if (entry_count == 0) return error.CorruptTable;

        const offsets_bytes = entry_count * 4;
        if (count_off < offsets_bytes) return error.CorruptTable;
        const offsets_off = count_off - offsets_bytes;

        return .{ .offsets_off = offsets_off, .entry_count = entry_count };
    }

    pub fn init(
        alloc: std.mem.Allocator,
        block: []const u8,
        base_key: []const u8,
    ) !BlockIterator {
        // See Table Layout diagram in builder.zig.
        // This should be a copy of that. If not, use builder.zig as the source of truth.
        //
        // Block layout is set as:
        // | ------------------------------------------------------------------------|
        // | --- entries_region --- | --- offsets to entries, each offset is u32 --- |
        // | --  entry count u32 -- | -- block crc u32 -- |
        // | ------------------------------------------------------------------------|
        //
        // Note that we only store the start offset of each entry i, not the end, because
        // the end offset of entry i = the start offset of entry i+1.
        //
        // Each entry is stored as such:
        // | ----------------------------------------------------------------------|
        // | -- u16 key overlap with base -- | -- u16 key diff length from base -- |
        // | -- key diff bytes -- | -- value bytes ------------------------------- |
        // | ----------------------------------------------------------------------|
        const layout = try parseBlockLayout(block);

        return .{
            .alloc = alloc,
            .key_buf = .empty,
            .block = block,
            .layout = layout,
            .entries_region = block[0..layout.offsets_off],
            .base_key = base_key,
            .entry_idx = 0,
            .val = &[_]u8{},
            .is_valid = false,
        };
    }

    pub fn seekToFirst(self: *BlockIterator) Error!void {
        try self.setIdx(0);
    }

    pub fn seekToLast(self: *BlockIterator) Error!void {
        if (self.layout.entry_count == 0) {
            self.is_valid = false;
            return;
        }
        try self.setIdx(self.layout.entry_count - 1);
    }

    pub fn next(self: *BlockIterator) Error!void {
        try self.setIdx(self.entry_idx + 1);
    }

    /// Binary search for the first entry with key >= target.
    pub fn seekGe(self: *BlockIterator, target: []const u8) Error!void {
        // This binary search is only possible because we're diffing keys
        // against the base key and not against the previous key.
        var lo: usize = 0;
        var hi: usize = self.layout.entry_count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            try self.setIdx(mid);
            if (!self.is_valid) {
                hi = mid;
                continue;
            }
            if (key_mod.compareKeys(self.key_buf.items, target) == .lt) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo < self.layout.entry_count) {
            try self.setIdx(lo);
        } else {
            self.is_valid = false;
        }
    }

    pub fn itemRef(self: *const BlockIterator) ?Item {
        if (!self.is_valid) return null;
        return .{ .key = self.key_buf.items, .value = self.val };
    }

    inline fn getOffset(self: *BlockIterator, i: usize) Error!usize {
        const offset = try idx.readU32LE(self.block, self.layout.offsets_off + (i * 4));
        return @intCast(offset);
    }

    fn setIdx(self: *BlockIterator, i: usize) Error!void {
        if (i >= self.layout.entry_count) {
            self.is_valid = false;
            return;
        }

        const off = try self.getOffset(i);
        if (off + 4 > self.entries_region.len) return error.CorruptTable;

        const key_overlap: usize = @intCast(try idx.readU16LE(self.entries_region, off));
        const key_diff_len: usize = @intCast(try idx.readU16LE(self.entries_region, off + 2));
        if (key_overlap > self.base_key.len) return error.CorruptTable;

        const diff_start = off + 4;
        const diff_end = diff_start + key_diff_len;
        if (diff_end > self.entries_region.len) return error.CorruptTable;
        const diff = self.entries_region[diff_start..diff_end];

        // Reconstruct full key into reusable buffer: base[0..overlap] ++ diff.
        self.key_buf.clearRetainingCapacity();
        try self.key_buf.appendSlice(self.alloc, self.base_key[0..key_overlap]);
        try self.key_buf.appendSlice(self.alloc, diff);

        // Value boundary: next entry's offset, or end of entries_region for the last entry.
        const value_start = diff_end;
        const value_end = if (i + 1 < self.layout.entry_count) try self.getOffset(i + 1) else self.entries_region.len;
        if (value_end < value_start or value_end > self.entries_region.len) return error.CorruptTable;

        self.val = self.entries_region[value_start..value_end];
        self.entry_idx = i;
        self.is_valid = true;
    }
};

/// TableIterator navigates all entries across all blocks in a table.
pub const TableIterator = struct {
    allocator: std.mem.Allocator,
    table: *const Table,
    scratch: std.heap.ArenaAllocator,
    bpos: usize,
    bi: ?BlockIterator,
    valid: bool,

    pub fn init(allocator: std.mem.Allocator, table: *const Table) TableIterator {
        return .{
            .allocator = allocator,
            .table = table,
            .scratch = std.heap.ArenaAllocator.init(allocator),
            .bpos = 0,
            .bi = null,
            .valid = false,
        };
    }

    pub fn deinit(self: *TableIterator) void {
        if (self.bi) |*bi| bi.deinit();
        self.scratch.deinit();
    }

    pub fn seekToFirst(self: *TableIterator) Error!void {
        if (self.table.blockCount() == 0) {
            self.valid = false;
            return;
        }
        try self.setBlock(0);
        try self.bi.?.seekToFirst();
        self.valid = self.bi.?.is_valid;
    }

    pub fn seek(self: *TableIterator, target: []const u8) Error!void {
        self.valid = false;
        const block_count = self.table.blockCount();
        if (block_count == 0) return;

        // Binary search: find the last block whose base key <= target.
        var lo: usize = 0;
        var hi: usize = block_count;
        while (lo + 1 < hi) {
            const mid = lo + (hi - lo) / 2;
            const base = try self.table.blockBaseKey(mid);
            if (key_mod.compareKeys(base, target) == .gt) {
                hi = mid;
            } else {
                lo = mid;
            }
        }

        try self.setBlock(lo);
        try self.bi.?.seekGe(target);
        if (self.bi.?.is_valid) {
            self.valid = true;
            return;
        }

        // Not found in block lo — target may sit at the start of the next block.
        if (lo + 1 < block_count) {
            try self.setBlock(lo + 1);
            try self.bi.?.seekToFirst();
            self.valid = self.bi.?.is_valid;
        }
    }

    pub fn next(self: *TableIterator) Error!void {
        if (!self.valid) return;

        try self.bi.?.next();
        if (self.bi.?.is_valid) return;

        // End of current block — advance to the next.
        const next_bpos = self.bpos + 1;
        if (next_bpos >= self.table.blockCount()) {
            self.valid = false;
            return;
        }
        try self.setBlock(next_bpos);
        try self.bi.?.seekToFirst();
        self.valid = self.bi.?.is_valid;
    }

    pub fn itemRef(self: *const TableIterator) ?Item {
        return self.bi.?.itemRef();
    }

    fn setBlock(self: *TableIterator, bpos: usize) Error!void {
        if (self.bi) |*bi| bi.deinit();
        _ = self.scratch.reset(.free_all);

        var block_buf = std.ArrayList(u8).empty;
        const block = try self.table.readBlock(self.scratch.allocator(), bpos, &block_buf);
        const base_key = try self.table.blockBaseKey(bpos);

        self.bi = try BlockIterator.init(self.allocator, block, base_key);
        self.bpos = bpos;
    }
};
