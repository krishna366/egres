const std = @import("std");
const key = @import("../key.zig");
const idx = @import("index.zig");
const buffers = @import("../buffers.gen.zig");
const compress = @import("utils").compression;
const binaryfuse = @import("../binaryfusefilter.zig");
const vfs = @import("../vfs.zig");
const cache_mod = @import("../cache.zig");

/// Module-level block cache singleton. Set before opening tables to enable caching.
/// When null (default), readBlock always reads from disk.
pub var block_cache: ?*cache_mod.ShardedCache(u64) = null;

pub const Error = idx.Error || buffers.Error || compress.Error || key.Error ||
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
    handle: vfs.FileHandle,
    index: buffers.TableIndexStruct,
    codec: compress.Codec,
    binary_fuse: ?binaryfuse.BinaryFuse(u16),
    ref: std.atomic.Value(i32),
    file_id: u44, // parsed from SST filename; 0 for in-memory tables
    path: ?[]const u8, // owned; non-null for file-backed tables; deleted on last decrRef

    pub fn fileSize(self: *const Table) u64 {
        return self.handle.file_size;
    }

    /// open opens an SST file through the global VFS instance (vfs.instance).
    /// The file loading mode is determined by vfs.instance; readAligned
    /// transparently handles mmap, direct, and memory-backed file handles.
    pub fn open(allocator: std.mem.Allocator, path: []const u8, priority: vfs.Priority) !*Table {
        const handle = try vfs.instance.openFile(allocator, path, .default);
        const t = try initFromHandle(allocator, handle, priority);
        errdefer t.decrRef();
        t.file_id = vfs.fileIdFromPath(path);
        t.path = try allocator.dupe(u8, path);
        return t;
    }

    /// openHandle creates a Table from a pre-opened FileHandle.
    /// Takes ownership of handle (closes it on error or when Table is freed).
    /// The resulting table has no path (decrRef frees memory only, no file deletion).
    pub fn openHandle(allocator: std.mem.Allocator, handle: vfs.FileHandle, priority: vfs.Priority) !*Table {
        return initFromHandle(allocator, handle, priority);
    }

    /// Single code path: reads trailer + index via handle.readAligned.
    fn initFromHandle(allocator: std.mem.Allocator, handle: vfs.FileHandle, priority: vfs.Priority) !*Table {
        errdefer handle.close();

        if (handle.file_size == 0) return error.CorruptTable;
        const size: usize = @intCast(handle.file_size);

        const t = try allocator.create(Table);
        errdefer allocator.destroy(t);

        // Read the last 8 bytes to get index_len and checksum.
        if (size < 8) return error.CorruptTable;
        const trailer_offset: u64 = @intCast(size - 8);
        const trailer_result = try handle.readAligned(allocator, trailer_offset, 8, priority);
        defer trailer_result.free();

        if (trailer_result.data.len < 8) return error.CorruptTable;
        const index_len = idx.readU32LE(trailer_result.data, 0) catch return error.CorruptTable;
        const stored_checksum = idx.readU32LE(trailer_result.data, 4) catch return error.CorruptTable;

        // Read index bytes.
        const index_end: u64 = @intCast(size - 8);
        if (index_len > index_end) return error.CorruptTable;
        const index_offset = index_end - index_len;
        const index_result = try handle.readAligned(allocator, index_offset, index_len, priority);
        defer index_result.free();

        if (index_result.data.len < index_len) return error.CorruptTable;
        const index_bytes = index_result.data[0..index_len];
        const got_checksum = std.hash.crc.Crc32Iscsi.hash(index_bytes);
        if (got_checksum != stored_checksum) return error.ChecksumMismatch;

        const index_reader = try buffers.TableIndexReader.init(index_bytes, 0);
        var table_index = try buffers.TableIndexStruct.fromReader(allocator, index_reader);
        errdefer table_index.deinit(allocator);

        const codec: compress.Codec = if (table_index.is_lz4)
            .lz4
        else if (table_index.is_zstd)
            .zstd
        else
            .none;

        // Steal the fingerprints slice from the decoded index struct into a
        // BinaryFuse(u16) that we keep for the lifetime of this Table.  After
        // the transfer table_index.binary_fuse holds no heap memory (fingerprints is
        // &.{}) so index.deinit() becomes a no-op for that field.
        const binary_fuse: ?binaryfuse.BinaryFuse(u16) = if (table_index.binary_fuse) |*bf| blk: {
            const filter = binaryfuse.BinaryFuse(u16){
                .seed = bf.seed,
                .segment_length = bf.segment_length,
                .segment_length_mask = bf.segment_length_mask,
                .segment_count = bf.segment_count,
                .segment_count_length = bf.segment_count_length,
                .fingerprints = bf.fingerprints,
            };
            bf.fingerprints = &.{};
            table_index.binary_fuse = null;
            break :blk filter;
        } else null;

        t.* = .{
            .allocator = allocator,
            .handle = handle,
            .index = table_index,
            .codec = codec,
            .binary_fuse = binary_fuse,
            .ref = std.atomic.Value(i32).init(1),
            .file_id = 0,
            .path = null,
        };
        return t;
    }

    fn deinit(self: *Table) void {
        if (self.path) |p| self.allocator.free(p);
        self.index.deinit(self.allocator);
        if (self.binary_fuse) |*bf| bf.deinit(self.allocator);
        self.handle.close();
    }

    /// detach forgets the SST file path without deleting it.
    /// Used by LevelHandler.deinit() (database close) so that the subsequent
    /// decrRef at zero frees memory only, leaving the file on disk intact.
    pub fn detach(self: *Table) void {
        if (self.path) |p| self.allocator.free(p);
        self.path = null;
    }

    /// Increment the reference count. Call this when sharing a Table pointer.
    pub fn incrRef(self: *Table) void {
        _ = self.ref.fetchAdd(1, .monotonic);
    }

    /// Decrement the reference count. When it reaches zero the table's resources are freed
    /// and, for file-backed tables, the SST file is deleted from disk.
    pub fn decrRef(self: *Table) void {
        const prev = self.ref.fetchSub(1, .acq_rel);
        std.debug.assert(prev > 0);
        if (prev == 1) {
            if (self.path) |p| std.fs.deleteFileAbsolute(p) catch {};
            const allocator = self.allocator;
            self.deinit();
            allocator.destroy(self);
        }
    }

    pub fn getIndex(self: *const Table) *const buffers.TableIndexStruct {
        return &self.index;
    }

    pub fn smallest(self: *const Table) []const u8 {
        if (self.index.blocks.len == 0) return &.{};
        return self.index.blocks[0].key;
    }

    pub fn biggest(self: *const Table) []const u8 {
        return self.index.last_key;
    }

    pub fn containsKey(self: *const Table, key_no_ts: []const u8) bool {
        if (self.binary_fuse) |bf| {
            const h = key.hashNoTs(key_no_ts);
            return bf.contain(h);
        }
        return true;
    }

    pub fn blockCount(self: Table) usize {
        return self.index.blocks.len;
    }

    pub fn blockBaseKey(self: Table, block_idx: usize) Error![]const u8 {
        if (block_idx >= self.index.blocks.len) return error.CorruptTable;
        return self.index.blocks[block_idx].key;
    }

    pub fn readBlock(self: *const Table, block_idx: usize, priority: vfs.Priority) Error!*cache_mod.Value {
        if (block_idx >= self.index.blocks.len) return error.CorruptTable;

        // Cache key: 44 bits file_id (MSB) | 20 bits block_idx (LSB).
        // null on overflow → cache_key = 0 → skip caching.
        const fid: u64 = self.file_id;
        const bidx: u64 = std.math.cast(u20, block_idx) orelse 0;
        const cache_key: u64 = if (fid != 0 and bidx != 0) (fid << 20) | bidx else 0;

        if (block_cache != null and cache_key != 0) {
            if (block_cache.?.get(cache_key)) |val| return val; // incrRef'd by get.
        }

        const bi = self.index.blocks[block_idx];
        if (bi.len < 4) return error.CorruptTable;

        // Read raw bytes (temporary)
        const result = self.handle.readAligned(
            self.allocator,
            @intCast(bi.offset),
            bi.len,
            priority,
        ) catch return error.CorruptTable;
        defer result.free();

        // Decompress (temporary)
        var out = std.ArrayList(u8).empty;
        defer out.deinit(self.allocator);
        const sz = try compress.decompress(self.allocator, self.codec, result.data, &out);
        if (sz < 4) return error.CorruptTable;
        std.debug.assert(out.items.len == sz);

        // Checksum
        const checksum_off = sz - 4;
        const stored_checksum = try idx.readU32LE(out.items, checksum_off);
        const got_checksum = std.hash.crc.Crc32Iscsi.hash(out.items[0..checksum_off]);
        if (stored_checksum != got_checksum) return error.ChecksumMismatch;

        // Create ref-counted Value with owned copy
        const owned_data = try out.toOwnedSlice(self.allocator);
        errdefer self.allocator.free(owned_data);

        // val has ref=1
        const val = cache_mod.Value.init(self.allocator, owned_data) catch return error.OutOfMemory;
        errdefer val.decrRef();

        // Cache insert (best-effort); put incrRefs on success → ref=2
        if (block_cache != null and cache_key != 0) {
            block_cache.?.put(cache_key, val) catch {};
        }

        return val; // caller holds ref (1 if not cached, or 1+cache_ref if cached)
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
        // | ------------------------------------------------------------------------||
        // | --- entries_region --- | --- offsets to entries, each offset is u32 --- |
        // | --  entry count u32 -- | -- block crc u32 -- |
        // | ------------------------------------------------------------------------||
        //
        // Note that we only store the start offset of each entry i, not the end, because
        // the end offset of entry i = the start offset of entry i+1.
        //
        // Each entry is stored as such:
        // | ----------------------------------------------------------------------||
        // | -- u16 key overlap with base -- | -- u16 key diff length from base -- |
        // | -- key diff bytes -- | -- value bytes ------------------------------- |
        // | ----------------------------------------------------------------------||
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
            if (key.compareWithTs(self.key_buf.items, target) == .lt) {
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
    table: *Table,
    bpos: usize,
    bi: ?BlockIterator,
    current_block: ?*cache_mod.Value = null,
    valid: bool,
    priority: vfs.Priority,

    pub fn init(allocator: std.mem.Allocator, table: *Table, priority: vfs.Priority) TableIterator {
        table.incrRef();
        return .{
            .allocator = allocator,
            .table = table,
            .bpos = 0,
            .bi = null,
            .current_block = null,
            .valid = false,
            .priority = priority,
        };
    }

    pub fn deinit(self: *TableIterator) void {
        self.table.decrRef();
        if (self.bi) |*bi| bi.deinit();
        if (self.current_block) |b| b.decrRef();
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
            if (key.compareWithTs(base, target) == .gt) {
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
        if (self.current_block) |b| b.decrRef();
        self.current_block = null;

        const val = try self.table.readBlock(bpos, self.priority);
        self.current_block = val;

        const base_key = try self.table.blockBaseKey(bpos);
        self.bi = try BlockIterator.init(self.allocator, val.data, base_key);
        self.bpos = bpos;
    }
};
