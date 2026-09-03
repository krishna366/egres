const std = @import("std");
const zbuf = @import("zbuf").zbuf;
const Key = @import("../kv.zig").Key;
const Meta = @import("../kv.zig").Meta;
pub const Item = @import("../kv.zig").Item;
const _idx = @import("index.zig");
const _buffers = @import("../buffers.gen.zig");
const utils = @import("utils");
const compress = utils.compression;
const _binaryfuse = @import("../binaryfusefilter.zig");
const vfs = @import("vfs");
const _cache = @import("../cache.zig");
const _fi = @import("../file_ids.zig");

/// Module-level block cache singleton. Set before opening tables to enable
/// caching. When null (default), readBlock always reads from disk.
///
/// Key layout: high 64 bits = file_id, low 64 bits = block_idx. Storing
/// both fields in their own halves of a u128 means no hash combine and
/// zero collision risk across the node_id-namespaced file_id space.
pub var block_cache: ?*_cache.ShardedCache(u128) = null;

pub const Error = _idx.Error || _buffers.Error || compress.Error ||
    std.mem.Allocator.Error || error{
    ChecksumMismatch,
    CorruptTable,
    UnsupportedTableFormat,
};

const log = std.log.scoped(.table);

// TODO: Consider using zbuf to store this.
/// Where each part of a block starts, all absolute from block[0]. See the
/// layout diagram at the top of builder.zig.
pub const BlockLayout = struct {
    /// Entry offsets array (u32 × entry_count).
    offsets_off: usize,
    /// Entries region — also where the header ends.
    entries_off: usize,
    entry_count: usize,
};

pub const Table = struct {
    allocator: std.mem.Allocator,
    handle: vfs.FileHandle,
    index: *_buffers.TableIndexStruct,
    codec: compress.Codec,
    binary_fuse: ?_binaryfuse.BinaryFuse(u16),
    ref: std.atomic.Value(i32),
    smallest: Key,
    biggest: Key,
    /// Parsed from the SST filename (see `zdb.file_ids.sstPath`). 0 for
    /// in-memory tables — also the sentinel "skip block-cache" value
    /// since 0 isn't a valid encoded id (node 0 is reserved).
    file_id: u64,
    // Owned path split into atomic pointer + length for thread-safe detach.
    // Use getPath() to read, claimPath() to atomically take ownership.
    path_ptr: std.atomic.Value(?[*]const u8),
    path_len: usize,

    pub fn fileSize(self: *const Table) u64 {
        return self.handle.file_size;
    }

    /// open opens an SST file through the global VFS instance (vfs.g).
    /// The file loading mode is determined by vfs.g(); readAligned
    /// transparently handles mmap, direct, and memory-backed file handles.
    pub fn open(allocator: std.mem.Allocator, path: []const u8, priority: vfs.Priority) !*Table {
        const handle = try vfs.g().openFile(allocator, path, .{});
        const t = try initFromHandle(allocator, handle, priority);
        errdefer t.decrRef();
        t.file_id = _fi.fromPath(path);
        const duped = try allocator.dupe(u8, path);
        t.path_ptr = std.atomic.Value(?[*]const u8).init(duped.ptr);
        t.path_len = duped.len;
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

        // Read the trailer: [u32 index_len][u64 index_hash] (idx.trailer_len bytes).
        if (size < _idx.trailer_len) return error.CorruptTable;
        const trailer_offset: u64 = @intCast(size - _idx.trailer_len);
        const trailer_result = try handle.readAligned(allocator, trailer_offset, _idx.trailer_len, priority);
        defer trailer_result.free();

        if (trailer_result.data.len < _idx.trailer_len) return error.CorruptTable;
        const index_len = _idx.readU32LE(trailer_result.data, 0) catch return error.CorruptTable;
        const stored_checksum = _idx.readU64LE(trailer_result.data, 4) catch return error.CorruptTable;

        // Read index bytes.
        const index_end: u64 = @intCast(size - _idx.trailer_len);
        if (index_len > index_end) return error.CorruptTable;
        const index_offset = index_end - index_len;
        const index_result = try handle.readAligned(allocator, index_offset, index_len, priority);
        defer index_result.free();

        if (index_result.data.len < index_len) return error.CorruptTable;
        const index_bytes = index_result.data[0..index_len];
        const got_checksum = zbuf.hash(index_bytes);
        if (got_checksum != stored_checksum) return error.ChecksumMismatch;

        const index_reader = try _buffers.TableIndexReader.init(index_bytes, 0);
        var table_index = try index_reader.toStruct(allocator);
        // destroy, not deinit: toStruct hands back a created pointer, and
        // deinit frees only what the struct points at. Same call Table.deinit
        // makes on the success path.
        errdefer table_index.destroy(allocator);

        // Refuse any SST whose entry encoding we don't speak. The block
        // checksums can't catch this: a stale table's blocks are internally
        // consistent, so they verify fine and then decode into keys shifted by
        // a byte. There is no migration path — wipe the data dir and reload.
        if (table_index.format_version != _idx.format_version) {
            log.err(
                "SST format_version={d}, expected {d}: wipe the data dir and reload",
                .{ table_index.format_version, _idx.format_version },
            );
            return error.UnsupportedTableFormat;
        }

        const codec: compress.Codec = @enumFromInt(@intFromEnum(table_index.compression));

        // Steal the fingerprints slice from the decoded index struct into a
        // BinaryFuse(u16) that we keep for the lifetime of this Table.  After
        // the transfer table_index.binary_fuse holds no heap memory (fingerprints is
        // &.{}) so index.deinit() becomes a no-op for that field.
        const binary_fuse: ?_binaryfuse.BinaryFuse(u16) = if (table_index.binary_fuse) |*bf| blk: {
            const filter = _binaryfuse.BinaryFuse(u16){
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

        const smallest_raw = if (table_index.blocks.len > 0) table_index.blocks[0].key else &.{};
        t.* = .{
            .allocator = allocator,
            .handle = handle,
            .index = table_index,
            .codec = codec,
            .binary_fuse = binary_fuse,
            .smallest = if (smallest_raw.len >= 16) Key.init(smallest_raw) else .{},
            .biggest = if (table_index.last_key.len >= 16) Key.init(table_index.last_key) else .{},
            .ref = std.atomic.Value(i32).init(1),
            .file_id = 0,
            .path_ptr = std.atomic.Value(?[*]const u8).init(null),
            .path_len = 0,
        };
        return t;
    }

    /// Atomically claim ownership of the path, returning the slice if we won the race.
    fn claimPath(self: *Table) ?[]const u8 {
        const ptr = self.path_ptr.swap(null, .acq_rel);
        if (ptr) |p| return p[0..self.path_len];
        return null;
    }

    fn deinit(self: *Table) void {
        if (self.claimPath()) |p| self.allocator.free(p);
        self.index.destroy(self.allocator);
        if (self.binary_fuse) |*bf| bf.deinit(self.allocator);
        self.handle.close();
    }

    /// detach forgets the SST file path without deleting it.
    /// Used by Level.deinit() (database close) so that the subsequent
    /// decrRef at zero frees memory only, leaving the file on disk intact.
    /// Thread-safe: concurrent calls race on an atomic swap; only the winner frees.
    pub fn detach(self: *Table) void {
        if (self.claimPath()) |p| self.allocator.free(p);
    }

    /// Increment the reference count. Call this when sharing a Table pointer.
    pub fn incrRef(self: *Table) void {
        _ = self.ref.fetchAdd(1, .monotonic);
    }

    /// Decrement the reference count. When it reaches zero the table's
    /// resources are freed.
    ///
    /// FILE DELETION DISABLED: prior behavior was to also `unlink` the SST
    /// file when refcount hit zero, but the in-memory refcount doesn't
    /// reflect "still needed by an older Raft snapshot" or "still listed
    /// by another in-flight compaction's view of the level". A live
    /// compaction was hitting `FileNotFound` because an input SST had
    /// been unlinked from under it. Until file lifetime is properly
    /// coordinated with snapshot retention (TODO), leaving orphan SSTs
    /// on disk is the safer of the two failure modes — disk usage grows
    /// but nothing breaks.
    pub fn decrRef(self: *Table) void {
        const prev = self.ref.fetchSub(1, .acq_rel);
        std.debug.assert(prev > 0);
        if (prev == 1) {
            if (self.claimPath()) |p| {
                self.allocator.free(p);
            }
            self.deinit();
        }
    }

    pub fn getIndex(self: *const Table) *const _buffers.TableIndexStruct {
        return &self.index;
    }

    pub fn containsKey(self: *const Table, key: Key) bool {
        if (self.binary_fuse) |bf| {
            const h = key.bloomHash();
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

    pub fn readBlock(self: *const Table, block_idx: usize, priority: vfs.Priority) Error!*_cache.Value {
        if (block_idx >= self.index.blocks.len) return error.CorruptTable;

        // file_id in high 64 bits, block_idx in low 64 bits — no hashing,
        // no collision possible. 0 = "skip cache" sentinel (file_id 0 is
        // the in-memory / non-SST marker).
        const fid: u64 = self.file_id;
        const cache_key: u128 = if (fid != 0)
            (@as(u128, fid) << 64) | @as(u128, block_idx)
        else
            0;

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
        // Block must contain at least the leading u64 hash (see zbuf.hash).
        if (sz < 8) return error.CorruptTable;
        std.debug.assert(out.items.len == sz);

        const stored_checksum = try _idx.readU64LE(out.items, 0);
        const got_checksum = zbuf.hash(out.items[8..]);
        if (stored_checksum != got_checksum) return error.ChecksumMismatch;

        // Create ref-counted Value with owned copy
        const owned_data = try out.toOwnedSlice(self.allocator);
        errdefer self.allocator.free(owned_data);

        // val has ref=1
        const val = _cache.Value.init(self.allocator, owned_data) catch return error.OutOfMemory;
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
    /// The entries region, sliced once at init. Entry offsets stored in
    /// the block are relative to this, not to the block.
    entries_region: []const u8,
    layout: BlockLayout,
    base_key: []const u8,
    entry_idx: usize,
    val: []const u8,
    meta: Meta,
    is_valid: bool,

    pub fn deinit(self: *BlockIterator) void {
        self.key_buf.deinit(self.alloc);
    }

    fn parseBlockLayout(block: []const u8) Error!BlockLayout {
        // Header is [u64 block_hash][u32 entry_count] then the entry
        // offsets (see builder.zig finishBlock). parseBlockLayout never
        // validates the hash itself — that's already done in readBlock's
        // decompress step.
        if (block.len < 12) return error.CorruptTable;

        const entry_count: usize = @intCast(try _idx.readU32LE(block, 8));
        if (entry_count == 0) return error.CorruptTable;

        const offsets_off: usize = 12;
        const entries_off = offsets_off + entry_count * 4;
        if (entries_off > block.len) return error.CorruptTable;

        return .{
            .offsets_off = offsets_off,
            .entries_off = entries_off,
            .entry_count = entry_count,
        };
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
        // | -- block crc u64 -- | -- entry count u32 -- |
        // | --- offsets to entries, u32 each, entry_count of them --- |
        // | --- entries region --- |
        // | ------------------------------------------------------------------------||
        //
        // Note that we only store the start offset of each entry i, not the end, because
        // the end offset of entry i = the start offset of entry i+1.
        //
        // Each entry is stored as such:
        // | ----------------------------------------------------------------------||
        // | -- u16 key overlap with base -- | -- u16 key diff length from base -- |
        // | -- u8 meta -- | -- key diff bytes -- | -- value bytes ----------- |
        // | ----------------------------------------------------------------------||
        const layout = try parseBlockLayout(block);

        return .{
            .alloc = alloc,
            .key_buf = .empty,
            .block = block,
            .layout = layout,
            .entries_region = block[layout.entries_off..],
            .base_key = base_key,
            .entry_idx = 0,
            .val = &[_]u8{},
            .meta = .{},
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

    /// A stored key, read in place: base_key[0..overlap] ++ diff, without
    /// pasting the two together. Lets a search compare an entry without
    /// materializing it — the whole point of seeking this way.
    const StoredKey = struct {
        base: []const u8,
        diff: []const u8,

        fn len(self: StoredKey) usize {
            return self.base.len + self.diff.len;
        }

        fn at(self: StoredKey, i: usize) u8 {
            if (i < self.base.len) return self.base[i];
            return self.diff[i - self.base.len];
        }

        /// The trailing 8 bytes, big-endian. They can straddle the
        /// base/diff seam — entry 0 of a block has no diff at all, and a
        /// version chain's diff is often just part of the timestamp.
        fn timestamp(self: StoredKey) u64 {
            var ts: u64 = 0;
            var i = self.len() - 8;
            while (i < self.len()) : (i += 1) {
                ts = (ts << 8) | self.at(i);
            }
            return ts;
        }
    };

    /// Read entry i's key without copying it.
    fn storedKeyAt(self: *BlockIterator, i: usize) Error!StoredKey {
        const er = self.entries_region;
        const off = try self.getOffset(i);
        if (off + 5 > er.len) return error.CorruptTable;

        const overlap: usize = @intCast(try _idx.readU16LE(er, off));
        const dlen: usize = @intCast(try _idx.readU16LE(er, off + 2));
        if (overlap > self.base_key.len) return error.CorruptTable;
        // 12-byte ns/tid prefix, >=1 primary byte, 8-byte timestamp.
        if (overlap + dlen < 21) return error.CorruptTable;

        const start = off + 5;
        const end = start + dlen;
        if (end > er.len) return error.CorruptTable;

        return .{
            .base = self.base_key[0..overlap],
            .diff = er[start..end],
        };
    }

    /// Order entry i against a marshaled target, per Key.order: the
    /// [ns|tid|primary] prefix bytewise (ns and tid are fixed-width
    /// big-endian, so bytewise IS field order, and only primary can
    /// differ in length, where shorter-is-less is what we want), then the
    /// trailing timestamp REVERSED. Comparing whole keys with one memcmp
    /// would be wrong twice: unequal primary lengths misalign the
    /// timestamp, and its order is inverted.
    ///
    /// `shared` is how many leading bytes the TARGET shares with
    /// base_key. Bytes below it that the entry also takes from base_key
    /// are equal on both sides and need no comparing — hence the start at
    /// min(shared, overlap): past `overlap` the entry leaves the base key
    /// and may differ there, so skipping further would miss it. Keys in a
    /// block share a long prefix, so the loop normally runs a byte or two.
    fn cmpStoredKey(
        self: *BlockIterator,
        i: usize,
        target_key: []const u8,
        shared_w_base: usize,
    ) Error!std.math.Order {
        const sk = try self.storedKeyAt(i);
        const slen = sk.len() - 8; // prefix, i.e. key minus timestamp
        const tlen = target_key.len - 8;

        var at = @min(shared_w_base, sk.base.len);
        const end = @min(slen, tlen);
        while (at < end) : (at += 1) {
            // byte by byte comparison
            const a = sk.at(at);
            const b = target_key[at];
            if (a != b) return if (a < b) .lt else .gt;
        }
        if (slen != tlen) return if (slen < tlen) .lt else .gt;

        const ets = sk.timestamp();
        const tts = std.mem.readInt(u64, target_key[tlen..][0..8], .big);
        if (ets == tts) return .eq;
        return if (ets > tts) .lt else .gt; // newest sorts first
    }

    /// Binary search for the first entry with key >= target. `traw` is
    /// `target` marshaled (Key.marshalTo): the caller marshals once per
    /// seek, then probes compare raw bytes and only the landing entry is
    /// materialized.
    pub fn seekGe(self: *BlockIterator, target_key: []const u8) Error!void {
        // This binary search is only possible because we're diffing keys
        // against the base key and not against the previous key.
        if (target_key.len == 0) return self.setIdx(0);
        if (target_key.len < 21) return error.CorruptTable;

        // Bytes the target shares with the base key, computed once for
        // the whole search; cmpEntry decides per entry how much of that
        // it can actually skip.
        const shared = std.mem.findDiff(u8, target_key, self.base_key) orelse
            @min(target_key.len, self.base_key.len);

        var lo: usize = 0;
        var hi: usize = self.layout.entry_count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const ord = try self.cmpStoredKey(mid, target_key, shared);
            if (ord == .lt) {
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
        // Parsed per call rather than cached in setIdx: Key.init is three
        // loads off a hot key_buf, where caching costs a 40-byte store plus
        // a reload, and every caller reads it about once per position.
        // Measured: caching made a key walk ~20% SLOWER.
        return .{
            .key = Key.init(self.key_buf.items),
            .value = self.val,
            .meta = self.meta,
        };
    }

    /// Start of entry i within entries_region.
    inline fn getOffset(self: *BlockIterator, i: usize) Error!usize {
        const at = self.layout.offsets_off + (i * 4);
        return @intCast(try _idx.readU32LE(self.block, at));
    }

    fn setIdx(self: *BlockIterator, i: usize) Error!void {
        if (i >= self.layout.entry_count) {
            self.is_valid = false;
            return;
        }

        const er = self.entries_region;
        const off = try self.getOffset(i);
        // 5-byte fixed header: [u16 overlap][u16 diff_len][u8 meta].
        if (off + 5 > er.len) return error.CorruptTable;

        const key_overlap: usize = @intCast(try _idx.readU16LE(er, off));
        const key_diff_len: usize = @intCast(try _idx.readU16LE(er, off + 2));
        if (key_overlap > self.base_key.len) return error.CorruptTable;
        const meta = Meta.fromByte(er[off + 4]);

        const diff_start = off + 5;
        const diff_end = diff_start + key_diff_len;
        if (diff_end > er.len) return error.CorruptTable;
        const diff = er[diff_start..diff_end];

        // Reconstruct full key into reusable buffer: base[0..overlap] ++ diff.
        self.key_buf.clearRetainingCapacity();
        try self.key_buf.appendSlice(self.alloc, self.base_key[0..key_overlap]);
        try self.key_buf.appendSlice(self.alloc, diff);

        // Value boundary: next entry's offset, or end of the entries
        // region for the last entry.
        const value_start = diff_end;
        const value_end = if (i + 1 < self.layout.entry_count)
            try self.getOffset(i + 1)
        else
            er.len;
        if (value_end < value_start or value_end > er.len) {
            return error.CorruptTable;
        }

        self.val = er[value_start..value_end];
        self.meta = meta;
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
    current_block: ?*_cache.Value = null,
    valid: bool,
    priority: vfs.Priority,
    end_key: ?Key = null, // exclusive upper bound; owned (duped)
    end_bpos: usize = 0, // block index where end_key lives
    /// Reusable scratch for the marshaled seek target — BlockIterator's
    /// binary search compares against raw bytes, so we build them once
    /// per seek rather than once per probe.
    seek_buf: std.ArrayList(u8) = .empty,

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
        if (self.end_key) |ek| ek.free(self.allocator);
        self.seek_buf.deinit(self.allocator);
        self.table.decrRef();
        if (self.bi) |*bi| bi.deinit();
        if (self.current_block) |b| b.decrRef();
    }

    /// Seek to the first entry >= start, stopping before any entry with key >= end.
    /// Empty start means seek to first; empty end means no upper bound.
    /// Dupes end so it remains valid for the lifetime of this iterator.
    pub fn seekRange(self: *TableIterator, start: Key, end: Key) !void {
        // Set up end bound.
        if (self.end_key) |ek| ek.free(self.allocator);
        self.end_key = null;

        if (end.len() > 0) {
            if (end.order(self.table.smallest) != .gt) {
                // end <= smallest: nothing to return.
                self.valid = false;
                return;
            }
            if (end.order(self.table.biggest) != .gt) {
                // The end key is within this table's key range.
                self.end_key = try end.dupe(self.allocator);
                // Find the block containing end_key: last block with base key <= end.
                const blocks = self.table.index.blocks;
                const ei = utils.search(_buffers.BlockIndexStruct, blocks, end, struct {
                    fn f(ek: Key, b: _buffers.BlockIndexStruct) bool {
                        return Key.init(b.key).order(ek) == .gt;
                    }
                }.f);
                self.end_bpos = if (ei > 0) ei - 1 else 0;
            }
        }

        // Seek. The landing item can be past `end` (start beyond the
        // last in-range key lands on whatever follows), so the bound
        // applies here just as it does after every next().
        if (start.len() == 0) {
            try self.seekToFirst();
        } else {
            try self.seekForward(start);
        }
        self.checkEndKey();
    }

    fn seekToFirst(self: *TableIterator) Error!void {
        if (self.table.blockCount() == 0) {
            self.valid = false;
            return;
        }
        try self.setBlock(0);
        try self.bi.?.seekToFirst();
        self.valid = self.bi.?.is_valid;
    }

    fn seekForward(self: *TableIterator, target: Key) Error!void {
        self.valid = false;
        const blocks = self.table.index.blocks;
        if (blocks.len == 0) return;

        // First block whose base key > target.
        const bi_idx = utils.search(_buffers.BlockIndexStruct, blocks, target, struct {
            fn f(sk: Key, b: _buffers.BlockIndexStruct) bool {
                return Key.init(b.key).order(sk) == .gt;
            }
        }.f);

        // The block we want is idx-1 (last block with base key <= target),
        // or 0 if all base keys are > target.
        const lo = if (bi_idx > 0) bi_idx - 1 else 0;

        // Marshal once; the in-block search compares against these bytes.
        self.seek_buf.clearRetainingCapacity();
        const traw = try target.marshalTo(self.allocator, &self.seek_buf);

        try self.setBlock(lo);
        try self.bi.?.seekGe(traw);
        if (self.bi.?.is_valid) {
            self.valid = true;
            return;
        }

        // Not found in block lo — target may sit at the start of the next block.
        if (lo + 1 < blocks.len) {
            try self.setBlock(lo + 1);
            try self.bi.?.seekToFirst();
            self.valid = self.bi.?.is_valid;
        }
    }

    pub fn next(self: *TableIterator) Error!void {
        if (!self.valid) return;
        defer self.checkEndKey(); // Always do it.

        try self.bi.?.next();
        if (self.bi.?.is_valid) {
            return;
        }

        // End of current block — advance to the next.
        const next_bpos = self.bpos + 1;
        if (self.end_key != null and next_bpos > self.end_bpos) {
            // No need to fetch the block.
            self.valid = false;
            return;
        }
        if (next_bpos >= self.table.blockCount()) {
            self.valid = false;
            return;
        }
        try self.setBlock(next_bpos);
        try self.bi.?.seekToFirst();
        self.valid = self.bi.?.is_valid;
    }

    /// Invalidates the iterator if current item >= end_key.
    inline fn checkEndKey(self: *TableIterator) void {
        if (!self.valid) return;
        const ek = self.end_key orelse return;

        if (self.bpos > self.end_bpos) {
            self.valid = false;
            return;
        }
        if (self.bpos < self.end_bpos) {
            return;
        }
        const item = self.bi.?.itemRef() orelse return;
        if (item.key.order(ek) != .lt) {
            self.valid = false;
        }
    }

    pub fn itemRef(self: *const TableIterator) ?Item {
        if (!self.valid) return null;
        return self.bi.?.itemRef();
    }

    /// The refcounted block backing the current item's value, or null
    /// when not positioned. A caller that keeps item.value past next()
    /// incrRefs this pin (and decrRefs when done) instead of copying
    /// the bytes — the block stays alive even after the iterator
    /// advances or dies.
    pub fn currentBlock(self: *const TableIterator) ?*_cache.Value {
        if (!self.valid) return null;
        return self.current_block;
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
