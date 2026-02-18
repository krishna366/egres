const std = @import("std");
const builtin = @import("builtin");
const raftlog = @import("raftwal.zig");

fn readInt(comptime T: type, buf: []const u8, endian: std.builtin.Endian) T {
    const n = @divExact(@typeInfo(T).int.bits, 8);
    std.debug.assert(buf.len >= n);
    const p: *const [n]u8 = @ptrCast(buf.ptr);
    return std.mem.readInt(T, p, endian);
}

fn writeInt(comptime T: type, buf: []u8, value: T, endian: std.builtin.Endian) void {
    const n = @divExact(@typeInfo(T).int.bits, 8);
    std.debug.assert(buf.len >= n);
    const p: *[n]u8 = @ptrCast(buf.ptr);
    std.mem.writeInt(T, p, value, endian);
}

// DiskWAL handles disk access and writes for the Raft write-ahead log.
// Directory layout contains `wal.meta` and `<start-idx>.wal` segment files.
//
// === wal.meta file ===
// This file is small and intended to stay in one page.
//
// Layout:
// 00-08 bytes: Raft ID
// 08-16 bytes: Group ID
// 16-24 bytes: Checkpoint index
// 512 bytes: HardState (marshalled)
// 1024-1032 bytes: Snapshot index
// 1032-1040 bytes: Snapshot term
// 1040 bytes: Snapshot (marshalled)
//
// --- <0000i>.wal files ---
// These files contain raft log entries.
//
// Each file stores a base entry in a small fixed header:
// - base index: u64 (last index of previous file)
// - base term: u64 (term at base index)
//
// Slot 0 is reserved as this base entry. Real entries start from slot 1 and are
// encoded as deltas from base, plus type, payload offset and payload length:
// - term_delta: u32
// - index_delta: u16
// - type: u16
// - data offset: u32
// - data length: u32
//
// Slot size is 16 bytes. We keep a fixed 1 MiB slot region (`log_file_offset`).
//
// We pre-allocate and zero this 1 MiB slot region. Zeroed slots allow restart recovery
// to find the first empty slot safely.
//
// Entry payload bytes are stored after `log_file_offset`. Entry slots keep offsets into
// this payload area. After ~60k entries, rotate to a new segment file.
//
// --- cleanup ---
// If snapshot index is `idx_s`, keep the first segment whose first entry is <= `idx_s`,
// and all segments after it. Segments strictly below that can be deleted.
//
// --- sync ---
// mmap generally handles process crash safety well enough for default mode.
// If hard sync is enabled, call msync/fdatasync after writes.

pub const meta_name = "wal.meta";
pub const meta_file_size: usize = 1 << 20;
pub const hard_state_offset: usize = 512;
pub const snapshot_offset: usize = 1024;

pub const max_num_entries: usize = 60_000;
pub const log_file_offset: usize = 1 << 20;
pub const log_file_size: usize = 256 << 20;
pub const entry_size: usize = 16;
pub const log_suffix = ".wal";
pub const log_header_magic: u32 = 0x57414C32; // "WAL2"
pub const log_header_version: u32 = 1;
pub const log_header_size: usize = 24;
pub const first_data_slot: usize = 1;

pub const MetaInfo = enum {
    raft_id,
    group_id,
    checkpoint_index,
    snapshot,
    hard_state,
};

pub const Error = error{
    CorruptMeta,
    InvalidMetaInfo,
    CorruptLogFile,
    InvalidLogOffset,
    InvalidLogHeader,
} || std.posix.MMapError || std.posix.MSyncError || std.posix.SyncError ||
    std.fs.Dir.OpenError || std.fs.Dir.MakeError || std.fs.File.OpenError ||
    std.fs.Dir.DeleteFileError ||
    std.fs.File.StatError || std.fs.File.SetEndPosError ||
    std.mem.Allocator.Error || std.fmt.BufPrintError;

pub const err_not_found = error{NotFound};

fn metaOffset(comptime info: MetaInfo) usize {
    return switch (info) {
        .raft_id => 0,
        .group_id => 8,
        .checkpoint_index => 16,
        .snapshot => snapshot_offset,
        .hard_state => hard_state_offset,
    };
}

fn writeSlice(dst: []u8, src: []const u8) Error!void {
    if (dst.len < 4 + src.len) return error.CorruptMeta;
    writeInt(u32, dst[0..4], @as(u32, @intCast(src.len)), .little);
    @memcpy(dst[4 .. 4 + src.len], src);
}

fn readSlice(src: []const u8) Error![]const u8 {
    if (src.len < 4) return error.CorruptMeta;
    const size = readInt(u32, src[0..4], .little);
    if (src.len < 4 + size) return error.CorruptMeta;
    return src[4 .. 4 + size];
}

pub const entry = struct {
    term: u64,
    index: u64,
    data_offset: u32,
    data_len: u32,
    ty: u16,
};

pub const metaFile = struct {
    alloc: std.mem.Allocator,
    file: std.fs.File,
    data: []align(std.heap.page_size_min) u8,

    pub fn openPath(path: []const u8, alloc: std.mem.Allocator) Error!metaFile {
        const cwd = std.fs.cwd();
        var dir = cwd.openDir(path, .{}) catch |err| switch (err) {
            error.FileNotFound => blk: {
                try cwd.makePath(path);
                break :blk try cwd.openDir(path, .{});
            },
            else => return err,
        };
        defer dir.close();
        return openInDir(dir, alloc);
    }

    pub fn openInDir(dir: std.fs.Dir, alloc: std.mem.Allocator) Error!metaFile {
        var created = false;
        var file = dir.openFile(meta_name, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => blk: {
                created = true;
                break :blk try dir.createFile(meta_name, .{ .read = true, .truncate = false });
            },
            else => return err,
        };
        errdefer file.close();

        const st = try file.stat();
        if (st.size < meta_file_size) {
            try file.setEndPos(meta_file_size);
        }

        const map = try std.posix.mmap(
            null,
            meta_file_size,
            std.posix.PROT.READ | std.posix.PROT.WRITE,
            .{ .TYPE = .SHARED },
            file.handle,
            0,
        );
        errdefer std.posix.munmap(map);

        if (created) {
            @memset(map, 0);
        }
        return .{
            .alloc = alloc,
            .file = file,
            .data = map,
        };
    }

    pub fn deinit(self: *metaFile) void {
        std.posix.munmap(self.data);
        self.file.close();
    }

    pub fn sync(self: *metaFile, hard: bool) Error!void {
        try std.posix.msync(self.data, if (hard) std.posix.MSF.SYNC else std.posix.MSF.ASYNC);
        if (hard) {
            try std.posix.fdatasync(self.file.handle);
        }
    }

    inline fn checkOffset(comptime offset: usize) void {
        if (offset % 8 != 0) {
            @compileError("offset must be a multiple of 8");
        }
    }
    inline fn u64Buf(self: *metaFile, info: MetaInfo, comptime offset: usize) []u8 {
        checkOffset(offset);
        const idx = metaOffset(info) + offset;
        return self.data[idx .. idx + 8];
    }

    pub fn hardState(self: *metaFile) Error!raftlog.HardState {
        if (self.data.len < hard_state_offset + 24) return error.CorruptMeta;
        return .{
            .current_term = readInt(u64, self.u64Buf(.hard_state, 0), .little),
            .voted_for = readInt(u64, self.u64Buf(.hard_state, 8), .little),
            .commit_index = readInt(u64, self.u64Buf(.hard_state, 16), .little),
        };
    }

    pub fn storeHardState(self: *metaFile, hs: raftlog.HardState) Error!void {
        if (self.data.len < hard_state_offset + 24) return error.CorruptMeta;
        writeInt(u64, self.u64Buf(.hard_state, 0), hs.current_term, .little);
        writeInt(u64, self.u64Buf(.hard_state, 8), hs.voted_for, .little);
        writeInt(u64, self.u64Buf(.hard_state, 16), hs.commit_index, .little);
    }

    pub fn snapshot(self: *metaFile) Error!raftlog.Snapshot {
        const snap_data = try readSlice(self.data[snapshot_offset + 16 ..]);
        return .{
            .index = readInt(u64, self.u64Buf(.snapshot, 0), .little),
            .term = readInt(u64, self.u64Buf(.snapshot, 8), .little),
            .data = if (snap_data.len == 0) &[_]u8{} else try self.alloc.dupe(u8, snap_data),
        };
    }

    pub fn storeSnapshot(self: *metaFile, snap: raftlog.Snapshot) Error!void {
        writeInt(u64, self.u64Buf(.snapshot, 0), snap.index, .little);
        writeInt(u64, self.u64Buf(.snapshot, 8), snap.term, .little);
        try writeSlice(self.data[snapshot_offset + 16 ..], snap.data);
    }
};

pub const logFile = struct {
    fid: u64,
    file: std.fs.File,
    data: []align(std.heap.page_size_min) u8,
    base_entry: entry,

    fn fname(fid: u64, buf: *[32]u8) Error![]const u8 {
        return std.fmt.bufPrint(buf, "{d:0>5}" ++ log_suffix, .{fid});
    }

    pub fn openPath(path: []const u8, fid: u64) Error!logFile {
        const cwd = std.fs.cwd();
        var dir = cwd.openDir(path, .{}) catch |err| switch (err) {
            error.FileNotFound => blk: {
                try cwd.makePath(path);
                break :blk try cwd.openDir(path, .{});
            },
            else => return err,
        };
        defer dir.close();
        return openInDir(dir, fid);
    }

    pub fn openInDir(dir: std.fs.Dir, fid: u64) Error!logFile {
        var name_buf: [32]u8 = undefined;
        const name = try fname(fid, &name_buf);

        var created = false;
        var file = dir.openFile(name, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => blk: {
                created = true;
                break :blk try dir.createFile(name, .{ .read = true, .truncate = false });
            },
            else => return err,
        };
        errdefer file.close();

        // TODO: logFile sizes are fixed right now to log_file_size. This is easier to reason
        // about because I'm mmap'ing the files. Later, if I switch to using a io_uring style
        // or Zig's async await system, it might be better to switch to just files, so we can
        // better batch up our reads and writes.
        const st = try file.stat();
        if (st.size < log_file_size) {
            try file.setEndPos(log_file_size);
        }

        const map = try std.posix.mmap(
            null,
            log_file_size,
            std.posix.PROT.READ | std.posix.PROT.WRITE,
            .{ .TYPE = .SHARED },
            file.handle,
            0,
        );
        errdefer std.posix.munmap(map);

        if (created) {
            // Zero slot area so restart can detect the first empty slot.
            @memset(map[0..log_file_offset], 0);
            writeInt(u32, map[0..4], log_header_magic, .little);
            writeInt(u32, map[4..8], log_header_version, .little);
            writeInt(u64, map[8..16], 0, .little);
            writeInt(u64, map[16..24], 0, .little);
        }

        const magic = readInt(u32, map[0..4], .little);
        const version = readInt(u32, map[4..8], .little);
        if (magic != log_header_magic or version != log_header_version) {
            return error.InvalidLogHeader;
        }
        const base = entry{
            .index = readInt(u64, map[8..16], .little),
            .term = readInt(u64, map[16..24], .little),
            .data_offset = 0,
            .data_len = 0,
            .ty = 0,
        };

        return .{
            .fid = fid,
            .file = file,
            .data = map,
            .base_entry = base,
        };
    }

    pub fn deinit(self: *logFile) void {
        std.posix.munmap(self.data);
        self.file.close();
    }

    pub fn sync(self: *logFile, hard: bool) Error!void {
        try std.posix.msync(self.data, if (hard) std.posix.MSF.SYNC else std.posix.MSF.ASYNC);
        if (hard) {
            try std.posix.fdatasync(self.file.handle);
        }
    }

    pub fn truncate(self: *logFile, size: usize) Error!void {
        try self.file.setEndPos(size);
    }

    pub fn entrySlot(self: *const logFile, slot_index: usize) []u8 {
        std.debug.assert(slot_index < max_num_entries);
        const off = log_header_size + slot_index * entry_size;
        return self.data[off .. off + entry_size];
    }

    pub fn getEntry(self: *const logFile, slot_index: usize) entry {
        if (slot_index == 0) return self.base_entry;

        const slot = self.entrySlot(slot_index);
        const term_delta = readInt(u32, slot[0..4], .little);
        const index_delta = readInt(u16, slot[4..6], .little);
        if (index_delta == 0) {
            return .{ .term = 0, .index = 0, .data_offset = 0, .data_len = 0, .ty = 0 };
        }
        return .{
            .term = self.base_entry.term + term_delta,
            .index = self.base_entry.index + index_delta,
            .ty = readInt(u16, slot[6..8], .little),
            .data_offset = readInt(u32, slot[8..12], .little),
            .data_len = readInt(u32, slot[12..16], .little),
        };
    }

    pub fn getIndex(self: *const logFile, slot_index: usize) u64 {
        std.debug.assert(slot_index < max_num_entries);
        if (slot_index == 0) return self.base_entry.index;

        const slot = self.entrySlot(slot_index);
        const index_delta = readInt(u16, slot[4..6], .little);
        if (index_delta == 0) return 0;

        return self.base_entry.index + index_delta;
    }

    pub fn setEntry(self: *logFile, slot_index: usize, e: entry) void {
        if (slot_index == 0) {
            self.setBaseEntry(e);
            return;
        }

        if (e.index == 0) {
            @memset(self.entrySlot(slot_index), 0);
            return;
        }

        std.debug.assert(e.index > self.base_entry.index);
        std.debug.assert(e.term >= self.base_entry.term);
        const index_delta = e.index - self.base_entry.index;
        const term_delta = e.term - self.base_entry.term;
        std.debug.assert(index_delta <= std.math.maxInt(u16));
        std.debug.assert(term_delta <= std.math.maxInt(u32));
        std.debug.assert(e.data_offset <= std.math.maxInt(u32));
        std.debug.assert(e.data_len <= std.math.maxInt(u32));

        const slot = self.entrySlot(slot_index);
        writeInt(u32, slot[0..4], @intCast(term_delta), .little);
        writeInt(u16, slot[4..6], @intCast(index_delta), .little);
        writeInt(u16, slot[6..8], e.ty, .little);
        writeInt(u32, slot[8..12], e.data_offset, .little);
        writeInt(u32, slot[12..16], e.data_len, .little);
    }

    pub fn setBaseEntry(self: *logFile, e: entry) void {
        self.base_entry = .{
            .term = e.term,
            .index = e.index,
            .data_offset = 0,
            .data_len = 0,
            .ty = 0,
        };
        writeInt(u64, self.data[8..16], self.base_entry.index, .little);
        writeInt(u64, self.data[16..24], self.base_entry.term, .little);
    }

    pub fn firstIndex(self: *const logFile) u64 {
        return self.base_entry.index + 1;
    }

    pub fn firstEmptySlot(self: *const logFile) usize {
        // Slots are contiguous from `first_data_slot` until first empty slot, then zero.
        // Use lower-bound to find first slot where index == 0.
        var lo: usize = first_data_slot;
        var hi: usize = max_num_entries;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.getIndex(mid) == 0) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        return lo;
    }

    pub fn lastEntry(self: *const logFile) entry {
        const first_empty = self.firstEmptySlot();
        if (first_empty <= first_data_slot) return self.base_entry;
        if (first_empty >= max_num_entries) return self.getEntry(max_num_entries - 1);
        return self.getEntry(first_empty - 1);
    }

    // Returns:
    // - `-1` if raft_index is before file range
    // - `[0, max_num_entries)` if found/insert position inside this file
    // - `max_num_entries` if raft_index is beyond this file range
    pub fn slotGe(self: *const logFile, raft_index: u64) isize {
        const base = self.base_entry.index;
        if (raft_index <= base) return -1;

        // Slot 0 is reserved for base. Real entries start at slot 1 and map to:
        // slot = raft_index - base_index.
        const diff = raft_index - base;
        if (diff >= max_num_entries) return max_num_entries;

        const idx: usize = @intCast(diff);
        const stored = self.getIndex(idx);
        if (stored == 0) return @intCast(idx);
        std.debug.assert(stored == raft_index);
        return @intCast(idx);
    }

    // Returns payload bytes from `[offset, offset + len)`.
    pub fn sliceAt(self: *const logFile, offset: usize, len: usize) Error![]const u8 {
        if (offset < log_file_offset or offset >= self.data.len) return error.InvalidLogOffset;
        if (offset + len > self.data.len) return error.InvalidLogOffset;
        return self.data[offset .. offset + len];
    }

    // Allocates payload region in-place and returns destination bytes plus next offset.
    pub fn allocateSlice(
        self: *logFile,
        offset: usize,
        payload_len: usize,
    ) Error!struct { dst: []u8, next: usize } {
        if (offset < log_file_offset or offset >= self.data.len) return error.InvalidLogOffset;
        if (offset + payload_len > self.data.len) return error.InvalidLogOffset;
        return .{
            .dst = self.data[offset .. offset + payload_len],
            .next = offset + payload_len,
        };
    }
};

pub const wal = struct {
    allocator: std.mem.Allocator,
    dir_path: []u8,
    files: std.ArrayList(logFile),
    current: logFile,
    next_entry_idx: usize,

    const Pos = struct {
        file_idx: isize, // -1 means current
        slot_idx: isize,
    };

    fn parseWalFid(name: []const u8) ?u64 {
        if (!std.mem.endsWith(u8, name, log_suffix)) return null;
        const base = name[0 .. name.len - log_suffix.len];
        if (base.len == 0) return null;
        var fid: u64 = 0;
        for (base) |c| {
            if (c < '0' or c > '9') return null;
            fid = fid * 10 + (c - '0');
        }
        return if (fid == 0) null else fid;
    }

    fn deleteFileByFid(self: *wal, fid: u64) void {
        var name_buf: [32]u8 = undefined;
        const name = logFile.fname(fid, &name_buf) catch return;
        const cwd = std.fs.cwd();
        var dir = cwd.openDir(self.dir_path, .{}) catch return;
        defer dir.close();
        dir.deleteFile(name) catch {};
    }

    fn deleteFidFiles(dir: std.fs.Dir, fids: []const u64) void {
        var name_buf: [32]u8 = undefined;
        for (fids) |fid| {
            const name = logFile.fname(fid, &name_buf) catch continue;
            dir.deleteFile(name) catch {};
        }
    }

    pub fn openPath(path: []const u8, alloc: std.mem.Allocator) Error!wal {
        const cwd = std.fs.cwd();
        cwd.access(path, .{}) catch |err| switch (err) {
            error.FileNotFound => try cwd.makePath(path),
            else => return err,
        };
        const dir_copy = try alloc.dupe(u8, path);
        errdefer alloc.free(dir_copy);

        var dir = try cwd.openDir(path, .{ .iterate = true });
        defer dir.close();

        // Discover all segment file ids and sort ascending for replay/recovery.
        var found_fids = std.ArrayList(u64).empty;
        defer found_fids.deinit(alloc);

        var it = dir.iterate();
        while (try it.next()) |entry_dir| {
            if (entry_dir.kind != .file) continue;

            const fid = parseWalFid(entry_dir.name) orelse continue;
            try found_fids.append(alloc, fid);
        }
        std.mem.sort(u64, found_fids.items, {}, comptime std.sort.asc(u64));

        // No segments yet: initialize first file as current.
        if (found_fids.items.len == 0) {
            var cur = try logFile.openPath(path, 1);
            errdefer cur.deinit();
            return .{
                .allocator = alloc,
                .dir_path = dir_copy,
                .files = std.ArrayList(logFile).empty,
                .current = cur,
                .next_entry_idx = cur.firstEmptySlot(),
            };
        }

        // Open all segments in order, and drop malformed trailing files.
        var opened = std.ArrayList(logFile).empty;
        errdefer {
            for (opened.items) |*lf| lf.deinit();
            opened.deinit(alloc);
        }
        var first_bad: ?usize = null;
        var prev_entry: ?entry = null;
        for (found_fids.items, 0..) |fid, idx| {
            var lf = logFile.openInDir(dir, fid) catch |err| switch (err) {
                error.InvalidLogHeader, error.CorruptLogFile => {
                    first_bad = idx;
                    break;
                },
                else => return err,
            };

            // Every next file must chain from previous file's last entry.
            if (prev_entry) |prev| {
                if (lf.base_entry.index != prev.index or lf.base_entry.term != prev.term) {
                    lf.deinit();
                    first_bad = idx;
                    break;
                }
            }

            // If this segment has no data entry at slot 1, but there are newer files,
            // treat this and later files as invalid trailing data.
            if (lf.getIndex(first_data_slot) == 0 and idx + 1 < found_fids.items.len) {
                lf.deinit();
                first_bad = idx;
                break;
            }

            prev_entry = lf.lastEntry();
            try opened.append(alloc, lf);
        }

        if (first_bad) |bad_idx| {
            if (!builtin.is_test) {
                std.log.err(
                    "raft/diskwal: dropping malformed trailing WAL files from fid={d} in {s}",
                    .{ found_fids.items[bad_idx], path },
                );
            }
            deleteFidFiles(dir, found_fids.items[bad_idx..]);
        }

        // If all discovered files were malformed, start fresh with file 1.
        if (opened.items.len == 0) {
            var cur = try logFile.openPath(path, 1);
            errdefer cur.deinit();
            return .{
                .allocator = alloc,
                .dir_path = dir_copy,
                .files = std.ArrayList(logFile).empty,
                .current = cur,
                .next_entry_idx = cur.firstEmptySlot(),
            };
        }

        // Last opened file is the active segment. Earlier ones are immutable history.
        var cur = opened.pop().?;
        var files = std.ArrayList(logFile).empty;
        errdefer {
            for (files.items) |*lf| lf.deinit();
            files.deinit(alloc);
            cur.deinit();
        }
        try files.ensureTotalCapacity(alloc, opened.items.len);
        for (opened.items) |lf| {
            files.appendAssumeCapacity(lf);
        }
        opened.deinit(alloc);

        return .{
            .allocator = alloc,
            .dir_path = dir_copy,
            .files = files,
            .current = cur,
            .next_entry_idx = cur.firstEmptySlot(),
        };
    }

    pub fn deinit(self: *wal) void {
        var i: usize = 0;
        while (i < self.files.items.len) : (i += 1) {
            self.files.items[i].deinit();
        }
        self.files.deinit(self.allocator);
        self.current.deinit();
        self.allocator.free(self.dir_path);
    }

    pub fn firstIndex(self: *const wal) u64 {
        const fi = if (self.files.items.len == 0)
            self.current.firstIndex()
        else
            self.files.items[0].firstIndex();
        return if (fi == 0) 1 else fi;
    }

    pub fn lastIndex(self: *const wal) u64 {
        if (self.next_entry_idx > first_data_slot) {
            return self.current.getIndex(self.next_entry_idx - 1);
        }
        var i = self.files.items.len;
        while (i > 0) : (i -= 1) {
            const e = self.files.items[i - 1].lastEntry();
            if (e.index > 0) return e.index;
        }
        return 0;
    }

    fn getEntryFile(self: *wal, file_idx: isize) ?*logFile {
        if (file_idx == -1) return &self.current;
        if (file_idx < 0) return null;
        const idx: usize = @intCast(file_idx);
        if (idx >= self.files.items.len) return null;
        return &self.files.items[idx];
    }

    fn slotGe(self: *const wal, raft_index: u64) Pos {
        // Fast path: most reads hit the active log file.
        const off_cur = self.current.slotGe(raft_index);
        if (off_cur >= 0) return .{ .file_idx = -1, .slot_idx = off_cur };

        if (self.files.items.len == 0) return .{ .file_idx = -1, .slot_idx = -1 };

        // Binary-search historical files by their first index to find the first
        // file whose range can contain `raft_index`.
        var lo: usize = 0;
        var hi: usize = self.files.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.files.items[mid].firstIndex() >= raft_index) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        var file_idx: usize = lo;
        // Exact boundary hit: first real entry is at slot 1.
        if (file_idx < self.files.items.len and self.files.items[file_idx].firstIndex() == raft_index) {
            return .{ .file_idx = @intCast(file_idx), .slot_idx = first_data_slot };
        }

        // Otherwise, step back to the previous file and do an in-file lower-bound.
        if (file_idx > 0) file_idx -= 1;
        const off = self.files.items[file_idx].slotGe(raft_index);
        return .{ .file_idx = @intCast(file_idx), .slot_idx = off };
    }

    fn seekEntry(self: *wal, raft_index: u64) (Error || raftlog.RaftLogError || err_not_found)!entry {
        if (raft_index == 0) {
            return .{ .term = 0, .index = 0, .data_offset = 0, .data_len = 0, .ty = 0 };
        }

        const pos = self.slotGe(raft_index);
        if (pos.slot_idx < 0) return raftlog.RaftLogError.Compacted;
        if (pos.slot_idx >= max_num_entries) return raftlog.RaftLogError.Unavailable;

        const lf = self.getEntryFile(pos.file_idx) orelse return raftlog.RaftLogError.Unavailable;
        const ent = lf.getEntry(@intCast(pos.slot_idx));
        if (ent.index == 0) return raftlog.RaftLogError.Unavailable;
        if (ent.index != raft_index) return err_not_found.NotFound;
        return ent;
    }

    pub fn term(self: *wal, idx: u64) (Error || raftlog.RaftLogError || err_not_found)!u64 {
        const ent = try self.seekEntry(idx);
        return ent.term;
    }

    pub fn allEntries(
        self: *wal,
        alloc: std.mem.Allocator,
        lo: u64,
        hi: u64,
        max_size: u64,
    ) (Error || raftlog.RaftLogError)![]raftlog.StoredEntry {
        var out = std.ArrayList(raftlog.StoredEntry).empty;
        errdefer out.deinit(alloc);

        var pos = self.slotGe(lo);
        if (pos.slot_idx < 0) pos.slot_idx = first_data_slot;

        var size: u64 = 0;
        var file_idx = pos.file_idx;
        var slot_idx = pos.slot_idx;
        var lf = self.getEntryFile(file_idx) orelse return out.toOwnedSlice(alloc);

        while (true) {
            if (slot_idx >= max_num_entries) {
                if (file_idx == -1) break;
                file_idx += 1;
                if (@as(usize, @intCast(file_idx)) >= self.files.items.len) {
                    file_idx = -1;
                }
                slot_idx = first_data_slot;
                lf = self.getEntryFile(file_idx) orelse break;
            }

            const ent = lf.getEntry(@intCast(slot_idx));
            if (ent.index == 0) {
                slot_idx = max_num_entries;
                continue;
            }
            if (ent.index >= hi) break;

            const payload = try lf.sliceAt(ent.data_offset, ent.data_len);
            size += payload.len;
            if (out.items.len > 0 and max_size > 0 and size > max_size) break;

            try out.append(alloc, .{
                .index = ent.index,
                .term = ent.term,
                .data_len = @intCast(payload.len),
                // Borrow payload directly from mmap. Caller must not free these bytes.
                .bytes = payload,
            });
            slot_idx += 1;
        }
        return out.toOwnedSlice(alloc);
    }

    fn zeroEntrySlots(lf: *logFile, start_slot: usize, end_slot: usize) void {
        const start = @max(start_slot, first_data_slot);
        if (end_slot <= start) return;
        const from = log_header_size + start * entry_size;
        const to = log_header_size + end_slot * entry_size;
        @memset(lf.data[from..to], 0);
    }

    fn rotate(self: *wal, first_index: u64, payload_offset: usize) Error!void {
        std.debug.assert(first_index > 0);
        try self.current.truncate(payload_offset);
        try self.current.sync(true);

        const prev = self.current.getEntry(self.next_entry_idx - 1);
        // Invariant: `current` always has the highest file id.
        for (self.files.items) |lf| {
            std.debug.assert(lf.fid < self.current.fid);
        }
        const next_fid = self.current.fid + 1;

        var next = try logFile.openPath(self.dir_path, next_fid);
        next.setBaseEntry(prev);
        try self.files.append(self.allocator, self.current);
        self.current = next;
        self.next_entry_idx = first_data_slot;
    }

    pub fn addEntries(self: *wal, entries: []const raftlog.LogEntryBuffer) (Error || raftlog.RaftLogError)!void {
        if (entries.len == 0) return;

        // Find where this batch begins in WAL space. If the leader is overwriting
        // conflicting suffix entries, this points to the replacement position.
        const first_reader = try entries[0].initReader();
        const first_index = first_reader.index();
        const pos = self.slotGe(first_index);

        if (pos.slot_idx >= 0) {
            const slot: usize = @intCast(pos.slot_idx);
            if (pos.file_idx == -1) {
                // Overwrite in current file: clear suffix [slot, next_entry_idx).
                if (self.next_entry_idx > slot) {
                    zeroEntrySlots(&self.current, slot, self.next_entry_idx);
                }
                self.next_entry_idx = slot;
            } else {
                // Overwrite in an older file: make that file current, drop newer files,
                // and clear suffix from overwrite slot to end of file.
                const fidx: usize = @intCast(pos.file_idx);
                const new_current = self.files.orderedRemove(fidx);
                while (self.files.items.len > fidx) {
                    var extra = self.files.pop().?;
                    self.deleteFileByFid(extra.fid);
                    extra.deinit();
                }
                // We are rolling current back to an older segment. Drop the
                // previous current segment file from disk so reopen cannot
                // resurrect stale tail entries.
                self.deleteFileByFid(self.current.fid);
                self.current.deinit();
                self.current = new_current;
                zeroEntrySlots(&self.current, slot, max_num_entries);
                self.next_entry_idx = slot;
            }
        }

        // Raft invariant: written indices must be strictly contiguous.
        // - if we already have data, batch must start at last_index + 1
        // - within batch, every entry must be previous + 1
        if (self.next_entry_idx > first_data_slot) {
            const prev_index = self.current.getIndex(self.next_entry_idx - 1);
            if (prev_index == 0 or first_index != prev_index + 1) return raftlog.RaftLogError.Invalid;
        } else if (first_index != self.current.base_entry.index + 1) {
            return raftlog.RaftLogError.Invalid;
        }

        // Compute payload append position from the previous entry payload tail.
        var payload_offset: usize = log_file_offset;
        if (self.next_entry_idx > first_data_slot) {
            const prev = self.current.getEntry(self.next_entry_idx - 1);
            payload_offset = prev.data_offset + prev.data_len;
        }

        // This loop validates batch-internal continuity. The check above validates
        // continuity against already persisted WAL state, while also writing the entries to the log.
        var expected_index = first_index;
        for (entries) |entry_buf| {
            const reader = try entry_buf.initReader();
            if (reader.index() != expected_index) return raftlog.RaftLogError.Invalid;
            expected_index += 1;

            const bytes = entry_buf.bytes orelse return raftlog.RaftLogError.Invalid;
            if (self.next_entry_idx >= max_num_entries or payload_offset + bytes.len > log_file_size) {
                // Segment full: persist and rotate, then continue writes in new file.
                try self.rotate(reader.index(), payload_offset);
                payload_offset = log_file_offset;
            }

            // Write raw encoded log-entry bytes in payload area and place fixed entry
            // metadata into slot region for fast index/term lookups.
            const alloc_res = try self.current.allocateSlice(payload_offset, bytes.len);
            @memcpy(alloc_res.dst, bytes);

            self.current.setEntry(self.next_entry_idx, .{
                .term = reader.term(),
                .index = reader.index(),
                .data_offset = @intCast(payload_offset),
                .data_len = @intCast(bytes.len),
                .ty = if (reader.isConfChange()) 1 else 0,
            });
            self.next_entry_idx += 1;
            payload_offset = alloc_res.next;
        }
    }

    pub fn deleteBefore(self: *wal, raft_index: u64) void {
        const pos = self.slotGe(raft_index);
        if (pos.slot_idx < 0) return;

        if (pos.file_idx == -1) {
            while (self.files.items.len > 0) {
                var ef = self.files.pop().?;
                ef.deinit();
            }
            return;
        }

        const keep_from: usize = @intCast(pos.file_idx);
        var i: usize = 0;
        while (i < keep_from and i < self.files.items.len) : (i += 1) {
            self.files.items[i].deinit();
        }
        if (keep_from > 0 and keep_from <= self.files.items.len) {
            const remain = self.files.items[keep_from..];
            std.mem.copyForwards(logFile, self.files.items[0..remain.len], remain);
            self.files.items.len = remain.len;
        }
    }

    pub fn reset(self: *wal) void {
        while (self.files.items.len > 0) {
            var ef = self.files.pop().?;
            ef.deinit();
        }
        @memset(self.current.data[0..log_file_offset], 0);
        writeInt(u32, self.current.data[0..4], log_header_magic, .little);
        writeInt(u32, self.current.data[4..8], log_header_version, .little);
        self.current.setBaseEntry(.{ .term = 0, .index = 0, .data_offset = 0, .data_len = 0, .ty = 0 });
        self.next_entry_idx = first_data_slot;
    }

    // Drops entries from `from_index` onward, keeping prefix entries intact.
    // This is used by Raft conflict resolution when a leader overwrites suffix.
    pub fn truncateFrom(self: *wal, from_index: u64) Error!void {
        const pos = self.slotGe(from_index);
        if (pos.slot_idx < 0) return;

        const slot: usize = @intCast(pos.slot_idx);
        if (pos.file_idx == -1) {
            if (self.next_entry_idx > slot) {
                zeroEntrySlots(&self.current, slot, self.next_entry_idx);
            }
            self.next_entry_idx = slot;
            return;
        }

        const fidx: usize = @intCast(pos.file_idx);
        const new_current = self.files.orderedRemove(fidx);
        while (self.files.items.len > fidx) {
            var extra = self.files.pop().?;
            self.deleteFileByFid(extra.fid);
            extra.deinit();
        }
        // Same rollback rule as addEntries: old current must be removed on disk.
        self.deleteFileByFid(self.current.fid);
        self.current.deinit();
        self.current = new_current;
        zeroEntrySlots(&self.current, slot, max_num_entries);
        self.next_entry_idx = slot;
    }

    pub fn sync(self: *wal, hard: bool) Error!void {
        // The other files should have already been synced. No need to resync them.
        try self.current.sync(hard);
    }
};

pub const DiskWAL = struct {
    allocator: std.mem.Allocator,
    meta: metaFile,
    wal_store: wal,
    hard_state_cache: raftlog.HardState,
    snapshot_cache: raftlog.Snapshot,
    scratch_entries: std.ArrayList(raftlog.StoredEntry),

    pub fn openPath(path: []const u8, allocator: std.mem.Allocator) Error!DiskWAL {
        var mf = try metaFile.openPath(path, allocator);
        errdefer mf.deinit();

        var wf = try wal.openPath(path, allocator);
        errdefer wf.deinit();

        const hs = try mf.hardState();
        const snap = try mf.snapshot();
        errdefer if (snap.data.len != 0) allocator.free(@constCast(snap.data));
        if (snap.index > 0 and wf.firstIndex() != snap.index + 1) {
            return error.CorruptLogFile;
        }
        const snap_data: []const u8 = if (snap.data.len == 0)
            &[_]u8{}
        else
            snap.data;

        return .{
            .allocator = allocator,
            .meta = mf,
            .wal_store = wf,
            .hard_state_cache = hs,
            .snapshot_cache = .{
                .index = snap.index,
                .term = snap.term,
                .data = snap_data,
            },
            .scratch_entries = std.ArrayList(raftlog.StoredEntry).empty,
        };
    }

    pub fn deinit(self: *DiskWAL) void {
        self.clearScratchEntries();
        self.scratch_entries.deinit(self.allocator);
        if (self.snapshot_cache.data.len != 0) {
            self.allocator.free(self.snapshot_cache.data);
        }
        self.wal_store.deinit();
        self.meta.deinit();
    }

    pub fn asRaftWAL(self: *DiskWAL) raftlog.RaftWAL {
        return .{ .ctx = self, .vtable = &vtable };
    }

    fn fromCtx(ctx: *anyopaque) *DiskWAL {
        return @ptrCast(@alignCast(ctx));
    }

    fn mapToRaftLogError(err: anyerror) raftlog.RaftLogError {
        return switch (err) {
            raftlog.RaftLogError.Compacted => raftlog.RaftLogError.Compacted,
            raftlog.RaftLogError.Unavailable => raftlog.RaftLogError.Unavailable,
            err_not_found.NotFound => raftlog.RaftLogError.Unavailable,
            error.OutOfMemory => error.OutOfMemory,
            else => raftlog.RaftLogError.Unavailable,
        };
    }

    fn clearScratchEntries(self: *DiskWAL) void {
        // Scratch entries borrow payload bytes from mmap-backed WAL segments.
        // Only clear metadata here; do not free the payload slices.
        self.scratch_entries.shrinkRetainingCapacity(0);
    }

    fn refillScratch(
        self: *DiskWAL,
        lo: u64,
        hi: u64,
        max_bytes: u64,
    ) raftlog.RaftLogError![]const raftlog.StoredEntry {
        self.clearScratchEntries();
        const fetched = self.wal_store.allEntries(self.allocator, lo, hi, max_bytes) catch |err| {
            return mapToRaftLogError(err);
        };
        defer self.allocator.free(fetched);

        try self.scratch_entries.ensureTotalCapacity(self.allocator, fetched.len);
        for (fetched) |stored| {
            self.scratch_entries.appendAssumeCapacity(stored);
        }
        return self.scratch_entries.items;
    }

    fn hardState(ctx: *anyopaque) raftlog.HardState {
        const self = fromCtx(ctx);
        return self.hard_state_cache;
    }

    fn setHardState(ctx: *anyopaque, hs: raftlog.HardState) raftlog.RaftLogError!void {
        const self = fromCtx(ctx);
        self.hard_state_cache = hs;
        self.meta.storeHardState(hs) catch |err| return mapToRaftLogError(err);
    }

    fn snapshot(ctx: *anyopaque) raftlog.Snapshot {
        const self = fromCtx(ctx);
        return self.snapshot_cache;
    }

    fn setSnapshot(ctx: *anyopaque, snap: raftlog.Snapshot) raftlog.RaftLogError!void {
        const self = fromCtx(ctx);
        const snap_copy: []const u8 = if (snap.data.len == 0)
            &[_]u8{}
        else
            try self.allocator.dupe(u8, snap.data);
        errdefer if (snap_copy.len != 0) self.allocator.free(snap_copy);

        self.meta.storeSnapshot(snap) catch |err| return mapToRaftLogError(err);
        if (self.snapshot_cache.data.len != 0) {
            self.allocator.free(self.snapshot_cache.data);
        }
        self.snapshot_cache = .{
            .index = snap.index,
            .term = snap.term,
            .data = snap_copy,
        };
    }

    fn firstIndex(ctx: *anyopaque) u64 {
        const self = fromCtx(ctx);
        return self.wal_store.firstIndex();
    }

    fn lastIndex(ctx: *anyopaque) u64 {
        const self = fromCtx(ctx);
        return self.wal_store.lastIndex();
    }

    fn lastTerm(ctx: *anyopaque) u64 {
        const self = fromCtx(ctx);
        const last_idx = self.wal_store.lastIndex();
        if (last_idx == 0) return self.snapshot_cache.term;
        return self.wal_store.term(last_idx) catch self.snapshot_cache.term;
    }

    fn term(ctx: *anyopaque, idx: u64) raftlog.RaftLogError!?u64 {
        if (idx == 0) return 0;
        const self = fromCtx(ctx);
        return self.wal_store.term(idx) catch |err| switch (err) {
            raftlog.RaftLogError.Compacted => null,
            raftlog.RaftLogError.Unavailable => null,
            err_not_found.NotFound => null,
            else => return mapToRaftLogError(err),
        };
    }

    fn entryRef(ctx: *anyopaque, idx: u64) raftlog.RaftLogError!?raftlog.StoredEntry {
        const self = fromCtx(ctx);
        const items = try self.refillScratch(idx, idx + 1, 0);
        if (items.len == 0) return null;
        return items[0];
    }

    fn entriesFromRef(
        ctx: *anyopaque,
        start_index: u64,
        max_count: u32,
    ) raftlog.RaftLogError![]const raftlog.StoredEntry {
        const self = fromCtx(ctx);
        if (max_count == 0) return &[_]raftlog.StoredEntry{};
        const hi = start_index + @as(u64, max_count);
        return self.refillScratch(start_index, hi, 0);
    }

    fn entriesRangeRef(
        ctx: *anyopaque,
        lo: u64,
        hi: u64,
        max_bytes: u64,
    ) raftlog.RaftLogError![]const raftlog.StoredEntry {
        const self = fromCtx(ctx);
        if (lo >= hi) return &[_]raftlog.StoredEntry{};
        return self.refillScratch(lo, hi, max_bytes);
    }

    fn firstIndexOfTerm(ctx: *anyopaque, term_value: u64) u64 {
        const self = fromCtx(ctx);
        var idx = self.wal_store.firstIndex();
        const last = self.wal_store.lastIndex();
        while (idx <= last) : (idx += 1) {
            const t = self.wal_store.term(idx) catch continue;
            if (t == term_value) return idx;
        }
        return 0;
    }

    fn lastIndexOfTerm(ctx: *anyopaque, term_value: u64) u64 {
        const self = fromCtx(ctx);
        var idx = self.wal_store.lastIndex();
        const first = self.wal_store.firstIndex();
        while (idx >= first and idx > 0) : (idx -= 1) {
            const t = self.wal_store.term(idx) catch continue;
            if (t == term_value) return idx;
        }
        return 0;
    }

    fn append(ctx: *anyopaque, entries: []const raftlog.LogEntryBuffer) raftlog.RaftLogError!void {
        const self = fromCtx(ctx);
        self.wal_store.addEntries(entries) catch |err| return mapToRaftLogError(err);
    }

    fn truncateFrom(ctx: *anyopaque, from_index: u64) void {
        const self = fromCtx(ctx);
        self.wal_store.truncateFrom(from_index) catch {};
    }

    fn compactUntil(ctx: *anyopaque, index: u64) void {
        const self = fromCtx(ctx);
        self.wal_store.deleteBefore(index);
    }

    fn sync(ctx: *anyopaque) raftlog.RaftLogError!void {
        const self = fromCtx(ctx);
        self.meta.sync(true) catch |err| return mapToRaftLogError(err);
        self.wal_store.sync(true) catch |err| return mapToRaftLogError(err);
    }

    const vtable = raftlog.RaftWAL.VTable{
        .hardState = hardState,
        .setHardState = setHardState,
        .snapshot = snapshot,
        .setSnapshot = setSnapshot,
        .firstIndex = firstIndex,
        .lastIndex = lastIndex,
        .lastTerm = lastTerm,
        .term = term,
        .entryRef = entryRef,
        .entriesFromRef = entriesFromRef,
        .entriesRangeRef = entriesRangeRef,
        .firstIndexOfTerm = firstIndexOfTerm,
        .lastIndexOfTerm = lastIndexOfTerm,
        .append = append,
        .truncateFrom = truncateFrom,
        .compactUntil = compactUntil,
        .sync = sync,
    };
};
