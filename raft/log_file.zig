const std = @import("std");
const builtin = @import("builtin");
const _raftlog = @import("raftwal.zig");
const zen = @import("buffers_gen");
const zbuf = @import("zbuf").zbuf;
const _vfs = @import("vfs");
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;
const DiskWAL = @import("diskwal.zig").DiskWAL;

// One log file of the raft log, and what surrounds it: the file
// layout and its options (`OpenOptions`), the log file itself
// (`LogFile`), the one prepared ahead of rotation (`NextLog`), and the
// batch buffer that feeds the current one (`LogBuf`). The log file list
// and the locked API are in diskwal.zig.
//
// --- <fid>.wal ---
// One log file holds a run of consecutive raft entries. Its name is the
// five-digit (or longer) file id, increasing with time; the highest id
// is the file being appended to.
//
// Layout:
//   [0, 16 MiB)             header region (`log_file_offset`)
//   [16 MiB, log_file_size) records, back to back
//
// The header region holds one zbuf `WalHeader` message inside a zbuf
// frame ([hash | len | payload], `zbuf.Frame`); the rest of the region
// is zeros, written by the fill or never written (a hole reads as
// zeros). The message is an array of `WalSlot`s, one per record:
// term, index, HLC timestamp, the record's offset and length in the
// file, flags, and the entry type. It is written when the file rotates
// out; until then the header is the empty message from `createNamed`.
//
// Each record is a zbuf frame around one `LogEntry` message. The frame's
// hash covers the payload, so a torn tail is detected on open.
//
// A file is created at `log_file_size` and, with `prezero` on,
// zero-filled to that size before its first record, so no append ever
// grows it (see `OpenOptions.log_file_size`). It rotates out when it
// holds `max_num_entries` records, the most the header can list, or
// when the next record would cross `log_file_size`, whichever comes
// first; rotation writes the header, truncates the file to its records,
// and switches to the next file, which `NextLog` created ahead of time
// as <fid>.wal.tmp and which rotation renames into place.
//
// On open, the highest id is indexed from its records: every intact
// frame up to the first zero or torn one, which the next append
// overwrites. Every lower id is indexed from its header alone, one read
// per file. A stray .wal.tmp, a next file whose rotation never came,
// is deleted.

// 16 MiB header / 41 bytes-per-slot (37-byte WalSlot + 4-byte offset
// entry in the WalHeader's slots array) ≈ 409,000 max slots; round down
// to 380k to leave margin for the WalHeader struct, the frame prefix,
// and any future schema growth (zbuf's size_bytes lets us add fields
// without breaking older WAL files). `writeHeader` checks the fit. The
// header is sized so that a 256 MiB file of ~700-byte records fills
// its bytes before its slots: at 20k slots a file of 1 KiB records
// rotated every 8 MiB, a rotation every two seconds under pgbench.
pub const max_num_entries: usize = 380_000;
/// Slot lookups return this for "past the last slot".
pub const no_slot: usize = std.math.maxInt(usize);
pub const log_file_offset: usize = 16 << 20;
/// Zero-fill writes go out in pieces this big, at the lowest I/O
/// priority. Two reasons: a batch write that arrives while a log file is
/// being prepared waits behind at most one piece (about 2 ms on the
/// bench NVMe), never behind 256 MiB of zeros in one O_DSYNC write,
/// which a full bandwidth bucket would otherwise grant; and the zeros
/// are one buffer of this size, not a 256 MiB one.
const zero_piece: usize = 1 << 20;
/// The zeros themselves: one block in .bss, only ever read, so its
/// pages are the kernel's shared zero page and cost nothing.
var zero_block: [zero_piece]u8 = @splat(0);

/// Whether a log file is zero-filled to `log_file_size` when it is
/// created, so that no append ever grows the file. See `OpenOptions`.
pub const Prezero = enum {
    /// Off on ZFS, btrfs and tmpfs, on elsewhere; decided by statfs
    /// on the WAL directory at open. Off on macOS, where APFS is
    /// copy-on-write.
    auto,
    true,
    false,

    /// The setting to apply to log files under `dir_path`: `true` or
    /// `false`, never `auto`. The wal calls this once at open and hands
    /// the settled value down; the log file functions never see `auto`.
    pub fn resolve(self: Prezero, dir_path: String) Prezero {
        if (self != .auto) return self;
        if (builtin.os.tag != .linux) return .false;
        const magic = _vfs.fsMagic(dir_path) orelse return .true;
        return switch (magic) {
            _vfs.fs_magic.zfs, _vfs.fs_magic.btrfs, _vfs.fs_magic.tmpfs => .false,
            else => .true,
        };
    }
};

pub const OpenOptions = struct {
    /// Entries kept in memory for reads (`EntryCache`).
    cache_capacity: usize = 1024,
    /// Log file size. Every log file is created at this size, zero-filled
    /// (see `prezero`), before it is written to, so no append ever grows
    /// a file: a write that grows a file journals the size (6 ms on the
    /// bench NVMe, O_DSYNC or not), a write into zeroed blocks is at the
    /// device's floor. A record that would cross it, or the one past
    /// `max_num_entries`, rotates to the next log file, which
    /// `prepareNext` created ahead of time.
    log_file_size: u64 = 256 << 20,
    /// Zero-fill a log file to `log_file_size` when it is created. Right
    /// for ext4 and xfs, where the first write into a block journals
    /// its allocation, so an overwrite of a written block is the only
    /// write that costs nothing but the data. Pointless on
    /// copy-on-write filesystems (ZFS, btrfs), where every write
    /// allocates anew and zeros are elided, and on tmpfs, where growing
    /// a file is free; `.auto` tells them apart by statfs.
    prezero: Prezero = .auto,
};

pub const log_suffix = ".wal";
/// Suffix of a log file prepared ahead of rotation; see `wal.rotate`.
pub const tmp_suffix = ".tmp";

pub const Error = error{
    CorruptLogFile,
    InvalidLogOffset,
    InvalidLogHeader,
    WriteFailed,
    CrcMismatch,
} || std.posix.MMapError || std.posix.MSyncError || std.posix.SyncError ||
    std.Io.Dir.OpenError || std.Io.Dir.CreateDirPathError || std.Io.File.OpenError ||
    std.Io.Dir.DeleteFileError ||
    std.Io.File.StatError || std.Io.File.SetLengthError ||
    std.mem.Allocator.Error || std.fmt.BufPrintError || zen.Error;

pub const EntryType = _raftlog.EntryType;

/// In-memory slot entry. Same shape as the on-disk zbuf format
/// (`zen.WalSlotStruct`) so we can read the disk slot directly with no
/// translation. Fields default to zero so an all-zero buffer slot decodes
/// as the empty sentinel (`index == 0`).
pub const entry = zen.WalSlotStruct;

pub const LogFile = struct {
    fid: u64,
    handle: _vfs.FileHandle,
    allocator: std.mem.Allocator,
    slots: std.ArrayList(entry), // each slots[i] is a real data entry
    write_offset: u64, // next write position (meaningful for current file)
    is_current: bool,
    /// Zero-filled to `log_file_size`, so no append grows the file.
    /// False for a file handed out while its prepare was still running
    /// (NextLog.swapIn): its appends extend it, and the wal rotates off
    /// it as soon as the prepared file is ready.
    filled: bool = true,

    /// Room for any log file name: the 20 digits of a u64 fid (the
    /// `0>5` is a minimum width, not a cap) plus `.wal` and `.tmp`, 28
    /// bytes at most. `bufPrint` fails with NoSpaceLeft rather than
    /// overrun if this arithmetic ever goes stale.
    pub const NameBuf = [32]u8;

    pub fn fname(fid: u64, buf: *NameBuf) Error!String {
        return std.fmt.bufPrint(buf, "{d:0>5}" ++ log_suffix, .{fid});
    }

    pub fn tmpName(fid: u64, buf: *NameBuf) Error!String {
        return std.fmt.bufPrint(buf, "{d:0>5}" ++ log_suffix ++ tmp_suffix, .{fid});
    }

    pub fn fullPath(alloc: std.mem.Allocator, dir_path: String, name: String) ![]u8 {
        return std.fmt.allocPrint(alloc, "{s}/{s}", .{ dir_path, name });
    }

    /// Inverse of writeHeader. Reads the framed WalHeader at the start
    /// of `handle`'s header region, parses the zbuf message, and returns
    /// the data-slot list (each entry is a real WAL record). Caller owns
    /// the returned list. The first read is one page, not the 12-byte
    /// frame prefix: the disk moves a whole block either way, and a
    /// page holds a header with a few slots whole, so most files cost
    /// one read. A larger header is read a second time at exactly its
    /// frame length, never the whole region.
    pub fn readHeader(
        alloc: std.mem.Allocator,
        handle: _vfs.FileHandle,
    ) !std.ArrayList(entry) {
        const page = try handle.readAligned(alloc, 0, _vfs.page_size, .p0);
        defer page.free();

        const prefix = zbuf.Frame.readPrefix(page.data) catch
            return error.InvalidLogHeader;
        const frame_len = prefix.frameLen();
        if (frame_len > log_file_offset) return error.InvalidLogHeader;
        if (frame_len <= page.data.len) return parseHeader(alloc, page.data);

        const whole = try handle.readAligned(alloc, 0, frame_len, .p0);
        defer whole.free();
        return parseHeader(alloc, whole.data);
    }

    /// The slot list of the framed WalHeader at the start of `hdr`.
    fn parseHeader(alloc: std.mem.Allocator, hdr: String) !std.ArrayList(entry) {
        // Frame prefix [hash | len] gates the WalHeader bytes and
        // verifies the hash in one pass.
        const msg_bytes = zbuf.Frame.read(hdr) catch return error.InvalidLogHeader;

        // parseRoot validates the zbuf message tail (table_id 42255 = WalHeader).
        const hdr_reader = zen.WalHeaderReader.parseRoot(msg_bytes) catch
            return error.InvalidLogHeader;

        var slots_list = std.ArrayList(entry).empty;
        errdefer slots_list.deinit(alloc);

        const slot_count = hdr_reader.slotsLen();
        var i: u32 = 0;
        while (i < slot_count) : (i += 1) {
            const sr = try hdr_reader.slotsAt(i);
            const srv = try sr.toStructValue(alloc);
            try slots_list.append(alloc, srv);
        }
        return slots_list;
    }

    /// Open a rotated <fid>.wal file. Reads header to populate in-memory slots.
    pub fn openRotated(
        vfs_inst: *const _vfs.VFS,
        alloc: std.mem.Allocator,
        dir_path: String,
        fid: u64,
    ) !LogFile {
        var name_buf: LogFile.NameBuf = undefined;
        const name = try fname(fid, &name_buf);
        const path = try fullPath(alloc, dir_path, name);
        defer alloc.free(path);

        // O_DSYNC as for the current log file: `truncateFrom` can make
        // this one current again, and the batch writes rely on it.
        var handle = try vfs_inst.openFile(alloc, path, .{
            .mode = .default,
            .flags = .{ .writable = true, .dsync = true },
        });
        errdefer handle.close();

        var slots_list = try readHeader(alloc, handle);
        errdefer slots_list.deinit(alloc);

        var wo: u64 = log_file_offset;
        if (slots_list.items.len > 0) {
            const last = slots_list.items[slots_list.items.len - 1];
            wo = last.data_offset + zbuf.Frame.hdr_size + last.data_len;
        }

        return .{
            .fid = fid,
            .handle = handle,
            .allocator = alloc,
            .slots = slots_list,
            .write_offset = wo,
            .is_current = false,
        };
    }

    /// Index the current log file from its frames: every well-formed
    /// record up to the first zero or torn frame. Reads the entire
    /// payload region in one read, then scans in memory. Rotated
    /// log files carry their slot list in the header instead.
    fn scanFrames(
        fid: u64,
        handle: _vfs.FileHandle,
        alloc: std.mem.Allocator,
    ) !LogFile {
        var slots_list = std.ArrayList(entry).empty;
        errdefer slots_list.deinit(alloc);

        // Single read of entire payload region.
        const payload_region_size: usize = @intCast(handle.file_size - log_file_offset);
        const result = handle.readAligned(alloc, log_file_offset, payload_region_size, .p0) catch {
            return .{
                .fid = fid,
                .handle = handle,
                .allocator = alloc,
                .slots = slots_list,
                .write_offset = log_file_offset,
                .is_current = true,
            };
        };
        defer result.free();
        const data = result.data;

        // Scan zbuf-framed records in the buffer. Each call to readFrame
        // verifies the hash and slices out the payload in one shot.
        var pos: usize = 0;
        while (pos < data.len) {
            const payload = zbuf.Frame.read(data[pos..]) catch |err| switch (err) {
                // Truncated tail (final frame extends past file or fewer
                // than 8 prefix bytes left) — leave the file alone, just stop.
                zbuf.Error.OutOfBounds => break,
                // Hash mismatch — drop this frame and everything after it.
                // A torn frame: the log ends here and the next append
                // overwrites it. Not truncated: the zeros past it are
                // what keep the appends from growing the file.
                zbuf.Error.Invalid => break,
            };
            // Empty payload = zero-filled prefix in the pre-zeroed region,
            // i.e. end of real data. Real entries always carry a non-empty
            // LogEntry zbuf message.
            if (payload.len == 0) break;

            const reader = zen.LogEntryReader.parseRoot(payload) catch break;
            try slots_list.append(alloc, .{
                .term = reader.term(),
                .index = reader.index(),
                .timestamp = reader.timestamp(),
                .data_offset = @intCast(log_file_offset + pos),
                .data_len = @intCast(payload.len),
                .entry_type = reader.entryType(),
            });

            pos += zbuf.Frame.hdr_size + payload.len;
        }
        return .{
            .fid = fid,
            .handle = handle,
            .allocator = alloc,
            .slots = slots_list,
            .write_offset = log_file_offset + pos,
            .is_current = true,
        };
    }

    /// Open the current (highest fid) log file: its entries come from
    /// `scanFrames`, whatever the header says.
    pub fn openCurrent(
        vfs_inst: *const _vfs.VFS,
        alloc: std.mem.Allocator,
        dir_path: String,
        fid: u64,
        opts: OpenOptions,
    ) !LogFile {
        std.debug.assert(opts.prezero != .auto);
        var name_buf: LogFile.NameBuf = undefined;
        const path = try fullPath(alloc, dir_path, try fname(fid, &name_buf));
        defer alloc.free(path);

        var handle = try vfs_inst.openFile(alloc, path, .{
            .mode = .default,
            .flags = .{ .writable = true, .dsync = true },
        });
        errdefer handle.close();

        // If the file is smaller than the header region, it was partially created
        // (crash during createCurrent). Re-create it.
        if (handle.file_size < log_file_offset) {
            handle.close();
            return createCurrent(vfs_inst, alloc, dir_path, fid, opts);
        }

        var lf = try scanFrames(fid, handle, alloc);
        // A log file from before pre-zeroing, or one whose preparation
        // was cut short: bring it to size so appends never grow it.
        if (lf.handle.file_size < opts.log_file_size and opts.prezero == .true) {
            try lf.zeroFill(lf.handle.file_size, opts.log_file_size);
            try lf.handle.syncData();
        }
        lf.filled = opts.prezero == .true;
        return lf;
    }

    /// Create a new <fid>.wal file with zeroed header region. The empty
    /// header parses cleanly via openRotated; the wal layer derives the
    /// first append's index from the previous file's lastEntry.
    pub fn createCurrent(
        vfs_inst: *const _vfs.VFS,
        alloc: std.mem.Allocator,
        dir_path: String,
        fid: u64,
        opts: OpenOptions,
    ) !LogFile {
        var name_buf: LogFile.NameBuf = undefined;
        return createNamed(vfs_inst, alloc, dir_path, try fname(fid, &name_buf), fid, opts);
    }

    /// `createCurrent` under a caller-chosen name: the log file prepared
    /// ahead of rotation is created as <fid>.wal.tmp (see `wal.rotate`).
    pub fn createNamed(
        vfs_inst: *const _vfs.VFS,
        alloc: std.mem.Allocator,
        dir_path: String,
        name: String,
        fid: u64,
        opts: OpenOptions,
    ) !LogFile {
        std.debug.assert(opts.prezero != .auto);
        const path = try fullPath(alloc, dir_path, name);
        defer alloc.free(path);

        // Log files are O_DSYNC: every pwrite returns durable, so a batch
        // costs one write and no fdatasync (and no fsops pool trip). That
        // is only as cheap as fdatasync (1.7 ms per batch on the bench
        // NVMe) because the log file is zero-filled to its full size when
        // it is created, so no write ever grows it; a write that extends
        // a file journals the size and costs 6 ms, O_DSYNC or not.
        const handle = try vfs_inst.createFile(path, .{
            .mode = .default,
            .flags = .{ .writable = true, .truncate = true, .dsync = true },
        });

        // Build the LogFile up front so a single deinit covers both the
        // handle and the (initially empty) slots list. Struct construction
        // is infallible, so no error path runs between this line and the
        // errdefer below — there's no window where ownership is in limbo.
        var lf = LogFile{
            .fid = fid,
            .handle = handle,
            .allocator = alloc,
            .slots = std.ArrayList(entry).empty,
            .write_offset = log_file_offset,
            .is_current = true,
            .filled = opts.prezero == .true,
        };
        errdefer lf.deinit();

        // Initialize the header region with a valid empty WalHeader (no
        // data slots). If the process crashes before the first append,
        // this file is reopened via openCurrent (which scans the data
        // region and ignores the header). If it later ends up read by
        // openRotated, the empty WalHeader still parses cleanly.
        try lf.writeHeader();
        if (opts.prezero == .true) {
            try lf.zeroFill(log_file_offset, opts.log_file_size);
        }
        try lf.handle.syncData();
        return lf;
    }

    /// Write zeros from `from` to `size`, in pieces, at the lowest I/O
    /// priority.
    fn zeroFill(self: *LogFile, from: u64, size: u64) !void {
        var at: u64 = from;
        while (at < size) {
            const n: usize = @intCast(@min(zero_piece, size - at));
            try self.handle.write(zero_block[0..n], at, .p2);
            at += n;
        }
    }
    pub fn deinit(self: *LogFile) void {
        self.slots.deinit(self.allocator);
        self.handle.close();
    }

    pub fn truncateFile(self: *LogFile, size: u64) !void {
        try self.handle.truncate(size);
    }

    pub fn numEntries(self: *const LogFile) usize {
        return self.slots.items.len;
    }

    // --- In-memory slot access (no disk I/O) ---

    pub fn getEntry(self: *const LogFile, slot_index: usize) ?entry {
        if (slot_index >= self.slots.items.len) return null;
        return self.slots.items[slot_index];
    }

    /// First raft index this file holds, or null if the file has no
    /// entries (only the current file can be empty).
    pub fn firstIndex(self: *const LogFile) ?u64 {
        if (self.slots.items.len == 0) return null;
        return self.slots.items[0].index;
    }

    pub fn firstEmptySlot(self: *const LogFile) usize {
        return self.slots.items.len;
    }

    pub fn lastEntry(self: *const LogFile) ?entry {
        if (self.slots.items.len == 0) return null;
        return self.slots.items[self.slots.items.len - 1];
    }

    /// Returns slot position for a raft index.
    /// null = before file range (or file is empty);
    /// no_slot = beyond file range.
    pub fn slotGe(self: *const LogFile, raft_index: u64) ?usize {
        const fi = self.firstIndex() orelse return null;
        if (raft_index < fi) return null;
        const slot: usize = @intCast(raft_index - fi);
        if (slot >= self.slots.items.len) return no_slot;
        std.debug.assert(self.slots.items[slot].index == raft_index);
        return slot;
    }

    /// Truncate slots and file from slot_index onward.
    pub fn truncateFromSlot(self: *LogFile, from_slot: usize) void {
        if (from_slot == 0) {
            self.slots.shrinkRetainingCapacity(0);
            self.write_offset = log_file_offset;
        } else if (from_slot < self.slots.items.len) {
            self.write_offset = self.slots.items[from_slot].data_offset;
            self.slots.shrinkRetainingCapacity(from_slot);
        } else {
            return;
        }
        self.truncateFile(self.write_offset) catch {};
    }

    // --- Disk I/O ---

    /// Append a framed record to a write buffer (no I/O). Updates in-memory slots.
    pub fn appendRecordToBuffer(
        self: *LogFile,
        buf: *std.ArrayList(u8),
        entry_bytes: String,
        reader: zen.LogEntryReader,
    ) !void {
        const sz = try zbuf.Frame.append(buf, self.allocator, entry_bytes);

        // Update in-memory slot table.
        try self.slots.append(self.allocator, .{
            .term = reader.term(),
            .index = reader.index(),
            .timestamp = reader.timestamp(),
            .data_offset = @intCast(self.write_offset),
            .data_len = @intCast(entry_bytes.len),
            .entry_type = reader.entryType(),
        });
        self.write_offset += sz;
    }

    /// Flush a write buffer to disk in one call.
    pub fn flushWriteBuffer(self: *LogFile, buf: String) !void {
        if (buf.len == 0) return;
        try self.handle.write(buf, self.write_offset - buf.len, .p0);
    }

    /// Extract and verify a single entry's payload from a pre-read buffer.
    /// `buf` starts at `buf_offset` in the file. Returns a slice into `buf` (no copy).
    pub fn readPayloadFromBuf(
        self: *const LogFile,
        slot_index: usize,
        buf: String,
        buf_offset: u64,
    ) !String {
        const ent = self.getEntry(slot_index) orelse return error.InvalidLogOffset;

        const rec_start = ent.data_offset - @as(u32, @intCast(buf_offset));
        const rec_end = rec_start + zbuf.Frame.hdr_size + ent.data_len;
        if (rec_end > buf.len) return error.InvalidLogOffset;

        // Frame.read validates the hash over the payload bytes; mismatched
        // bytes (e.g. partial torn write captured in our pre-read) surface
        // here as zbuf.Error.Invalid instead of silently returning bad data.
        return zbuf.Frame.read(buf[rec_start..rec_end]) catch error.CrcMismatch;
    }

    /// Build the WAL header as a single WalHeader zbuf message and write
    /// it in one syscall. `slots` stores every data entry in arrival
    /// order; the per-file `first_index` is implicit in slots[0].index for
    /// non-empty files (and discoverable by walking the chain across files
    /// for empty ones), so it isn't persisted. The encoded message must
    /// fit within `log_file_offset`; only the frame is written, the rest
    /// of the region stays as it is (zeros from the fill, or a hole). The
    /// zbuf message tail (table_id + root offset) lets the reader validate
    /// the format with `WalHeaderReader.parseRoot`, replacing the legacy
    /// magic/version check.
    pub fn writeHeader(self: *LogFile) !void {
        const hdr_region = try self.allocator.alloc(u8, log_file_offset);
        defer self.allocator.free(hdr_region);

        var msg_buf = std.ArrayList(u8).initBuffer(hdr_region[zbuf.Frame.hdr_size..]);
        var hdr_writer = try zen.WalHeaderWriter.initWithBuf(self.allocator, &msg_buf);

        // Each data entry becomes a WalSlot child; collect their offsets
        // for setSlots, which writes them as the array index.
        if (self.slots.items.len > 0) {
            const offsets = try self.allocator.alloc(u32, self.slots.items.len);
            defer self.allocator.free(offsets);
            for (self.slots.items, 0..) |ent, i| {
                offsets[i] = try ent.marshalTo(self.allocator, &msg_buf);
            }
            try hdr_writer.setSlots(offsets);
        }

        _ = try hdr_writer.finishWithTail();

        // The whole encoded header plus the 8-byte frame prefix must fit
        // in the reserved region; otherwise we'd corrupt the data region.
        if (zbuf.Frame.hdr_size + msg_buf.items.len > log_file_offset) {
            return error.InvalidLogHeader;
        }

        // Wrap the zbuf message in a [hash | len] frame so the read path
        // can find its end and verify it.
        try zbuf.Frame.write(hdr_region, msg_buf.items);

        const frame_len = @as(usize, zbuf.Frame.hdr_size) + msg_buf.items.len;
        try self.handle.write(hdr_region[0..frame_len], 0, .p0);
    }
};

/// The log file after `current`. Once `current` is half full, `prepare`
/// spawns a fiber that creates and zero-fills it under a temporary name
/// (<fid>.wal.tmp, which `openPath` ignores and removes, so a restart
/// never takes it for the current log file while the real one, whose
/// header is written only when it rotates, sits below it with an empty
/// header). `swapIn` renames the prepared file into place at rotation,
/// under a fid taken then, so file order is time order whichever file
/// it hands back; a prepare still running does not delay the rotation
/// (see swapIn). Every method runs with the wal lock held; the fiber
/// itself only hands back its result through the join handle.
pub const NextLog = struct {
    allocator: std.mem.Allocator,
    vfs_inst: *const _vfs.VFS,
    dir_path: String,
    opts: OpenOptions,
    /// Every log file fid comes from here, prepared or created inline.
    next_fid: u64,
    /// The create fiber, from `prepare` until `swapIn` or `discard`
    /// joins it. A spawned fiber is freed by its join, so exactly once.
    creating: ?fiber.JoinHandle(anyerror!LogFile) = null,

    /// Start creating the next log file, unless one is being created or
    /// is ready. With prezero off there is nothing to do ahead: the
    /// file is then a header write and one dsync, the same cost inline
    /// at rotation. A spawn failure is logged and skipped: `swapIn`
    /// then creates inline, unfilled.
    pub fn prepare(self: *NextLog) void {
        if (self.opts.prezero == .false) return;
        if (self.creating != null) return;
        const fid = self.takeFid();
        self.creating = fiber.spawn(
            .{ .lane = .raft },
            createAndFill,
            .{ self, fid },
        ) catch |err| {
            std.log.warn("raft/diskwal: no fiber to prepare log file {d}: {s}", .{
                fid, @errorName(err),
            });
            return;
        };
    }

    fn createAndFill(self: *NextLog, fid: u64) anyerror!LogFile {
        std.debug.assert(self.opts.prezero == .true);
        var name_buf: LogFile.NameBuf = undefined;
        return LogFile.createNamed(
            self.vfs_inst,
            self.allocator,
            self.dir_path,
            try LogFile.tmpName(fid, &name_buf),
            fid,
            self.opts,
        );
    }

    /// Whether a prepared log file is waiting for rotation.
    pub fn isReady(self: *const NextLog) bool {
        const h = self.creating orelse return false;
        return h.fiber.isFinished();
    }

    /// The next current log file, at once. The prepared one, renamed
    /// into place under a fresh fid, when its fill is done; otherwise a
    /// file created here, never zero-filled: rotation runs under the
    /// wal lock, and a fill here (log_file_size at the lowest I/O priority,
    /// starved whenever compaction is writing) would stall every commit
    /// in flight. An unfilled file's appends extend it, a few ms of
    /// journal work each, bounded by the file's life: the wal rotates
    /// again as soon as a prepared file is ready. A prepare still
    /// running is left running for that rotation.
    pub fn swapIn(self: *NextLog) !LogFile {
        var inline_opts = self.opts;
        inline_opts.prezero = .false;
        if (self.creating) |*handle| {
            if (!handle.fiber.isFinished()) {
                std.log.warn(
                    "raft/diskwal: next log file not ready at rotation; " ++
                        "using an unfilled one",
                    .{},
                );
                return LogFile.createCurrent(
                    self.vfs_inst,
                    self.allocator,
                    self.dir_path,
                    self.takeFid(),
                    inline_opts,
                );
            }

            // Zero-filled file is ready. Use it.
            defer self.creating = null;
            if (handle.join()) |lf| {
                var prepared = lf;
                const tmp_fid = prepared.fid;
                // The tmp fid is still the next one unless a rotation
                // went by meanwhile; then a fresh fid keeps file order
                // equal to time order.
                const fid = if (tmp_fid + 1 == self.next_fid)
                    tmp_fid
                else
                    self.takeFid();
                self.rename(tmp_fid, fid) catch |err| {
                    prepared.deinit();
                    deleteTmpByFid(self.allocator, self.dir_path, tmp_fid);
                    return err;
                };
                prepared.fid = fid;
                return prepared;
            } else |err| {
                std.log.err("raft/diskwal: prepared log file failed: {s}; creating inline", .{
                    @errorName(err),
                });
            }
        }
        return LogFile.createCurrent(
            self.vfs_inst,
            self.allocator,
            self.dir_path,
            self.takeFid(),
            inline_opts,
        );
    }

    /// Join a creation in flight and delete what it made.
    pub fn discard(self: *NextLog) void {
        if (self.creating) |*handle| {
            defer self.creating = null;
            var lf = handle.join() catch return;
            deleteTmpByFid(self.allocator, self.dir_path, lf.fid);
            lf.deinit();
        }
    }

    fn takeFid(self: *NextLog) u64 {
        const fid = self.next_fid;
        self.next_fid += 1;
        return fid;
    }

    /// <tmp_fid>.wal.tmp becomes <fid>.wal, and the name is made durable
    /// before anything is appended under it. The two differ when a
    /// rotation went by while the file was still being prepared.
    fn rename(self: *NextLog, tmp_fid: u64, fid: u64) !void {
        var tmp_buf: LogFile.NameBuf = undefined;
        const tmp_path = try LogFile.fullPath(
            self.allocator,
            self.dir_path,
            try LogFile.tmpName(tmp_fid, &tmp_buf),
        );
        defer self.allocator.free(tmp_path);

        var name_buf: LogFile.NameBuf = undefined;
        const path = try LogFile.fullPath(self.allocator, self.dir_path, try LogFile.fname(fid, &name_buf));
        defer self.allocator.free(path);

        try _vfs.renameFile(tmp_path, path);
        _vfs.syncDir(self.dir_path);
    }
};

/// The batch write path. Records are framed into `write_buf` under the
/// wal lock by `append`, which also wakes the writer fiber; the writer
/// swaps the buffer out as `sync_buf` and writes it with the lock
/// released, so appends keep landing while the disk is busy. It is the
/// only fiber that writes batches: everyone else waits on `synced`,
/// the highest log index on disk, to reach what they appended.
pub const LogBuf = struct {
    allocator: std.mem.Allocator,
    /// Buffered framed records for the current file. `addEntries`
    /// appends here without touching disk.
    write_buf: std.ArrayList(u8) = .empty,
    /// The batch on its way to disk. Private to the writer between
    /// `take` and the end of `write`; non-empty after a failed write
    /// until the next `take` or `flush` puts it back in front of
    /// `write_buf`.
    sync_buf: std.ArrayList(u8) = .empty,
    /// Running from `take` until the write returns. Anything that
    /// writes, moves or truncates log files under the lock waits it out
    /// (`waitInflight`); the writer marks it done without the lock.
    inflight: fiber.Event = .{},
    /// Highest log index whose framed bytes are on disk: the leader's
    /// own vote toward commit, what `addEntries` consults before
    /// letting the cache evict (an entry above it is flushed first, so
    /// every entry is in cache OR on disk), and what `sync` waits on.
    /// Read without the lock; advanced by the writer after its write,
    /// reset under the lock when a truncation cuts the log below it.
    synced: fiber.Watermark,
    /// Highest log index framed into `write_buf`, advanced by the
    /// caller once per batch of appends (`advance`): the writer parks
    /// until it is past `synced`, i.e. there is something to write.
    /// Reset with `synced` on truncation, so the pair keeps meaning
    /// "pending or in flight" rather than a stale high-water mark.
    buffered: fiber.Watermark,
    /// The writer fiber, from `start` until `stop` joins it.
    writer: ?fiber.JoinHandle(void) = null,
    stopping: std.atomic.Value(bool) = .init(false),

    /// A swapped-out batch: what `write` needs with the lock released.
    /// `handle` is a copy, so a log file list that moves meanwhile
    /// cannot dangle it (and nothing closes the current log file without
    /// `waitInflight`).
    pub const Batch = struct {
        handle: _vfs.FileHandle,
        offset: u64,
        bytes: String,
        first_index: u64,
        last_index: u64,
    };

    pub fn init(allocator: std.mem.Allocator) LogBuf {
        return .{
            .allocator = allocator,
            .synced = fiber.Watermark.init(allocator),
            .buffered = fiber.Watermark.init(allocator),
        };
    }

    pub fn deinit(self: *LogBuf) void {
        self.stop();
        self.write_buf.deinit(self.allocator);
        self.sync_buf.deinit(self.allocator);
        self.synced.deinit();
        self.buffered.deinit();
    }

    /// Spawn the writer fiber, once. `dw` must be at its final address:
    /// the fiber takes its lock and reads its wal for the life of the
    /// DiskWAL.
    pub fn start(self: *LogBuf, dw: *DiskWAL) !void {
        if (self.writer != null) return;
        self.writer = try fiber.spawn(.{ .lane = .raft }, writerLoop, .{ self, dw });
    }

    /// Wake every waiter with error.Closed, the writer included, and
    /// join the writer. Before the log files are closed.
    pub fn stop(self: *LogBuf) void {
        self.stopping.store(true, .release);
        self.buffered.close();
        self.synced.close();
        if (self.writer) |*h| {
            defer self.writer = null;
            h.join();
        }
    }

    /// Frame a record into `write_buf` (the log file's slot list and
    /// write offset advance with it). Does not wake the writer: the
    /// caller `advance`s once after its batch, so a batch of appends
    /// is one write, not several. Lock held.
    pub fn append(self: *LogBuf, current: *LogFile, entry_bytes: String, reader: zen.LogEntryReader) !void {
        try current.appendRecordToBuffer(&self.write_buf, entry_bytes, reader);
    }

    /// Everything through `index` is in `write_buf`: wakes the writer.
    /// Lock held.
    pub fn advance(self: *LogBuf, index: u64) void {
        self.buffered.advance(index);
    }

    /// Park until `index` is on disk. `error.Reset`: the synced index
    /// was lowered meanwhile (a truncation); the caller re-checks what
    /// it waits for.
    pub fn waitSynced(self: *LogBuf, index: u64) error{ Closed, OutOfMemory, Reset }!void {
        return self.synced.waitForOrReset(index);
    }

    pub fn isInflight(self: *LogBuf) bool {
        return self.inflight.isRunning();
    }

    /// Park until no batch is in flight.
    pub fn waitInflight(self: *LogBuf) void {
        self.inflight.waitForDone();
    }

    pub fn syncedIndex(self: *const LogBuf) u64 {
        return self.synced.load();
    }

    /// For the log shrinking under it (truncation) or recovery finding
    /// everything on disk. Lock held.
    pub fn setSynced(self: *LogBuf, index: u64) void {
        self.synced.reset(index);
        self.buffered.reset(index);
    }

    /// Make everything appended so far durable, on the caller's fiber,
    /// with the lock held: what rotation, truncation and cache eviction
    /// need before they touch the log files.
    pub fn flush(self: *LogBuf, current: *LogFile, last_index: u64) !void {
        self.waitInflight();
        self.reclaimFailed();
        if (self.write_buf.items.len == 0) return;

        try current.flushWriteBuffer(self.write_buf.items);
        utils.clearRetainCap(&self.write_buf);
        self.synced.advance(last_index);
    }

    /// Wait out the write in flight, drop what is buffered, nothing
    /// durable. For `wal.reset`.
    pub fn reset(self: *LogBuf) void {
        self.waitInflight();
        utils.clearRetainCap(&self.sync_buf);
        utils.clearRetainCap(&self.write_buf);
        self.synced.reset(0);
        self.buffered.reset(0);
    }

    /// The writer: drains everything pending one batch at a time, then
    /// parks until `buffered` moves past `synced`. A truncation resets
    /// both, which wakes it to recompute the target rather than wait
    /// for an index that may never come back. Exits when `stop` closes
    /// `buffered`.
    fn writerLoop(self: *LogBuf, dw: *DiskWAL) void {
        while (true) {
            while (self.writeOne(dw)) {}
            if (self.stopping.load(.acquire)) return;
            self.buffered.waitForOrReset(self.synced.load() + 1) catch |err| switch (err) {
                error.Reset => continue,
                error.Closed, error.OutOfMemory => return,
            };
        }
    }

    /// One batch: take under the lock, write with it released. False
    /// when nothing was pending. A failed write is logged and retried
    /// after a pause: the bytes stay in front, and nothing is durable
    /// until they land.
    fn writeOne(self: *LogBuf, dw: *DiskWAL) bool {
        dw.mu.lock(@src());
        const ws = &dw.wal_store;
        const batch = self.take(ws.current, ws.lastIndex());
        dw.mu.unlock();

        const b = batch orelse return false;
        const started = fiber.timestampNow(.monotonic, .micros);
        self.write(b) catch |err| {
            std.log.err("raft/diskwal: batch write through index {d}: {s}", .{
                b.last_index, @errorName(err),
            });
            if (self.stopping.load(.acquire)) return false;
            fiber.sleepMs(100);
            return true;
        };
        const done = fiber.timestampNow(.monotonic, .micros);
        if (dw.on_written) |hook| hook.call(hook.ctx, .{
            .first_index = b.first_index,
            .last_index = b.last_index,
            .started_us = started,
            .done_us = done,
        });
        return true;
    }

    /// Swap the pending records out for the writer. Lock held; nothing
    /// in flight, since only the writer takes. Null when nothing is
    /// pending.
    fn take(self: *LogBuf, current: *const LogFile, last_index: u64) ?Batch {
        std.debug.assert(!self.isInflight());

        self.reclaimFailed();
        if (self.write_buf.items.len == 0) return null;

        std.mem.swap(std.ArrayList(u8), &self.write_buf, &self.sync_buf);
        self.inflight.setRunning();
        return .{
            .handle = current.handle,
            .offset = current.write_offset - self.sync_buf.items.len,
            .bytes = self.sync_buf.items,
            .first_index = self.synced.load() + 1,
            .last_index = last_index,
        };
    }

    /// Write a taken batch with the lock released; the log file is
    /// O_DSYNC, so the write returns durable. On error the bytes stay
    /// in `sync_buf` for `reclaimFailed`.
    fn write(self: *LogBuf, b: Batch) !void {
        defer self.inflight.setDone();
        try b.handle.write(b.bytes, b.offset, .p0);

        utils.clearRetainCap(&self.sync_buf);
        self.synced.advance(b.last_index);
    }

    /// A failed write left its bytes in `sync_buf`; they precede
    /// whatever was appended since, so they go back in front.
    fn reclaimFailed(self: *LogBuf) void {
        if (self.sync_buf.items.len == 0) return;
        self.write_buf.insertSlice(
            self.allocator,
            0,
            self.sync_buf.items,
        ) catch |err| {
            std.debug.panic("wal: cannot reclaim a failed batch: {s}", .{
                @errorName(err),
            });
        };
        utils.clearRetainCap(&self.sync_buf);
    }
};

/// Delete `<dir_path>/<name>`, errors ignored.
pub fn deleteFileByName(alloc: std.mem.Allocator, dir_path: String, name: String) void {
    const path = LogFile.fullPath(alloc, dir_path, name) catch return;
    defer alloc.free(path);
    _vfs.deleteFile(path) catch {};
}

/// Delete the temporary file of a log file prepared ahead of rotation.
pub fn deleteTmpByFid(alloc: std.mem.Allocator, dir_path: String, fid: u64) void {
    var name_buf: LogFile.NameBuf = undefined;
    const name = LogFile.tmpName(fid, &name_buf) catch return;
    deleteFileByName(alloc, dir_path, name);
}
