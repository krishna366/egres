const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const zio = @import("zio");
const bandwidth = @import("bandwidth.zig");

pub const Flags = struct {
    /// Open for reading and writing instead of read-only.
    writable: bool = false,
    /// Truncate an existing file instead of failing with O_EXCL.
    truncate: bool = false,
};

pub const FileOptions = struct {
    mode: Mode = .default,
    flags: Flags = .{},
};

pub const page_size: usize = 4096;
pub const page_alignment = std.mem.Alignment.fromByteUnits(page_size);

pub const Mode = enum {
    default, // default via filesystem
    direct, // O_DIRECT + zio reads/writes
    mmap, // mmap the file, reads are slices into the mapped region
    memory, // read entire file into owned buffer
};

pub const Priority = enum(u2) {
    p0, // highest — Any user facing real-time reads
    p1, // medium  — WAL and L0 compaction RW
    p2, // lowest  — L1+ compaction RW
};

pub const IOKind = enum(u1) {
    read,
    write,
};

pub const ReadResult = struct {
    buf: ?[]align(page_size) u8,
    data: []const u8,
    allocator: std.mem.Allocator,

    pub fn free(self: ReadResult) void {
        if (self.buf) |b| self.allocator.free(b);
    }
};

pub fn fileIdFromPath(path: []const u8) u44 {
    const basename = std.fs.path.basename(path);
    if (std.mem.endsWith(u8, basename, ".sst")) {
        const stem = basename[0 .. basename.len - 4];
        return std.fmt.parseInt(u44, stem, 10) catch 0;
    }
    return 0; // non-SST: no caching
}

pub const FileHandle = struct {
    file: ?zio.File = null,
    file_size: u64,
    file_id: u64 = 0,
    mapped: ?[]align(page_size) const u8 = null,
    owned_buf: ?[]u8 = null, // memory mode: entire file read into this buffer
    buf_allocator: ?std.mem.Allocator = null, // for freeing owned_buf on close
    vfs: *const VFS,

    pub fn readAligned(
        self: FileHandle,
        allocator: std.mem.Allocator,
        offset: u64,
        len: usize,
        priority: Priority,
    ) !ReadResult {
        // In-memory: slice from owned buffer (no allocation, no bandwidth).
        if (self.owned_buf) |buf| {
            return .{ .buf = null, .data = buf[offset .. offset + len], .allocator = allocator };
        }

        // Round offset down to page boundary.
        const aligned_offset = offset & ~@as(u64, page_size - 1);
        const prefix: usize = @intCast(offset - aligned_offset);

        // Round total size up to page boundary.
        const aligned_size = std.mem.alignForward(usize, prefix + len, page_size);

        if (self.mapped) |mapped| {
            // MMAP: slice from mapped memory. Fault in pages under bandwidth
            // control so mmap I/O respects the same rate limits as pread.
            var total_read: usize = 0;
            while (total_read < aligned_size) {
                const remaining = aligned_size - total_read;
                const read_len = if (self.vfs.bw) |bw| bw.acquireAligned(priority, remaining, .read) else remaining;

                // Touch one byte per page to trigger page faults.
                // XOR into a sink so we need only one doNotOptimizeAway per chunk.
                var sink: u8 = 0;
                var pos: usize = total_read;
                while (pos < total_read + read_len) : (pos += page_size) {
                    sink ^= mapped[aligned_offset + pos];
                }
                std.mem.doNotOptimizeAway(sink);
                total_read += read_len;
            }
            return .{ .buf = null, .data = mapped[offset .. offset + len], .allocator = allocator };
        }

        // Allocate page-aligned buffer for O_DIRECT compatibility.
        const buf = try allocator.alignedAlloc(u8, page_alignment, aligned_size);
        errdefer allocator.free(buf);

        const f = self.file.?;
        var total_read: usize = 0;
        while (total_read < aligned_size) {
            const remaining = aligned_size - total_read;
            const read_len = if (self.vfs.bw) |bw| bw.acquireAligned(priority, remaining, .read) else remaining;
            const n = try f.read(buf[total_read .. total_read + read_len], aligned_offset + total_read);
            if (n == 0) break; // EOF
            total_read += n;
        }

        // It's possible that buf was set up for a bigger read, but the file ended before that.
        // In that case, the end offset could be total_read.
        const end = @min(prefix + len, total_read);
        return .{
            .buf = buf,
            .data = buf[prefix..end],
            .allocator = allocator,
        };
    }

    const default_write_chunk: usize = 256 * page_size; // 1 MB

    pub fn write(
        self: FileHandle,
        data: []const u8,
        offset: u64,
        priority: Priority,
    ) !void {
        const f = self.file.?;
        var written: usize = 0;
        while (written < data.len) {
            const remaining = data.len - written;
            const chunk_len = if (self.vfs.bw) |bw|
                bw.acquireAligned(priority, @min(remaining, default_write_chunk), .write)
            else
                @min(remaining, default_write_chunk);

            var chunk_written: usize = 0;
            while (chunk_written < chunk_len) {
                const n = try f.write(data[written + chunk_written .. written + chunk_len], offset + written + chunk_written);
                if (n == 0) return error.WriteFailed;
                chunk_written += n;
            }
            written += chunk_len;
        }
    }

    pub fn sync(self: FileHandle) !void {
        if (self.file) |f| f.sync(.{}) catch {};
    }

    pub fn truncate(self: FileHandle, size: u64) !void {
        try self.file.?.setSize(size);
    }

    pub fn close(self: FileHandle) void {
        if (self.owned_buf) |b| {
            if (self.buf_allocator) |a| a.free(b);
            return; // memory-backed: no file or mapping to release
        }
        if (self.mapped) |m| posix.munmap(m);
        if (self.file) |f| f.close();
    }
};

/// Module-level singleton. Configure via vfs.instance before opening tables.
pub var instance: VFS = .{ .bw = null };

pub const VFS = struct {
    bw: ?*bandwidth.Limiter,

    pub const init: VFS = .{ .bw = null };

    pub fn openFile(self: *const VFS, allocator: std.mem.Allocator, path: []const u8, opts: FileOptions) !FileHandle {
        const fid = fileIdFromPath(path);
        switch (opts.mode) {
            .mmap => {
                const fd = try openDirect(path, false, false);
                errdefer posix.close(fd);

                const file_size = try (std.fs.File{ .handle = fd }).getEndPos();
                const mapped = try posix.mmap(null, file_size, posix.PROT.READ, .{ .TYPE = .PRIVATE }, fd, 0);
                return .{ .file = zio.File.fromFd(fd), .file_size = file_size, .file_id = fid, .mapped = mapped, .vfs = self };
            },
            .default, .direct => {
                const fd = try openDirect(path, opts.mode == .direct, opts.flags.writable);
                errdefer posix.close(fd);

                const file_size = try (std.fs.File{ .handle = fd }).getEndPos();
                return .{ .file = zio.File.fromFd(fd), .file_size = file_size, .file_id = fid, .vfs = self };
            },
            .memory => {
                const fd = try openDirect(path, false, false);
                defer posix.close(fd); // closed after reading; FileHandle won't own it

                const file = std.fs.File{ .handle = fd };
                const file_size = try file.getEndPos();
                const size: usize = @intCast(file_size);
                const buf = try allocator.alloc(u8, size);
                errdefer allocator.free(buf);
                _ = try file.readAll(buf);
                return .{
                    .file_size = file_size,
                    .file_id = fid,
                    .owned_buf = buf,
                    .buf_allocator = allocator,
                    .vfs = self,
                };
            },
        }
    }

    pub fn createFile(self: *const VFS, path: []const u8, opts: FileOptions) !FileHandle {
        if (opts.mode == .mmap or opts.mode == .memory) return error.InvalidMode;

        var open_flags: posix.O = .{ .ACCMODE = .WRONLY, .CREAT = true };
        if (opts.flags.truncate) {
            open_flags.TRUNC = true;
        } else {
            open_flags.EXCL = true;
        }
        if (builtin.os.tag == .linux and opts.mode == .direct) open_flags.DIRECT = true;

        const fd = posix.open(path, open_flags, 0o644) catch |err| switch (err) {
            // tmpfs doesn't support O_DIRECT — fall back to plain open.
            error.BadPathName => return self.createFile(path, .{ .flags = opts.flags }),
            else => return err,
        };

        if (builtin.os.tag == .macos and opts.mode == .direct) {
            _ = posix.fcntl(fd, 48, 1) catch {};
        }

        return .{ .file = zio.File.fromFd(fd), .file_size = 0, .vfs = self };
    }

    pub fn renameFile(old_path: []const u8, new_path: []const u8) !void {
        try zio.Dir.cwd().rename(old_path, zio.Dir.cwd(), new_path);
    }

    /// Best-effort directory fsync so newly created filenames are durable.
    /// Silently ignores errors (tmpfs, some network mounts don't support dir fsync).
    pub fn syncDir(path: []const u8) void {
        const fd = posix.open(path, .{ .ACCMODE = .RDONLY, .DIRECTORY = true }, 0) catch return;
        const f = zio.File.fromFd(fd);
        defer f.close();
        f.sync(.{}) catch {};
    }

    pub fn deleteFile(path: []const u8) !void {
        try zio.Dir.cwd().deleteFile(path);
    }
};

/// When direct_io is set, open read-only with O_DIRECT on Linux. Falls back to plain open if the
/// filesystem doesn't support it (e.g. tmpfs returns EINVAL → BadPathName).
fn openDirect(path: []const u8, direct_io: bool, writable: bool) !posix.fd_t {
    const accmode: posix.ACCMODE = if (writable) .RDWR else .RDONLY;
    var flags: posix.O = .{ .ACCMODE = accmode };
    if (builtin.os.tag == .linux and direct_io) flags.DIRECT = true;

    const fd = posix.open(path, flags, 0) catch |err| switch (err) {
        // EINVAL is mapped to error.BadPathName
        error.BadPathName => return posix.open(path, .{ .ACCMODE = accmode }, 0),
        else => return err,
    };

    if (builtin.os.tag == .macos and direct_io) {
        // F_NOCACHE = 48 on macOS — bypass the unified buffer cache.
        _ = posix.fcntl(fd, 48, 1) catch {};
    }

    return fd;
}
