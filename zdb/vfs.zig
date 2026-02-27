const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const zio = @import("zio");
const bandwidth = @import("bandwidth.zig");

pub const page_size: usize = 4096;
pub const page_alignment = std.mem.Alignment.fromByteUnits(page_size);

pub const Mode = enum {
    direct, // O_DIRECT + zio reads/writes
    mmap, // mmap the file, reads are slices into the mapped region
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

pub const FileHandle = struct {
    fd: posix.fd_t,
    file_size: u64,
    mapped: ?[]align(page_size) const u8 = null,
};

pub const ReadResult = struct {
    buf: ?[]align(page_size) u8,
    data: []const u8,
    allocator: std.mem.Allocator,

    pub fn free(self: ReadResult) void {
        if (self.buf) |b| self.allocator.free(b);
    }
};

pub const VFS = struct {
    bw: ?*bandwidth.Limiter,
    mode: Mode,

    pub const init: VFS = .{ .bw = null, .mode = .direct };

    pub fn openFile(self: VFS, path: []const u8) !FileHandle {
        switch (self.mode) {
            .direct => {
                const fd = try openDirect(path);
                errdefer posix.close(fd);
                const file_size = try (std.fs.File{ .handle = fd }).getEndPos();
                return .{ .fd = fd, .file_size = file_size };
            },
            .mmap => {
                const fd = try posix.open(path, .{ .ACCMODE = .RDONLY }, 0);
                errdefer posix.close(fd);
                const file_size = try (std.fs.File{ .handle = fd }).getEndPos();
                const mapped = try posix.mmap(null, file_size, posix.PROT.READ, .{ .TYPE = .PRIVATE }, fd, 0);
                return .{ .fd = fd, .file_size = file_size, .mapped = mapped };
            },
        }
    }

    pub fn readAligned(
        self: VFS,
        allocator: std.mem.Allocator,
        handle: FileHandle,
        offset: u64,
        len: usize,
        priority: Priority,
    ) !ReadResult {
        // MMAP doesn't abide by the bandwidth laws for now.
        if (handle.mapped) |mapped| {
            return .{ .buf = null, .data = mapped[offset .. offset + len], .allocator = allocator };
        }

        // Round offset down to page boundary.
        const aligned_offset = offset & ~@as(u64, page_size - 1);
        const prefix: usize = @intCast(offset - aligned_offset);

        // Round total size up to page boundary.
        const aligned_size = std.mem.alignForward(usize, prefix + len, page_size);

        // Allocate page-aligned buffer for O_DIRECT compatibility.
        const buf = try allocator.alignedAlloc(u8, page_alignment, aligned_size);
        errdefer allocator.free(buf);

        const file = zio.File.fromFd(handle.fd);
        var total_read: usize = 0;
        while (total_read < aligned_size) {
            const remaining = aligned_size - total_read;
            const read_len = if (self.bw) |bw| bw.acquireAligned(priority, remaining, .read) else remaining;
            const n = try file.read(buf[total_read .. total_read + read_len], aligned_offset + total_read);
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
        self: VFS,
        handle: FileHandle,
        data: []const u8,
        offset: u64,
        priority: Priority,
    ) !void {
        const file = zio.File.fromFd(handle.fd);
        var written: usize = 0;
        while (written < data.len) {
            const remaining = data.len - written;
            const chunk_len = if (self.bw) |bw|
                bw.acquireAligned(priority, @min(remaining, default_write_chunk), .write)
            else
                @min(remaining, default_write_chunk);

            var chunk_written: usize = 0;
            while (chunk_written < chunk_len) {
                const n = try file.write(data[written + chunk_written .. written + chunk_len], offset + written + chunk_written);
                if (n == 0) return error.WriteFailed;
                chunk_written += n;
            }
            written += chunk_len;
        }
    }

    pub fn sync(handle: FileHandle) !void {
        const file = std.fs.File{ .handle = handle.fd };
        try file.sync();
    }

    pub fn close(handle: FileHandle) void {
        if (handle.mapped) |m| posix.munmap(m);
        posix.close(handle.fd);
    }

    pub fn deleteFile(path: []const u8) !void {
        try std.fs.deleteFileAbsolute(path);
    }
};

/// Open read-only with O_DIRECT on Linux. Falls back to plain open if the
/// filesystem doesn't support it (e.g. tmpfs returns EINVAL → BadPathName).
fn openDirect(path: []const u8) !posix.fd_t {
    var flags: posix.O = .{ .ACCMODE = .RDONLY };
    if (comptime builtin.os.tag == .linux) flags.DIRECT = true;

    const fd = posix.open(path, flags, 0) catch |err| switch (err) {
        // EINVAL is mapped to error.BadPathName
        error.BadPathName => return posix.open(path, .{ .ACCMODE = .RDONLY }, 0),
        else => return err,
    };

    if (comptime builtin.os.tag == .macos) {
        // F_NOCACHE = 48 on macOS — bypass the unified buffer cache.
        _ = posix.fcntl(fd, 48, 1) catch {};
    }

    return fd;
}
