//! FileStore — the S3-equivalent object store that distributes immutable
//! files (SSTs) across nodes. The creator uploads an object BEFORE
//! proposing the manifest change that references it, so store durability
//! precedes consensus; followers download at apply time. Objects are keyed
//! by name (callers own the naming — e.g. the SST filename), write-once,
//! and single-writer by construction, which is what lets the `dir` backend
//! share a plain directory between nodes without any locking.
//!
//! Reads go through an `Object` handle (open once, read ranges) so block
//! consumers — today whole-object `getAll`, later tiered/lazy table
//! reads — map onto ranged GETs instead of whole-file transfers.

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;
const _vfs = @import("vfs.zig");
const _s3 = @import("s3.zig");
const _lookahead = @import("lookahead.zig");

pub const S3Config = _s3.S3Config;

pub const FileStore = union(enum) {
    /// No store configured — single-node deployments. Multi-node requires
    /// a store: seeded nodes fail fast at startup and the leader rejects
    /// joins without one.
    none,
    dir: DirStore,
    s3: _s3.S3Store,

    pub fn initDir(a: std.mem.Allocator, depot: String) !FileStore {
        return .{ .dir = try DirStore.init(a, depot) };
    }

    pub fn initS3(a: std.mem.Allocator, cfg: S3Config) !FileStore {
        return .{ .s3 = try _s3.S3Store.init(a, cfg) };
    }

    pub fn deinit(self: *FileStore) void {
        switch (self.*) {
            .none => {},
            .dir => |*d| d.deinit(),
            .s3 => |*s| s.deinit(),
        }
    }

    /// Upload a whole object. Idempotent: re-putting an existing name
    /// rewrites identical bytes (write-once contract). `data` must be
    /// page-aligned and a page multiple — the dir backend writes
    /// O_DIRECT and rejects anything else (error.UnalignedPut). SST
    /// buffers satisfy this by construction.
    pub fn put(self: *FileStore, name: String, data: String) !void {
        switch (self.*) {
            .none => return error.NoFileStore,
            .dir => |*d| try d.put(name, data),
            .s3 => |*s| try s.put(name, data),
        }
    }

    /// Durability barrier for prior puts. put fsyncs each object's BYTES;
    /// the dir backend's rename leaves the NAME's durability to a depot
    /// directory fsync — done here, once per batch, instead of per put.
    /// s3: no-op, every PUT is durable when the response arrives. Call
    /// after a batch of puts, before proposing changes that reference them.
    pub fn sync(self: *FileStore) !void {
        switch (self.*) {
            .none => return error.NoFileStore,
            .dir => |*d| d.sync(),
            .s3 => {},
        }
    }

    pub fn exists(self: *FileStore, name: String) !bool {
        switch (self.*) {
            .none => return error.NoFileStore,
            .dir => |*d| return d.exists(name),
            .s3 => |*s| return s.exists(name),
        }
    }

    pub fn delete(self: *FileStore, name: String) !void {
        switch (self.*) {
            .none => return error.NoFileStore,
            .dir => |*d| try d.delete(name),
            .s3 => |*s| try s.delete(name),
        }
    }

    /// Open an object for ranged reads. error.FileNotFound if absent.
    pub fn open(self: *FileStore, name: String) !Object {
        switch (self.*) {
            .none => return error.NoFileStore,
            .dir => |*d| return d.open(name),
            .s3 => |*s| {
                const sz = try s.head(name);
                return .{ .s3 = .{
                    .store = s,
                    .name = try s.a.dupe(u8, name),
                    .sz = sz,
                } };
            },
        }
    }

    /// Convenience: fetch a whole object. Caller owns the bytes.
    pub fn getAll(self: *FileStore, a: std.mem.Allocator, name: String) ![]u8 {
        var obj = try self.open(name);
        defer obj.close();
        return obj.read(a, 0, obj.size());
    }

    /// Measure the ideal readahead for this store endpoint (see
    /// lookahead.calibrate): the size whose transfer time equals the
    /// per-request fixed cost — a network round trip for s3, syscall +
    /// device latency for dir.
    pub fn calibrateLookahead(self: *FileStore, block_size: u64) !u64 {
        switch (self.*) {
            .none => return 0,
            .dir => |*d| return d.calibrateLookahead(block_size),
            .s3 => |*s| return s.calibrateLookahead(block_size),
        }
    }

    /// The calibrated readahead in bytes; 0 = none needed/uncalibrated.
    pub fn lookahead(self: *const FileStore) u64 {
        switch (self.*) {
            .none => return 0,
            .dir => |*d| return d.lookahead,
            .s3 => |*s| return s.lookahead,
        }
    }
};

/// A readable object: open once, read many ranges. Mirrors the shape of
/// vfs.FileHandle so the tiered-storage stage can swap it in under Table.
pub const Object = union(enum) {
    dir: DirObject,
    s3: S3Object,

    pub fn size(self: *const Object) u64 {
        switch (self.*) {
            .dir => |*o| return o.sz,
            .s3 => |*o| return o.sz,
        }
    }

    /// Read `len` bytes at `offset`. Caller owns the returned buffer.
    pub fn read(self: *Object, a: std.mem.Allocator, offset: u64, len: u64) ![]u8 {
        switch (self.*) {
            .dir => |*o| return o.read(a, offset, len),
            .s3 => |*o| return o.store.readRange(a, o.name, offset, len),
        }
    }

    pub fn close(self: *Object) void {
        switch (self.*) {
            .dir => |*o| o.close(),
            .s3 => |*o| o.store.a.free(o.name),
        }
    }
};

/// Each read is one ranged GET; the handle is where keep-alive
/// connection reuse lands later.
const S3Object = struct {
    store: *_s3.S3Store,
    name: String,
    sz: u64,
};

/// A shared write-once blob depot on a filesystem (local dir, NFS mount).
/// NEVER a live data dir: nodes keep their own LSM dirs; only immutable
/// blobs live here. put() is tmp+rename so a reader can never observe a
/// partial object, and only the creating node ever writes a given name,
/// so tmp names cannot collide across nodes.
pub const DirStore = struct {
    a: std.mem.Allocator,
    depot: String,
    /// calibrateLookahead at startup. 0 = uncalibrated. Not consumed yet —
    /// tiered reads will size their readahead from it.
    lookahead: u64 = 0,

    pub fn init(a: std.mem.Allocator, depot: String) !DirStore {
        const tmp_dir = try std.fmt.allocPrint(a, "{s}/tmp", .{depot});
        defer a.free(tmp_dir);
        try _vfs.createDirPath(tmp_dir);
        return .{ .a = a, .depot = try a.dupe(u8, depot) };
    }

    pub fn deinit(self: *DirStore) void {
        self.a.free(self.depot);
    }

    fn objectPath(self: *DirStore, name: String) ![]u8 {
        return std.fmt.allocPrint(self.a, "{s}/{s}", .{ self.depot, name });
    }

    pub fn put(self: *DirStore, name: String, data: String) !void {
        // O_DIRECT requires it, and every legit caller already complies
        // (the SST builder pads + aligns its output; forest.readFile
        // returns page-aligned reads) — reject rather than bounce.
        if (@intFromPtr(data.ptr) % _vfs.page_size != 0 or data.len % _vfs.page_size != 0)
            return error.UnalignedPut;

        const tmp = try std.fmt.allocPrint(
            self.a,
            "{s}/tmp/{s}",
            .{ self.depot, name },
        );
        defer self.a.free(tmp);
        const final = try self.objectPath(name);
        defer self.a.free(final);
        // fsync-before-rename (inside writeAndSync): uploads must be
        // durable before the manifest change referencing them is proposed.
        // Direct I/O — depot objects are write-once distribution bytes
        // this node never reads back; they must not churn the page cache.
        // truncate — a crashed put can leave a partial tmp file;
        // re-putting must overwrite it. .p1: uploads gate proposes, the
        // same class as flush I/O. The NAME's durability (dir entry) is
        // the caller's batch-level barrier: one FileStore.sync after all
        // puts.
        try _vfs.writeAndSync(
            tmp,
            data,
            .{ .mode = .direct, .flags = .{ .truncate = true } },
            .p1,
        );
        try _vfs.renameFile(tmp, final);
    }

    fn sync(self: *DirStore) void {
        _vfs.syncDir(self.depot);
    }

    fn exists(self: *DirStore, name: String) !bool {
        const path = try self.objectPath(name);
        defer self.a.free(path);

        _vfs.access(path) catch return false;
        return true;
    }

    fn delete(self: *DirStore, name: String) !void {
        const path = try self.objectPath(name);
        defer self.a.free(path);

        try _vfs.deleteFile(path);
    }

    fn open(self: *DirStore, name: String) !Object {
        const path = try self.objectPath(name);
        defer self.a.free(path);

        const f = try fiber.Dir.cwd().openFile(path, .{});
        errdefer f.close();

        const sz = (try f.stat()).size;
        return .{ .dir = .{ .f = f, .sz = sz } };
    }

    /// Object size in bytes; errors if absent.
    pub fn head(self: *DirStore, name: String) !u64 {
        const path = try self.objectPath(name);
        defer self.a.free(path);
        return _vfs.fileSize(path);
    }

    /// Measure this depot's ideal readahead (policy in vfs/lookahead.zig;
    /// this store contributes probeRead and the object ops) and record it
    /// in `lookahead`. Meaningful locally too: the knee is device latency
    /// × device bandwidth (~100s of KB on NVMe) instead of network round
    /// trip × link bandwidth (~MBs).
    pub fn calibrateLookahead(self: *DirStore, block_size: u64) !u64 {
        self.lookahead = try _lookahead.calibrate(self.a, self, block_size);
        return self.lookahead;
    }

    /// One timed ranged read of the probe object — the disk analog of
    /// s3's timed ranged GET, and the measurement primitive behind
    /// lookahead.calibrate. Same contract as the s3 probe: total_ns =
    /// end-to-end cost of a production read of this size (one pread, its
    /// own fixed cost included), ttfb_ns = a sample of the fixed
    /// per-request cost alone. A positional read has no observable
    /// "first byte" mid-syscall, so the fixed cost is sampled as its OWN
    /// single-page pread at an unrelated offset — splitting the main
    /// read instead would charge every probe two serial device round
    /// trips that production reads never pay. Opened O_DIRECT so the
    /// page cache can't serve repeats and turn the numbers into memory
    /// bandwidth.
    pub fn probeRead(self: *DirStore, off: u64, len: u64) !_lookahead.Probe {
        const path = try self.objectPath(_lookahead.probe_object);
        defer self.a.free(path);
        // O_DIRECT needs page-aligned offset/length/buffer; align the
        // request itself — any aligned window of ~len bytes probes
        // equally well.
        const a_off = off & ~@as(u64, _vfs.page_size - 1);
        const a_len: usize = std.mem.alignForward(usize, @intCast(@max(len, _vfs.page_size)), _vfs.page_size);
        const buf = try self.a.alignedAlloc(u8, _vfs.page_alignment, a_len);
        defer self.a.free(buf);

        const f = fiber.File.fromFd(try _vfs.openDirect(path, .{ .mode = .direct }));
        defer f.close();
        const fsize = (try f.stat()).size;
        if (a_off + a_len > fsize) return error.ShortRead;

        // The fixed-cost page: hashed from the (already random) offset so
        // repeats can't sit in one device cache slot, dodged out of the
        // main window so it can't pre-warm the ranged read.
        const pages = fsize / _vfs.page_size;
        var page_off = ((off *% 0x9E3779B97F4A7C15) % pages) * _vfs.page_size;
        if (page_off >= a_off and page_off < a_off + a_len) {
            page_off = ((page_off + a_len) / _vfs.page_size % pages) * _vfs.page_size;
        }
        const t0 = _lookahead.nowNs();
        if (try f.read(buf[0.._vfs.page_size], page_off) != _vfs.page_size) return error.ShortRead;
        const fixed_ns = _lookahead.nowNs() - t0;
        if (!_lookahead.verifyProbeData(page_off, buf[0.._vfs.page_size])) return error.BadProbeData;

        const t1 = _lookahead.nowNs();
        var got: usize = 0;
        while (got < a_len) {
            const n = try f.read(buf[got..], a_off + got);
            if (n == 0) return error.ShortRead;
            got += n;
        }
        const total = _lookahead.nowNs() - t1;
        if (!_lookahead.verifyProbeData(a_off, buf)) return error.BadProbeData;
        return .{ .size = a_len, .ttfb_ns = fixed_ns, .total_ns = total };
    }
};

const DirObject = struct {
    f: fiber.File,
    sz: u64,

    fn read(self: *DirObject, a: std.mem.Allocator, offset: u64, len: u64) ![]u8 {
        const buf = try a.alloc(u8, @intCast(len));
        errdefer a.free(buf);

        var off: u64 = 0;
        while (off < len) {
            const n = try self.f.read(buf[@intCast(off)..], offset + off);
            if (n == 0) return error.UnexpectedEof;
            off += n;
        }
        return buf;
    }

    fn close(self: *DirObject) void {
        self.f.close();
    }
};
