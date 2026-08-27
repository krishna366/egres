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
const vfs = @import("vfs.zig");
const s3_mod = @import("s3.zig");

pub const S3Config = s3_mod.S3Config;

pub const FileStore = union(enum) {
    /// No store configured — single-node deployments. Multi-node requires
    /// a store: seeded nodes fail fast at startup and the leader rejects
    /// joins without one.
    none,
    dir: DirStore,
    s3: s3_mod.S3Store,

    pub fn initDir(a: std.mem.Allocator, depot: []const u8) !FileStore {
        return .{ .dir = try DirStore.init(a, depot) };
    }

    pub fn initS3(a: std.mem.Allocator, cfg: S3Config) !FileStore {
        return .{ .s3 = try s3_mod.S3Store.init(a, cfg) };
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
    pub fn put(self: *FileStore, name: []const u8, data: []const u8) !void {
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

    pub fn exists(self: *FileStore, name: []const u8) !bool {
        switch (self.*) {
            .none => return error.NoFileStore,
            .dir => |*d| return d.exists(name),
            .s3 => |*s| return s.exists(name),
        }
    }

    pub fn delete(self: *FileStore, name: []const u8) !void {
        switch (self.*) {
            .none => return error.NoFileStore,
            .dir => |*d| try d.delete(name),
            .s3 => |*s| try s.delete(name),
        }
    }

    /// Open an object for ranged reads. error.FileNotFound if absent.
    pub fn open(self: *FileStore, name: []const u8) !Object {
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
    pub fn getAll(self: *FileStore, a: std.mem.Allocator, name: []const u8) ![]u8 {
        var obj = try self.open(name);
        defer obj.close();
        return obj.read(a, 0, obj.size());
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
    store: *s3_mod.S3Store,
    name: []const u8,
    sz: u64,
};

/// A shared write-once blob depot on a filesystem (local dir, NFS mount).
/// NEVER a live data dir: nodes keep their own LSM dirs; only immutable
/// blobs live here. put() is tmp+rename so a reader can never observe a
/// partial object, and only the creating node ever writes a given name,
/// so tmp names cannot collide across nodes.
pub const DirStore = struct {
    a: std.mem.Allocator,
    depot: []const u8,

    pub fn init(a: std.mem.Allocator, depot: []const u8) !DirStore {
        const tmp_dir = try std.fmt.allocPrint(a, "{s}/tmp", .{depot});
        defer a.free(tmp_dir);
        try vfs.createDirPath(tmp_dir);
        return .{ .a = a, .depot = try a.dupe(u8, depot) };
    }

    pub fn deinit(self: *DirStore) void {
        self.a.free(self.depot);
    }

    fn objectPath(self: *DirStore, name: []const u8) ![]u8 {
        return std.fmt.allocPrint(self.a, "{s}/{s}", .{ self.depot, name });
    }

    fn put(self: *DirStore, name: []const u8, data: []const u8) !void {
        // O_DIRECT requires it, and every legit caller already complies
        // (the SST builder pads + aligns its output; forest.readFile
        // returns page-aligned reads) — reject rather than bounce.
        if (@intFromPtr(data.ptr) % vfs.page_size != 0 or data.len % vfs.page_size != 0)
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
        try vfs.writeAndSync(
            tmp,
            data,
            .{ .mode = .direct, .flags = .{ .truncate = true } },
            .p1,
        );
        try vfs.renameFile(tmp, final);
    }

    fn sync(self: *DirStore) void {
        vfs.syncDir(self.depot);
    }

    fn exists(self: *DirStore, name: []const u8) !bool {
        const path = try self.objectPath(name);
        defer self.a.free(path);

        vfs.access(path) catch return false;
        return true;
    }

    fn delete(self: *DirStore, name: []const u8) !void {
        const path = try self.objectPath(name);
        defer self.a.free(path);

        try vfs.deleteFile(path);
    }

    fn open(self: *DirStore, name: []const u8) !Object {
        const path = try self.objectPath(name);
        defer self.a.free(path);

        const f = try fiber.Dir.cwd().openFile(path, .{});
        errdefer f.close();

        const sz = (try f.stat()).size;
        return .{ .dir = .{ .f = f, .sz = sz } };
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
