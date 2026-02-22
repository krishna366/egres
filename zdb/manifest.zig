const std = @import("std");
const buffers = @import("buffers.gen.zig");

// -------------------------------------------------------------------------
// Constants
// -------------------------------------------------------------------------

const manifest_magic: u32 = 0x666E4D5A; // "ZMnf" little-endian
const manifest_version: u16 = 1;
const manifest_filename = "MANIFEST";
const manifest_rewrite_filename = "MANIFEST-REWRITE";
const default_deletions_rewrite_threshold: u32 = 10_000;
const deletions_ratio: u32 = 10;
const op_create: u32 = 0;
const op_delete: u32 = 1;

// -------------------------------------------------------------------------
// Error set
// -------------------------------------------------------------------------

pub const ManifestError = error{
    BadMagic,
    UnsupportedVersion,
    ChecksumMismatch,
    CorruptManifest,
    DuplicateTableId, // CREATE when table already exists
    TableNotFound, // DELETE on unknown table_id
};

// -------------------------------------------------------------------------
// State types
// -------------------------------------------------------------------------

pub const TableManifest = struct {
    level: u8,
};

pub const LevelManifest = struct {
    tables: std.AutoHashMap(u64, void), // set of table_ids at this level
};

pub const ManifestState = struct {
    allocator: std.mem.Allocator,
    tables: std.AutoHashMap(u64, TableManifest),
    levels: std.ArrayList(LevelManifest),
    creations: u32,
    deletions: u32,
    // I/O fields — null/undefined until openOrCreate sets them.
    file: ?std.fs.File,
    dir: std.fs.Dir,
    deletions_rewrite_threshold: u32,
    mu: std.Thread.Mutex,

    // Initialises the map state only. I/O fields are left undefined and
    // must be filled in by openOrCreate before the value is returned.
    fn initMaps(allocator: std.mem.Allocator) ManifestState {
        return .{
            .allocator = allocator,
            .tables = std.AutoHashMap(u64, TableManifest).init(allocator),
            .levels = std.ArrayList(LevelManifest).empty,
            .creations = 0,
            .deletions = 0,
            .file = null,
            .dir = undefined,
            .deletions_rewrite_threshold = undefined,
            .mu = undefined,
        };
    }

    // Frees map memory only. Used on error paths before I/O fields are set.
    fn freeMaps(self: *ManifestState) void {
        for (self.levels.items) |*lm| {
            lm.tables.deinit();
        }
        self.levels.deinit(self.allocator);
        self.tables.deinit();
    }

    pub fn close(self: *ManifestState) void {
        if (self.file) |f| f.close();
        self.dir.close();
        self.freeMaps();
    }

    /// Applies a single CREATE or DELETE operation to this state.
    pub fn applyChange(self: *ManifestState, op: u32, table_id: u64, level: u8) !void {
        if (op == op_create) {
            if (self.tables.contains(table_id)) return ManifestError.DuplicateTableId;

            // Grow levels list if needed.
            while (self.levels.items.len <= @as(usize, level)) {
                // An empty AutoHashMap doesn't allocate; safe to drop on append failure.
                const lm = LevelManifest{ .tables = std.AutoHashMap(u64, void).init(self.allocator) };
                try self.levels.append(self.allocator, lm);
            }

            try self.tables.put(table_id, .{ .level = level });
            try self.levels.items[level].tables.put(table_id, {});
            self.creations += 1;
        } else if (op == op_delete) {
            const tm = self.tables.get(table_id) orelse return ManifestError.TableNotFound;
            if (tm.level >= self.levels.items.len) return ManifestError.CorruptManifest;
            _ = self.tables.remove(table_id);
            _ = self.levels.items[tm.level].tables.remove(table_id);
            self.deletions += 1;
        }
    }

    /// Reads self.file into memory, validates the header, applies all valid records,
    /// then truncates and seeks to the end of the last complete record, discarding
    /// any partial tail.
    fn replay(self: *ManifestState) !void {
        std.debug.assert(self.tables.count() == 0);
        std.debug.assert(self.levels.items.len == 0);

        const file = self.file.?;
        const file_size = try file.getEndPos();

        var read_buf: [4096]u8 = undefined;
        var r = file.reader(&read_buf);
        const data = try r.interface.readAlloc(self.allocator, file_size);
        defer self.allocator.free(data);

        if (data.len < 8) return ManifestError.BadMagic;

        const magic = std.mem.readInt(u32, data[0..4], .little);
        if (magic != manifest_magic) return ManifestError.BadMagic;

        // data[4..6] = external_magic (reserved, not validated)
        const version = std.mem.readInt(u16, data[6..8], .little);
        if (version != manifest_version) return ManifestError.UnsupportedVersion;

        var pos: usize = 8;
        var trunc_offset: usize = 8; // valid through end of header

        while (true) {
            // Snapshot position at start of this record.
            trunc_offset = pos;

            if (pos + 8 > data.len) break; // no room for record header — truncate here

            const payload_len = std.mem.readInt(u32, data[pos..][0..4], .little);
            const stored_crc = std.mem.readInt(u32, data[pos + 4 ..][0..4], .little);
            pos += 8;

            if (pos + payload_len > data.len) break; // short payload — truncate at record start

            const payload = data[pos .. pos + payload_len];
            pos += payload_len;

            // Verify CRC32C (iSCSI / Castagnoli polynomial).
            const computed_crc = std.hash.crc.Crc32Iscsi.hash(payload);
            if (computed_crc != stored_crc) return ManifestError.ChecksumMismatch;

            // Decode ManifestChangeSet and apply each change.
            const set_reader = try buffers.ManifestChangeSetReader.parseRoot(payload);
            const changes_len = set_reader.changesLen();
            var i: u32 = 0;
            while (i < changes_len) : (i += 1) {
                const change = try set_reader.changesAt(i);
                try self.applyChange(change.op(), change.tableId(), @intCast(change.level()));
            }

            // Advance trunc_offset past this fully-consumed record.
            trunc_offset = pos;
        }

        // Truncate any partial tail and position at end for appending.
        try file.setEndPos(trunc_offset);
        try file.seekTo(trunc_offset);
    }

    /// Compacts the manifest to a single record of live CREATEs, atomically replaces
    /// the MANIFEST file, and updates self.file to the new handle.
    /// The caller is responsible for holding a mutex lock.
    fn rewrite(self: *ManifestState) !void {
        {
            const tmp = try self.dir.createFile(manifest_rewrite_filename, .{ .truncate = true });
            defer tmp.close();
            errdefer self.dir.deleteFile(manifest_rewrite_filename) catch {};

            try writeHeader(tmp);
            if (self.tables.count() > 0) {
                var entries: std.ArrayList(Change) = .empty;
                defer entries.deinit(self.allocator);

                var it = self.tables.iterator();
                while (it.next()) |kv| {
                    try entries.append(self.allocator, .{
                        .op = op_create,
                        .table_id = kv.key_ptr.*,
                        .level = @as(u32, kv.value_ptr.*.level),
                    });
                }

                const payload = try encodeChangeSet(self.allocator, entries.items);
                defer self.allocator.free(payload);

                try writeRecord(tmp, payload);
            }

            try tmp.sync();
        }

        try std.fs.rename(self.dir, manifest_rewrite_filename, self.dir, manifest_filename);

        const new_file = try self.dir.openFile(manifest_filename, .{ .mode = .read_write });
        errdefer new_file.close();
        try new_file.seekFromEnd(0);

        if (self.file) |f| {
            f.close();
        }
        self.file = new_file;
        self.creations = @intCast(self.tables.count());
        self.deletions = 0;
    }

    /// Writes changes as a single record to the manifest file, then applies them to state.
    /// Triggers a compaction rewrite if the deletion threshold is exceeded.
    pub fn commit(self: *ManifestState, changes: []const Change) !void {
        if (changes.len == 0) return;

        self.mu.lock();
        defer self.mu.unlock();

        // Write to file first; state is only updated after the write succeeds.
        // A write failure therefore leaves in-memory state consistent with the file.
        const payload = try encodeChangeSet(self.allocator, changes);
        defer self.allocator.free(payload);

        try writeRecord(self.file.?, payload);
        try self.file.?.sync();

        // Apply changes to state now that the write is durable.
        for (changes) |c| {
            try self.applyChange(c.op, c.table_id, @intCast(c.level));
        }

        // Trigger a compaction rewrite if the deletion ratio warrants it.
        const live = self.creations -| self.deletions;
        const should_rewrite = self.deletions > self.deletions_rewrite_threshold and
            self.deletions > deletions_ratio *| live;

        if (should_rewrite) {
            try self.rewrite(); // sets self.file to the new handle
        }
    }
};

// -------------------------------------------------------------------------
// Change
// -------------------------------------------------------------------------

pub const Change = struct {
    op: u32,
    table_id: u64,
    level: u32,

    pub fn create(table_id: u64, level: u8) Change {
        return .{ .op = op_create, .table_id = table_id, .level = level };
    }

    pub fn delete(table_id: u64) Change {
        return .{ .op = op_delete, .table_id = table_id, .level = 0 };
    }
};

// -------------------------------------------------------------------------
// openOrCreate
// -------------------------------------------------------------------------

/// Opens an existing MANIFEST or creates a fresh one.
/// The returned ManifestState owns the file and directory handles; call close() when done.
pub fn openOrCreate(
    allocator: std.mem.Allocator,
    dir_path: []const u8,
) !ManifestState {
    var dir = try std.fs.cwd().openDir(dir_path, .{});
    errdefer dir.close();

    var state = ManifestState.initMaps(allocator);
    errdefer state.freeMaps();

    // Set I/O fields before any call that needs them (rewrite uses self.dir).
    state.dir = dir;
    state.deletions_rewrite_threshold = default_deletions_rewrite_threshold;
    state.mu = .{};

    // Attempt to open existing manifest.
    const file = dir.openFile(manifest_filename, .{ .mode = .read_write }) catch |err| {
        if (err != error.FileNotFound) return err;

        // No manifest on disk — write a fresh one.
        // rewrite sets state.file.
        try state.rewrite();
        return state;
    };
    errdefer file.close();

    // Assign first so replay can use self.file.
    state.file = file;
    try state.replay();
    return state;
}

// -------------------------------------------------------------------------
// Encoding helpers
// -------------------------------------------------------------------------

fn encodeChangeSet(allocator: std.mem.Allocator, entries: []const Change) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);

    var sw = try buffers.ManifestChangeSetWriter.initWithBuf(allocator, &buf);

    if (entries.len > 0) {
        const offsets = try allocator.alloc(u32, entries.len);
        defer allocator.free(offsets);

        for (entries, 0..) |entry, i| {
            var cw = try buffers.ManifestChangeWriter.initWithBuf(allocator, &buf);
            try cw.setOp(entry.op);
            try cw.setTableId(entry.table_id);
            try cw.setLevel(entry.level);
            const result = try cw.finish();
            offsets[i] = result.offset;
        }

        try sw.setChanges(offsets);
    }

    _ = try sw.finishWithTail();
    return buf.toOwnedSlice(allocator);
}

fn writeRecord(file: std.fs.File, payload: []const u8) !void {
    var record_header: [8]u8 = undefined;
    std.mem.writeInt(u32, record_header[0..4], @intCast(payload.len), .little);

    const crc = std.hash.crc.Crc32Iscsi.hash(payload);
    std.mem.writeInt(u32, record_header[4..8], crc, .little);

    try file.writeAll(&record_header);
    try file.writeAll(payload);
}

fn writeHeader(file: std.fs.File) !void {
    var header: [8]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], manifest_magic, .little);
    std.mem.writeInt(u16, header[4..6], 0, .little); // external_magic reserved
    std.mem.writeInt(u16, header[6..8], manifest_version, .little);
    try file.writeAll(&header);
}
