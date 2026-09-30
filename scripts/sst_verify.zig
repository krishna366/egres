// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const zdb = @import("zdb");
const utils = @import("utils");
const String = utils.String;
const vfs = zdb.vfs;
const Table = zdb.table.Table;
const TableIterator = zdb.table.TableIterator;
const Key = zdb.kv.Key;

/// Returns true on success, false on any failure. Prints the result line
/// directly so we don't need to retain pointers into the table after it's
/// dropped (smallest/biggest live in the table's index memory).
/// The file id an SST's name encodes (`<id:016x>.sst`, see
/// `zdb.sstPath`). 0 when the name does not follow that layout — this
/// tool only reports the id, so a stray name is not an error.
fn idFromName(basename: String) u64 {
    if (!std.mem.endsWith(u8, basename, ".sst")) return 0;
    const stem = basename[0 .. basename.len - 4];
    if (stem.len != 16) return 0;
    return std.fmt.parseInt(u64, stem, 16) catch 0;
}

fn verifyOne(allocator: std.mem.Allocator, path: String) bool {
    const base = std.fs.path.basename(path);

    const t = Table.open(allocator, path, idFromName(base), .p2) catch |err| {
        std.debug.print("FAIL {s} open={s}\n", .{ base, @errorName(err) });
        return false;
    };
    defer t.decrRef();

    const file_size = t.handle.file_size;
    const fid = t.file_id;

    // The file carries no namespace or table_id; a standalone check
    // stamps 0 and 0, and only the primaries are looked at.
    var it = TableIterator.init(
        allocator,
        t,
        .p2,
        .{ .namespace = 0, .table_id = 0 },
    );
    defer it.deinit();
    it.seekRange(Key.inf, Key.inf) catch |err| {
        std.debug.print("FAIL {s} size={d} fid={d} seek={s}\n", .{ base, file_size, fid, @errorName(err) });
        return false;
    };

    var prev_buf: std.ArrayList(u8) = .empty;
    defer prev_buf.deinit(allocator);
    var keys: u64 = 0;
    var have_prev = false;

    while (it.itemRef()) |item| {
        // Entries ascend strictly by key, one entry per key.
        if (have_prev) {
            const prev = Key.init(prev_buf.items);
            if (item.key.order(prev) != .gt) {
                std.debug.print("FAIL {s} fid={d} bad_order_at={d}\n", .{ base, fid, keys });
                return false;
            }
        }
        // An entry's versions descend strictly in timestamp.
        const vers = item.versions.slice();
        var vi: usize = 1;
        while (vi < vers.len) : (vi += 1) {
            if (vers[vi].ts >= vers[vi - 1].ts) {
                std.debug.print(
                    "FAIL {s} fid={d} bad_version_order_at={d}\n",
                    .{ base, fid, keys },
                );
                return false;
            }
        }
        prev_buf.clearRetainingCapacity();
        _ = item.key.marshalTo(allocator, &prev_buf) catch |err| {
            std.debug.print("FAIL {s} fid={d} marshal={s}\n", .{ base, fid, @errorName(err) });
            return false;
        };
        have_prev = true;
        keys += 1;
        it.next() catch |err| {
            std.debug.print("FAIL {s} fid={d} keys={d} next={s}\n", .{ base, fid, keys, @errorName(err) });
            return false;
        };
    }
    std.debug.print("OK   {s} size={d} fid={d} keys={d} smallest_pri={x} biggest_pri={x}\n", .{
        base, file_size, fid, keys, t.smallest.primary, t.biggest.primary,
    });
    return true;
}

pub fn main(init: std.process.Init) !void {
    utils.context.initGlobal(init);
    const allocator = utils.context.g().gpa;
    const io = utils.context.g().io;
    const argv = try utils.context.g().argv();

    const dir_path = if (argv.len >= 2) argv[1] else {
        std.debug.print("usage: sst-verify <dir>\n", .{});
        std.process.exit(2);
    };

    // Same defaults the server uses; the verifier only reads, but the global
    // VFS still needs a bandwidth controller.
    vfs.VFS.initGlobal(.{ .bytes_per_sec = 500 * 1024 * 1024 });

    var dir = try std.Io.Dir.cwd().openDir(io, dir_path, .{ .iterate = true });
    defer dir.close(io);

    var paths: std.ArrayList(String) = .empty;
    defer {
        for (paths.items) |p| allocator.free(p);
        paths.deinit(allocator);
    }
    var it = dir.iterate();
    while (try it.next(io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".sst")) continue;
        const full = try std.fs.path.join(allocator, &.{ dir_path, entry.name });
        try paths.append(allocator, full);
    }
    std.mem.sort(String, paths.items, {}, struct {
        fn lt(_: void, a: String, b: String) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);

    var ok: u64 = 0;
    var bad: u64 = 0;
    for (paths.items) |p| {
        if (verifyOne(allocator, p)) ok += 1 else bad += 1;
    }
    std.debug.print("\nsummary: {d} OK, {d} FAIL\n", .{ ok, bad });
    if (bad > 0) std.process.exit(1);
}
