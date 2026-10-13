// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Disk calibration at startup: the data directory's read and write
//! bandwidth, measured with several requests in flight, which
//! `bwConfigFrom` turns into the bandwidth limiter's settings. Works
//! on a scratch file under the data dir, removed afterwards. Runs
//! before the limiter is configured, so nothing here is throttled.

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;
const _vfs = @import("vfs.zig");
const _lookahead = @import("lookahead.zig");
const _bandwidth = @import("bandwidth.zig");

const log = std.log.scoped(.calibrate);

pub const Profile = struct {
    read_bps: u64,
    write_bps: u64,
};

/// Under the data dir; removed when calibration ends.
const scratch_dir = "calibrate";
/// Requests in flight per direction: one synchronous request at a
/// time measures a fraction of what the device does with a queue,
/// and the limiter regulates the sum of many fibers' I/O.
const depth = 8;
const request_size: u64 = 1 << 20;

/// The `--io` flag: what the disk bandwidth limiter is configured from. By
/// default the data directory is probed at startup (vfs/calibrate.zig)
/// for its read and write bandwidth; the two numbers are the fallback,
/// and the setting when the probe is off.
pub const Cfg = struct {
    auto_calibrate: bool = true,
    bandwidth_mib: u64 = 1024,
    write_cost: u8 = 2,

    pub const docs = .{
        .self = "Disk bandwidth for the I/O limiter: one budget in device " ++
            "time, refilled at the read bandwidth, where a write costs " ++
            "write_cost reads of the same size.",
        .auto_calibrate = "Probe the data directory at startup and take " ++
            "both numbers from the device. A failed probe falls back to " ++
            "bandwidth_mib and write_cost.",
        .bandwidth_mib = "Read bandwidth in MiB/s the budget refills at, " ++
            "when not calibrated.",
        .write_cost = "How many reads of the same size a write costs, " ++
            "when not calibrated. 1 to 32.",
    };
};

/// A device given its own VFS by `deviceVfs`.
pub const Device = struct {
    /// `st_dev` of its directories.
    dev: u64,
    /// The first of its directories, for the dump.
    path: String,
    vfs: *const _vfs.VFS,
};

/// The devices in `deviceVfs` order; startup-only mutation.
var g_devices: std.ArrayList(Device) = .empty;

pub fn devices() []const Device {
    return g_devices.items;
}

/// Validate `--io` and give the global VFS its pinned budget: what
/// files on no `deviceVfs` device charge.
pub fn initGlobal(io_cfg: Cfg) !void {
    if (io_cfg.write_cost < 1 or io_cfg.write_cost > 32) {
        log.err("--io write_cost must be between [1, 32]", .{});
        return error.InvalidFlag;
    }
    _vfs.VFS.initGlobal(pinnedConfig(io_cfg));
}

/// The VFS to open `dir`'s files through: one per device, so
/// directories on one disk share one budget and disks never throttle
/// each other. The first directory on a device probes it (with
/// auto_calibrate; a failed probe, or auto_calibrate=false, pins the
/// flag's numbers) and makes its VFS; later ones get the same. Startup
/// only, before any file on the device is opened: the device list is
/// read without a lock.
pub fn deviceVfs(
    allocator: std.mem.Allocator,
    io_cfg: Cfg,
    dir: String,
) !*const _vfs.VFS {
    try _vfs.createDirPath(dir);
    const dev = try _vfs.deviceOfPath(dir);
    for (g_devices.items) |d| {
        if (d.dev != dev) continue;
        log.info("{s} shares its device with {s}", .{ dir, d.path });
        return d.vfs;
    }

    const cfg = deviceConfig(allocator, io_cfg, pinnedConfig(io_cfg), dir);
    const bw = try allocator.create(_bandwidth.Limiter);
    bw.* = _bandwidth.Limiter.init(cfg);
    const v = try allocator.create(_vfs.VFS);
    v.* = _vfs.VFS.new(bw);
    try g_devices.append(allocator, .{
        .dev = dev,
        .path = dir,
        .vfs = v,
    });

    return v;
}

/// The `--io` numbers as a limiter config.
fn pinnedConfig(io_cfg: Cfg) _bandwidth.Config {
    return .{
        .bytes_per_sec = io_cfg.bandwidth_mib << 20,
        .write_cost = io_cfg.write_cost,
    };
}

/// The limiter config for the device holding `dir`: probed, or pinned.
fn deviceConfig(
    allocator: std.mem.Allocator,
    io_cfg: Cfg,
    pinned: _bandwidth.Config,
    dir: String,
) _bandwidth.Config {
    if (!io_cfg.auto_calibrate) return pinned;
    const p = runCalibration(allocator, dir) catch |err| {
        log.warn("disk calibration of {s} failed: {s}; using --io " ++
            "bandwidth_mib={d} write_cost={d}", .{
            dir, @errorName(err), io_cfg.bandwidth_mib, io_cfg.write_cost,
        });
        return pinned;
    };
    const cfg = bwConfigFrom(p);
    log.info("disk calibrated: {s}: read {d} MiB/s, write {d} MiB/s; " ++
        "setting bandwidth_mib={d} write_cost={d}", .{
        dir,                     p.read_bps >> 20, p.write_bps >> 20,
        cfg.bytes_per_sec >> 20, cfg.write_cost,
    });
    return cfg;
}

/// Probe `data_dir`'s device. The probe file is written and read
/// O_DIRECT so the numbers are the device's, not the page cache's. On
/// tmpfs, which has no O_DIRECT, memory is the device, and its
/// bandwidth is the right one for the limiter.
pub fn runCalibration(a: std.mem.Allocator, data_dir: String) !Profile {
    const depot = try std.fmt.allocPrint(a, "{s}/{s}", .{
        data_dir, scratch_dir,
    });
    defer a.free(depot);

    try _vfs.createDirPath(depot);
    defer _vfs.deleteTree(depot) catch {};

    const path = try std.fmt.allocPrint(a, "{s}/{s}", .{
        depot, _lookahead.probe_object,
    });
    defer a.free(path);
    try fillProbeFile(a, path);

    const read_bps = try throughput(a, path, .read);
    const write_bps = try throughput(a, path, .write);
    if (read_bps == 0 or write_bps == 0) return error.CalibrationFailed;
    return .{ .read_bps = read_bps, .write_bps = write_bps };
}

/// The limiter that fits `p`: one budget in device time, refilled at
/// the read bandwidth, where a write page costs as many read pages as
/// the device takes longer to write it than to read it, to the nearest
/// whole page. Reads and writes share the device, so the budget is
/// shared; the ratio is the exchange rate.
pub fn bwConfigFrom(p: Profile) _bandwidth.Config {
    const write_bps = @max(p.write_bps, 1);
    const ratio = (p.read_bps + write_bps / 2) / write_bps;
    const cost = std.math.clamp(ratio, 1, 255);
    return .{ .bytes_per_sec = p.read_bps, .write_cost = @intCast(cost) };
}

/// The probe file: `probe_max` bytes, written O_DIRECT and synced, so
/// the reads that follow hit the device and not the page cache.
fn fillProbeFile(a: std.mem.Allocator, path: String) !void {
    const size: usize = @intCast(_lookahead.probe_max);
    const data = try a.alignedAlloc(u8, _vfs.page_alignment, size);
    defer a.free(data);
    @memset(data, 0x5A);
    const create: _vfs.FileOptions = .{
        .mode = .direct,
        .flags = .{ .truncate = true },
    };
    try _vfs.writeAndSync(path, data, create);
}

/// Bytes per second over 250ms with `depth` fibers, one per
/// worker, each issuing `request_size` O_DIRECT requests at random
/// offsets of the probe file, back to back.
fn throughput(a: std.mem.Allocator, path: String, kind: _vfs.IOKind) !u64 {
    var stop = std.atomic.Value(bool).init(false);
    var group = fiber.Group(anyerror!u64).init(a);
    defer group.deinit();
    const t0 = utils.timestampNow(.monotonic, .nanos);
    for (0..depth) |i| {
        try group.spawn(.{ .worker_id = @intCast(i) }, worker, .{
            a, path, kind, &stop,
        });
    }
    fiber.sleepMs(250);
    stop.store(true, .release);

    var total: u64 = 0;
    for (group.wait()) |r| total += try r;

    const t1 = utils.timestampNow(.monotonic, .nanos);
    const elapsed_ns: u64 = @intCast(@max(t1 - t0, 1));
    const moved: u128 = @as(u128, total) * std.time.ns_per_s;
    const bps: u64 = @intCast(moved / elapsed_ns);
    log.info("{s} at depth {d}: {d} MiB in {d} ms = {d} MiB/s", .{
        @tagName(kind),                  depth,     total >> 20,
        elapsed_ns / std.time.ns_per_ms, bps >> 20,
    });
    return bps;
}

/// One fiber's share of `throughput`: the file opened once, requests
/// until `stop`, the bytes moved returned.
fn worker(
    a: std.mem.Allocator,
    path: String,
    kind: _vfs.IOKind,
    stop: *std.atomic.Value(bool),
) anyerror!u64 {
    const size: usize = @intCast(request_size);
    const buf = try a.alignedAlloc(u8, _vfs.page_alignment, size);
    defer a.free(buf);
    @memset(buf, 0xA5);

    const f = fiber.File.fromFd(try _vfs.openDirect(path, .{
        .mode = .direct,
        .flags = .{ .writable = kind == .write },
    }));
    defer f.close();

    var seed: [8]u8 = undefined;
    utils.context.g().io.random(&seed);
    const seed_int = std.mem.readInt(u64, &seed, .little);
    var prng = std.Random.DefaultPrng.init(seed_int);
    const rand = prng.random();
    const pages = (_lookahead.probe_max - request_size) / _vfs.page_size;

    var bytes: u64 = 0;
    while (!stop.load(.acquire)) {
        const off = rand.uintAtMost(u64, pages) * _vfs.page_size;
        var done: usize = 0;
        while (done < size) {
            const n = switch (kind) {
                .read => try f.read(buf[done..], off + done),
                .write => try f.write(buf[done..], off + done),
            };
            if (n == 0) return error.ShortIo;
            done += n;
        }
        bytes += size;
    }
    return bytes;
}
