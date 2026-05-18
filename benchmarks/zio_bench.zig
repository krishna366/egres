const std = @import("std");
const zio = @import("zio");

const BLOCK_SIZE: usize = 4096;

// ── Stats ────────────────────────────────────────────────────────────

const Stats = struct {
    total_ops: u64,
    total_ns: u64,
    latencies: ?[]u64,

    fn opsPerSec(self: Stats) f64 {
        return @as(f64, @floatFromInt(self.total_ops)) /
            (@as(f64, @floatFromInt(self.total_ns)) / 1e9);
    }

    fn mbPerSec(self: Stats) f64 {
        return self.opsPerSec() * @as(f64, @floatFromInt(BLOCK_SIZE)) / (1024.0 * 1024.0);
    }

    fn percentile(self: Stats, p: f64) u64 {
        const lats = self.latencies orelse return 0;
        if (lats.len == 0) return 0;
        const idx: usize = @intFromFloat(@as(f64, @floatFromInt(lats.len)) * p);
        return lats[@min(idx, lats.len - 1)];
    }

    fn p50us(self: Stats) f64 {
        return @as(f64, @floatFromInt(self.percentile(0.50))) / 1000.0;
    }

    fn p99us(self: Stats) f64 {
        return @as(f64, @floatFromInt(self.percentile(0.99))) / 1000.0;
    }
};

// ── I/O work ─────────────────────────────────────────────────────────

fn ioWorkPosix(fd: std.posix.fd_t, offsets: []const u64, latencies: []u64) void {
    var buf: [BLOCK_SIZE]u8 align(4096) = undefined;
    for (offsets, 0..) |offset, i| {
        const t0 = std.time.nanoTimestamp();
        _ = std.posix.pread(fd, &buf, offset) catch {};
        const t1 = std.time.nanoTimestamp();
        latencies[i] = @intCast(t1 - t0);
    }
}

fn ioWorkZio(fd_int: std.posix.fd_t, offsets: []const u64, latencies: []u64) void {
    var buf: [BLOCK_SIZE]u8 align(4096) = undefined;
    const file = zio.File.fromFd(fd_int);
    for (offsets, 0..) |offset, i| {
        const t0 = std.time.nanoTimestamp();
        _ = file.read(&buf, offset) catch {};
        const t1 = std.time.nanoTimestamp();
        latencies[i] = @intCast(t1 - t0);
    }
}

// ── CPU work ─────────────────────────────────────────────────────────

fn cpuWork(count: usize) void {
    var buf: [BLOCK_SIZE]u8 = undefined;
    @memset(&buf, 0xAB);
    var sink: u32 = 0;
    for (0..count) |i| {
        buf[0] = @truncate(i);
        sink ^= std.hash.Crc32.hash(&buf);
    }
    std.mem.doNotOptimizeAway(sink);
}

fn fiberCpuWorker(count: usize) void {
    cpuWork(count);
}

// ── Benchmarks ───────────────────────────────────────────────────────

fn benchThreadCpu(num_workers: usize, ops_per_worker: usize) Stats {
    var threads: [1024]std.Thread = undefined;
    const t0 = std.time.nanoTimestamp();
    for (0..num_workers) |i| {
        threads[i] = std.Thread.spawn(.{}, cpuWork, .{ops_per_worker}) catch @panic("spawn");
    }
    for (0..num_workers) |i| threads[i].join();
    const t1 = std.time.nanoTimestamp();
    return .{ .total_ops = @intCast(num_workers * ops_per_worker), .total_ns = @intCast(t1 - t0), .latencies = null };
}

fn benchFiberCpu(num_workers: usize, ops_per_worker: usize) !Stats {
    var group = zio.Group{};
    defer group.cancel();
    const t0 = std.time.nanoTimestamp();
    for (0..num_workers) |_| try group.spawn(fiberCpuWorker, .{ops_per_worker});
    try group.wait();
    const t1 = std.time.nanoTimestamp();
    return .{ .total_ops = @intCast(num_workers * ops_per_worker), .total_ns = @intCast(t1 - t0), .latencies = null };
}

fn benchThreadIo(
    allocator: std.mem.Allocator,
    num_workers: usize,
    total_ops: usize,
    fd: std.posix.fd_t,
    offsets: []const u64,
) Stats {
    const ops_per = total_ops / num_workers;
    const latencies = allocator.alloc(u64, num_workers * ops_per) catch @panic("OOM");
    var threads: [1024]std.Thread = undefined;

    const t0 = std.time.nanoTimestamp();
    for (0..num_workers) |i| {
        threads[i] = std.Thread.spawn(.{}, ioWorkPosix, .{
            fd,
            offsets[i * ops_per .. (i + 1) * ops_per],
            latencies[i * ops_per .. (i + 1) * ops_per],
        }) catch @panic("spawn");
    }
    for (0..num_workers) |i| threads[i].join();
    const t1 = std.time.nanoTimestamp();

    std.mem.sort(u64, latencies, {}, std.sort.asc(u64));
    return .{ .total_ops = @intCast(num_workers * ops_per), .total_ns = @intCast(t1 - t0), .latencies = latencies };
}

fn benchFiberIo(
    allocator: std.mem.Allocator,
    num_fibers: usize,
    total_ops: usize,
    fd: std.posix.fd_t,
    offsets: []const u64,
) !Stats {
    const ops_per = total_ops / num_fibers;
    const latencies = try allocator.alloc(u64, num_fibers * ops_per);

    var group = zio.Group{};
    defer group.cancel();

    const t0 = std.time.nanoTimestamp();
    for (0..num_fibers) |i| {
        try group.spawn(ioWorkZio, .{
            fd,
            offsets[i * ops_per .. (i + 1) * ops_per],
            latencies[i * ops_per .. (i + 1) * ops_per],
        });
    }
    try group.wait();
    const t1 = std.time.nanoTimestamp();

    std.mem.sort(u64, latencies, {}, std.sort.asc(u64));
    return .{ .total_ops = @intCast(num_fibers * ops_per), .total_ns = @intCast(t1 - t0), .latencies = latencies };
}

// ── File setup ───────────────────────────────────────────────────────

fn createBenchFile(path: []const u8, size: usize) !void {
    const file = try std.fs.createFileAbsolute(path, .{});
    defer file.close();
    var buf: [BLOCK_SIZE]u8 = undefined;
    @memset(&buf, 0xDE);
    var written: usize = 0;
    while (written < size) {
        const n = @min(BLOCK_SIZE, size - written);
        try file.writeAll(buf[0..n]);
        written += n;
    }
    try file.sync();
}

fn openDirect(path: []const u8) !std.posix.fd_t {
    var flags: std.posix.O = .{ .ACCMODE = .RDONLY };
    flags.DIRECT = true;
    return std.posix.openat(std.posix.AT.FDCWD, path, flags, 0) catch |err| {
        if (err == error.BadPathName) {
            std.debug.print("WARNING: O_DIRECT not supported, falling back to buffered I/O\n", .{});
            return std.posix.openat(std.posix.AT.FDCWD, path, .{ .ACCMODE = .RDONLY }, 0);
        }
        return err;
    };
}

fn generateOffsets(allocator: std.mem.Allocator, count: usize, file_size: usize) ![]u64 {
    const offsets = try allocator.alloc(u64, count);
    var prng = std.Random.DefaultPrng.init(42);
    const rand = prng.random();
    const max_block = file_size / BLOCK_SIZE;
    for (offsets) |*o| {
        o.* = rand.intRangeLessThan(u64, 0, max_block) * BLOCK_SIZE;
    }
    return offsets;
}

// ── Main ─────────────────────────────────────────────────────────────

pub fn main() !void {
    const allocator = std.heap.smp_allocator;
    const num_cores = std.Thread.getCpuCount() catch 4;

    const file_size: usize = 1024 * 1024 * 1024; // 1 GB — larger to avoid NVMe cache effects
    const tmp_path = "/data/zio_bench.dat";

    std.debug.print("\nzio benchmark — {d} cores, block={d}B, file={d}MB, O_DIRECT\n", .{
        num_cores, BLOCK_SIZE, file_size / 1024 / 1024,
    });

    std.debug.print("Creating benchmark file...\n", .{});
    try createBenchFile(tmp_path, file_size);
    defer std.fs.deleteFileAbsolute(tmp_path) catch {};

    const fd = try openDirect(tmp_path);
    defer std.posix.close(fd);

    // ── CPU benchmark ──
    {
        const ops: usize = 100_000;
        std.debug.print("\n=== CPU: CRC32 {d}B, {d} workers x {d} ops ===\n", .{ BLOCK_SIZE, num_cores, ops });

        const thread = benchThreadCpu(num_cores, ops);
        std.debug.print("  std.Thread     {d:>10.0} ops/sec\n", .{thread.opsPerSec()});

        var rt = try zio.Runtime.init(allocator, .{ .executors = .auto });
        const fiber = try benchFiberCpu(num_cores, ops);
        rt.deinit();

        std.debug.print("  zio.Group      {d:>10.0} ops/sec\n", .{fiber.opsPerSec()});
    }

    // ── I/O concurrency sweep ──
    //
    // For threads: spawning more threads than cores just adds context-switch
    // overhead — each thread blocks in the pread syscall. We cap at num_cores.
    //
    // For fibers: a suspended fiber costs ~zero. We can have hundreds of
    // concurrent io_uring submissions with only num_cores OS threads.
    // This is where io_uring wins.

    const total_io_ops: usize = 320_000;
    const max_offsets: usize = total_io_ops; // enough for any concurrency level
    const offsets = try generateOffsets(allocator, max_offsets, file_size);
    defer allocator.free(offsets);

    const concurrency_levels = [_]usize{ 1, 2, 4, 8, 16, 32, 64, 128, 256, 512 };

    std.debug.print("\n=== I/O: random pread {d}B, O_DIRECT, {d} total ops ===\n", .{ BLOCK_SIZE, total_io_ops });
    std.debug.print("  {s:>8}  {s:>12} {s:>10} {s:>10} {s:>10}  {s:>12} {s:>10} {s:>10} {s:>10}\n", .{
        "conc", "thr ops/s", "thr MB/s", "thr p50", "thr p99", "fib ops/s", "fib MB/s", "fib p50", "fib p99",
    });

    var rt = try zio.Runtime.init(allocator, .{ .executors = .exact(4) });

    for (concurrency_levels) |conc| {
        // Threads: cap at 4 — simulates a real app with a small thread pool.
        // This is the constraint io_uring lifts: fibers scale concurrency
        // without needing more OS threads.
        const thr_conc = @min(conc, 4);
        const thr = benchThreadIo(allocator, thr_conc, total_io_ops, fd, offsets);
        defer allocator.free(thr.latencies.?);

        // Fibers: use the full concurrency — this is io_uring's advantage
        const fib = benchFiberIo(allocator, conc, total_io_ops, fd, offsets) catch {
            std.debug.print("  {d:>8}  (fiber bench failed)\n", .{conc});
            continue;
        };
        defer allocator.free(fib.latencies.?);

        std.debug.print("  {d:>8}  {d:>12.0} {d:>9.1}M {d:>9.0}us {d:>9.0}us  {d:>12.0} {d:>9.1}M {d:>9.0}us {d:>9.0}us\n", .{
            conc,
            thr.opsPerSec(),
            thr.mbPerSec(),
            thr.p50us(),
            thr.p99us(),
            fib.opsPerSec(),
            fib.mbPerSec(),
            fib.p50us(),
            fib.p99us(),
        });
    }

    rt.deinit();
    std.debug.print("\n", .{});
}
