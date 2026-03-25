const std = @import("std");
const zio = @import("zio");

const Timestamp = zio.Timestamp;
const Duration = zio.Duration;
const log = std.log.scoped(.zdb_client);

const Args = struct {
    addr: []const u8 = "127.0.0.1:8001",
    clients: u32 = 1,
    records: u64 = 1000,
    ops: u64 = 10000,
    read_pct: u8 = 50,
    value_size: u32 = 100,
    phase: Phase = .both,

    const Phase = enum { load, run, both };
};

fn usage() void {
    std.debug.print(
        \\usage: zdb-client [options]
        \\
        \\  --addr <ip:port>     server address (default: 127.0.0.1:8001)
        \\  --clients <n>        concurrent clients (default: 1)
        \\  --records <n>        records to load (default: 1000)
        \\  --ops <n>            operations per client in run phase (default: 10000)
        \\  --read-pct <0-100>   read percentage (default: 50, i.e. workload A)
        \\  --value-size <n>     value size in bytes (default: 100)
        \\  --load               load phase only
        \\  --run                run phase only
        \\
        \\Workload presets (set via --read-pct):
        \\  50  = YCSB-A (read-heavy update)
        \\  95  = YCSB-B (read-mostly)
        \\  100 = YCSB-C (read-only)
        \\  0   = write-only
        \\
    , .{});
}

fn parseArgs(argv: []const []const u8) !Args {
    var args = Args{};
    var i: usize = 1;
    while (i < argv.len) {
        const arg = argv[i];
        if (std.mem.eql(u8, arg, "--addr")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            args.addr = argv[i];
        } else if (std.mem.eql(u8, arg, "--clients")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            args.clients = try std.fmt.parseInt(u32, argv[i], 10);
        } else if (std.mem.eql(u8, arg, "--records")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            args.records = try std.fmt.parseInt(u64, argv[i], 10);
        } else if (std.mem.eql(u8, arg, "--ops")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            args.ops = try std.fmt.parseInt(u64, argv[i], 10);
        } else if (std.mem.eql(u8, arg, "--read-pct")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            args.read_pct = try std.fmt.parseInt(u8, argv[i], 10);
            if (args.read_pct > 100) return error.InvalidArgs;
        } else if (std.mem.eql(u8, arg, "--value-size")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            args.value_size = try std.fmt.parseInt(u32, argv[i], 10);
        } else if (std.mem.eql(u8, arg, "--load")) {
            args.phase = .load;
        } else if (std.mem.eql(u8, arg, "--run")) {
            args.phase = .run;
        } else {
            return error.InvalidArgs;
        }
        i += 1;
    }
    return args;
}

// --- Text protocol (client side) ---

fn sendSet(stream: zio.net.Stream, ns: u32, key: []const u8, ts: u64, value: []const u8) !void {
    var hdr_buf: [128]u8 = undefined;
    const hdr = std.fmt.bufPrint(&hdr_buf, "set {d} {s} {d} ", .{ ns, key, ts }) catch unreachable;
    try stream.writeAll(hdr, .none);
    try stream.writeAll(value, .none);
    try stream.writeAll("\n", .none);
}

const SetResult = struct {
    ok: bool,
    not_leader: bool = false,
    leader_id: u64 = 0,
};

fn recvSetResponse(stream: zio.net.Stream) !SetResult {
    var buf: [256]u8 = undefined;
    const line = try readResponseLine(stream, &buf);
    if (std.mem.eql(u8, line, "OK")) {
        return .{ .ok = true };
    } else if (std.mem.startsWith(u8, line, "NOT_LEADER ")) {
        const id_str = line["NOT_LEADER ".len..];
        const leader_id = std.fmt.parseInt(u64, id_str, 10) catch 0;
        return .{ .ok = false, .not_leader = true, .leader_id = leader_id };
    } else {
        return .{ .ok = false };
    }
}

fn sendGet(stream: zio.net.Stream, ns: u32, key: []const u8, ts: u64) !void {
    var hdr_buf: [128]u8 = undefined;
    const hdr = std.fmt.bufPrint(&hdr_buf, "get {d} {s} {d}\n", .{ ns, key, ts }) catch unreachable;
    try stream.writeAll(hdr, .none);
}

const GetResult = struct {
    found: bool,
    ts: u64 = 0,
};

fn recvGetResponse(stream: zio.net.Stream) !GetResult {
    var buf: [16 * 1024 * 1024 + 256]u8 = undefined;
    const line = try readResponseLine(stream, &buf);
    if (std.mem.eql(u8, line, "NOT_FOUND")) {
        return .{ .found = false };
    } else if (std.mem.startsWith(u8, line, "FOUND ")) {
        const rest = line["FOUND ".len..];
        // Parse ts from "FOUND <ts> <value>"
        if (std.mem.indexOfScalar(u8, rest, ' ')) |sep| {
            const ts = std.fmt.parseInt(u64, rest[0..sep], 10) catch 0;
            return .{ .found = true, .ts = ts };
        }
        return .{ .found = true };
    } else {
        return .{ .found = false };
    }
}

fn readResponseLine(stream: zio.net.Stream, buf: []u8) ![]const u8 {
    var pos: usize = 0;
    while (pos < buf.len) {
        var byte: [1]u8 = undefined;
        try stream.readAll(&byte, .none);
        if (byte[0] == '\n') {
            const end = if (pos > 0 and buf[pos - 1] == '\r') pos - 1 else pos;
            return buf[0..end];
        }
        buf[pos] = byte[0];
        pos += 1;
    }
    return error.LineTooLong;
}

// --- Monotonic timestamp generator ---

var g_ts = std.atomic.Value(u64).init(0);

fn initTimestamp() void {
    const now = Timestamp.now(.realtime).toNanoseconds();
    g_ts.store(now, .release);
}

fn nextTimestamp() u64 {
    return g_ts.fetchAdd(1, .monotonic);
}

// --- Key generation (YCSB-style) ---

fn formatKey(buf: *[24]u8, id: u64) []const u8 {
    const result = std.fmt.bufPrint(buf, "user{d:0>16}", .{id}) catch unreachable;
    return result;
}

// --- Per-worker stats ---

const WorkerStats = struct {
    read_ok: u64 = 0,
    read_miss: u64 = 0,
    read_err: u64 = 0,
    write_ok: u64 = 0,
    write_err: u64 = 0,
    total_read_ns: u64 = 0,
    total_write_ns: u64 = 0,
    min_read_ns: u64 = std.math.maxInt(u64),
    max_read_ns: u64 = 0,
    min_write_ns: u64 = std.math.maxInt(u64),
    max_write_ns: u64 = 0,

    fn merge(self: *WorkerStats, other: WorkerStats) void {
        self.read_ok += other.read_ok;
        self.read_miss += other.read_miss;
        self.read_err += other.read_err;
        self.write_ok += other.write_ok;
        self.write_err += other.write_err;
        self.total_read_ns += other.total_read_ns;
        self.total_write_ns += other.total_write_ns;
        self.min_read_ns = @min(self.min_read_ns, other.min_read_ns);
        self.max_read_ns = @max(self.max_read_ns, other.max_read_ns);
        self.min_write_ns = @min(self.min_write_ns, other.min_write_ns);
        self.max_write_ns = @max(self.max_write_ns, other.max_write_ns);
    }
};

// --- Worker coroutine ---

const WorkerCtx = struct {
    id: u32,
    args: *const Args,
    server_addr: zio.net.IpAddress,
    stats: WorkerStats = .{},
    load_elapsed_ns: u64 = 0,
    run_elapsed_ns: u64 = 0,
    err: ?anyerror = null,
};

fn workerFn(ctx: *WorkerCtx) void {
    workerImpl(ctx) catch |err| {
        ctx.err = err;
    };
}

fn workerImpl(ctx: *WorkerCtx) !void {
    const args = ctx.args;

    log.info("worker {d}: connecting to server", .{ctx.id});
    const stream = ctx.server_addr.connect(.{}) catch |err| {
        log.err("worker {d}: connect failed: {s}", .{ ctx.id, @errorName(err) });
        return err;
    };
    defer stream.close();
    stream.socket.setNoDelay(true) catch {};
    log.info("worker {d}: connected", .{ctx.id});

    // Generate value once.
    var value_buf: [16 * 1024]u8 = undefined;
    const value_size: usize = @min(args.value_size, value_buf.len);
    var prng = std.Random.DefaultPrng.init(@as(u64, ctx.id) ^ 0xdeadbeef);
    prng.random().bytes(value_buf[0..value_size]);
    const value = value_buf[0..value_size];

    // Load phase: each worker inserts its slice of records.
    if (args.phase == .load or args.phase == .both) {
        const total = args.records;
        const n_clients: u64 = args.clients;
        const per_worker = total / n_clients;
        const start_key = per_worker * ctx.id;
        const end_key = if (ctx.id == args.clients - 1) total else start_key + per_worker;

        log.info("worker {d}: load phase keys [{d}..{d})", .{ ctx.id, start_key, end_key });
        const t0 = Timestamp.now(.monotonic);
        var k: u64 = start_key;
        while (k < end_key) : (k += 1) {
            var key_buf: [24]u8 = undefined;
            const key = formatKey(&key_buf, k);
            try sendSet(stream, 0, key, nextTimestamp(), value);
            const resp = try recvSetResponse(stream);
            if (resp.ok) {
                ctx.stats.write_ok += 1;
            } else {
                ctx.stats.write_err += 1;
                if (resp.not_leader) {
                    log.warn("worker {d}: not leader (leader_id={d})", .{ ctx.id, resp.leader_id });
                }
            }
            // Progress every 1000 records.
            if ((ctx.stats.write_ok + ctx.stats.write_err) % 1000 == 0) {
                log.info("worker {d}: loaded {d}/{d} records", .{
                    ctx.id, ctx.stats.write_ok + ctx.stats.write_err, end_key - start_key,
                });
            }
        }
        const t1 = Timestamp.now(.monotonic);
        ctx.load_elapsed_ns = Timestamp.durationTo(t0, t1).toNanoseconds();
        log.info("worker {d}: load complete — {d} ok, {d} err, {d:.1} ms", .{
            ctx.id,
            ctx.stats.write_ok,
            ctx.stats.write_err,
            @as(f64, @floatFromInt(ctx.load_elapsed_ns)) / 1e6,
        });
    }

    // Run phase: random read/write mix.
    if (args.phase == .run or args.phase == .both) {
        log.info("worker {d}: run phase — {d} ops, {d}% reads", .{ ctx.id, args.ops, args.read_pct });
        const t0 = Timestamp.now(.monotonic);
        var i: u64 = 0;
        while (i < args.ops) : (i += 1) {
            const r = prng.random().intRangeAtMost(u8, 0, 99);
            var key_buf: [24]u8 = undefined;
            const key_id = prng.random().intRangeLessThan(u64, 0, args.records);
            const key = formatKey(&key_buf, key_id);

            if (r < args.read_pct) {
                // Read
                const op_start = Timestamp.now(.monotonic);
                sendGet(stream, 0, key, std.math.maxInt(u64)) catch {
                    ctx.stats.read_err += 1;
                    continue;
                };
                const result = recvGetResponse(stream) catch {
                    ctx.stats.read_err += 1;
                    continue;
                };
                const lat = Timestamp.durationTo(op_start, Timestamp.now(.monotonic)).toNanoseconds();
                ctx.stats.total_read_ns += lat;
                ctx.stats.min_read_ns = @min(ctx.stats.min_read_ns, lat);
                ctx.stats.max_read_ns = @max(ctx.stats.max_read_ns, lat);
                if (result.found) {
                    ctx.stats.read_ok += 1;
                } else {
                    ctx.stats.read_miss += 1;
                }
            } else {
                // Write
                const op_start = Timestamp.now(.monotonic);
                sendSet(stream, 0, key, nextTimestamp(), value) catch {
                    ctx.stats.write_err += 1;
                    continue;
                };
                const resp = recvSetResponse(stream) catch {
                    ctx.stats.write_err += 1;
                    continue;
                };
                const lat = Timestamp.durationTo(op_start, Timestamp.now(.monotonic)).toNanoseconds();
                ctx.stats.total_write_ns += lat;
                ctx.stats.min_write_ns = @min(ctx.stats.min_write_ns, lat);
                ctx.stats.max_write_ns = @max(ctx.stats.max_write_ns, lat);
                if (resp.ok) {
                    ctx.stats.write_ok += 1;
                } else {
                    ctx.stats.write_err += 1;
                }
            }
            // Progress every 1000 ops.
            const done = ctx.stats.read_ok + ctx.stats.read_miss + ctx.stats.read_err + ctx.stats.write_ok + ctx.stats.write_err;
            if (done % 1000 == 0) {
                log.info("worker {d}: {d}/{d} ops done", .{ ctx.id, done, args.ops });
            }
        }
        const t1 = Timestamp.now(.monotonic);
        ctx.run_elapsed_ns = Timestamp.durationTo(t0, t1).toNanoseconds();
        log.info("worker {d}: run complete — reads={d} writes={d} {d:.1} ms", .{
            ctx.id,
            ctx.stats.read_ok + ctx.stats.read_miss,
            ctx.stats.write_ok,
            @as(f64, @floatFromInt(ctx.run_elapsed_ns)) / 1e6,
        });
    }
}

// --- Printing results ---

fn printResults(args: *const Args, contexts: []WorkerCtx) void {
    var total = WorkerStats{};
    var load_ns: u64 = 0;
    var run_ns: u64 = 0;
    var had_error = false;

    for (contexts) |*ctx| {
        if (ctx.err) |err| {
            std.debug.print("worker {d}: error: {s}\n", .{ ctx.id, @errorName(err) });
            had_error = true;
        }
        total.merge(ctx.stats);
        load_ns = @max(load_ns, ctx.load_elapsed_ns);
        run_ns = @max(run_ns, ctx.run_elapsed_ns);
    }

    if (had_error) {
        std.debug.print("\n", .{});
    }

    if (args.phase == .load or args.phase == .both) {
        const load_recs = total.write_ok + total.write_err;
        if (load_ns > 0 and load_recs > 0) {
            const load_ops_sec = load_recs * 1_000_000_000 / load_ns;
            std.debug.print("--- Load Phase ---\n", .{});
            std.debug.print("  records:   {d}\n", .{load_recs});
            std.debug.print("  elapsed:   {d:.2} ms\n", .{@as(f64, @floatFromInt(load_ns)) / 1e6});
            std.debug.print("  throughput: {d} ops/sec\n", .{load_ops_sec});
            std.debug.print("  errors:    {d}\n", .{total.write_err});
        }
        // Reset write stats if both phases — load writes shouldn't mix with run writes.
        if (args.phase == .both) {
            // Re-aggregate from scratch for run phase reporting below.
            total = .{};
            for (contexts) |*ctx| {
                total.merge(ctx.stats);
            }
        }
    }

    if (args.phase == .run or args.phase == .both) {
        const total_ops = total.read_ok + total.read_miss + total.read_err + total.write_ok + total.write_err;
        if (run_ns > 0 and total_ops > 0) {
            const ops_sec = total_ops * 1_000_000_000 / run_ns;
            const reads = total.read_ok + total.read_miss;
            const writes = total.write_ok;

            std.debug.print("--- Run Phase ---\n", .{});
            std.debug.print("  operations: {d} ({d} reads, {d} writes)\n", .{ total_ops, reads, writes });
            std.debug.print("  elapsed:    {d:.2} ms\n", .{@as(f64, @floatFromInt(run_ns)) / 1e6});
            std.debug.print("  throughput: {d} ops/sec\n", .{ops_sec});
            std.debug.print("  errors:     {d} read, {d} write\n", .{ total.read_err, total.write_err });

            if (reads > 0) {
                const avg_read = total.total_read_ns / reads;
                std.debug.print("  read  avg: {d:.2} us, min: {d:.2} us, max: {d:.2} us\n", .{
                    @as(f64, @floatFromInt(avg_read)) / 1e3,
                    @as(f64, @floatFromInt(total.min_read_ns)) / 1e3,
                    @as(f64, @floatFromInt(total.max_read_ns)) / 1e3,
                });
            }
            if (writes > 0) {
                const avg_write = total.total_write_ns / writes;
                std.debug.print("  write avg: {d:.2} us, min: {d:.2} us, max: {d:.2} us\n", .{
                    @as(f64, @floatFromInt(avg_write)) / 1e3,
                    @as(f64, @floatFromInt(total.min_write_ns)) / 1e3,
                    @as(f64, @floatFromInt(total.max_write_ns)) / 1e3,
                });
            }

            if (total.read_miss > 0) {
                std.debug.print("  read misses: {d}\n", .{total.read_miss});
            }
        }
    }
}

// --- Main ---

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const argv = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, argv);

    const args = parseArgs(argv) catch {
        usage();
        return;
    };

    std.debug.print("zdb-client: addr={s} clients={d} records={d} ops={d} read%={d} value_size={d}\n", .{
        args.addr, args.clients, args.records, args.ops, args.read_pct, args.value_size,
    });

    // Init a zio runtime directly — this makes the main thread a zio executor,
    // so all zio operations (Group, connect, readAll, etc.) work from here.
    const n_threads: u6 = @intCast(@max(2, args.clients));
    var rt = try zio.Runtime.init(allocator, .{
        .executors = @enumFromInt(n_threads),
    });
    defer rt.deinit();

    log.info("runtime initialized with {d} threads, starting benchmark", .{n_threads});
    runBenchmark(allocator, &args);
}

fn runBenchmark(allocator: std.mem.Allocator, args: *const Args) void {
    log.info("runBenchmark", .{});
    initTimestamp();
    const server_addr = zio.net.IpAddress.parseIpAndPort(args.addr) catch |err| {
        log.err("invalid address '{s}': {s}", .{ args.addr, @errorName(err) });
        return;
    };
    log.info("server_addr done", .{});

    // Allocate worker contexts.
    const contexts = allocator.alloc(WorkerCtx, args.clients) catch {
        log.err("out of memory allocating worker contexts", .{});
        return;
    };
    defer allocator.free(contexts);

    for (contexts, 0..) |*ctx, i| {
        ctx.* = .{
            .id = @intCast(i),
            .args = args,
            .server_addr = server_addr,
        };
    }

    log.info("spawning {d} worker(s)", .{args.clients});

    // Spawn all workers.
    var group: zio.Group = .init;
    defer group.cancel();

    for (contexts) |*ctx| {
        group.spawn(workerFn, .{ctx}) catch {
            log.err("failed to spawn worker {d}", .{ctx.id});
            return;
        };
    }

    // Wait for all workers.
    group.wait() catch return;

    log.info("all workers finished", .{});
    printResults(args, contexts);
}
