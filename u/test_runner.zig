const std = @import("std");
const builtin = @import("builtin");
const zio = @import("zio");
// Reach into utils module rather than @import("bench.zig") so the runner
// and any test importing utils.bench end up sharing the same module tree
// (avoids "file exists in two modules" errors).
const utils = @import("utils");
const bench = utils.bench;

// ════════════════════════════════════════════════════════════════════════
// Why we have our own test + bench runner
// ════════════════════════════════════════════════════════════════════════
//
// Zig ships a default test runner. `zig build test` invokes the test
// binary with `--listen=-`; the runner emits a structured event stream
// over stdout, and the build runner reads it into a summary tree. Fine
// for a small project. As egres grew we ran into limits — none fatal in
// isolation, but cumulative:
//
//   1. Test names hidden. Names exist in the IPC stream but the build
//      runner captures them into `+- run test 8 passed 7s`. To see
//      *which* test took 7s you had to run the cached binary directly
//      or maintain a parallel `test-trace` step with `stdio = .inherit`.
//
//   2. No concurrent execution. `zig test` runs tests serially. With
//      ~180 tests, a Debug suite was ~60s of mostly-sequential I/O.
//
//   3. Per-file `addTest()` plumbing. build.zig had ~250 lines of
//      stanzas — one `addTest()` per test file with module imports,
//      compression sources, dependency wiring. Adding a test file meant
//      editing build.zig.
//
//   4. No first-class benchmark support. We had four standalone
//      executables (bench-crc, bench-zio, bench-queue, bench-diskwal)
//      each with its own `pub fn main`, hand-tuned iteration counts,
//      and ad-hoc output format.
//
//   5. Test output interleaving. Once we did get concurrent execution,
//      `info(forest): foo` from one test would land between status
//      lines of another. No way to attribute log output to its test.
//
// What owning the runner gets us
// ────────────────────────────────────────────────────────────────────
//
//   • Visible names + timings on every line — no IPC dance:
//       [142/178]  zdb.cache_test    Cache basic put/get round-trip ... OK (5ms)
//
//   • Concurrent execution via zio fibers gated by a semaphore (default
//     max(2, cpu_count/2)). On a 32-core box the suite drops from ~60s
//     serial to ~8s with default parallelism.
//
//   • Auto-discovery. build.zig walks the source tree for `*_test.zig`
//     and `*_bench.zig` and feeds the union into a single test artifact.
//     New test files are picked up without touching build.zig.
//
//   • Per-fiber log capture. `std.log` calls are routed to a per-task
//     buffer (looked up via `zio.getCurrentTaskOrNull`) and printed
//     indented under each test's status line — concurrent output stops
//     interleaving.
//
//   • Color. OK green, FAIL red, SKIP yellow, dim prefix/timing/log
//     lines. Auto TTY detection respects `NO_COLOR` and `CLICOLOR_FORCE`.
//
//   • Stack trace on FAIL. Error return trace dumps right under the
//     FAIL line, so you see exactly which `try std.testing.expect…`
//     blew up.
//
//   • Auto-retry of failures. Tests that fail under heavy parallelism
//     but pass on serial retry are tagged `flaky` and don't break CI.
//     Tests that fail twice stay failed.
//
//   • Prefix-gated test classes. `BIG: ` for slow tests, `BENCH: ` for
//     benchmarks. `zig build test` runs the regular set; `zig build
//     bench` runs only benches in ReleaseFast mode.
//
//   • Go-style benchmark framework. `BENCH:` tests get a `*Bench`
//     handle: `b.n` (auto-tuned to ~200ms target), `b.alloc` (no
//     leak-tracking overhead), `b.resetTimer()` for setup/measure
//     boundary, `b.addBytes(n)` for throughput tracking. Output reports
//     ns/op, MiB/s, iter count, total bytes, sample duration in
//     fixed-width columns.
//
//   • Runtime flags: `--filter`, `--parallel`, `--bigtest`,
//     `--only-bench`, `--help`. No rebuild to re-target a subset.
//
//   • One binary, one entry point: `zig build test` and `zig build
//     bench`. The fan-out of `test-zdb`, `test-raft`, `test-zbuf`,
//     `bench-crc`, `bench-zio`, etc. is gone.
//
// What we gave up
// ────────────────────────────────────────────────────────────────────
//
//   • IPC build-cache integration. The build runner can't show `cached`
//     for our test step because we don't speak its event protocol. The
//     test binary still benefits from compile cache (incremental); only
//     the *run* always re-executes. Acceptable — the run is fast.
//
//   • `std.debug.print` capture. Zig 0.15's stdlib has no hook for it
//     (writes to a global stderr writer via `std.Progress`). Tests that
//     want attributed output should use `std.log.scoped(...).info(...)`
//     instead. Three test-side `std.debug.print` calls were converted.
//
// Other notes
// ────────────────────────────────────────────────────────────────────
//
// Tests run in `builtin.test_functions` order. Empirical: explicit
// duration-sorted (longest-first) ordering clustered heavy tests at the
// start and made wall time *worse*. Trust the natural order.
//
// CLI flags (forwarded via `zig build test -- ...`):
//   --filter <substr>   Run only tests whose name contains <substr>.
//   --exclude <substr>  Skip tests whose name contains <substr> (composes with --filter).
//   --bigtest           Include `BIG: ...` prefixed tests.
//   --only-bench        Run only `BENCH: ...` tests (used by `zig build bench`).
//   --parallel <N>      Concurrent test fibers. 0 (default) = max(2, cpu_count/2).

pub const std_options: std.Options = .{
    .log_scope_levels = &.{
        .{ .scope = .zio, .level = .info },
    },
    .logFn = capturingLogFn,
};

const default_parallel: u6 = 0;

fn resolveParallel(p: u6) u6 {
    if (p != 0) return p;
    // Half the cores by default — leaves CPU headroom for tests that fork
    // their own threads/fibers (e.g., bandwidth tests, ShardedCache concurrent
    // test) and reduces the timing-sensitive flake rate. Floor of 2 so single-
    // and dual-core boxes still get some parallelism.
    const n = std.Thread.getCpuCount() catch return 2;
    return @intCast(@max(2, @min(n / 2, 64)));
}

const Args = struct {
    filter: ?[]const u8 = null,
    exclude: ?[]const u8 = null,
    run_big: bool = false,
    only_bench: bool = false,
    parallel: u6 = default_parallel,
};

const Counters = struct {
    passed: std.atomic.Value(u32) = .{ .raw = 0 },
    failed: std.atomic.Value(u32) = .{ .raw = 0 },
    skipped: std.atomic.Value(u32) = .{ .raw = 0 },
};

// ---------------------------------------------------------------------------
// Per-fiber std.log capture (Option A).
//
// std.options.logFn intercepts every `std.log.info(...)` etc. call. We look
// up the running zio task and write into a buffer registered under that
// task. The runner then prints the captured lines indented under each
// test's status. logs from contexts without a registered buffer (zio
// internals, runner setup) fall through to the default stderr writer.
// ---------------------------------------------------------------------------

const FiberLog = struct {
    bytes: std.ArrayList(u8) = .empty,
    overflowed: bool = false,

    const max_size: usize = 1 * 1024 * 1024; // 1 MiB cap per test
};

// Map keyed on the `*AnyTask` pointer (cast to usize for AutoHashMap).
// Multi-threaded: zio executors live on separate OS threads and call into
// logFn from any of them. std.Thread.Mutex (not zio.Mutex) because logFn
// must work in any context, including when zio's scheduler is mid-handoff.
var log_map_mu: std.Thread.Mutex = .{};
var log_map: ?std.AutoHashMap(usize, *FiberLog) = null;

/// Set in `main()` before the runtime starts; read from `runOne`/`runBench`
/// after each test completion to bump the no-progress timer. A global is
/// simpler than threading a pointer through `driver` and the three
/// `SharedCtx` instances; null until set so a unit-test runner without a
/// watchdog (none today, but easy to imagine) is a no-op call.
var thread_watchdog: ?*ThreadWatchdog = null;

fn registerFiberLog(log: *FiberLog) void {
    const task = zio.getCurrentTaskOrNull() orelse return;
    log_map_mu.lock();
    defer log_map_mu.unlock();
    if (log_map) |*m| m.put(@intFromPtr(task), log) catch {};
}

fn unregisterFiberLog() void {
    const task = zio.getCurrentTaskOrNull() orelse return;
    log_map_mu.lock();
    defer log_map_mu.unlock();
    if (log_map) |*m| _ = m.remove(@intFromPtr(task));
}

fn lookupFiberLog() ?*FiberLog {
    const task = zio.getCurrentTaskOrNull() orelse return null;
    log_map_mu.lock();
    defer log_map_mu.unlock();
    if (log_map) |*m| return m.get(@intFromPtr(task));
    return null;
}

fn capturingLogFn(
    comptime message_level: std.log.Level,
    comptime scope: @Type(.enum_literal),
    comptime format: []const u8,
    args: anytype,
) void {
    const log = lookupFiberLog() orelse {
        std.log.defaultLog(message_level, scope, format, args);
        return;
    };
    if (log.bytes.items.len > FiberLog.max_size) {
        log.overflowed = true;
        return;
    }
    const level_txt = comptime message_level.asText();
    const prefix2 = if (scope == .default) ": " else "(" ++ @tagName(scope) ++ "): ";
    log.bytes.print(std.heap.page_allocator, level_txt ++ prefix2 ++ format ++ "\n", args) catch {};
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

const help_text =
    \\Usage: zig build test [-- <runner-flags>]
    \\
    \\Runs every discovered test (`*_test.zig`, `test.zig`, plus a few extras)
    \\in one binary, on a zio runtime so test bodies can use zio APIs without
    \\setting up their own runtime. Tests run concurrently as fibers.
    \\
    \\std.log output is captured per-test and printed indented under each
    \\test's status line. std.debug.print is NOT captured (no stdlib hook in
    \\Zig 0.15) — use std.log for attributed output.
    \\
    \\Flags (forward via `zig build test -- ...`):
    \\  --filter <substr>   Run only tests whose name contains <substr>.
    \\  --exclude <substr>  Skip tests whose name contains <substr> (composes with --filter).
    \\  --bigtest           Include `BIG: …` prefixed tests (slow, opt-in).
    \\  --only-bench        Run only `BENCH: …` tests (used by `zig build bench`).
    \\  --parallel <N>      Concurrent test fibers. 0 (default) = max(2, cpu_count/2).
    \\  --help, -h          Show this help and exit.
    \\
    \\Examples:
    \\  zig build test                          # default suite (skips BIG:/BENCH:)
    \\  zig build test -- --bigtest             # include the slow tests
    \\  zig build test -- --filter "forest:"    # only forest tests
    \\  zig build test -- --exclude pgwire      # skip every pgwire test
    \\  zig build bench                         # run benchmarks (always ReleaseFast)
    \\
;

fn parseArgs(alloc: std.mem.Allocator) !Args {
    var args: Args = .{};
    const argv = try std.process.argsAlloc(alloc);
    var i: usize = 1;
    while (i < argv.len) : (i += 1) {
        const a = argv[i];
        if (std.mem.eql(u8, a, "--help") or std.mem.eql(u8, a, "-h")) {
            std.debug.print("{s}", .{help_text});
            std.process.exit(0);
        } else if (std.mem.eql(u8, a, "--filter") and i + 1 < argv.len) {
            args.filter = argv[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, a, "--exclude") and i + 1 < argv.len) {
            args.exclude = argv[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, a, "--bigtest")) {
            args.run_big = true;
        } else if (std.mem.eql(u8, a, "--only-bench")) {
            args.only_bench = true;
        } else if (std.mem.eql(u8, a, "--parallel") and i + 1 < argv.len) {
            args.parallel = try std.fmt.parseInt(u6, argv[i + 1], 10);
            i += 1;
        } else {
            std.debug.print("test_runner: unknown arg: {s}\n\n{s}", .{ a, help_text });
            std.process.exit(2);
        }
    }
    return args;
}

// ---------------------------------------------------------------------------
// Test execution
// ---------------------------------------------------------------------------

// Fixed column widths. Test names overflowing these will push the line
// right but won't bloat the column for everyone else.
const prefix_width: usize = 10; // "[NNN/NNN] " up to 999 tests
const module_width: usize = 28; // longest module today: "zdb-server.protocol_test" (24)
const bench_name_width: usize = 50; // longest BENCH name today: ~45 chars
const bench_nsop_width: usize = 12; // right-aligned ns/op column
const bench_tput_width: usize = 12; // right-aligned throughput column
const bench_iters_width: usize = 8; // right-aligned iters column
const bench_total_width: usize = 10; // right-aligned total-bytes column
const bench_dur_width: usize = 9; // right-aligned sample-duration column

const SharedCtx = struct {
    sem: *zio.Semaphore,
    print_mu: *zio.Mutex,
    counters: *Counters,
    seq: *std.atomic.Value(u32),
    total: u32,
    out: *std.io.Writer,
    tty: std.io.tty.Config,
    // Set during the parallel round to collect failures for serial retry.
    // Null during the retry round itself (one bite at the apple).
    failures: ?*std.ArrayList(std.builtin.TestFn),
    // Watchdog state: every running test pushes its entry on entry and pops
    // on exit. The watchdog fiber periodically scans this list and dumps
    // diagnostics if any test has been running longer than `stuck_after_s`.
    running: *RunningSet,
};

/// Currently-running tests, used by the watchdog to identify wedges.
/// Guarded by a `std.Thread.Mutex` rather than `zio.Mutex` so the
/// std.Thread watchdog can safely snapshot it without depending on the
/// zio runtime being alive — critical sections here are pure CPU work
/// (no yields), so grabbing a thread mutex from a fiber is safe.
const RunningSet = struct {
    mu: std.Thread.Mutex = .{},
    list: std.ArrayList(Entry) = .empty,

    const Entry = struct {
        name: []const u8,
        start_ns: i128,
    };

    fn add(self: *RunningSet, name: []const u8) void {
        self.mu.lock();
        defer self.mu.unlock();
        self.list.append(std.heap.page_allocator, .{
            .name = name,
            .start_ns = std.time.nanoTimestamp(),
        }) catch {};
    }

    fn remove(self: *RunningSet, name: []const u8) void {
        self.mu.lock();
        defer self.mu.unlock();
        for (self.list.items, 0..) |t, i| {
            if (t.name.ptr == name.ptr and t.name.len == name.len) {
                _ = self.list.orderedRemove(i);
                return;
            }
        }
    }

    fn deinit(self: *RunningSet) void {
        self.list.deinit(std.heap.page_allocator);
    }
};

/// std.Thread-based watchdog. Runs OUTSIDE the zio runtime, so it stays
/// alive — and capable of dumping diagnostics — even when the runtime is
/// mid-deinit (which is where a 64-hour wedge was observed). Operation:
///
///   • `touch()` is called from each test completion. It just stores a
///     monotonic-clock timestamp atomically; safe to call from any
///     thread/fiber/anywhere.
///   • The thread wakes every `tick_s` seconds via `wake.timedWait`.
///   • Fires a dump when EITHER:
///       (a) no test has completed in `stuck_after_s` seconds, OR
///       (b) some specific running test has been live > `stuck_after_s`.
///     The dump names the stuck tests (with elapsed-ms) and includes
///     a TrackedMutex snapshot. Keeps watching so a growing wedge gets
///     fresh dumps each tick.
///
/// Owns its own RunningSet so the watchdog and test runner share one
/// source of truth on what's currently executing.
///
/// Lifecycle: `init` → `start` → `touch` ... `touch` → `stop`. `stop`
/// flips the wake event and joins; without that the join would block
/// for up to `tick_s` waiting for the next tick.
const ThreadWatchdog = struct {
    last_progress_ns: std.atomic.Value(i64),
    /// Set by `stop()`. The loop uses `wake.timedWait(tick_s)` instead of
    /// raw `std.Thread.sleep` so `stop()` can interrupt the sleep
    /// immediately — without this, shutdown would pay up to `tick_s`
    /// seconds of latency waiting for the next tick.
    wake: std.Thread.ResetEvent = .{},
    thread: ?std.Thread = null,
    stuck_after_s: u64,
    tick_s: u64,
    running: RunningSet = .{},

    fn init(stuck_after_s: u64, tick_s: u64) ThreadWatchdog {
        return .{
            .last_progress_ns = .init(@intCast(std.time.nanoTimestamp())),
            .stuck_after_s = stuck_after_s,
            .tick_s = tick_s,
        };
    }

    fn deinit(self: *ThreadWatchdog) void {
        self.running.deinit();
    }

    fn start(self: *ThreadWatchdog) !void {
        self.thread = try std.Thread.spawn(.{}, threadWatchdogLoop, .{self});
    }

    fn touch(self: *ThreadWatchdog) void {
        self.last_progress_ns.store(@intCast(std.time.nanoTimestamp()), .release);
    }

    fn stop(self: *ThreadWatchdog) void {
        self.wake.set();
        if (self.thread) |t| {
            t.join();
            self.thread = null;
        }
    }
};

/// Top-level (non-method) entry point so `std.Thread.spawn` doesn't trip
/// the Zig backend bug "genSetReg called with a value larger than dst_reg"
/// when it monomorphizes a method through `spawn(.{}, fn_ptr, ...)`.
fn threadWatchdogLoop(self: *ThreadWatchdog) void {
    // Write diagnostics straight to stderr (bypassing the zio-aware
    // print_mu and io_writer). The runtime may be wedged; the print
    // mutex itself may be the deadlocked lock. Using stderr directly
    // keeps the watchdog independent of any zio state.
    const tick_ns: u64 = self.tick_s * std.time.ns_per_s;
    const threshold_ns: i128 = @as(i128, self.stuck_after_s) * std.time.ns_per_s;
    while (true) {
        // timedWait returns error.Timeout on normal tick, void on stop()
        // having called .set(). Either way fall through to the wake check;
        // we exit only when .set() was called.
        self.wake.timedWait(tick_ns) catch {};
        if (self.wake.isSet()) return;

        const now_i128 = std.time.nanoTimestamp();
        const now_i64: i64 = @intCast(now_i128);
        const last = self.last_progress_ns.load(.acquire);
        const elapsed_s: i64 = @divTrunc(now_i64 - last, std.time.ns_per_s);
        const stuck_global = elapsed_s >= @as(i64, @intCast(self.stuck_after_s));

        // Snapshot stuck tests under the lock; release before printing so
        // we never hold it across I/O.
        var stuck: std.ArrayList(RunningSet.Entry) = .empty;
        defer stuck.deinit(std.heap.page_allocator);
        {
            self.running.mu.lock();
            defer self.running.mu.unlock();
            for (self.running.list.items) |t| {
                if (now_i128 - t.start_ns >= threshold_ns) {
                    stuck.append(std.heap.page_allocator, t) catch {};
                }
            }
        }

        if (!stuck_global and stuck.items.len == 0) continue;

        var stderr_buf: [4096]u8 = undefined;
        var stderr_w = std.fs.File.stderr().writer(&stderr_buf);
        const w = &stderr_w.interface;
        w.print(
            "\n=== ThreadWatchdog: no progress for {d}s (threshold={d}s) ===\n",
            .{ elapsed_s, self.stuck_after_s },
        ) catch {};
        for (stuck.items) |t| {
            const ms = @divTrunc(now_i128 - t.start_ns, std.time.ns_per_ms);
            w.print("  RUNNING {d}ms: {s}\n", .{ ms, t.name }) catch {};
        }
        w.print("TrackedMutex state:\n", .{}) catch {};
        utils.tracked_mutex.dump(w) catch {};
        w.print("\n", .{}) catch {};
        w.flush() catch {};
    }
}

fn writeColored(
    out: *std.io.Writer,
    tty: std.io.tty.Config,
    color: std.io.tty.Color,
    comptime fmt: []const u8,
    args: anytype,
) void {
    tty.setColor(out, color) catch {};
    out.print(fmt, args) catch {};
    tty.setColor(out, .reset) catch {};
}

/// Format a byte count using the largest binary-prefix unit that keeps
/// the leading number under 1024. Returns a slice of `buf`. Pass
/// `per_s = true` to append "/s" (used for throughput); `false` for a
/// raw byte total.
fn formatBinaryBytes(buf: []u8, bytes: f64, per_s: bool) []const u8 {
    const units = [_][]const u8{ "B", "KiB", "MiB", "GiB", "TiB", "PiB" };
    var v = bytes;
    var idx: usize = 0;
    while (v >= 1024.0 and idx + 1 < units.len) : (idx += 1) v /= 1024.0;
    const suffix: []const u8 = if (per_s) "/s" else "";
    return std.fmt.bufPrint(buf, "{d:.1} {s}{s}", .{ v, units[idx], suffix }) catch "??";
}

/// Format a count with a decimal SI suffix (K=1e3, M=1e6, G=1e9, T=1e12).
/// Used for iteration counts in benchmark output.
fn formatCount(buf: []u8, n: u64) []const u8 {
    if (n < 1000) return std.fmt.bufPrint(buf, "{d}", .{n}) catch "??";
    const suffixes = [_]u8{ 'K', 'M', 'G', 'T' };
    var v = @as(f64, @floatFromInt(n)) / 1000.0;
    var idx: usize = 0;
    while (v >= 1000.0 and idx + 1 < suffixes.len) : (idx += 1) v /= 1000.0;
    return std.fmt.bufPrint(buf, "{d:.1}{c}", .{ v, suffixes[idx] }) catch "??";
}

/// Format a duration (in nanoseconds) using the largest unit that keeps
/// the leading number under 1000. Returns a slice of `buf`. Uses ASCII
/// "us" rather than UTF-8 "µs" so byte-width padding aligns visually in
/// monospace output (µ is 2 bytes but renders as 1 column).
fn formatDuration(buf: []u8, ns: f64) []const u8 {
    const units = [_][]const u8{ "ns", "us", "ms", "s " };
    var v = ns;
    var idx: usize = 0;
    while (v >= 1000.0 and idx + 1 < units.len) : (idx += 1) v /= 1000.0;
    return std.fmt.bufPrint(buf, "{d:.1} {s}", .{ v, units[idx] }) catch "??";
}

// Zig formats test names as `<module_path>.test.<user-supplied name>`. The
// `.test.` infix is how the compiler distinguishes test decls from regular
// fns/consts; it's just noise to a reader. Display as `<module_path>  <name>`
// with the module padded to a shared width so test names line up vertically.
fn splitModuleName(name: []const u8) struct { module: []const u8, test_name: []const u8 } {
    if (std.mem.indexOf(u8, name, ".test.")) |p| {
        return .{ .module = name[0..p], .test_name = name[p + ".test.".len ..] };
    }
    return .{ .module = "", .test_name = name };
}

fn writeDisplayName(out: *std.io.Writer, tty: std.io.tty.Config, name: []const u8) void {
    const split = splitModuleName(name);
    if (split.module.len == 0) {
        out.writeAll(split.test_name) catch {};
        return;
    }
    // Module path in dim so the test name reads first.
    writeColored(out, tty, .dim, "{s}", .{split.module});
    const pad = if (module_width > split.module.len) module_width - split.module.len else 1;
    for (0..pad) |_| out.writeByte(' ') catch {};
    out.writeAll(split.test_name) catch {};
}

fn runOne(t: std.builtin.TestFn, ctx: *SharedCtx) void {
    ctx.sem.waitUncancelable();
    defer ctx.sem.post();

    ctx.running.add(t.name);
    defer {
        ctx.running.remove(t.name);
        if (thread_watchdog) |w| w.touch();
    }

    var fiber_log: FiberLog = .{};
    defer fiber_log.bytes.deinit(std.heap.page_allocator);
    registerFiberLog(&fiber_log);
    defer unregisterFiberLog();

    // Capture the error return trace into owned memory IMMEDIATELY in the
    // catch arm. The trace lives in thread-local state; if the fiber yields
    // and resumes on a different OS thread, the pointer would become stale.
    // Copying the addresses sidesteps that.
    var trace_addrs: []usize = &.{};
    defer std.heap.page_allocator.free(trace_addrs);

    // Per-test deadline: 30 s normally, 60 s for BIG:-prefixed tests
    // that advertise longer runtimes. AutoCancel.set arms a timer on
    // this fiber; when it fires, the next I/O yield (sleep, lock,
    // recv, …) returns error.Canceled and `timeout.triggered` is set.
    // A test stuck in a tight CPU loop without yielding cannot be
    // canceled cooperatively.
    const user_name = if (std.mem.indexOf(u8, t.name, ".test.")) |p|
        t.name[p + ".test.".len ..]
    else
        t.name;
    const is_big = std.mem.startsWith(u8, user_name, "BIG: ");

    var timeout = zio.AutoCancel.init;
    defer timeout.clear();
    timeout.set(zio.Timeout.fromSeconds(if (is_big) 60 else 30));

    const t0 = std.time.nanoTimestamp();
    const result: ?anyerror = if (t.func()) |_| null else |e| blk: {
        if (e != error.SkipZigTest) {
            if (@errorReturnTrace()) |trace| {
                const n = @min(trace.index, trace.instruction_addresses.len);
                trace_addrs = std.heap.page_allocator.dupe(usize, trace.instruction_addresses[0..n]) catch &.{};
            }
        }
        break :blk e;
    };
    const elapsed_ms = @divTrunc(std.time.nanoTimestamp() - t0, std.time.ns_per_ms);
    const timed_out = if (result) |e| e == error.Canceled and timeout.triggered else false;

    const seq = ctx.seq.fetchAdd(1, .seq_cst) + 1;

    ctx.print_mu.lockUncancelable();
    defer ctx.print_mu.unlock();

    // [N/M] prefix dimmed so the eye lands on the test name + status.
    // Format into a small buffer first so we can pad to a fixed column width.
    var prefix_buf: [32]u8 = undefined;
    const prefix_str = std.fmt.bufPrint(&prefix_buf, "[{d}/{d}]", .{ seq, ctx.total }) catch unreachable;
    writeColored(ctx.out, ctx.tty, .dim, "{s}", .{prefix_str});
    const prefix_pad = if (prefix_width > prefix_str.len) prefix_width - prefix_str.len else 1;
    for (0..prefix_pad) |_| ctx.out.writeByte(' ') catch {};

    writeDisplayName(ctx.out, ctx.tty, t.name);
    ctx.out.print(" ... ", .{}) catch {};

    if (result) |err| switch (err) {
        error.SkipZigTest => {
            _ = ctx.counters.skipped.fetchAdd(1, .seq_cst);
            writeColored(ctx.out, ctx.tty, .bright_yellow, "SKIP\n", .{});
        },
        else => {
            _ = ctx.counters.failed.fetchAdd(1, .seq_cst);
            if (timed_out) {
                writeColored(ctx.out, ctx.tty, .bright_red, "FAIL: TIMEOUT", .{});
            } else {
                writeColored(ctx.out, ctx.tty, .bright_red, "FAIL: {s}", .{@errorName(err)});
            }
            writeColored(ctx.out, ctx.tty, .dim, " ({d}ms)\n", .{elapsed_ms});
            // Capture for serial retry. We're inside print_mu, so the append
            // is implicitly serialized.
            if (ctx.failures) |list| list.append(std.heap.page_allocator, t) catch {};
            // On timeout, log the test name once up front so the diag
            // block self-identifies even if scrolled past the status line.
            if (timed_out) {
                writeColored(ctx.out, ctx.tty, .bright_red, "  | TIMEOUT in '{s}' — diagnostics follow\n", .{user_name});
            }
            // Dump the captured stack trace (writes to stderr directly via
            // its own lock; flush our buffered writer first so order is
            // preserved on the terminal).
            if (trace_addrs.len > 0) {
                ctx.out.flush() catch {};
                std.debug.dumpStackTrace(.{
                    .index = trace_addrs.len,
                    .instruction_addresses = trace_addrs,
                });
            }
            // On timeout, also dump TrackedMutex state — currently-held
            // locks are the most likely smoking gun for a deadlock.
            if (timed_out) {
                ctx.out.flush() catch {};
                writeColored(ctx.out, ctx.tty, .dim, "  | TrackedMutex state for '{s}':\n", .{user_name});
                utils.tracked_mutex.dump(ctx.out) catch {};
                ctx.out.flush() catch {};
            }
        },
    } else {
        _ = ctx.counters.passed.fetchAdd(1, .seq_cst);
        writeColored(ctx.out, ctx.tty, .bright_green, "OK", .{});
        writeColored(ctx.out, ctx.tty, .dim, " ({d}ms)\n", .{elapsed_ms});
    }

    // Print captured log lines indented under the status line.
    if (fiber_log.bytes.items.len > 0) {
        var lines = std.mem.splitScalar(u8, fiber_log.bytes.items, '\n');
        while (lines.next()) |line| {
            if (line.len == 0) continue;
            writeColored(ctx.out, ctx.tty, .dim, "  | {s}\n", .{line});
        }
    }
    if (fiber_log.overflowed) {
        writeColored(ctx.out, ctx.tty, .dim, "  | (output truncated at {d} bytes)\n", .{FiberLog.max_size});
    }
    ctx.out.flush() catch {};
}

fn runBench(t: std.builtin.TestFn, ctx: *SharedCtx) void {
    ctx.sem.waitUncancelable();
    defer ctx.sem.post();

    ctx.running.add(t.name);
    defer {
        ctx.running.remove(t.name);
        if (thread_watchdog) |w| w.touch();
    }

    var fiber_log: FiberLog = .{};
    defer fiber_log.bytes.deinit(std.heap.page_allocator);
    registerFiberLog(&fiber_log);
    defer unregisterFiberLog();

    var b: bench.Bench = .{};
    bench.register(&b);
    defer bench.unregister();

    // Auto-tune: run the bench fn with growing n until a sample exceeds
    // the target duration. The user-controlled timer (b.resetTimer /
    // b.stopTimer / b.startTimer) decides what counts; setup outside
    // those calls isn't measured.
    const target_ns: i128 = 200 * std.time.ns_per_ms;
    const max_n: u64 = 100_000_000_000;
    const max_growth: u64 = 100;

    var sample_ns: i128 = 0;
    var err: ?anyerror = null;
    while (true) {
        // Reset per-sample state so the user starts each sample fresh.
        // resetTimer() in the bench function will move the start point
        // forward to discount setup; addBytes() accumulates into total_bytes.
        b.elapsed_ns = 0;
        b.start_ns = std.time.nanoTimestamp();
        b.timer_running = true;
        b.total_bytes = 0;
        if (t.func()) |_| {} else |e| {
            err = e;
            break;
        }
        sample_ns = b.elapsed();
        if (sample_ns >= target_ns) break;
        if (b.n >= max_n) break;
        const ns_u: u64 = @intCast(@max(1, sample_ns));
        const target_u: u64 = @intCast(target_ns);
        // Extrapolate: pick n such that sample reaches target. Cap growth
        // factor so a sub-microsecond first sample doesn't request 1e12 iters.
        var next_n = (b.n * target_u) / ns_u + 1;
        if (next_n > b.n * max_growth) next_n = b.n * max_growth;
        if (next_n > max_n) next_n = max_n;
        if (next_n <= b.n) next_n = b.n + 1;
        b.n = next_n;
    }

    const seq = ctx.seq.fetchAdd(1, .seq_cst) + 1;

    ctx.print_mu.lockUncancelable();
    defer ctx.print_mu.unlock();

    var prefix_buf: [32]u8 = undefined;
    const prefix_str = std.fmt.bufPrint(&prefix_buf, "[{d}/{d}]", .{ seq, ctx.total }) catch unreachable;
    writeColored(ctx.out, ctx.tty, .dim, "{s}", .{prefix_str});
    const prefix_pad = if (prefix_width > prefix_str.len) prefix_width - prefix_str.len else 1;
    for (0..prefix_pad) |_| ctx.out.writeByte(' ') catch {};

    // Module column (dim) + test name padded to a fixed width so the `→`
    // and metric columns line up across all benches.
    const split = splitModuleName(t.name);
    writeColored(ctx.out, ctx.tty, .dim, "{s}", .{split.module});
    const mod_pad = if (module_width > split.module.len) module_width - split.module.len else 1;
    for (0..mod_pad) |_| ctx.out.writeByte(' ') catch {};
    ctx.out.writeAll(split.test_name) catch {};
    const name_pad = if (bench_name_width > split.test_name.len) bench_name_width - split.test_name.len else 1;
    for (0..name_pad) |_| ctx.out.writeByte(' ') catch {};

    if (err) |e| switch (e) {
        error.SkipZigTest => {
            _ = ctx.counters.skipped.fetchAdd(1, .seq_cst);
            writeColored(ctx.out, ctx.tty, .bright_yellow, "SKIP\n", .{});
        },
        else => {
            _ = ctx.counters.failed.fetchAdd(1, .seq_cst);
            writeColored(ctx.out, ctx.tty, .bright_red, "FAIL: {s}\n", .{@errorName(e)});
        },
    } else {
        _ = ctx.counters.passed.fetchAdd(1, .seq_cst);
        const ns_per_op: f64 = @as(f64, @floatFromInt(sample_ns)) / @as(f64, @floatFromInt(b.n));
        // Format each metric into a buffer, then right-pad to a fixed
        // column. ns/op auto-scales to ns / µs / ms / s so a 9 ns hash
        // and a 4 ms WAL append are both readable in the same column.
        var ns_buf: [32]u8 = undefined;
        const ns_str = formatDuration(&ns_buf, ns_per_op);
        const ns_pad = if (bench_nsop_width > ns_str.len) bench_nsop_width - ns_str.len else 0;
        for (0..ns_pad) |_| ctx.out.writeByte(' ') catch {};
        writeColored(ctx.out, ctx.tty, .bright_green, "{s}/op", .{ns_str});

        if (b.total_bytes > 0) {
            const total_bytes: f64 = @floatFromInt(b.total_bytes);
            const seconds: f64 = @as(f64, @floatFromInt(sample_ns)) / 1e9;
            const bytes_per_s: f64 = total_bytes / seconds;
            var tput_buf: [32]u8 = undefined;
            const tput = formatBinaryBytes(&tput_buf, bytes_per_s, true);
            ctx.out.writeAll("  ") catch {};
            const tput_pad = if (bench_tput_width > tput.len) bench_tput_width - tput.len else 0;
            for (0..tput_pad) |_| ctx.out.writeByte(' ') catch {};
            writeColored(ctx.out, ctx.tty, .bright_green, "{s}", .{tput});
        }

        var count_buf: [16]u8 = undefined;
        const iters = formatCount(&count_buf, b.n);
        ctx.out.writeAll("  ") catch {};
        const iters_pad = if (bench_iters_width > iters.len) bench_iters_width - iters.len else 0;
        for (0..iters_pad) |_| ctx.out.writeByte(' ') catch {};
        writeColored(ctx.out, ctx.tty, .dim, "{s} iters", .{iters});

        if (b.total_bytes > 0) {
            var total_buf: [32]u8 = undefined;
            const total = formatBinaryBytes(&total_buf, @floatFromInt(b.total_bytes), false);
            ctx.out.writeAll("  ") catch {};
            const total_pad = if (bench_total_width > total.len) bench_total_width - total.len else 0;
            for (0..total_pad) |_| ctx.out.writeByte(' ') catch {};
            writeColored(ctx.out, ctx.tty, .dim, "{s} total", .{total});
        }

        // Wall-clock time the final sample took. Mostly an auto-tune sanity
        // check — should be close to the 200ms target unless a single iter
        // overshot.
        var dur_buf: [32]u8 = undefined;
        const dur_str = formatDuration(&dur_buf, @as(f64, @floatFromInt(sample_ns)));
        ctx.out.writeAll("  ") catch {};
        const dur_pad = if (bench_dur_width > dur_str.len) bench_dur_width - dur_str.len else 0;
        for (0..dur_pad) |_| ctx.out.writeByte(' ') catch {};
        writeColored(ctx.out, ctx.tty, .dim, "in {s}\n", .{dur_str});
    }

    if (fiber_log.bytes.items.len > 0) {
        var lines = std.mem.splitScalar(u8, fiber_log.bytes.items, '\n');
        while (lines.next()) |line| {
            if (line.len == 0) continue;
            writeColored(ctx.out, ctx.tty, .dim, "  | {s}\n", .{line});
        }
    }
    ctx.out.flush() catch {};
}

fn driver(
    args: Args,
    counters: *Counters,
    retry_counters: *Counters,
    bench_counters: *Counters,
    alloc: std.mem.Allocator,
    out: *std.io.Writer,
    tty: std.io.tty.Config,
) !void {
    // Spawn the coarse.Clock tick fiber on the runner's long-lived runtime
    // so it survives across every test fiber. `Clock.start()` is idempotent
    // and uses a process-global `started` flag — re-spawning it inside an
    // individual test would attach the tick fiber to that test's runtime
    // (or be a no-op if any earlier test had already set the flag), and
    // when that runtime is deinit'd the fiber dies. Result: subsequent
    // tests calling `Clock.ms()` would read a frozen cache. `Clock.stop()`
    // in `main()` runs after all tests finish, draining this fiber.
    try utils.coarse.Clock.start();

    // Currently-running tests, owned by the std.Thread ThreadWatchdog so
    // its diagnostics can name which tests are stuck. The watchdog itself
    // is set up in main() before the zio runtime starts; here we just
    // grab a pointer to its RunningSet for the test ctx.
    const running: *RunningSet = if (thread_watchdog) |w| &w.running else unreachable;

    var sem = zio.Semaphore{ .permits = args.parallel };
    var print_mu: zio.Mutex = .init;
    var seq: std.atomic.Value(u32) = .{ .raw = 0 };
    var failures: std.ArrayList(std.builtin.TestFn) = .empty;
    defer failures.deinit(std.heap.page_allocator);

    // Partition into regular tests and BENCH:-prefixed benchmarks. Regular
    // tests run in parallel (with retry); benchmarks run serially with
    // auto-tuned iteration counts.
    var kept: std.ArrayList(std.builtin.TestFn) = .empty;
    defer kept.deinit(alloc);
    var bench_tests: std.ArrayList(std.builtin.TestFn) = .empty;
    defer bench_tests.deinit(alloc);

    for (builtin.test_functions) |t| {
        if (std.mem.startsWith(u8, t.name, ".test_root.")) continue;
        const user_name = if (std.mem.indexOf(u8, t.name, ".test.")) |p|
            t.name[p + ".test.".len ..]
        else
            t.name;
        const is_big = std.mem.startsWith(u8, user_name, "BIG: ");
        const is_bench = std.mem.startsWith(u8, user_name, "BENCH: ");
        if (args.filter) |f| if (std.mem.indexOf(u8, t.name, f) == null) continue;
        if (args.exclude) |x| if (std.mem.indexOf(u8, t.name, x) != null) continue;
        if (is_bench) {
            if (!args.only_bench) continue;
            try bench_tests.append(alloc, t);
            continue;
        }
        if (args.only_bench) continue;
        if (is_big and !args.run_big) continue;
        try kept.append(alloc, t);
    }

    var ctx = SharedCtx{
        .sem = &sem,
        .print_mu = &print_mu,
        .counters = counters,
        .seq = &seq,
        .total = @intCast(kept.items.len),
        .out = out,
        .tty = tty,
        .failures = &failures,
        .running = running,
    };

    // Parallel round: run all tests, collecting failures.
    {
        var group = zio.Group{};
        for (kept.items) |t| try group.spawn(runOne, .{ t, &ctx });
        group.wait() catch {};
    }

    // Serial retry round: re-run any failures one at a time. Skips when
    // parallel was already 1 (retry would be the same execution mode) or
    // there's nothing to retry.
    if (failures.items.len > 0 and args.parallel > 1) {
        print_mu.lockUncancelable();
        out.writeAll("\n") catch {};
        writeColored(out, tty, .bright_yellow, "Retrying {d} failed test(s) serially...\n", .{failures.items.len});
        out.flush() catch {};
        print_mu.unlock();

        var retry_sem = zio.Semaphore{ .permits = 1 };
        var retry_seq: std.atomic.Value(u32) = .{ .raw = 0 };
        var retry_ctx = SharedCtx{
            .sem = &retry_sem,
            .print_mu = &print_mu,
            .counters = retry_counters,
            .seq = &retry_seq,
            .total = @intCast(failures.items.len),
            .out = out,
            .tty = tty,
            .failures = null, // one round of retry only
            .running = running,
        };

        var retry_group = zio.Group{};
        for (failures.items) |t| try retry_group.spawn(runOne, .{ t, &retry_ctx });
        retry_group.wait() catch {};
    }

    // Bench round: serial, auto-tuned iteration count. Only runs when
    // there are BENCH:-prefixed tests in the kept set.
    if (bench_tests.items.len == 0) return;

    print_mu.lockUncancelable();
    out.writeAll("\n") catch {};
    writeColored(out, tty, .bright_yellow, "Running {d} benchmark(s)...\n", .{bench_tests.items.len});
    out.flush() catch {};
    print_mu.unlock();

    // Sort benchmarks alphabetically by full name. Sequential dispatch
    // means execution order = sort order, so output groups related
    // variants together rather than declaration order.
    std.mem.sort(std.builtin.TestFn, bench_tests.items, {}, struct {
        fn lt(_: void, a: std.builtin.TestFn, b_: std.builtin.TestFn) bool {
            return std.mem.lessThan(u8, a.name, b_.name);
        }
    }.lt);

    var bench_sem = zio.Semaphore{ .permits = 1 };
    var bench_seq: std.atomic.Value(u32) = .{ .raw = 0 };
    var bench_ctx = SharedCtx{
        .sem = &bench_sem,
        .print_mu = &print_mu,
        .counters = bench_counters,
        .seq = &bench_seq,
        .total = @intCast(bench_tests.items.len),
        .out = out,
        .tty = tty,
        .failures = null,
        .running = running,
    };

    var bench_group = zio.Group{};
    for (bench_tests.items) |t| try bench_group.spawn(runBench, .{ t, &bench_ctx });
    bench_group.wait() catch {};
}

pub fn main() !void {
    var arena: std.heap.ArenaAllocator = .init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var args = try parseArgs(alloc);
    args.parallel = resolveParallel(args.parallel);

    var stderr_buf: [4096]u8 = undefined;
    var stderr_w = std.fs.File.stderr().writer(&stderr_buf);
    const out = &stderr_w.interface;
    const tty = std.io.tty.Config.detect(std.fs.File.stderr());

    log_map = std.AutoHashMap(usize, *FiberLog).init(std.heap.page_allocator);
    defer if (log_map) |*m| m.deinit();

    bench.initRegistry(std.heap.page_allocator);
    defer bench.deinitRegistry();

    // std.Thread watchdog: runs outside the zio runtime so it stays alive
    // (and dumping) even when the runtime itself is mid-deinit. Threshold:
    // 120s of no test completion (covers normal idle gaps between runs,
    // any slow BIG: test, and Clock.stop + rt.deinit hiccups). Tick: 10s
    // so shutdown stop() waits at most 10s.
    var twatch = ThreadWatchdog.init(120, 10);
    defer twatch.deinit();
    thread_watchdog = &twatch;
    try twatch.start();
    defer {
        twatch.stop();
        thread_watchdog = null;
    }

    const rt = try zio.Runtime.init(std.heap.page_allocator, .{
        .executors = .exact(args.parallel),
    });
    defer rt.deinit();

    var counters: Counters = .{};
    var retry_counters: Counters = .{};
    var bench_counters: Counters = .{};
    const t0_all = std.time.nanoTimestamp();

    var handle = try rt.spawn(driver, .{ args, &counters, &retry_counters, &bench_counters, alloc, out, tty });
    try handle.join();
    // Drain the coarse.Clock tick fiber, if any test started it.
    // Without this, `rt.deinit()` asserts task_count == 0 fails.
    utils.coarse.Clock.stop();

    const total_ms = @divTrunc(std.time.nanoTimestamp() - t0_all, std.time.ns_per_ms);
    const main_passed = counters.passed.load(.seq_cst);
    const main_failed = counters.failed.load(.seq_cst);
    const main_skipped = counters.skipped.load(.seq_cst);
    const retry_passed = retry_counters.passed.load(.seq_cst);
    const retry_failed = retry_counters.failed.load(.seq_cst);
    const bench_passed = bench_counters.passed.load(.seq_cst);
    const bench_failed = bench_counters.failed.load(.seq_cst);
    const bench_skipped = bench_counters.skipped.load(.seq_cst);

    // Effective totals across rounds. Retry: if a test failed parallel but
    // passed serial, count it as passed and tag it flaky. Bench: passed/
    // failed/skipped counts roll into the totals so the summary line is
    // accurate when only benchmarks ran.
    const had_retry = retry_passed + retry_failed > 0;
    const eff_passed = main_passed + retry_passed + bench_passed;
    const main_eff_failed = if (had_retry) retry_failed else main_failed;
    const eff_failed = main_eff_failed + bench_failed;
    const eff_skipped = main_skipped + bench_skipped;
    const flaky = retry_passed;

    out.writeAll("\n") catch {};
    writeColored(out, tty, .bright_green, "{d} passed", .{eff_passed});
    out.writeAll(", ") catch {};
    if (eff_failed > 0) {
        writeColored(out, tty, .bright_red, "{d} failed", .{eff_failed});
    } else {
        out.print("{d} failed", .{eff_failed}) catch {};
    }
    out.writeAll(", ") catch {};
    if (eff_skipped > 0) {
        writeColored(out, tty, .bright_yellow, "{d} skipped", .{eff_skipped});
    } else {
        out.print("{d} skipped", .{eff_skipped}) catch {};
    }
    if (flaky > 0) {
        out.writeAll(" (") catch {};
        writeColored(out, tty, .bright_yellow, "{d} flaky", .{flaky});
        out.writeAll(")") catch {};
    }
    const total_s = @as(f64, @floatFromInt(total_ms)) / 1000.0;
    writeColored(out, tty, .dim, " in {d:.1}s (parallel={d})\n", .{ total_s, args.parallel });
    out.flush() catch {};

    if (eff_failed > 0) std.process.exit(1);
}
