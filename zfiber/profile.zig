//! In-process CPU sampling profiler, started and stopped from the admin
//! protocol (`profile cpu <seconds> [hz]`). Replaces running the server
//! under gdb: no ptrace, no process-wide stop, a few percent of one
//! worker's time at most.
//!
//! Linux: one POSIX timer per worker thread on that thread's CPU clock,
//! delivering SIGPROF to that thread (SIGEV_THREAD_ID), so each worker
//! is sampled in proportion to its own CPU use and an idle worker is
//! never sampled. macOS: the process-wide profiling interval timer,
//! which delivers SIGPROF to whichever thread is running.
//!
//! The handler walks frame pointers from the interrupted context into a
//! preallocated ring: no allocation, no locks, atomics only. Frame
//! pointers stay on in every build (build.zig), and a walk stops at the
//! first frame that is not above the previous one, so a foreign frame
//! without one ends the sample rather than derailing it.
//!
//! `stop` symbolizes each distinct address once (a few hundred
//! milliseconds, the first call scanning the debug info) and writes
//! either pprof's `top` table, flat and cumulative samples per function,
//! or folded stacks: root first, frames joined by semicolons, the
//! sample count at the end, what flamegraph tools and their diff tools
//! read.

const std = @import("std");
const builtin = @import("builtin");
const _rt = @import("runtime.zig");
const timestampNow = @import("coarse_clock.zig").timestampNow;
const _ctx = @import("context.zig");
const Worker = _rt.Worker;
const Allocator = std.mem.Allocator;

pub const max_frames = 64;
/// Header words before a sample's frames in the ring: the frame count,
/// then the worker id with `on_fiber` set when the sample was taken on
/// a fiber rather than in the worker's own loop.
const header = 2;
const on_fiber: usize = 1 << 8;

pub const Options = struct {
    /// Samples per second of CPU time, per thread.
    hz: u32 = 200,
    /// How long the sampler runs, to size the ring.
    secs: u32 = 10,
};

pub const Report = struct {
    samples: usize,
    /// Samples that found the ring full.
    dropped: usize,
    stacks: usize,
};

/// What `stop` writes: pprof's `top` table, or folded stacks for a
/// flamegraph, function names or `file:line` per frame.
pub const Output = union(enum) {
    top: usize,
    folded: Folded,

    pub const Folded = struct {
        lines: bool = false,
        /// Off: frames as hex addresses, for `addr2line` later, or a
        /// test that cannot afford the debug info scan.
        symbolize: bool = true,
    };
};

const State = struct {
    ring: []usize,
    next: std.atomic.Value(usize) = .init(0),
    samples: std.atomic.Value(usize) = .init(0),
    dropped: std.atomic.Value(usize) = .init(0),
    /// Handlers between their first and last touch of the ring, so
    /// `stop` can wait for them before reading it.
    inflight: std.atomic.Value(usize) = .init(0),
    armed: std.atomic.Value(bool) = .init(false),
    allocator: Allocator,
    timers: []Timer,
    opts: Options,
    started_ms: i64,
};

var state: ?*State = null;

// ---------------------------------------------------------------------------
// The handler
// ---------------------------------------------------------------------------

fn onProf(
    _: std.posix.SIG,
    _: *const std.posix.siginfo_t,
    ctx_ptr: ?*anyopaque,
) callconv(.c) void {
    const st = state orelse return;

    _ = st.inflight.fetchAdd(1, .acquire);
    defer _ = st.inflight.fetchSub(1, .release);
    if (!st.armed.load(.acquire)) return;

    const ctx = std.debug.cpu_context.fromPosixSignalContext(ctx_ptr) orelse
        return;

    var frames: [max_frames]usize = undefined;
    frames[0] = ctx.getPc();
    var n: usize = 1;

    // The worker's thread-local, read directly: the accessor is not
    // for signal context, this plain load is. Its running fiber gives
    // the exact stack; off a fiber, the stack is taken to run up to
    // 16 MiB above the interrupted stack pointer.
    const worker = Worker.current_do_not_use_directly;
    var tag: usize = if (worker) |w| w.id else std.math.maxInt(u8);
    const sp = stackPointer(&ctx);
    var stack_lo: usize = sp;
    var stack_hi: usize = sp +| (16 << 20);
    if (worker) |w| if (w.current_fiber) |f| {
        const info = f.coro.context.stack_info;
        stack_lo = @intFromPtr(info.allocation_ptr);
        stack_hi = stack_lo + info.allocation_len;
        tag |= on_fiber;
    };

    // A frame holds [saved fp, return address]; the chain climbs the
    // stack, so each fp is above the last and inside the stack. Code
    // without frame pointers (libc, the vDSO) leaves rbp pointing at
    // anything; the bounds keep the walk off it, so such a sample is
    // its pc alone rather than a fault in a signal handler.
    var fp = ctx.getFp();
    var lo = @max(sp, stack_lo);
    while (n < max_frames and fp % @alignOf(usize) == 0 and fp >= lo and
        fp + 2 * @sizeOf(usize) <= stack_hi)
    {
        const slot: [*]const usize = @ptrFromInt(fp);
        const ret = slot[1];
        if (ret == 0) break;
        // The return address points past the call; one back is inside
        // the calling instruction, which symbolizes to the call site.
        frames[n] = ret - 1;
        n += 1;
        lo = fp + @sizeOf(usize);
        fp = slot[0];
    }
    const len = header + n;
    const base = st.next.fetchAdd(len, .monotonic);
    if (base + len > st.ring.len) {
        _ = st.dropped.fetchAdd(1, .monotonic);
        return;
    }
    st.ring[base] = n;
    st.ring[base + 1] = tag;
    @memcpy(st.ring[base + header ..][0..n], frames[0..n]);
    _ = st.samples.fetchAdd(1, .monotonic);
}

/// Whether `pc` is the instruction after a syscall instruction: the
/// two bytes before it are `syscall` (0f 05) on x86_64, the four bytes
/// `svc #0` on aarch64. `pc` was just executed, so its code is mapped.
fn afterSyscall(pc: usize) bool {
    switch (builtin.cpu.arch) {
        .x86_64 => {
            if (pc < 2) return false;
            const b: [*]const u8 = @ptrFromInt(pc - 2);
            return b[0] == 0x0f and b[1] == 0x05;
        },
        .aarch64 => {
            if (pc < 4 or pc % 4 != 0) return false;
            const w: *const u32 = @ptrFromInt(pc - 4);
            return w.* == 0xd4000001;
        },
        else => return false,
    }
}

/// The interrupted stack pointer, 0 on an architecture the context
/// does not expose it for (then only the fiber bounds apply).
fn stackPointer(ctx: *const std.debug.cpu_context.Native) usize {
    return switch (builtin.cpu.arch) {
        .x86_64 => @intCast(ctx.gprs.get(.rsp)),
        .aarch64 => @intCast(ctx.sp),
        else => 0,
    };
}

// ---------------------------------------------------------------------------
// Timers: per-thread CPU clocks on Linux, the process timer elsewhere
// ---------------------------------------------------------------------------

const Timer = if (builtin.os.tag == .linux) LinuxTimer else ProcessTimer;

const LinuxTimer = struct {
    id: timer_t = null,

    const timer_t = ?*anyopaque;
    // glibc's struct sigevent: sigval, signo, notify, then a union whose
    // first member is the thread id for SIGEV_THREAD_ID; 64 bytes.
    const sigevent = extern struct {
        value: usize,
        signo: c_int,
        notify: c_int,
        tid: c_int,
        pad: [11]c_int = @splat(0),
    };
    const SIGEV_THREAD_ID: c_int = 4;
    const itimerspec = extern struct {
        interval: std.os.linux.timespec,
        value: std.os.linux.timespec,
    };
    extern "c" fn timer_create(clk: c_int, ev: *sigevent, id: *timer_t) c_int;
    extern "c" fn timer_settime(
        id: timer_t,
        flags: c_int,
        new: *const itimerspec,
        old: ?*itimerspec,
    ) c_int;
    extern "c" fn timer_delete(id: timer_t) c_int;

    /// Arm a timer on `tid`'s CPU clock (user + system time), firing
    /// SIGPROF at that thread. The clock id is the kernel's encoding of
    /// "thread tid, CPUCLOCK_PROF", what pthread_getcpuclockid builds.
    fn arm(self: *LinuxTimer, tid: i32, hz: u32) !void {
        const clk: c_int = @bitCast((@as(u32, @bitCast(~tid)) << 3) | 4);
        var ev: sigevent = .{
            .value = 0,
            .signo = @intFromEnum(std.posix.SIG.PROF),
            .notify = SIGEV_THREAD_ID,
            .tid = tid,
        };
        if (timer_create(clk, &ev, &self.id) != 0) {
            std.log.warn("profile: timer_create for tid {d}: {s}", .{
                tid, @tagName(std.posix.errno(-1)),
            });
            return error.TimerCreate;
        }
        const period = interval(hz);
        const spec: itimerspec = .{ .interval = period, .value = period };
        if (timer_settime(self.id, 0, &spec, null) != 0) {
            _ = timer_delete(self.id);
            self.id = null;
            return error.TimerSet;
        }
    }

    fn disarm(self: *LinuxTimer) void {
        if (self.id) |id| _ = timer_delete(id);
        self.id = null;
    }

    fn interval(hz: u32) std.os.linux.timespec {
        const ns: i64 = @intCast(std.time.ns_per_s / @as(u64, hz));
        return .{ .sec = @intCast(@divTrunc(ns, std.time.ns_per_s)), .nsec = @intCast(@mod(ns, std.time.ns_per_s)) };
    }
};

/// One ITIMER_PROF for the process: the kernel picks the running thread.
const ProcessTimer = struct {
    const itimerval = extern struct {
        interval: std.c.timeval,
        value: std.c.timeval,
    };
    const ITIMER_PROF: c_int = 2;
    extern "c" fn setitimer(
        which: c_int,
        new: *const itimerval,
        old: ?*itimerval,
    ) c_int;

    fn arm(_: *ProcessTimer, _: i32, hz: u32) !void {
        const us: i64 = @intCast(std.time.us_per_s / @as(u64, hz));
        const tv: std.c.timeval = .{ .sec = 0, .usec = @intCast(us) };
        const val: itimerval = .{ .interval = tv, .value = tv };
        if (setitimer(ITIMER_PROF, &val, null) != 0) return error.TimerSet;
    }

    fn disarm(_: *ProcessTimer) void {
        const zero: std.c.timeval = .{ .sec = 0, .usec = 0 };
        const val: itimerval = .{ .interval = zero, .value = zero };
        _ = setitimer(ITIMER_PROF, &val, null);
    }
};

// ---------------------------------------------------------------------------
// Start / stop
// ---------------------------------------------------------------------------

pub const StartError = error{ AlreadyRunning, TimerCreate, TimerSet } ||
    Allocator.Error;

/// Install the handler and arm the timers. The ring holds `secs` of
/// samples at `hz` on every worker with an average stack; a fuller
/// stack or a longer run drops the excess, counted in the report.
pub fn start(allocator: Allocator, opts: Options) StartError!void {
    if (state != null) return error.AlreadyRunning;

    const workers = _rt.runtime().workers;
    const per_sample = header + 24;
    const words = @as(usize, opts.secs) * opts.hz * workers.len * per_sample;

    const ring = try allocator.alloc(usize, @min(words, 64 << 20));
    errdefer allocator.free(ring);

    const timers = try allocator.alloc(Timer, if (Timer == LinuxTimer)
        workers.len
    else
        1);
    errdefer allocator.free(timers);
    for (timers) |*t| t.* = .{};

    const st = try allocator.create(State);
    errdefer allocator.destroy(st);
    st.* = .{
        .ring = ring,
        .allocator = allocator,
        .timers = timers,
        .opts = opts,
        .started_ms = timestampNow(.monotonic, .millis),
    };

    var sa: std.posix.Sigaction = .{
        .handler = .{ .sigaction = &onProf },
        .mask = std.posix.sigemptyset(),
        .flags = std.posix.SA.SIGINFO | std.posix.SA.RESTART,
    };
    std.posix.sigaction(std.posix.SIG.PROF, &sa, null);
    state = st;
    st.armed.store(true, .release);
    errdefer {
        for (timers) |*t| t.disarm();
        state = null;
    }
    if (Timer == LinuxTimer) {
        // A worker whose thread has not started yet has no tid and
        // nothing to sample.
        for (workers, timers) |*w, *t| {
            if (w.tid != 0) try t.arm(w.tid, opts.hz);
        }
    } else {
        try timers[0].arm(0, opts.hz);
    }
}

pub fn running() bool {
    return state != null;
}

/// Disarm, restore the handler, symbolize, and write the profile to
/// `out` in the chosen form. Frees everything `start` allocated.
pub fn stop(out: *std.Io.Writer, output: Output) !Report {
    const st = state orelse return error.NotRunning;
    st.armed.store(false, .release);
    for (st.timers) |*t| t.disarm();

    // A signal a deleted timer already queued is still delivered, and
    // the default disposition of SIGPROF ends the process. Ignore it
    // from here on rather than put that default back.
    var ignore: std.posix.Sigaction = .{
        .handler = .{ .handler = std.posix.SIG.IGN },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.PROF, &ignore, null);
    // A handler that took the signal before the disarm may still be
    // writing; it finishes within microseconds.
    while (st.inflight.load(.acquire) != 0) std.Thread.yield() catch {};
    state = null;
    defer {
        st.allocator.free(st.ring);
        st.allocator.free(st.timers);
        st.allocator.destroy(st);
    }
    const report: Report = .{
        .samples = st.samples.load(.monotonic),
        .dropped = st.dropped.load(.monotonic),
        .stacks = 0,
    };
    const used = @min(st.next.load(.monotonic), st.ring.len);
    const ring = st.ring[0..used];
    const secs_f = @as(f64, @floatFromInt(timestampNow(.monotonic, .millis) -
        st.started_ms)) / 1000.0;

    var arena = std.heap.ArenaAllocator.init(st.allocator);
    defer arena.deinit();
    var names: Names = .{ .a = arena.allocator() };
    const t_sym = timestampNow(.monotonic, .millis);
    defer std.log.info("profile: symbolized {d} addresses in {d} ms " ++
        "(first {d} ms)", .{
        names.cache.count(), timestampNow(.monotonic, .millis) - t_sym,
        names.first_ms,
    });
    switch (output) {
        .top => |n| {
            const fibers = onFiber(ring);
            try out.print("profile cpu: {d} samples, {d} Hz x {d} workers, " ++
                "{d:.1} s, dropped {d}\n", .{
                report.samples, st.opts.hz, st.timers.len, secs_f,
                report.dropped,
            });
            try out.print("{d} on fibers, {d} in the runtime loop\n", .{
                fibers, report.samples - fibers,
            });
            try writeTop(st.allocator, &names, ring, n, out);
            return report;
        },
        .folded => |f| {
            names.lines = f.lines;
            names.names = f.symbolize;
            var stacks = try fold(st.allocator, ring);
            defer stacks.deinit(st.allocator);
            try writeFolded(&names, &stacks, out);
            return .{
                .samples = report.samples,
                .dropped = report.dropped,
                .stacks = stacks.count(),
            };
        },
    }
}

// ---------------------------------------------------------------------------
// Folding and symbolizing
// ---------------------------------------------------------------------------

const Stacks = std.HashMapUnmanaged(
    []const usize,
    usize,
    StackCtx,
    std.hash_map.default_max_load_percentage,
);

const StackCtx = struct {
    pub fn hash(_: StackCtx, k: []const usize) u64 {
        return std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(k));
    }
    pub fn eql(_: StackCtx, a: []const usize, b: []const usize) bool {
        return std.mem.eql(usize, a, b);
    }
};

/// Samples taken on a fiber; the rest were the worker's own loop:
/// scheduling, polling, the spin before a park.
fn onFiber(ring: []const usize) usize {
    var count: usize = 0;
    var i: usize = 0;
    while (i + header <= ring.len) {
        if (ring[i + 1] & on_fiber != 0) count += 1;
        i += header + ring[i];
    }
    return count;
}

/// Count identical stacks. The keys point into `ring`.
fn fold(allocator: Allocator, ring: []const usize) !Stacks {
    var stacks: Stacks = .empty;
    errdefer stacks.deinit(allocator);
    var i: usize = 0;
    while (i + header <= ring.len) {
        const n = ring[i];
        const frames = ring[i + header ..][0..n];
        i += header + n;
        const gop = try stacks.getOrPut(allocator, frames);
        if (!gop.found_existing) gop.value_ptr.* = 0;
        gop.value_ptr.* += 1;
    }
    return stacks;
}

fn writeFolded(names: *Names, stacks: *Stacks, out: *std.Io.Writer) !void {
    var it = stacks.iterator();
    while (it.next()) |e| {
        const frames = e.key_ptr.*;
        // Root first: the leaf is frames[0].
        var i = frames.len;
        while (i > 0) {
            i -= 1;
            try out.writeAll(try names.get(frames[i]));
            try out.writeByte(if (i == 0) ' ' else ';');
        }
        try out.print("{d}\n", .{e.value_ptr.*});
    }
}

/// pprof's `top`: per function, `flat` is the samples whose leaf is in
/// it, `cum` the samples it appears in at all (once per sample, so a
/// recursive function is not counted twice).
fn writeTop(
    allocator: Allocator,
    names: *Names,
    ring: []const usize,
    top_n: usize,
    out: *std.Io.Writer,
) !void {
    const Row = struct {
        name: []const u8,
        flat: usize,
        cum: usize,
        seen: usize,
    };
    var rows: std.StringHashMapUnmanaged(Row) = .empty;
    defer rows.deinit(allocator);
    var total: usize = 0;
    var i: usize = 0;
    var sample: usize = 0;
    while (i + header <= ring.len) : (sample += 1) {
        const n = ring[i];
        const frames = ring[i + header ..][0..n];
        i += header + n;
        total += 1;
        for (frames, 0..) |addr, depth| {
            const name = try names.get(addr);
            const gop = try rows.getOrPut(allocator, name);
            if (!gop.found_existing) {
                gop.value_ptr.* = .{ .name = name, .flat = 0, .cum = 0, .seen = std.math.maxInt(usize) };
            }
            const r = gop.value_ptr;
            if (depth == 0) r.flat += 1;
            if (r.seen != sample) {
                r.seen = sample;
                r.cum += 1;
            }
        }
    }
    const sorted = try allocator.alloc(Row, rows.count());
    defer allocator.free(sorted);
    var it = rows.valueIterator();
    var k: usize = 0;
    while (it.next()) |r| : (k += 1) sorted[k] = r.*;
    std.mem.sort(Row, sorted, {}, struct {
        fn lt(_: void, a: Row, b: Row) bool {
            if (a.flat != b.flat) return a.flat > b.flat;
            return a.cum > b.cum;
        }
    }.lt);
    try out.writeAll("     flat  flat%   sum%      cum   cum%\n");
    const denom: f64 = @floatFromInt(@max(total, 1));
    var sum: usize = 0;
    for (sorted[0..@min(top_n, sorted.len)]) |r| {
        sum += r.flat;
        try out.print("{d:>9} {d:>5.1}% {d:>5.1}% {d:>8} {d:>5.1}%  {s}\n", .{
            r.flat,
            100.0 * @as(f64, @floatFromInt(r.flat)) / denom,
            100.0 * @as(f64, @floatFromInt(sum)) / denom,
            r.cum,
            100.0 * @as(f64, @floatFromInt(r.cum)) / denom,
            r.name,
        });
    }
}

/// Address to name, each address resolved once. The names live in the
/// arena `a`. The debug info here has no inline expansion, so a sample
/// in an inlined function counts for the function it was inlined into.
const Names = struct {
    a: Allocator,
    lines: bool = false,
    /// Off: hex addresses instead of names.
    names: bool = true,
    cache: std.AutoHashMapUnmanaged(usize, []const u8) = .empty,
    /// How long the first lookup took: the debug info scan.
    first_ms: i64 = 0,
    si: ??*std.debug.SelfInfo = null,

    fn get(self: *Names, address: usize) ![]const u8 {
        const gop = try self.cache.getOrPut(self.a, address);
        if (!gop.found_existing) gop.value_ptr.* = try self.symbolize(address);
        return gop.value_ptr.*;
    }

    fn symbolize(self: *Names, address: usize) ![]const u8 {
        if (!self.names) {
            return std.fmt.allocPrint(self.a, "0x{x}", .{address});
        }
        // A sample taken while the thread was in the kernel is delivered
        // on the way back to user space, at the instruction after the
        // syscall: the kernel time of that syscall lands there. Name it
        // for what it is rather than for the errno check that follows.
        const sym = try self.lookup(address);
        if (afterSyscall(address)) {
            return std.fmt.allocPrint(self.a, "syscall ({s})", .{sym.file});
        }
        if (self.lines and sym.line != 0) {
            return std.fmt.allocPrint(self.a, "{s} ({s}:{d})", .{
                sym.name, sym.file, sym.line,
            });
        }
        return std.fmt.allocPrint(self.a, "{s} ({s})", .{ sym.name, sym.file });
    }

    const Sym = struct { name: []const u8, file: []const u8, line: u64 };

    /// The function and file at `address`; the address in hex as the
    /// name when the debug info has none.
    fn lookup(self: *Names, address: usize) !Sym {
        const hex = try std.fmt.allocPrint(self.a, "0x{x}", .{address});
        if (self.si == null) self.si = std.debug.getSelfDebugInfo() catch null;
        const info = self.si.? orelse
            return .{ .name = hex, .file = "?", .line = 0 };
        var syms: std.ArrayList(std.debug.Symbol) = .empty;
        const io = _ctx.g().io;
        const t0 = timestampNow(.monotonic, .millis);
        info.getSymbols(io, self.a, self.a, address, false, &syms) catch
            return .{ .name = hex, .file = "?", .line = 0 };
        if (self.cache.count() == 1) {
            self.first_ms = timestampNow(.monotonic, .millis) - t0;
        }
        if (syms.items.len == 0) {
            return .{ .name = hex, .file = "?", .line = 0 };
        }
        const sym = syms.items[0];
        // The DWARF name is unqualified (`next`, `lock`); the file it
        // is in says which one.
        const file: []const u8 = if (sym.source_location) |loc|
            std.fs.path.basename(loc.file_name)
        else if (sym.compile_unit_name) |cu|
            std.fs.path.basename(cu)
        else
            "?";
        return .{
            .name = sym.name orelse hex,
            .file = file,
            .line = if (sym.source_location) |loc| loc.line else 0,
        };
    }
};
