//! Per-statement context: the record of a statement's path through the
//! system, and the latency-bucketed traces kept for the admin port.
//!
//! A statement creates a `Ctx`, attaches it to its fiber, and any code
//! it calls stamps the moment it was reached with
//! `fiber.ctx().record("loaded {d} tables", .{n})`. The records are a
//! flat timeline, not nested spans: read top to bottom they show where
//! the time went. `ctx()` is never null. Code running off a fiber, or
//! on a fiber with no statement, gets the background context, whose
//! `record` does nothing, so library code never branches on it.
//!
//! When the statement finishes, `detach` offers the context to the
//! sampler: one ring of the newest few traces per latency bucket (at
//! least 1 ms, 10 ms, ...), the shape of Go's net/trace. A statement
//! goes into the highest bucket it qualifies for, so the slow ones are
//! not pushed out by the merely slow. `writeTraces` prints them for
//! the admin command `traces [min_ms]`.
//!
//! Memory: a context owns one arena over the runtime allocator holding
//! the name, the formatted labels and the record list. Keeping a trace
//! moves the arena into the ring, so nothing is copied at the end of a
//! statement, and evicting a trace frees it whole.

const std = @import("std");
const _rt = @import("runtime.zig");
const _clock = @import("utils").coarse_clock;
const _date = @import("utils").date;
const Spinlock = @import("spinlock.zig").Spinlock;

const String = @import("utils").String;

/// Statement text kept per context. A bulk insert can carry megabytes
/// of values; the head is what identifies it in a trace.
const name_max = 512;

/// Bucket thresholds, in microseconds, and how many traces each keeps.
const thresholds_us = [_]u64{
    1_000,   10_000,  20_000,    50_000,
    100_000, 500_000, 1_000_000, 10_000_000,
};
const per_bucket = 8;

pub const Record = struct {
    label: String,
    /// Microseconds since the context started.
    at_us: u64,
};

pub const Ctx = struct {
    arena: std.heap.ArenaAllocator,
    name: String,
    /// Realtime, read once, so a trace can be placed on an absolute
    /// timeline; durations use the monotonic clock.
    start_unix_us: i64,
    start_mono_us: i64,
    records: std.ArrayList(Record) = .empty,
    /// Joined helper fibers record into the same list from other
    /// workers; the arena is not thread safe either, so formatting a
    /// label happens under this lock too. A few dozen records per
    /// statement, so the lock is never contended for long.
    lock: Spinlock = .{},

    /// Copies the head of `name` (the statement text). Needs a running
    /// runtime, whose allocator backs the arena.
    pub fn init(name: String) Ctx {
        var arena = std.heap.ArenaAllocator.init(_rt.runtime().allocator);
        const head = name[0..@min(name.len, name_max)];
        const owned = arena.allocator().dupe(u8, head) catch "";
        return .{
            .arena = arena,
            .name = owned,
            .start_unix_us = _clock.timestampNow(.realtime, .micros),
            .start_mono_us = _clock.timestampNow(.monotonic, .micros),
        };
    }

    /// Makes this the current fiber's context. Call once the Ctx has
    /// its final address; the fiber keeps a pointer to it.
    pub fn attach(self: *Ctx) void {
        const f = _rt.currentFiber();
        std.debug.assert(f.ctx == null);
        f.ctx = self;
    }

    /// Ends the statement: clears the fiber's slot, and either hands
    /// the name and records to the sampler or frees them. Must run on
    /// the fiber that attached.
    pub fn detach(self: *Ctx) void {
        const f = _rt.currentFiber();
        std.debug.assert(f.ctx == self);
        f.ctx = null;
        if (!offer(self, self.elapsedUs())) self.arena.deinit();
    }

    /// Stamps the moment with a label. A label without args is a
    /// comptime string and is stored as is; with args it is printed
    /// into the arena. Out of memory drops the record; a statement
    /// never fails over its trace.
    pub fn record(self: *Ctx, comptime fmt: String, args: anytype) void {
        if (self == &background) return;
        const at_us = self.elapsedUs();

        self.lock.lock();
        defer self.lock.unlock();

        const arloc = self.arena.allocator();
        const label = if (args.len == 0)
            fmt
        else
            std.fmt.allocPrint(arloc, fmt, args) catch return;

        self.records.append(
            arloc,
            .{ .label = label, .at_us = at_us },
        ) catch return;
    }

    pub fn elapsedUs(self: *const Ctx) u64 {
        const now = _clock.timestampNow(.monotonic, .micros);
        return @intCast(@max(0, now - self.start_mono_us));
    }
};

/// The context of code with no statement. `record` is a no-op on it
/// and it is never attached, so its arena is never touched.
var background: Ctx = .{
    .arena = undefined,
    .name = "",
    .start_unix_us = 0,
    .start_mono_us = 0,
};

/// The current fiber's context, or the background context off a fiber
/// or on a fiber with no statement. Never null.
pub fn current() *Ctx {
    const f = _rt.currentFiberOrNull() orelse return &background;
    return f.ctx orelse &background;
}

/// A finished statement kept by the sampler. Owns its arena, which
/// holds the name, the labels and the records.
pub const Trace = struct {
    arena: std.heap.ArenaAllocator,
    name: String,
    start_unix_us: i64,
    elapsed_us: u64,
    records: []const Record,
};

/// A ring of the newest traces at or above one threshold.
const Bucket = struct {
    traces: [per_bucket]?Trace = .{null} ** per_bucket,
    /// The slot the next trace overwrites: the oldest once full.
    next: usize = 0,
};

var buckets: [thresholds_us.len]Bucket = @splat(.{});
var buckets_lock: Spinlock = .{};

/// The highest bucket `elapsed_us` qualifies for, or null below the
/// lowest threshold.
fn bucketFor(elapsed_us: u64) ?usize {
    var i = thresholds_us.len;
    while (i > 0) {
        i -= 1;
        if (elapsed_us >= thresholds_us[i]) return i;
    }
    return null;
}

/// Moves a finished statement into its bucket's ring, taking over the
/// arena. Returns false, without locking, when the statement is too
/// fast to keep: the common path, and the caller frees the context.
fn offer(ctx: *Ctx, elapsed_us: u64) bool {
    const b = bucketFor(elapsed_us) orelse return false;
    const trace: Trace = .{
        .arena = ctx.arena,
        .name = ctx.name,
        .start_unix_us = ctx.start_unix_us,
        .elapsed_us = elapsed_us,
        .records = ctx.records.items,
    };

    var evicted: ?Trace = null;
    {
        buckets_lock.lock();
        defer buckets_lock.unlock();

        const bucket = &buckets[b];
        evicted = bucket.traces[bucket.next];
        bucket.traces[bucket.next] = trace;
        bucket.next = (bucket.next + 1) % per_bucket;
    }
    if (evicted) |*old| old.arena.deinit();
    return true;
}

/// Prints every kept trace that took at least `min_ms`, slowest bucket
/// first, in no particular order within a bucket: the statement line,
/// then its records indented with their offsets from the start.
pub fn writeTraces(w: *std.Io.Writer, min_ms: u64) !void {
    buckets_lock.lock();
    defer buckets_lock.unlock();

    var b = thresholds_us.len;
    while (b > 0) {
        b -= 1;
        const bucket = &buckets[b];
        var shown: usize = 0;
        for (bucket.traces) |maybe| {
            const trace = maybe orelse continue;
            if (trace.elapsed_us < min_ms * 1000) continue;
            if (shown == 0) {
                try w.print(">= {d}ms\n", .{thresholds_us[b] / 1000});
            }
            shown += 1;
            try writeTrace(w, &trace);
        }
    }
}

fn writeTrace(w: *std.Io.Writer, trace: *const Trace) !void {
    try w.writeAll("  ");
    try _date.formatTimestampUtc(w, trace.start_unix_us);
    try w.print("  {d}.{d:0>3}ms  {s}\n", .{
        trace.elapsed_us / 1000, trace.elapsed_us % 1000, trace.name,
    });
    for (trace.records) |r| {
        try w.print("      +{d}us  {s}\n", .{ r.at_us, r.label });
    }
}
