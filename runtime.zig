//! The egres ↔ zio seam for fiber placement.
//!
//! Callers spawn fibers via `runtime.spawnOn(group, sharing, func, args)`,
//! labelling the fiber by *what kind of work* it is (raft / query /
//! background) and how it expects to share a thread (`.exclusive` /
//! `.shared`). The mapping from group → placement lives here and only
//! here.
//!
//! Today: every spawn lands on a single shared zio.Runtime with
//! `total_threads` executor threads. The `group` argument is recorded
//! intent but doesn't yet pin the fiber to a thread subset — zio
//! schedules across all threads.
//!
//! Planned: partition the `total_threads` into per-group ranges and
//! route `spawnOn(.raft, ...)` to a thread in the raft range. This
//! depends on a new `zio.spawnOn(thread_idx, ...)` API upstream.
//!
//! Future enforcement: track which threads in a group already hold an
//! `.exclusive` fiber — so e.g. raft.mainLoop and raft.applyLoop can be
//! asserted onto distinct threads. Best-effort when the group has only
//! one thread.

const std = @import("std");
const zio = @import("zio");

/// Thread-group label. Captures *what kind of work* a fiber does so
/// the seam can later place it on the right thread subset.
pub const Runtime = enum(u2) {
    raft = 0,
    query = 1,
    background = 2,
};

/// Process-wide configuration for the single shared runtime.
pub const Config = struct {
    /// Total executor threads in the singleton zio.Runtime.
    /// 0 = auto-detect from CPU count. Cap is `zio.Executor.max_executors`
    /// (255 with our patch — was 64 in stock zio).
    total_threads: u8 = 0,
};

var g_rt: ?*zio.Runtime = null;

/// Initialize the process-wide runtime. Call exactly once, before any
/// `spawnOn` / `Group` use. Panics if called twice.
pub fn init(allocator: std.mem.Allocator, cfg: Config) !void {
    if (g_rt != null) @panic("runtime already initialized");
    // Resolve total_threads ourselves so the chosen value is visible in
    // logs (zio's `.auto` does the same computation internally but
    // doesn't surface it). Cap by zio's `Executor.max_executors` (255
    // with our patch). Fall back to 2 on getCpuCount failure so
    // single-CPU-detection failures still leave us multi-threaded.
    const max = 255; // mirrors zio.Executor.max_executors in our patches/v0.11
    const n: u8 = if (cfg.total_threads != 0)
        cfg.total_threads
    else
        @intCast(@min(max, std.Thread.getCpuCount() catch 2));
    std.log.info("runtime: starting with {d} executor threads", .{n});
    g_rt = try zio.Runtime.init(allocator, .{ .executors = .exact(n) });
}

pub fn deinit() void {
    const rt = g_rt orelse return;
    rt.deinit();
    g_rt = null;
}

/// Access the underlying zio.Runtime. Reserved for entry-point code
/// (e.g. bootstrap fiber join) and not intended for fiber-spawn use —
/// callers should go through `spawnOn` so the seam can later inject
/// placement.
pub fn underlying() *zio.Runtime {
    return g_rt orelse @panic("runtime not initialized");
}

/// Thread-sharing intent for a `spawnOn` call.
///
/// - `.exclusive`: this fiber is long-running and should not share its
///   thread with another `.exclusive` fiber in the same group. Used
///   for loops that would block their thread-mate from making progress
///   (raft.mainLoop, raft.applyLoop). Best-effort when the group has
///   only one thread.
/// - `.shared`: short-lived or low-duty-cycle work that can co-schedule
///   freely with other fibers on any thread in the group.
///
/// Not enforced today — recorded intent for the future
/// `zio.spawnOn(thread_idx)` placement work (see file header).
pub const Sharing = enum { exclusive, shared };

fn ReturnOf(comptime func: anytype) type {
    return @typeInfo(@TypeOf(func)).@"fn".return_type.?;
}

/// Spawn a fiber on the shared runtime, labelled by `group` and
/// `sharing`. Today both labels are recorded intent; placement is left
/// to zio's scheduler across all threads.
pub fn spawnOn(
    group: Runtime,
    sharing: Sharing,
    comptime func: anytype,
    args: std.meta.ArgsTuple(@TypeOf(func)),
) !zio.JoinHandle(ReturnOf(func)) {
    _ = group; // not enforced yet — see file header
    _ = sharing; // not enforced yet — see Sharing docs
    return underlying().spawn(func, args);
}

/// Collective fan-out + wait/cancel over fibers that all run on the
/// same `Runtime` group and all return the same type `T`. Wraps the
/// pattern `var handles: ArrayList(JoinHandle(T)); for {...} spawn; for
/// {...} join` so the call site stays compact and the cancel path is
/// uniform.
///
/// Use cases:
///   - long-running fiber bundles (raft mainLoop + applyLoop + …)
///   - sub-query fan-out (`Group(QueryResult)`, parent reads each
///     result via `handles.items[i].join()` after `wait()`)
///
/// `wait()` doesn't return the results because Group(void) is the
/// common case; callers needing typed results read them off the
/// handles directly (a second `join()` on a finished handle returns
/// the cached value, see zio JoinHandle docs).
pub fn Group(comptime T: type) type {
    return struct {
        const Self = @This();

        rt: Runtime,
        handles: std.ArrayList(zio.JoinHandle(T)),
        /// Populated by `collect()`. Lives until `deinit()`.
        results: std.ArrayList(T),
        alloc: std.mem.Allocator,

        pub fn init(alloc: std.mem.Allocator, rt: Runtime) Self {
            return .{ .rt = rt, .handles = .empty, .results = .empty, .alloc = alloc };
        }

        pub fn deinit(self: *Self) void {
            self.handles.deinit(self.alloc);
            self.results.deinit(self.alloc);
        }

        /// Pre-size the internal handle list. Optional; `spawn`
        /// allocates on demand otherwise.
        pub fn ensureTotalCapacity(self: *Self, n: usize) !void {
            try self.handles.ensureTotalCapacity(self.alloc, n);
        }

        /// Spawn a fiber on this group's runtime and register it for
        /// collective wait/cancel. The spawned function's return type
        /// must equal `T`. For typed groups (T ≠ void), also reserves
        /// a result slot so `wait()` is infallible.
        pub fn spawn(
            self: *Self,
            sharing: Sharing,
            comptime func: anytype,
            args: std.meta.ArgsTuple(@TypeOf(func)),
        ) !void {
            const h = try spawnOn(self.rt, sharing, func, args);
            try self.handles.append(self.alloc, h);
            if (comptime T != void) try self.results.append(self.alloc, undefined);
        }

        /// Join every spawned fiber. Return type depends on `T`:
        ///   - `Group(void).wait()`     → `void`        (just synchronize)
        ///   - `Group(T).wait()` (T≠void) → `[]const T` (results in spawn order)
        /// Infallible — result-slot capacity is reserved at `spawn` time.
        /// The returned slice is owned by the Group and freed on `deinit()`.
        /// Safe to call multiple times; joins are cached per handle.
        pub fn wait(self: *Self) WaitResult {
            for (self.handles.items, 0..) |*h, i| {
                const r = h.join();
                if (comptime T != void) self.results.items[i] = r;
            }
            if (comptime T != void) return self.results.items;
        }

        /// Comptime-selected return type for `wait()`.
        const WaitResult = if (T == void) void else []const T;

        /// Request cancellation on every spawned fiber and wait for
        /// each to finish. Serial today (cancel-and-wait per handle);
        /// if a parent ever needs parallel cancel for fail-fast over
        /// many slow tasks, we'd patch zio for a split
        /// "signal-then-wait" API.
        pub fn cancel(self: *Self) void {
            for (self.handles.items) |*h| h.cancel();
        }
    };
}
