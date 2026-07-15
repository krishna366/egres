//! Shutdown — a one-shot "shutdown has begun" signal that is pollable,
//! blockable, and timed-blockable, with optional SIGINT/SIGTERM wiring.
//!
//! One instance lives at process scope (the raft Node owns it): fatal paths
//! call `start()`, hot loops poll `hasStarted()`, fibers that must sleep
//! until shutdown and then act block in `wait()`, and periodic background
//! loops replace `sleepMs(interval)` with `waitTimeout(interval)` so they
//! keep their cadence but react to shutdown instantly.
//!
//! Built on a `Watermark` that nothing ever advances: waiters park on target
//! 1, which is never reached, so a wait ends only via `close()` (= start) or
//! its own timeout — and the Watermark's timeout/claim machinery is the
//! battle-tested path for exactly that race.
//!
//! Signal path: a POSIX handler may only use async-signal-safe operations —
//! no spinlocks, no rt.wake (pushes onto growable spinlocked worker rings),
//! no allocation — so it cannot call `start()` directly. The handler just
//! stores a process-wide atomic flag; a watcher fiber polls it and fires
//! `start()` from fiber context.

const std = @import("std");
const builtin = @import("builtin");
const rt = @import("runtime.zig");
const watermark = @import("watermark.zig");
const sleepMs = @import("deadlines.zig").sleepMs;

/// How often the signal watcher checks the handler's flag. Bounds both
/// Ctrl-C latency and how long `deinit`'s join can take.
const poll_ms: u64 = 300;

pub const Shutdown = struct {
    started: std.atomic.Value(bool) = .init(false),
    wm: watermark.Watermark,
    watcher: ?rt.JoinHandle(void) = null,

    pub fn init(allocator: std.mem.Allocator) Shutdown {
        return .{ .wm = watermark.Watermark.init(allocator) };
    }

    /// Start first (idempotent) so the watcher's next poll sees it and
    /// exits, then join it — nothing stays parked on freed state.
    pub fn deinit(self: *Shutdown) void {
        self.start();
        if (self.watcher) |*h| h.join();
        self.wm.deinit();
    }

    /// Begin shutdown: flip `hasStarted` and wake every waiter. Idempotent.
    pub fn start(self: *Shutdown) void {
        self.started.store(true, .release);
        self.wm.close();
    }

    pub fn hasStarted(self: *const Shutdown) bool {
        return self.started.load(.acquire);
    }

    /// Block until `start` runs; returns immediately if it already has. The
    /// blocking complement of polling `hasStarted`.
    pub fn wait(self: *Shutdown) void {
        while (!self.hasStarted()) {
            self.wm.waitForUntimed(1) catch |err| switch (err) {
                error.Closed => return,
                error.OutOfMemory => sleepMs(poll_ms), // degrade to polling
            };
        }
    }

    /// Block until shutdown starts or `timeout_ms` elapses: true = shutdown
    /// has started (before or during the wait), false = timed out. The
    /// shutdown-aware replacement for `sleepMs` in periodic loops:
    ///     if (node.shutdown.waitTimeout(interval_ms)) return;
    pub fn waitTimeout(self: *Shutdown, timeout_ms: u64) bool {
        if (self.hasStarted()) return true;
        self.wm.waitFor(1, timeout_ms) catch |err| switch (err) {
            error.Closed => return true,
            error.Timeout => return false,
            error.OutOfMemory => sleepMs(@min(timeout_ms, poll_ms)), // degrade
        };
        // Target 1 is never reached (nothing advances the watermark), so a
        // successful wait can't happen; only the OOM fallback lands here.
        return self.hasStarted();
    }

    /// Install SIGINT/SIGTERM handlers that trigger `start()`. Call once,
    /// from the binary that owns process lifetime, inside the fiber runtime.
    /// POSIX-only; Windows console-ctrl handling is a TODO.
    pub fn install(self: *Shutdown) !void {
        if (builtin.os.tag == .windows) return;
        self.watcher = try rt.spawn(.{}, watchSignal, .{self});

        var sa = std.posix.Sigaction{
            .handler = .{ .handler = signalHandler },
            .mask = std.posix.sigemptyset(),
            .flags = std.posix.SA.RESTART,
        };
        std.posix.sigaction(std.posix.SIG.INT, &sa, null);
        std.posix.sigaction(std.posix.SIG.TERM, &sa, null);
    }

    /// Polls the handler's flag and fires `start()` from fiber context; also
    /// exits once shutdown started by any other path (checked first, so a
    /// pre-fired signal or start is seen without sleeping).
    fn watchSignal(self: *Shutdown) void {
        while (!self.hasStarted()) {
            if (g_signal.load(.acquire)) {
                self.start();
                return;
            }
            sleepMs(poll_ms);
        }
    }
};

/// Process-wide: a signal handler can't carry context, so the flag is a
/// module global no matter how many instances exist (only one is expected
/// to call `install`).
var g_signal: std.atomic.Value(bool) = .init(false);

fn signalHandler(_: std.posix.SIG) callconv(.c) void {
    g_signal.store(true, .release);
}
