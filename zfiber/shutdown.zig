//! Shutdown — a one-shot "shutdown has begun" signal that is both pollable
//! and blockable, with optional SIGINT/SIGTERM wiring.
//!
//! One instance lives at process scope (the raft Node owns it): fatal paths
//! call `start()`, hot loops poll `hasStarted()`, and fibers that must sleep
//! until shutdown and then act (listener watchers breaking a parked
//! accept()) block in `wait()`.
//!
//! Signal path: a POSIX handler may only use async-signal-safe operations —
//! no spinlocks, no rt.wake (pushes onto growable spinlocked worker rings),
//! no allocation — so it cannot call `start()` directly. The handler just
//! stores a process-wide atomic flag; a watcher fiber polls it and fires
//! `start()` from fiber context.

const std = @import("std");
const builtin = @import("builtin");
const rt = @import("runtime.zig");
const channel = @import("channel.zig");
const sleepMs = @import("deadlines.zig").sleepMs;

/// How often the watcher checks the handler's flag. Bounds both Ctrl-C
/// latency and how long `deinit`'s join can take.
const poll_ms: u64 = 300;

pub const Shutdown = struct {
    started: std.atomic.Value(bool) = .init(false),
    /// Nothing is ever sent — `wait` parks on receive; `start`'s close wakes
    /// every waiter at once.
    ch: channel.Channel(void),
    watcher: ?rt.JoinHandle(void) = null,

    pub fn init(allocator: std.mem.Allocator) !Shutdown {
        // Capacity 1 (Channel.init asserts > 0); the channel is only ever
        // closed, never sent to — and `void` items are zero bytes anyway.
        return .{ .ch = try channel.Channel(void).initAlloc(allocator, 1) };
    }

    /// Start first (idempotent) so the watcher's next poll sees it and
    /// exits, then join it — nothing stays parked on freed state.
    pub fn deinit(self: *Shutdown) void {
        self.start();
        if (self.watcher) |*h| h.join();
        self.ch.deinit();
    }

    /// Begin shutdown: flip `hasStarted` and wake every `wait`er. Idempotent.
    pub fn start(self: *Shutdown) void {
        self.started.store(true, .release);
        self.ch.close(.immediate);
    }

    pub fn hasStarted(self: *const Shutdown) bool {
        return self.started.load(.acquire);
    }

    /// Block until `start` runs; returns immediately if it already has. The
    /// blocking complement of polling `hasStarted`.
    pub fn wait(self: *Shutdown) void {
        self.ch.receive() catch {}; // ChannelClosed == start() ran
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
