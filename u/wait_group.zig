const std = @import("std");
const zio = @import("zio");

/// WaitGroup provides Go-style Add/Done/Wait synchronization for zio coroutines.
///
/// Usage:
///   var wg: WaitGroup = .{};
///   wg.init();  // must be called at final address (no moves after this)
///   wg.add(1);
///   // ... pass &wg to another coroutine/callback ...
///   try wg.wait();  // blocks until counter reaches 0
///
/// The callback side calls wg.done() to decrement the counter.
/// When the counter reaches zero, all coroutines blocked in wait() are unblocked.
///
/// Safe to call done() from any zio runtime or coroutine.
/// Not reusable — create a new WaitGroup for each synchronization point.
pub const WaitGroup = struct {
    counter: std.atomic.Value(i32) = std.atomic.Value(i32).init(0),
    ch_buf: [1]u8 = undefined,
    ch: zio.Channel(u8) = undefined,

    /// Initialize the channel. Must be called after the WaitGroup is at its
    /// final memory address — do not move the struct after calling init().
    pub fn init(self: *WaitGroup) void {
        self.ch = zio.Channel(u8).init(&self.ch_buf);
    }

    /// Add delta to the counter. Typically called before spawning work.
    pub fn add(self: *WaitGroup, delta: i32) void {
        const prev = self.counter.fetchAdd(delta, .acq_rel);
        if (prev + delta == 0) {
            self.ch.close(.graceful);
        }
    }

    /// Decrement the counter by 1. When the counter reaches zero,
    /// all coroutines blocked in wait() are unblocked.
    /// Safe to call from any zio runtime or coroutine.
    pub fn done(self: *WaitGroup) void {
        self.add(-1);
    }

    /// Block the current zio coroutine until the counter reaches zero.
    /// Returns immediately if the counter is already zero.
    /// Multiple coroutines can wait simultaneously.
    pub fn wait(self: *WaitGroup) !void {
        if (self.counter.load(.acquire) == 0) return;
        _ = self.ch.receive() catch return;
    }
};
