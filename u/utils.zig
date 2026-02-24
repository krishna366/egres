const std = @import("std");
const builtin = @import("builtin");
pub const compression = @import("compress.zig");

// testPrint helps emit debugging output from tests without affecting non-test builds.
pub fn testPrint(comptime fmt: []const u8, args: anytype) void {
    if (!builtin.is_test) return;
    // std.debug.print(fmt, args);
    _ = fmt;
    _ = args;
}

/// search finds the first index in [0, items.len) for which pred(ctx, items[i]) is true,
/// assuming the range is partitioned as [false…false, true…true].
/// Returns items.len when the predicate is never true (mirrors Go's sort.Search).
pub fn search(
    comptime T: type,
    items: []const T,
    ctx: anytype,
    comptime pred: fn (@TypeOf(ctx), T) bool,
) usize {
    var lo: usize = 0;
    var hi: usize = items.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (pred(ctx, items[mid])) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    return lo;
}

// Queue blocks producers when full and consumers when empty.
// Queue is thread safe.
pub fn Queue(comptime T: type, comptime capacity: usize) type {
    return struct {
        const Self = @This();
        pub const Error = error{QueueClosed};

        buf: [capacity]T = undefined,
        head: usize = 0,
        len: usize = 0,
        closed: bool = false,
        mutex: std.Thread.Mutex = .{},
        not_empty: std.Thread.Condition = .{},
        not_full: std.Thread.Condition = .{},

        pub fn pushOrClosed(q: *Self, item: T) Error!void {
            q.mutex.lock();
            defer q.mutex.unlock();

            while (q.len == capacity and !q.closed) {
                q.not_full.wait(&q.mutex);
            }
            if (q.closed) return error.QueueClosed;

            q.buf[(q.head + q.len) % capacity] = item;
            q.len += 1;
            q.not_empty.signal();
        }

        pub fn tryPush(q: *Self, item: T) bool {
            q.mutex.lock();
            defer q.mutex.unlock();

            if (q.closed or q.len == capacity) return false;
            q.buf[(q.head + q.len) % capacity] = item;
            q.len += 1;
            q.not_empty.signal();
            return true;
        }

        pub fn popOrNull(q: *Self) ?T {
            q.mutex.lock();
            defer q.mutex.unlock();

            while (q.len == 0 and !q.closed) {
                q.not_empty.wait(&q.mutex);
            }
            if (q.len == 0) return null;

            const item = q.buf[q.head];
            q.head = (q.head + 1) % capacity;
            q.len -= 1;
            q.not_full.signal();
            return item;
        }

        pub fn tryPop(q: *Self) ?T {
            q.mutex.lock();
            defer q.mutex.unlock();

            if (q.len == 0) return null;
            const item = q.buf[q.head];
            q.head = (q.head + 1) % capacity;
            q.len -= 1;
            q.not_full.signal();
            return item;
        }

        pub fn peek(q: *Self) ?T {
            q.mutex.lock();
            defer q.mutex.unlock();
            if (q.len == 0) return null;
            return q.buf[q.head];
        }

        pub fn count(q: *Self) usize {
            q.mutex.lock();
            defer q.mutex.unlock();
            return q.len;
        }

        pub fn isClosed(q: *Self) bool {
            q.mutex.lock();
            defer q.mutex.unlock();
            return q.closed;
        }

        pub fn close(q: *Self) void {
            q.mutex.lock();
            defer q.mutex.unlock();
            if (q.closed) return;
            q.closed = true;
            q.not_empty.broadcast();
            q.not_full.broadcast();
        }
    };
}
