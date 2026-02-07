const std = @import("std");

// Queue blocks producers when full and consumers when empty.
// Queue is thread safe.
pub fn Queue(comptime T: type, comptime capacity: usize) type {
    return struct {
        const Self = @This();

        buf: [capacity]T = undefined,
        head: usize = 0,
        len: usize = 0,
        mutex: std.Thread.Mutex = .{},
        not_empty: std.Thread.Condition = .{},
        not_full: std.Thread.Condition = .{},

        pub fn push(q: *Self, item: T) void {
            q.mutex.lock();
            defer q.mutex.unlock();

            while (q.len == capacity) {
                q.not_full.wait(&q.mutex);
            }

            q.buf[(q.head + q.len) % capacity] = item;
            q.len += 1;
            q.not_empty.signal();
        }

        pub fn pop(q: *Self) T {
            q.mutex.lock();
            defer q.mutex.unlock();

            while (q.len == 0) {
                q.not_empty.wait(&q.mutex);
            }

            const item = q.buf[q.head];
            q.head = (q.head + 1) % capacity;
            q.len -= 1;
            q.not_full.signal();
            return item;
        }
    };
}
