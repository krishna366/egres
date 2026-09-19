const std = @import("std");
const fiber = @import("zfiber");
const builtin = @import("builtin");
pub const compression = @import("compress.zig");
pub const hlc = @import("hlc.zig");
pub const bench = @import("bench.zig");
pub const host = @import("host.zig");
pub const date = @import("date.zig");

/// A borrowed byte string: a name, row bytes, an encoded key. The
/// same type as []const u8; it reads better in a slice of them.
pub const String = []const u8;
pub const context = fiber.context;
pub const GuardAllocator = @import("guard_allocator.zig").GuardAllocator;

/// WaitGroup provides Go-style Add/Done/Wait synchronization for fiber coroutines.
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
/// Safe to call done() from any fiber runtime or coroutine.
/// Not reusable — create a new WaitGroup for each synchronization point.
/// TODO: Reconsider if we still need it with zfiber sync structs.
pub const WaitGroup = struct {
    counter: std.atomic.Value(i32) = std.atomic.Value(i32).init(0),
    ch_buf: [1]u8 = undefined,
    ch: fiber.Channel(u8) = undefined,

    pub fn init(self: *WaitGroup) void {
        self.ch = fiber.Channel(u8).init(&self.ch_buf);
    }

    pub fn add(self: *WaitGroup, delta: i32) void {
        const prev = self.counter.fetchAdd(delta, .acq_rel);
        if (prev + delta == 0) {
            self.ch.close(.graceful);
        }
    }

    pub fn done(self: *WaitGroup) void {
        self.add(-1);
    }

    pub fn isDone(self: *WaitGroup) bool {
        return self.counter.load(.acquire) == 0;
    }

    pub fn wait(self: *WaitGroup) !void {
        if (self.counter.load(.acquire) == 0) return;
        _ = self.ch.receive() catch return;
    }
};

/// Empty an ArrayList and keep its buffer. `clearRetainingCapacity`
/// does the same but first memsets the whole slice with `undefined`,
/// a real 0xAA fill in Debug and ReleaseSafe, which showed up on hot
/// resets (a version merge per key on a full-table scan). One place to
/// change if the reset strategy ever changes.
pub fn clearRetainCap(list: anytype) void {
    list.items.len = 0;
}

pub fn testPrint(comptime fmt: String, args: anytype) void {
    if (!builtin.is_test) return;
    // std.debug.print(fmt, args);
    _ = fmt;
    _ = args;
}

/// Rate-limited log: each call site (unique comptime instantiation) logs at most once per interval.
pub fn logEvery(comptime interval_secs: i64, comptime fmt: String, args: anytype) void {
    const S = struct {
        var last: i64 = 0;
    };
    const now = fiber.coarseMs();
    if (now - S.last > interval_secs * 1_000) {
        std.log.debug(fmt, args);
        S.last = now;
    }
}

/// search finds the first index in [0, items.len) for which pred(c, items[i]) is true,
/// assuming the range is partitioned as [false…false, true…true].
/// Returns items.len when the predicate is never true (mirrors Go's sort.Search).
pub fn search(
    comptime T: type,
    items: []const T,
    c: anytype,
    comptime pred: fn (@TypeOf(c), T) bool,
) usize {
    var lo: usize = 0;
    var hi: usize = items.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (pred(c, items[mid])) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    return lo;
}

/// Lock-free, first-error-wins container. Safe to call set() from many threads;
/// only the first error is retained.
pub const SafeError = struct {
    raw: std.atomic.Value(u16) = std.atomic.Value(u16).init(0),

    pub fn set(self: *SafeError, e: anyerror) void {
        _ = self.raw.cmpxchgStrong(0, @intFromError(e), .acq_rel, .monotonic);
    }

    pub fn get(self: *const SafeError) ?anyerror {
        const v = self.raw.load(.acquire);
        if (v == 0) return null;
        return @errorFromInt(v);
    }
};

/// Heap-allocated, ref-counted byte buffer. Used to share payloads across
/// fan-out send paths (e.g. AE / file push) without per-peer memcpy: caller
/// wraps an owned slice in `RefValue.init`, then `incrRef` per peer and
/// hands each ref off to the transport queue. Each writer fiber `decrRef`s
/// after writing — when the count reaches zero, the data slice and the
/// RefValue itself are freed with the original allocator.
pub const RefValue = struct {
    data: []u8,
    allocator: std.mem.Allocator,
    ref: std.atomic.Value(i32),

    pub fn init(allocator: std.mem.Allocator, data: []u8) !*RefValue {
        const v = try allocator.create(RefValue);
        v.* = .{ .data = data, .allocator = allocator, .ref = std.atomic.Value(i32).init(1) };
        return v;
    }

    /// A freed value's count is left at this poison, so a late take or
    /// release on it trips the guards below instead of corrupting the
    /// heap silently. These checks stay on in every build mode: a
    /// refcount bug here is a cross-thread double free.
    const dead: i32 = -0x4000_0000;

    pub fn incrRef(self: *RefValue) *RefValue {
        const prev = self.ref.fetchAdd(1, .monotonic);
        if (prev <= 0) @panic("RefValue.incrRef on a freed value");
        return self;
    }

    pub fn decrRef(self: *RefValue) void {
        const prev = self.ref.fetchSub(1, .acq_rel);
        if (prev <= 0) @panic("RefValue.decrRef below zero (double release)");
        if (prev == 1) {
            self.ref.store(dead, .release);
            self.allocator.free(self.data);
            self.allocator.destroy(self);
        }
    }

    pub fn byteSize(self: *const RefValue) usize {
        return self.data.len;
    }
};

/// Thread-safe wrapper around std.AutoHashMap. All operations are mutex-protected.
/// Init with `.init(allocator)`.
pub fn SafeHashMap(comptime K: type, comptime V: type) type {
    return struct {
        const Self = @This();
        const HashMap = std.AutoHashMap(K, V);

        map: HashMap,
        /// Readers share; only put/remove/getOrPut take it exclusively. A
        /// map on a read hot path (the forest's table map) would otherwise
        /// serialize every lookup behind one mutex.
        mu: fiber.RWMutex(.untracked) = .{},

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{ .map = HashMap.init(allocator) };
        }

        pub fn get(self: *Self, key: K) ?V {
            self.mu.lockShared(@src());
            defer self.mu.unlockShared();
            return self.map.get(key);
        }

        pub fn put(self: *Self, key: K, value: V) !void {
            self.mu.lock(@src());
            defer self.mu.unlock();
            try self.map.put(key, value);
        }

        pub fn remove(self: *Self, key: K) bool {
            self.mu.lock(@src());
            defer self.mu.unlock();
            return self.map.remove(key);
        }

        pub fn fetchRemove(self: *Self, key: K) ?HashMap.KV {
            self.mu.lock(@src());
            defer self.mu.unlock();
            return self.map.fetchRemove(key);
        }

        pub fn contains(self: *Self, key: K) bool {
            self.mu.lockShared(@src());
            defer self.mu.unlockShared();
            return self.map.contains(key);
        }

        /// Insert `value` under `key` if absent; return the stored value —
        /// the existing one when the key was already present (the caller
        /// disposes of its unused `value` then). One atomic step, unlike
        /// contains-then-put. (Not fetchPut: that REPLACES the stored
        /// value, which would hand two racers two different values.)
        pub fn getOrPut(self: *Self, key: K, value: V) !V {
            self.mu.lock(@src());
            defer self.mu.unlock();
            const gop = try self.map.getOrPut(key);
            if (!gop.found_existing) gop.value_ptr.* = value;
            return gop.value_ptr.*;
        }

        /// Unlocked iteration for teardown — caller must guarantee no
        /// concurrent access.
        pub fn valueIterator(self: *Self) HashMap.ValueIterator {
            return self.map.valueIterator();
        }

        /// Iteration under the map lock (shared — readers still proceed,
        /// writers wait), held from `iterator()` until `deinit`. Keep the
        /// body short and never touch this map from inside it (the lock is
        /// not reentrant).
        pub const Iterator = struct {
            owner: *Self,
            inner: HashMap.Iterator,

            pub fn next(self: *Iterator) ?HashMap.Entry {
                return self.inner.next();
            }

            pub fn deinit(self: *Iterator) void {
                self.owner.mu.unlockShared();
            }
        };

        pub fn iterator(self: *Self) Iterator {
            self.mu.lockShared(@src());
            return .{ .owner = self, .inner = self.map.iterator() };
        }

        pub fn count(self: *Self) u32 {
            self.mu.lockShared(@src());
            defer self.mu.unlockShared();
            return self.map.count();
        }

        pub fn countValue(self: *Self, value: V) u32 {
            self.mu.lockShared(@src());
            defer self.mu.unlockShared();
            var n: u32 = 0;
            var it = self.map.valueIterator();
            while (it.next()) |v| {
                if (std.meta.eql(v.*, value)) n += 1;
            }
            return n;
        }

        pub fn deinit(self: *Self) void {
            self.map.deinit();
        }
    };
}

/// Mutex-protected std.ArrayList. Init with `.init(allocator)`.
pub fn SafeArray(comptime T: type) type {
    return struct {
        const Self = @This();

        list: std.ArrayList(T),
        allocator: std.mem.Allocator,
        /// One exclusive lock for everything: an array has no read-only
        /// operation worth letting through in parallel — even `len` is a
        /// single load — so there is nothing for a shared mode to buy.
        mu: fiber.Mutex(.untracked) = .{},

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{ .list = .empty, .allocator = allocator };
        }

        pub fn deinit(self: *Self) void {
            self.list.deinit(self.allocator);
        }

        pub fn append(self: *Self, v: T) !void {
            self.mu.lock(@src());
            defer self.mu.unlock();
            try self.list.append(self.allocator, v);
        }

        pub fn appendSlice(self: *Self, vs: []const T) !void {
            self.mu.lock(@src());
            defer self.mu.unlock();
            try self.list.appendSlice(self.allocator, vs);
        }

        /// Take the last element, or null when empty.
        pub fn pop(self: *Self) ?T {
            self.mu.lock(@src());
            defer self.mu.unlock();
            return self.list.pop();
        }

        pub fn len(self: *Self) usize {
            self.mu.lock(@src());
            defer self.mu.unlock();
            return self.list.items.len;
        }
    };
}
