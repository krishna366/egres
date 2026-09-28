const std = @import("std");
const _sync = @import("sync.zig");

/// Thread-safe wrapper around std.AutoHashMap. Every operation takes
/// the lock.
/// Init with `.init(allocator)`.
pub fn SafeHashMap(comptime K: type, comptime V: type) type {
    return struct {
        const Self = @This();
        const HashMap = std.AutoHashMap(K, V);

        map: HashMap,
        /// Readers share; only put/remove/getOrPut take it exclusively. A
        /// map on a read hot path (the forest's table map) would otherwise
        /// serialize every lookup behind one mutex.
        mu: _sync.RWMutex(.untracked) = .{},

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
