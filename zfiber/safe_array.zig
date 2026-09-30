// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const _sync = @import("sync.zig");

/// Mutex-protected std.ArrayList. Init with `.init(allocator)`.
pub fn SafeArray(comptime T: type) type {
    return struct {
        const Self = @This();

        list: std.ArrayList(T),
        allocator: std.mem.Allocator,
        /// One exclusive lock for everything: an array has no read-only
        /// operation worth letting through in parallel — even `len` is a
        /// single load — so there is nothing for a shared mode to buy.
        mu: _sync.Mutex(.untracked) = .{},

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
