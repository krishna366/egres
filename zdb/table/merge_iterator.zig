const std = @import("std");
const table = @import("table.zig");
const Key = @import("../key.zig").Key;
const utils = @import("utils");

pub const Error = table.Error;

/// Iterator is a tagged union that lets MergeIterator accept both a single
/// TableIterator and a ConcatIterator (for non-overlapping sorted tables).
pub const Iterator = union(enum) {
    table: table.TableIterator,
    concat: ConcatIterator,

    pub fn seekRange(self: *Iterator, start: Key, end: Key) !void {
        switch (self.*) {
            inline else => |*it| try it.seekRange(start, end),
        }
    }

    pub fn itemRef(self: *const Iterator) ?table.Item {
        return switch (self.*) {
            inline else => |*it| it.itemRef(),
        };
    }

    pub fn next(self: *Iterator) Error!void {
        switch (self.*) {
            inline else => |*it| try it.next(),
        }
    }

    pub fn deinit(self: *Iterator) void {
        switch (self.*) {
            inline else => |*it| it.deinit(),
        }
    }
};

fn iterOrder(iters: []const Iterator, a: usize, b: usize) std.math.Order {
    const item_a = iters[a].itemRef().?;
    const item_b = iters[b].itemRef().?;
    return item_a.key.order(item_b.key);
}

pub const MergeIterator = struct {
    allocator: std.mem.Allocator,
    iters: []Iterator, // caller-owned slice
    heap: std.PriorityQueue(usize, []const Iterator, iterOrder),

    pub fn init(allocator: std.mem.Allocator, iters: []Iterator) MergeIterator {
        return .{
            .allocator = allocator,
            .iters = iters,
            .heap = std.PriorityQueue(usize, []const Iterator, iterOrder).init(allocator, iters),
        };
    }

    pub fn deinit(self: *MergeIterator) void {
        self.heap.deinit();
        for (self.iters) |*it| it.deinit();
    }

    pub fn seekRange(self: *MergeIterator, start: Key, end: Key) !void {
        while (self.heap.removeOrNull() != null) {}
        for (self.iters) |*it| try it.seekRange(start, end);
        for (self.iters, 0..) |*it, i| {
            if (it.itemRef() != null) try self.heap.add(i);
        }
    }

    pub fn itemRef(self: *MergeIterator) ?table.Item {
        const i = self.heap.peek() orelse return null;
        return self.iters[i].itemRef();
    }

    pub fn next(self: *MergeIterator) Error!void {
        const i = self.heap.removeOrNull() orelse return;
        try self.iters[i].next();
        if (self.iters[i].itemRef() != null) try self.heap.add(i);
    }
};

pub const ConcatIterator = struct {
    iters: []table.TableIterator, // caller-owned slice; sorted by smallest key
    current: usize, // == iters.len when exhausted

    pub fn init(iters: []table.TableIterator) ConcatIterator {
        return .{ .iters = iters, .current = 0 };
    }

    pub fn deinit(self: *ConcatIterator) void {
        for (self.iters) |*it| it.deinit();
    }

    pub fn seekRange(self: *ConcatIterator, start: Key, end: Key) !void {
        if (self.iters.len == 0) return;

        if (start.len() == 0) {
            self.current = 0;
            try self.iters[0].seekRange(.{}, end);
            return;
        }

        // First table whose biggest key >= start.
        const idx = utils.search(table.TableIterator, self.iters, start, struct {
            fn f(sk: Key, it: table.TableIterator) bool {
                return it.table.biggest.order(sk) != .lt;
            }
        }.f);
        self.current = idx;
        if (self.current >= self.iters.len) return;

        try self.iters[self.current].seekRange(start, end);
        for (self.iters[self.current + 1 ..]) |*itr| {
            try itr.seekRange(.{}, end);
        }
    }

    pub fn itemRef(self: *const ConcatIterator) ?table.Item {
        if (self.current >= self.iters.len) return null;
        return self.iters[self.current].itemRef();
    }

    pub fn next(self: *ConcatIterator) Error!void {
        if (self.current >= self.iters.len) return;

        // Advance
        try self.iters[self.current].next();

        // This loop can skip over empty iterators, even though we don't expect them.
        while (self.iters[self.current].itemRef() == null) {
            self.current += 1;
            if (self.current >= self.iters.len) return;
        }
    }
};
