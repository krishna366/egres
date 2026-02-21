const std = @import("std");
const key = @import("../key.zig");
const table = @import("table.zig");

pub const Error = table.Error || key.Error;

pub const MergeIterator = struct {
    iters: []table.TableIterator,
    current_idx: ?usize,

    pub fn init(iters: []table.TableIterator) MergeIterator {
        return .{
            .iters = iters,
            .current_idx = null,
        };
    }

    pub fn deinit(self: *MergeIterator) void {
        for (self.iters) |*it| it.deinit();
    }

    pub fn seekToFirst(self: *MergeIterator) Error!void {
        for (self.iters) |*it| try it.seekToFirst();
        try self.pickCurrent();
    }

    pub fn item(self: *const MergeIterator) ?table.Item {
        const i = self.current_idx orelse return null;
        return self.iters[i].itemRef();
    }

    pub fn next(self: *MergeIterator) Error!void {
        if (self.current_idx) |i| try self.iters[i].next();
        try self.pickCurrent();
    }

    fn pickCurrent(self: *MergeIterator) Error!void {
        self.current_idx = null;
        for (self.iters, 0..) |it, i| {
            const candidate = it.itemRef() orelse continue;
            if (self.current_idx) |cur| {
                const cur_item = self.iters[cur].itemRef().?;
                if (key.compareKeys(candidate.key, cur_item.key) == .lt) {
                    self.current_idx = i;
                }
            } else {
                self.current_idx = i;
            }
        }
    }
};
