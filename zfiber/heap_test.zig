const std = @import("std");
const heap = @import("zfiber").heap;

const Item = struct {
    key: i64,
    heap_index: usize = std.math.maxInt(usize),
};

const Heap = heap.MinHeap(Item, "key", "heap_index");

test "MinHeap: push + popMin yields ascending order" {
    var h: Heap = .{};
    defer h.deinit(std.testing.allocator);

    var items = [_]Item{
        .{ .key = 7 },
        .{ .key = 3 },
        .{ .key = 5 },
        .{ .key = 1 },
        .{ .key = 4 },
        .{ .key = 6 },
        .{ .key = 2 },
    };
    for (&items) |*it| try h.push(std.testing.allocator, it);

    var prev: i64 = std.math.minInt(i64);
    var count: usize = 0;
    while (h.popMin()) |it| : (count += 1) {
        try std.testing.expect(it.key >= prev);
        prev = it.key;
    }
    try std.testing.expectEqual(@as(usize, items.len), count);
}

test "MinHeap: remove pulls a non-root entry without breaking order" {
    var h: Heap = .{};
    defer h.deinit(std.testing.allocator);

    var items: [10]Item = undefined;
    for (&items, 0..) |*it, i| {
        it.* = .{ .key = @intCast(i) };
        try h.push(std.testing.allocator, it);
    }

    // Remove the entry with key=5 from the middle. After removal,
    // popping should yield 0,1,2,3,4,6,7,8,9 in order.
    h.remove(&items[5]);
    try std.testing.expectEqual(Heap.not_in_heap, items[5].heap_index);
    try std.testing.expectEqual(@as(usize, 9), h.count());

    var seen = std.ArrayList(i64).empty;
    defer seen.deinit(std.testing.allocator);
    while (h.popMin()) |it| try seen.append(std.testing.allocator, it.key);

    const expected = [_]i64{ 0, 1, 2, 3, 4, 6, 7, 8, 9 };
    try std.testing.expectEqualSlices(i64, &expected, seen.items);
}

test "MinHeap: remove on not-enqueued entry is a no-op" {
    var h: Heap = .{};
    defer h.deinit(std.testing.allocator);

    var item: Item = .{ .key = 42 };
    h.remove(&item); // not_in_heap initially; should just return
    try std.testing.expectEqual(@as(usize, 0), h.count());

    try h.push(std.testing.allocator, &item);
    h.remove(&item);
    try std.testing.expectEqual(@as(usize, 0), h.count());
    h.remove(&item); // already removed; no-op again
    try std.testing.expectEqual(@as(usize, 0), h.count());
}

test "MinHeap: peekMin matches the next popMin" {
    var h: Heap = .{};
    defer h.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(?*Item, null), h.peekMin());

    var items = [_]Item{
        .{ .key = 30 },
        .{ .key = 10 },
        .{ .key = 20 },
    };
    for (&items) |*it| try h.push(std.testing.allocator, it);

    const top = h.peekMin().?;
    try std.testing.expectEqual(@as(i64, 10), top.key);
    const popped = h.popMin().?;
    try std.testing.expectEqual(top, popped);
}
