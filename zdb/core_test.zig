const std = @import("std");
const zdb = @import("zdb");

test "options validate rejects single compactor and in-memory dir" {
    const bad_compactors = zdb.options.Options{
        .num_compactors = 1,
    };
    try std.testing.expectError(
        zdb.options.Error.InvalidNumCompactors,
        bad_compactors.validate(),
    );

    const bad_in_memory_dir = zdb.options.Options{
        .in_memory = true,
        .dir = "/tmp/db",
    };
    try std.testing.expectError(
        zdb.options.Error.InvalidDirForInMemory,
        bad_in_memory_dir.validate(),
    );
}

test "keyWithTs parseTs parseKey and compareKeys match badger ordering" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const a_v9 = try zdb.key.keyWithTs(alloc, "a", 9);
    const a_v7 = try zdb.key.keyWithTs(alloc, "a", 7);
    const b_v1 = try zdb.key.keyWithTs(alloc, "b", 1);

    try std.testing.expectEqual(@as(u64, 9), zdb.key.parseTs(a_v9));
    try std.testing.expectEqualStrings("a", zdb.key.parseKey(a_v9));

    // Same user key: larger version must sort first.
    try std.testing.expectEqual(std.math.Order.lt, zdb.key.compareKeys(a_v9, a_v7));
    try std.testing.expectEqual(std.math.Order.gt, zdb.key.compareKeys(a_v7, a_v9));

    // User key still sorts before timestamp suffix.
    try std.testing.expectEqual(std.math.Order.lt, zdb.key.compareKeys(a_v7, b_v1));
}

test "entry flags and estimate size" {
    const e = zdb.types.Entry
        .init("hello", "world")
        .withVersion(7)
        .withUserMeta(3)
        .withDelete()
        .withDiscardEarlierVersions();

    try std.testing.expectEqual(@as(usize, 12), e.estimateSize());
    try std.testing.expect(e.isDelete());
    try std.testing.expect(e.discardEarlierVersions());
    try std.testing.expectEqual(@as(u64, 7), e.version);
    try std.testing.expectEqual(@as(u8, 3), e.user_meta);
}
