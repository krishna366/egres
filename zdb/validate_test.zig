const std = @import("std");
const zdb = @import("zdb");
const validate = zdb.validate;
const buffers = zdb.buffers;

const talloc = std.testing.allocator;

/// What index writers will do: emit a RoaringDelta message directly
/// with the generated writer, tail-less, at buffer offset 0.
fn encodeDelta(adds: []const u64, rems: []const u64) ![]u8 {
    var list: std.ArrayList(u8) = .empty;
    errdefer list.deinit(talloc);
    var w = try buffers.RoaringDeltaWriter.initWithBuf(talloc, &list);
    if (adds.len > 0) try w.setAdds(adds);
    if (rems.len > 0) try w.setRemoves(rems);
    _ = try w.finish();
    return list.toOwnedSlice(talloc);
}

test "validate: validateDelta is the write-side gate" {
    // A well-formed record passes.
    const ok = try encodeDelta(&.{ 1, 2, 3 }, &.{ 10, 20 });
    defer talloc.free(ok);
    try validate.validateDelta(ok);

    // Garbage bytes: not a parsable message.
    try std.testing.expectError(
        validate.Error.CorruptPosting,
        validate.validateDelta(&.{ 1, 2, 3 }),
    );

    // Truncated element region: header's array bounds point past end.
    try std.testing.expectError(
        validate.Error.CorruptPosting,
        validate.validateDelta(ok[0 .. ok.len - 8]),
    );

    // Unsorted / duplicate elements (corrupt a valid record's
    // little-endian tail in place).
    const adds_only = try encodeDelta(&.{ 1, 2, 3 }, &.{});
    defer talloc.free(adds_only);
    const bad = try talloc.dupe(u8, adds_only);
    defer talloc.free(bad);
    std.mem.writeInt(u64, bad[bad.len - 8 ..][0..8], 1, .little);
    try std.testing.expectError(
        validate.Error.CorruptPosting,
        validate.validateDelta(bad),
    );
    std.mem.writeInt(u64, bad[bad.len - 8 ..][0..8], 2, .little);
    try std.testing.expectError(
        validate.Error.CorruptPosting,
        validate.validateDelta(bad),
    );

    // Overlapping lists: the same egid added and removed in one delta
    // has no defined meaning (disjointness is the writer's contract);
    // fold order must never silently resolve it.
    const overlap = try encodeDelta(&.{ 3, 5 }, &.{ 5, 9 });
    defer talloc.free(overlap);
    try std.testing.expectError(
        validate.Error.CorruptPosting,
        validate.validateDelta(overlap),
    );
}
