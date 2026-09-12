const std = @import("std");
const zdb = @import("zdb");
const Meta = zdb.kv.Meta;
const Kind = zdb.kv.Kind;
const Encoding = zdb.kv.Encoding;
const Item = zdb.kv.Item;
const Key = zdb.kv.Key;
const Version = zdb.kv.Version;

const expectEqual = std.testing.expectEqual;

// These bit positions are on-disk state: every SST entry stores a Meta byte
// (see zdb/table/builder.zig). Reordering the fields in Meta silently
// reinterprets every table ever written, so pin the layout here — this test is
// what makes it safe to have no separate bit_delete/bit_* constants to
// disagree with the struct.
test "Meta: bit layout is frozen" {
    // kind occupies the low two bits.
    try expectEqual(@as(u8, 0), (Meta{ .kind = .state }).toByte());
    try expectEqual(@as(u8, 1), (Meta{ .kind = .delta }).toByte());
    try expectEqual(@as(u8, 2), (Meta{ .kind = .deleted }).toByte());

    // Encoding occupies the high nibble.
    try expectEqual(@as(u8, 0x00), (Meta{ .encoding = .raw }).toByte());
    try expectEqual(@as(u8, 0x10), (Meta{ .encoding = .roaring }).toByte());

    // kind and encoding must not smear into each other.
    const both = Meta{ .kind = .deleted, .encoding = .roaring };
    try expectEqual(@as(u8, 0x12), both.toByte());

    try expectEqual(@as(usize, 1), @sizeOf(Meta));
    try expectEqual(@as(usize, 8), @bitSizeOf(Meta));
}

test "Meta: default is raw with no flags" {
    try expectEqual(@as(u8, 0), (Meta{}).toByte());
    try expectEqual(@as(u8, 0), Meta.raw.toByte());
    // A KeyValue written before the meta field existed reads back 0, and this
    // is why that decodes as a plain state row rather than something surprising.
    const from_absent_field = Meta.fromByte(0);
    try expectEqual(Kind.state, from_absent_field.kind);
    try expectEqual(Encoding.raw, from_absent_field.encoding);
}

// Meta is @bitCast from bytes read off disk, so every one of the 256 possible
// bytes has to survive the round trip. This is the guard on Encoding staying
// non-exhaustive: make it exhaustive and the undefined tags (2..15) turn this
// into a panic in Debug and UB in ReleaseFast.
test "Meta: every byte round-trips" {
    for (0..256) |i| {
        const b: u8 = @intCast(i);
        try expectEqual(b, Meta.fromByte(b).toByte());
    }
}

test "Meta: reserved bits survive a round trip" {
    // A future writer may set bits we don't know about yet. We must carry them
    // through untouched rather than normalizing them away.
    const unknown = Meta.fromByte(0b1000_1100);
    try expectEqual(@as(u2, 0b11), unknown._reserved);
    try expectEqual(@as(u8, 0b1000_1100), unknown.toByte());
}

/// Three versions of one row, newest first, for the Item tests below.
const three = [_]Version{
    .{ .ts = 30, .meta = .{ .kind = .delta }, .value = "c" },
    .{ .ts = 20, .meta = .{ .kind = .delta }, .value = "b" },
    .{ .ts = 10, .meta = .{ .kind = .state }, .value = "a" },
};

test "Item: visibleAt cuts at the newest version not above ts" {
    const item = Item{
        .key = .{ .primary = "k" },
        .versions = .{ .many = &three },
    };

    // At a version's own ts: that version is visible.
    try expectEqual(@as(usize, 3), item.visibleAt(30).?.versions.len());
    try expectEqual(@as(u64, 30), item.visibleAt(30).?.newest().ts);
    try expectEqual(@as(u64, 10), item.visibleAt(10).?.newest().ts);
    try expectEqual(@as(usize, 1), item.visibleAt(10).?.versions.len());

    // Between two versions: the older one wins.
    try expectEqual(@as(u64, 20), item.visibleAt(25).?.newest().ts);
    try expectEqual(@as(usize, 2), item.visibleAt(25).?.versions.len());

    // Above every version: the whole item.
    try expectEqual(@as(usize, 3), item.visibleAt(99).?.versions.len());

    // Below every version: nothing is visible yet.
    try expectEqual(@as(?Item, null), item.visibleAt(9));

    // The key travels with the window.
    try std.testing.expectEqualStrings("k", item.visibleAt(25).?.key.primary);
}

test "Item: chain stops at a state, a tombstone, or the end" {
    const at_state = Item{
        .key = .{ .primary = "k" },
        .versions = .{ .many = &three },
    };
    // Two deltas plus the state they fold onto.
    const state_chain = at_state.chain();
    try expectEqual(@as(usize, 3), state_chain.versions.len());
    try expectEqual(@as(u64, 10), state_chain.versions.slice()[2].ts);

    const with_tomb = [_]Version{
        .{ .ts = 30, .meta = .{ .kind = .delta }, .value = "c" },
        .{ .ts = 20, .meta = .{ .kind = .deleted }, .value = "" },
        .{ .ts = 10, .meta = .{ .kind = .state }, .value = "a" },
    };
    const at_tomb = Item{
        .key = .{ .primary = "k" },
        .versions = .{ .many = &with_tomb },
    };
    const tomb_chain = at_tomb.chain();
    try expectEqual(@as(usize, 2), tomb_chain.versions.len());
    try expectEqual(@as(u64, 20), tomb_chain.versions.slice()[1].ts);

    // All deltas: the chain is every version, and stays open.
    const deltas = [_]Version{
        .{ .ts = 30, .meta = .{ .kind = .delta }, .value = "c" },
        .{ .ts = 20, .meta = .{ .kind = .delta }, .value = "b" },
    };
    const open = Item{
        .key = .{ .primary = "k" },
        .versions = .{ .many = &deltas },
    };
    try expectEqual(@as(usize, 2), open.chain().versions.len());

    // A state at the head is its own chain.
    const solo = Item{
        .key = .{ .primary = "k" },
        .versions = .{ .one = .{ .ts = 30, .meta = .{}, .value = "c" } },
    };
    try expectEqual(@as(usize, 1), solo.chain().versions.len());
}

test "Item: dupe copies every version, free releases them" {
    const alloc = std.testing.allocator;
    const item = Item{
        .key = .{ .namespace = 7, .table_id = 3, .primary = "k" },
        .versions = .{ .many = &three },
    };

    const copy = try item.dupe(alloc);
    defer copy.free(alloc);

    try expectEqual(@as(u64, 7), copy.key.namespace);
    try expectEqual(@as(u32, 3), copy.key.table_id);
    try std.testing.expectEqualStrings("k", copy.key.primary);
    try expectEqual(@as(usize, 3), copy.versions.len());
    for (item.versions.slice(), copy.versions.slice()) |src, dst| {
        try expectEqual(src.ts, dst.ts);
        try expectEqual(src.meta.toByte(), dst.meta.toByte());
        try std.testing.expectEqualStrings(src.value, dst.value);
        // Owned storage, not the original bytes.
        try std.testing.expect(src.value.ptr != dst.value.ptr);
    }
    try std.testing.expect(item.key.primary.ptr != copy.key.primary.ptr);
}

test "Key: marshal round trip carries no timestamp" {
    const alloc = std.testing.allocator;
    const key = Key{ .namespace = 9, .table_id = 4, .primary = "row" };

    const raw = try key.marshal(alloc);
    defer alloc.free(raw);
    // 8-byte namespace, 4-byte table_id, then the primary — nothing else.
    try expectEqual(@as(usize, 12 + 3), raw.len);
    try expectEqual(key.len(), raw.len);

    const back = Key.init(raw);
    try expectEqual(key.namespace, back.namespace);
    try expectEqual(key.table_id, back.table_id);
    try std.testing.expectEqualStrings(key.primary, back.primary);

    // Order is namespace, then table_id, then primary bytes, which is
    // also the bytewise order of the marshaled keys.
    const b = Key{ .namespace = 9, .table_id = 4, .primary = "rox" };
    const raw_b = try b.marshal(alloc);
    defer alloc.free(raw_b);
    try expectEqual(std.math.Order.lt, key.order(b));
    try expectEqual(std.math.Order.lt, std.mem.order(u8, raw, raw_b));

    const other_tid = Key{ .namespace = 9, .table_id = 5, .primary = "a" };
    try expectEqual(std.math.Order.lt, key.order(other_tid));
    // An empty primary is a bound, not a row: it marshals to nothing.
    try expectEqual(@as(usize, 0), (Key{ .namespace = 9 }).len());
}
