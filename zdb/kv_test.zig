const std = @import("std");
const zdb = @import("zdb");
const Meta = zdb.kv.Meta;
const Kind = zdb.kv.Kind;
const Encoding = zdb.kv.Encoding;

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
