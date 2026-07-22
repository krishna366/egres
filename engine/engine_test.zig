//! Wire-format tests for the .data Mutations payload: a flat kv list
//! with full self-describing keys, one namespace per entry.

const std = @import("std");
const testing = std.testing;
const engine = @import("engine");
const zdb = @import("zdb");

const zen = zdb.buffers;
const Key = zdb.kv.Key;
const Meta = zdb.kv.Meta;

test "mutations: flat multi-table round-trip" {
    const alloc = testing.allocator;

    // Two tables in one namespace. Keys go on the wire fully marshaled,
    // carrying the writer's timestamp — apply rewrites it to the entry's
    // commit HLC, so the values here are arbitrary.
    const ka = try (Key{
        .namespace = 7,
        .table_id = 16,
        .primary = "row-a",
        .timestamp = 111,
    }).marshal(alloc);
    defer alloc.free(ka);
    const kb = try (Key{
        .namespace = 7,
        .table_id = 17,
        .primary = "row-b",
        .timestamp = 222,
    }).marshal(alloc);
    defer alloc.free(kb);

    var kvs = [_]zen.KeyValueStruct{
        .{ .key = ka, .value = "va" },
        .{ .key = kb, .meta = (Meta{ .kind = .deleted }).toByte() },
    };
    const payload = try engine.Engine.encodeMutations(alloc, 7, &kvs);
    defer alloc.free(payload);

    const ms = switch (try zen.parseMessageWithTail(payload)) {
        .Mutations => |r| r,
        else => return error.TestUnexpectedResult,
    };
    try testing.expectEqual(7, ms.namespace());
    try testing.expectEqual(2, ms.kvsLen());

    // kv 0: a plain put — the meta zero-default decodes as a raw state
    // row. Parsing the wire key recovers the full routing identity.
    {
        const kv = try ms.kvsAt(0);
        try testing.expectEqualSlices(u8, ka, kv.key());
        try testing.expectEqualSlices(u8, "va", kv.value());
        try testing.expect(!Meta.fromByte(kv.meta()).isDeleted());
        const key = Key.init(kv.key());
        try testing.expectEqual(7, key.namespace);
        try testing.expectEqual(16, key.table_id);
        try testing.expectEqualSlices(u8, "row-a", key.primary);
    }
    // kv 1: a tombstone in the other table — empty value, kind deleted.
    {
        const kv = try ms.kvsAt(1);
        try testing.expectEqualSlices(u8, kb, kv.key());
        try testing.expectEqual(0, kv.value().len);
        try testing.expect(Meta.fromByte(kv.meta()).isDeleted());
        try testing.expectEqual(17, Key.init(kv.key()).table_id);
    }
}
