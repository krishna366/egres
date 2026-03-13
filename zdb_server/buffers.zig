const schema = @import("schema");

/// A single key-value mutation proposed through raft.
pub const Mutation = schema.Table(.{
    .{ .id = 1, .name = "namespace", .ty = schema.U32 },
    .{ .id = 2, .name = "key", .ty = schema.Bytes },
    .{ .id = 3, .name = "value", .ty = schema.Bytes },
    .{ .id = 4, .name = "timestamp", .ty = schema.U64 },
});

const ZDB_SERVER_NS: u16 = 0xC700;

fn tableId(comptime local_id: u16) u16 {
    return ZDB_SERVER_NS | local_id;
}

pub const tables = .{
    .{ .name = "Mutation", .ty = Mutation, .id = tableId(1) },
};
