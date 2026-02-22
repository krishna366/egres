const schema = @import("schema");

// Port of badger/fb/flatbuffer.fbs:
// - table BlockOffset { key:[ubyte]; offset:uint; len:uint; }
// - table TableIndex { offsets:[BlockOffset]; bloom_filter:[ubyte]; max_version:uint64; ... }
pub const BlockIndex = schema.Table(.{
    .{ .id = 1, .name = "key", .ty = schema.Bytes },
    .{ .id = 2, .name = "offset", .ty = schema.U32 },
    .{ .id = 3, .name = "len", .ty = schema.U32 },
});

pub const TableIndex = schema.Table(.{
    .{ .id = 1, .name = "max_version", .ty = schema.U64 },
    .{ .id = 2, .name = "key_count", .ty = schema.U32 },
    .{ .id = 3, .name = "uncompressed_size", .ty = schema.U32 },
    .{ .id = 4, .name = "compressed_size", .ty = schema.U32 },
    .{ .id = 5, .name = "stale_data_size", .ty = schema.U32 },

    .{ .id = 6, .name = "compression", .ty = schema.B64 },
    .{ .name = "is_none", .ty = schema.Bool(.{ .field_id = 6, .bit = 0 }) },
    .{ .name = "is_lz4", .ty = schema.Bool(.{ .field_id = 6, .bit = 1 }) },
    .{ .name = "is_zstd", .ty = schema.Bool(.{ .field_id = 6, .bit = 2 }) },

    .{ .id = 7, .name = "last_key", .ty = schema.Bytes },
    .{ .id = 8, .name = "blocks", .ty = schema.Array(BlockIndex) },
    .{ .id = 9, .name = "bloom_filter", .ty = schema.Bytes },
});

pub const ManifestChange = schema.Table(.{
    .{ .id = 1, .name = "op", .ty = schema.U32 }, // 0=CREATE, 1=DELETE
    .{ .id = 2, .name = "table_id", .ty = schema.U64 },
    .{ .id = 3, .name = "level", .ty = schema.U32 }, // 0 for DELETE
});

pub const ManifestChangeSet = schema.Table(.{
    .{ .id = 1, .name = "changes", .ty = schema.Array(ManifestChange) },
});

// zbuf tail stores table_id as u16, so use a module namespace in high bits and
// OR local ids into low bits.
const ZDB_TABLE_NS: u16 = 0xB600;

fn tableId(comptime local_id: u16) u16 {
    return ZDB_TABLE_NS | local_id;
}

pub const tables = .{
    .{ .name = "BlockIndex", .ty = BlockIndex, .id = tableId(1) },
    .{ .name = "TableIndex", .ty = TableIndex, .id = tableId(2) },
    .{ .name = "ManifestChange", .ty = ManifestChange, .id = tableId(3) },
    .{ .name = "ManifestChangeSet", .ty = ManifestChangeSet, .id = tableId(4) },
};
