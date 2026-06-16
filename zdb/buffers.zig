const schema = @import("schema");

// Port of badger/fb/flatbuffer.fbs:
// - table BlockOffset { key:[ubyte]; offset:uint; len:uint; }
// - table TableIndex { offsets:[BlockOffset]; bloom_filter:[ubyte]; max_version:uint64; ... }
pub const BlockIndex = schema.Table(.{
    .{ .id = 1, .name = "key", .ty = schema.Bytes },
    .{ .id = 2, .name = "offset", .ty = schema.U32 },
    .{ .id = 3, .name = "len", .ty = schema.U32 },
});

pub const BinaryFuseFilter = schema.Table(.{
    .{ .id = 1, .name = "seed", .ty = schema.U64 },
    .{ .id = 2, .name = "segment_length", .ty = schema.U32 },
    .{ .id = 3, .name = "segment_length_mask", .ty = schema.U32 },
    .{ .id = 4, .name = "segment_count", .ty = schema.U32 },
    .{ .id = 5, .name = "segment_count_length", .ty = schema.U32 },
    .{ .id = 6, .name = "fingerprints", .ty = schema.Array(schema.U16) },
});

// Tags chosen to match `u/compress.zig::Codec` so @enumFromInt(@intFromEnum(...))
// is identity at the wire boundary.
pub const Compression = schema.EnumDef(.{
    .{ .name = "none", .tag = 0 },
    .{ .name = "zstd", .tag = 1 },
    .{ .name = "lz4", .tag = 2 },
});

pub const TableIndex = schema.Table(.{
    .{ .id = 1, .name = "max_version", .ty = schema.U64 },
    .{ .id = 2, .name = "key_count", .ty = schema.U32 },
    .{ .id = 3, .name = "uncompressed_size", .ty = schema.U32 },
    .{ .id = 4, .name = "compressed_size", .ty = schema.U32 },
    .{ .id = 5, .name = "stale_data_size", .ty = schema.U32 },
    .{ .id = 6, .name = "compression", .ty = schema.Enum(Compression) },
    .{ .id = 7, .name = "last_key", .ty = schema.Bytes },
    .{ .id = 8, .name = "blocks", .ty = schema.Array(BlockIndex) },
    .{ .id = 9, .name = "binary_fuse", .ty = BinaryFuseFilter },
});

pub const Op = schema.EnumDef(.{
    .{ .name = "create", .tag = 0 },
    .{ .name = "delete", .tag = 1 },
    .{ .name = "update", .tag = 2 },
});

// TreeChange is used to create a new tree, delete a tree or update a tree's
// range. `tree_id` is namespace-local and packs (table_id << 32) | split_id, so
// the tree's SQL table bucket is derived from it (Key.treeTableId) and never
// stored separately. `namespace_id` + `tree_id` together are the tree's global
// identity (Key.TreeKey).
pub const TreeChange = schema.Table(.{
    .{ .id = 1, .name = "op", .ty = schema.Enum(Op) }, // 0=CREATE, 1=DELETE, 2=UPDATE
    .{ .id = 2, .name = "namespace_id", .ty = schema.U64 },
    .{ .id = 3, .name = "tree_id", .ty = schema.U64 },
    .{ .id = 4, .name = "left_key", .ty = schema.Bytes },
    .{ .id = 5, .name = "right_key", .ty = schema.Bytes },
});

// SSTableChange records one SST file (`sst_id`) entering or leaving a tree at a
// level. `sst_id` is the on-disk SST identifier (file_ids.allocate), NOT a SQL
// table id — the SQL table is the tree, derivable from tree_id. One tree holds
// many SSTs; one SST can belong to many trees (shared after a split/clone, freed
// when its refcount hits 0). `namespace_id` + `tree_id` locate the owning tree —
// a single flush spans many namespaces, so the namespace can't be implied by the
// batch.
pub const SSTableChange = schema.Table(.{
    .{ .id = 1, .name = "op", .ty = schema.Enum(Op) }, // 0=CREATE, 1=DELETE
    .{ .id = 2, .name = "level", .ty = schema.U32 }, // 0 for DELETE
    .{ .id = 3, .name = "sst_id", .ty = schema.U64 }, // on-disk SST id
    .{ .id = 4, .name = "tree_id", .ty = schema.U64 },
    .{ .id = 5, .name = "namespace_id", .ty = schema.U64 },
    .{ .id = 6, .name = "checksum", .ty = schema.U32 }, // CRC32 of SST file (CREATE only)
    .{ .id = 7, .name = "size", .ty = schema.U64 }, // file size in bytes (CREATE only)
});

// A namespace's next-free table_id high-water-mark. Persisted ONLY inside a
// snapshot (a serialized ForestState), so a re-elected leader never re-issues a
// table_id even after the table's tree was dropped from the manifest. Live
// (non-snapshot) ManifestChanges leave table_counters empty.
pub const TableId = schema.Table(.{
    .{ .id = 1, .name = "namespace", .ty = schema.U64 },
    .{ .id = 2, .name = "next_table_id", .ty = schema.U32 },
});

pub const ManifestChange = schema.Table(.{
    .{ .id = 1, .name = "tree_changes", .ty = schema.Array(TreeChange) },
    .{ .id = 2, .name = "sstable_changes", .ty = schema.Array(SSTableChange) },
    // TODO: cmd needs to switch to enum.
    .{ .id = 3, .name = "cmd", .ty = schema.U32 }, // 1=new_memtable, 2=flush, 3=compaction, 4=tree_split, 5=ns_create, 6=create_tree, 7=drop_tree
    .{ .id = 4, .name = "memtable_id", .ty = schema.U64 }, // for flush: which memtable was flushed
    .{ .id = 5, .name = "next_table_ids", .ty = schema.Array(TableId) }, // snapshot only
    .{ .id = 6, .name = "next_ns_id", .ty = schema.U64 }, // snapshot only: global namespace counter
});

/// A single key-value tuple. Carried inside `Mutations` which groups them
/// by namespace — so the per-tuple envelope only holds the bytes themselves.
pub const KeyValue = schema.Table(.{
    .{ .id = 1, .name = "key", .ty = schema.Bytes },
    .{ .id = 2, .name = "value", .ty = schema.Bytes },
});

/// One or more key-value writes proposed through raft as a single entry. The
/// enclosing `LogEntry.timestamp` is the commit HLC for the whole batch (all kvs
/// share it). `namespace` + `table_id` sit at the Mutations level so every kv
/// lands in the same (namespace, table_id) bucket — keeps routing + apply
/// simple. table_id 0 is the raw-KV / default bucket.
pub const Mutations = schema.Table(.{
    .{ .id = 1, .name = "namespace", .ty = schema.U64 },
    .{ .id = 2, .name = "table_id", .ty = schema.U32 },
    .{ .id = 3, .name = "kvs", .ty = schema.Array(KeyValue) },
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
    .{ .name = "BinaryFuseFilter", .ty = BinaryFuseFilter, .id = tableId(3) },

    .{ .name = "TreeChange", .ty = TreeChange, .id = tableId(4) },
    .{ .name = "SSTableChange", .ty = SSTableChange, .id = tableId(5) },
    .{ .name = "ManifestChange", .ty = ManifestChange, .id = tableId(6) },
    .{ .name = "KeyValue", .ty = KeyValue, .id = tableId(7) },
    .{ .name = "Mutations", .ty = Mutations, .id = tableId(8) },
    .{ .name = "TableId", .ty = TableId, .id = tableId(9) },
};
