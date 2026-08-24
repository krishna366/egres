const std = @import("std");

/// How a value's bytes are encoded. Lives in Meta's high nibble.
///
/// Non-exhaustive on purpose: Meta is @bitCast from bytes we read off disk, so
/// every u4 must be a legal value. An exhaustive enum would make an unknown tag
/// illegal behavior at the bitCast — a panic in Debug, UB in ReleaseFast — on
/// data some future writer produced. Switches on this must carry an `else`.
pub const Encoding = enum(u4) {
    raw = 0,
    roaring = 1,
    _,
};

/// What a row version is, relative to the older versions of the same key.
/// Compaction consults this to decide how far down a key's history it must keep
/// (see Tree.buildCompaction).
///
/// Non-exhaustive for the same reason as Encoding: Meta is @bitCast from disk
/// bytes, so the fourth (unused) u2 value must stay legal. `state` is 0 so a
/// zeroed Meta is a self-contained row — the safe default. Switches must carry
/// an `else`.
pub const Kind = enum(u2) {
    /// A complete row: reading it needs nothing older. Once the read horizon
    /// passes it, every version beneath it is dead history.
    state = 0,
    /// A partial row layered on the versions below it. The nearest `state`
    /// (and any deltas between) must survive for this to be reconstructable, so
    /// compaction never seals a key on a delta.
    delta = 1,
    /// A tombstone: the key reads as absent from this version onward.
    deleted = 2,
    _,
};

/// One byte of metadata about a row, carried verbatim end-to-end: wire →
/// memtable → SST → compaction → read. Low nibble holds the row `kind`, high
/// nibble the value `encoding`.
///
/// `kind` is interpreted: `deleted` makes a key read as absent (Forest.get) and
/// lets bottom-level compaction forget it, while `state`/`delta` decide how much
/// version history that compaction must retain (Tree.buildCompaction). The
/// `encoding` nibble selects the value's format: `roaring` marks posting
/// values (zdb/validate.zig) — a zroar bitmap when kind=state, a RoaringDelta
/// message when kind=delta — folded at read (Forest.getBitmap) and rolled up
/// into synthetic states by compaction (Tree.zig Emitter).
///
/// Meta belongs to a row (an Item), never to a Key. A Key doubles as a bare
/// position in keyspace — tree boundaries, iterator bounds, block base keys —
/// where "is this a tombstone" has no meaning. Keeping meta off Key is what
/// stops e.g. Tree.findSplitKey from minting a boundary that claims to be
/// deleted.
///
/// Zig packs LSB-first (u/hlc.zig relies on the same), so the bit positions
/// below are the on-disk positions. They are frozen: zdb/kv_test.zig asserts
/// them, and SSTs on disk depend on them.
pub const Meta = packed struct(u8) {
    kind: Kind = .state, // bits 0-1
    _reserved: u2 = 0, // bits 2-3
    encoding: Encoding = .raw, // bits 4-7

    /// A plain state row, raw value. The default for anything that doesn't care.
    pub const raw: Meta = .{};

    pub inline fn isState(self: Meta) bool {
        return self.kind == .state;
    }

    pub inline fn isDelta(self: Meta) bool {
        return self.kind == .delta;
    }

    pub inline fn isDeleted(self: Meta) bool {
        return self.kind == .deleted;
    }

    pub inline fn isRoaring(self: Meta) bool {
        return self.encoding == .roaring;
    }

    pub inline fn toByte(self: Meta) u8 {
        return @bitCast(self);
    }

    pub inline fn fromByte(b: u8) Meta {
        return @bitCast(b);
    }
};

/// One row: where it lives (key), what it holds (value), and what's true about
/// it (meta).
pub const Item = struct {
    key: Key,
    value: []const u8,
    // No default. Every construction site is a place where silently defaulting
    // to .raw would drop a row's metadata, so let the compiler catch a new one.
    meta: Meta,

    pub fn dupe(self: Item, allocator: std.mem.Allocator) !Item {
        const primary = try allocator.dupe(u8, self.key.primary);
        errdefer allocator.free(primary);

        const v = try allocator.dupe(u8, self.value);
        return .{
            .key = .{
                .namespace = self.key.namespace,
                .table_id = self.key.table_id,
                .primary = primary,
                .timestamp = self.key.timestamp,
            },
            .value = v,
            .meta = self.meta,
        };
    }

    /// `dupe` without the value bytes (the copy keeps `value = ""`).
    /// For callers that only need the key — e.g. DELETE's match
    /// collection, which locks and tombstones by primary and never
    /// reads the row back.
    pub fn dupeNoValue(self: Item, allocator: std.mem.Allocator) !Item {
        return .{
            .key = .{
                .namespace = self.key.namespace,
                .table_id = self.key.table_id,
                .primary = try allocator.dupe(u8, self.key.primary),
                .timestamp = self.key.timestamp,
            },
            .value = "",
            .meta = self.meta,
        };
    }

    pub fn free(self: Item, allocator: std.mem.Allocator) void {
        allocator.free(self.key.primary);
        allocator.free(self.value);
    }
};

/// Global identity of an LSM tree: a namespace (u64, global) plus a
/// namespace-local tree_id. The tree_id packs `(table_id << 32) | split_id`
/// (see makeTreeId), so a tree's SQL table is derivable from it and is
/// never stored separately. Two namespaces can reuse the same tree_id, so the
/// namespace must always travel with it — this struct is the map key for both
/// `Forest.trees` and `ForestState.trees`.
pub const TreeKey = packed struct(u128) {
    namespace: u64,
    tree_id: u64,
};

/// Global identity of a table (one routing entry): a namespace (u64) plus a
/// namespace-local table_id (u32), muxed into a single u96. The map key for
/// `Forest.tables`. table_id 0 is the namespace's default (raw-KV)
/// table, so a namespace "exists" iff its `{namespace, 0}` key is present.
pub const TableKey = packed struct(u96) {
    namespace: u64,
    table_id: u32,

    /// Identity hash for lock-table (and future conflict-oracle)
    /// keys. Seed 1 keeps table keys disjoint from row identities
    /// (Key.identityHash, seed 0), so a table's lock can never collide
    /// with a row's.
    pub fn identityHash(self: TableKey) u64 {
        const v: u128 = @as(u96, @bitCast(self));
        return std.hash.XxHash3.hash(1, std.mem.asBytes(&v));
    }
};

/// Identity hash of a whole namespace — the database-level intent-lock
/// key. Seed 2 keeps it disjoint from row identities (Key.identityHash,
/// seed 0) and table identities (TableKey.identityHash, seed 1).
pub fn namespaceHash(namespace: u64) u64 {
    return std.hash.XxHash3.hash(2, std.mem.asBytes(&namespace));
}

/// Compose a namespace-local tree_id from its table (high 32 bits) and
/// split index (low 32 bits). split_id 0 is the table's first/only tree; it
/// grows by one on every split.
pub inline fn makeTreeId(table_id: u32, split_id: u32) u64 {
    return (@as(u64, table_id) << 32) | @as(u64, split_id);
}

/// The SQL table a tree belongs to (high 32 bits of its tree_id).
pub inline fn treeTableId(tree_id: u64) u32 {
    return @intCast(tree_id >> 32);
}

/// The split index of a tree within its table (low 32 bits of its tree_id).
pub inline fn treeSplitId(tree_id: u64) u32 {
    return @truncate(tree_id);
}

pub const Key = struct {
    namespace: u64 = 0,
    table_id: u32 = 0,
    primary: []const u8 = "",
    timestamp: u64 = 0,

    /// Parse a marshaled key: [ns:u64 BE][tid:u32 BE][primary][ts:u64 BE].
    /// For keys without the full prefix/suffix, construct Key directly via
    /// Key{ .primary = ... }.
    pub fn init(raw: []const u8) Key {
        if (raw.len == 0) {
            return .{};
        }
        const ts_idx = raw.len - 8;
        return .{
            .namespace = std.mem.readInt(u64, raw[0..8], .big),
            .table_id = std.mem.readInt(u32, raw[8..12], .big),
            .primary = raw[12..ts_idx],
            .timestamp = std.mem.readInt(u64, raw[ts_idx..][0..8], .big),
        };
    }

    pub fn initAlloc(allocator: std.mem.Allocator, raw: []const u8) !Key {
        var k = init(raw);
        k.primary = try allocator.dupe(u8, k.primary);
        return k;
    }

    pub fn dupe(self: Key, allocator: std.mem.Allocator) !Key {
        var out = self;
        out.primary = try allocator.dupe(u8, self.primary);
        return out;
    }

    pub fn free(self: Key, allocator: std.mem.Allocator) void {
        if (self.primary.len > 0) {
            allocator.free(self.primary);
        }
    }

    pub fn marshal(self: Key, allocator: std.mem.Allocator) ![]const u8 {
        if (self.primary.len == 0) {
            return allocator.alloc(u8, 0);
        }
        const buf = try allocator.alloc(u8, 12 + self.primary.len + 8);
        self.writeTo(buf);
        return buf;
    }

    pub fn marshalTo(self: Key, allocator: std.mem.Allocator, data: *std.ArrayList(u8)) ![]const u8 {
        if (self.primary.len == 0) {
            return allocator.alloc(u8, 0);
        }
        const start = data.items.len;
        try data.appendNTimes(allocator, 0, 12 + self.primary.len + 8);
        const buf = data.items[start..];
        self.writeTo(buf);
        return buf;
    }

    /// Identity hash over (namespace, table_id, primary) — the
    /// timestamp is deliberately excluded: all versions of a row share
    /// one identity. Row-lock key for fiber.SemaphoreMap (and the
    /// future conflict oracle). Seed 0; TableKey.identityHash uses
    /// seed 1 so the two key shapes can never collide.
    pub fn identityHash(self: Key) u64 {
        var h = std.hash.XxHash3.init(0);
        h.update(std.mem.asBytes(&self.namespace));
        h.update(std.mem.asBytes(&self.table_id));
        h.update(self.primary);
        return h.final();
    }

    // We're no longer storing timestamp with negate op. When comparing keys, we are already
    // parsing timestamp separately, otherwise our variable length primary key could spill over
    // into comparison, making it wrong. As such, we can just do a basic u64 comparison, instead
    // of relying on a std.mem.order.
    fn writeTo(self: Key, buf: []u8) void {
        std.mem.writeInt(u64, buf[0..8], self.namespace, .big);
        std.mem.writeInt(u32, buf[8..12], self.table_id, .big);
        @memcpy(buf[12 .. 12 + self.primary.len], self.primary);
        std.mem.writeInt(u64, buf[12 + self.primary.len ..][0..8], self.timestamp, .big);
    }

    pub fn order(self: Key, other: Key) std.math.Order {
        const ns_ord = std.math.order(self.namespace, other.namespace);
        if (ns_ord != .eq) return ns_ord;
        const tid_ord = std.math.order(self.table_id, other.table_id);
        if (tid_ord != .eq) return tid_ord;
        return self.orderPrimary(other);
    }

    /// Order within one table: primary ascending, then timestamp
    /// DESCENDING so a key's newest version comes first. Callers that
    /// already know both keys share a (namespace, table_id) — a merge
    /// over one table's sources, say — skip those fields and call this.
    /// The sole definition of the reversed-timestamp rule; `order` is
    /// this plus the namespace/table_id prefix.
    pub fn orderPrimary(self: Key, other: Key) std.math.Order {
        const cmp = std.mem.order(u8, self.primary, other.primary);
        if (cmp != .eq) return cmp;

        if (self.timestamp == other.timestamp) return .eq;
        // Reverse the order when comparing timestamp.
        return if (self.timestamp > other.timestamp) .lt else .gt;
    }

    pub fn orderWithoutTs(self: Key, other: Key) std.math.Order {
        const ns_ord = std.math.order(self.namespace, other.namespace);
        if (ns_ord != .eq) return ns_ord;
        const tid_ord = std.math.order(self.table_id, other.table_id);
        if (tid_ord != .eq) return tid_ord;
        return std.mem.order(u8, self.primary, other.primary);
    }

    pub inline fn len(self: Key) usize {
        if (self.primary.len == 0) {
            return 0;
        }
        return 12 + self.primary.len + 8;
    }

    /// Hash of the primary key only — the per-table SST bloom-filter
    /// hash (namespace/table are implied by the table). NOT a row
    /// identity: use identityHash for locks and conflict tracking.
    pub fn bloomHash(self: Key) u64 {
        return std.hash.Wyhash.hash(0, self.primary);
    }

    /// Same primary key, ignoring namespace/table_id/timestamp.
    pub fn equalPrimary(self: Key, other: Key) bool {
        return std.mem.eql(u8, self.primary, other.primary);
    }
};
