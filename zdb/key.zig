const std = @import("std");

pub const Item = struct {
    key: Key,
    value: []const u8,

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
};

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
        const cmp = self.orderWithoutTs(other);
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

    /// Hash primary key only (for bloom filter).
    pub fn hash(self: Key) u64 {
        return std.hash.Wyhash.hash(0, self.primary);
    }

    /// Same primary key, ignoring namespace/table_id/timestamp.
    pub fn equalPrimary(self: Key, other: Key) bool {
        return std.mem.eql(u8, self.primary, other.primary);
    }
};
