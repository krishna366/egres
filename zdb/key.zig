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

pub const Key = struct {
    namespace: u32 = 0,
    table_id: u32 = 0,
    primary: []const u8 = "",
    timestamp: u64 = 0,

    /// Parse a marshaled key: [ns:u32 BE][tid:u32 BE][primary][ts:u64 BE].
    /// For keys without the full prefix/suffix, construct Key directly via
    /// Key{ .primary = ... }.
    pub fn init(raw: []const u8) Key {
        if (raw.len == 0) {
            return .{};
        }
        const ts_idx = raw.len - 8;
        return .{
            .namespace = std.mem.readInt(u32, raw[0..4], .big),
            .table_id = std.mem.readInt(u32, raw[4..8], .big),
            .primary = raw[8..ts_idx],
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
        const buf = try allocator.alloc(u8, 8 + self.primary.len + 8);
        self.writeTo(buf);
        return buf;
    }

    pub fn marshalTo(self: Key, allocator: std.mem.Allocator, data: *std.ArrayList(u8)) ![]const u8 {
        if (self.primary.len == 0) {
            return allocator.alloc(u8, 0);
        }
        const start = data.items.len;
        try data.appendNTimes(allocator, 0, 8 + self.primary.len + 8);
        const buf = data.items[start..];
        self.writeTo(buf);
        return buf;
    }

    // We're no longer storing timestamp with negate op. When comparing keys, we are already
    // parsing timestamp separately, otherwise our variable length primary key could spill over
    // into comparison, making it wrong. As such, we can just do a basic u64 comparison, instead
    // of relying on a std.mem.order.
    fn writeTo(self: Key, buf: []u8) void {
        std.mem.writeInt(u32, buf[0..4], self.namespace, .big);
        std.mem.writeInt(u32, buf[4..8], self.table_id, .big);
        @memcpy(buf[8 .. 8 + self.primary.len], self.primary);
        std.mem.writeInt(u64, buf[8 + self.primary.len ..][0..8], self.timestamp, .big);
    }

    pub fn order(self: Key, other: Key) std.math.Order {
        const cmp = self.orderWithoutTs(other);
        if (cmp != .eq) return cmp;

        if (self.timestamp == other.timestamp) return .eq;
        // Reverse the order when comparing timestamp.
        return if (self.timestamp > other.timestamp) .lt else .gt;
    }

    pub fn orderWithoutTs(self: Key, other: Key) std.math.Order {
        if (self.namespace != 0 and other.namespace != 0) {
            const ns_ord = std.math.order(self.namespace, other.namespace);
            if (ns_ord != .eq) return ns_ord;
        }
        if (self.table_id != 0 and other.table_id != 0) {
            const tid_ord = std.math.order(self.table_id, other.table_id);
            if (tid_ord != .eq) return tid_ord;
        }
        return std.mem.order(u8, self.primary, other.primary);
    }

    pub inline fn len(self: Key) usize {
        if (self.primary.len == 0) {
            return 0;
        }
        return 8 + self.primary.len + 8;
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
