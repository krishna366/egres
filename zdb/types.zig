pub const bit_delete: u8 = 1 << 0;
pub const bit_discard_earlier_versions: u8 = 1 << 2;

pub const Entry = struct {
    key: []const u8,
    value: []const u8,
    version: u64 = 0,
    user_meta: u8 = 0,
    meta: u8 = 0,

    pub fn init(key: []const u8, value: []const u8) Entry {
        return .{
            .key = key,
            .value = value,
        };
    }

    pub fn withVersion(self: Entry, version: u64) Entry {
        var out = self;
        out.version = version;
        return out;
    }

    pub fn withUserMeta(self: Entry, user_meta: u8) Entry {
        var out = self;
        out.user_meta = user_meta;
        return out;
    }

    pub fn withDelete(self: Entry) Entry {
        var out = self;
        out.meta |= bit_delete;
        return out;
    }

    pub fn withDiscardEarlierVersions(self: Entry) Entry {
        var out = self;
        out.meta |= bit_discard_earlier_versions;
        return out;
    }

    pub fn estimateSize(self: Entry) usize {
        return self.key.len + self.value.len + 2;
    }

    pub fn isDelete(self: Entry) bool {
        return (self.meta & bit_delete) != 0;
    }

    pub fn discardEarlierVersions(self: Entry) bool {
        return (self.meta & bit_discard_earlier_versions) != 0;
    }
};

