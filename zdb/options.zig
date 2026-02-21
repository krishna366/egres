const utils = @import("utils");

pub const Error = error{
    InvalidNumCompactors,
    InvalidMemtableSize,
    InvalidNumMemtables,
    InvalidBlockSize,
    InvalidTableSize,
    InvalidMaxLevels,
    InvalidDirForInMemory,
};

pub const Options = struct {
    dir: []const u8 = "",
    in_memory: bool = false,
    read_only: bool = false,

    memtable_size: usize = 64 * 1024 * 1024,
    num_memtables: u32 = 15,
    block_size: usize = 4 * 1024,
    table_size: usize = 8 * 1024 * 1024,
    compression: utils.compression.Codec = .none,
    max_levels: u8 = 7,
    num_compactors: u8 = 4,

    pub fn validate(self: Options) Error!void {
        // Badger semantics: exactly one compactor causes pathological level behavior.
        if (self.num_compactors == 1) return error.InvalidNumCompactors;
        if (self.memtable_size == 0) return error.InvalidMemtableSize;
        if (self.num_memtables == 0) return error.InvalidNumMemtables;
        if (self.block_size == 0) return error.InvalidBlockSize;
        if (self.table_size == 0) return error.InvalidTableSize;
        if (self.max_levels == 0) return error.InvalidMaxLevels;
        if (self.in_memory and self.dir.len != 0) return error.InvalidDirForInMemory;
    }
};
