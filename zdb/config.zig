const std = @import("std");
const utils = @import("utils");

pub const Error = error{
    InvalidNumCompactors,
    InvalidMemtableSize,
    InvalidNumMemtables,
    InvalidBlockSize,
    InvalidMaxLevels,
    InvalidDirForInMemory,
};

pub const Config = struct {
    dir: []const u8 = "",
    in_memory: bool = false,
    read_only: bool = false,

    memtable_size: usize = 64 * 1024 * 1024,
    num_memtables: u32 = 15,

    block_size: usize = 64 * 1024,
    compression: utils.compression.Codec = .lz4,
    num_compactors: u8 = 2,

    // Level sizes. Lmax target is derived from `tree_split_size`; upper
    // levels cascade by dividing by `level_size_multiplier`. There is no
    // separate base-level-size knob — every tree caps at the split size.
    level_size_multiplier: i64 = 5,
    max_levels: u8 = 4,

    // Per-level SST file size. L0 inherits the memtable size. L1 uses
    // `l1_table_size`; each level below ramps by `table_size_multiplier`
    // up to `max_table_size`.
    l1_table_size: i64 = 32 * 1024 * 1024,
    table_size_multiplier: i64 = 2,
    max_table_size: i64 = 128 * 1024 * 1024,

    // Tree split threshold. Compared against the bottom level (Lmax) size
    // after each compaction; when Lmax exceeds this, the tree splits at the
    // median key into two trees. Lmax holds ~90% of an LSM tree's data, so
    // this is effectively the per-tree size cap.
    tree_split_size: i64 = 5 * 1024 * 1024 * 1024, // 5 GiB

    // L0 backpressure (RocksDB-style write slowdown). When a tree's L0
    // table count exceeds `l0_backpressure_start_count`, mutations sleep
    // `(excess) * l0_backpressure_step_us` microseconds before proposing,
    // capped at `l0_backpressure_max_us`. The intent is to give flush +
    // compaction a chance to drain L0 instead of letting it grow into
    // 10s-of-GiB monolithic compaction targets. This is a *trigger*, not
    // a cap — L0 may still grow past it; writes just slow down. With
    // defaults, a tree at the trigger imposes 0 ms; +10 tables ~10 ms;
    // +100 tables hits the cap of 100 ms. Set
    // `l0_backpressure_start_count = 0` to disable backpressure entirely.
    l0_backpressure_start_count: u32 = 8,
    l0_backpressure_step_us: u32 = 1000,
    l0_backpressure_max_us: u32 = 100_000,

    // Per-compaction byte budget for L0→L1: the picker walks L0 oldest-
    // first and stops once the total input bytes (sum of selected L0
    // file sizes + their cumulative L1 overlap file sizes) would exceed
    // this. Always picks at least one L0 table so a single oversized
    // input still makes progress. Backpressure normally keeps L0 small;
    // this is the safety bound that prevents a flush burst from landing
    // in one multi-GiB compaction that blocks the loop for minutes.
    l0_compaction_max_bytes: u64 = 1 << 30, // 1 GiB

    // Roaring posting rollup (zdb/compaction.zig's VersionTracker).
    // Compaction folds a
    // version run whose newest item is a roaring delta into one full
    // bitmap state, so reads stop at their first hit instead of
    // walking a delta chain.
    //
    // A chain whose base state is among the compaction's own inputs
    // rolls up for free (the merge already streams every input byte
    // through a rebuild), gated only by `rollup_min_deltas` — a short
    // chain isn't worth rewriting a large bitmap for; reads fold it
    // on the fly.
    //
    // A chain WITHOUT its base must fetch it from the levels below
    // the destination — the one rollup path that costs extra read
    // I/O. Two triggers allow it, either one suffices:
    //   dest >= rollup_at_level   compacting into this level or
    //     deeper always regenerates, so the deep levels converge on
    //     one state per posting (default 2: L0→L1 stays free-only,
    //     L1→L2 and below regenerate).
    //   chain >= rollup_max_deltas   a chain this long regenerates at
    //     ANY level, bounding read-path fold depth and amortizing the
    //     deep seek to one per max_deltas writes to that index key.
    // Chain length is also what bounds rollup buffering — no
    // separate byte caps.
    rollup_enabled: bool = true,
    rollup_min_deltas: u32 = 1,
    rollup_at_level: u8 = 2,
    rollup_max_deltas: u32 = 32,

    pub fn validate(self: Config) Error!void {
        // Badger semantics: exactly one compactor causes pathological level
        // behavior.
        if (self.num_compactors == 1) return error.InvalidNumCompactors;
        if (self.memtable_size == 0) return error.InvalidMemtableSize;
        if (self.num_memtables == 0) return error.InvalidNumMemtables;
        if (self.block_size == 0) return error.InvalidBlockSize;
        if (self.max_levels == 0) return error.InvalidMaxLevels;
        if (self.in_memory and
            self.dir.len != 0) return error.InvalidDirForInMemory;
    }
};
