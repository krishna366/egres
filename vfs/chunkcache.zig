// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Local-disk cache of remote objects, in fixed-size chunks: chunk `idx`
//! of an object is its bytes [idx*C, (idx+1)*C) for C = `chunkBytes()`,
//! a grid over the object's bytes that knows nothing of block
//! boundaries. A block (any byte range) maps to the chunks it covers,
//! usually one. Chunks are keyed by (object name, idx).
//!
//! Planned layout: one preallocated slot file of equal slots, read and
//! written with direct I/O (a page fault would stall the worker thread,
//! not the fiber; and the page cache would hold a second copy of what
//! the block cache holds decoded). A header region at the front names
//! each slot's (object, idx, len), so a restart rebuilds the in-memory
//! map from it; S3-FIFO over slot ids decides eviction. The chunk size
//! is fixed for the file's life: set from config, or calibrated once
//! when the file is created and recorded in its header.
//!
//! Stub today: `enabled()` is false, every read misses, put drops the
//! bytes. The read paths are written against this API so the real
//! cache drops in without reshaping them.

const std = @import("std");
const utils = @import("utils");
const String = utils.String;

pub const ChunkCache = struct {
    chunk_bytes: u64,

    pub const Config = struct {
        /// Slot file path. Empty = no disk cache.
        path: String = "",
        capacity_bytes: u64 = 0,
        /// Chunk size when the slot file is created; 0 = calibrate
        /// against the endpoint then. An existing file keeps its own.
        chunk_bytes: u64 = 0,
    };

    /// Chunk size when neither config nor an existing file sets one.
    pub const default_chunk_bytes: u64 = 4 << 20;

    pub fn init(a: std.mem.Allocator, cfg: Config) !ChunkCache {
        _ = a;
        return .{
            .chunk_bytes = if (cfg.chunk_bytes > 0)
                cfg.chunk_bytes
            else
                default_chunk_bytes,
        };
    }

    pub fn deinit(self: *ChunkCache) void {
        _ = self;
    }

    /// False while there is no slot file: reads then fetch exact
    /// ranges from the remote, not whole chunks nothing would keep.
    pub fn enabled(self: *const ChunkCache) bool {
        _ = self;
        return false;
    }

    pub fn chunkBytes(self: *const ChunkCache) u64 {
        return self.chunk_bytes;
    }

    /// Copy `dst.len` bytes from offset `off` within chunk `idx` of
    /// `name`. False on a miss. Any `off`/`dst.len` inside the chunk;
    /// alignment for the slot file's direct I/O is handled here.
    /// Atomic to the caller: a slot evicted and refilled while the read
    /// is in flight must come back as a miss (pin the slot, or re-check
    /// its key after the read), never as another chunk's bytes.
    pub fn read(
        self: *ChunkCache,
        name: String,
        idx: u64,
        off: u64,
        dst: []u8,
    ) !bool {
        _ = self;
        _ = name;
        _ = idx;
        _ = off;
        _ = dst;
        return false;
    }

    /// Offer bytes [off, off+data.len) of chunk `idx` of `name`, just
    /// fetched. Kept only when they are the whole chunk (`off` 0 and
    /// `data.len` the chunk size, or the object's tail for its last
    /// chunk): a partial chunk would serve a short read as a hit later.
    /// Dropped when disabled.
    pub fn put(
        self: *ChunkCache,
        name: String,
        idx: u64,
        off: u64,
        data: String,
    ) !void {
        _ = self;
        _ = name;
        _ = idx;
        _ = off;
        _ = data;
    }

    /// Drop every chunk of `name` (the object was deleted). The slots
    /// are freed in the map; their bytes stay until reused.
    pub fn forget(self: *ChunkCache, name: String) void {
        _ = self;
        _ = name;
    }
};
