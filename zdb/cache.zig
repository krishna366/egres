// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The block cache: the generic S3-FIFO `cache` module over
//! ref-counted byte buffers, budgeted in bytes the process pays (the
//! buffers plus the cache's bookkeeping). Values are `*Value`; the
//! cache incrRefs on put/get and decrRefs on eviction/deinit, and
//! callers decrRef what `get` returns.

const utils = @import("utils");
const cache = @import("cache");

pub const Value = utils.RefValue;
pub const Ghost = cache.Ghost;
pub const Hit = cache.Hit;
pub const hashKey = cache.hashKey;

pub fn Cache(comptime K: type) type {
    return cache.Cache(K, *Value, .cost_and_overhead);
}

pub fn ShardedCache(comptime K: type) type {
    return cache.ShardedCache(K, *Value, .cost_and_overhead);
}
