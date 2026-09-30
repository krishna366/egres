// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Write-side validation of roaring posting values.
//!
//! A posting is the set of u64 row ids (egids) stored under one index
//! key. Two value layouts share `Meta.encoding = .roaring`:
//!
//!   kind = .state  — a serialized zroar bitmap: the full set.
//!   kind = .delta  — a zbuf `RoaringDelta` message (zdb/buffers.zig):
//!                    egids to add and remove, layered on the versions
//!                    below it. Writers use RoaringDeltaWriter
//!                    directly; stored without a message tail — the
//!                    meta byte already types the value.
//!
//! This module owns only the write-side format concerns: what a
//! delta record is and `validateDelta`, the gate every record passes
//! before being proposed. Everything runtime — chain classification and
//! THE fold — lives in zdb/compaction.zig's VersionTracker, which sees a
//! primary's whole chain in one item (the fold is `foldChain`, and reads
//! fold through the same `baseBitmap` / `applyDelta` pair; that sharing
//! is what makes "rollup is read-invariant" testable).
//!
//! Both delta lists are strictly ascending and mutually disjoint —
//! WRITER contracts, enforced at write time via `validateDelta` (the
//! mandatory gate before a delta is proposed; bytes can't rot after
//! that — SST blocks are checksummed). Ascending is the order zroar
//! mutations want; disjointness holds because a row's insert and
//! delete are serialized by its row lock, so one batch can't both add
//! and remove an egid. Reads do NOT re-validate content: the fold
//! checks only structure (bounds), and an out-of-contract record that
//! slipped past a buggy writer still folds deterministically by set
//! semantics (removes-then-adds per delta) on every node.

const std = @import("std");
const _buffers = @import("buffers.gen.zig");
const utils = @import("utils");
const String = utils.String;

const DeltaReader = _buffers.RoaringDeltaReader;

pub const Error = error{CorruptPosting};

/// The write-side gate: every delta record MUST pass this before it
/// is proposed. Bounds-verifies the record and checks both lists are
/// strictly ascending and mutually disjoint — content contracts,
/// enforced once per record lifetime, never on reads.
pub fn validateDelta(value: String) Error!void {
    const r = DeltaReader.init(value, 0) catch return Error.CorruptPosting;
    r.verify() catch return Error.CorruptPosting;

    var i: u32 = 0;
    while (i < r.addsLen()) : (i += 1) {
        const v = r.addsAt(i) catch return Error.CorruptPosting;
        if (i > 0 and v <= (r.addsAt(i - 1) catch unreachable))
            return Error.CorruptPosting;
    }
    i = 0;
    while (i < r.removesLen()) : (i += 1) {
        const v = r.removesAt(i) catch return Error.CorruptPosting;
        if (i > 0 and v <= (r.removesAt(i - 1) catch unreachable))
            return Error.CorruptPosting;
    }

    // Disjointness: one batch cannot add and remove the same egid (a
    // row's insert and delete are serialized by its row lock), so an
    // egid in both lists has no defined meaning. Without this check
    // the fold's removes-then-adds order would silently resolve the
    // overlap to "added". Both lists are sorted, so a linear merge
    // finds any collision; access is infallible after the loops above.
    var a: u32 = 0;
    var b: u32 = 0;
    while (a < r.addsLen() and b < r.removesLen()) {
        const av = r.addsAt(a) catch unreachable;
        const rv = r.removesAt(b) catch unreachable;
        if (av == rv) return Error.CorruptPosting;
        if (av < rv) a += 1 else b += 1;
    }
}
