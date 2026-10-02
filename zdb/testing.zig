// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Helpers for tests that drive a Forest directly, without an Engine
//! or Raft. Not a `_test.zig` file, so it holds no tests of its own;
//! test files import it by path.
const std = @import("std");
const zdb = @import("zdb");

const Forest = zdb.forest.Forest;
const Tree = zdb.tree.Tree;
const TableKey = zdb.kv.TableKey;
const KeyRange = zdb.kv.KeyRange;
const _manifest = zdb.manifest;

/// Create a tree for `table_key` over `range` in one node's manifest
/// and install it, as a proposal's apply would: no Raft entry behind
/// it, so it is listed at index 0. Returns the new tree_id, derived
/// from the table's next free split index.
pub fn createTree(
    f: *Forest,
    table_key: TableKey,
    range: KeyRange,
    kind: _manifest.TreeKind,
) !u64 {
    const tree_id = f.nextTreeId(table_key);

    const left_raw = try range.left.marshal(f.allocator);
    defer f.allocator.free(left_raw);
    const right_raw = try range.right.marshal(f.allocator);
    defer f.allocator.free(right_raw);

    try f.manifest.commit(0, &.{}, &.{
        _manifest.createTreeChange(
            tree_id,
            table_key.namespace,
            left_raw,
            right_raw,
            kind,
        ),
    });
    try f.installTree(table_key.namespace, tree_id);
    return tree_id;
}

/// Builds the tree's next compaction as the engine does: the job is
/// taken (`reserveJob`), built, and released. Empty when the tree
/// wants nothing. The caller installs the changes (`applyCompaction`).
pub fn buildCompaction(
    tree: *Tree,
    snapshot_ts: u64,
) !std.ArrayList(_manifest.SSTableChange) {
    const job = try tree.reserveJob() orelse return .empty;
    defer tree.releaseJob(job);
    return tree.buildCompaction(job, snapshot_ts);
}
