// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Intrusive min-heap of pointers, with O(log N) middle removal via
//! a back-pointer index stored on each entry. Generic over the entry
//! type, the field name to compare on, and the field name used as
//! the back-pointer.
//!
//! Designed for the "deadline heap" use case where:
//!   - The entry struct lives on the caller's stack (intrusive index
//!     avoids the alloc-per-entry pattern of `std.PriorityQueue`).
//!   - **Middle removal (`remove`) is the common path** — e.g., a
//!     timeout is cancelled when the underlying op finishes. The
//!     back-pointer makes this O(log N) instead of the O(N) scan
//!     a value-based heap would require.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// `T` is the entry struct. `key_field` names a field of `T` whose
/// values define the min-heap order (smaller = closer to root).
/// `index_field` names a `usize` field on `T` that this heap uses
/// to track each entry's current slot. Initial value is irrelevant;
/// after `remove`, it's set to `not_in_heap`.
pub fn MinHeap(
    comptime T: type,
    comptime key_field: []const u8,
    comptime index_field: []const u8,
) type {
    return struct {
        const Self = @This();
        const Key = @TypeOf(@field(@as(T, undefined), key_field));

        items: std.ArrayList(*T) = .empty,

        pub const not_in_heap: usize = std.math.maxInt(usize);

        pub fn deinit(self: *Self, allocator: Allocator) void {
            self.items.deinit(allocator);
        }

        pub fn count(self: *const Self) usize {
            return self.items.items.len;
        }

        pub fn peekMin(self: *const Self) ?*T {
            if (self.items.items.len == 0) return null;
            return self.items.items[0];
        }

        pub fn push(self: *Self, allocator: Allocator, t: *T) !void {
            const idx = self.items.items.len;
            try self.items.append(allocator, t);
            @field(t, index_field) = idx;
            self.siftUp(idx);
        }

        pub fn popMin(self: *Self) ?*T {
            if (self.items.items.len == 0) return null;
            const root = self.items.items[0];
            const last = self.items.pop().?;
            if (self.items.items.len > 0 and last != root) {
                self.items.items[0] = last;
                @field(last, index_field) = 0;
                self.siftDown(0);
            }
            @field(root, index_field) = not_in_heap;
            return root;
        }

        /// O(log N) middle removal given a pointer to the entry.
        /// Safe on entries that aren't in the heap (no-op).
        pub fn remove(self: *Self, t: *T) void {
            const idx = @field(t, index_field);
            if (idx == not_in_heap) return;
            // The back-pointer must still name `t`'s own slot. A mismatch
            // means a stale index (double-remove, or a remove against the
            // wrong heap) that would otherwise silently relocate an unrelated
            // entry and corrupt the heap.
            std.debug.assert(self.items.items[idx] == t);

            const last = self.items.pop().?;
            if (last == t) {
                @field(t, index_field) = not_in_heap;
                return;
            }
            self.items.items[idx] = last;
            @field(last, index_field) = idx;
            // The replacement can need to move in either direction.
            self.siftDown(idx);
            self.siftUp(idx);
            @field(t, index_field) = not_in_heap;
        }

        fn siftUp(self: *Self, from: usize) void {
            var i = from;
            while (i > 0) {
                const parent = (i - 1) / 2;
                const p_key = @field(self.items.items[parent], key_field);
                const i_key = @field(self.items.items[i], key_field);
                if (p_key <= i_key) return;
                swapEntries(self, i, parent);
                i = parent;
            }
        }

        fn siftDown(self: *Self, from: usize) void {
            var i = from;
            const n = self.items.items.len;
            while (true) {
                const l = 2 * i + 1;
                const r = 2 * i + 2;
                var best = i;
                if (l < n) {
                    const l_key = @field(self.items.items[l], key_field);
                    const best_key = @field(self.items.items[best], key_field);
                    if (l_key < best_key) best = l;
                }
                if (r < n) {
                    const r_key = @field(self.items.items[r], key_field);
                    const best_key = @field(self.items.items[best], key_field);
                    if (r_key < best_key) best = r;
                }
                if (best == i) return;
                swapEntries(self, i, best);
                i = best;
            }
        }

        fn swapEntries(self: *Self, a: usize, b: usize) void {
            const tmp = self.items.items[a];
            self.items.items[a] = self.items.items[b];
            self.items.items[b] = tmp;
            @field(self.items.items[a], index_field) = a;
            @field(self.items.items[b], index_field) = b;
        }
    };
}
