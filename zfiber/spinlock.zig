// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Atomic test-and-test-and-set spinlock. ~15 LOC. Use for short
//! critical sections (≤ 10s of cycles) where setting up a futex
//! would dominate the cost. Wraps composite ops on data structures
//! that can't be made atomic at the field level — e.g., the
//! `WaitQueue`'s doubly-linked-list mutations, or the `Runtime`'s
//! fibers registry append/remove.
//!
//! Not for fiber-side waiting — a parked fiber on this would
//! block its worker OS thread. Mutexes that fibers wait on go
//! through `fiber.Mutex` (which uses this internally to protect
//! its own wait queue).

const std = @import("std");

pub const Spinlock = struct {
    state: std.atomic.Value(u32) align(std.atomic.cache_line) = .init(0),

    pub fn lock(self: *Spinlock) void {
        while (true) {
            // Test-then-CAS: spin on a relaxed load to keep the
            // cache line in shared state, only attempt the CAS once
            // we observe it's likely free. Avoids hammering the
            // line into modified state on every contender's CPU.
            if (self.state.load(.monotonic) == 0) {
                if (self.state.cmpxchgWeak(0, 1, .acquire, .monotonic) == null) return;
            }
            std.atomic.spinLoopHint();
        }
    }

    pub fn tryLock(self: *Spinlock) bool {
        return self.state.cmpxchgWeak(0, 1, .acquire, .monotonic) == null;
    }

    pub fn unlock(self: *Spinlock) void {
        self.state.store(0, .release);
    }
};
