const std = @import("std");
const fiber = @import("zfiber");

const SemaphoreMap = fiber.SemaphoreMap;
const gpa = std.testing.allocator;

fn waiterCount(sm: *SemaphoreMap, key: u64) usize {
    sm.sl.lock();
    defer sm.sl.unlock();
    const e = sm.entries.get(key) orelse return 0;
    return e.waiters.items.len;
}

/// Park the test until `key` has at least `n` registered waiters — the
/// point where a contender's wait edge is published and it is about to
/// (or already did) park in the entry's RWMutex.
fn awaitWaiters(sm: *SemaphoreMap, key: u64, n: usize) void {
    while (waiterCount(sm, key) < n) fiber.yield();
}

test "SemaphoreMap: uncontended acquire/release reclaims the entry" {
    var sm = try SemaphoreMap.init(gpa);
    defer sm.deinit();

    try std.testing.expect(try sm.acquire(1, 1, .exclusive));
    try std.testing.expect(try sm.acquire(2, 1, .shared));
    try std.testing.expectEqual(2, sm.entries.count());

    sm.release(1, 1);
    sm.release(2, 1);
    // Drained entries leave the map for the reuse pool.
    try std.testing.expectEqual(0, sm.entries.count());
    try std.testing.expectEqual(2, sm.pool.items.len);

    // The next acquire reuses a pooled entry instead of allocating.
    try std.testing.expect(try sm.acquire(3, 1, .exclusive));
    try std.testing.expectEqual(1, sm.pool.items.len);
    sm.release(3, 1);
    try std.testing.expectEqual(2, sm.pool.items.len);
}

test "SemaphoreMap: tryRelease releases a held key, else is a no-op" {
    var sm = try SemaphoreMap.init(gpa);
    defer sm.deinit();

    // Not held at all, and held by someone else: no-ops.
    try std.testing.expect(!sm.tryRelease(1, 1));
    try std.testing.expect(try sm.acquire(1, 2, .shared));
    try std.testing.expect(!sm.tryRelease(1, 1));
    try std.testing.expectEqual(1, sm.entries.count());

    // Held: released, and the entry drains like a plain release.
    try std.testing.expect(sm.tryRelease(1, 2));
    try std.testing.expectEqual(0, sm.entries.count());
    try std.testing.expect(!sm.tryRelease(1, 2));

    // The barrier pattern: take, drop, retake, then one deferred
    // tryRelease covers both the retaken and the never-retaken case.
    try std.testing.expect(try sm.acquire(5, 3, .exclusive));
    sm.release(5, 3);
    try std.testing.expect(try sm.acquire(5, 3, .exclusive));
    try std.testing.expect(!try sm.acquire(5, 3, .exclusive)); // re-entrant
    try std.testing.expect(sm.tryRelease(5, 3));
    try std.testing.expect(!sm.tryRelease(5, 3));
    try std.testing.expectEqual(0, sm.entries.count());
}

test "SemaphoreMap: re-entrant acquire by the holder is a no-op" {
    var sm = try SemaphoreMap.init(gpa);
    defer sm.deinit();

    try std.testing.expect(try sm.acquire(10, 7, .exclusive));
    // Same or weaker mode by the same owner: no park, no new hold.
    try std.testing.expect(!try sm.acquire(10, 7, .exclusive));
    try std.testing.expect(!try sm.acquire(10, 7, .shared));
    sm.release(10, 7);
    try std.testing.expectEqual(0, sm.entries.count());

    // Shared self re-acquire is likewise a no-op.
    try std.testing.expect(try sm.acquire(10, 7, .shared));
    try std.testing.expect(!try sm.acquire(10, 7, .shared));
    // But a shared holder asking for exclusive is refused, not upgraded.
    try std.testing.expectError(
        error.Upgrade,
        sm.acquire(10, 7, .exclusive),
    );
    sm.release(10, 7);
    try std.testing.expectEqual(0, sm.entries.count());
}

test "SemaphoreMap: FIFO hand-off under exclusive contention" {
    var sm = try SemaphoreMap.init(gpa);
    defer sm.deinit();

    try std.testing.expect(try sm.acquire(10, 100, .exclusive));

    // Three contenders queue behind us; each records its grant slot.
    var order: [3]u8 = .{ 0, 0, 0 };
    var next: std.atomic.Value(u8) = .init(0);
    const Body = struct {
        fn run(
            m: *SemaphoreMap,
            id: u8,
            slot_src: *std.atomic.Value(u8),
            ord: []u8,
        ) !void {
            try std.testing.expect(try m.acquire(10, id, .exclusive));
            ord[slot_src.fetchAdd(1, .monotonic)] = id;
            m.release(10, id);
        }
    };
    // Wait until each contender is queued before spawning the next, so
    // the FIFO order is deterministic.
    var h1 = try fiber.spawn(.{}, Body.run, .{ &sm, 1, &next, &order });
    awaitWaiters(&sm, 10, 1);
    var h2 = try fiber.spawn(.{}, Body.run, .{ &sm, 2, &next, &order });
    awaitWaiters(&sm, 10, 2);
    var h3 = try fiber.spawn(.{}, Body.run, .{ &sm, 3, &next, &order });
    awaitWaiters(&sm, 10, 3);

    sm.release(10, 100);
    try h1.join();
    try h2.join();
    try h3.join();

    try std.testing.expectEqual([3]u8{ 1, 2, 3 }, order);
    try std.testing.expectEqual(0, sm.entries.count());
}

test "SemaphoreMap: shared holders coexist; writer preference holds" {
    var sm = try SemaphoreMap.init(gpa);
    defer sm.deinit();

    // Three shared owners hold concurrently — none blocks.
    try std.testing.expect(try sm.acquire(10, 1, .shared));
    try std.testing.expect(try sm.acquire(10, 2, .shared));
    try std.testing.expect(try sm.acquire(10, 3, .shared));

    // A writer queues behind them...
    var writer_done: std.atomic.Value(bool) = .init(false);
    const Writer = struct {
        fn run(m: *SemaphoreMap, done: *std.atomic.Value(bool)) !void {
            try std.testing.expect(try m.acquire(10, 4, .exclusive));
            done.store(true, .release);
            m.release(10, 4);
        }
    };
    var hw = try fiber.spawn(.{}, Writer.run, .{ &sm, &writer_done });
    awaitWaiters(&sm, 10, 1);

    // ...and while it is pending, a NEW reader must queue too (writer
    // preference), not sneak past.
    var reader_done: std.atomic.Value(bool) = .init(false);
    const Reader = struct {
        fn run(m: *SemaphoreMap, done: *std.atomic.Value(bool)) !void {
            try std.testing.expect(try m.acquire(10, 5, .shared));
            done.store(true, .release);
            m.release(10, 5);
        }
    };
    var hr = try fiber.spawn(.{}, Reader.run, .{ &sm, &reader_done });
    awaitWaiters(&sm, 10, 2);
    try std.testing.expect(!reader_done.load(.acquire));

    // Writer runs first once the shared holders drain, then the reader.
    sm.release(10, 1);
    sm.release(10, 2);
    try std.testing.expect(!writer_done.load(.acquire));
    sm.release(10, 3);
    try hw.join();
    try hr.join();
    try std.testing.expect(writer_done.load(.acquire));
    try std.testing.expect(reader_done.load(.acquire));
    try std.testing.expectEqual(0, sm.entries.count());
}

test "SemaphoreMap: two-party deadlock refused at the closing edge" {
    var sm = try SemaphoreMap.init(gpa);
    defer sm.deinit();

    // Owners are tokens, not fibers: the test holds A as owner 1 and B
    // as owner 2 from the main fiber, then parks a helper as owner 1.
    try std.testing.expect(try sm.acquire(0xA, 1, .exclusive));
    try std.testing.expect(try sm.acquire(0xB, 2, .exclusive));

    const Body = struct {
        fn run(m: *SemaphoreMap) !void {
            // Owner 1 blocks on B (held by 2) — edge 1→2 published.
            try std.testing.expect(try m.acquire(0xB, 1, .exclusive));
            m.release(0xB, 1);
        }
    };
    var h = try fiber.spawn(.{}, Body.run, .{&sm});
    awaitWaiters(&sm, 0xB, 1);

    // Owner 2 requesting A would close 2→1→2: refused, never parks.
    try std.testing.expectError(error.Deadlock, sm.acquire(0xA, 2, .exclusive));

    // Owner 2 backs off; owner 1's blocked acquire proceeds.
    sm.release(0xB, 2);
    try h.join();
    sm.release(0xA, 1);
    try std.testing.expectEqual(0, sm.entries.count());
}

test "SemaphoreMap: deadlock through a shared holder is detected" {
    var sm = try SemaphoreMap.init(gpa);
    defer sm.deinit();

    try std.testing.expect(try sm.acquire(0xA, 1, .shared));
    try std.testing.expect(try sm.acquire(0xB, 2, .exclusive));

    const Body = struct {
        fn run(m: *SemaphoreMap) !void {
            // Owner 1 (shared holder of A) blocks on B exclusively.
            try std.testing.expect(try m.acquire(0xB, 1, .exclusive));
            m.release(0xB, 1);
        }
    };
    var h = try fiber.spawn(.{}, Body.run, .{&sm});
    awaitWaiters(&sm, 0xB, 1);

    // Owner 2's exclusive request on A waits on A's SHARED holder 1,
    // who waits on B held by 2 — a cycle through a shared edge.
    try std.testing.expectError(error.Deadlock, sm.acquire(0xA, 2, .exclusive));

    sm.release(0xB, 2);
    try h.join();
    sm.release(0xA, 1);
    try std.testing.expectEqual(0, sm.entries.count());
}

test "SemaphoreMap: three-party cycle is detected" {
    var sm = try SemaphoreMap.init(gpa);
    defer sm.deinit();

    try std.testing.expect(try sm.acquire(0xA, 1, .exclusive));
    try std.testing.expect(try sm.acquire(0xB, 2, .exclusive));
    try std.testing.expect(try sm.acquire(0xC, 3, .exclusive));

    const Blocked = struct {
        fn run(m: *SemaphoreMap, key: u64, owner: u64) !void {
            try std.testing.expect(try m.acquire(key, owner, .exclusive));
            m.release(key, owner);
        }
    };
    // 1 waits on B (held by 2); 2 waits on C (held by 3).
    var h1 = try fiber.spawn(.{}, Blocked.run, .{ &sm, 0xB, 1 });
    awaitWaiters(&sm, 0xB, 1);
    var h2 = try fiber.spawn(.{}, Blocked.run, .{ &sm, 0xC, 2 });
    awaitWaiters(&sm, 0xC, 1);

    // 3 requesting A closes 3→1→2→3 — DFS beyond depth two.
    try std.testing.expectError(error.Deadlock, sm.acquire(0xA, 3, .exclusive));

    // Unwind: 3 releases C → 2 completes; 2's main-held B frees → 1
    // completes; drain A.
    sm.release(0xC, 3);
    try h2.join();
    sm.release(0xB, 2);
    try h1.join();
    sm.release(0xA, 1);
    try std.testing.expectEqual(0, sm.entries.count());
}

test "BIG: SemaphoreMap: 64 owners cycling shared holds free drained entries" {
    // Stress for the drain-and-recycle path under real concurrency: with
    // more than `max_kept_capacity` holders at once, drained entries are
    // freed rather than pooled. `tryRelease` once unlocked the entry's rw
    // AFTER dropping `sl`, so a concurrent release could recycle (or
    // free) the entry first and the late unlock landed on a reset lock
    // or freed memory. That race showed under pgbench's I/O-bound load
    // and NOT here — three runs of this test passed on the broken
    // ordering — so this is a stress exercise of the path, not the
    // tripwire; the rw's underflow assert in a ReleaseSafe server is.
    // Opt-in (`--bigtest`): a few seconds in Debug.
    var sm = try SemaphoreMap.init(gpa);
    defer sm.deinit();

    const owners = 64;
    const rounds = 10_000;
    const Body = struct {
        fn run(map: *SemaphoreMap, owner: u64) void {
            var i: usize = 0;
            while (i < rounds) : (i += 1) {
                _ = map.acquire(1, owner, .shared) catch unreachable;
                _ = map.acquire(2, owner, .shared) catch unreachable;
                if (i % 7 == 0) fiber.yield();
                map.release(2, owner);
                map.release(1, owner);
            }
        }
    };
    var handles: [owners]fiber.JoinHandle(void) = undefined;
    for (&handles, 0..) |*h, i| {
        h.* = try fiber.spawn(.{}, Body.run, .{ &sm, @as(u64, i + 1) });
    }
    for (&handles) |*h| h.join();
    try std.testing.expectEqual(0, sm.entries.count());
}
