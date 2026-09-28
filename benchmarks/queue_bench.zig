// Single-threaded, uncontended micro-benchmarks of the per-worker ready queue
// `fiber.LocalQueue(.none)` vs `fiber.LocalQueue(.withlock)`.
//
// We measure the PER-OP overhead of guarding the owner's own push/pop with a
// `Spinlock`. `.none` is the owner-only ring the scheduler ships today — no
// atomics, no locking. `.withlock` wraps every push/pop in an uncontended
// `Spinlock` acquire+release (the cheapest path: a single test-and-set CAS
// that always succeeds, plus a relaxed store to unlock). The ratio between the
// two numbers answers: "is a spinlock cheap enough to protect a stealable
// LocalQueue, or do we need a lock-free deque?" — the question that motivated
// the Chase-Lev work-stealing experiment.
//
// `push`/`pop` flip `Fiber.qstate` via assertEnQ/assertDQ wherever runtime
// safety is on, so the fibers we feed the ring have to be real memory. They
// need not be real FIBERS though — nothing else in the struct is touched —
// so we hand out pointers into one zeroed slab and the hot loop still
// measures ring + lock cost alone, with no allocation inside it.
//
// Run via `zig build -Drelease bench` for accurate numbers (Debug bounds
// checks dominate the loop at this scale). Filter with
// `zig build bench -- --filter localqueue`.

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const bench = fiber.bench;
const Bench = bench.Bench;

const Fiber = fiber.Fiber;
const cap = 16; // bench ring depth; seeded as the ring's initial capacity

// A slab of distinct dummy fibers, zeroed so `qstate` starts at 0 and the
// push/pop assertions see the 0→1→0 cycle they expect. Distinct pointers
// (rather than one repeated) keep the stored slots varying so the compiler
// can't fold the ring into a constant. Synthetic @ptrFromInt addresses were
// used here once; they segfault the moment runtime safety turns those
// assertions on, i.e. in every build except the ReleaseFast one `zig build
// bench` produces.
fn dummies(alloc: std.mem.Allocator) ![]Fiber {
    const ds = try alloc.alloc(Fiber, cap);
    @memset(std.mem.sliceAsBytes(ds), 0);
    return ds;
}

// One push immediately followed by one pop: the dominant scheduler pattern
// (a fiber becomes ready, then is picked up to run), keeping the ring at
// depth 0↔1. ns/op = time for a single push+pop PAIR. In `.withlock` this is
// two uncontended lock/unlock cycles; in `.none`, none.
fn benchPushPop(b: *Bench, comptime mode: fiber.QueueLockMode) !void {
    var q = try fiber.LocalQueue(mode).init(std.heap.page_allocator, cap);
    defer q.deinit();
    const ds = try dummies(std.heap.page_allocator);
    defer std.heap.page_allocator.free(ds);
    b.resetTimer();

    var sink: usize = 0;
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        try q.push(&ds[i & (cap - 1)]);
        sink +%= @intFromPtr(q.pop().?);
    }
    std.mem.doNotOptimizeAway(sink);
}

// Fill the ring to capacity-1, then drain it. Exercises the non-trivial-depth
// path (wrap-around indexing, the size==capacity full check) rather than the
// always-near-empty fast path. ns/op = time for one full fill+drain cycle of
// (cap-1) pushes and (cap-1) pops.
fn benchFillDrain(b: *Bench, comptime mode: fiber.QueueLockMode) !void {
    // Seed at `cap` so the fill-to-`cap-1` loop never triggers a grow — we're
    // measuring steady-state push/pop + lock cost, not the realloc path.
    var q = try fiber.LocalQueue(mode).init(std.heap.page_allocator, cap);
    defer q.deinit();
    const ds = try dummies(std.heap.page_allocator);
    defer std.heap.page_allocator.free(ds);
    b.resetTimer();

    var sink: usize = 0;
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        var j: usize = 0;
        while (j < cap - 1) : (j += 1) try q.push(&ds[j]);
        while (q.pop()) |f| sink +%= @intFromPtr(f);
    }
    std.mem.doNotOptimizeAway(sink);
}

// ── Benchmarks ───────────────────────────────────────────────────────────

test "BENCH: localqueue nolock   push+pop" {
    try benchPushPop(bench.current(), .none);
}
test "BENCH: localqueue withlock push+pop" {
    try benchPushPop(bench.current(), .withlock);
}

test "BENCH: localqueue nolock   fill+drain" {
    try benchFillDrain(bench.current(), .none);
}
test "BENCH: localqueue withlock fill+drain" {
    try benchFillDrain(bench.current(), .withlock);
}
