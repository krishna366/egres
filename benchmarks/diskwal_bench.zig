// DiskWAL micro-benchmarks: append + (optionally) fsync records through the
// raw `raft.diskwal` layer, bypassing Raft itself. Isolates the WAL from
// the propose queue / quorum / apply loop so we can tell whether the
// end-to-end ceiling we see in the egres load test is the WAL or
// something upstream.
//
// Run via `zig build bench`. Each cell auto-tunes its iteration count;
// each iteration appends one batch (and fsyncs per `fsync_mode`).
//
// What this file no longer does (vs. the previous standalone executable):
//   - No 3D matrix sweep (payload × batch × fsync). We pick representative
//     cells; if the matrix view is needed for a one-off perf investigation,
//     run cells via `zig build bench -- --filter <name>`.
//   - No p50/p99/max latency reporting. The framework reports `ns/op` (which
//     is the per-batch latency for these benches). Add latency percentiles
//     to the framework if you need them — see u/bench.zig + test_runner.zig.

const std = @import("std");
const raft = @import("raft");
const vfs = @import("vfs");
const utils = @import("utils");
const bench = utils.bench;
const Bench = bench.Bench;

const zen = raft.buffers;
const diskwal = raft.diskwal;

const FsyncMode = enum {
    /// Append only, no fsync. Measures the OS write-cache ceiling.
    never,
    /// One fsync per batch. Models normal Raft batched commit.
    per_batch,
    /// One fsync per entry. Worst case for fsync-bound workloads.
    per_append,
};

/// Run one bench cell. The framework drives `b.n` outer iterations; each
/// iteration appends `batch_size` entries (plus optional fsync). Setup —
/// dir creation, payload allocation, ArrayList prep — is excluded from
/// the measurement via `b.resetTimer()`.
fn benchOne(
    b: *Bench,
    payload_size: usize,
    batch_size: u32,
    fsync_mode: FsyncMode,
) !void {
    // VFS uses a global bandwidth limiter; raise it so the bench measures
    // the WAL, not the throttle. Idempotent if already initialized; safe
    // to call across samples.
    vfs.VFS.initGlobal(.{ .bytes_per_sec = 10 * 1024 * 1024 * 1024 });

    // Fresh dir per sample. `std.testing.tmpDir` puts each one under
    // `.zig-cache/tmp/<random_hex>/` — real-disk path (NOT tmpfs, so
    // fsync actually waits for the disk), gitignored, unique per call so
    // concurrent `zig build bench` invocations don't collide.
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const io = utils.context.g().io;
    const dir = try tmp.dir.realPathFileAlloc(io, ".", b.alloc);
    defer b.alloc.free(dir);

    var dwal = try diskwal.DiskWAL.openPath(dir, b.alloc, 0);
    defer dwal.deinit();

    const payload = try b.alloc.alloc(u8, payload_size);
    defer b.alloc.free(payload);
    for (payload, 0..) |*c, idx| c.* = @truncate(idx ^ 0xA5);

    // Reusable batch scratch — entries get rebuilt each iter (index must
    // be monotonic) but the ArrayList stays put.
    var batch: std.ArrayList(zen.LogEntryBuffer) = .empty;
    defer {
        for (batch.items) |*e| e.deinit();
        batch.deinit(b.alloc);
    }
    try batch.ensureTotalCapacity(b.alloc, batch_size);

    b.resetTimer();

    const bytes_per_iter: u64 = @as(u64, batch_size) * @as(u64, payload_size);
    var next_index: u64 = 1;
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        for (batch.items) |*e| e.deinit();
        batch.clearRetainingCapacity();
        var k: u32 = 0;
        while (k < batch_size) : (k += 1) {
            var writer = try zen.LogEntryWriter.init(b.alloc);
            defer writer.deinit();
            try writer.set(.{
                .index = next_index,
                .term = 1,
                .timestamp = 0,
                .data = payload,
                .entry_type = zen.EntryTypeEnum.data,
                .conf_change = zen.NullOffset,
            });
            batch.appendAssumeCapacity(try writer.finishWithTail());
            next_index += 1;
        }

        switch (fsync_mode) {
            .per_append => for (batch.items) |*e| {
                try dwal.wal_store.addEntries(&.{e.*});
                try dwal.wal_store.sync(true);
            },
            .per_batch => {
                try dwal.wal_store.addEntries(batch.items);
                try dwal.wal_store.sync(true);
            },
            .never => try dwal.wal_store.addEntries(batch.items),
        }
        b.addBytes(bytes_per_iter);
    }
}

// ── Benchmarks ───────────────────────────────────────────────────────────

test "BENCH: diskwal 1KiB batch=1 per-batch-fsync" {
    try benchOne(bench.current(), 1024, 1, .per_batch);
}

test "BENCH: diskwal 1KiB batch=16 per-batch-fsync" {
    try benchOne(bench.current(), 1024, 16, .per_batch);
}

test "BENCH: diskwal 1KiB batch=256 per-batch-fsync" {
    try benchOne(bench.current(), 1024, 256, .per_batch);
}

test "BENCH: diskwal 1KiB batch=1 no-fsync" {
    try benchOne(bench.current(), 1024, 1, .never);
}

test "BENCH: diskwal 1KiB batch=16 no-fsync" {
    try benchOne(bench.current(), 1024, 16, .never);
}

test "BENCH: diskwal 1KiB batch=256 no-fsync" {
    try benchOne(bench.current(), 1024, 256, .never);
}
