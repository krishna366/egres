// Hash throughput benchmarks. Three implementations are compared:
//
//   1. `std.hash.crc.Crc32Iscsi.hash` — table-based software CRC.
//   2. `crc32cHw` (this file)         — inline SSE4.2 `crc32q` for 8-byte
//                                       chunks + std software CRC for the
//                                       0–7 byte tail. Bit-compatible with
//                                       column 1.
//   3. `std.hash.XxHash3.hash`        — what zbuf framing now ships.
//                                       Modern non-cryptographic hash, uses
//                                       @Vector(8, u64) blocks → AVX2 SIMD
//                                       on x86_64.
//
// Production bytes flow through XxHash3 — see `zbuf/zbuf.zig` for the
// rationale on why xxhash3 over CRC. We keep the others to keep the speed
// gap motivating the switch inspectable.
//
// Run via `zig build -Drelease bench` for accurate numbers (Debug bounds
// checks dominate at small sizes).

const std = @import("std");
const builtin = @import("builtin");
const utils = @import("utils");
const String = utils.String;
const bench = utils.bench;
const Bench = bench.Bench;

// ── Hardware CRC32C (x86_64-only, kept local to the bench) ────────────────

inline fn crc32qStep(crc_in: u64, v: u64) u64 {
    return asm ("crc32q %[v], %[c]"
        : [c] "=r" (-> u64),
        : [v] "r" (v),
          [crcin] "0" (crc_in),
    );
}

/// Hybrid hardware/software CRC32C. Bit-compatible with Crc32Iscsi.hash.
/// Hardware path used for 8-byte chunks; std software CRC handles the 0–7
/// byte tail (sidesteps the self-hosted x86 backend's missing crc32b
/// encoding without changing output bits).
fn crc32cHw(payload: String) u32 {
    var crc: u64 = 0xFFFF_FFFF;
    var i: usize = 0;
    while (i + 8 <= payload.len) : (i += 8) {
        const v = std.mem.readInt(u64, payload[i..][0..8], .little);
        crc = crc32qStep(crc, v);
    }
    var sw = std.hash.crc.Crc32Iscsi{ .crc = @truncate(crc) };
    sw.update(payload[i..]);
    return sw.final();
}

fn fillRandom(buf: []u8) void {
    var prng = std.Random.DefaultPrng.init(0xC0FFEE);
    prng.random().bytes(buf);
}

// ── Per-impl benchmark functions (Go-style: take *Bench) ─────────────────
//
// Mutate one byte of the payload per iteration so LLVM can't hoist the
// hash out of the loop (which it does for tiny inputs to XxHash3,
// producing nonsensical multi-TiB/s numbers). One-byte write per iter is
// ≪ the hash work at every measured size.

fn benchSw(b: *Bench, size: usize) !void {
    const buf = try b.alloc.alloc(u8, size);
    defer b.alloc.free(buf);
    fillRandom(buf);
    b.resetTimer();

    var sink: u32 = 0;
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        buf[0] = @truncate(i);
        sink ^= std.hash.crc.Crc32Iscsi.hash(buf);
        b.addBytes(buf.len);
    }
    std.mem.doNotOptimizeAway(sink);
}

fn benchHw(b: *Bench, size: usize) !void {
    if (builtin.cpu.arch != .x86_64) return error.SkipZigTest;

    const buf = try b.alloc.alloc(u8, size);
    defer b.alloc.free(buf);
    fillRandom(buf);
    b.resetTimer();

    var sink: u32 = 0;
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        buf[0] = @truncate(i);
        sink ^= crc32cHw(buf);
        b.addBytes(buf.len);
    }
    std.mem.doNotOptimizeAway(sink);
}

fn benchXxHash3(b: *Bench, size: usize) !void {
    const buf = try b.alloc.alloc(u8, size);
    defer b.alloc.free(buf);
    fillRandom(buf);
    b.resetTimer();

    var sink: u64 = 0;
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        buf[0] = @truncate(i);
        sink ^= std.hash.XxHash3.hash(0, buf);
        b.addBytes(buf.len);
    }
    std.mem.doNotOptimizeAway(sink);
}

// ── Benchmarks ───────────────────────────────────────────────────────────

const small: usize = 64;
const medium: usize = 4 * 1024;
const large: usize = 64 * 1024;

test "BENCH: 64B crc32c-sw" { try benchSw(bench.current(), small); }
test "BENCH: 64B crc32c-hw" { try benchHw(bench.current(), small); }
test "BENCH: 64B xxhash3" { try benchXxHash3(bench.current(), small); }

test "BENCH: 4KiB crc32c-sw" { try benchSw(bench.current(), medium); }
test "BENCH: 4KiB crc32c-hw" { try benchHw(bench.current(), medium); }
test "BENCH: 4KiB xxhash3" { try benchXxHash3(bench.current(), medium); }

test "BENCH: 64KiB crc32c-sw" { try benchSw(bench.current(), large); }
test "BENCH: 64KiB crc32c-hw" { try benchHw(bench.current(), large); }
test "BENCH: 64KiB xxhash3" { try benchXxHash3(bench.current(), large); }
