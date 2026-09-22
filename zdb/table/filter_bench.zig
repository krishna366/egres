//! Size of the two SST filters over egid key sets: the binary fuse
//! filter (hashes of the keys) against a roaring bitmap of the keys
//! themselves. Run with `zig build bench` and read the table it prints.
const std = @import("std");
const zdb = @import("zdb");
const zroar = zdb.zroar;
const BinaryFuse = zdb.binaryfusefilter.BinaryFuse;

/// How an egid set is shaped: the ids a table holds out of a range.
const Shape = enum {
    /// Every id in the range: a fresh table, or a compacted range with
    /// no deletes.
    dense,
    /// One in two, alternating: heavy deletes, every other row gone.
    half,
    /// One in ten.
    tenth,
    /// One in a hundred: a cold range after most rows moved on.
    hundredth,
    /// Random 50%: deletes with no pattern.
    random_half,
    /// Random 5%.
    random_5pct,
};

fn egidsOf(alloc: std.mem.Allocator, shape: Shape, n: usize) ![]u64 {
    var out: std.ArrayList(u64) = .empty;
    var prng = std.Random.DefaultPrng.init(42);
    const rng = prng.random();
    var id: u64 = 1;
    while (out.items.len < n) : (id += 1) {
        const keep = switch (shape) {
            .dense => true,
            .half => id % 2 == 0,
            .tenth => id % 10 == 0,
            .hundredth => id % 100 == 0,
            .random_half => rng.boolean(),
            .random_5pct => rng.uintLessThan(u32, 100) < 5,
        };
        if (keep) try out.append(alloc, id);
    }
    return out.toOwnedSlice(alloc);
}

test "BENCH: fuse vs roaring filter size over egid sets" {
    const alloc = std.testing.allocator;
    std.debug.print("\n{s:<12} {s:>9} {s:>11} {s:>11} {s:>11} {s:>8}\n", .{
        "shape", "keys", "span", "fuse_bytes", "roar_bytes", "ratio",
    });
    for ([_]usize{ 4_096, 65_536, 1_000_000 }) |n| {
        for (std.enums.values(Shape)) |shape| {
            const egids = try egidsOf(alloc, shape, n);
            defer alloc.free(egids);

            const hashes = try alloc.alloc(u64, n);
            defer alloc.free(hashes);
            for (egids, hashes) |e, *h| {
                var kb: [8]u8 = undefined;
                std.mem.writeInt(u64, &kb, e, .big);
                h.* = zdb.kv.bloomHash(&kb);
            }
            var fuse = try BinaryFuse(u16).build(alloc, hashes);
            defer fuse.deinit(alloc);
            const fuse_bytes = fuse.fingerprints.len * 2;

            var bm = try zroar.Bitmap.fromSortedList(alloc, egids);
            defer bm.deinit();
            const roar_bytes = bm.toBuffer().len;

            std.debug.print("{s:<12} {d:>9} {d:>11} {d:>11} {d:>11} {d:>8.2}\n", .{
                @tagName(shape),
                n,
                egids[egids.len - 1] - egids[0] + 1,
                fuse_bytes,
                roar_bytes,
                @as(f64, @floatFromInt(roar_bytes)) / @as(f64, @floatFromInt(fuse_bytes)),
            });
        }
    }
}
