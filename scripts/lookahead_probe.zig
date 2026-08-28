//! lookahead-probe — run the FileStore lookahead calibration against any
//! store backend — an S3-compatible endpoint or a local filesystem
//! depot — and print the latency profile. The same probing egres runs at
//! startup, as a standalone tool for sizing up a bucket provider
//! (SeaweedFS, B2, AWS, ...) or a local disk.
//!
//! Usage:
//!   AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
//!   lookahead-probe <endpoint-url> <bucket> [--region r] [--prefix p]
//!            [--block n]
//!   lookahead-probe --dir <depot-path> [--block n]
//!
//! The endpoint is a URL — https://s3.us-east-005.backblazeb2.com or
//! http://127.0.0.1:8333 — the scheme decides TLS, and DNS names resolve
//! at init.
//!
//! --dir runs the same calibration against a local filesystem depot
//! (O_DIRECT reads): the knee becomes device latency × device bandwidth
//! instead of round trip × link bandwidth.

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const vfs = @import("vfs");

var probe_ok = false;

pub fn main(init: std.process.Init) !void {
    utils.context.initGlobal(init);
    const allocator = utils.context.g().gpa;

    var fiber_rt: fiber.Runtime = undefined;
    try fiber_rt.init(allocator, .{});
    defer fiber_rt.deinit();
    try fiber.spawnDetached(.{}, probeAndStop, .{allocator});
    fiber_rt.run();
    if (!probe_ok) std.process.exit(1);
}

fn probeAndStop(a: std.mem.Allocator) void {
    defer fiber.stop();
    probe(a) catch |err| {
        std.debug.print("probe failed: {s}\n", .{@errorName(err)});
        return;
    };
    probe_ok = true;
}

fn probe(a: std.mem.Allocator) !void {
    const argv = try utils.context.g().argv();
    if (argv.len < 3) {
        std.debug.print(
            "usage: lookahead-probe <endpoint-url> <bucket> [--region r] [--prefix p] [--block n]\n" ++
                "       lookahead-probe --dir <depot-path> [--block n]\n" ++
                "endpoint-url: https://s3.us-east-005.backblazeb2.com or http://127.0.0.1:8333\n",
            .{},
        );
        return error.BadUsage;
    }

    if (std.mem.eql(u8, argv[1], "--dir")) {
        var block_size: u64 = 4096;
        var i: usize = 3;
        while (i < argv.len) : (i += 1) {
            if (std.mem.eql(u8, argv[i], "--block") and i + 1 < argv.len) {
                i += 1;
                block_size = try std.fmt.parseInt(u64, argv[i], 10);
            } else {
                std.debug.print("unknown arg '{s}'\n", .{argv[i]});
                return error.BadUsage;
            }
        }
        var store = try vfs.FileStore.initDir(a, argv[2]);
        defer store.deinit();
        const la = try store.calibrateLookahead(block_size);
        std.debug.print("ideal lookahead: {d} bytes ({d} KiB)\n", .{ la, la / 1024 });
        return;
    }

    var cfg = vfs.s3.S3Config{ .endpoint = argv[1], .bucket = argv[2] };
    var block_size: u64 = 4096;
    var i: usize = 3;
    while (i < argv.len) : (i += 1) {
        const arg = argv[i];
        if (std.mem.eql(u8, arg, "--region") and i + 1 < argv.len) {
            i += 1;
            cfg.region = argv[i];
        } else if (std.mem.eql(u8, arg, "--prefix") and i + 1 < argv.len) {
            i += 1;
            cfg.prefix = argv[i];
        } else if (std.mem.eql(u8, arg, "--block") and i + 1 < argv.len) {
            i += 1;
            block_size = try std.fmt.parseInt(u64, argv[i], 10);
        } else {
            std.debug.print("unknown arg '{s}'\n", .{arg});
            return error.BadUsage;
        }
    }

    var store = try vfs.s3.S3Store.init(a, cfg);
    defer store.deinit();

    const lookahead = try store.calibrateLookahead(block_size);
    std.debug.print("ideal lookahead: {d} bytes ({d} KiB)\n", .{ lookahead, lookahead / 1024 });
}
