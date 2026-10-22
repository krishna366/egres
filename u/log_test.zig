// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const log = @import("utils").log;

fn line(buf: []u8, unix_us: i64) ![]u8 {
    var w = std.Io.Writer.fixed(buf);
    try log.writeLine(&w, unix_us, .warn, .raft, "n={d}", .{7});
    return w.buffered();
}

test "log: timestamp, level and scope" {
    var buf: [128]u8 = undefined;
    try std.testing.expectEqualStrings(
        "1970-01-01T00:00:00.000000Z WARN  raft: n=7\n",
        try line(&buf, 0),
    );
    try std.testing.expectEqualStrings(
        "2025-10-22T14:05:31.123456Z WARN  raft: n=7\n",
        try line(&buf, 1_761_141_931_123_456),
    );
    // A leap day, the last microsecond of it.
    try std.testing.expectEqualStrings(
        "2024-02-29T23:59:59.999999Z WARN  raft: n=7\n",
        try line(&buf, 1_709_251_199_999_999),
    );
}

test "log: the default scope prints as egres" {
    var buf: [128]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try log.writeLine(&w, 1_000_000, .err, .default, "x", .{});
    try std.testing.expectEqualStrings(
        "1970-01-01T00:00:01.000000Z ERROR egres: x\n",
        w.buffered(),
    );
}
