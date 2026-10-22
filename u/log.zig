// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The server's log line:
//!
//!     2026-10-22T14:05:31.123456Z INFO  raft: become-leader tick=12 ...
//!
//! Time first (UTC, RFC 3339, microseconds) so lines sort and compare
//! across boxes; the level upper-case and padded so messages line up;
//! then the scope. Installed through `std_options.logFn` in main.zig
//! in place of std.log.defaultLog, whose `info(scope): msg` form has
//! no time. No colour: the log is a file. Scripts that count levels
//! match the second field (`^[^ ]+ ERROR `).

const std = @import("std");
const date = @import("date.zig");
const String = @import("utils.zig").String;

pub fn logFn(
    comptime level: std.log.Level,
    comptime scope: @EnumLiteral(),
    comptime format: String,
    args: anytype,
) void {
    // The real clock through the debug io: the same read as
    // `timestampNow`, but usable before the runtime and its process
    // context exist, where the first lines of main print.
    const io = std.Options.debug_io;
    const us = std.Io.Timestamp.now(io, .real).toMicroseconds();
    const prev = io.swapCancelProtection(.blocked);
    defer _ = io.swapCancelProtection(prev);
    var buffer: [256]u8 = undefined;
    const w = &std.debug.lockStderr(&buffer).file_writer.interface;
    defer std.debug.unlockStderr();
    writeLine(w, us, level, scope, format, args) catch {};
}

pub fn writeLine(
    w: *std.Io.Writer,
    unix_us: i64,
    comptime level: std.log.Level,
    comptime scope: @EnumLiteral(),
    comptime format: String,
    args: anytype,
) !void {
    try date.formatTimestampUtc(w, unix_us);
    const prefix = comptime " " ++ levelText(level) ++ " " ++
        (if (scope == .default) "egres" else @tagName(scope)) ++ ": ";
    try w.writeAll(prefix);
    try w.print(format ++ "\n", args);
}

/// Five wide, so the scope starts in the same column on every line.
fn levelText(comptime level: std.log.Level) String {
    return switch (level) {
        .err => "ERROR",
        .warn => "WARN ",
        .info => "INFO ",
        .debug => "DEBUG",
    };
}
