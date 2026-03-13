const std = @import("std");
const zio = @import("zio");

pub const Command = union(enum) {
    set: SetCmd,
    get: GetCmd,
};

pub const SetCmd = struct {
    ns: u32,
    key: []const u8,
    ts: u64,
    value: []const u8,
};

pub const GetCmd = struct {
    ns: u32,
    key: []const u8,
    ts: u64,
};

const max_line_len = 64 * 1024;

/// Read one text command from the stream.
/// Format: "set <ns> <key> <ts> <value>\n" or "get <ns> <key> <ts>\n"
/// Returns null on EOF.
pub fn readCommand(stream: zio.net.Stream, buf: []u8) !?Command {
    const line = try readLine(stream, buf) orelse return null;
    return try parseLine(line);
}

fn readLine(stream: zio.net.Stream, buf: []u8) !?[]const u8 {
    var pos: usize = 0;
    while (pos < buf.len) {
        var byte: [1]u8 = undefined;
        stream.readAll(&byte, .none) catch |err| {
            if (pos == 0) return null; // clean EOF
            return err;
        };
        if (byte[0] == '\n') {
            // Strip trailing \r if present.
            const end = if (pos > 0 and buf[pos - 1] == '\r') pos - 1 else pos;
            return buf[0..end];
        }
        buf[pos] = byte[0];
        pos += 1;
    }
    return error.LineTooLong;
}

fn parseLine(line: []const u8) !Command {
    var it = std.mem.splitScalar(u8, line, ' ');
    const cmd = it.next() orelse return error.EmptyCommand;

    if (std.mem.eql(u8, cmd, "set")) {
        const ns_str = it.next() orelse return error.MissingNamespace;
        const key = it.next() orelse return error.MissingKey;
        const ts_str = it.next() orelse return error.MissingTimestamp;
        const value = it.rest();
        if (value.len == 0) return error.MissingValue;

        return .{ .set = .{
            .ns = std.fmt.parseInt(u32, ns_str, 10) catch return error.InvalidNamespace,
            .key = key,
            .ts = std.fmt.parseInt(u64, ts_str, 10) catch return error.InvalidTimestamp,
            .value = value,
        } };
    } else if (std.mem.eql(u8, cmd, "get")) {
        const ns_str = it.next() orelse return error.MissingNamespace;
        const key = it.next() orelse return error.MissingKey;
        const ts_str = it.next() orelse return error.MissingTimestamp;

        return .{ .get = .{
            .ns = std.fmt.parseInt(u32, ns_str, 10) catch return error.InvalidNamespace,
            .key = key,
            .ts = std.fmt.parseInt(u64, ts_str, 10) catch return error.InvalidTimestamp,
        } };
    } else {
        return error.UnknownCommand;
    }
}

/// Write a successful set response: "OK\n"
pub fn writeOk(stream: zio.net.Stream) !void {
    try stream.writeAll("OK\n", .none);
}

/// Write an error response: "ERR <msg>\n"
pub fn writeErr(stream: zio.net.Stream, msg: []const u8) !void {
    try stream.writeAll("ERR ", .none);
    try stream.writeAll(msg, .none);
    try stream.writeAll("\n", .none);
}

/// Write a not-leader response: "NOT_LEADER <leader_id>\n"
pub fn writeNotLeader(stream: zio.net.Stream, leader_id: u64) !void {
    var buf: [32]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, "NOT_LEADER {d}\n", .{leader_id}) catch unreachable;
    try stream.writeAll(s, .none);
}

/// Write a get-found response: "FOUND <ts> <value>\n"
pub fn writeFound(stream: zio.net.Stream, ts: u64, value: []const u8) !void {
    var buf: [64]u8 = undefined;
    const hdr = std.fmt.bufPrint(&buf, "FOUND {d} ", .{ts}) catch unreachable;
    try stream.writeAll(hdr, .none);
    try stream.writeAll(value, .none);
    try stream.writeAll("\n", .none);
}

/// Write a get-not-found response: "NOT_FOUND\n"
pub fn writeNotFound(stream: zio.net.Stream) !void {
    try stream.writeAll("NOT_FOUND\n", .none);
}
