//! What the host lets this process have: memory from sysinfo, capped
//! by the cgroup limit when one applies (Docker, Kubernetes).

const std = @import("std");
const fiber = @import("zfiber");
const String = @import("utils.zig").String;

/// The memory this process may use. `source` names the bound that
/// won, for the startup log. 0 bytes when neither could be read.
pub const Memory = struct { bytes: u64, source: String };

pub fn memory() Memory {
    var out = Memory{
        .bytes = std.process.totalSystemMemory() catch 0,
        .source = "sysinfo",
    };
    if (cgroupLimit()) |limit| {
        if (out.bytes == 0 or limit < out.bytes) {
            out = .{ .bytes = limit, .source = "cgroup" };
        }
    }
    return out;
}

/// The cgroup memory limit: v2 reads `memory.max` of the process's own
/// cgroup (the `0::<path>` line of /proc/self/cgroup, `/` inside a
/// container), v1 `memory.limit_in_bytes`. Null when unlimited or not
/// in a cgroup.
fn cgroupLimit() ?u64 {
    var buf: [512]u8 = undefined;
    if (readSmall("/proc/self/cgroup", &buf)) |text| {
        var lines = std.mem.splitScalar(u8, text, '\n');
        while (lines.next()) |line| {
            if (!std.mem.startsWith(u8, line, "0::")) continue;
            var path: [512]u8 = undefined;
            const file = std.fmt.bufPrint(
                &path,
                "/sys/fs/cgroup{s}/memory.max",
                .{line[3..]},
            ) catch break;
            if (parseLimit(file)) |n| return n;
            break;
        }
    }
    if (parseLimit("/sys/fs/cgroup/memory.max")) |n| return n;
    return parseLimit("/sys/fs/cgroup/memory/memory.limit_in_bytes");
}

/// The number in a cgroup limit file, or null when the file is absent,
/// says `max`, or holds v1's "no limit" (the type's maximum, page
/// rounded).
fn parseLimit(path: String) ?u64 {
    var buf: [64]u8 = undefined;
    const text = readSmall(path, &buf) orelse return null;
    const trimmed = std.mem.trim(u8, text, " \n");
    if (std.mem.eql(u8, trimmed, "max")) return null;
    const n = std.fmt.parseInt(u64, trimmed, 10) catch return null;
    if (n >= 1 << 60) return null;
    return n;
}

/// A proc or sysfs file read whole into `buf` (their stat sizes mean
/// nothing, so read until EOF). Null on any error.
fn readSmall(path: String, buf: []u8) ?String {
    const f = fiber.Dir.cwd().openFile(path, .{}) catch return null;
    defer f.close();
    var off: usize = 0;
    while (off < buf.len) {
        const n = f.read(buf[off..], off) catch return null;
        if (n == 0) break;
        off += n;
    }
    return buf[0..off];
}
