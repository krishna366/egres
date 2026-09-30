// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");

const Allocator = std.mem.Allocator;
const Io = std.Io;

/// Process-wide context. Populated once in `main` from `std.process.Init`
/// and read everywhere via `context.g()`. Mirrors the existing singleton
/// style used by `vfs.g()` and `flags.g()`.
pub const Context = struct {
    gpa: Allocator,
    arena: Allocator,
    io: Io,
    args: std.process.Args,
    environ_map: *std.process.Environ.Map,

    /// argv as `[]const []const u8`, allocated in arena (no caller free).
    /// Callers that want sentinel-terminated strings can use `args.toSlice`
    /// directly instead.
    pub fn argv(self: *const Context) ![]const []const u8 {
        var list: std.ArrayList([]const u8) = .empty;
        var it = self.args.iterate();
        while (it.next()) |a| try list.append(self.arena, a);
        return list.toOwnedSlice(self.arena);
    }

    /// Environment variable lookup (process-lifetime memory, no free).
    pub fn env(self: *const Context, key: []const u8) ?[]const u8 {
        return self.environ_map.get(key);
    }
};

var global: Context = undefined;
var inited: bool = false;

/// Call exactly once at the top of `main(init: std.process.Init)`.
///
pub fn initGlobal(init: std.process.Init) void {
    initGlobalWithAllocator(init, init.gpa);
}

/// Production can replace `init.gpa` while tests and tools retain Zig's
/// normal process allocator. The initialization arena is intentionally kept:
/// it owns only process-lifetime arguments and other startup state.
pub fn initGlobalWithAllocator(
    init: std.process.Init,
    gpa: Allocator,
) void {
    std.debug.assert(!inited);
    global = .{
        .gpa = gpa,
        .arena = init.arena.allocator(),
        .io = init.io,
        .args = init.minimal.args,
        .environ_map = init.environ_map,
    };
    inited = true;
}

pub fn g() *const Context {
    std.debug.assert(inited);
    return &global;
}
