// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const mem = @import("mem.zig");

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
    /// The allocator `gpa` is: the one passed to `initGlobal...`,
    /// counted. Its live bytes are what the cache governor and the admin
    /// `mem` command read (`counting()`).
    counting: mem.CountingAlloc,

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

/// Production can replace `init.gpa` (with jemalloc); tests and tools
/// keep Zig's process allocator. Either way `gpa` is that allocator
/// counted: `global` is static, so `gpa`'s pointer to `counting` stays
/// valid. The initialization arena is intentionally kept: it owns only
/// process-lifetime arguments and other startup state.
pub fn initGlobalWithAllocator(
    init: std.process.Init,
    inner: Allocator,
) void {
    std.debug.assert(!inited);
    global = .{
        .gpa = undefined,
        .counting = .init(inner),
        .arena = init.arena.allocator(),
        .io = init.io,
        .args = init.minimal.args,
        .environ_map = init.environ_map,
    };
    global.gpa = global.counting.allocator();
    inited = true;
}

/// The counting allocator behind `gpa`, mutable: the governor sets its
/// limit.
pub fn counting() *mem.CountingAlloc {
    std.debug.assert(inited);
    return &global.counting;
}

pub fn g() *const Context {
    std.debug.assert(inited);
    return &global;
}
