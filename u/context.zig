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

    /// argv as `[]const []const u8`, allocated in arena (no caller free).
    /// Callers that want sentinel-terminated strings can use `args.toSlice`
    /// directly instead.
    pub fn argv(self: *const Context) ![]const []const u8 {
        var list: std.ArrayList([]const u8) = .empty;
        var it = self.args.iterate();
        while (it.next()) |a| try list.append(self.arena, a);
        return list.toOwnedSlice(self.arena);
    }
};

var global: Context = undefined;
var inited: bool = false;

/// Call exactly once at the top of `main(init: std.process.Init)`.
///
/// TODO(perf): benchmark `std.heap.c_allocator` vs `std.heap.smp_allocator`
/// under our workloads (memtable churn in zdb, allocator-bound paths in
/// raft, pgwire request handling). On ReleaseFast with libc linked,
/// `init.gpa` resolves to `c_allocator` (glibc ptmalloc on Linux). The
/// 0.16-new `smp_allocator` is lock-free and may win on multi-core
/// allocation-heavy paths; we'd select it here before populating `global`.
/// Decision artifact: a /tmp/zdb/bench.log run comparing both under load.
pub fn initGlobal(init: std.process.Init) void {
    std.debug.assert(!inited);
    global = .{
        .gpa = init.gpa,
        .arena = init.arena.allocator(),
        .io = init.io,
        .args = init.minimal.args,
    };
    inited = true;
}

pub fn g() *const Context {
    std.debug.assert(inited);
    return &global;
}
