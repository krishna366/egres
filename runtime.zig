const std = @import("std");
const zio = @import("zio");

pub const Runtime = enum(u2) {
    raft = 0,
    query = 1,
    background = 2,
};

pub const Config = struct {
    /// Number of executor threads per runtime.
    /// 0 = auto-detect based on CPU count.
    raft_threads: u6 = 2,
    query_threads: u6 = 0,
    background_threads: u6 = 2,
};

var g_pool: ?*Pool = null;

/// Get the global Pool singleton. Panics if not initialized.
pub fn pool() *Pool {
    return g_pool orelse @panic("runtime.Pool not initialized");
}

pub const Pool = struct {
    runtimes: [3]*zio.Runtime,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, cfg: Config) !*Pool {
        if (g_pool != null) @panic("runtime.Pool already initialized");

        var runtimes: [3]*zio.Runtime = undefined;
        const counts = [3]u6{ cfg.raft_threads, cfg.query_threads, cfg.background_threads };
        var initialized: usize = 0;
        errdefer for (runtimes[0..initialized]) |rt| rt.deinit();

        for (counts, 0..) |n, i| {
            runtimes[i] = try zio.Runtime.init(allocator, .{
                .executors = @enumFromInt(n),
            });
            initialized += 1;
        }

        const p = try allocator.create(Pool);
        p.* = .{ .runtimes = runtimes, .allocator = allocator };
        g_pool = p;
        return p;
    }

    pub fn deinit(self: *Pool) void {
        var i: usize = self.runtimes.len;
        while (i > 0) {
            i -= 1;
            self.runtimes[i].deinit();
        }
        if (g_pool == self) g_pool = null;
        self.allocator.destroy(self);
    }

    pub fn get(self: *Pool, which: Runtime) *zio.Runtime {
        return self.runtimes[@intFromEnum(which)];
    }
};
