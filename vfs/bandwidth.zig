const std = @import("std");
const zio = @import("zio");
const vfs = @import("vfs.zig");

pub const Config = struct {
    bytes_per_sec: u64, // bytes per second
    write_cost: u8 = 8, // write multiplier - each write costs k * read cost.
    timeout_ms: u64 = 100, // After this timeout, provide min_alloc.
    min_alloc: u64 = 256 * vfs.page_size, // 1 MB read, divide by write_cost for writes.
};

pub const Limiter = struct {
    tokens: std.atomic.Value(u64),
    last_refill: std.atomic.Value(i64), // monotonic millis
    waiting_p1: std.atomic.Value(u32),
    conf: Config,

    pub fn init(config: Config) Limiter {
        return .{
            .tokens = std.atomic.Value(u64).init(config.bytes_per_sec),
            .last_refill = std.atomic.Value(i64).init(std.time.milliTimestamp()),
            .conf = config,
            .waiting_p1 = std.atomic.Value(u32).init(0),
        };
    }

    /// Try to refill the bucket. CAS on last_refill ensures only one thread
    /// wins per time window — losers skip and proceed to token deduction.
    fn tryRefill(self: *Limiter) void {
        const old_refill = self.last_refill.load(.monotonic);
        const now = std.time.milliTimestamp();
        const elapsed_ms = now - old_refill;
        if (elapsed_ms <= 0) return;

        const elapsed: u64 = @intCast(elapsed_ms);
        const added = @min(self.conf.bytes_per_sec, elapsed * self.conf.bytes_per_sec / 1_000);
        if (added == 0) return;

        // Race to claim this refill window.
        if (self.last_refill.cmpxchgStrong(old_refill, now, .monotonic, .monotonic) != null) return;

        // We won — add tokens, capped at rate.
        while (true) {
            const old_tokens = self.tokens.load(.monotonic);
            const new_tokens = @min(old_tokens + added, self.conf.bytes_per_sec);
            if (self.tokens.cmpxchgWeak(old_tokens, new_tokens, .monotonic, .monotonic) == null) break;
        }
    }

    /// Returns granted bytes, always a multiple of page_size (suitable for O_DIRECT).
    pub fn acquireAligned(self: *Limiter, priority: vfs.Priority, bytes: usize, kind: vfs.IOKind) usize {
        const cost: u64 = if (kind == .write) @as(u64, bytes) * self.conf.write_cost else bytes;

        if (priority == .p0) {
            self.tryRefill();
            // Saturating subtract — bucket clamps to 0, P0 always gets full grant.
            while (true) {
                const old = self.tokens.load(.monotonic);
                const new = old -| cost;
                if (self.tokens.cmpxchgWeak(old, new, .monotonic, .monotonic) == null) break;
            }
            return alignToPage(bytes);
        }

        if (priority == .p1) _ = self.waiting_p1.fetchAdd(1, .monotonic);
        defer if (priority == .p1) {
            _ = self.waiting_p1.fetchSub(1, .monotonic);
        };

        const start = std.time.milliTimestamp();
        const timeout_ms: i64 = 100;

        while (true) {
            self.tryRefill();

            const should_yield = (priority == .p2 and self.waiting_p1.load(.monotonic) > 0);
            const cur = self.tokens.load(.monotonic);

            if (cur == 0 or should_yield) {
                if (std.time.milliTimestamp() - start >= timeout_ms) {
                    const divisor: usize = if (kind == .write) self.conf.write_cost else 1;
                    return alignToPage(@max(self.conf.min_alloc / divisor, 1));
                }
                zio.sleep(.fromMilliseconds(1)) catch {};
                continue;
            }

            // Try to deduct tokens via CAS.
            const granted_cost = @min(cost, cur);
            if (self.tokens.cmpxchgWeak(cur, cur - granted_cost, .monotonic, .monotonic) == null) {
                const divisor: u64 = if (kind == .write) self.conf.write_cost else 1;
                return alignToPage(@intCast(granted_cost / divisor));
            }
            // CAS failed — another thread grabbed tokens, loop.
        }
    }

    /// Round down to page boundary, minimum page_size.
    fn alignToPage(bytes: usize) usize {
        return @max(bytes & ~@as(usize, vfs.page_size - 1), vfs.page_size);
    }
};
