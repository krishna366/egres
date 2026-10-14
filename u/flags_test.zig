// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");
const flags = @import("flags");
const utils = @import("utils");
const String = utils.String;

const RaftCfg = struct {
    name: String = "default",
    retries: u32 = 3,
    ratio: f64 = 0.5,
    tracing: bool = false,
    tick_interval_ms: u32 = 10,

    pub const docs = .{
        .self = "Raft consensus tunables. Hot-reloadable.",
        .name = "Human-readable identifier for this raft node.",
        .retries = "Number of attempts before giving up on a proposal.",
        .ratio = "Sampling ratio in [0,1].",
        .tracing = "Emit per-event trace logs (very chatty).",
        .tick_interval_ms = "Wall-clock tick period for the raft loop.",
    };
};

const NetCfg = struct {
    listen_addr: String = ":9000",
    enable_tls: bool = false,

    pub const docs = .{
        .listen_addr = "Address the server listens on.",
        .enable_tls = "Whether incoming connections require TLS.",
    };
};

// ---------- Live(T) tests ----------

test "init with empty input uses defaults" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(RaftCfg).init(alloc, "");
    defer live.deinit();
    const c = live.cur();
    try std.testing.expectEqualStrings("default", c.name);
    try std.testing.expectEqual(@as(u32, 3), c.retries);
    try std.testing.expectApproxEqAbs(@as(f64, 0.5), c.ratio, 1e-9);
    try std.testing.expect(!c.tracing);
    try std.testing.expectEqual(@as(u32, 10), c.tick_interval_ms);
}

test "init applies partial overrides" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(RaftCfg).init(alloc, "retries=5;");
    defer live.deinit();
    const c = live.cur();
    try std.testing.expectEqual(@as(u32, 5), c.retries);
    try std.testing.expectApproxEqAbs(@as(f64, 0.5), c.ratio, 1e-9);
}

test "update merges, leaves unspecified fields alone" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(RaftCfg).init(alloc, "retries=5; ratio=0.9;");
    defer live.deinit();
    try live.update("tracing=true;");
    const c = live.cur();
    try std.testing.expectEqual(@as(u32, 5), c.retries);
    try std.testing.expectApproxEqAbs(@as(f64, 0.9), c.ratio, 1e-9);
    try std.testing.expect(c.tracing);
}

test "update rejects unknown key, leaves state intact" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(RaftCfg).init(alloc, "retries=5;");
    defer live.deinit();
    const v_before = live.version();
    try std.testing.expectError(error.UnknownOption, live.update("mystery=1;"));
    const c = live.cur();
    try std.testing.expectEqual(@as(u32, 5), c.retries);
    try std.testing.expectEqual(v_before, live.version());
}

test "update rejects malformed value, leaves state intact" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(RaftCfg).init(alloc, "retries=5;");
    defer live.deinit();
    try std.testing.expectError(error.InvalidInt, live.update("retries=abc;"));
    try std.testing.expectEqual(@as(u32, 5), live.cur().retries);
}

test "string field arena lifetime survives later updates" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(RaftCfg).init(alloc, "name=first;");
    defer live.deinit();
    const snap1 = live.cur();
    try std.testing.expectEqualStrings("first", snap1.name);

    try live.update("name=second;");
    try live.update("name=third;");
    // snap1 still references the original arena slice; it must still equal "first".
    try std.testing.expectEqualStrings("first", snap1.name);
    try std.testing.expectEqualStrings("third", live.cur().name);
}

test "_ms duration field accepts unit notation" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(RaftCfg).init(alloc, "tick-interval-ms=500ms;");
    defer live.deinit();
    try std.testing.expectEqual(@as(u32, 500), live.cur().tick_interval_ms);

    try live.update("tick-interval-ms=2s;");
    try std.testing.expectEqual(@as(u32, 2000), live.cur().tick_interval_ms);

    try live.update("tick-interval-ms=1h;");
    try std.testing.expectEqual(@as(u32, 3_600_000), live.cur().tick_interval_ms);

    try live.update("tick-interval-ms=42;");
    try std.testing.expectEqual(@as(u32, 42), live.cur().tick_interval_ms);
}

test "bool accepts short and long forms (case-insensitive)" {
    const alloc = std.testing.allocator;
    const trues = [_]String{ "true", "TRUE", "t", "T", "1", "yes", "y", "on", "ON" };
    const falses = [_]String{ "false", "f", "F", "0", "no", "n", "off", "OFF" };
    for (trues) |v| {
        const opt = try std.fmt.allocPrint(alloc, "tracing={s};", .{v});
        defer alloc.free(opt);
        var live = try flags.Live(RaftCfg).init(alloc, opt);
        defer live.deinit();
        try std.testing.expect(live.cur().tracing);
    }
    for (falses) |v| {
        const opt = try std.fmt.allocPrint(alloc, "tracing={s};", .{v});
        defer alloc.free(opt);
        var live = try flags.Live(RaftCfg).init(alloc, opt);
        defer live.deinit();
        try std.testing.expect(!live.cur().tracing);
    }
    var bad = try flags.Live(RaftCfg).init(alloc, "");
    defer bad.deinit();
    try std.testing.expectError(error.InvalidBool, bad.update("tracing=garbage;"));
}

test "key name mapping accepts all reasonable variants" {
    const alloc = std.testing.allocator;
    const variants = [_]String{
        "tick-interval-ms=42;",
        "tick_interval_ms=42;",
        "Tick-Interval-MS=42;",
        "TICK_INTERVAL_MS=42;",
    };
    for (variants) |v| {
        var live = try flags.Live(RaftCfg).init(alloc, v);
        defer live.deinit();
        try std.testing.expectEqual(@as(u32, 42), live.cur().tick_interval_ms);
    }
}

test "version monotonic on success, unchanged on failure" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(RaftCfg).init(alloc, "");
    defer live.deinit();
    try std.testing.expectEqual(@as(u64, 0), live.version());
    try live.update("retries=1;");
    try std.testing.expectEqual(@as(u64, 1), live.version());
    try live.update("retries=2;");
    try std.testing.expectEqual(@as(u64, 2), live.version());
    try std.testing.expectError(error.UnknownOption, live.update("nope=1;"));
    try std.testing.expectEqual(@as(u64, 2), live.version());
}

const ConcurrentCtx = struct {
    live: *flags.Live(RaftCfg),
    stop: *std.atomic.Value(bool),
    err: *std.atomic.Value(u16),
    observed: *std.atomic.Value(u64),
};

/// The writer encodes one logical version `i` into all three mutated fields
/// atomically (under the write lock):
///   name           = "v{i}"
///   retries        = i % 10
///   tracing        = (i % 2 == 0)
/// The reader parses `i` back out of `name`, then asserts the other two
/// fields agree. A torn snapshot — e.g. name from version i but retries
/// from i-1 — fails the invariant on the very first read. Also tracks the
/// number of snapshots that satisfied the invariant so the test can
/// assert that readers actually ran (caught the case where the writer
/// finished so fast the readers never observed anything).
fn readerLoop(ctx: ConcurrentCtx) void {
    while (!ctx.stop.load(.acquire)) {
        const c = ctx.live.cur();
        // Skip the initial "initial" value the test sets at init time —
        // we only validate the writer's "v{N}" encoding.
        if (c.name.len >= 2 and c.name[0] == 'v') {
            const i = std.fmt.parseInt(usize, c.name[1..], 10) catch {
                _ = ctx.err.cmpxchgStrong(0, 1, .acq_rel, .monotonic);
                return;
            };
            const want_retries: u32 = @intCast(i % 10);
            const want_tracing: bool = (i % 2 == 0);
            if (c.retries != want_retries or c.tracing != want_tracing) {
                _ = ctx.err.cmpxchgStrong(0, 2, .acq_rel, .monotonic);
                return;
            }
            _ = ctx.observed.fetchAdd(1, .monotonic);
        }
        fiber.yield();
    }
}

test "concurrent readers do not see torn state" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(RaftCfg).init(alloc, "name=initial;");
    defer live.deinit();

    var stop = std.atomic.Value(bool).init(false);
    var err_flag = std.atomic.Value(u16).init(0);
    var observed = std.atomic.Value(u64).init(0);
    const ctx = ConcurrentCtx{
        .live = &live,
        .stop = &stop,
        .err = &err_flag,
        .observed = &observed,
    };

    // Use spawnWith so the fiber dump shows where each reader was
    // spawned, distinguishing them from the test fiber itself.
    var handles: [4]fiber.JoinHandle(void) = undefined;
    for (&handles) |*h| h.* = try fiber.spawn(.{}, readerLoop, .{ctx});

    // Hammer with updates. Each `i` writes a self-consistent triple
    // (name=v{i}, retries=i%10, tracing=i%2==0) that readers verify.
    var i: usize = 0;
    while (i < 2000) : (i += 1) {
        const buf = try std.fmt.allocPrint(alloc, "name=v{d}; retries={d}; tracing={s};", .{
            i, @as(u32, @intCast(i % 10)), if (i % 2 == 0) "true" else "false",
        });
        defer alloc.free(buf);
        try live.update(buf);
        // Yield so the co-located reader fibers get to run. fiber's scheduler
        // is cooperative + no-work-stealing + local-first: fibers spawned from
        // this worker sit on its local queue (no spill until the ring overflows
        // at 16), so idle peers never pull them. A non-yielding writer would
        // monopolize the worker and the readers would never observe an update.
        fiber.yield();
    }

    stop.store(true, .release);
    for (&handles) |*h| h.join();
    try std.testing.expectEqual(@as(u16, 0), err_flag.load(.acquire));
    // Sanity: readers must have actually observed at least one v{N}
    // snapshot, otherwise the invariant check above was never exercised.
    try std.testing.expect(observed.load(.monotonic) > 0);
}

// ---------- Registry tests ----------

test "registry: register returns Live pointer; dispatch routes to it" {
    const alloc = std.testing.allocator;
    var reg = flags.Registry.init(alloc);
    defer reg.deinit();
    const raft = try reg.register("raft", RaftCfg);
    const net = try reg.register("net", NetCfg);

    try reg.update("raft", "tracing=true; retries=7;");
    try reg.update("net", "enable-tls=true;");

    try std.testing.expect(raft.cur().tracing);
    try std.testing.expectEqual(@as(u32, 7), raft.cur().retries);
    try std.testing.expect(net.cur().enable_tls);
    try std.testing.expectEqualStrings(":9000", net.cur().listen_addr);
}

test "registry: duplicate name rejected" {
    const alloc = std.testing.allocator;
    var reg = flags.Registry.init(alloc);
    defer reg.deinit();
    _ = try reg.register("raft", RaftCfg);
    try std.testing.expectError(error.DuplicateModule, reg.register("raft", RaftCfg));
}

test "registry: unknown module returns error" {
    const alloc = std.testing.allocator;
    var reg = flags.Registry.init(alloc);
    defer reg.deinit();
    const raft = try reg.register("raft", RaftCfg);
    try std.testing.expectError(error.UnknownModule, reg.update("ghost", "x=1;"));
    try std.testing.expectEqual(@as(u32, 3), raft.cur().retries);
}

test "registry: module name normalization" {
    const alloc = std.testing.allocator;
    var reg = flags.Registry.init(alloc);
    defer reg.deinit();
    const raft = try reg.register("My_Module", RaftCfg);

    try reg.update("my-module", "retries=1;");
    try std.testing.expectEqual(@as(u32, 1), raft.cur().retries);
    try reg.update("MY_MODULE", "retries=2;");
    try std.testing.expectEqual(@as(u32, 2), raft.cur().retries);
    try reg.update("My-Module", "retries=3;");
    try std.testing.expectEqual(@as(u32, 3), raft.cur().retries);
}

test "registry: parseArgv happy path" {
    const alloc = std.testing.allocator;
    var reg = flags.Registry.init(alloc);
    defer reg.deinit();
    const raft = try reg.register("raft", RaftCfg);
    const net = try reg.register("net", NetCfg);

    var leftover: std.ArrayList(String) = .empty;
    defer leftover.deinit(alloc);
    const argv = [_]String{
        "--raft=retries=7;",
        "--net",
        "listen-addr=:8080;",
        "data.db",
    };
    try reg.parseArgv(&argv, &leftover);
    try std.testing.expectEqual(@as(u32, 7), raft.cur().retries);
    try std.testing.expectEqualStrings(":8080", net.cur().listen_addr);
    try std.testing.expectEqual(@as(usize, 1), leftover.items.len);
    try std.testing.expectEqualStrings("data.db", leftover.items[0]);
}

test "registry: parseArgv strips surviving outer quotes from payload" {
    const alloc = std.testing.allocator;
    var reg = flags.Registry.init(alloc);
    defer reg.deinit();
    const raft = try reg.register("raft", RaftCfg);
    const net = try reg.register("net", NetCfg);

    var leftover: std.ArrayList(String) = .empty;
    defer leftover.deinit(alloc);
    const argv = [_]String{
        "--raft=\"retries=7; ratio=0.6\"",
        "--net",
        "'listen-addr=:8080; enable-tls=true'",
    };
    try reg.parseArgv(&argv, &leftover);
    try std.testing.expectEqual(@as(u32, 7), raft.cur().retries);
    try std.testing.expectApproxEqAbs(@as(f64, 0.6), raft.cur().ratio, 1e-9);
    try std.testing.expectEqualStrings(":8080", net.cur().listen_addr);
    try std.testing.expect(net.cur().enable_tls);
}

test "registry: parseArgv unknown module" {
    const alloc = std.testing.allocator;
    var reg = flags.Registry.init(alloc);
    defer reg.deinit();
    const raft = try reg.register("raft", RaftCfg);

    var leftover: std.ArrayList(String) = .empty;
    defer leftover.deinit(alloc);
    const argv = [_]String{ "--ghost=x=1;", "--raft=retries=2;" };
    try std.testing.expectError(error.UnknownModule, reg.parseArgv(&argv, &leftover));
    try std.testing.expectEqual(@as(u32, 3), raft.cur().retries);
}

test "registry: writeHelpAll renders both modules with their docs" {
    const alloc = std.testing.allocator;
    var reg = flags.Registry.init(alloc);
    defer reg.deinit();
    _ = try reg.register("raft", RaftCfg);
    _ = try reg.register("net", NetCfg);

    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(alloc);
    try reg.writeHelpAll(&out);

    // raft has a self-doc; rendered next to the module name.
    try std.testing.expect(std.mem.indexOf(u8, out.items, "Raft consensus tunables. Hot-reloadable.\n") != null);
    // net has no self-doc; module name appears bare on its own line.
    try std.testing.expect(std.mem.indexOf(u8, out.items, "  net\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.items, "tick-interval-ms=") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.items, "listen-addr=") != null);
    // Field doc strings get rendered.
    try std.testing.expect(std.mem.indexOf(u8, out.items, "Wall-clock tick period for the raft loop.") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.items, "Whether incoming connections require TLS.") != null);
}

test "global registry: initGlobal / g() / deinitGlobal" {
    const alloc = std.testing.allocator;
    flags.initGlobal(alloc);
    defer flags.deinitGlobal();

    const raft = try flags.g().register("raft", RaftCfg);
    try flags.g().update("raft", "retries=42;");
    try std.testing.expectEqual(@as(u32, 42), raft.cur().retries);
}

const MultiUpdaterCtx = struct {
    live: *flags.Live(RaftCfg),
    iters: usize,
    err: *std.atomic.Value(u16),
};

fn updaterLoop(ctx: MultiUpdaterCtx) void {
    var i: usize = 0;
    while (i < ctx.iters) : (i += 1) {
        var buf: [64]u8 = undefined;
        const s = std.fmt.bufPrint(&buf, "retries={d};", .{i % 100}) catch {
            _ = ctx.err.cmpxchgStrong(0, 1, .acq_rel, .monotonic);
            return;
        };
        ctx.live.update(s) catch {
            _ = ctx.err.cmpxchgStrong(0, 2, .acq_rel, .monotonic);
            return;
        };
    }
}

test "registry: concurrent updates same module are serialised" {
    const alloc = std.testing.allocator;
    var reg = flags.Registry.init(alloc);
    defer reg.deinit();
    const raft = try reg.register("raft", RaftCfg);

    var err_flag = std.atomic.Value(u16).init(0);
    const ctx = MultiUpdaterCtx{ .live = raft, .iters = 100, .err = &err_flag };

    var handles: [4]fiber.JoinHandle(void) = undefined;
    for (&handles) |*h| h.* = try fiber.spawn(.{}, updaterLoop, .{ctx});
    for (&handles) |*h| h.join();

    try std.testing.expectEqual(@as(u16, 0), err_flag.load(.acquire));
    try std.testing.expectEqual(@as(u64, 4 * 100), raft.version());
}

const ModeCfg = struct {
    mode: enum { auto, on, off, true, false } = .auto,

    pub const docs = .{
        .mode = "auto picks per filesystem.",
    };
};

test "enum field: parsed by tag name, rejects anything else" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(ModeCfg).init(alloc, "");
    defer live.deinit();
    try std.testing.expectEqual(.auto, live.cur().mode);
    try live.update("mode=off");
    try std.testing.expectEqual(.off, live.cur().mode);
    try std.testing.expectError(error.InvalidEnum, live.update("mode=maybe"));
    try std.testing.expectEqual(.off, live.cur().mode);
    // `true` and `false` are ordinary tags when the enum declares them.
    try live.update("mode=true");
    try std.testing.expectEqual(.true, live.cur().mode);
    try live.update("mode=false");
    try std.testing.expectEqual(.false, live.cur().mode);
}

/// Prose long enough to cross `help_width` however wide that is, and one
/// word wider than it: the fixture tracks the width instead of assuming it.
const long_prose = "wrapped text " ** (flags.help_width / "wrapped text ".len + 2);
const long_word = "https://" ++ ("x" ** flags.help_width);

const WrapCfg = struct {
    cache_bytes: u64 = 1 << 30,
    odd_bytes: u64 = 1500,
    idle_seconds: u64 = 30 * 60,
    lease_seconds: u64 = 2 * 3600,
    tick_ms: i64 = 5000,
    retries: u32 = 3,
    endpoint: String = "",

    pub const docs = .{
        .self = "A module whose description is long enough that the help" ++
            " renderer has to break it across more than one output line: " ++
            long_prose,
        .cache_bytes = "A byte count that is a whole number of GiB, with a" ++
            " description that also needs wrapping to stay inside the width: " ++
            long_prose,
        .odd_bytes = "A byte count that is not a whole number of any unit.",
        .idle_seconds = "A duration that is a whole number of minutes.",
        .lease_seconds = "A duration that is a whole number of hours.",
        .tick_ms = "A millisecond duration that is a whole number of seconds.",
        .retries = "A plain count.",
        .endpoint = "A word longer than the width is left whole: " ++ long_word,
    };
};

test "writeHelp wraps at help_width and indents continuation lines" {
    const alloc = std.testing.allocator;
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(alloc);
    try flags.writeHelp(WrapCfg, "wrap", &out, alloc);

    var wrapped_module = false;
    var wrapped_field = false;
    var lines = std.mem.splitScalar(u8, out.items, '\n');
    while (lines.next()) |line| {
        // Only a single over-long word may pass the width.
        if (line.len > flags.help_width) {
            try std.testing.expect(std.mem.indexOf(u8, line, "https://") != null);
            try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, std.mem.trim(u8, line, " "), " ") + 1);
        }
        if (std.mem.startsWith(u8, line, "    ") and !std.mem.startsWith(u8, line, "     ")) wrapped_module = true;
        if (std.mem.startsWith(u8, line, "          ")) wrapped_field = true;
    }
    try std.testing.expect(wrapped_module);
    try std.testing.expect(wrapped_field);
    // Nothing is lost in the wrap: the words of a doc string still appear in order.
    try std.testing.expect(std.mem.indexOf(u8, out.items, "break it across") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.items, "wrapped text wrapped text") != null);
}

test "_bytes field accepts a unit suffix" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(WrapCfg).init(alloc, "cache-bytes=10gib;");
    defer live.deinit();
    try std.testing.expectEqual(@as(u64, 10 << 30), live.cur().cache_bytes);

    try live.update("cache-bytes=64MiB;");
    try std.testing.expectEqual(@as(u64, 64 << 20), live.cur().cache_bytes);
    try live.update("cache-bytes=1.5KiB;");
    try std.testing.expectEqual(@as(u64, 1536), live.cur().cache_bytes);
    try live.update("cache-bytes=4096;");
    try std.testing.expectEqual(@as(u64, 4096), live.cur().cache_bytes);
    try live.update("cache-bytes=0;");
    try std.testing.expectEqual(@as(u64, 0), live.cur().cache_bytes);

    // Decimal or bare units are refused: 1g could mean 10^9 or 2^30.
    const Err = error.InvalidBytes;
    try std.testing.expectError(Err, live.update("cache-bytes=10g;"));
    try std.testing.expectError(Err, live.update("cache-bytes=1gb;"));
    try std.testing.expectError(Err, live.update("cache-bytes=gib;"));
    try std.testing.expectError(Err, live.update("cache-bytes=1gibb;"));
}

test "writeHelp prints byte and duration defaults in readable units" {
    const alloc = std.testing.allocator;
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(alloc);
    try flags.writeHelp(WrapCfg, "wrap", &out, alloc);

    // Each flag line ends after its type; the doc follows on its own
    // line.
    const help = out.items;
    for ([_]String{
        "cache-bytes=1GiB (bytes)\n",
        "odd-bytes=1500 (bytes)\n",
        "idle-seconds=1800 (u64, 30 min)\n",
        "lease-seconds=7200 (u64, 2 h)\n",
        "tick-ms=5000 (duration, 5 s)\n",
        "retries=3 (u32)\n          A plain count.\n",
    }) |want| {
        try std.testing.expect(std.mem.indexOf(u8, help, want) != null);
    }
}

const FixedCfg = struct {
    dir: String = "",
    workers: u8 = 2,
    poll_ms: u32 = 50,

    pub const docs = .{
        .dir = "Where the files live.",
        .workers = "Worker fibers spawned at start.",
        .poll_ms = "How often the loop wakes.",
    };
    pub const fixed = .{ "dir", "workers" };
};

test "fixed fields: set at init, refused by update, whole update dropped" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(FixedCfg).init(alloc, "dir=/d;workers=4;poll_ms=10");
    defer live.deinit();
    try std.testing.expectEqualStrings("/d", live.cur().dir);
    try std.testing.expectEqual(@as(u8, 4), live.cur().workers);

    try std.testing.expectError(error.FixedOption, live.update("workers=8"));
    try std.testing.expectError(error.FixedOption, live.update("dir=/e"));
    // A live field in the same input is not applied either.
    try std.testing.expectError(error.FixedOption, live.update("poll_ms=99;workers=8"));
    try std.testing.expectEqual(@as(u8, 4), live.cur().workers);
    try std.testing.expectEqual(@as(u32, 10), live.cur().poll_ms);

    try live.update("poll_ms=99");
    try std.testing.expectEqual(@as(u32, 99), live.cur().poll_ms);
}

test "fixed fields: set and parseArgv assign them at startup" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(FixedCfg).init(alloc, "");
    defer live.deinit();
    try live.set("dir=/d;workers=4;poll_ms=10");
    try std.testing.expectEqualStrings("/d", live.cur().dir);
    try std.testing.expectEqual(@as(u8, 4), live.cur().workers);
    // Once set, the field is still fixed to a live update.
    try std.testing.expectError(error.FixedOption, live.update("workers=8"));

    // The command line goes through `set`: the process's own startup.
    var reg = flags.Registry.init(alloc);
    defer reg.deinit();
    const lp = try reg.register("fx", FixedCfg);
    var leftover: std.ArrayList(String) = .empty;
    defer leftover.deinit(alloc);
    try reg.parseArgv(&.{ "--fx=dir=/argv;workers=2", "rest" }, &leftover);
    try std.testing.expectEqualStrings("/argv", lp.cur().dir);
    try std.testing.expectEqual(@as(u8, 2), lp.cur().workers);
    try std.testing.expectEqual(@as(usize, 1), leftover.items.len);
    try std.testing.expectError(error.FixedOption, reg.update("fx", "dir=/x"));
}

test "writeHelp marks fixed fields" {
    const alloc = std.testing.allocator;
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(alloc);
    try flags.writeHelp(FixedCfg, "fx", &out, alloc);

    // The mark sits on the doc line under the flag's line.
    var lines = std.mem.splitScalar(u8, out.items, '\n');
    var seen: u8 = 0;
    var want_fixed = false;
    while (lines.next()) |line| {
        const fixed = std.mem.indexOf(u8, line, "Fixed at startup.") != null;
        try std.testing.expectEqual(want_fixed, fixed);
        if (fixed) seen += 1;
        want_fixed = std.mem.indexOf(u8, line, "dir=") != null or
            std.mem.indexOf(u8, line, "workers=") != null;
    }
    try std.testing.expectEqual(@as(u8, 2), seen);
}
