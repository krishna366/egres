const std = @import("std");
const fiber = @import("zfiber");

test "Shutdown: start before wait returns immediately, idempotent" {
    var sd = fiber.Shutdown.init(std.testing.allocator);
    defer sd.deinit();

    try std.testing.expect(!sd.hasStarted());
    sd.start();
    sd.start(); // idempotent
    try std.testing.expect(sd.hasStarted());
    sd.wait(); // must not block
    try std.testing.expect(sd.waitTimeout(60_000)); // nor this
}

test "Shutdown: waitTimeout times out while not started, wakes early on start" {
    var sd = fiber.Shutdown.init(std.testing.allocator);
    defer sd.deinit();

    // Not started: the wait runs its full course and reports timeout.
    try std.testing.expect(!sd.waitTimeout(20));
    try std.testing.expect(!sd.hasStarted());

    // start() fired mid-wait ends a long wait immediately with true.
    const Waiter = struct {
        fn run(s: *fiber.Shutdown, woke: *std.atomic.Value(bool)) void {
            if (s.waitTimeout(60_000)) woke.store(true, .release);
        }
    };
    var woke = std.atomic.Value(bool).init(false);
    var h = try fiber.spawn(.{}, Waiter.run, .{ &sd, &woke });
    fiber.sleepMs(20); // let it park
    try std.testing.expect(!woke.load(.acquire));
    sd.start();
    h.join();
    try std.testing.expect(woke.load(.acquire));
}

test "Shutdown: start wakes every parked waiter" {
    var sd = fiber.Shutdown.init(std.testing.allocator);
    defer sd.deinit();

    const N = 4;
    var woke = [_]std.atomic.Value(bool){.init(false)} ** N;
    const Waiter = struct {
        fn run(s: *fiber.Shutdown, flag: *std.atomic.Value(bool)) void {
            s.wait();
            flag.store(true, .release);
        }
    };
    var handles: [N]fiber.JoinHandle(void) = undefined;
    for (&handles, &woke) |*h, *w| h.* = try fiber.spawn(.{}, Waiter.run, .{ &sd, w });

    fiber.sleepMs(20); // let them park
    for (&woke) |*w| try std.testing.expect(!w.load(.acquire));

    sd.start();
    for (&handles) |*h| h.join();
    for (&woke) |*w| try std.testing.expect(w.load(.acquire));
}

test "Shutdown: install — SIGTERM fires start, deinit joins the watcher" {
    if (@import("builtin").os.tag == .windows) return;
    const posix = std.posix;

    // Save + restore the process handlers so this test can't leak its
    // sigaction into unrelated tests.
    var old_int: posix.Sigaction = undefined;
    var old_term: posix.Sigaction = undefined;
    posix.sigaction(posix.SIG.INT, null, &old_int);
    posix.sigaction(posix.SIG.TERM, null, &old_term);
    defer {
        posix.sigaction(posix.SIG.INT, &old_int, null);
        posix.sigaction(posix.SIG.TERM, &old_term, null);
    }

    var sd = fiber.Shutdown.init(std.testing.allocator);
    // deinit start()s (idempotent) then joins the watcher fiber.
    defer sd.deinit();

    try sd.install();
    try std.testing.expect(!sd.hasStarted());
    try posix.raise(posix.SIG.TERM);
    sd.wait(); // handler flags -> watcher poll fires start()
    try std.testing.expect(sd.hasStarted());
}
