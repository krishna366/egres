const std = @import("std");
const fiber = @import("zfiber");

test "Shutdown: start before wait returns immediately, idempotent" {
    var sd = try fiber.Shutdown.init(std.testing.allocator);
    defer sd.deinit();

    try std.testing.expect(!sd.hasStarted());
    sd.start();
    sd.start(); // idempotent
    try std.testing.expect(sd.hasStarted());
    sd.wait(); // must not block
}

test "Shutdown: start wakes every parked waiter" {
    var sd = try fiber.Shutdown.init(std.testing.allocator);
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

    var sd = try fiber.Shutdown.init(std.testing.allocator);
    // deinit start()s (idempotent) then joins the watcher fiber.
    defer sd.deinit();

    try sd.install();
    try std.testing.expect(!sd.hasStarted());
    try posix.raise(posix.SIG.TERM);
    sd.wait(); // handler flags -> watcher poll fires start()
    try std.testing.expect(sd.hasStarted());
}
