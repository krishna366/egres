//! Collective fan-out + wait over fibers returning the same type `T`.
//! Wraps the "vector-of-handles + for-spawn / for-join" pattern so the
//! call site stays compact.
//!
//! Use cases:
//!   - long-running fiber bundles (server loops, background workers)
//!   - sub-query fan-out (`Group(QueryResult)`, parent reads results
//!     from the slice returned by `wait()`)
//!
//! `wait()`'s return type is comptime-selected:
//!   - `Group(void).wait()` → `void`        (synchronize only)
//!   - `Group(T).wait()`    → `[]const T`   (results in spawn order)
//!
//! Result slots are reserved during `spawn()` so `wait()` is infallible.
//! The returned slice is owned by the Group and valid until `deinit()`.
//!
//! No `cancel()` today — `JoinHandle` doesn't carry a cancel primitive
//! yet, and "cancel a parked fiber" needs a real semantics decision
//! before it lands here.

const std = @import("std");
const _fb = @import("fiber.zig");
const _runtime = @import("runtime.zig");

pub fn Group(comptime T: type) type {
    return struct {
        const Self = @This();

        alloc: std.mem.Allocator,
        handles: std.ArrayList(_runtime.JoinHandle(T)) = .empty,
        /// Pre-sized slot per spawn so `wait()` can write without
        /// allocating. Empty when `T == void`.
        results: std.ArrayList(T) = .empty,
        /// Set once `wait()` has joined (and thereby freed) every handle, so a
        /// second `wait()` is a no-op that returns the cached results instead of
        /// double-joining the now-freed fibers. Internal to `wait()`.
        done: bool = false,

        pub fn init(alloc: std.mem.Allocator) Self {
            return .{ .alloc = alloc };
        }

        pub fn deinit(self: *Self) void {
            // `wait()` consumes (joins → frees) the handles, so after a wait
            // there's nothing to dispose here — just free our own lists. A Group
            // deinit'd WITHOUT a wait() leaves its fibers in the registry until
            // `Runtime.deinit` (bounded). All current Group users wait() first.
            self.handles.deinit(self.alloc);
            self.results.deinit(self.alloc);
        }

        /// Pre-size the handle list (and result slots, for typed
        /// groups). Optional; `spawn` allocates on demand otherwise.
        pub fn ensureTotalCapacity(self: *Self, n: usize) !void {
            try self.handles.ensureTotalCapacity(self.alloc, n);
            if (comptime T != void) try self.results.ensureTotalCapacity(self.alloc, n);
        }

        /// Spawn `func(args...)` on the process-wide runtime and
        /// register the handle. The spawned function's return type
        /// must equal `T` (checked at comptime). Reserves a result
        /// slot for typed groups so `wait()` is infallible.
        ///
        /// If `handles.append` fails, the fiber has already started —
        /// the orphaned handle is lost and its return value is leaked.
        /// Pre-size with `ensureTotalCapacity` if that matters.
        pub fn spawn(
            self: *Self,
            opts: _fb.SpawnOptions,
            comptime func: anytype,
            args: std.meta.ArgsTuple(@TypeOf(func)),
        ) !void {
            comptime {
                if (_fb.ReturnOf(func) != T) {
                    @compileError(
                        "Group(" ++ @typeName(T) ++ ").spawn: func must return " ++
                            @typeName(T) ++ ", got " ++ @typeName(_fb.ReturnOf(func)),
                    );
                }
            }
            const h = try _runtime.spawn(opts, func, args);
            try self.handles.append(self.alloc, h);
            if (comptime T != void) try self.results.append(self.alloc, undefined);
        }

        const WaitResult = if (T == void) void else []const T;

        /// Join every spawned fiber in spawn order, which frees each fiber
        /// (`join()` is consuming). Idempotent: the first call joins + frees and
        /// caches the results; a second call returns the cached results without
        /// re-joining the now-freed fibers (so a defer-wait + body-wait pair is
        /// safe). The returned slice is owned by the Group and valid until
        /// `deinit()`.
        pub fn wait(self: *Self) WaitResult {
            if (!self.done) {
                for (self.handles.items, 0..) |*h, i| {
                    const r = h.join();
                    if (comptime T != void) self.results.items[i] = r;
                }
                self.done = true;
            }
            if (comptime T != void) return self.results.items;
        }
    };
}
