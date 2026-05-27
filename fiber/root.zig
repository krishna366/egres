//! Module root. Re-exports every public symbol from sibling files so
//! consumers see one flat namespace (`fiber.Fiber`, `fiber.Runtime`,
//! `fiber.spawn`, …) regardless of which file the symbol lives in.

// From fiber.zig — the Fiber primitive and its types.
const fb = @import("fiber.zig");

pub const coro = fb.coro;
pub const default_stack_max = fb.default_stack_max;
pub const default_stack_commit = fb.default_stack_commit;
pub const Priority = fb.Priority;
pub const Tag = fb.Tag;
pub const State = fb.State;
pub const YieldIntent = fb.YieldIntent;
pub const SpawnOptions = fb.SpawnOptions;
pub const FiberFn = fb.FiberFn;
pub const Fiber = fb.Fiber;
pub const JoinState = fb.JoinState;
pub const ReturnOf = fb.ReturnOf;

// From runtime.zig — the scheduler.
const rt = @import("runtime.zig");

pub const Worker = rt.Worker;
pub const Runtime = rt.Runtime;
pub const CoarseClock = rt.CoarseClock;
pub const Deadlines = rt.Deadlines;
pub const Timeout = rt.Timeout;
pub const Stats = rt.Stats;
pub const JoinHandle = rt.JoinHandle;
pub const spawn = rt.spawn;
pub const yield = rt.yield;
pub const park = rt.park;
pub const wake = rt.wake;
pub const currentWorker = rt.currentWorker;
pub const currentFiber = rt.currentFiber;
pub const dumpFibers = rt.dumpFibers;
pub const tscNsPerCycleQ32 = rt.tscNsPerCycleQ32;

// Lower-level data structure used by `Deadlines`.
pub const heap = @import("heap.zig");
