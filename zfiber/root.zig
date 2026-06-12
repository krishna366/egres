//! Module root. Re-exports every public symbol from sibling files so
//! consumers see one flat namespace (`fiber.Fiber`, `fiber.Runtime`,
//! `fiber.spawn`, …) regardless of which file the symbol lives in.

// The libxev backend the runtime drives (loops, async/eventfd wakes, timers).
// Re-exported so tools/benches can use the same primitives the scheduler does.
pub const xev = @import("xev");

// From fiber.zig — the Fiber primitive and its types.
const fb = @import("fiber.zig");

pub const coro = fb.coro;
pub const default_stack_size = fb.default_stack_size;
pub const Priority = fb.Priority;
pub const Lane = fb.Lane;
pub const Tag = fb.Tag;
pub const State = fb.State;
pub const YieldIntent = fb.YieldIntent;
pub const SpawnOptions = fb.SpawnOptions;
pub const FiberFn = fb.FiberFn;
pub const Fiber = fb.Fiber;
pub const JoinState = fb.JoinState;
pub const JoinHandle = fb.JoinHandle;
pub const ReturnOf = fb.ReturnOf;

// From runtime.zig — the scheduler.
const rt = @import("runtime.zig");

pub const Worker = rt.Worker;
pub const Runtime = rt.Runtime;
pub const CoarseClock = rt.CoarseClock;
pub const coarseMs = rt.coarseMs;
pub const clockMsUseRarely = @import("coarse_clock.zig").clockMsUseRarely;
pub const ClockKind = @import("coarse_clock.zig").ClockKind;
pub const Deadlines = rt.Deadlines;
pub const Timeout = rt.Timeout;
pub const Stats = rt.Stats;
pub const spawn = rt.spawn;
pub const spawnDetached = rt.spawnDetached;
pub const yield = rt.yield;
pub const park = rt.park;
pub const wake = rt.wake;
pub const stop = rt.stop;
pub const runtime = rt.runtime;
pub const currentWorker = rt.currentWorker;
pub const currentWorkerOrNull = rt.currentWorkerOrNull;
pub const currentFiber = rt.currentFiber;
pub const currentFiberOrNull = rt.currentFiberOrNull;
pub const dumpFibers = rt.dumpFibers;
pub const dumpWorkerCpu = rt.dumpWorkerCpu;
pub const tscNsPerCycleQ32 = rt.tscNsPerCycleQ32;

// Lower-level data structure used by `Deadlines`.
pub const heap = @import("heap.zig");

// Per-worker ready queue + its lock-mode constructor. Exported so benches can
// measure `LocalQueue(.none)` vs `LocalQueue(.withlock)` (the runtime uses
// `.none`); the scheduler reaches `queue.zig` directly.
const queue = @import("queue.zig");
pub const LocalQueue = queue.LocalQueue;
pub const QueueLockMode = queue.LockMode;

// Sync primitives built on `park` / `wake`. `Mutex`/`RWMutex` are
// type constructors: `Mutex(.tracked)` records its holder for `dumpLocks`,
// `Mutex(.untracked)` is the zero-overhead variant.
const sync = @import("sync.zig");
pub const WaitQueue = sync.WaitQueue;
pub const Track = sync.Track;
pub const Mutex = sync.Mutex;
pub const Condition = sync.Condition;
pub const RWMutex = sync.RWMutex;
pub const Notify = sync.Notify;

// Lock-holder tracking for the tracked Mutex/RWMutex modes — `dumpLocks`
// answers "which locks are held, by what code?" on a wedged server.
const lock_registry = @import("lock_registry.zig");
pub const dumpLocks = lock_registry.dumpLocks;
pub const trackedLockCount = lock_registry.registeredCount;

const channel_mod = @import("channel.zig");
pub const Channel = channel_mod.Channel;
pub const CloseMode = channel_mod.CloseMode;

const group_mod = @import("group.zig");
pub const Group = group_mod.Group;

const watermark_mod = @import("watermark.zig");
pub const Watermark = watermark_mod.Watermark;

// Process-wide context (gpa/arena/io/args). Lives here — the universal base
// module — so any layer can reach it without a cycle (utils re-exports it as
// `utils.context` for existing callers). `coarse_clock` reads `context.g().io`.
pub const context = @import("context.zig");

// `sleepMs` lives in deadlines.zig — it parks on the per-worker Deadlines array
// and PANICS on a spurious wake (resumed before its own deadline fired).
pub const sleepMs = @import("deadlines.zig").sleepMs;

// From fs.zig — file I/O that cooperatively parks (zio.File/Dir-compatible).
pub const fs = @import("fs.zig");
pub const File = fs.File;
pub const Dir = fs.Dir;

// From fsops.zig — blocking-op offload pool (fsync + fs metadata) that parks
// the calling fiber and wakes it when a pool thread finishes the syscall.
pub const fsops = @import("fsops.zig");

// From net.zig — TCP networking with zio.net-compatible Stream/Server/
// IpAddress and the std.Io.Reader/Writer socket adapters.
pub const net = @import("net.zig");

