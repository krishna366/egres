//! Oracle: which keys committed recently, and at what timestamp.
//!
//! One map per namespace, keyed by row/table identity hash
//! (zdb `Key.identityHash` / `TableKey.identityHash`), holding the
//! newest known `commit_ts` per key. Two consumers, one question —
//! "has this key changed since a transaction's read_ts?":
//!   - write path: after taking a row lock, `commitTs(h) > read_ts`
//!     means a conflicting commit landed after the txn began — bump
//!     read_ts to that value (or abort).
//!   - commit validation: a bumped txn re-asks the same question for
//!     every key it read.
//! `updateItems` runs on the apply path, BEFORE the committing txn's
//! locks release — so a later acquirer of any written key always sees
//! its commit.
//!
//! Memory is bounded by TWO GENERATIONS: updates go into `current`;
//! lookups check `current` then `previous`; a rotation retires
//! `previous`, `current` takes its place, and updates start over. A
//! generation ends at `max_keys` entries, or on the caller's clock
//! (`rotate`, every few minutes) when writes are slow. An entry
//! therefore lives one to two generations. The horizon makes eviction
//! safe:
//!
//!   INVARIANT: every commit with `commit_ts > horizon_ts` is present
//!   in current or previous.
//!
//! Rotation maintains it without any wall-clock reasoning: each
//! generation tracks the max commit_ts ever written into it, and the
//! horizon max-folds the dying generation's. An absent key's last
//! commit is thus provably <= horizon_ts, and `commitTs` answers
//! `horizon_ts` for it — conservative (can demand an unneeded bump,
//! for transactions older than a rotation period) but never wrong (a
//! conflict can never hide). Nothing here needs to survive a leader
//! change: a new leader's transactions begin with read_ts at the
//! applied frontier, at or above every commit that ever registered.
//!
//! The two maps are sized for `max_keys` once and swapped, never
//! freed or grown: a generation's map is cleared and reused as the
//! next `current`. Under a 160k rows/s load a time-based generation
//! reached 64M keys, and each doubling of the map rehashed it under
//! the lock on the apply fiber, stalling every commit for seconds.
const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const zdb = @import("zdb");

pub const Oracle = struct {
    allocator: std.mem.Allocator,
    mu: fiber.RWMutex(.tracked) = .{},
    current: Generation = .{},
    previous: Generation = .{},
    /// See the module invariant. Only ever rises.
    horizon_ts: u64 = 0,
    /// Coarse clock at the last rotation of either kind. `rotate`
    /// skips a tick that would retire a generation younger than the
    /// period, so an entry always lives at least one period.
    last_rotation_ms: i64,
    /// Entries per generation; the maps hold this many without a
    /// grow. At 100k writes/s the default is an 80 s window, which a
    /// transaction may outlive at the cost of a conservative bump.
    max_keys: u32,

    /// 8M entries: about 270 MB per map with the hash map's load
    /// factor, two maps per namespace.
    pub const default_max_keys: u32 = 8 << 20;

    const Generation = struct {
        /// key identity hash -> newest commit_ts this window.
        map: std.AutoHashMapUnmanaged(u64, u64) = .empty,
        /// Max commit_ts ever recorded in this generation; folded into
        /// `horizon_ts` when the generation is dropped.
        max_ts: u64 = 0,
    };

    pub fn init(allocator: std.mem.Allocator, max_keys: u32) !Oracle {
        var self: Oracle = .{
            .allocator = allocator,
            .max_keys = max_keys,
            .last_rotation_ms = utils.coarseMs(),
        };
        errdefer self.deinit();
        try self.current.map.ensureTotalCapacity(allocator, max_keys);
        try self.previous.map.ensureTotalCapacity(allocator, max_keys);
        return self;
    }

    pub fn deinit(self: *Oracle) void {
        self.current.map.deinit(self.allocator);
        self.previous.map.deinit(self.allocator);
        self.mu.deinit();
    }

    /// Record a run of committed items — one mu hold for the whole write
    /// set (the apply fiber records a commit at once). Each item's
    /// version timestamp is its commit ts (apply stamps it with the
    /// entry's HLC); apply order makes per-key timestamps non-decreasing,
    /// and the max-fold keeps a same-window re-commit from regressing.
    pub fn updateItems(self: *Oracle, items: []const zdb.kv.Item) !void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        for (items) |item| {
            const h = item.key.identityHash();
            const ts = item.newest().ts;
            // A full generation ends here, on the apply fiber, so the
            // insert below always has room and never grows the map.
            if (self.current.map.count() >= self.max_keys) self.rotateLocked();
            const gop = self.current.map.getOrPutAssumeCapacity(h);
            if (!gop.found_existing or gop.value_ptr.* < ts)
                gop.value_ptr.* = ts;
            if (ts > self.current.max_ts) self.current.max_ts = ts;
        }
    }

    /// The newest commit_ts known for key-hash `h`. For a key with no
    /// live entry this is `horizon_ts` — an upper bound on whatever its
    /// true last commit was (module invariant), so callers comparing
    /// `commitTs > read_ts` err toward seeing a conflict, never miss
    /// one.
    pub fn commitTs(self: *Oracle, h: u64) u64 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        return self.lookupLocked(h);
    }

    /// The newest commit_ts across a whole set of key-hashes — one
    /// mu hold instead of a call per key (commit validation checks a
    /// txn's full read set at once). Absent keys contribute
    /// `horizon_ts`, as in `commitTs`; an empty set answers 0.
    pub fn maxCommitTs(self: *Oracle, hashes: []const u64) u64 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        var max: u64 = 0;
        for (hashes) |h| max = @max(max, self.lookupLocked(h));
        return max;
    }

    fn lookupLocked(self: *Oracle, h: u64) u64 {
        // A re-committed key can sit in both generations; `current`
        // holds the newer value.
        return self.current.map.get(h) orelse
            self.previous.map.get(h) orelse
            self.horizon_ts;
    }

    /// Age out the older generation on the caller's clock: `previous`
    /// is retired (its max folds into the horizon first), `current`
    /// becomes `previous`, and updates start over. A generation also
    /// ends on its own when it reaches `max_keys` (see `updateItems`);
    /// a tick within `period_ms` of any rotation is skipped, or it
    /// would retire entries written seconds ago and force needless
    /// bumps. Pass 0 to rotate unconditionally.
    pub fn rotate(self: *Oracle, period_ms: u64) void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        const age: u64 = @intCast(utils.coarseMs() - self.last_rotation_ms);
        if (age < period_ms) return;
        self.rotateLocked();
    }

    /// The retired map is cleared and reused as the new `current`, so
    /// the maps keep their capacity for the life of the oracle.
    fn rotateLocked(self: *Oracle) void {
        if (self.previous.max_ts > self.horizon_ts)
            self.horizon_ts = self.previous.max_ts;
        std.mem.swap(Generation, &self.previous, &self.current);
        self.current.map.clearRetainingCapacity();
        self.current.max_ts = 0;
        self.last_rotation_ms = utils.coarseMs();
    }
};
