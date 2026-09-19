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
//! lookups check `current` then `previous`; `rotate()` (caller-timed,
//! every few minutes) drops `previous` and starts a fresh `current`.
//! An entry therefore lives one to two rotation periods. The horizon
//! makes eviction safe:
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
const std = @import("std");
const fiber = @import("zfiber");
const zdb = @import("zdb");

pub const Oracle = struct {
    allocator: std.mem.Allocator,
    mu: fiber.RWMutex(.tracked) = .{},
    current: Generation = .{},
    previous: Generation = .{},
    /// See the module invariant. Only ever rises.
    horizon_ts: u64 = 0,

    const Generation = struct {
        /// key identity hash -> newest commit_ts this window.
        map: std.AutoHashMapUnmanaged(u64, u64) = .empty,
        /// Max commit_ts ever recorded in this generation; folded into
        /// `horizon_ts` when the generation is dropped.
        max_ts: u64 = 0,
    };

    pub fn init(allocator: std.mem.Allocator) Oracle {
        return .{ .allocator = allocator };
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
            const gop = try self.current.map.getOrPut(self.allocator, h);
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

    /// Age out the older generation: `previous` is dropped (its max
    /// folds into the horizon first), `current` becomes `previous`, and
    /// updates start over in a fresh map. The caller decides cadence;
    /// entries live one to two rotation periods.
    pub fn rotate(self: *Oracle) void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        if (self.previous.max_ts > self.horizon_ts)
            self.horizon_ts = self.previous.max_ts;
        self.previous.map.deinit(self.allocator);
        self.previous = self.current;
        self.current = .{};
    }
};
