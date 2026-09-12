const std = @import("std");
const Atomic = std.atomic.Value;
const Allocator = std.mem.Allocator;
const utils = @import("utils");
const String = utils.String;
const Key = @import("../kv.zig").Key;
const Item = @import("../kv.zig").Item;
const Meta = @import("../kv.zig").Meta;

pub const Error = error{
    OutOfMemory,
    ArenaFull,
};

pub const SkipList = struct {
    const Self = @This();
    const max_height = 16;
    const branch_probability = 4;
    const max_arena_size = 64 * 1024 * 1024;

    pub const Node = struct {
        key: String,
        value: String,
        // Not swapped atomically with `value` on overwrite — see `put`. The
        // key is immutable for a node's lifetime; only these two change.
        meta: Meta,
        next: []Atomic(?*Node),
    };

    // Concurrency contract:
    // - single writer calls `put`
    // - many readers may call `get`/iterate
    // - ownership is refcounted via `acquire`/`release`
    id: u64,
    /// Log index of the Raft entry that created this memtable (0 for the
    /// initial placeholder created at Forest.open before any replay).
    /// Used to compute the safe snapshot index — replay must be able to
    /// rebuild the oldest in-memory memtable from log entries above this
    /// index, so `snapshot.index <= oldest.created_at_log_index - 1`.
    created_at_log_index: u64,
    allocator: Allocator,
    arena: std.heap.ArenaAllocator,
    head: *Node,
    max_height_seen: Atomic(u8),
    rng: std.Random.DefaultPrng,
    ref_count: Atomic(u32),
    node_count: Atomic(usize),
    /// Live data bytes (key + value) written into this memtable. Used by
    /// `memoryUsage`/`shouldFreeze` instead of arena capacity: the arena is
    /// preheated to `max_arena_size`, so `queryCapacity()` reports the full
    /// reservation even when empty — which would make an empty memtable look
    /// "full" and trip the freeze threshold immediately.
    used_bytes: Atomic(usize),

    pub fn init(allocator: Allocator) !*Self {
        return initCapacity(allocator, max_arena_size);
    }

    /// `init` with a caller-chosen starting arena capacity. Pass 0 to
    /// skip the preheat and grow on demand — right for small private
    /// skiplists like a transaction's pending writes.
    pub fn initCapacity(allocator: Allocator, capacity: usize) !*Self {
        var arena = std.heap.ArenaAllocator.init(allocator);
        if (capacity > 0) {
            // Preheat the first chunk to `capacity` bytes.
            _ = try arena.allocator().alloc(u8, capacity);
            _ = arena.reset(.retain_capacity);
        }

        const next_arr = try arena.allocator().alloc(Atomic(?*Node), max_height);
        for (next_arr) |*p| p.* = Atomic(?*Node).init(null);
        const head = try arena.allocator().create(Node);
        head.* = .{
            .key = &[_]u8{},
            .value = &[_]u8{},
            .meta = .{},
            .next = next_arr,
        };

        const self = try allocator.create(Self);
        self.* = .{
            .id = 0,
            .created_at_log_index = 0,
            .allocator = allocator,
            .arena = arena,
            .head = head,
            .max_height_seen = Atomic(u8).init(1),
            .rng = std.Random.DefaultPrng.init(@intCast(std.Io.Timestamp.now(utils.context.g().io, .real).nanoseconds)),
            .ref_count = Atomic(u32).init(1),
            .node_count = Atomic(usize).init(0),
            .used_bytes = Atomic(usize).init(0),
        };
        return self;
    }

    pub fn acquire(self: *Self) void {
        const prev = self.ref_count.fetchAdd(1, .acquire);
        std.debug.assert(prev > 0);
    }

    pub fn release(self: *Self) void {
        const old_count = self.ref_count.fetchSub(1, .release);
        std.debug.assert(old_count > 0);
        if (old_count == 1) {
            const allocator = self.allocator;
            self.arena.deinit();
            allocator.destroy(self);
        }
    }

    fn randomHeight(self: *Self) u8 {
        var height: u8 = 1;
        const rand = self.rng.random();
        while (height < max_height and rand.uintLessThan(u32, branch_probability) == 0) {
            height += 1;
        }
        return height;
    }

    const FindResult = struct {
        node: ?*Node,
        exact: bool,
    };

    fn findNode(self: *Self, search_key: Key, lefties: ?*[max_height]?*Node) FindResult {
        var current = self.head;
        var level: i32 = @intCast(self.max_height_seen.load(.acquire) - 1);

        while (level >= 0) {
            const level_idx: usize = @intCast(level);
            // This loop always stops at the last node in the level, where next.key > given key and
            // current.key < given key. If ever next.key == given key, we'd return the node and mark
            // it as an exact match.
            // The lefties are useful when we want to add the node to the list.
            // But if we find an exact match, we don't care that lefties isn't being fully filled
            // because we're returning earlier (cmp == .eq).
            while (current.next[level_idx].load(.acquire)) |next| {
                const cmp = Key.init(next.key).order(search_key);
                if (cmp == .gt) break;
                if (cmp == .eq) return .{ .node = next, .exact = true };
                current = next;
            }

            if (lefties) |u| u[level_idx] = current;
            level -= 1;
        }

        return .{
            .node = current.next[0].load(.acquire),
            .exact = false,
        };
    }

    /// Look up a key. Returns an Item whose slices point into arena memory.
    /// Caller must ensure the skiplist stays alive (via acquire/release) while
    /// using the returned Item, or dupe it.
    pub fn get(self: *Self, key: Key) ?Item {
        const result = self.findNode(key, null);
        const node = result.node orelse return null;
        const found = Key.init(node.key);
        // The memtable is global across namespaces and tables, so a
        // findNode landing on the next key ≥ search can be a same-primary key
        // under a different (namespace, table_id). Filter all three.
        if (found.namespace != key.namespace) return null;
        if (found.table_id != key.table_id) return null;
        if (!found.equalPrimary(key)) return null;
        if (found.timestamp > key.timestamp) return null;

        return .{ .key = found, .value = node.value, .meta = node.meta };
    }

    /// Convenience for callers with nothing to say about the row: raw
    /// encoding, no flags. The write path uses putItem.
    pub fn put(self: *Self, key: Key, value: String) Error!void {
        return self.putItem(.{ .key = key, .value = value, .meta = .{} });
    }

    pub fn putItem(self: *Self, item: Item) Error!void {
        const key = item.key;
        const value = item.value;
        const alloc = self.arena.allocator();

        // Build full key for the findNode lookup. This key is allocated on arena.
        const keyWithTs = key.marshal(alloc) catch return Error.ArenaFull;
        const valOwn = alloc.dupe(u8, value) catch return Error.ArenaFull;

        var lefties = [_]?*Node{null} ** max_height;
        const find_result = self.findNode(key, &lefties);
        if (find_result.exact) {
            var existing = find_result.node.?;
            // The previous value can't really be deallocated in arena allocator.
            // So, don't worry about it. Allocate a new one and move on.
            existing.value = alloc.dupe(u8, value) catch return Error.ArenaFull;
            // value and meta are two separate non-atomic stores, so a
            // concurrent reader can see the new value with the old meta. Same
            // exposure the value store already had on its own (it's a fat
            // pointer), and it only arises on an exact overwrite: same key AND
            // same timestamp, i.e. twice within one raft batch. Fixing it
            // properly means one atomic pointer to an immutable {value, meta}
            // record, which costs an indirection on every read.
            existing.meta = item.meta;
            // Old value stays resident in the arena, so memory grows by the
            // new value's bytes even on overwrite.
            _ = self.used_bytes.fetchAdd(value.len, .monotonic);
            return;
        }

        const height = self.randomHeight();
        const next_arr = try alloc.alloc(Atomic(?*Node), height);
        for (next_arr) |*p| p.* = Atomic(?*Node).init(null);
        const new_node = try alloc.create(Node);
        new_node.* = .{
            .key = keyWithTs,
            .value = valOwn,
            .meta = item.meta,
            .next = next_arr,
        };

        // We expect multiple readers, but only expect one writer.
        // So, we can just do a load and store, without worrying about the value changing
        // since we loaded it.
        if (height > self.max_height_seen.load(.acquire)) {
            self.max_height_seen.store(height, .release);
        }

        // Here we're inserting new_node between lefties[i] and it's next.
        // If lefties[i] is null, then we're using head[i] as the lefties[i].
        for (0..height) |level| {
            const next = if (lefties[level]) |u| u.next[level].load(.acquire) else null;
            new_node.next[level].store(next, .release);

            if (lefties[level]) |left| {
                left.next[level].store(new_node, .release);
            } else {
                self.head.next[level].store(new_node, .release);
            }
        }

        _ = self.node_count.fetchAdd(1, .monotonic);
        _ = self.used_bytes.fetchAdd(keyWithTs.len + valOwn.len, .monotonic);
    }

    pub fn contains(self: *Self, key: Key) bool {
        return self.get(key) != null;
    }

    pub fn numNodes(self: *Self) usize {
        return self.node_count.load(.monotonic);
    }

    pub fn memoryUsage(self: *Self) usize {
        // Live data bytes (key + value) written, tracked on every `put`.
        // We deliberately do NOT use `arena.queryCapacity()`: the arena is
        // preheated to `max_arena_size`, so an empty memtable would report
        // the full 64 MiB reservation and trip `shouldFreeze` immediately,
        // spinning the newMemtable loop forever.
        return self.used_bytes.load(.monotonic);
    }

    pub fn memoryCapacity(self: *Self) usize {
        return self.arena.queryCapacity();
    }

    pub fn iterator(self: *Self) Iterator {
        return Iterator.init(self);
    }
};

/// Forward iterator over a SkipList with a peek/advance API matching the
/// merge iterator (see zdb/table/merge_iterator.zig). `acquire`s the
/// underlying skiplist on init; `release`s on deinit. Safe to hand
/// across the lifetime of a flush completion: as long as the iterator
/// holds a ref, the arena stays mapped even after the immutable's
/// owning slot in `Memtables` is popped.
pub const Iterator = struct {
    skiplist: *SkipList,
    current: ?*SkipList.Node,
    end_key: Key = .{},

    pub fn init(skiplist: *SkipList) Iterator {
        skiplist.acquire();
        return .{
            .skiplist = skiplist,
            .current = skiplist.head.next[0].load(.acquire),
        };
    }

    pub fn deinit(self: *Iterator) void {
        self.skiplist.release();
    }

    /// Position at the first entry with key >= `start` (inclusive).
    /// Empty `start.primary` means "first node." `end` is an
    /// exclusive upper bound; empty means "no upper bound."
    pub fn seekRange(self: *Iterator, start: Key, end: Key) !void {
        self.end_key = end;
        if (start.primary.len == 0) {
            self.current = self.skiplist.head.next[0].load(.acquire);
        } else {
            const find_result = self.skiplist.findNode(start, null);
            self.current = find_result.node;
        }
        self.checkEnd();
    }

    pub fn itemRef(self: *const Iterator) ?Item {
        const node = self.current orelse return null;
        return .{
            .key = Key.init(node.key),
            .value = node.value,
            .meta = node.meta,
        };
    }

    pub fn next(self: *Iterator) !void {
        const node = self.current orelse return;
        self.current = node.next[0].load(.acquire);
        self.checkEnd();
    }

    fn checkEnd(self: *Iterator) void {
        if (self.end_key.primary.len == 0) return;
        const node = self.current orelse return;
        if (Key.init(node.key).order(self.end_key) != .lt) self.current = null;
    }
};
