const std = @import("std");
const Atomic = std.atomic.Value;
const Allocator = std.mem.Allocator;
const key_mod = @import("../key.zig");
const Key = key_mod.Key;
const Item = key_mod.Item;

pub const Error = error{
    OutOfMemory,
    ArenaFull,
};

pub const SkipList = struct {
    const Self = @This();
    const max_height = 16;
    const branch_probability = 4;
    const max_arena_size = 64 * 1024 * 1024;

    const Node = struct {
        key: []const u8,
        value: []const u8,
        next: []Atomic(?*Node),
    };

    // Concurrency contract:
    // - single writer calls `put`
    // - many readers may call `get`/iterate
    // - ownership is refcounted via `acquire`/`release`
    arena: std.heap.ArenaAllocator,
    head: *Node,
    max_height_seen: Atomic(u8),
    rng: std.Random.DefaultPrng,
    ref_count: Atomic(u32),
    node_count: Atomic(usize),

    pub fn init(allocator: Allocator) !Self {
        var arena = std.heap.ArenaAllocator.init(allocator);
        // Preheat first chunk so we start near the old 64 MiB capacity behavior.
        const warmup = try arena.allocator().alloc(u8, max_arena_size);
        _ = warmup;
        _ = arena.reset(.{ .retain_with_limit = max_arena_size });

        const next_arr = try arena.allocator().alloc(Atomic(?*Node), max_height);
        for (next_arr) |*p| p.* = Atomic(?*Node).init(null);
        const head = try arena.allocator().create(Node);
        head.* = .{ .key = &[_]u8{}, .value = &[_]u8{}, .next = next_arr };

        return Self{
            .arena = arena,
            .head = head,
            .max_height_seen = Atomic(u8).init(1),
            .rng = std.Random.DefaultPrng.init(@intCast(std.time.nanoTimestamp())),
            .ref_count = Atomic(u32).init(1),
            .node_count = Atomic(usize).init(0),
        };
    }

    pub fn acquire(self: *Self) void {
        const prev = self.ref_count.fetchAdd(1, .acquire);
        std.debug.assert(prev > 0);
    }

    pub fn release(self: *Self) void {
        const old_count = self.ref_count.fetchSub(1, .release);
        std.debug.assert(old_count > 0);
        if (old_count == 1) {
            self.arena.deinit();
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

    pub fn get(self: *Self, key: Key) ?[]const u8 {
        self.acquire();
        defer self.release();

        const result = self.findNode(key, null);
        if (result.exact) return result.node.?.value;
        return null;
    }

    pub fn put(self: *Self, key: Key, value: []const u8) Error!void {
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
            return;
        }

        const height = self.randomHeight();
        const next_arr = try alloc.alloc(Atomic(?*Node), height);
        for (next_arr) |*p| p.* = Atomic(?*Node).init(null);
        const new_node = try alloc.create(Node);
        new_node.* = .{ .key = keyWithTs, .value = valOwn, .next = next_arr };

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
    }

    pub fn contains(self: *Self, key: Key) bool {
        return self.get(key) != null;
    }

    pub fn numNodes(self: *Self) usize {
        return self.node_count.load(.monotonic);
    }

    pub fn memoryUsage(self: *Self) usize {
        return self.arena.state.end_index;
    }

    pub fn memoryCapacity(self: *Self) usize {
        return self.arena.queryCapacity();
    }

    pub const Iterator = struct {
        skiplist: *Self,
        current: ?*Node,

        pub fn init(skiplist: *Self) Iterator {
            skiplist.acquire();
            return .{
                .skiplist = skiplist,
                .current = skiplist.head.next[0].load(.acquire),
            };
        }

        pub fn deinit(self: *Iterator) void {
            self.skiplist.release();
        }

        pub fn next(self: *Iterator) ?Item {
            if (self.current) |node| {
                const result = Item{
                    .key = Key.init(node.key),
                    .value = node.value,
                };
                self.current = node.next[0].load(.acquire);
                return result;
            }
            return null;
        }

        pub fn seek(self: *Iterator, target: Key) ?Item {
            const find_result = self.skiplist.findNode(target, null);
            if (find_result.node) |node| {
                const result = Item{
                    .key = Key.init(node.key),
                    .value = node.value,
                };
                self.current = node.next[0].load(.acquire);
                return result;
            }
            self.current = null;
            return null;
        }
    };

    pub fn iterator(self: *Self) Iterator {
        return Iterator.init(self);
    }
};
