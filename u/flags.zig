const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;

pub const Error = error{
    MissingValue,
    UnknownOption,
    UnknownModule,
    DuplicateModule,
    InvalidBool,
    InvalidInt,
    InvalidFloat,
    InvalidEnum,
    InvalidDuration,
    InvalidArgv,
    /// A live update named a field the struct lists in `fixed`.
    FixedOption,
} || std.mem.Allocator.Error;

const whitespace = " \t\r\n";

// ---------- key normalisation ----------

fn normalizeKeyInto(name: String, buf: []u8) []u8 {
    std.debug.assert(buf.len >= name.len);
    for (name, 0..) |c, i| {
        const lower = std.ascii.toLower(c);
        buf[i] = if (lower == '_') '-' else lower;
    }
    return buf[0..name.len];
}

fn normalizeKeyAlloc(allocator: std.mem.Allocator, name: String) Error![]u8 {
    const out = try allocator.alloc(u8, name.len);
    _ = normalizeKeyInto(name, out);
    return out;
}

/// Comptime-evaluated canonical form of a struct field name.
/// Returns a fixed-size array (the caller takes its address with `&`).
fn comptimeCanonical(comptime name: String) [name.len]u8 {
    var buf: [name.len]u8 = undefined;
    for (name, 0..) |c, i| {
        const lower = std.ascii.toLower(c);
        buf[i] = if (lower == '_') '-' else lower;
    }
    return buf;
}

// ---------- value parsing ----------

fn parseBool(s: String) Error!bool {
    const trimmed = std.mem.trim(u8, s, whitespace);
    const true_set = [_]String{ "1", "t", "true", "y", "yes", "on" };
    const false_set = [_]String{ "0", "f", "false", "n", "no", "off" };
    for (true_set) |w| if (std.ascii.eqlIgnoreCase(trimmed, w)) return true;
    for (false_set) |w| if (std.ascii.eqlIgnoreCase(trimmed, w)) return false;
    return error.InvalidBool;
}

/// Parse a duration literal into milliseconds.
/// Forms:
///   - bare integer ("500" → 500 ms)
///   - "<int>d" for days
///   - concatenated <number><unit> segments using ns/us/µs/ms/s/m/h
///   - optional leading sign
fn parseDurationMs(raw: String) Error!i64 {
    const s = std.mem.trim(u8, raw, whitespace);
    if (s.len == 0) return 0;

    // Day shortcut: "<int>d"
    if (s[s.len - 1] == 'd') {
        const prefix = s[0 .. s.len - 1];
        if (std.fmt.parseInt(i64, prefix, 0)) |days| {
            return days * 24 * 60 * 60 * 1000;
        } else |_| {}
        // Fall through to general parser if it wasn't a clean "<int>d".
    }

    // Bare integer (no unit).
    if (std.fmt.parseInt(i64, s, 0)) |n| return n else |_| {}

    var i: usize = 0;
    var sign: f64 = 1;
    if (s[0] == '+') {
        i = 1;
    } else if (s[0] == '-') {
        sign = -1;
        i = 1;
    }
    if (i == s.len) return error.InvalidDuration;

    var total_ms: f64 = 0;
    var saw_segment = false;
    while (i < s.len) {
        const num_start = i;
        while (i < s.len and (std.ascii.isDigit(s[i]) or s[i] == '.')) : (i += 1) {}
        if (i == num_start) return error.InvalidDuration;
        const value = std.fmt.parseFloat(f64, s[num_start..i]) catch return error.InvalidDuration;

        const unit_start = i;
        while (i < s.len and !std.ascii.isDigit(s[i]) and s[i] != '.') : (i += 1) {}
        if (i == unit_start) return error.InvalidDuration;
        const unit = s[unit_start..i];

        const unit_ms: f64 = blk: {
            if (std.mem.eql(u8, unit, "ns")) break :blk 1.0 / 1_000_000.0;
            if (std.mem.eql(u8, unit, "us")) break :blk 1.0 / 1000.0;
            if (std.mem.eql(u8, unit, "µs")) break :blk 1.0 / 1000.0;
            if (std.mem.eql(u8, unit, "ms")) break :blk 1.0;
            if (std.mem.eql(u8, unit, "s")) break :blk 1000.0;
            if (std.mem.eql(u8, unit, "m")) break :blk 60_000.0;
            if (std.mem.eql(u8, unit, "h")) break :blk 3_600_000.0;
            return error.InvalidDuration;
        };
        total_ms += value * unit_ms;
        saw_segment = true;
    }
    if (!saw_segment) return error.InvalidDuration;
    return @intFromFloat(sign * total_ms);
}

// ---------- string tokenizer ----------

const KvMap = std.StringHashMap(String);

/// Tokenize a `k=v; k=v;` string into a map of normalized-key → raw-value.
/// Both keys and values point into freshly-allocated memory owned by `allocator`.
/// Caller is responsible for freeing entries (use freeKvMap).
fn parseFlagString(allocator: std.mem.Allocator, input: String) Error!KvMap {
    var map = KvMap.init(allocator);
    errdefer freeKvMap(allocator, &map);

    var it = std.mem.splitScalar(u8, input, ';');
    while (it.next()) |raw| {
        const segment = std.mem.trim(u8, raw, whitespace);
        if (segment.len == 0) continue;

        const eq = std.mem.indexOfScalar(u8, segment, '=') orelse return error.MissingValue;
        const raw_key = std.mem.trim(u8, segment[0..eq], whitespace);
        const raw_val = std.mem.trim(u8, segment[eq + 1 ..], whitespace);
        if (raw_key.len == 0) return error.MissingValue;

        const key = try normalizeKeyAlloc(allocator, raw_key);
        errdefer allocator.free(key);
        const val = try allocator.dupe(u8, raw_val);
        errdefer allocator.free(val);

        if (map.fetchRemove(key)) |old| {
            allocator.free(old.key);
            allocator.free(old.value);
        }
        try map.put(key, val);
    }
    return map;
}

fn freeKvMap(allocator: std.mem.Allocator, map: *KvMap) void {
    var it = map.iterator();
    while (it.next()) |entry| {
        allocator.free(entry.key_ptr.*);
        allocator.free(entry.value_ptr.*);
    }
    map.deinit();
}

// ---------- comptime shape validation ----------

fn isAllowedFieldType(comptime FT: type) bool {
    const info = @typeInfo(FT);
    return switch (info) {
        .bool => true,
        .int => |i| i.bits <= 64,
        .float => |f| f.bits == 32 or f.bits == 64,
        .@"enum" => true,
        .pointer => |p| blk: {
            if (p.size != .slice) break :blk false;
            if (p.child != u8) break :blk false;
            if (!p.is_const) break :blk false;
            break :blk true;
        },
        else => false,
    };
}

fn validateShape(comptime T: type) void {
    // The doc-string scan below walks every byte of every doc at comptime.
    @setEvalBranchQuota(200_000);
    const info = @typeInfo(T);
    if (info != .@"struct") {
        @compileError("flags.Live requires a struct type, got " ++ @typeName(T));
    }
    inline for (info.@"struct".fields) |f| {
        if (!isAllowedFieldType(f.type)) {
            @compileError("flags.Live: field '" ++ f.name ++
                "' has unsupported type " ++ @typeName(f.type) ++
                " (allowed: bool, int <=64, f32/f64, []const u8)");
        }
        if (std.mem.eql(u8, f.name, "self")) {
            @compileError("flags.Live: field name 'self' conflicts with the " ++
                "reserved 'docs.self' module-description key on " ++ @typeName(T));
        }
    }
    // Mandatory `pub const docs = .{ .field = "..." }`. Every field needs an
    // entry; every entry must name a real field. The special key `.self` is
    // an optional one-liner about the module itself (rendered next to the
    // module name in writeHelp).
    if (!@hasDecl(T, "docs")) {
        @compileError("flags.Live: '" ++ @typeName(T) ++ "' must declare " ++
            "'pub const docs = .{ .field = \"...\", ... }' covering every field");
    }
    const DocsT = @TypeOf(T.docs);
    const docs_info = @typeInfo(DocsT);
    if (docs_info != .@"struct") {
        @compileError("flags.Live: '" ++ @typeName(T) ++ ".docs' must be a struct literal");
    }
    inline for (info.@"struct".fields) |f| {
        if (!@hasField(DocsT, f.name)) {
            @compileError("flags.Live: '" ++ @typeName(T) ++ ".docs' is missing entry for field '" ++ f.name ++ "'");
        }
        // writeHelp prints the default from the struct itself, so a doc
        // string that restates it can only drift from it. Any case: "0 =
        // the default" and "false (default)" both name one.
        if (std.ascii.indexOfIgnoreCase(@field(T.docs, f.name), "default") != null) {
            @compileError("flags.Live: '" ++ @typeName(T) ++ ".docs." ++ f.name ++
                "' states a default; help already prints the field's default");
        }
    }
    inline for (docs_info.@"struct".fields) |df| {
        if (std.mem.eql(u8, df.name, "self")) continue; // module-level doc; allowed
        if (!@hasField(T, df.name)) {
            @compileError("flags.Live: '" ++ @typeName(T) ++ ".docs." ++ df.name ++
                "' has no matching field on " ++ @typeName(T));
        }
    }
    // Optional `pub const fixed = .{ "field", ... }`: fields read once at
    // startup. `update` refuses them and writeHelp says so; every name
    // must be a real field.
    if (@hasDecl(T, "fixed")) {
        inline for (T.fixed) |name| {
            if (!@hasField(T, name)) {
                @compileError("flags.Live: '" ++ @typeName(T) ++ ".fixed' names '" ++
                    name ++ "', which is not a field of " ++ @typeName(T));
            }
        }
    }
}

/// Whether `field_name` is in `T.fixed`: set at startup, refused by
/// `update`.
fn isFixed(comptime T: type, comptime field_name: String) bool {
    if (!@hasDecl(T, "fixed")) return false;
    inline for (T.fixed) |name| {
        if (std.mem.eql(u8, name, field_name)) return true;
    }
    return false;
}

/// Returns the doc string for `field_name`. Both T and the entry are
/// guaranteed by validateShape to exist.
fn docFor(comptime T: type, comptime field_name: String) String {
    return @field(T.docs, field_name);
}

/// Optional one-line module description from `T.docs.self`.
fn selfDocFor(comptime T: type) ?String {
    if (!@hasField(@TypeOf(T.docs), "self")) return null;
    return @field(T.docs, "self");
}

// ---------- per-field parser ----------

fn parseField(
    comptime FT: type,
    comptime field_name: String,
    value: String,
    arena: std.mem.Allocator,
) Error!FT {
    const info = @typeInfo(FT);
    return switch (info) {
        .bool => try parseBool(value),
        .int => |int_info| blk: {
            // Duration handling for integer fields whose name ends in "_ms".
            if (comptime std.mem.endsWith(u8, field_name, "_ms")) {
                const ms = try parseDurationMs(value);
                if (int_info.signedness == .unsigned and ms < 0) return error.InvalidInt;
                break :blk std.math.cast(FT, ms) orelse error.InvalidInt;
            }
            const trimmed = std.mem.trim(u8, value, whitespace);
            break :blk std.fmt.parseInt(FT, trimmed, 0) catch error.InvalidInt;
        },
        .float => blk: {
            const trimmed = std.mem.trim(u8, value, whitespace);
            break :blk std.fmt.parseFloat(FT, trimmed) catch error.InvalidFloat;
        },
        .@"enum" => std.meta.stringToEnum(FT, std.mem.trim(u8, value, whitespace)) orelse
            error.InvalidEnum,
        .pointer => try arena.dupe(u8, value),
        else => unreachable,
    };
}

// ---------- Live(T) ----------

pub fn Live(comptime T: type) type {
    comptime validateShape(T);
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        arena: std.heap.ArenaAllocator,
        mu: fiber.RWMutex(.untracked) = .{},
        /// The value as of the last `update`.
        latest: T = .{},
        version_val: u64 = 0,

        pub fn init(allocator: std.mem.Allocator, input: String) Error!Self {
            var self: Self = .{
                .allocator = allocator,
                .arena = std.heap.ArenaAllocator.init(allocator),
            };
            errdefer self.arena.deinit();
            if (input.len > 0) {
                var kv = try parseFlagString(allocator, input);
                defer freeKvMap(allocator, &kv);
                var it = kv.iterator();
                while (it.next()) |entry| {
                    try assignByKey(&self.latest, entry.key_ptr.*, entry.value_ptr.*, self.arena.allocator());
                }
            }
            return self;
        }

        /// A Live holding `value` as-is: for code that takes a Live but
        /// is given a struct, such as a test opening a forest.
        pub fn initFrom(allocator: std.mem.Allocator, value: T) Self {
            return .{
                .allocator = allocator,
                .arena = std.heap.ArenaAllocator.init(allocator),
                .latest = value,
            };
        }

        /// `initFrom` on the heap, at an address that outlives the
        /// caller's frame: what a Forest keeps a pointer to. `destroy`
        /// releases it.
        pub fn create(allocator: std.mem.Allocator, value: T) !*Self {
            const self = try allocator.create(Self);
            self.* = initFrom(allocator, value);
            return self;
        }

        pub fn destroy(self: *Self) void {
            const allocator = self.allocator;
            self.deinit();
            allocator.destroy(self);
        }

        pub fn deinit(self: *Self) void {
            self.arena.deinit();
        }

        /// A live change, after startup: refuses any field in `T.fixed`.
        pub fn update(self: *Self, input: String) Error!void {
            return self.apply(input, false);
        }

        /// The startup assignment (the command line): every field may
        /// be set, fixed ones included. Fixed means fixed once the
        /// process is running, not absent from the command line.
        pub fn set(self: *Self, input: String) Error!void {
            return self.apply(input, true);
        }

        fn apply(self: *Self, input: String, allow_fixed: bool) Error!void {
            // Parse the input outside any lock — that work touches no shared
            // state. The coerce-and-install phase runs under the exclusive
            // write lock so the arena (not thread-safe) is single-writer.
            var kv = try parseFlagString(self.allocator, input);
            defer freeKvMap(self.allocator, &kv);

            // A fixed field anywhere in the input refuses the whole
            // update, before any live field in it is changed.
            if (!allow_fixed) {
                var keys = kv.keyIterator();
                while (keys.next()) |key| {
                    if (fixedByKey(key.*)) return error.FixedOption;
                }
            }

            self.mu.lock(@src());
            defer self.mu.unlock();

            var next: T = self.latest;
            var it = kv.iterator();
            while (it.next()) |entry| {
                try assignByKey(&next, entry.key_ptr.*, entry.value_ptr.*, self.arena.allocator());
            }
            self.latest = next;
            self.version_val += 1;
        }

        /// Whether `key` (canonical, as parsed) names a field in `T.fixed`.
        fn fixedByKey(key: String) bool {
            @setEvalBranchQuota(20_000);
            const fields = @typeInfo(T).@"struct".fields;
            inline for (fields) |f| {
                if (comptime !isFixed(T, f.name)) continue;
                const canonical = comptime comptimeCanonical(f.name);
                if (std.mem.eql(u8, &canonical, key)) return true;
            }
            return false;
        }

        pub fn version(self: *Self) u64 {
            self.mu.lockShared(@src());
            defer self.mu.unlockShared();
            return self.version_val;
        }

        /// The value now: one copy under the shared lock. Take it once
        /// per unit of work, not per field. A field in `T.fixed` reads
        /// the same at every call, since `update` refuses it.
        pub fn cur(self: *Self) T {
            self.mu.lockShared(@src());
            defer self.mu.unlockShared();
            return self.latest;
        }

        fn assignByKey(target: *T, key: String, value: String, arena: std.mem.Allocator) Error!void {
            // One canonical name per field, computed a character at a
            // time: a struct with a few dozen fields passes the default
            // comptime branch budget.
            @setEvalBranchQuota(20_000);
            const fields = @typeInfo(T).@"struct".fields;
            inline for (fields) |f| {
                const canonical = comptime comptimeCanonical(f.name);
                if (std.mem.eql(u8, &canonical, key)) {
                    @field(target, f.name) = try parseField(f.type, f.name, value, arena);
                    return;
                }
            }
            return error.UnknownOption;
        }
    };
}

// ---------- comptime help renderer (per-struct) ----------

/// Renders a help block for one module, mirroring ristretto/z SuperFlagHelp:
///
///   <module> -- <T.docs.self>     (the "-- ..." part is omitted if absent)
///       <field>=<default> (<type>[, <default in human units>]); <field doc>
///       ...
///
/// Every line is wrapped at `help_width` columns; a continuation line is
/// indented past the line it continues. The default comes from the struct's
/// own field default, so doc strings never restate it (validateShape rejects
/// one that does).
pub fn writeHelp(
    comptime T: type,
    name: String,
    out: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
) Error!void {
    comptime validateShape(T);

    var line: HelpLine = .{ .out = out, .allocator = allocator, .indent = 4 };
    try line.start("  ");
    try line.word(name);
    if (comptime selfDocFor(T)) |s| {
        try line.word("--");
        try line.text(s);
    }
    try out.append(allocator, '\n');

    const fields = @typeInfo(T).@"struct".fields;
    const default_t: T = .{};
    line.indent = 10;
    inline for (fields) |f| {
        const canonical = comptime comptimeCanonical(f.name);
        var head: std.ArrayList(u8) = .empty;
        defer head.deinit(allocator);

        try head.appendSlice(allocator, &canonical);
        try head.append(allocator, '=');
        try writeDefault(f.type, @field(default_t, f.name), &head, allocator);
        try line.start("      ");
        try line.word(head.items);

        utils.clearRetainCap(&head);
        try head.append(allocator, '(');
        try head.appendSlice(allocator, comptime humanType(f.type, f.name));
        try writeHumanDefault(f.type, f.name, @field(default_t, f.name), &head, allocator);
        try head.appendSlice(allocator, ");");
        try line.text(head.items);

        try line.text(comptime docFor(T, f.name));
        if (comptime isFixed(T, f.name)) try line.text("Fixed at startup.");
        try out.append(allocator, '\n');
    }
}

/// Help output never runs past this column, except for a single word that is
/// itself longer (a URL in a doc string).
pub const help_width: usize = 1000;

/// One logical help line being written into `out`, wrapped at `help_width`.
const HelpLine = struct {
    out: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    /// Spaces a continuation line starts with.
    indent: usize,
    /// Columns written on the current output line.
    col: usize = 0,
    /// Whether the next word needs a space before it.
    spaced: bool = false,

    fn start(self: *HelpLine, prefix: String) Error!void {
        try self.out.appendSlice(self.allocator, prefix);
        self.col = prefix.len;
        self.spaced = false;
    }

    /// Append every space-separated word of `s`.
    fn text(self: *HelpLine, s: String) Error!void {
        var words = std.mem.tokenizeScalar(u8, s, ' ');
        while (words.next()) |w| try self.word(w);
    }

    fn word(self: *HelpLine, w: String) Error!void {
        // Columns, not bytes: doc strings carry the odd multi-byte character.
        const width = std.unicode.utf8CountCodepoints(w) catch w.len;
        const sep: usize = @intFromBool(self.spaced);
        if (self.spaced and self.col + sep + width > help_width) {
            try self.out.append(self.allocator, '\n');
            try self.out.appendNTimes(self.allocator, ' ', self.indent);
            self.col = self.indent;
        } else if (self.spaced) {
            try self.out.append(self.allocator, ' ');
            self.col += 1;
        }
        try self.out.appendSlice(self.allocator, w);
        self.col += width;
        self.spaced = true;
    }
};

/// For a byte count or a duration, append ", <value in a readable unit>":
/// `min-bytes=1073741824 (u64, 1 GiB)`. The unit comes from the field name's
/// suffix. Nothing is appended when the raw number already reads well (zero,
/// or not a whole number of the next unit up).
fn writeHumanDefault(
    comptime FT: type,
    comptime field_name: String,
    value: FT,
    out: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
) Error!void {
    if (@typeInfo(FT) != .int) return;
    if (value <= 0) return;
    const v: u64 = @intCast(value);
    const Unit = struct { per: u64, name: String };
    const units: []const Unit = comptime if (std.mem.endsWith(u8, field_name, "_bytes"))
        &.{ .{ .per = 1 << 30, .name = "GiB" }, .{ .per = 1 << 20, .name = "MiB" }, .{ .per = 1 << 10, .name = "KiB" } }
    else if (std.mem.endsWith(u8, field_name, "_seconds"))
        &.{ .{ .per = 3600, .name = "h" }, .{ .per = 60, .name = "min" } }
    else if (std.mem.endsWith(u8, field_name, "_ms"))
        &.{ .{ .per = 60_000, .name = "min" }, .{ .per = 1000, .name = "s" } }
    else
        &.{};
    for (units) |u| {
        if (v % u.per != 0) continue;
        try out.print(allocator, ", {d} {s}", .{ v / u.per, u.name });
        return;
    }
}

fn humanType(comptime FT: type, comptime field_name: String) String {
    const info = @typeInfo(FT);
    return switch (info) {
        .bool => "bool",
        .int => blk: {
            if (comptime std.mem.endsWith(u8, field_name, "_ms")) break :blk "duration";
            break :blk @typeName(FT);
        },
        .float => "float",
        .@"enum" => |e| comptime blk: {
            var names: String = "";
            for (e.fields, 0..) |f, i| {
                names = names ++ (if (i == 0) "" else "|") ++ f.name;
            }
            break :blk names;
        },
        .pointer => "string",
        else => unreachable,
    };
}

fn writeDefault(comptime FT: type, value: FT, out: *std.ArrayList(u8), allocator: std.mem.Allocator) Error!void {
    const info = @typeInfo(FT);
    switch (info) {
        .bool => try out.appendSlice(allocator, if (value) "true" else "false"),
        .int => try out.print(allocator, "{d}", .{value}),
        .float => try out.print(allocator, "{d}", .{value}),
        .@"enum" => try out.appendSlice(allocator, @tagName(value)),
        .pointer => try out.appendSlice(allocator, value),
        else => unreachable,
    }
}

// ---------- Registry ----------

const Entry = struct {
    live_ptr: *anyopaque,
    update_fn: *const fn (*anyopaque, String) Error!void,
    set_fn: *const fn (*anyopaque, String) Error!void,
    help_fn: *const fn (*anyopaque, String, *std.ArrayList(u8), std.mem.Allocator) Error!void,
    deinit_fn: *const fn (*anyopaque, std.mem.Allocator) void,
};

pub const Registry = struct {
    allocator: std.mem.Allocator,
    mu: fiber.Mutex(.untracked) = .{},
    entries: std.StringHashMap(Entry),

    pub fn init(allocator: std.mem.Allocator) Registry {
        return .{
            .allocator = allocator,
            .entries = std.StringHashMap(Entry).init(allocator),
        };
    }

    pub fn deinit(self: *Registry) void {
        var it = self.entries.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit_fn(entry.value_ptr.live_ptr, self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.entries.deinit();
    }

    /// Allocate a Live(T) owned by this registry, populate it from struct
    /// defaults, and return a pointer for the caller to read from. The
    /// registry will deinit the Live(T) when the registry itself is deinited;
    /// the caller MUST NOT call deinit() on the returned pointer.
    pub fn register(
        self: *Registry,
        name: String,
        comptime T: type,
    ) Error!*Live(T) {
        const Wrappers = struct {
            fn update(p: *anyopaque, input: String) Error!void {
                const l: *Live(T) = @ptrCast(@alignCast(p));
                return l.update(input);
            }
            fn set(p: *anyopaque, input: String) Error!void {
                const l: *Live(T) = @ptrCast(@alignCast(p));
                return l.set(input);
            }
            fn help(p: *anyopaque, module_name: String, out: *std.ArrayList(u8), alloc: std.mem.Allocator) Error!void {
                _ = p;
                return writeHelp(T, module_name, out, alloc);
            }
            fn deinit(p: *anyopaque, alloc: std.mem.Allocator) void {
                const l: *Live(T) = @ptrCast(@alignCast(p));
                l.deinit();
                alloc.destroy(l);
            }
        };

        const live = try self.allocator.create(Live(T));
        errdefer self.allocator.destroy(live);

        live.* = try Live(T).init(self.allocator, "");
        errdefer live.deinit();

        const key = try normalizeKeyAlloc(self.allocator, name);
        errdefer self.allocator.free(key);

        self.mu.lock(@src());
        defer self.mu.unlock();
        if (self.entries.contains(key)) return error.DuplicateModule;

        try self.entries.put(key, .{
            .live_ptr = @ptrCast(live),
            .update_fn = Wrappers.update,
            .set_fn = Wrappers.set,
            .help_fn = Wrappers.help,
            .deinit_fn = Wrappers.deinit,
        });
        return live;
    }

    /// A live change to one module: `Live.update`, so a fixed field is
    /// refused.
    pub fn update(self: *Registry, name: String, input: String) Error!void {
        const entry = try self.lookup(name);
        return entry.update_fn(entry.live_ptr, input);
    }

    /// The startup assignment to one module: `Live.set`, fixed fields
    /// included. What `parseArgv` uses.
    pub fn set(self: *Registry, name: String, input: String) Error!void {
        const entry = try self.lookup(name);
        return entry.set_fn(entry.live_ptr, input);
    }

    fn lookup(self: *Registry, name: String) Error!Entry {
        var name_buf: [256]u8 = undefined;
        if (name.len > name_buf.len) return error.UnknownModule;
        const key = normalizeKeyInto(name, &name_buf);

        self.mu.lock(@src());
        defer self.mu.unlock();
        return self.entries.get(key) orelse error.UnknownModule;
    }

    /// Walk a CLI argv slice and dispatch each `--<module>` arg to its
    /// registered Live(T).update. Three input shapes are handled:
    ///
    ///   1. `--<name>=<payload>`         — name + payload in one arg
    ///   2. `--<name>` then `<payload>`  — name and payload as two args
    ///   3. anything else                — pushed verbatim into `leftover`
    ///
    /// A bare `--` switches off flag parsing for the rest of argv (everything
    /// after it goes to `leftover`), matching the GNU convention.
    ///
    /// On `error.UnknownModule`, parsing stops at the offending arg. Modules
    /// dispatched before that point keep their updates; this is best-effort,
    /// not transactional.
    pub fn parseArgv(
        self: *Registry,
        argv: []const String,
        leftover: *std.ArrayList(String),
    ) Error!void {
        var i: usize = 0;
        while (i < argv.len) : (i += 1) {
            const arg = argv[i];

            // Non-flag positional arg — caller's responsibility.
            if (!std.mem.startsWith(u8, arg, "--")) {
                try leftover.append(self.allocator, arg);
                continue;
            }

            // Bare "--": stop parsing flags; remaining args are all positional.
            if (arg.len == 2) {
                i += 1;
                while (i < argv.len) : (i += 1) try leftover.append(self.allocator, argv[i]);
                return;
            }

            // `body` is the text after the leading `--`, e.g. "raft=tracing=true;"
            // or just "raft" if the payload is the next arg.
            const body = arg[2..];

            if (std.mem.indexOfScalar(u8, body, '=')) |eq| {
                // Form 1: --<name>=<payload>. Split on the FIRST `=` only —
                // the payload itself contains its own `=` separators.
                // Strip any surviving outer quotes around the payload.
                try self.set(body[0..eq], stripSurroundingQuotes(body[eq + 1 ..]));
            } else {
                // Form 2: --<name> <payload>. Consume the next arg as payload;
                // a trailing --<name> with no follow-up is a malformed argv.
                if (i + 1 >= argv.len) return error.InvalidArgv;
                try self.set(body, stripSurroundingQuotes(argv[i + 1]));
                i += 1;
            }
        }
    }

    pub fn writeHelpAll(
        self: *Registry,
        out: *std.ArrayList(u8),
    ) Error!void {
        // Snapshot entries under the lock: a concurrent register() could resize
        // the hashmap and invalidate the iterator. help_fn is stateless (comptime-
        // derived from T), so we needn't hold any lock across the per-module render
        // — and dropping it first keeps the registry responsive during a long write.
        self.mu.lock(@src());
        var names = std.ArrayList(String).empty;
        defer names.deinit(self.allocator);
        var it = self.entries.keyIterator();
        while (it.next()) |k| names.append(self.allocator, k.*) catch {
            self.mu.unlock();
            return error.OutOfMemory;
        };
        std.mem.sort(String, names.items, {}, lessThanSlice);
        // Build a parallel list of entries so we don't re-look-up.
        var entries = std.ArrayList(Entry).empty;
        defer entries.deinit(self.allocator);
        for (names.items) |n| {
            entries.append(self.allocator, self.entries.get(n).?) catch {
                self.mu.unlock();
                return error.OutOfMemory;
            };
        }
        self.mu.unlock();

        for (names.items, entries.items, 0..) |n, e, idx| {
            if (idx > 0) try out.append(self.allocator, '\n');
            try e.help_fn(e.live_ptr, n, out, self.allocator);
        }
    }
};

fn lessThanSlice(_: void, a: String, b: String) bool {
    return std.mem.lessThan(u8, a, b);
}

/// Strip a single matched pair of surrounding `"..."` or `'...'` quotes.
/// Used for argv payloads where the shell may not have stripped them
/// (e.g. payloads injected from config files, wrapper scripts, or test code
/// that builds argv literally). Unmatched or absent quotes pass through.
fn stripSurroundingQuotes(s: String) String {
    if (s.len < 2) return s;
    const first = s[0];
    if (first != '"' and first != '\'') return s;
    if (s[s.len - 1] != first) return s;
    return s[1 .. s.len - 1];
}

// ---------- global registry ----------

var g_registry: ?Registry = null;

/// Initialise the process-wide flag registry. Must be called once before g().
pub fn initGlobal(allocator: std.mem.Allocator) void {
    std.debug.assert(g_registry == null);
    g_registry = Registry.init(allocator);
}

/// Tear down the global registry and every Live(T) it owns.
pub fn deinitGlobal() void {
    if (g_registry) |*reg| {
        reg.deinit();
        g_registry = null;
    }
}

/// Process-wide flag registry. Panics if initGlobal was not called.
pub fn g() *Registry {
    if (g_registry == null) @panic("flags.initGlobal() must be called first");
    return &g_registry.?;
}
