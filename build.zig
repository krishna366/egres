const std = @import("std");

const RaftTestSteps = struct {
    default_step: *std.Build.Step,
    all_step: *std.Build.Step,
};

fn bigTestNameLessThan(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.lessThan(u8, a, b);
}

fn discoverBigTestNames(b: *std.Build) []const []const u8 {
    const result = std.process.Child.run(.{
        .allocator = b.allocator,
        .argv = &.{
            "rg",
            "-n",
            "--no-heading",
            "--glob",
            "*.zig",
            "-F",
            "test \"BIGTEST ",
            ".",
        },
    }) catch |err| switch (err) {
        error.FileNotFound => @panic("missing required tool `rg` in PATH for BIGTEST discovery"),
        else => @panic("failed to execute `rg` for BIGTEST discovery"),
    };
    defer b.allocator.free(result.stdout);
    defer b.allocator.free(result.stderr);

    const ok = switch (result.term) {
        .Exited => |code| code == 0,
        else => false,
    };
    if (!ok) {
        std.debug.print("rg BIGTEST discovery failed:\n{s}\n", .{result.stderr});
        @panic("failed to discover BIGTEST names with `rg`");
    }

    var seen = std.StringHashMap(void).init(b.allocator);
    defer seen.deinit();

    var names = std.ArrayList([]const u8).empty;
    var lines = std.mem.splitScalar(u8, result.stdout, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;

        const marker = "test \"";
        const marker_pos = std.mem.indexOf(u8, line, marker) orelse continue;
        const name_start = marker_pos + marker.len;
        const tail = line[name_start..];
        const name_end = std.mem.indexOfScalar(u8, tail, '"') orelse continue;
        const name = tail[0..name_end];
        if (!std.mem.startsWith(u8, name, "BIGTEST ")) continue;
        if (seen.contains(name)) continue;

        const dup = b.allocator.dupe(u8, name) catch @panic("OOM duplicating BIGTEST name");
        names.append(b.allocator, dup) catch @panic("OOM storing BIGTEST names");
        seen.put(dup, {}) catch @panic("OOM indexing BIGTEST names");
    }

    if (names.items.len == 0) {
        @panic("no BIGTEST names discovered via `rg`; check tests naming convention");
    }

    std.mem.sort([]const u8, names.items, {}, bigTestNameLessThan);
    return names.toOwnedSlice(b.allocator) catch @panic("OOM finalizing BIGTEST list");
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    addServer(b, target, optimize);

    const schema_mod = makeSchemaModule(b, target, optimize);
    const regen_buffers_step = addRegenAllBuffers(b, target, optimize, schema_mod);
    const raft_mod = makeRaftModule(b, target, optimize);

    addZbufCli(b, target, optimize, schema_mod);
    addRaftDaemon(b, target, optimize, raft_mod);
    const zbuf_test_step = addZbufTests(b, target, optimize, regen_buffers_step);
    const raft_test_steps = addRaftTests(b, target, optimize, regen_buffers_step, raft_mod);
    const multiproc_step = addRaftMultiprocHarness(b, target, optimize, raft_mod, regen_buffers_step);

    const all_test_step = b.step("test", "Run all tests");
    all_test_step.dependOn(regen_buffers_step);
    all_test_step.dependOn(zbuf_test_step);
    all_test_step.dependOn(raft_test_steps.all_step);
    all_test_step.dependOn(multiproc_step);
}

fn makeModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    root_source_file: []const u8,
) *std.Build.Module {
    return b.createModule(.{
        .root_source_file = b.path(root_source_file),
        .target = target,
        .optimize = optimize,
    });
}

fn makeSchemaModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Module {
    return makeModule(b, target, optimize, "zbuf/schema.zig");
}

fn generatedPathForSchema(b: *std.Build, schema_path: []const u8) []const u8 {
    if (std.mem.endsWith(u8, schema_path, ".zig")) {
        return b.fmt("{s}.gen.zig", .{schema_path[0 .. schema_path.len - 4]});
    }
    return b.fmt("{s}.gen.zig", .{schema_path});
}

fn addZbufGenRun(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    schema_mod: *std.Build.Module,
    buffers_path: []const u8,
    out_path: []const u8,
    exe_name: []const u8,
) *std.Build.Step {
    const buffers_mod = makeModule(b, target, optimize, buffers_path);
    buffers_mod.addImport("schema", schema_mod);

    const gen = b.addExecutable(.{
        .name = exe_name,
        .root_module = makeModule(b, target, optimize, "zbuf/gen.zig"),
    });
    gen.root_module.addImport("schema", schema_mod);
    gen.root_module.addImport("buffers", buffers_mod);

    const run_gen = b.addRunArtifact(gen);
    run_gen.addArg(out_path);
    return &run_gen.step;
}

fn addRegenAllBuffers(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    schema_mod: *std.Build.Module,
) *std.Build.Step {
    const step = b.step("buffers", "Regenerate all discovered */buffers.gen.zig files");
    const schema_paths = collectBufferSchemas(b);
    for (schema_paths, 0..) |schema_path, idx| {
        const out_path = generatedPathForSchema(b, schema_path);
        const run_step = addZbufGenRun(
            b,
            target,
            optimize,
            schema_mod,
            schema_path,
            out_path,
            b.fmt("zbuf-gen-all-{d}", .{idx}),
        );
        step.dependOn(run_step);
    }
    return step;
}

fn collectBufferSchemas(b: *std.Build) [][]const u8 {
    var paths = std.ArrayList([]const u8).empty;
    var root_dir = std.fs.cwd().openDir(".", .{ .iterate = true }) catch {
        @panic("failed to open workspace dir");
    };
    defer root_dir.close();

    var walker = root_dir.walk(b.allocator) catch @panic("failed to walk workspace");
    defer walker.deinit();

    while (walker.next() catch @panic("failed to read workspace entry")) |entry| {
        if (entry.kind != .file) continue;
        const base = std.fs.path.basename(entry.path);
        if (!std.mem.eql(u8, base, "buffers.zig") and !std.mem.eql(u8, base, "buffers")) continue;
        if (isSkippedPath(entry.path)) continue;

        const dup = b.allocator.dupe(u8, entry.path) catch {
            @panic("out of memory collecting buffer schemas");
        };
        paths.append(b.allocator, dup) catch {
            @panic("out of memory collecting buffer schemas");
        };
    }
    return paths.toOwnedSlice(b.allocator) catch {
        @panic("out of memory collecting buffer schemas");
    };
}

fn isSkippedPath(path: []const u8) bool {
    return std.mem.startsWith(u8, path, ".zig-cache/") or
        std.mem.startsWith(u8, path, ".git/") or
        std.mem.startsWith(u8, path, "zig-out/") or
        std.mem.startsWith(u8, path, "node_modules/");
}

fn addServer(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) void {
    const exe = b.addExecutable(.{
        .name = "server",
        .root_module = makeModule(b, target, optimize, "server.zig"),
    });

    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);

    const run_step = b.step("run", "Run the server");
    run_step.dependOn(&run_cmd.step);
}

fn addZbufCli(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    schema_mod: *std.Build.Module,
) void {
    // Zbuf CLI: zig build zbuf -- path/to/buffers.zig
    // Defaults to u/buffers.zig if no args are provided.
    const buffers_path = if (b.args) |args| blk: {
        if (args.len > 0) break :blk args[0];
        break :blk "u/buffers.zig";
    } else "u/buffers.zig";

    const out_path = generatedPathForSchema(b, buffers_path);
    const run_gen_step = addZbufGenRun(
        b,
        target,
        optimize,
        schema_mod,
        buffers_path,
        out_path,
        "zbuf-gen-dyn",
    );

    const zbuf_cli_step = b.step("zbuf", "Generate buffers code from a schema path");
    zbuf_cli_step.dependOn(run_gen_step);
}

fn addZbufTests(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    regen_buffers_step: *std.Build.Step,
) *std.Build.Step {
    const zbuf_mod = makeModule(b, target, optimize, "zbuf.zig");

    const buffers_gen_mod = makeModule(b, target, optimize, "zbuf/buffers.gen.zig");
    buffers_gen_mod.addImport("zbuf", zbuf_mod);

    const zbuf_tests = b.addTest(.{
        .root_module = makeModule(b, target, optimize, "zbuf/buffers_test.zig"),
    });
    zbuf_tests.root_module.addImport("buffers_gen", buffers_gen_mod);
    zbuf_tests.root_module.addImport("zbuf", zbuf_mod);
    zbuf_tests.step.dependOn(regen_buffers_step);

    const zbuf_negative_tests = b.addTest(.{
        .root_module = makeModule(b, target, optimize, "zbuf/negative_test.zig"),
    });

    const zbuf_test_step = b.step("test-zbuf", "Run zbuf serialization tests");
    zbuf_test_step.dependOn(regen_buffers_step);
    zbuf_test_step.dependOn(&b.addRunArtifact(zbuf_tests).step);
    zbuf_test_step.dependOn(&b.addRunArtifact(zbuf_negative_tests).step);
    return zbuf_test_step;
}

fn addDiskwalTestRun(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    raft_mod: *std.Build.Module,
    build_options_mod: *std.Build.Module,
    regen_buffers_step: *std.Build.Step,
    filters: []const []const u8,
) *std.Build.Step {
    const test_artifact = b.addTest(.{
        .root_module = makeModule(b, target, optimize, "raft/diskwal_test.zig"),
        .filters = filters,
    });
    test_artifact.root_module.addImport("raft", raft_mod);
    test_artifact.root_module.addImport("build_options", build_options_mod);
    test_artifact.step.dependOn(regen_buffers_step);
    return &b.addRunArtifact(test_artifact).step;
}

fn makeRaftModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Module {
    const zbuf_mod = makeModule(b, target, optimize, "zbuf.zig");
    const utils_mod = makeModule(b, target, optimize, "u/utils.zig");

    const raft_buffers_gen_mod = makeModule(b, target, optimize, "raft/buffers.gen.zig");
    raft_buffers_gen_mod.addImport("zbuf", zbuf_mod);

    const raft_mod = makeModule(b, target, optimize, "raft/raft.zig");
    raft_mod.addImport("buffers_gen", raft_buffers_gen_mod);
    raft_mod.addImport("zbuf", zbuf_mod);
    raft_mod.addImport("utils", utils_mod);
    return raft_mod;
}

fn addRaftDaemon(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    raft_mod: *std.Build.Module,
) void {
    const raftd_exe = b.addExecutable(.{
        .name = "raftd",
        .root_module = makeModule(b, target, optimize, "raftd.zig"),
    });
    raftd_exe.root_module.addImport("raft", raft_mod);
    b.installArtifact(raftd_exe);

    const raftd_step = b.step("raftd", "Build raft daemon binary");
    raftd_step.dependOn(&raftd_exe.step);
}

fn addRaftMultiprocHarness(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    raft_mod: *std.Build.Module,
    regen_buffers_step: *std.Build.Step,
) *std.Build.Step {
    const harness_exe = b.addExecutable(.{
        .name = "raft-multiproc-harness",
        .root_module = makeModule(b, target, optimize, "raft/testing/multiproc_harness.zig"),
    });
    harness_exe.root_module.addImport("raft", raft_mod);

    const run = b.addRunArtifact(harness_exe);
    run.step.dependOn(regen_buffers_step);
    run.step.dependOn(b.getInstallStep());
    run.addArg("zig-out/bin/raftd");

    const step = b.step(
        "test-raft-multiproc",
        "Run multi-process raft integration harness",
    );
    step.dependOn(&run.step);
    return step;
}

fn addRaftTests(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    regen_buffers_step: *std.Build.Step,
    raft_mod: *std.Build.Module,
) RaftTestSteps {
    const raft_tests = b.addTest(.{
        .root_module = makeModule(b, target, optimize, "raft/testing/tests.zig"),
    });
    raft_tests.root_module.addImport("raft", raft_mod);
    raft_tests.step.dependOn(regen_buffers_step);

    const run_raft_tests = b.addRunArtifact(raft_tests);
    const run_raft_tests_trace = b.addRunArtifact(raft_tests);
    run_raft_tests_trace.stdio = .inherit;

    const raft_small_opts = b.addOptions();
    raft_small_opts.addOption(bool, "enable_bigtests", false);
    const raft_all_opts = b.addOptions();
    raft_all_opts.addOption(bool, "enable_bigtests", true);

    const run_diskwal_tests = addDiskwalTestRun(
        b,
        target,
        optimize,
        raft_mod,
        raft_small_opts.createModule(),
        regen_buffers_step,
        &.{},
    );
    const all_bigtests_opts_mod = raft_all_opts.createModule();
    const bigtest_names = discoverBigTestNames(b);

    const raft_test_step = b.step("test-raft", "Run raft tests");
    raft_test_step.dependOn(regen_buffers_step);
    raft_test_step.dependOn(&run_raft_tests.step);
    raft_test_step.dependOn(run_diskwal_tests);

    const raft_test_all_step = b.step("test-raft-all", "Run raft tests including BIGTEST cases");
    raft_test_all_step.dependOn(regen_buffers_step);
    raft_test_all_step.dependOn(&run_raft_tests.step);
    raft_test_all_step.dependOn(run_diskwal_tests);
    for (bigtest_names) |name| {
        const one_filter = b.allocator.alloc([]const u8, 1) catch {
            @panic("OOM allocating BIGTEST filter slice");
        };
        one_filter[0] = name;
        // TODO: For now, we're only doing BIGTEST here. Later, we can expand to other modules.
        // rg would help easily expand to those modules, so let's keep it for now.
        const run_big = addDiskwalTestRun(
            b,
            target,
            optimize,
            raft_mod,
            all_bigtests_opts_mod,
            regen_buffers_step,
            one_filter,
        );
        raft_test_all_step.dependOn(run_big);
    }

    const raft_test_trace_step = b.step("test-raft-trace", "Run raft tests with raft debug logs");
    raft_test_trace_step.dependOn(&run_raft_tests_trace.step);

    return .{
        .default_step = raft_test_step,
        .all_step = raft_test_all_step,
    };
}
