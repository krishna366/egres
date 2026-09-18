const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;

/// The compiler run blocks its thread in the pipe reads and the wait,
/// about half a second. On a fiber worker that would stall the worker's
/// loop, and every deadline registered there, for the whole run: the
/// timing tests in the suite would then fail under parallelism. So it
/// runs on the offload pool.
fn runCompiler(alloc: std.mem.Allocator, argv: []const String) std.process.RunError!std.process.RunResult {
    return std.process.run(alloc, utils.context.g().io, .{ .argv = argv });
}

fn runGenExpectFail(alloc: std.mem.Allocator, schema_path: String, expect_substr: String) !void {
    const argv = [_]String{
        "zig",
        "build",
        "zbuf",
        "--",
        schema_path,
    };

    const res = try fiber.fsops.run(runCompiler, .{ alloc, &argv });
    defer alloc.free(res.stdout);
    defer alloc.free(res.stderr);

    const exit_code = switch (res.term) {
        .exited => |code| code,
        else => return error.UnexpectedChildTermination,
    };
    try std.testing.expect(exit_code != 0);

    const in_stderr = std.mem.indexOf(u8, res.stderr, expect_substr) != null;
    const in_stdout = std.mem.indexOf(u8, res.stdout, expect_substr) != null;
    try std.testing.expect(in_stderr or in_stdout);
}

test "generator fails when bool references missing backing field" {
    try runGenExpectFail(
        std.testing.allocator,
        "zbuf/testing/invalid_bool_unknown_field.zig",
        "Bool field references unknown backing field id",
    );
}

test "generator fails when two bools map to same backing bit" {
    try runGenExpectFail(
        std.testing.allocator,
        "zbuf/testing/invalid_bool_duplicate_mapping.zig",
        "duplicate Bool mapping to same backing field bit",
    );
}

test "generator fails when bool bit index is out of range" {
    try runGenExpectFail(
        std.testing.allocator,
        "zbuf/testing/invalid_bool_bit_out_of_range.zig",
        "Bool field bit index exceeds backing field width",
    );
}
