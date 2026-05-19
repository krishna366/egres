const std = @import("std");
const utils = @import("utils");

fn runGenExpectFail(alloc: std.mem.Allocator, schema_path: []const u8, expect_substr: []const u8) !void {
    const argv = [_][]const u8{
        "zig",
        "build",
        "zbuf",
        "--",
        schema_path,
    };

    const res = try std.process.run(alloc, utils.context.g().io, .{
        .argv = &argv,
    });
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
