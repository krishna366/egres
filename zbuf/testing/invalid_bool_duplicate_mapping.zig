// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const schema = @import("schema");

pub const BadDuplicate = schema.Table(&.{
    .{ .id = 1, .name = "flags", .ty = schema.U16 },
    .{ .name = "one", .ty = schema.Bool(.{ .field_id = 1, .bit = 7 }) },
    .{ .name = "two", .ty = schema.Bool(.{ .field_id = 1, .bit = 7 }) },
});

pub const records: []const schema.Named = &.{
    .{ .name = "BadDuplicate", .ty = BadDuplicate, .id = 1 },
};
