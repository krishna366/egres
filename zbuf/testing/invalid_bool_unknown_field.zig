// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const schema = @import("schema");

pub const BadUnknown = schema.Table(&.{
    .{ .id = 1, .name = "flags", .ty = schema.U16 },
    .{ .name = "ghost", .ty = schema.Bool(.{ .field_id = 99, .bit = 1 }) },
});

pub const records: []const schema.Named = &.{
    .{ .name = "BadUnknown", .ty = BadUnknown, .id = 1 },
};
