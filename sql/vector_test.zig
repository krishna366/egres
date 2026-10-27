// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const sql = @import("sql");

test "sql.vector: exact distances follow pgvector operator semantics" {
    const alloc = testing.allocator;
    const a = try sql.value.parseVector(alloc, "[1,0,0]", 3);
    defer alloc.free(a.vector);
    const b = try sql.value.parseVector(alloc, "[0,1,0]", 3);
    defer alloc.free(b.vector);
    const c = try sql.value.parseVector(alloc, "[2,0,0]", 3);
    defer alloc.free(c.vector);

    try testing.expectApproxEqAbs(@as(f64, 1.4142135623730951), try sql.vector.l2(a.vector, b.vector), 1e-12);
    try testing.expectApproxEqAbs(@as(f64, 1), try sql.vector.cosine(a.vector, b.vector), 1e-12);
    try testing.expectApproxEqAbs(@as(f64, -2), try sql.vector.negativeInnerProduct(a.vector, c.vector), 1e-12);
}

test "sql.vector: invalid dimensions and zero cosine inputs are rejected" {
    const alloc = testing.allocator;
    const two = try sql.value.parseVector(alloc, "[1,2]", 2);
    defer alloc.free(two.vector);
    const three = try sql.value.parseVector(alloc, "[1,2,3]", 3);
    defer alloc.free(three.vector);
    const zero = try sql.value.parseVector(alloc, "[0,0]", 2);
    defer alloc.free(zero.vector);

    try testing.expectError(error.DimensionMismatch, sql.vector.l2(two.vector, three.vector));
    try testing.expectError(error.ZeroVector, sql.vector.cosine(two.vector, zero.vector));
    try testing.expectError(error.InvalidVector, sql.vector.l2("bad", two.vector));
}
