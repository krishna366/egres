// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Exact dense-vector distance functions over the row codec's float32 bytes.
//!
//! Vectors are stored as little-endian IEEE-754 float32 elements (value.zig).
//! Accumulating in float64 keeps the scalar operators stable for practical
//! embedding dimensions without changing the stored representation.

const std = @import("std");
const _value = @import("value.zig");

const String = _value.String;

pub const Error = error{
    DimensionMismatch,
    InvalidVector,
    ZeroVector,
    NonFiniteResult,
};

/// Euclidean (`<->`) distance between equally dimensioned vectors.
pub fn l2(lhs: String, rhs: String) Error!f64 {
    try validatePair(lhs, rhs);
    var sum: f64 = 0;
    for (0..lhs.len / 4) |i| {
        const delta = element(lhs, i) - element(rhs, i);
        sum += delta * delta;
    }
    const result = std.math.sqrt(sum);
    if (!std.math.isFinite(result)) return error.NonFiniteResult;
    return result;
}

/// Cosine (`<=>`) distance: one minus cosine similarity. A zero vector has
/// no cosine direction and is rejected instead of receiving an arbitrary rank.
pub fn cosine(lhs: String, rhs: String) Error!f64 {
    try validatePair(lhs, rhs);
    var dot: f64 = 0;
    var lhs_norm: f64 = 0;
    var rhs_norm: f64 = 0;
    for (0..lhs.len / 4) |i| {
        const x = element(lhs, i);
        const y = element(rhs, i);
        dot += x * y;
        lhs_norm += x * x;
        rhs_norm += y * y;
    }
    if (lhs_norm == 0 or rhs_norm == 0) return error.ZeroVector;
    const result = 1 - dot / std.math.sqrt(lhs_norm * rhs_norm);
    if (!std.math.isFinite(result)) return error.NonFiniteResult;
    return result;
}

/// Negative inner-product (`<#>`) distance, so ascending order ranks the
/// greatest similarity first, matching pgvector's operator convention.
pub fn negativeInnerProduct(lhs: String, rhs: String) Error!f64 {
    try validatePair(lhs, rhs);
    var dot: f64 = 0;
    for (0..lhs.len / 4) |i| dot += element(lhs, i) * element(rhs, i);
    const result = -dot;
    if (!std.math.isFinite(result)) return error.NonFiniteResult;
    return result;
}

fn validatePair(lhs: String, rhs: String) Error!void {
    if (lhs.len % 4 != 0 or rhs.len % 4 != 0) return error.InvalidVector;
    if (lhs.len != rhs.len) return error.DimensionMismatch;
    for (0..lhs.len / 4) |i| {
        if (!std.math.isFinite(element(lhs, i)) or
            !std.math.isFinite(element(rhs, i))) return error.InvalidVector;
    }
}

fn element(bytes: String, index: usize) f64 {
    const bits = std.mem.readInt(u32, bytes[index * 4 ..][0..4], .little);
    return @as(f32, @bitCast(bits));
}
