// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Embedding-provider configuration and statement-scoped result validation.

const std = @import("std");
const utils = @import("utils");
const _value = @import("value.zig");

pub const String = utils.String;

pub const Protocol = enum { openai_compatible };

pub const Provider = struct {
    name: String,
    endpoint: String,
    model: String,
    dimensions: u32,
    credential_ref: String,
    timeout_ms: u32 = 5000,
    max_concurrency: u16 = 16,

    pub fn validate(self: Provider) !void {
        if (self.name.len == 0 or self.model.len == 0 or self.credential_ref.len == 0)
            return error.InvalidProvider;
        if (self.dimensions == 0 or self.dimensions > _value.Type.max_vector_dimensions)
            return error.InvalidDimensions;
        if (!std.mem.startsWith(u8, self.endpoint, "https://") and
            !std.mem.startsWith(u8, self.endpoint, "http://"))
            return error.InvalidEndpoint;
        if (self.timeout_ms == 0 or self.max_concurrency == 0)
            return error.InvalidLimits;
    }
};

/// Per-server registry. Its storage boundary is deliberately explicit: the
/// catalog DDL layer will persist and hydrate the same Provider records.
pub const Registry = struct {
    alloc: std.mem.Allocator,
    providers: std.StringHashMap(Provider),

    pub fn init(alloc: std.mem.Allocator) Registry {
        return .{ .alloc = alloc, .providers = std.StringHashMap(Provider).init(alloc) };
    }

    pub fn deinit(self: *Registry) void {
        var it = self.providers.iterator();
        while (it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            self.alloc.free(entry.value_ptr.name);
            self.alloc.free(entry.value_ptr.endpoint);
            self.alloc.free(entry.value_ptr.model);
            self.alloc.free(entry.value_ptr.credential_ref);
        }
        self.providers.deinit();
    }

    pub fn add(self: *Registry, provider: Provider) !void {
        try provider.validate();
        const key = try self.alloc.dupe(u8, provider.name);
        errdefer self.alloc.free(key);
        const owned = try copyProvider(self.alloc, provider);
        errdefer freeProvider(self.alloc, owned);
        const result = try self.providers.getOrPut(key);
        if (result.found_existing) return error.DuplicateProvider;
        result.key_ptr.* = key;
        result.value_ptr.* = owned;
    }

    pub fn get(self: *const Registry, name: String) ?Provider {
        return self.providers.get(name);
    }
};

/// Validate a provider response already decoded into float values, then encode
/// it in the vector column's canonical float32 representation.
pub fn validateEmbedding(
    alloc: std.mem.Allocator,
    provider: Provider,
    values: []const f64,
) !_value.Value {
    if (values.len != provider.dimensions) return error.DimensionMismatch;
    var bytes: std.ArrayList(u8) = .empty;
    errdefer bytes.deinit(alloc);
    try bytes.ensureTotalCapacityPrecise(alloc, values.len * 4);
    for (values) |value| {
        if (!std.math.isFinite(value)) return error.NonFiniteEmbedding;
        const narrow: f32 = @floatCast(value);
        if (!std.math.isFinite(narrow)) return error.NonFiniteEmbedding;
        const start = bytes.items.len;
        bytes.items.len += 4;
        std.mem.writeInt(u32, bytes.items[start..][0..4], @bitCast(narrow), .little);
    }
    return .{ .vector = try bytes.toOwnedSlice(alloc) };
}

fn copyProvider(alloc: std.mem.Allocator, p: Provider) !Provider {
    return .{ .name = try alloc.dupe(u8, p.name), .endpoint = try alloc.dupe(u8, p.endpoint), .model = try alloc.dupe(u8, p.model), .dimensions = p.dimensions, .credential_ref = try alloc.dupe(u8, p.credential_ref), .timeout_ms = p.timeout_ms, .max_concurrency = p.max_concurrency };
}

fn freeProvider(alloc: std.mem.Allocator, p: Provider) void {
    alloc.free(p.name);
    alloc.free(p.endpoint);
    alloc.free(p.model);
    alloc.free(p.credential_ref);
}
