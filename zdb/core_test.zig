// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const zdb = @import("zdb");

test "config validate rejects single compactor and in-memory dir" {
    const bad_compactors = zdb.config.Config{
        .num_compactors = 1,
    };
    try std.testing.expectError(
        zdb.config.Error.InvalidNumCompactors,
        bad_compactors.validate(),
    );

    const bad_in_memory_dir = zdb.config.Config{
        .in_memory = true,
        .dir = "/tmp/db",
    };
    try std.testing.expectError(
        zdb.config.Error.InvalidDirForInMemory,
        bad_in_memory_dir.validate(),
    );
}
