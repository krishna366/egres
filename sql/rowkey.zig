//! egid ↔ row-key encoding. A SQL data table keys each row by its egid — a
//! per-table, monotonically increasing u64 — stored as an 8-byte big-endian
//! primary key, so byte order matches numeric order: scans walk rows in
//! insertion order and the largest key is the largest egid. zdb sees only
//! opaque key bytes; this is where the SQL layer gives them meaning.

const std = @import("std");

/// The 8-byte big-endian primary key for a row with this egid.
pub fn encodeEgid(egid: u64) [8]u8 {
    var k: [8]u8 = undefined;
    std.mem.writeInt(u64, &k, egid, .big);
    return k;
}

/// The egid a primary key encodes, or null if it isn't egid-shaped (8 bytes) —
/// e.g. a raw-KV or catalog key.
pub fn decodeEgid(primary: []const u8) ?u64 {
    if (primary.len != 8) return null;
    return std.mem.readInt(u64, primary[0..8], .big);
}
