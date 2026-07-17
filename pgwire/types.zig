//! Shared pgwire types and constants. No I/O.
//!
//! The Postgres wire protocol is document-of-record; OIDs come from
//! `src/include/catalog/pg_type.dat` in the upstream source. We only
//! enumerate the OIDs we currently emit. Adding new types means adding
//! to this table.

const std = @import("std");

pub const Oid = u32;

/// Subset of pg_type OIDs we emit. RowDescription always advertises
/// text format (format code 0) regardless of what `pgwire/binary.zig`
/// can actually encode.
/// TODO: publish per-column binary format codes in RowDescription
/// when the handler opts in.
pub const oid = struct {
    pub const text: Oid = 25;
    pub const int2: Oid = 21;
    pub const int4: Oid = 23;
    pub const int8: Oid = 20;
    pub const bool_: Oid = 16;
    pub const float4: Oid = 700;
    pub const float8: Oid = 701;
    pub const numeric: Oid = 1700;
    pub const unknown: Oid = 705;
};

/// One column descriptor for a result set. `format_code = 0` means text.
/// `type_modifier = -1` means "no modifier" (e.g. for varchar(N), N is
/// encoded here; we don't carry varchars yet).
pub const FieldDescription = struct {
    name: []const u8,
    type_oid: Oid = oid.text,
    type_size: i16 = -1,
    type_modifier: i32 = -1,
    format_code: i16 = 0,
    /// Optional table OID + attribute number; 0/0 means "not from a
    /// table" (literal/expression). Defaulted to 0/0 because we have
    /// no catalog/query plan to derive real values from.
    /// TODO: derive table_oid + column_attr from the executor's plan
    /// once one exists.
    table_oid: Oid = 0,
    column_attr: i16 = 0,
};

/// SQLSTATE codes for ErrorResponse 'C' fields (like `oid` above, a
/// namespace so one export covers them all). 'XX000' is the catch-all
/// "internal_error" — used for everything without a specific code yet.
/// TODO: mirror Postgres' SQLSTATE taxonomy and emit the correct code
/// per error class (constraint violation, type mismatch, etc.).
pub const sqlstate = struct {
    pub const internal_error: [5]u8 = .{ 'X', 'X', '0', '0', '0' };
    pub const syntax_error: [5]u8 = .{ '4', '2', '6', '0', '1' };
    pub const duplicate_table: [5]u8 = .{ '4', '2', 'P', '0', '7' };
    pub const undefined_table: [5]u8 = .{ '4', '2', 'P', '0', '1' };
    pub const undefined_column: [5]u8 = .{ '4', '2', '7', '0', '3' };
    pub const invalid_catalog_name: [5]u8 = .{ '3', 'D', '0', '0', '0' };
    pub const feature_not_supported: [5]u8 = .{ '0', 'A', '0', '0', '0' };
    pub const invalid_text_representation: [5]u8 = .{ '2', '2', 'P', '0', '2' };
    pub const unique_violation: [5]u8 = .{ '2', '3', '5', '0', '5' };
};

/// Severity strings used in ErrorResponse 'S' / 'V' fields.
pub const severity_error = "ERROR";

/// Postgres protocol version 3.0, big-endian on the wire.
pub const protocol_version_3: u32 = (3 << 16) | 0;

/// SSLRequest / GSSEncRequest sentinel codes that some clients send
/// before the real Startup. We sniff for these so we can answer 'N'
/// (decline) and let the client retry without TLS.
pub const ssl_request_code: u32 = (1234 << 16) | 5679;
pub const gss_enc_request_code: u32 = (1234 << 16) | 5680;
pub const cancel_request_code: u32 = (1234 << 16) | 5678;

/// Status byte values for ReadyForQuery.
pub const TxStatus = enum(u8) {
    idle = 'I',
    in_tx = 'T',
    failed_tx = 'E',
};

test {
    std.testing.refAllDecls(@This());
}
