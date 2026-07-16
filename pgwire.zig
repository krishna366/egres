//! pgwire — Postgres v3 wire protocol surface for egres.
//!
//! Layered structure:
//!   - proto / conn_state / parser / types: pure logic, no I/O. Do
//!     not import the fiber runtime in any of them.
//!   - driver: the only place sockets / fiber / syscalls appear.
//!
//! See pgwire/README.md for usage.

pub const proto = @import("pgwire/proto.zig");
pub const conn_state = @import("pgwire/conn_state.zig");
pub const parser_mod = @import("pgwire/parser.zig");
pub const driver_mod = @import("pgwire/driver.zig");
pub const types = @import("pgwire/types.zig");
pub const binary = @import("pgwire/binary.zig");

const types_mod = types;

// Driver — the one I/O entry point most callers want.
pub const serve = driver_mod.serve;
pub const Options = driver_mod.Options;
pub const Stream = driver_mod.Stream;

// State machine — exposed for callers that want to drive pgwire from
// somewhere other than fiber.net.Stream (e.g. tests, alternate runtimes).
pub const ConnState = conn_state.ConnState;
pub const Config = conn_state.Config;
pub const Event = conn_state.Event;
pub const Phase = conn_state.Phase;
pub const ExecuteCtx = conn_state.ExecuteCtx;
pub const DescribeCtx = conn_state.DescribeCtx;
pub const DescribeKind = conn_state.DescribeKind;

// Parser — libpg_query JSON wrapper.
pub const parse = parser_mod.parse;
pub const ParseTree = parser_mod.ParseTree;
pub const ParseError = parser_mod.ParseError;
pub const ErrorInfo = parser_mod.ErrorInfo;
pub const parserShutdown = parser_mod.shutdown;

// Shared protocol types.
pub const FieldDescription = types_mod.FieldDescription;
pub const Oid = types_mod.Oid;
pub const oid = types_mod.oid;
pub const TxStatus = types_mod.TxStatus;
pub const sqlstate = types_mod.sqlstate;

// Re-export proto.* for callers that want to talk frames directly
// (test fixtures, alternative drivers).
pub const DataValue = proto.DataValue;
pub const ErrorField = proto.ErrorField;
pub const BackendTag = proto.BackendTag;
pub const FrontendTag = proto.FrontendTag;
pub const decodeMessage = proto.decodeMessage;
pub const decodeStartup = proto.decodeStartup;

test {
    const std = @import("std");
    std.testing.refAllDecls(@This());
}
