//! sql — the relational layer's physical row codec.
//!
//! value:  Type / Value / Row — runtime SQL types and typed cells.
//! schema: LogicalColumn / LogicalSchema / PhysicalSchema / compile — the
//!         logical catalog view and the compiled byte layout for a version.
//! row:    marshalRow / unmarshalRow — encode a row, and decode a
//!         projection of columns in O(1) each.
//! catalog: table 0 — the on-disk store of every table's logical schema,
//!         plus its own binary-compiled bootstrap schema.

const std = @import("std");

pub const value = @import("sql/value.zig");
pub const schema = @import("sql/schema.zig");
pub const row = @import("sql/row.zig");
pub const catalog = @import("sql/catalog.zig");

pub const Type = value.Type;
pub const Value = value.Value;
pub const Interval = value.Interval;
pub const Row = value.Row;

pub const LogicalColumn = schema.LogicalColumn;
pub const LogicalSchema = schema.LogicalSchema;
pub const ColumnLayout = schema.ColumnLayout;
pub const PhysicalSchema = schema.PhysicalSchema;
pub const compile = schema.compile;

pub const marshalRow = row.marshalRow;
pub const unmarshalRow = row.unmarshalRow;

test {
    std.testing.refAllDecls(@This());
}
