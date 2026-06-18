//! sql — the relational layer's physical row codec.
//!
//! value:  Type / Value / Row — runtime SQL types and typed cells.
//! schema: LogicalColumn / LogicalSchema / PhysicalSchema / compile — the
//!         logical catalog view and the compiled byte layout for a version.
//! row:    marshalRow / unmarshalProjectedRow — encode a row, and decode a
//!         projection of columns in O(1) each.

const std = @import("std");

pub const value = @import("sql/value.zig");
pub const schema = @import("sql/schema.zig");
pub const row = @import("sql/row.zig");

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
pub const unmarshalProjectedRow = row.unmarshalProjectedRow;

test {
    std.testing.refAllDecls(@This());
}
