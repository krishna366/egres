# Zbuf Module

This module implements a small, FlatBuffers-style wire format and a Zig
build-time generator that emits zero-copy readers and forward-writing
writers from Zig schema definitions.

A schema is plain data: `schema.Table`, `schema.Packed` and
`schema.EnumDef` build `schema.Type` values, a field names its type, and
the generator is an ordinary program that walks those values and writes
text. A schema mistake is reported on stderr with its message, and the
run fails. The `records` list names the records to generate, tables
with a message id each and packed records without; enums are found by
their `pub const` declarations.

## Generator

- Generator entrypoint: `zbuf/gen.zig`
- Schema DSL: `zbuf/schema.zig`
- Example test schema: `zbuf/buffers.zig`

Generated code includes:

- `XReader` (reader)
- `XWriter` (writer)
- `addTail()` / `parseTail()` helpers
- `parseMessageFromTail()` which returns the correct root reader based on the tail

## Build Commands

Generate code for a schema file:

```
zig build zbuf -- path/to/buffers.zig
```

This writes `path/to/buffers.gen.zig` and prints the generated path.

Regenerate every discovered `*/buffers.gen.zig` at once:

```
zig build buffers
```

Run the tests. There is no zbuf-only step: `build.zig` auto-discovers every
`*_test.zig` in the repo and compiles them into a single binary.

```
zig build test
```

## Example Flow

```
var buf: std.ArrayList(u8) = .{};
var root = try gen.TableWriter.initWithBuf(alloc, &buf);
// set fields and finish
const table_finish = try root.finish();
const table_off = table_finish.offset;
try gen.addTail(alloc, &buf, gen.TableReader, table_off);

const msg = try gen.parseMessageFromTail(buf.items);
const view = switch (msg) {
    .Table => |v| v,
    else => unreachable,
};
```

## Table Layout

Each table begins with a fixed header:

- `u16 size_bytes` (total bytes of the fixed header, including this field)

Field slots start at offset 2, immediately after it. Note there is **no
version field** anywhere — not in the table, not in the tail. `size_bytes`
alone carries the compatibility information: a reader compares each field's
slot against it and treats anything past the end as absent (see "Table and
Field IDs" below).

### Table and Field IDs

Each table has a **table id** (set in the schema’s `records` list). It is used
in the message tail to identify the root table type, but is not stored inside
the table itself.

Each field has a **stable field id** (the `.id` in the schema). Field names
may be renamed or reordered in the source without changing the wire layout.
The generator sorts fields by `id` in ascending order to determine the slot
order in the fixed header. New fields may only be appended with new ids, and
readers use `size_bytes` to skip unknown slots.

Supported field encodings (slot sizes are `schema.slotSize`):

- Scalars (`u8` 1, `u16` 2, `u32` 4, `u64` 8) stored inline in the fixed
  header.
- Enums stored inline as a 1-byte tag.
- Bools take **no slot of their own** (size 0): each addresses one bit of
  another field's slot, named by `.field_id` and `.bit`. That backing field
  must exist and be wide enough — the generator rejects it otherwise, and
  rejects two bools sharing a bit (see `zbuf/negative_test.zig`).
- Bytes (`[]u8`) stored as `{ u32 off, u32 len }`.
- Arrays (`[]T`) stored as `{ u32 index_off, u32 count }`. For arrays of
  tables, `index_off` points to a contiguous `u32[count]` of child offsets;
  for arrays of scalars it points straight at the packed elements.
- Direct table fields (`T`) stored as `u32 off` pointing at the child table.
  A null table is encoded as `zbuf.NullOffset` (`0xFFFF_FFFF`).

All offsets are **relative to payload base**.

## Layout Overview

A message consists of a payload and an optional fixed-length tail. All table
offsets are **relative to the payload base** (the start of the payload), so
appending the tail does not change any internal offsets. The tail is optional;
it is only needed when the receiver does not already know the root table type.

The **root table always lives at payload offset 0** — the writers enforce this
in `finishWithTail`, and `parseRoot` relies on it (it strips the tail and inits
at base 0). That is why the tail carries no root offset.

Message tail (optional, `zbuf.message_tail_size` = 4 bytes at end of buffer):

- `u16 table_id` = unique id for the root table
- `u16 magic` = `zbuf.message_magic` = `0x425A` ("ZB" little-endian) — enough
  to discriminate zbuf bytes from other formats

Payload bytes and index arrays are padded to a 4-byte boundary
(`payload_alignment`), so small gaps between a table header and the data it
points at are expected.

### Example (Table) with Concrete Fields

Using the test schema in `zbuf/buffers.zig` (reproduced here — `Bool` fields
have no slot of their own, they address a bit inside their `field_id`'s slot):

```zig
pub const Cell = schema.Table(&.{
    .{ .id = 1, .name = "tag", .ty = schema.U32 },
    .{ .id = 2, .name = "data", .ty = schema.Bytes },
    .{ .id = 3, .name = "cell_flags", .ty = schema.U8 },
    .{ .name = "cell_hot", .ty = schema.Bool(.{ .field_id = 3, .bit = 0 }) },
});

pub const Lifecycle = schema.EnumDef(&.{
    .{ .name = "active", .tag = 0 },
    .{ .name = "draining", .tag = 1 },
    .{ .name = "bootstrapping", .tag = 2 },
    .{ .name = "retiring", .tag = 3 },
});

pub const Row = schema.Table(&.{
    .{ .id = 1, .name = "name", .ty = schema.Bytes },
    .{ .id = 2, .name = "value", .ty = schema.U64 },
    .{ .id = 3, .name = "cells", .ty = schema.Array(Cell) },
    .{ .id = 4, .name = "row_cell", .ty = Cell },
    .{ .id = 5, .name = "flags", .ty = schema.U64 },
    .{ .name = "is_pinned", .ty = schema.Bool(.{ .field_id = 5, .bit = 0 }) },
    .{ .name = "is_deleted", .ty = schema.Bool(.{ .field_id = 5, .bit = 1 }) },
    .{ .id = 6, .name = "lifecycle", .ty = Lifecycle },
    .{ .id = 7, .name = "u32_values", .ty = schema.Array(schema.U32) },
    .{ .id = 8, .name = "u64_values", .ty = schema.Array(schema.U64) },
});

pub const Table = schema.Table(&.{
    .{ .id = 1, .name = "rows", .ty = schema.Array(Row) },
    .{ .id = 2, .name = "payload", .ty = schema.Bytes },
    .{ .id = 3, .name = "count", .ty = schema.U32 },
    .{ .id = 4, .name = "table_row", .ty = Row },
});
```

Slot sizes come from `schema.slotSize`: `u8` 1, `u16` 2, `u32` 4, `u64` 8,
`Bytes` 8 (`off`+`len`), `Array` 8 (`off`+`count`), a direct table 4 (`off`),
an enum 1, and a bool 0. Header size is 2 (`size_bytes`) plus the slots, in
ascending field-id order — this is `<Type>Reader.SIZE_BYTES` in the generated
code:

- `Cell` header = **15** bytes — 2 + tag 4 + data 8 + cell_flags 1
- `Row` header = **55** bytes — 2 + name 8 + value 8 + cells 8 + row_cell 4 +
  flags 8 + lifecycle 1 + u32_values 8 + u64_values 8
- `Table` header = **26** bytes — 2 + rows 8 + payload 8 + count 4 +
  table_row 4

Now a concrete payload: one `Cell{tag=7, data="DATA"}`, one
`Row{name="NAME", value=42, cells=[cell], row_cell=cell}`, and a root
`Table{rows=[row], payload="PAYL", count=1, table_row=row}`.

The **root table is written first, at offset 0**; children follow. Byte blobs
and index arrays are 4-byte aligned, which is where the small gaps come from
(e.g. the `Cell` header ends at `0x0029` but `"DATA"` starts at `0x002C`).

```
0x0000  Table header (26 = 0x1A bytes)   <- root, always at 0
0x001A  Cell header (15 = 0x0F bytes)
0x002C  Cell.data bytes "DATA"
0x0030  Row header (55 = 0x37 bytes)
0x0068  Row.name bytes "NAME"
0x006C  Row.cells index array (u32[1] = 0x001A)
0x0070  Table.rows index array (u32[1] = 0x0030)
0x0074  Table.payload bytes "PAYL"
0x0078  tail (4 bytes)
0x007C  end (total 124 bytes)
```

Concrete header contents (little-endian):

```
Table @0x0000                     Cell @0x001A
  u16 size_bytes    = 0x001A        u16 size_bytes  = 0x000F
  u32 rows_off      = 0x0070        u32 tag         = 7
  u32 rows_count    = 1             u32 data_off    = 0x002C
  u32 payload_off   = 0x0074        u32 data_len    = 4
  u32 payload_len   = 4             u8  cell_flags  = 0
  u32 count         = 1
  u32 table_row_off = 0x0030

Row @0x0030
  u16 size_bytes        = 0x0037    u64 flags             = 0
  u32 name_off          = 0x0068    u8  lifecycle         = 0 (active)
  u32 name_len          = 4         u32 u32_values_off    = 0
  u64 value             = 42        u32 u32_values_count  = 0
  u32 cells_off         = 0x006C    u32 u64_values_off    = 0
  u32 cells_count       = 1         u32 u64_values_count  = 0
  u32 row_cell_off      = 0x001A
```

If you choose to add a tail, it is appended after the payload. For the example
above that is the last 4 bytes, `03 00 5A 42`:

```
tail (optional, 4 bytes) @0x0078
┌─────────────────────────────────┐
│ table_id = 3 (Table) │ "ZB"     │
│ 03 00                │ 5A 42    │
└─────────────────────────────────┘
```

There is no root offset in the tail: the root is always at payload offset 0.

## Packed Records

`schema.Packed` is a second record kind for formats where every byte
counts and random access is by computed offset, such as an SST block.
It takes the same field types as `Table`, encoded inline with no
header and no padding, in ascending field-id order:

- Scalars and enums exactly as wide as their value.
- `Bytes` as `[len][bytes]` and `Array` as `[count][elements]`. The
  prefix is as wide as the field's `.len` says: `.u8`, `.u16` or `.u32`
  (the default). A `Bytes` with `.len = .rest` has no prefix and runs to
  the end of the record; it must be the last field.
- A nested `Packed` inline, which must be fixed size. `Array` elements
  are fixed-size (scalars, enums, a fixed `Packed`) back to back, or
  variable-size `Packed` records behind a table of u32 end offsets, one
  per record: record `i` lies between ends `i - 1` and `i`, and the last
  end is the region's length, so the table checks itself.
- No bools and no `Table` fields; a `Packed` cannot be a `Table` field.

A packed record has no `size_bytes`, so nothing marks a field absent:
its layout changes only behind a version gate the container provides.
Packed types sit in `records` beside the tables, without a message id:
they have no tail and no place in `parseMessageWithTail`.

```zig
pub const VersionHdr = schema.Packed(&.{
    .{ .id = 1, .name = "ts", .ty = schema.U64 },
    .{ .id = 2, .name = "meta", .ty = schema.U8 },
    .{ .id = 3, .name = "off", .ty = schema.U32 },
});                                                   // SIZE = 13

pub const Entry = schema.Packed(&.{
    .{ .id = 1, .name = "key_overlap", .ty = schema.U16 },
    .{ .id = 2, .name = "key_diff", .ty = schema.Bytes, .len = .u16 },
    .{ .id = 3, .name = "versions", .ty = schema.Array(VersionHdr) },
    .{ .id = 4, .name = "values", .ty = schema.Bytes, .len = .rest },
});
```

Generated code: `{Name}Reader.init(buf)` takes the record's own slice
and nothing more. A packed record is trusted: its container checks it
whole (an SST block is checksummed before it is decoded), so a field is
read where the layout puts it when it is asked for, at a constant
offset (`{field}_off`, public) while every field before it is
fixed-size and through a small offset function after the first
variable one. Getters cannot fail
(`keyDiff()`, `values()`, `size()`); a fixed record exports `SIZE`. An
array of records comes as a view located once, `versions()` (a
`zbuf.ArrayFixed`) or `entries()` (a `zbuf.ArrayVar`, records of
varying size), with `len()` and a checked `at(i)`, a bounds test and
a slice. An array of scalars has `weightsLen()` and `weightsAt(i)`.
`{Name}Writer.initWithBuf(alloc, &list)` appends the record a field at
a time, in schema order since the layout is positional: `setKeyDiff(b)`
for a field written whole, `versionCount(n)` then `addVersion(v)` per
element for an array (`addEntryBytes(b)` takes an already-encoded
variable-size record), `addValues(b)` any number of times for the
trailing rest bytes. `set(struct)` writes every field from one value
through the same methods, and `finish()` marks the record complete. In
safe builds a `zbuf.FieldCursor` asserts the order, that an array got
the elements its count promised, and at `finish` that no field was
skipped.
`{Name}Struct` is the owned form, with `toStructValue`, `deinit` and
`marshalTo`.
