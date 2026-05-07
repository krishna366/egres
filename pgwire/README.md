# pgwire

Postgres v3 wire-protocol server. Pure-logic protocol layer + zio-backed I/O
driver. The protocol layer (`proto.zig` / `conn_state.zig` / `parser.zig` /
`binary.zig`) is socket-free; only `driver.zig` touches `zio.net.Stream`.

## Layers

| File | Role |
|------|------|
| `proto.zig` | Frame encode/decode + body parsers |
| `conn_state.zig` | `ConnState` state machine |
| `parser.zig` | libpg_query bridge (`@cImport`) |
| `binary.zig` | Encoders/decoders for the 6 primitive binary types |
| `driver.zig` | `serve()` — bridges a `zio.net.Stream` to a `ConnState` |

Tests live in `*_test.zig` siblings; auto-discovered by `build.zig`.

## Connection lifecycle

One `ConnState` per accepted TCP socket. Embedder spawns a fiber per
connection and calls `pgwire.serve(alloc, &stream, &handler, opts)`:

```
serve():
  r     := stream.reader(read_buf)
  w     := stream.writer(write_buf)
  state := ConnState.init(alloc, &r.interface, &w.interface, cfg)
  defer state.deinit()
  loop while state.phase != .closed:
      handleEvent(&state, handler)      # blocks pulling from r,
                                        # may queue frames into w
      w.interface.flush()               # logical-boundary flush
  w.interface.flush()                   # final trailing flush
```

`nextEvent` blocks on the Reader for input; `sendXxx` methods queue output
frames through the Writer (auto-flushes when the write buffer fills, giving
TCP backpressure during large result sets without holding the result in
memory).

## Events surfaced to the handler

| Event | Driver action |
|-------|---------------|
| `startup_complete` | State machine has queued AuthOk + ParameterStatus + BackendKeyData + RFQ. |
| `query` | `handler.onQuery(state, sql)`, then `state.endCommand()` (queues RFQ). |
| `execute` | `handler.onExecute(state, ExecuteCtx)`. RFQ is **not** auto-emitted — arrives on the next `Sync`. |
| `describe` | `handler.onDescribe(state, DescribeCtx)`. ParameterDescription already emitted for `stmt` kind. |
| `sync` | RFQ already queued; driver flushes. |
| `terminate` | Frontend `X` frame, clean EOF, or fatal error. Outer loop exits. |

`onExecute` and `onDescribe` are detected via `@hasDecl`; simple-query-only
embedders can omit them and the driver falls back to `ErrorResponse` /
`NoData`.

## Features

### Implemented

- [x] Simple query (`Q` frame)
- [x] Extended query (`Parse` / `Bind` / `Describe` / `Execute` / `Close` /
      `Sync` / `Flush`) with per-connection statement and portal registries
- [x] Spec-correct extended-batch error recovery — drop until next `Sync`
      (§4.1.4)
- [x] Binary format for 6 primitive types: `bool`, `int2`, `int4`, `int8`,
      `float4`, `float8`
- [x] Startup handshake — AuthenticationOk + 11 ParameterStatus
      (incl. `standard_conforming_strings=on`) + BackendKeyData +
      ReadyForQuery
- [x] Format-code validation — 0 (text) and 1 (binary) only
- [x] SSL / GSSEnc decline (`'N'` byte; client retries plain)
- [x] EmptyQueryResponse for whitespace/comment-only queries
- [x] Length-framed message dispatch with partial-frame handling, up to a
      16 MiB per-frame ceiling
- [ ] SCRAM-SHA-256 / MD5 authentication — today only AuthenticationOk
- [ ] TLS — SSLRequest declined; no post-`S` handshake
- [ ] CancelRequest routing — sentinel accepted, no executor-cancel plumbing
- [ ] Server-side type inference at `Parse` — today we echo client-supplied
      OIDs, so a `Parse` with empty `param_oids` produces an empty
      `ParameterDescription`
- [ ] Binary format for non-primitive types (timestamps, numeric, UUIDs,
      arrays)
- [ ] Multi-statement Query — per-statement RowDescription / DataRow* /
      CommandComplete with a single trailing RFQ
- [ ] COPY protocol (`CopyInResponse` / `CopyOutResponse` / `CopyData` /
      `CopyDone` / `CopyFail`)
- [ ] LISTEN / NOTIFY — server-side `NotificationResponse`
- [ ] NoticeResponse — non-fatal warnings
- [ ] `pg_catalog` backend (`pg_class`, `pg_attribute`, etc. — what psql
      `\d` queries)
- [ ] Full `pg_type` OID table — today 7 primitive entries in `types.zig`

## Real-client compatibility

Drove `pgwire-shakeout` (a stub server that returns one fake row for any
query) against:

| Client | Mode | Result |
|--------|------|--------|
| `psql -c "SELECT 1"` | simple-query | ✅ |
| `psql -c "BAD SYNTAX"` | simple-query | ✅ ErrorResponse renders cleanly |
| `psql -c ""` (empty) | simple-query | ✅ EmptyQueryResponse honored |
| `psql -c "SELECT 1; SELECT 2;"` | simple-query | ✅ psql splits client-side |
| `psql -c "\d"` (metacommand) | simple-query | ✅ pg_catalog SQL flows through; executor would NXDOMAIN |
| `psql` interactive | simple-query | ✅ |
| `psycopg2`, `autocommit=True` | simple-query | ✅ |
| `psycopg2`, default | simple-query | ⚠️ stub returns rows where none should be (handler-contract gap) |
| `pgx`, no params | extended query | ✅ |
| `pgx`, with params | extended query | ⚠️ blocked on type inference |
| `pgx ?default_query_exec_mode=simple_protocol` | simple-query | ✅ |

## Shakeout methodology

```bash
zig build pgwire-shakeout
./zig-out/bin/pgwire-shakeout              # listens 127.0.0.1:5433
psql -h 127.0.0.1 -p 5433 -U u -d db -c "SELECT 1"
```

Stub handler echoes the libpg_query statement kind back as a single text
column named `kind`. Enough to drive the protocol path through to
ReadyForQuery without committing to an executor.
