---
title: Introduction
description: What Egres is, and where to start reading.
---

Egres is a Postgres-compatible database for heavy, multi-tenant workloads.
Written in Zig, Egres isn't just a Postgres rewrite: it is a ground-up, first
principles redesign.

Egres:

- Speaks the Postgres protocol. psql, drivers and ORM connect unchanged.
- Thousands of tenants on one process. Queries run on a custom coroutine system,
  using direct I/Os and measured RAM, with metered resource usage per query and
  per tenant.
- Uses a new storage engine, called zdb, which is an LSM forest (tree of LSM
  trees) for storage, with block compression, checksums,
  multi-version concurrency control and garbage collection.
- Local disk or remote S3 storage. With S3, Egres servers are stateless,
  utilizing (an optional) local disk cache.
- Raft based replication, with read-your-write guarantees.
- Instant database and table branching, without doing copy-on-write.
- Open Source under Apache 2.0.

## Start here

- The [Quickstart](quickstart.md) builds Egres, starts a server and connects with `psql`.
- [Postgres compatibility](postgres-compatibility.md) lists exactly which SQL runs today and what each rejection looks like.
- The design pages cover [multi-tenancy](design/multi-tenancy.md), [storage](design/storage.md), [replication](design/replication.md), [garbage collection](design/no-vacuum.md), [indexes](design/indexes.md), [branching](design/branching.md) and [transactions](design/transactions.md).
- [Benchmarks](benchmarks.md) has the numbers against Postgres 18.
