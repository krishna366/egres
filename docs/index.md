---
title: Introduction
description: What Egres is, and where to start reading.
---

Egres is a Postgres-compatible database for heavy, multi-tenant workloads.
Written in Zig, Egres isn't just a Postgres rewrite: it is a ground-up, first
principles redesign that speaks Postgres. Apart from [libpg_query][], it uses no
Postgres code.

[libpg_query]: https://github.com/pganalyze/libpg_query

Egres:

- Speaks the Postgres protocol. psql, drivers and ORM connect unchanged.
- Can run thousands of tenants on one process. Queries run on a custom coroutine
  (fiber) system, with CPU ticks, memory and disk IOPS metered per query and
  per tenant.
- Uses a new storage engine, called zdb, which is an LSM forest (tree of LSM
  trees) for storage, with block compression, checksums, multi-version
  concurrency control and garbage collection.
- Runs on local disk or object storage. With S3, a node keeps only its WAL and
  an optional NVMe disk cache locally.
- Provides Raft based replication, with read-your-write guarantees across any
  replica.
- Provides instant database and table branching with zero copy and without doing
  copy-on-write.
- Open Source under Apache 2.0.

This release `v0.2610.x` runs as a single node. The multi-node code is in but
not enabled yet; that would land in the next release. Most of the work in this
version went into storage, replication, resource metering and fiber based query
execution. The [query features](postgres-compatibility.md) are basic for now,
equality-only and no joins, and will grow with each release.

## Start here

- The [Quickstart](quickstart.md) installs Egres, starts a server and connects with `psql`.
- [Postgres compatibility](postgres-compatibility.md) lists exactly which SQL runs today and what each rejection looks like.
- The design pages cover [multi-tenancy](design/multi-tenancy.md), [storage](design/storage.md), [replication](design/replication.md), [garbage collection](design/no-vacuum.md), [indexes](design/indexes.md), [branching](design/branching.md) and [transactions](design/transactions.md).
- [Benchmarks](benchmarks.md) has the numbers against Postgres 18.
