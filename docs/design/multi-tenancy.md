---
title: Thousands of Tenants in one Process
description: One process, a custom coroutine system, and metered cpu ticks, memory and disk IOPS per query and per tenant.
sidebar:
  label: Multi-tenancy
  order: 1
---

*Status: Mechanisms are built. Cap enforcement mid-query in future release.*

Egres is a single process, using a custom designed coroutine system called
[zfiber][]. zfiber spawns as many threads as the number of logical cores, and
cooperatively schedules fibers across these threads. All work across the
database: connections, queries, WAL, compactions, disk and network I/O are run
using these fibers.

[zfiber]: https://github.com/manishrjain/egres/tree/main/zfiber

## Cpu Ticks and zfiber

zfiber is custom-built to the needs of a database. It is different from Go's
goroutines or Rust's Tokio in that it allows certain fibers to have higher
priority than others. For example, the fibers running Raft loops get higher
priority and are chosen over others if ready and waiting. zfiber also supports
dedicated lanes (dedicated threads) that fibers can't cross. So a very heavy
query workload would not spill into the threads serving the Raft fibers.

zfiber threads have local task queues and run task stealing so no thread sits
idle while another has a queue. While zfiber doesn't natively evict a running
fiber, functions in Egres run a `maybeYield` call which can be used to
optionally yield a fiber. zfiber tracks CPU ticks spent per fiber so it can tell
how much CPU a query took.

<figure class="fig">
  <img src="../figures/tenants-scheduler.svg" width="1000" height="588" alt="The Egres fiber scheduler: eight threads, tenant A parked at its cpu cap while everyone else keeps running, and the tenants on this process" loading="lazy" />
</figure>

## Direct I/O

Egres does direct I/O, bypassing the filesystem cache. It stores the blocks it
reads in an [S3-FIFO cache][s3-fifo] instead of relying on the page cache. All
storage I/O goes through a virtual filesystem API, which can meter the traffic
to a device and throttle disk IOPS as needed. Egres runs a quick calibration on
boot to figure out each device's bandwidth, or it can be fed that bandwidth via
command line flags. It also calculates the cost of a write as a factor of the
cost of a read, so it can charge reads and writes accordingly.

Any disk activity causes a fiber to park and wait for the I/O completion,
meanwhile other fibers can run on the threads. zfiber uses Mitchell Hashimoto's
[libxev][] which provides io_uring on Linux.

Disk operations are throttled based on priority (and tenancy in future).
Inspired by the [SILK paper][silk], disk operations carry one of 3 priorities:
P0 is for queries, P1 for L0 flushes, and P2 for L1+ compactions. Knowing the
full bandwidth of the disk, lower priorities get the bandwidth the higher ones
leave unused. Thus, background compactions don't interfere with queries, and
query latencies stay low even with many compactors running at the same time.
The same mechanisms could be applied to object storage, based on the number of
pending requests in addition to bandwidth.

[s3-fifo]: https://s3fifo.com/
[libxev]: https://github.com/mitchellh/libxev
[silk]: https://www.usenix.org/conference/atc19/presentation/balmau

## Memory

All memory is allocated via a custom allocator, a thin wrapper over jemalloc,
which Zig's allocator interface makes easy to do. A memory governor tracks live
memory against the total memory allocated to the process. Every 100ms it sizes
the block cache up and down to make room for queries and background tasks,
keeping the process under its specified memory cap.

As of this release, only the total memory used by the process is tracked.
Future releases will track memory per query and per tenant. An allocation could
then park the fiber for some duration, waiting for memory to become available,
or, if over the cap, fail with an error propagated back to the user instead of
an OOM kill.

## One Process: Thousands of Tenants

Given these mechanisms, Egres would be able to accurately meter and throttle the
CPU ticks spent, the memory used, and the disk IOPS per query, per tenant. That
allows thousands of tenants to share resources, without any noisy neighbor
problem, and without running a VM per tenant.

<figure class="fig">
  <img src="../figures/query-metering.svg" width="1000" height="432" alt="One query as a fiber: its cpu ticks, memory and disk iops are charged to it and checked against the tenant's caps; over a cap the fiber is parked and resumes as soon as budget allows" loading="lazy" />
</figure>

## MicroVMs

Beyond resource management, Neon and AlloyDB wrap each Postgres in a microVM for
two reasons, which don't directly apply to Egres.

The first reason is best phrased by [AlloyDB][alloydb-arch]: "shared allocations
mean shared fate". Postgres's memory cannot be capped from outside, so a cgroup
limit means the [OOM killer][oom-kill] takes the postmaster down and every
tenant with it. In Egres, running out of a resource is a function error, which
can be treated like every other error and returned all the way back to the user.
Tenants share the resources but do not share the fate.

The second is security. Postgres has no boundary inside the process: extensions
are C running with the engine's full power, and a memory-safety bug in that
code, reached through one tenant's input, reaches every tenant's data. A VM per
tenant puts a kernel between them.

Egres is one process too, so what it does instead is shrink the attack surface.
A client can send only SQL, a session is bound to one database at connection
time (auth coming in future release), and no tenant native code runs in the
process. Extensions will be compiled into the engine rather than loaded at
runtime, and user-defined functions, likely WebAssembly, will run in a sandbox
metered like any other fiber. That is the bet every multi-tenant web service
makes: a small surface instead of a kernel per customer.

<figure class="fig">
  <img src="../figures/tenants-one-process.svg" width="1000" height="400" alt="A VM per tenant versus one Egres process holding thousands of tenants" loading="lazy" />
</figure>

[alloydb-arch]: https://cloud.google.com/blog/products/databases/alloydbs-agentic-database-architecture
[oom-kill]: https://www.crunchydata.com/blog/deep-postgresql-thoughts-the-linux-assassin
