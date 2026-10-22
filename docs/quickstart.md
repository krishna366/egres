---
title: Quickstart
description: Build Egres, start a server, connect with the psql you already have, and branch your first database.
---

Build Egres, start a server, connect with the `psql` you already have, and branch your first database. About five minutes, plus the build.

:::note
Egres speaks the PostgreSQL wire protocol. Anything that connects to Postgres — `psql`, your ORM, your drivers — connects to Egres unchanged.
:::

## Build

Egres is one binary. It needs [Zig 0.16.0](https://ziglang.org/download/) and a C compiler for the vendored parser; everything else is in the repository.

```sh
$ git clone https://github.com/manishrjain/egres
$ cd egres
$ zig build -Doptimize=ReleaseFast
$ zig-out/bin/egres --help
```

Prebuilt binaries come with the first release.

## Start the server

Egres keeps two directories: the store, where every SSTable lives, and the write-ahead log. A single node needs no flags: both go under `egres-data` in the working directory, created if they do not exist, and `--store dir_path=` and `--wal dir=` move them.

```sh
$ egres
2026-10-22T14:05:31.123456Z INFO  egres: starting egres id=1 cluster=1 raft=127.0.0.1:4210 admin=127.0.0.1:5210 sql=127.0.0.1:3210 wal=egres-data/wal tick=20ms seeds=0
2026-10-22T14:05:31.140211Z INFO  sql: sql listener started on port 3210 (pgwire)
```

Postgres clients connect on port `3210`. The admin port is `5210` and the Raft port is `4210`; `--listen` moves all three. Every flag is `--group key=value`, and `egres --help` lists them with their defaults.

## Connect

Use any Postgres client. With `psql`, connect to the database `egres`, which exists from the first boot:

```sh
$ psql -h localhost -p 3210 -d egres
psql (18.6, server 13.0)
Type "help" for help.

egres=>
```

There is no password yet, and the server version it reports is the protocol version it speaks.

## Create a database and a table

A database of your own, then a table in it. Each table becomes its own LSM tree in the store, so tables never share files.

```sql
CREATE DATABASE shop;
\c shop
CREATE TABLE orders (
  id        bigint PRIMARY KEY,
  customer  text,
  total     double precision,
  placed_at timestamptz
);
INSERT INTO orders VALUES (1, 'ada', 42.0, CURRENT_TIMESTAMP);
SELECT * FROM orders;
```

```
 id | customer | total |           placed_at
----+----------+-------+-------------------------------
  1 | ada      |    42 | 2026-10-14 16:52:44.25602+00
```

`CURRENT_TIMESTAMP` is the transaction's start time. Column defaults and `numeric` are not supported yet; [Postgres compatibility](postgres-compatibility.md) has the full list of what is.

## Branch the database

A branch is a new database — or a new table — that re-references every SSTable of its parent. It is created instantly, regardless of how much data the parent holds, and there is no copy-on-write: writes to either side go into that side's own memtable and new SSTables, so a shared file is never duplicated.

```sql
CREATE DATABASE shop_staging TEMPLATE shop;
```

Connect to the branch and write to it. The parent does not see the write:

```sql
\c shop_staging
INSERT INTO orders VALUES (2, 'grace', 7.5, CURRENT_TIMESTAMP);
SELECT count(*) FROM orders;   -- 2
\c shop
SELECT count(*) FROM orders;   -- 1
```

One table branches the same way, indexes included:

```sql
CREATE TABLE orders_experiment AS TABLE orders;
```

Run a migration against the branch, and drop it when you are done:

```sql
DROP DATABASE shop_staging;
DROP TABLE orders_experiment;
```

See [Branching](design/branching.md) for how branches diverge and are compacted.

## Kill it

Every commit is in the write-ahead log before the client hears back. Kill the server without warning and start it again:

```sh
$ kill -9 $(pgrep -x egres)
$ egres --raft id=1 --store dir_path=./egres-data --wal dir=./egres-wal
```

```sql
SELECT * FROM orders;
```

The row is there. `Ctrl-C` is the clean way to stop: the server drains the statements in flight, then exits.

## Next steps

Read [Storage](design/storage.md) to see how zdb lays data out on disk, or the [Benchmarks](benchmarks.md) for the numbers against Postgres 18.
