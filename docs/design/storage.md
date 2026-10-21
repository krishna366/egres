---
title: Half the Disk, local or remote
description: zdb, an LSM forest with shared memtables, tree splits, block compression and checksums, on local disk or S3.
sidebar:
  label: Storage
  order: 2
---

*Status: available in the current release.*

## ZDB: LSM Forest

Egres runs on top of a new storage engine called zdb, inspired by
[Badger][]. While it bootstrapped as a port from Go to Zig, it went way beyond
Badger. Instead of being an LSM tree, zdb is an LSM tree of trees, aka an LSM
forest, integrates heavily with raft consensus and uses raft write-ahead log
instead of having its own.

[Badger]: https://github.com/dgraph-io/badger

Instead of being a single LSM tree, zdb follows a hierarchy. It has namespaces
(1:1 mapped to Postgres' databases). Each namespace owns many tables (1:1 mapped
to sql tables). Each table has one or more LSM trees. Each sql table's index
maps to a table in zdb. The benefit of having trees per table is it allows zdb
to instantly drop a namespace, table or an index, or instantly branch a table or
namespace (see [branching](branching.md)).

<figure class="fig">
  <img src="../figures/zdb-forest.svg" width="1000" height="360" alt="The zdb forest: writes fill the shared memtables at the top, a full memtable is frozen and flushed as one L0 file into each tree, and each tree then compacts its levels on its own" loading="lazy" />
</figure>

## Shared Memtables

As you can see, an Egres instance which has hundreds of tables would have
hundreds of LSM trees. A typical LSM tree would have in-memory tables, called
memtables, and having memtables per tree would cause excessive memory usage. To
avoid that, zdb shares the same memtables across all namespaces, with keys
prefixed with ns and table id. SSTables are owned by the tree and therefore the
ns and table id can be inferred. So, SSTables don't store that prefix. The
SSTable iterator sets the (ns, table id) prefix to the keys it outputs during
iteration, thus allowing accurate key comparisons across tables and namespaces.

## Tree Splits

Each table starts with one LSM tree, but as tree size grows (> 10 GiB), zdb can
split it in half to keep the compactions running predictably. Because each
tree's size is bounded, zdb only uses 4 levels (Level 0 to Level 3) per tree.
Thus, each table can own many trees. Both the size and levels per tree are
configurable.

Tree splits don't cause any data movement. zdb finds a median key which would
split the sizes as equally as possible based on the files in the last level, and
assigns SSTable files to each side. The files which overlap are assigned to
both. Later compactions only iterate over each tree's assigned key range. Once
split, trees can run compactions independently and concurrently. Moreover, a
tree whose key range doesn't get updates can avoid compactions entirely.

<figure class="fig">
  <img src="../figures/tree-split.svg" width="1000" height="368" alt="A tree split: files drawn at their key range, a file that straddles the median key shared by both trees until each has compacted its half, then deleted" loading="lazy" />
</figure>

## Compression and Checksum

zdb does block level compression, using the faster lz4 compression for L0 and L1
files, and the slightly slower but more powerful zstd for L2 and L3. This allows
Egres to take half the disk space compared to Postgres, bringing disk savings
that [MyRocks][fb-myrocks] brought to MySQL. Of course, this depends a lot of
the kind of data that's being stored, so YMMV.

Every block is auto-checksummed when fetched to ensure it's still valid. In the
future, these blocks could also be encrypted to provide encryption at rest.

[fb-myrocks]: https://research.facebook.com/publications/myrocks-lsm-tree-database-storage-engine-serving-facebooks-social-graph/

## Serverless Egres

zdb supports both local disk and remote S3 storage. This allows Egres to achieve
a blazing fast NVMe experience with local storage. Or, a fully serverless
experience with remote S3 storage. When using S3, zdb uses a disk cache to store
fixed-size file chunks on local disk (if available), reducing S3 API calls and
the associated latency. This disk cache is in addition to the in-memory cache
that zdb already uses to store SSTable blocks across both storage mediums.

In fact, zdb internally uses a simple interface for storage, which can be
extended to other technologies like Azure Blob Store or Google Cloud Storage
APIs, or HDFS, etc.

<figure class="fig">
  <img src="../figures/zdb-read-path.svg" width="1000" height="448" alt="zdb on S3: a block read checks the memory block cache, then the local NVMe chunk cache, and only then fetches a whole 128 KiB chunk from S3, so neighbouring blocks are served locally afterwards" loading="lazy" />
</figure>
