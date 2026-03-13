# TODO.md

### PROJECT: Switch over diskwal to VFS and zio

- We're currently doing mmap for the wal. We should switch over to VFS
mechanism. The only problem is we need to use a cache, and we don't know how
much cache to allocate to this. Or, could be as simple as the last 100 entries
or something.

### PROJECT: Add Context to all requests

- That way, we can cancel a context and stop extra work that's going on.

### PROJECT: Implement priority based and multiplexing TCP connections

- For all internal comms, we should have 3 tcp conns per pair of servers.
- For .p0, .p1, and .p2.
- p0 is used for heartbeats and voting
- p1 is for proposals
- p2 is for background jobs like compactions
- Also implementing framing, so we can multiplex multiple requests and responses
  over the same connection via streams.
- We don't need to throttle a stream -- the overall connection can just get
  throttled if the receiver isn't able to process it. So, there's natural
  backpressure.

### PROJECT: MANIFEST to be run via Raft

- All MANIFEST changes should be run via Raft.
- To achieve this we might want to have a monotonically increasing counter which
  tracks how many changes have been made to MANIFEST. This would allow a
  follower to also propose changes, while proving that it has the latest copy of
  MANIFEST -- equivalent of atomic.compareAndSwap.
- At a cursory look, commit might be the entry point, where we can do a propose
  and wait for Raft commit to happen.

### PROJECT: Make zdb values smarter
*Mar 3, 2026*

- Instead of just using dumb []u8 's for values, we need to make them smart.
  - Each table belongs to one SQL Table. An sstable never crosses sql table
    boundaries. Side benefit: makes drop table super easy.
  - The table stores the schema version for that table, which tells us how to
    read the values.
  - The value has a bitmap of valid columns,
  - followed by a fixed-width entry, which has either values for fixed-width
    cols, or pointers to variable width cols.
  - followed by the actual variable values.
  - When doing iteration, we should specifically mention which col-value we're
    looking for (or all of them).
  - We should check this against the current "schema" of the value for the
    table.
  - We either drop the value or convert the value to the right type -- for
    example, if value is int32, but schema is int64, we should convert on-the-fly.

The question is -- should this live in ZDB, or should this live outside ZDB? If
it lives within ZDB, it makes it much easier to jump over the values, instead of
reading the entire row and then picking the right cols from it. Also, when doing
compactions, we can then convert the values to the latest schema. So, ZDB has to
be aware of how to handle these values, and how to pick and convert them.


### PROJECT: MANIFEST to be namespace aware
*Mar 3, 2026*
DONE: Mar 6, 2026

We share in-memory tables across namespaces to decrease memory pressure. But, as
soon as we hit L0, the in-memory file is broken across namespaces, and belong to
the various LSM trees corresponding to the namespace. The entire LSM tree must
be under the namespace. We should get this done soon, so we're namespace aware.

### PROJECT: Add guards within the LSM tree
*Mar 3, 2026*
DONE: Mar 9, 2026 - "Ability to split a tree"

We should add guards within the LSM tree, so we can split it up into two. This
is for the LSM corresponding to just one namespace. So, essentially, within the
namespace, we add a bunch of guards to split up the LSM trees, so they're easier
to handle. That way, we only run one compaction per tree, while running multiple
compactions across these trees.

### PROJECT: Integrate with object storage
*Mar 3, 2026*

We should send lower level tables to object storage. So, on a crash, we're
limiting how much data we need to pass around before we can spin up a new node.
For this, we'd also need to device a local disk cache -- possibly a variation on
the s3-fifo cache we already have.

### PROJECT: Benchmark s3-fifo cache
*Mar 3, 2026*

Is it good? Does it have good hit ratios, and can it scale as well as ristretto
did? Does it have less overhead?


---
Feb 26, 2026

- So, now that I have the s3-fifo cache, what's next?
- Switch to writing data in 4K blocks.
- Switch to reading data in 4K blocks using zio.
- Add a virtual fs, so I can control the bandwidth of writes better.
- Do direct i/o writes for sstables for now (and later WAL).

- Also, integrate zdb with Raft, so we can do distributed compactions.


Design for the multi-tenant, cloud-native LSM tree

So, each of the namespace would be an automatic "guard". All its tables would
be physically different from other namespaces, even though they're all in the
same LSM tree. Then for a very big namespace, we'd find median keys from lower
level, so even that namespace can be naturally broken up across guards. These
guards would allow us to run parallel compactions across the tree, and make each
of the "range" more manageable.

Now, for the lower levels, as soon as we do compactions, we can send those
tables over to S3. And we could even have external workers do the compactions
for those levels for us. We only handle the upper level compactions, where the
  data is small. Even for these upper levels, we need to ensure that the
  compaction only happens once, and not all the replicas are doing them -- while
  also ensuring the files output are exactly the same.

This means, that if we need to bring up a new node in case of a crash, there's
minimal data transfer that needs to happen. A new node can be up and running
quickly within minutes, and the queries can start flowing to this new node. It
can then download the data from S3 as required.

Meanwhile, we'd also want to have a disk based storage mechanism, so perhaps
s3-fifo, but where data is located on disk -- and something which can survive a
process reboot.

We'd also want a way for us to delete all the blocks belonging to a table when
the table gets compacted away.

We can keep an index of table -> block ids that are being cached. And then use
it so when a table is deleted, we delete the corresponding block cache keys.

Maybe instead of using a HashMap, we use a binary
search tree?? Might be slower, but would allow us to do a range scan to find all
blocks for the same table next to each other and delete them. Or, perhaps
repurpose skiplist for it? Or, perhaps just iterate over the hash map itself.
