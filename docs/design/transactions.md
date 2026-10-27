---
title: "Transactions: Repeatable Read, Pessimistic Locking"
description: Snapshot isolation at the leader's last applied timestamp, row locks in a SemaphoreMap with deadlock detection, and conflict detection that bumps the read timestamp instead of aborting.
sidebar:
  label: Transactions
  order: 7
---

*Status: available in the current release.*

Egres provides Snapshot Isolation, the same as Postgres' repeatable read
isolation. Serializable isolation is planned for future releases.

In Egres, write transactions run on the leader. At BEGIN, the transaction
assigns leader's last applied timestamp as the read timestamp,
T<sub>read</sub>. From then on, every read gets a fixed snapshot of the
database as of T<sub>read</sub>. Reads do not block and do not acquire any locks,
unless `FOR UPDATE`.

## Deadlock Detection

<figure class="fig">
  <img src="../figures/semaphore-map.svg" width="1000" height="422" alt="The SemaphoreMap: three transactions each hold a row, then each asks for the next one's row; the third request would close a wait cycle, the walk of the wait-for graph finds it before parking, and that transaction is refused with a deadlock error while the other two proceed" loading="lazy" />
</figure>

Writes use pessimistic locking. A write statement first gathers all the rows it
would change, then locks them in key order to avoid cycles. Locking is done via
[SemaphoreMap][] which exists only in leader's memory. In addition to holding
locks, the map maintains a wait-for graph and detects any deadlocks at the time
of lock acquisition. In case it finds a deadlock cycle, the transaction is
immediately aborted and all held locks are released. There are no lock timeouts.

[SemaphoreMap]: https://github.com/manishrjain/egres/blob/main/zfiber/semaphore_map.zig

## Conflict Detection

The leader also keeps an in-memory [Oracle][oracle]: a map from row hash to its
latest commit timestamp. After locks are acquired, their hashes are checked
against the Oracle to find the max commit timestamp among them,
T<sub>max_commit</sub>. If T<sub>max_commit</sub> is at or below
T<sub>read</sub>, nobody else changed them and the statement proceeds.

[oracle]: https://github.com/manishrjain/egres/blob/main/sql/oracle.zig

If T<sub>max_commit</sub> is higher than T<sub>read</sub>, the rows are stale.
Egres bumps up the T<sub>read</sub> to T<sub>max_commit</sub> and re-runs the
single statement; already acquired locks stay held.

A multi-statement transaction first checks if the earlier statements' row hashes
have T<sub>max_commit</sub> above T<sub>read</sub>. If not, it executes the same
way as above, updating T<sub>read</sub> to T<sub>max_commit</sub> and re-running
the current statement. Otherwise, it fails the transaction with 40001, the same
serialization failure Postgres returns.

<figure class="fig">
  <img src="../figures/conflict-detection.svg" width="1000" height="402" alt="Conflict detection: a transaction reads at 1020, its first statement locks a row last committed at 1010 and proceeds, another transaction commits a second row at 1038, the second statement locks that row and finds it stale, the oracle confirms the first row has not moved, the read timestamp is bumped to 1038 and only the second statement re-runs with its locks held, then the write set commits at 1042" loading="lazy" />
</figure>

All of a transaction's row and index writes go into a memtable owned by that
transaction. When reading, this memtable is overlaid on zdb, so the transaction
can see its own pending writes. Nothing touches storage or the Raft log until
COMMIT, which proposes the entire write set, applied atomically on every
replica. There is no validation phase at commit, because every conflict was
settled by the locking described above before the statement wrote anything.

Pessimistic locking was chosen over optimistic on purpose. Dgraph had optimistic
locking which caused abort storms under high contention. N transactions would
race for a hot key, one wins, and N-1 waste all their work and have to re-do it.
Pessimistic locking turns that contention into a queue, while disjoint
transactions still run in parallel. Locks are taken on rows and unique index
keys only. Non-unique index updates to the same index key run concurrently.
