---
title: No VACUUM. No Repack.
description: Versions carry 64-bit commit timestamps, a read floor is agreed through Raft, and compactions drop what nobody can read any more.
sidebar:
  label: Garbage collection
  order: 4
---

*Status: available in the current release.*

zdb stores updates to the same row as versions. Each version
carries its 64-bit commit timestamp, and a read at a timestamp returns the newest
version at or below it. Every read is a fixed snapshot over the entire database.

zdb does one interesting thing here. Other LSM trees make (key, version
timestamp) composite as the key, with one entry per version. zdb encodes the
versions in the value itself, keeping only one key per row. This reduces the
number of keys in the system by a factor, results in smaller SSTable block
indexes and faster key lookups.

## Garbage Collection

Every Egres node tracks the minimum transaction timestamps currently pending in
the system, so it can surface the lowest timestamp (T<sub>low</sub>) across all open
transactions. It is bound by a configurable maximum transaction age so an idle
transaction cannot pin it forever. When followers reply to the leader's
heartbeats, they include their T<sub>low</sub>.

When creating and proposing a snapshot, leader picks a raft index in the past,
which is either past a certain age `min-age`, or leaves certain WAL size
`min-bytes` while ensuring it is picking all writes still in
memtable. For that Raft entry, it finds its HLC timestamp = T<sub>index</sub>.

Then leader calculates the minimum across all of these, with T<sub>snap</sub> =
MIN(T<sub>index</sub>, T<sub>low for node i</sub>). When creating and proposing a
snapshot, leader sets this T<sub>snap</sub>. Once applied, every node in the
cluster rejects reads below T<sub>snap</sub>. Note that because every replica's
open readers already counted towards the T<sub>snap</sub>, a replica doesn't
have to kill a running query to keep up.

All compactions can then drop unneeded versions below T<sub>snap</sub>, hence
garbage collecting them.

<figure class="fig">
  <img src="../figures/read-floor.svg" width="1000" height="594" alt="The read floor: each node's lowest open read timestamp rides on its heartbeat replies, the leader takes the minimum with the snapshot's own timestamp, proposes it as the snapshot's read floor, every replica applies it without cutting off a running query, and compaction drops the versions below it" loading="lazy" />
</figure>

Postgres can do the same with hot standby feedback, which is off by default; the
default is to cancel the standby query after a delay. With it on, both systems
keep old versions for as long as the slowest reader needs them. The differences
are what happens afterwards.

In Egres the hold is always on and bounded by the transaction age, and when the
T<sub>snap</sub> moves the dead versions vanish at the next compaction of their files,
which writes only the live ones into dense new files. In Postgres they become
holes in heap and index pages: VACUUM marks the space reusable, but the table
stays at its high-water mark, with live rows spread thin across pages, until
VACUUM FULL or pg_repack rewrites it.

Egres' garbage collection is simpler and effective, reusing compactions which
are far lighter than a VACUUM scan. Each compaction operates on a few files at a
time, releasing disk space whenever possible. Timestamps are 64-bit, so there's
no counter to wrap. Postgres has to vacuum even a table nobody writes to, to
freeze its transaction ids before the 32-bit counter does. Egres never has a
reason to visit a cold tree, and cold trees skip compaction altogether.
