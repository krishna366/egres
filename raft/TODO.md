## Multi-Process Raft TODO

1. [x] Define wire transport boundary.
- Frame each message on TCP/Unix socket as `u32 len` + bytes.
- Keep zbuf tail inside the frame payload.
- Add `sendMessage(fd, []u8)` and `readMessage(fd) -> []u8`.

2. [x] Build a network adapter around `Node`.
- Single-threaded event loop per process.
- Poll sockets + timer tick.
- Feed inbound frames to `node.handleInbound(...)`.
- Flush `node.takeOutbox()` to peer connections.

3. [x] Add peer addressing and connection manager.
- Static config first: `node_id -> host:port`.
- Maintain reconnecting outbound connections.
- Accept inbound connections and map to peer id via handshake.

4. Add process-level durable log implementation.
- Keep `MemWAL` for tests.
- Add `FileRaftLog` implementing the same `RaftWAL` vtable.
- Append raw `LogEntryBuffer.bytes` to WAL (length-prefixed records).
- Recover hard state + log on startup.

5. Add bootstrap and membership mode.
- Boot with initial voters/learners from config.
- Start node as voter/learner based on config.
- Keep dynamic conf-change flow for later once base cluster is stable.

6. Add integration harness for multi-process tests.
- Spawn 3 processes with ports.
- Wait for leader election.
- Submit proposals to leader endpoint.
- Verify replication and leader failover by killing one process.

7. Add minimal client/admin RPCs.
- `propose(data)` endpoint on each node (forward/reject when not leader).
- `status` endpoint (id/state/term/leader/commit/last_index).

8. Hardening pass.
- Backpressure limits on outbound queue.
- Max frame size check before allocation.
- Connection/auth sanity checks.
- Structured logs for term transitions and message flow.

9. Add proposal commit notification path.
- Emit `CommitEvent{ index, term, ... }` from the Raft loop into thread-safe queue.
- Keep caller-side pending map keyed by `(index, term)` to resolve proposal outcomes.
- Fail all pending waiters on step-down/shutdown and handle timeout cleanup.
