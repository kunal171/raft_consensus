# Raft Consensus

A Rust implementation of the [Raft](https://raft.github.io/) distributed consensus algorithm with a load-balancer front end.

Raft keeps a cluster of servers agreeing on an ordered log of commands even as nodes crash and recover. One node is elected **leader**; all writes go through it and are replicated to a majority of nodes before being committed and applied to a replicated key-value state machine. If the leader fails, the survivors elect a new one and continue without losing any committed data.

This implementation is deliberately **synchronous** — RPCs are plain method calls rather than network messages — so the algorithm's logic stays in the foreground. For the full theory (roles, terms, RPCs, election, replication, safety, and interview Q&A) see **[RAFT_CONCEPTS.md](RAFT_CONCEPTS.md)**.

---

## What it does

- Elects a single leader per term via the `RequestVote` RPC.
- Replicates client commands (`Set` / `Delete` / `Noop`) through the `AppendEntries` RPC.
- Commits an entry once a majority of nodes have it, then applies it to a `HashMap<String, String>` state machine.
- Survives leader crashes: a new leader is elected and committed entries are never lost.
- Exposes a `LoadBalancer` that routes writes to the leader and reads to any node.

---

## Architecture

```
┌─────────────────────────────────────────┐
│              LoadBalancer               │
│  route_write → leader                   │
│  route_read  → any node                 │
└────────────────┬────────────────────────┘
                 │
    ┌────────────┼────────────┐
    ▼            ▼            ▼
┌────────┐  ┌────────┐  ┌────────┐
│ Node 1 │  │ Node 2 │  │ Node 3 │
│ Leader │←→│Follower│←→│Follower│   RequestVote + AppendEntries
└────────┘  └────────┘  └────────┘
   log          log         log
[set x=5]   [set x=5]   [set x=5]
[set y=2]   [set y=2]   [set y=2]
```

Every server is a `RaftNode` holding its term, vote, log, role, commit/apply
indexes, peer list, and state machine. The two RPCs drive all state changes;
the `LoadBalancer` sits in front and hides which node is the leader from clients.

---

## Source layout

```
src/
├── lib.rs            — crate docs + module declarations
├── types.rs          — NodeId, Term, LogIndex, Role, Command, LogEntry
├── messages.rs       — RequestVote / AppendEntries and their responses
├── node.rs           — RaftNode struct, constructor, shared helpers
├── election.rs       — start_election, handle_request_vote, count_votes
├── log.rs            — append_command, build/handle AppendEntries, commit, apply
├── load_balancer.rs  — LoadBalancer: find_leader, route_write, route_read
└── main.rs           — two-act end-to-end demo
tests/
└── integration.rs    — full-cluster scenarios (election, replication, crash)
```

| Module | Responsibility |
|--------|----------------|
| `types` | Core data types shared everywhere. |
| `messages` | The two RPC request/response pairs. |
| `node` | Per-server state and small helpers (`majority`, `last_log_index`, …). |
| `election` | Leader election logic. |
| `log` | Log replication, commit, and state-machine application. |
| `load_balancer` | Client-facing routing layer. |

The `election` and `log` methods are implemented as extra `impl RaftNode`
blocks, so they appear as ordinary methods on `RaftNode`.

---

## Running

```bash
cargo run     # runs the two-act demo (algorithm + fault tolerance, then the LoadBalancer)
cargo test    # runs unit tests + tests/integration.rs
cargo doc --open   # renders the API docs from the doc comments
```

The demo prints a full cluster lifecycle: a leader is elected, entries are
replicated and committed, the leader crashes, a new leader is elected with the
surviving majority, and the same cluster is then driven through the
`LoadBalancer`.

---

## Dependencies

None — pure Rust standard library.

---

## Notes and simplifications

This is a learning implementation. Compared to a production Raft it leaves out:

- **Networking / async** — RPCs are method calls; there is no transport, no timers, and no real concurrency.
- **Per-peer `next_index` / `match_index`** — replication passes the target index explicitly instead of the leader tracking each follower's progress.
- **Persistence** — state lives in memory only; nothing is written to disk.
- **Log compaction / snapshots** and **dynamic membership changes**.

The safety-critical logic — term handling, the vote up-to-date check, the
`AppendEntries` consistency check, and the current-term commit rule — is
implemented faithfully.
