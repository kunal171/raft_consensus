# Raft Consensus

A Rust implementation of the Raft distributed consensus algorithm with a load balancer layer.

For the full theory — roles, terms, RPCs, election, log replication, safety guarantees, and interview Q&A — see [RAFT_CONCEPTS.md](RAFT_CONCEPTS.md).

---

## Architecture

```
┌─────────────────────────────────────────┐
│              Load Balancer              │
│  routes writes → Leader                 │
│  routes reads  → any node               │
└────────────────┬────────────────────────┘
                 │
    ┌────────────┼────────────┐
    ▼            ▼            ▼
┌────────┐  ┌────────┐  ┌────────┐
│ Node 1 │  │ Node 2 │  │ Node 3 │
│ Leader │←→│Follower│←→│Follower│
└────────┘  └────────┘  └────────┘
   log          log         log
[set x=5]   [set x=5]   [set x=5]
[set y=2]   [set y=2]   [set y=2]
```

---

## Milestones

**Milestone 1 — Node Structure** ✅
Define the data model: node state, roles, log entries, RPC message types.
Files: `types.rs`, `node.rs`, `messages.rs`

**Milestone 2 — Leader Election** ✅
RequestVote RPC, vote counting, term tracking. A candidate can nominate itself,
collect votes, and become leader on majority. Covered by unit tests in `election.rs`.
Files: `election.rs`

**Milestone 3 — Log Replication** ✅
AppendEntries RPC, consistency check, commit index, state machine application.
Leader replicates entries to followers, commits on majority, applies to a KV store.
Covered by a replication unit test in `log.rs`.
Files: `log.rs`

**Milestone 4 — Load Balancer** *(current)*
Routes writes to the current leader, reads to any available node.
Files: `load_balancer.rs`

**Milestone 5 — Integration Tests**
Simulate real cluster scenarios: elect a leader, replicate entries, kill the leader, verify no committed entries are lost.

---

## Project Structure

```
src/
├── lib.rs            — module declarations
├── types.rs          — NodeId, Term, LogIndex, Role, Command, LogEntry
├── node.rs           — RaftNode struct, constructor, state transitions
├── messages.rs       — RequestVote, AppendEntries and their responses
├── election.rs       — start_election, handle_request_vote
├── log.rs            — append_entries, commit, apply to state machine
└── main.rs           — demo / entry point
```

---

## Running

```bash
cargo build
cargo test
cargo run
```

---

## Dependencies

None yet — pure Rust standard library. Tokio will be added in Milestone 2 for async networking and timers.
