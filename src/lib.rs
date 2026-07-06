//! # raft_consensus
//!
//! An educational, single-threaded implementation of the [Raft] consensus
//! algorithm with a load-balancer front end.
//!
//! Raft keeps a cluster of servers agreeing on an ordered log of commands even
//! as nodes crash and recover. One node is elected **leader**; all writes go
//! through it and are replicated to a majority before being committed and
//! applied to a replicated key-value state machine.
//!
//! ## Module map
//!
//! | Module | Responsibility |
//! |--------|----------------|
//! | [`types`] | Core nouns: `NodeId`, `Term`, `LogIndex`, `Role`, `Command`, `LogEntry`. |
//! | [`messages`] | The two RPCs: `RequestVote` and `AppendEntries` (+ responses). |
//! | [`node`] | The `RaftNode` struct — all per-server state — and shared helpers. |
//! | [`election`] | Leader election: `start_election`, `handle_request_vote`, `count_votes`. |
//! | [`log`] | Log replication: append, replicate, commit, and apply. |
//! | [`load_balancer`] | Routes client writes to the leader and reads to any node. |
//!
//! The methods in [`election`] and [`log`] are implemented as extra
//! `impl RaftNode` blocks, so they appear as ordinary methods on
//! [`node::RaftNode`].
//!
//! This implementation is deliberately synchronous: RPCs are plain method calls
//! rather than network messages, which keeps the algorithm's logic in the
//! foreground. See `RAFT_CONCEPTS.md` for the theory.
//!
//! [Raft]: https://raft.github.io/

pub mod types;
pub mod messages;
pub mod node;
pub mod election;
pub mod log;
pub mod load_balancer;
