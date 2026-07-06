//! The [`RaftNode`] struct — the complete state of a single server.
//!
//! Every operation in Raft is a mutation of this struct. The election logic
//! ([`crate::election`]) and the replication logic ([`crate::log`]) are both
//! implemented as additional `impl RaftNode` blocks in their own files; this
//! module holds the struct definition, the constructor, and small shared
//! helpers.

use crate::types::*;
use std::collections::HashMap;

/// The full state of one Raft server.
///
/// The fields divide into three groups, following the Raft paper:
/// **persistent** state that must survive a crash, **volatile** state that is
/// rebuilt on restart, and static **cluster membership**.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftNode {
    // --- persistent state (would be written to disk before responding) ---
    /// This node's stable identity.
    pub id: NodeId,
    /// Latest term seen. Increases monotonically.
    pub current_term: Term,
    /// Who this node voted for in `current_term` (`None` = not yet voted).
    pub voted_for: Option<NodeId>,
    /// The replicated log. `log[i]` holds logical index `i + 1`.
    pub log: Vec<LogEntry>,

    // --- volatile state (reset on restart) ---
    /// Current role: Follower, Candidate, or Leader.
    pub role: Role,
    /// Highest log index known to be replicated on a majority.
    pub commit_index: LogIndex,
    /// Highest log index actually applied to `state_machine`.
    pub last_applied: LogIndex,

    // --- cluster membership ---
    /// Ids of the other servers, used to send RPCs and compute a majority.
    pub peers: Vec<NodeId>,

    /// The replicated key-value store — the result of applying committed
    /// commands in order. Identical across all nodes that have applied the
    /// same prefix of the log.
    pub state_machine: HashMap<String, String>,
}

impl RaftNode {
    /// Create a fresh node. Every node starts as a `Follower` at term 0 with an
    /// empty log and an empty state machine.
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        RaftNode {
            id,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            role: Role::Follower,
            commit_index: 0,
            last_applied: 0,
            peers,
            state_machine: HashMap::new(),
        }
    }

    /// Votes needed to win an election or commit an entry: `(cluster / 2) + 1`.
    /// `peers.len().div_ceil(2)` computes `ceil(peers / 2)`, and the trailing
    /// `+ 1` accounts for this node itself — together a strict majority.
    pub fn majority(&self) -> usize {
        self.peers.len().div_ceil(2) + 1
    }

    /// Index of the last log entry, or `0` if the log is empty. Sent in
    /// `RequestVote` so voters can compare log freshness.
    pub fn last_log_index(&self) -> LogIndex {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    /// Term of the last log entry, or `0` if the log is empty. Paired with
    /// [`Self::last_log_index`] for the up-to-date check.
    pub fn last_log_term(&self) -> Term {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }
}
