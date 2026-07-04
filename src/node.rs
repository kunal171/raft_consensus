use crate::types::*;
use std::collections::HashMap;

// Raft node Struct captures the state of a Raft node, including its persistent and volatile state, as well as its cluster membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftNode {
     // persistent state — must survive crashes
    pub id: NodeId,
    pub current_term : Term,
    pub voted_for: Option<NodeId>,
    pub log : Vec<LogEntry>,
    // Volatile State - reset on restart
    pub role : Role,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,

    //cluster membership
    pub peers: Vec<NodeId>,
    // Volatile state on leaders - reinitialized after election
    pub state_machine: HashMap<String, String>,

}

impl RaftNode {
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        RaftNode {
            id,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            role: Role::Follower,   // every node starts as follower
            commit_index: 0,
            last_applied: 0,
            peers,
            state_machine: HashMap::new(),
        }
    }

    // Votes needed to win an election: (cluster size / 2) + 1
    pub fn majority(&self) -> usize {
        (self.peers.len() + 1) / 2 + 1
    }

    // Index of the last log entry — sent in RequestVote so voters can check log freshness
    pub fn last_log_index(&self) -> LogIndex {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    // Term of the last log entry — paired with last_log_index for the up-to-date check
    pub fn last_log_term(&self) -> Term {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }
}
