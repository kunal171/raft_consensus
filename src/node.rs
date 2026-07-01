pub use crate::types::*;
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
        }
    }
}
