pub type NodeId = u64;  //NodeId gives every Raft server a stable identity.
pub type Term = u64;  //Term is Raft’s logical clock. It increases during elections.
pub type LogIndex = u64;  //LogIndex identifies entries in the replicated log.

//Role struct captures three Raft states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower, 
    Candidate,
    Leader,
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Set { key: String, value: String },
    Delete { key: String },
    Noop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub index: LogIndex,
    pub term: Term,
    pub command: Command,
}

// Candidate → all nodes during election
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVote {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

// Leader → all followers (also used as heartbeat when entries is empty)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntries {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
}


//Raft node 
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
