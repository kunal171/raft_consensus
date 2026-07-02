use crate::types::*;
// Candidate → all nodes during election
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVote {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

// Candidate → candidate
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

// AppendEntriesResponse is sent by followers to the leader in response to AppendEntries RPCs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
}