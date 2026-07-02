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




