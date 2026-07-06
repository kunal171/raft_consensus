//! Core data types shared across the whole crate.
//!
//! These are the nouns of the Raft algorithm: the identifiers (`NodeId`,
//! `Term`, `LogIndex`), the three server states (`Role`), the operations the
//! replicated state machine understands (`Command`), and the unit of the
//! replicated log (`LogEntry`). Every other module builds on these.

/// Stable identity of a server in the cluster. Assigned once and never reused.
pub type NodeId = u64;

/// Raft's logical clock. Increments on every election; a higher term always
/// wins over a lower one and forces stale nodes to step down.
pub type Term = u64;

/// Position of an entry in the replicated log. 1-based: the first entry is 1,
/// and `0` means "no entry" (an empty log).
pub type LogIndex = u64;

/// The three states a Raft server can be in at any moment.
///
/// A node starts as `Follower`, becomes a `Candidate` when its election timer
/// fires, and becomes `Leader` if it wins a majority of votes. There is at most
/// one leader per term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Passive state: responds to leaders and candidates, never initiates.
    Follower,
    /// Actively campaigning for votes after an election timeout.
    Candidate,
    /// The single node that accepts client writes and drives replication.
    Leader,
}

/// An operation applied to the replicated key-value state machine.
///
/// Commands are stored in the log and only executed once the entry that carries
/// them is committed (replicated to a majority).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Store a key-value pair.
    Set { key: String, value: String },
    /// Remove a key.
    Delete { key: String },
    /// No-op. A freshly elected leader appends one of these for its own term so
    /// that committing it implicitly commits any carried-over prior-term entries.
    Noop,
}

/// A single record in the replicated log.
///
/// The `term` stamp is what the consistency check and safety rules key off:
/// two entries with the same `index` and `term` are guaranteed identical, and
/// so is every entry before them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    /// 1-based position of this entry in the log.
    pub index: LogIndex,
    /// The leader term in which this entry was created.
    pub term: Term,
    /// The operation to apply to the state machine.
    pub command: Command,
}
