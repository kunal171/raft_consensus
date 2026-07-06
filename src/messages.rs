//! The RPC message types exchanged between nodes.
//!
//! Raft runs on exactly two remote procedure calls, each with a request and a
//! response:
//!
//! - [`RequestVote`] / [`RequestVoteResponse`] — the election RPC.
//! - [`AppendEntries`] / [`AppendEntriesResponse`] — the replication RPC, which
//!   doubles as the heartbeat when its `entries` list is empty.
//!
//! These are plain data carriers; the logic that produces and consumes them
//! lives in [`crate::election`] and [`crate::log`].

use crate::types::*;

/// Sent by a `Candidate` to every peer to ask for a vote during an election.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVote {
    /// The candidate's term.
    pub term: Term,
    /// Who is asking for the vote.
    pub candidate_id: NodeId,
    /// Index of the candidate's last log entry — used for the up-to-date check.
    pub last_log_index: LogIndex,
    /// Term of the candidate's last log entry — paired with `last_log_index`.
    pub last_log_term: Term,
}

/// A voter's reply to a [`RequestVote`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteResponse {
    /// The voter's current term, so a stale candidate learns it is behind.
    pub term: Term,
    /// `true` if the vote was granted.
    pub vote_granted: bool,
}

/// Sent by the `Leader` to replicate log entries. With an empty `entries` list
/// it acts as a heartbeat that resets followers' election timers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntries {
    /// The leader's term.
    pub term: Term,
    /// Who the leader is (so followers can redirect clients).
    pub leader_id: NodeId,
    /// Index of the entry immediately preceding the new ones.
    pub prev_log_index: LogIndex,
    /// Term of the entry at `prev_log_index` — the follower must match both.
    pub prev_log_term: Term,
    /// New entries to append (empty = heartbeat).
    pub entries: Vec<LogEntry>,
    /// The leader's commit index, so followers can advance their own.
    pub leader_commit: LogIndex,
}

/// A follower's reply to an [`AppendEntries`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    /// The follower's current term.
    pub term: Term,
    /// `true` if the consistency check passed and the entries were appended.
    pub success: bool,
}
