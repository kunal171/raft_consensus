use crate::node::RaftNode;
use crate::messages::{AppendEntriesResponse, RequestVote, RequestVoteResponse};
use crate::types::Role;

impl RaftNode {
    pub fn handle_append_entries(&mut self, req: AppendEntries) -> AppendEntriesResponse {
        //Rule 1: Reject if the leader's term is less than the current term
        if req.term < self.current_term {
            return AppendEntriesResponse {
                term: self.current_term,
                success: false,
            };
        }

         // Rule 2: valid leader for a >= term. Adopt its term and accept it as leader.
        // Any legitimate AppendEntries means "there is a leader" → become Follower.
        self.current_term = req.term;
        self.role = Role::Follower;

        // Rule 3: consistency check.
        // prev_log_index == 0 means the leader is sending from the very start — nothing to check.
        // Otherwise our log must HAVE an entry at prev_log_index whose term == prev_log_term.
        if req.prev_log_index > 0 {
            let pos = (req.prev_log_index - 1) as usize; // 1-based logical index → 0-based Vec position
            match self.log.get(pos) {
                Some(entry) if entry.term == req.prev_log_term => { /* match — continue */ }
                _ => {
                    // gap or term mismatch → reject; leader will back up and retry
                    return AppendEntriesResponse { term: self.current_term, success: false };
                }
            }
        }

        // Rule 4: append new entries, deleting any conflicting suffix.
        // Truncate everything at/after prev_log_index, then append the leader's entries.
        self.log.truncate(req.prev_log_index as usize);
        self.log.extend(req.entries);

        // Rule 5: advance commit index.
        // Follower can commit up to whatever the leader has committed, but no further than its own log.
        if req.leader_commit > self.commit_index {
            let last_index = self.last_log_index();
            self.commit_index = req.leader_commit.min(last_index);
        }

        AppendEntriesResponse { term: self.current_term, success: true }
    }
}