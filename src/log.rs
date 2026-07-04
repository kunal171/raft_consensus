use crate::node::RaftNode;
use crate::messages::{AppendEntriesResponse, AppendEntries};
use crate::types::{Role, Command, LogEntry, LogIndex};

impl RaftNode {

    // Handle an incoming AppendEntries RPC from the leader. This is used both for log replication and as a heartbeat.
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

    // Append a new command to the log. Only the leader may accept client writes.
    pub fn append_command(&mut self, command: Command) -> Option<LogEntry> {
        // Only a leader may accept client writes. Followers reject — the caller
        // (later: the load balancer) should route the write to the real leader.
        if self.role != Role::Leader {
            return None;
        }

        // Next slot = one past the current last index. First entry gets index 1.
        let entry = LogEntry {
            index: self.last_log_index() + 1,
            term: self.current_term,
            command,
        };

        self.log.push(entry.clone());
        Some(entry)
    }

    // Build an AppendEntries RPC to send to a follower, starting at the given next_index.
    pub fn build_append_entries(&self, next_index: LogIndex) -> AppendEntries {
        // The entry immediately before the ones we're about to send.
        // next_index == 1 → prev is index 0 (nothing before it) → base case.
        let prev_log_index = next_index.saturating_sub(1);
        let prev_log_term = if prev_log_index == 0 {
            0
        } else {
            self.log[(prev_log_index - 1) as usize].term
        };

        // Everything from next_index to the end of the log.
        // If the follower is fully caught up, this is empty → a pure heartbeat.
        let entries: Vec<LogEntry> = self
            .log
            .iter()
            .filter(|e| e.index >= next_index)
            .cloned()
            .collect();

        AppendEntries {
            term: self.current_term,
            leader_id: self.id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
        }
    }

    

}