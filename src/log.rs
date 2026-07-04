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

    // Handle responses from followers to our AppendEntries RPCs. This is used to track replication progress and commit entries.
    pub fn handle_append_responses(
        &mut self,
        responses: Vec<AppendEntriesResponse>,
        replicated_index: LogIndex, // the highest index we just tried to replicate
    ) {
        // Only a leader commits entries.
        if self.role != Role::Leader {
            return;
        }

        // Start at 1 — the leader already has the entry in its own log.
        let mut acks = 1;

        for resp in responses {
            // A reply from a newer term means we're stale — step down immediately.
            if resp.term > self.current_term {
                self.current_term = resp.term;
                self.role = Role::Follower;
                self.voted_for = None;
                return;
            }
            if resp.success {
                acks += 1;
            }
        }

        // Advance commit only if BOTH hold:
        //  (a) a majority (incl. leader) acknowledged, and
        //  (b) the entry is from THIS term (Raft's current-term commit rule).
        if acks >= self.majority() && replicated_index > self.commit_index {
            if let Some(entry) = self.log.get((replicated_index - 1) as usize) {
                if entry.term == self.current_term {
                    self.commit_index = replicated_index;
                }
            }
        }
    }

    // Apply all committed but not yet applied log entries to the state machine.
    pub fn apply_committed(&mut self) {
    // Run every entry that's committed but not yet applied.
        while self.last_applied < self.commit_index {
            self.last_applied += 1;

            // Clone the command out first so the immutable borrow of `self.log`
            // ends before we mutate `self.state_machine`. Avoids a borrow clash.
            let command = self.log[(self.last_applied - 1) as usize].command.clone();

            match command {
                Command::Set { key, value } => {
                    self.state_machine.insert(key, value);
                }
                Command::Delete { key } => {
                    self.state_machine.remove(&key);
                }
                Command::Noop => { /* placeholder entry — no state change */ }
            }
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_leader(id: u64, peers: Vec<u64>) -> RaftNode {
        let mut n = RaftNode::new(id, peers);
        n.role = Role::Leader;
        n.current_term = 1;
        n
    }

    #[test]
    fn leader_replicates_and_commits() {
        let mut leader = make_leader(1, vec![2, 3]);
        let mut f2 = RaftNode::new(2, vec![1, 3]);
        let mut f3 = RaftNode::new(3, vec![1, 2]);

        // 1. Client write — leader appends to its own log (uncommitted).
        let entry = leader.append_command(Command::Set {
            key: "x".to_string(),
            value: "5".to_string(),
        });
        assert!(entry.is_some());
        assert_eq!(leader.last_log_index(), 1);
        assert_eq!(leader.commit_index, 0); // not committed yet

        // 2. Leader builds the message (followers need from index 1).
        let msg = leader.build_append_entries(1);

        // 3. Followers apply it — both accept (empty logs, base case).
        let r2 = f2.handle_append_entries(msg.clone());
        let r3 = f3.handle_append_entries(msg);
        assert!(r2.success);
        assert!(r3.success);

        // 4. Leader tallies acks for index 1 → majority → commit advances.
        leader.handle_append_responses(vec![r2, r3], 1);
        assert_eq!(leader.commit_index, 1);

        // 5. Leader applies the committed entry to its state machine.
        leader.apply_committed();
        assert_eq!(leader.state_machine.get("x"), Some(&"5".to_string()));

        // All three logs are now identical — the core Raft guarantee.
        assert_eq!(leader.log, f2.log);
        assert_eq!(leader.log, f3.log);
    }
}
