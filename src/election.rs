//! Leader election — the `RequestVote` half of Raft.
//!
//! Three methods on [`RaftNode`] cover a full election:
//! - [`RaftNode::start_election`] — a follower nominates itself (candidate side).
//! - [`RaftNode::handle_request_vote`] — a node decides whether to grant a vote
//!   (voter side); this is where the safety rules are enforced.
//! - [`RaftNode::count_votes`] — a candidate tallies replies and becomes leader
//!   on a majority.

use crate::node::RaftNode;
use crate::messages::{RequestVote, RequestVoteResponse};
use crate::types::Role;

impl RaftNode {
    /// Begin an election: advance the term, become a `Candidate`, vote for self,
    /// and return the `RequestVote` to broadcast to peers.
    ///
    /// Called when a follower's election timer expires without hearing a leader.
    pub fn start_election(&mut self) -> RequestVote {
        // 1 advance the logical clock — new election = new term
        self.current_term += 1;

        //2 Become a candidate and vote for self
        self.role = Role::Candidate;
        self.voted_for = Some(self.id);

        //3 build the RequestVote message to send to all other nodes
        RequestVote {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        }
    }

    /// Decide whether to grant a vote to a candidate and return the response.
    ///
    /// Enforces the four voting rules: reject stale terms, step down on newer
    /// terms, require the candidate's log to be at least as up-to-date, and
    /// grant at most one vote per term.
    pub fn handle_request_vote(&mut self, req: RequestVote) -> RequestVoteResponse{
        //Rule 1 : Reject if the candidate's term is less than the current term
        if req.term < self.current_term {
            return RequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
            };
        } 
        // Rule 2: if the candidate's term is newer, step down and adopt it
        if req.term > self.current_term {
            self.current_term = req.term;
            self.role = Role::Follower;
            self.voted_for = None;
        }

        //Rule 3: candidate's log must be at least as up-to-date as ours
        let log_ok = req.last_log_term > self.last_log_term() 
            || (req.last_log_term == self.last_log_term() 
                && req.last_log_index >= self.last_log_index());

          // Rule 4: grant only if we haven't voted for someone else this term
        let can_vote = self.voted_for.is_none()
            || self.voted_for == Some(req.candidate_id);

        if can_vote && log_ok{
            self.voted_for = Some(req.candidate_id);
            RequestVoteResponse { term: self.current_term, vote_granted: true } 
        }else {
            RequestVoteResponse {
                term: self.current_term,
                vote_granted: false,
            }
        }

    }

    /// Tally the vote responses and, on a majority, become `Leader`.
    ///
    /// Steps down immediately if any reply carries a newer term. Counts the
    /// node's own self-vote, so it starts at 1.
    pub fn count_votes(&mut self, responses: Vec<RequestVoteResponse>)  {
        // GuardL only candidate can win an election
        // IF we alredy stepped down (someone had a bigger term), do nothing 
        if self.role != Role::Candidate {
            return;
        }

        //Start vote from 1 because we voted for ourselves in the start election. 
        let mut votes = 1;

        for resp in responses {
            // Rule 1: a reply from a newer term means we've lost. Step down.
            if resp.term > self.current_term {
                self.current_term = resp.term;
                self.role = Role::Follower;
                self.voted_for = None;
                return; // abandon the election immediately
            }

            // Rule 2: tally the yes-votes.
            if resp.vote_granted {
                votes += 1;
            }
        }
        // Rule 3: if we reached majority, become the Leader.
        if votes >= self.majority() {
            self.role = Role::Leader;
        }

    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A candidate that wins a majority in a 3-node cluster becomes Leader.
    #[test]
    fn candidate_wins_election() {
        let mut node1 = RaftNode::new(1, vec![2, 3]);
        let mut node2 = RaftNode::new(2, vec![1, 3]);
        let mut node3 = RaftNode::new(3, vec![1, 2]);

        // Node 1 nominates itself and broadcasts the vote request.
        let req = node1.start_election();

        // Both peers vote on it (fresh term, empty logs → both grant).
        let r2 = node2.handle_request_vote(req.clone());
        let r3 = node3.handle_request_vote(req);

        // Node 1 tallies: 1 (self) + 2 grants = 3 >= majority(2).
        node1.count_votes(vec![r2, r3]);

        assert_eq!(node1.role, Role::Leader);
        assert_eq!(node1.current_term, 1);
        assert!(r2_and_r3_granted(&node2, &node3));
    }

    fn r2_and_r3_granted(node2: &RaftNode, node3: &RaftNode) -> bool {
        node2.voted_for == Some(1) && node3.voted_for == Some(1)
    }

    // A node only votes once per term: a second candidate in the same term is denied.
    #[test]
    fn one_vote_per_term() {
        let mut voter = RaftNode::new(2, vec![1, 3]);

        let req_from_1 = RequestVote { term: 1, candidate_id: 1, last_log_index: 0, last_log_term: 0 };
        let req_from_3 = RequestVote { term: 1, candidate_id: 3, last_log_index: 0, last_log_term: 0 };

        let first = voter.handle_request_vote(req_from_1);
        let second = voter.handle_request_vote(req_from_3);

        assert!(first.vote_granted);   // granted to node 1
        assert!(!second.vote_granted); // denied to node 3 — already voted this term
    }

    // A stale request from an older term is rejected.
    #[test]
    fn rejects_old_term() {
        let mut voter = RaftNode::new(2, vec![1, 3]);
        voter.current_term = 5;

        let stale = RequestVote { term: 3, candidate_id: 1, last_log_index: 0, last_log_term: 0 };
        let resp = voter.handle_request_vote(stale);

        assert!(!resp.vote_granted);
        assert_eq!(resp.term, 5);
    }
}