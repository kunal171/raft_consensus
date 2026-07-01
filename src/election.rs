use crate::node::RaftNode;
use crate::messages::{RequestVote, RequestVoteResponse};
use crate::types::Role;

impl RaftNode {

    // Start a new election by transitioning to the Candidate state, incrementing the current term, voting for self, and sending RequestVote messages to all other nodes.
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
}