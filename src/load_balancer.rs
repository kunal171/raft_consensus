//! A routing layer that sits in front of the cluster.
//!
//! Unlike everything else in the crate, [`LoadBalancer`] is not a method on
//! [`RaftNode`] — it *owns* the nodes and hides which one is the leader from
//! clients. Writes are routed to the leader and replicated; reads can be served
//! from any node's state machine. It turns a pile of Raft nodes into a usable
//! key-value service.
//!
//! The load balancer is a pure router: it does not run elections. Elect a
//! leader on the nodes first, then hand them over with [`LoadBalancer::new_from_nodes`].

use crate::node::RaftNode;
use crate::types::{Command, NodeId, Role};

/// Front door to the cluster: owns the nodes and routes client requests.
pub struct LoadBalancer {
    /// The Raft servers this balancer routes to.
    pub nodes: Vec<RaftNode>,
}

impl Default for LoadBalancer {
    fn default() -> Self {
        Self::new()
    }
}

impl LoadBalancer {
    /// Create an empty balancer; add nodes with [`Self::add_node`].
    pub fn new() -> Self {
        LoadBalancer { nodes: Vec::new() }
    }

    /// Create a balancer from an already-built cluster — typically after a
    /// leader has been elected on loose nodes, which are then handed over.
    pub fn new_from_nodes(nodes: Vec<RaftNode>) -> Self {
        LoadBalancer { nodes }
    }

    /// Register one more node with the balancer.
    pub fn add_node(&mut self, node: RaftNode) {
        self.nodes.push(node);
    }

    /// Return the id of the current leader, or `None` during an election.
    pub fn find_leader(&self) -> Option<NodeId> {
        self.nodes
            .iter()
            .find(|n| n.role == Role::Leader)
            .map(|n| n.id)
    }

    /// Map a node id to its position in `nodes`.
    fn index_of_node(&self, node_id: NodeId) -> Option<usize> {
        self.nodes.iter().position(|node| node.id == node_id)
    }

    /// Route a client write to the leader and replicate it across the cluster.
    ///
    /// Runs the full cycle — leader append, replicate to followers, commit on
    /// majority, apply, then a heartbeat so followers commit and apply too.
    /// Returns `Err` if there is no leader (e.g. mid-election).
    pub fn route_write(&mut self, command: Command) -> Result<(), String> {
        let leader_id = self.find_leader().ok_or("no leader available")?;
        let leader_idx = self.index_of_node(leader_id).unwrap();

        //1 Leader appends the command to its own log 
        self.nodes[leader_idx].append_command(command);

        // 2. Build the replication message from the (now-updated) log.
        let last = self.nodes[leader_idx].last_log_index();
        let msg = self.nodes[leader_idx].build_append_entries(1);

        let mut acks = Vec::new();
        for i in 0..self.nodes.len() {
                if i == leader_idx { continue; }
                acks.push(self.nodes[i].handle_append_entries(msg.clone()));
        }
        
         // 3. Leader commits on majority, then applies.
        self.nodes[leader_idx].handle_append_responses(acks, last);
        self.nodes[leader_idx].apply_committed();

        // 4. Heartbeat so followers commit + apply.
        let hb_next = self.nodes[leader_idx].last_log_index() + 1;
        let hb = self.nodes[leader_idx].build_append_entries(hb_next);
        for i in 0..self.nodes.len() {
            if i == leader_idx { continue; }
            self.nodes[i].handle_append_entries(hb.clone());
            self.nodes[i].apply_committed();
        }

        Ok(())
    }

    /// Serve a read from a node's state machine. Reads don't need the leader;
    /// any node answers. Note this is eventually consistent — a follower may
    /// lag the leader by a commit or two.
    pub fn route_read(&self, key: &str) -> Option<String> {
        // Any node can answer a read. First node for now; could round-robin.
        self.nodes.first()?.state_machine.get(key).cloned()
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_routes_to_leader_and_reads_back() {
        // Build 3 nodes and elect node 1 on the loose nodes first.
        let mut n1 = RaftNode::new(1, vec![2, 3]);
        let mut n2 = RaftNode::new(2, vec![1, 3]);
        let mut n3 = RaftNode::new(3, vec![1, 2]);

        let req = n1.start_election();
        let r2 = n2.handle_request_vote(req.clone());
        let r3 = n3.handle_request_vote(req);
        n1.count_votes(vec![r2, r3]);

        // Hand the elected cluster to the load balancer.
        let mut lb = LoadBalancer::new_from_nodes(vec![n1, n2, n3]);

        // The LB can identify the leader.
        assert_eq!(lb.find_leader(), Some(1));

        // Route a write through the LB, then read it back from the cluster.
        lb.route_write(Command::Set { key: "a".into(), value: "1".into() }).unwrap();
        assert_eq!(lb.route_read("a"), Some("1".to_string()));
    }

    #[test]
    fn write_without_leader_errors() {
        // Three followers, no election → no leader.
        let nodes = vec![
            RaftNode::new(1, vec![2, 3]),
            RaftNode::new(2, vec![1, 3]),
            RaftNode::new(3, vec![1, 2]),
        ];
        let mut lb = LoadBalancer::new_from_nodes(nodes);

        assert_eq!(lb.find_leader(), None);
        assert!(lb
            .route_write(Command::Set { key: "a".into(), value: "1".into() })
            .is_err());
    }
}
