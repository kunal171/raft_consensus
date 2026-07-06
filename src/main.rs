//! End-to-end demo of the `raft_consensus` crate.
//!
//! Runs two acts against in-memory clusters (no real networking):
//!
//! - **Act 1 — the algorithm & fault tolerance:** a node-level walkthrough of
//!   election, replication, a delete, a leader crash, and re-election.
//! - **Act 2 — the client API:** the same cluster driven through the
//!   [`LoadBalancer`], routing writes to the leader and reads to any node.

use raft_consensus::load_balancer::LoadBalancer;
use raft_consensus::node::RaftNode;
use raft_consensus::types::Command;

fn main() {
    act1_algorithm_and_fault_tolerance();
    act2_load_balancer_api();
}

// ---------------------------------------------------------------------------
// Act 1 — cluster internals: election, replication, delete, crash, re-election
// ---------------------------------------------------------------------------
fn act1_algorithm_and_fault_tolerance() {
    println!("################  ACT 1 — algorithm & fault tolerance  ################\n");

    // Build a 3-node cluster (everyone starts as a Follower at term 0).
    let mut n1 = RaftNode::new(1, vec![2, 3]);
    let mut n2 = RaftNode::new(2, vec![1, 3]);
    let mut n3 = RaftNode::new(3, vec![1, 2]);

    println!("=== Cluster started: 3 followers at term 0 ===");
    print_roles(&n1, &n2, &n3);

    // Election 1: node 1's timer fires, it runs and wins.
    let req = n1.start_election();
    let r2 = n2.handle_request_vote(req.clone());
    let r3 = n3.handle_request_vote(req);
    n1.count_votes(vec![r2, r3]);

    println!(
        "\n=== Election 1: node {} becomes Leader at term {} ===",
        n1.id, n1.current_term
    );
    print_roles(&n1, &n2, &n3);

    // Leader accepts two writes and replicates them.
    n1.append_command(Command::Set { key: "x".into(), value: "5".into() });
    n1.append_command(Command::Set { key: "y".into(), value: "9".into() });
    replicate_all(&mut n1, &mut [&mut n2, &mut n3]);
    println!("\n=== After SET x=5, SET y=9 (committed) ===");
    print_state(&n1, &n2, &n3);

    // Leader deletes x.
    n1.append_command(Command::Delete { key: "x".into() });
    replicate_all(&mut n1, &mut [&mut n2, &mut n3]);
    println!("\n=== After DELETE x (committed) ===");
    print_state(&n1, &n2, &n3);

    // The leader crashes — we simply stop talking to node 1.
    println!("\n=== 💥 Node 1 (leader) crashes — no more heartbeats ===");

    // Election 2: node 2's timer fires, runs for term 2. Only node 3 can vote.
    let req = n2.start_election();
    let v3 = n3.handle_request_vote(req);
    n2.count_votes(vec![v3]);
    println!(
        "\n=== Election 2: node {} becomes Leader at term {} (node 1 down) ===",
        n2.id, n2.current_term
    );
    println!("node2: {:?}, node3: {:?}", n2.role, n3.role);

    // New leader accepts a write, committed with just 2 of 3 nodes.
    n2.append_command(Command::Set { key: "z".into(), value: "7".into() });
    replicate_all(&mut n2, &mut [&mut n3]);
    println!("\n=== After SET z=7 under new leader (committed with 2 of 3 nodes) ===");
    println!("node2 state: {:?}", sorted(&n2));
    println!("node3 state: {:?}", sorted(&n3));

    println!(
        "\n=== Logs on live nodes (2 & 3) identical: {} ===\n",
        n2.log == n3.log
    );
}

// ---------------------------------------------------------------------------
// Act 2 — the client-facing API: route writes/reads through the LoadBalancer
// ---------------------------------------------------------------------------
fn act2_load_balancer_api() {
    println!("################  ACT 2 — LoadBalancer client API  ################\n");

    // Build a fresh cluster and elect node 1 on the loose nodes.
    let mut n1 = RaftNode::new(1, vec![2, 3]);
    let mut n2 = RaftNode::new(2, vec![1, 3]);
    let mut n3 = RaftNode::new(3, vec![1, 2]);
    let req = n1.start_election();
    let r2 = n2.handle_request_vote(req.clone());
    let r3 = n3.handle_request_vote(req);
    n1.count_votes(vec![r2, r3]);

    // Hand the elected cluster to the router. Clients now talk only to the LB.
    let mut lb = LoadBalancer::new_from_nodes(vec![n1, n2, n3]);
    println!("Cluster wrapped in LoadBalancer. Leader = node {:?}", lb.find_leader());

    // A client writes without knowing which node is the leader.
    lb.route_write(Command::Set { key: "name".into(), value: "raft".into() }).unwrap();
    lb.route_write(Command::Set { key: "lang".into(), value: "rust".into() }).unwrap();
    println!("\nRouted 2 writes through the LB.");

    // Reads are served from the cluster.
    println!("read name -> {:?}", lb.route_read("name"));
    println!("read lang -> {:?}", lb.route_read("lang"));
    println!("read missing -> {:?}", lb.route_read("missing"));

    // A delete routed the same way.
    lb.route_write(Command::Delete { key: "name".into() }).unwrap();
    println!("\nAfter DELETE name: read name -> {:?}", lb.route_read("name"));
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// One replication round for Act 1: push the leader's log to each follower,
/// commit on majority, apply, then heartbeat so followers commit and apply too.
/// Sends the whole log each round (prev_log_index = 0) — simple and correct for
/// a demo. (The [`LoadBalancer`] does the same cycle internally in Act 2.)
fn replicate_all(leader: &mut RaftNode, followers: &mut [&mut RaftNode]) {
    let mut acks = Vec::new();
    for f in followers.iter_mut() {
        let msg = leader.build_append_entries(1);
        acks.push(f.handle_append_entries(msg));
    }

    let last = leader.last_log_index();
    leader.handle_append_responses(acks, last);
    leader.apply_committed();

    for f in followers.iter_mut() {
        let hb = leader.build_append_entries(leader.last_log_index() + 1);
        f.handle_append_entries(hb);
        f.apply_committed();
    }
}

fn print_roles(n1: &RaftNode, n2: &RaftNode, n3: &RaftNode) {
    println!(
        "node1: {:?}, node2: {:?}, node3: {:?}",
        n1.role, n2.role, n3.role
    );
}

fn print_state(n1: &RaftNode, n2: &RaftNode, n3: &RaftNode) {
    println!("node1: {:?}", sorted(n1));
    println!("node2: {:?}", sorted(n2));
    println!("node3: {:?}", sorted(n3));
}

/// HashMap has no stable iteration order; sort keys so the printout is
/// deterministic and easy to eyeball.
fn sorted(n: &RaftNode) -> Vec<(String, String)> {
    let mut kv: Vec<(String, String)> = n
        .state_machine
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    kv.sort();
    kv
}
