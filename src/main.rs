use raft_consensus::node::RaftNode;
use raft_consensus::types::Command;

fn main() {
    // ---- 1. Build a 3-node cluster (everyone starts as a Follower at term 0) ----
    let mut n1 = RaftNode::new(1, vec![2, 3]);
    let mut n2 = RaftNode::new(2, vec![1, 3]);
    let mut n3 = RaftNode::new(3, vec![1, 2]);

    println!("=== Cluster started: 3 followers at term 0 ===");
    print_roles(&n1, &n2, &n3);

    // ---- 2. Election: node 1's timer fires, it runs for leader ----
    let vote_req = n1.start_election();
    let r2 = n2.handle_request_vote(vote_req.clone());
    let r3 = n3.handle_request_vote(vote_req);
    n1.count_votes(vec![r2, r3]);

    println!("\n=== After election (node 1 ran) ===");
    print_roles(&n1, &n2, &n3);
    println!("Leader is node {} at term {}", n1.id, n1.current_term);

    // ---- 3. Client writes: only the leader accepts them ----
    n1.append_command(Command::Set { key: "x".into(), value: "5".into() });
    n1.append_command(Command::Set { key: "y".into(), value: "9".into() });
    println!("\n=== Leader appended 2 commands (uncommitted) ===");
    println!(
        "Leader log length: {}, commit_index: {}",
        n1.log.len(),
        n1.commit_index
    );

    // ---- 4. Replicate the new entries to both followers ----
    let msg = n1.build_append_entries(1); // followers need everything from index 1
    let a2 = n2.handle_append_entries(msg.clone());
    let a3 = n3.handle_append_entries(msg);
    println!(
        "\n=== Replicated to followers (ack2={}, ack3={}) ===",
        a2.success, a3.success
    );

    // ---- 5. Leader commits once a majority acked, then applies ----
    let last = n1.last_log_index();
    n1.handle_append_responses(vec![a2, a3], last);
    n1.apply_committed();
    println!("\n=== After commit + apply on leader ===");
    println!(
        "Leader commit_index: {}, state: {:?}",
        n1.commit_index, n1.state_machine
    );

    // ---- 6. Next heartbeat carries leader_commit → followers commit + apply ----
    let hb = n1.build_append_entries(n1.last_log_index() + 1); // no new entries, just the commit info
    let _ = n2.handle_append_entries(hb.clone());
    let _ = n3.handle_append_entries(hb);
    n2.apply_committed();
    n3.apply_committed();
    println!("\n=== After heartbeat propagated the commit ===");
    println!("Node 2 state: {:?}", n2.state_machine);
    println!("Node 3 state: {:?}", n3.state_machine);

    // ---- 7. The core Raft guarantee: identical logs across the cluster ----
    let logs_match = n1.log == n2.log && n1.log == n3.log;
    println!("\n=== Logs identical across all nodes: {} ===", logs_match);
}

fn print_roles(n1: &RaftNode, n2: &RaftNode, n3: &RaftNode) {
    println!(
        "node1: {:?}, node2: {:?}, node3: {:?}",
        n1.role, n2.role, n3.role
    );
}
