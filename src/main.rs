use raft_consensus::node::RaftNode;
use raft_consensus::types::Command;

fn main() {
    // ---- 1. Build a 3-node cluster (everyone starts as a Follower at term 0) ----
    let mut n1 = RaftNode::new(1, vec![2, 3]);
    let mut n2 = RaftNode::new(2, vec![1, 3]);
    let mut n3 = RaftNode::new(3, vec![1, 2]);

    println!("=== Cluster started: 3 followers at term 0 ===");
    print_roles(&n1, &n2, &n3);

    // ---- 2. Election 1: node 1's timer fires, it runs and wins ----
    let req = n1.start_election();
    let r2 = n2.handle_request_vote(req.clone());
    let r3 = n3.handle_request_vote(req);
    n1.count_votes(vec![r2, r3]);

    println!(
        "\n=== Election 1: node {} becomes Leader at term {} ===",
        n1.id, n1.current_term
    );
    print_roles(&n1, &n2, &n3);

    // ---- 3. Leader accepts two writes and replicates them ----
    n1.append_command(Command::Set { key: "x".into(), value: "5".into() });
    n1.append_command(Command::Set { key: "y".into(), value: "9".into() });
    replicate_all(&mut n1, &mut [&mut n2, &mut n3]);

    println!("\n=== After SET x=5, SET y=9 (committed) ===");
    print_state(&n1, &n2, &n3);

    // ---- 4. Leader deletes x ----
    n1.append_command(Command::Delete { key: "x".into() });
    replicate_all(&mut n1, &mut [&mut n2, &mut n3]);

    println!("\n=== After DELETE x (committed) ===");
    print_state(&n1, &n2, &n3);

    // ---- 5. The leader crashes. We simply stop talking to node 1. ----
    println!("\n=== 💥 Node 1 (leader) crashes — no more heartbeats ===");

    // ---- 6. Election 2: node 2's timer fires, runs for term 2. ----
    //          Only node 3 can vote (node 1 is down). Self + node 3 = majority.
    let req = n2.start_election();
    let v3 = n3.handle_request_vote(req);
    n2.count_votes(vec![v3]);

    println!(
        "\n=== Election 2: node {} becomes Leader at term {} (node 1 down) ===",
        n2.id, n2.current_term
    );
    println!("node2: {:?}, node3: {:?}", n2.role, n3.role);

    // ---- 7. New leader accepts a write, replicates to the one live follower ----
    //          Committing with 2 of 3 nodes proves the cluster tolerates 1 failure.
    n2.append_command(Command::Set { key: "z".into(), value: "7".into() });
    replicate_all(&mut n2, &mut [&mut n3]);

    println!("\n=== After SET z=7 under new leader (committed with 2 of 3 nodes) ===");
    println!("node2 state: {:?}", sorted(&n2));
    println!("node3 state: {:?}", sorted(&n3));

    // ---- 8. Live nodes agree; x is gone, z survived the leader change ----
    println!(
        "\n=== Logs on live nodes (2 & 3) identical: {} ===",
        n2.log == n3.log
    );
}

// One full replication round: push the leader's log to each follower, commit on
// majority, apply, then send a heartbeat so followers learn the commit and apply too.
// Sends the whole log each round (prev_log_index = 0) — simple and always correct for a demo.
fn replicate_all(leader: &mut RaftNode, followers: &mut [&mut RaftNode]) {
    // Round 1: replicate entries and collect acks.
    let mut acks = Vec::new();
    for f in followers.iter_mut() {
        let msg = leader.build_append_entries(1);
        acks.push(f.handle_append_entries(msg));
    }

    // Leader commits once a majority (incl. itself) acked, then applies.
    let last = leader.last_log_index();
    leader.handle_append_responses(acks, last);
    leader.apply_committed();

    // Round 2 (heartbeat): carry the new leader_commit so followers commit + apply.
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

// HashMap has no stable iteration order; sort keys so the printout is deterministic.
fn sorted(n: &RaftNode) -> Vec<(String, String)> {
    let mut kv: Vec<(String, String)> = n
        .state_machine
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    kv.sort();
    kv
}
