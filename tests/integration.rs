//! Milestone 5 — integration tests.
//!
//! These exercise the public API end-to-end, simulating real cluster
//! scenarios: electing a leader, replicating entries, surviving a leader crash,
//! and routing client traffic through the load balancer. RPCs are delivered as
//! direct method calls (this implementation is synchronous), so each test
//! drives the message flow by hand via the two helpers below.

use raft_consensus::load_balancer::LoadBalancer;
use raft_consensus::node::RaftNode;
use raft_consensus::types::{Command, Role};

/// Run an election in which `candidate` campaigns against `voters`.
fn elect(candidate: &mut RaftNode, voters: &mut [&mut RaftNode]) {
    let req = candidate.start_election();
    let mut responses = Vec::new();
    for v in voters.iter_mut() {
        responses.push(v.handle_request_vote(req.clone()));
    }
    candidate.count_votes(responses);
}

/// One full replication round: push the leader's log to each follower, commit
/// on majority, apply, then heartbeat so followers commit and apply too.
fn replicate(leader: &mut RaftNode, followers: &mut [&mut RaftNode]) {
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

#[test]
fn elects_single_leader_in_three_node_cluster() {
    let mut n1 = RaftNode::new(1, vec![2, 3]);
    let mut n2 = RaftNode::new(2, vec![1, 3]);
    let mut n3 = RaftNode::new(3, vec![1, 2]);

    elect(&mut n1, &mut [&mut n2, &mut n3]);

    assert_eq!(n1.role, Role::Leader);
    assert_eq!(n2.role, Role::Follower);
    assert_eq!(n3.role, Role::Follower);
    assert_eq!(n1.current_term, 1);

    // Exactly one leader exists in the cluster.
    let leaders = [&n1, &n2, &n3]
        .iter()
        .filter(|n| n.role == Role::Leader)
        .count();
    assert_eq!(leaders, 1);
}

#[test]
fn replicates_entries_to_all_followers() {
    let mut n1 = RaftNode::new(1, vec![2, 3]);
    let mut n2 = RaftNode::new(2, vec![1, 3]);
    let mut n3 = RaftNode::new(3, vec![1, 2]);
    elect(&mut n1, &mut [&mut n2, &mut n3]);

    n1.append_command(Command::Set { key: "a".into(), value: "1".into() });
    n1.append_command(Command::Set { key: "b".into(), value: "2".into() });
    replicate(&mut n1, &mut [&mut n2, &mut n3]);

    // Logs are identical across the whole cluster.
    assert_eq!(n1.log, n2.log);
    assert_eq!(n1.log, n3.log);

    // State machines converged, and the leader committed both entries.
    assert_eq!(n2.state_machine.get("a"), Some(&"1".to_string()));
    assert_eq!(n3.state_machine.get("b"), Some(&"2".to_string()));
    assert_eq!(n1.commit_index, 2);
}

#[test]
fn new_leader_elected_after_leader_crash() {
    let mut n1 = RaftNode::new(1, vec![2, 3]);
    let mut n2 = RaftNode::new(2, vec![1, 3]);
    let mut n3 = RaftNode::new(3, vec![1, 2]);
    elect(&mut n1, &mut [&mut n2, &mut n3]);
    assert_eq!(n1.role, Role::Leader);

    // Node 1 crashes; node 2 runs an election that only node 3 can answer.
    elect(&mut n2, &mut [&mut n3]);

    assert_eq!(n2.role, Role::Leader);
    assert_eq!(n2.current_term, 2);
    assert!(n2.current_term > n1.current_term);
}

#[test]
fn committed_entries_survive_leader_change() {
    let mut n1 = RaftNode::new(1, vec![2, 3]);
    let mut n2 = RaftNode::new(2, vec![1, 3]);
    let mut n3 = RaftNode::new(3, vec![1, 2]);
    elect(&mut n1, &mut [&mut n2, &mut n3]);

    // Commit an entry under leader 1.
    n1.append_command(Command::Set { key: "durable".into(), value: "yes".into() });
    replicate(&mut n1, &mut [&mut n2, &mut n3]);

    // Leader 1 crashes; node 2 takes over at a higher term.
    elect(&mut n2, &mut [&mut n3]);
    assert_eq!(n2.role, Role::Leader);

    // The committed entry survived the leader change on the live nodes.
    assert_eq!(n2.state_machine.get("durable"), Some(&"yes".to_string()));
    assert_eq!(n3.state_machine.get("durable"), Some(&"yes".to_string()));

    // The new leader can still make progress with the surviving majority.
    n2.append_command(Command::Set { key: "after".into(), value: "crash".into() });
    replicate(&mut n2, &mut [&mut n3]);
    assert_eq!(n3.state_machine.get("after"), Some(&"crash".to_string()));
    assert_eq!(n2.log, n3.log);
}

#[test]
fn load_balancer_round_trips_writes_and_reads() {
    let mut n1 = RaftNode::new(1, vec![2, 3]);
    let mut n2 = RaftNode::new(2, vec![1, 3]);
    let mut n3 = RaftNode::new(3, vec![1, 2]);
    elect(&mut n1, &mut [&mut n2, &mut n3]);

    let mut lb = LoadBalancer::new_from_nodes(vec![n1, n2, n3]);
    assert_eq!(lb.find_leader(), Some(1));

    lb.route_write(Command::Set { key: "k".into(), value: "v".into() })
        .unwrap();
    assert_eq!(lb.route_read("k"), Some("v".to_string()));

    lb.route_write(Command::Delete { key: "k".into() }).unwrap();
    assert_eq!(lb.route_read("k"), None);
}

#[test]
fn write_without_leader_is_rejected() {
    // Three followers, no election has happened.
    let nodes = vec![
        RaftNode::new(1, vec![2, 3]),
        RaftNode::new(2, vec![1, 3]),
        RaftNode::new(3, vec![1, 2]),
    ];
    let mut lb = LoadBalancer::new_from_nodes(nodes);

    assert_eq!(lb.find_leader(), None);
    assert!(lb
        .route_write(Command::Set { key: "k".into(), value: "v".into() })
        .is_err());
}
