# Raft — Concepts and Theory

Deep reference for the Raft consensus algorithm. Read this to understand the *why* behind every design decision in the code.

---

## The Problem Raft Solves

If you have 5 servers storing data and a client writes a value, which server accepts it? What if that server crashes mid-write? How do the others know what happened?

Without coordination, different servers end up with different data — your system is inconsistent. Raft solves this by electing one **Leader** that all writes go through. The leader replicates every write to a majority of servers before confirming it to the client. If the leader crashes, the remaining servers elect a new one and continue.

---

## Roles

Every node is always in one of three states:

```
Follower   — default state, listens and responds to leader
Candidate  — trying to become leader during an election
Leader     — exactly one at a time, handles all client writes
```

Every node starts as a **Follower**. If it stops hearing from the leader (election timeout fires), it becomes a **Candidate** and tries to get elected.

```
         timeout                  wins majority
Follower ────────→ Candidate ──────────────────→ Leader
   ↑                   │                            │
   │   sees higher     │   sees higher term          │
   └───term or loses───┘   or steps down             │
   └────────────────────────────────────────────────┘
              heartbeat from new leader
```

---

## Terms

Time is divided into **terms** — numbered integers that increment with every election.

```
Term 1: Node A wins election, becomes leader
Term 2: Node A crashes, new election, Node B becomes leader
Term 3: Node B crashes, new election, Node C becomes leader
```

Terms act as a logical clock. If a node receives a message from an old term, it ignores it — that information is stale. If it receives a message from a higher term than its own, it immediately reverts to Follower and updates its term.

**Key rule**: a node votes for at most one candidate per term.

---

## The Log

The log is an ordered list of commands. Every node maintains its own copy.

```
Index:  1         2         3         4
       ┌─────────┬─────────┬─────────┬─────────┐
Term:  │ term=1  │ term=1  │ term=2  │ term=2  │
       ├─────────┼─────────┼─────────┼─────────┤
Cmd:   │ set x=1 │ set y=2 │ del x   │ set z=9 │
       └─────────┴─────────┴─────────┴─────────┘
                            ↑
                       commit_index (entries up to here are safe)
```

Each entry stores:
- **index** — position in the log (1-based)
- **term** — which leader term this entry was created in
- **command** — the actual operation (Set, Delete, Noop)

The **state machine** is just the result of applying log entries in order. All nodes that apply the same log entries in the same order end up with identical state — this is the core guarantee.

---

## The Two RPCs

Everything in Raft runs on exactly two remote procedure calls.

### RequestVote

Sent by Candidates during an election to ask for votes.

```
Candidate → all nodes:
  "I'm running for term T.
   My log ends at index I, term LT.
   Please vote for me."

Each node responds:
  vote_granted = true   if: term is current, haven't voted yet, candidate log is up-to-date
  vote_granted = false  if: already voted, or own log is more up-to-date
```

**Log up-to-date check**: a candidate's log is at least as up-to-date as a voter's log if:
- Its last log term is higher, OR
- Terms are equal and its last log index is >= voter's last log index

This prevents a node with a stale log from becoming leader and overwriting committed entries.

### AppendEntries

Sent by the Leader to replicate log entries AND as a heartbeat (empty entries) to prevent elections.

```
Leader → all followers:
  "I am the leader for term T.
   My previous entry was at index PI, term PT.
   Here are new entries: [...]
   Entries up to index LC are committed."

Each follower responds:
  success = true   if: term matches, prev_log matches, entries appended
  success = false  if: term is stale, or log doesn't contain prev_log entry
```

**Heartbeat**: an AppendEntries with an empty entries list. Sent every ~150ms to reset follower election timers. If a follower doesn't hear a heartbeat within its timeout (150–300ms random), it starts an election.

---

## Leader Election — Step by Step

```
1. Follower's election timer expires (no heartbeat received in 150–300ms)
2. Follower → Candidate:
     current_term += 1
     voted_for = Some(self.id)
     role = Candidate
3. Candidate sends RequestVote to all peers
4. Peers respond with vote_granted = true or false
5. If votes received >= majority (n/2 + 1):
     role = Leader
     send heartbeats immediately to reset all timers
6. If another leader announces itself (AppendEntries arrives):
     role = Follower
7. If election timer fires again (split vote):
     start new election with incremented term
```

**Why random timeouts?** If all nodes had the same timeout they'd all become candidates at the same time, split votes, and never elect a leader. Random timeouts (150–300ms) ensure one node usually fires first and wins before others time out.

---

## Log Replication — Step by Step

```
1. Client sends write to Leader: "set x = 5"
2. Leader appends to its own log (uncommitted, index=N)
3. Leader sends AppendEntries to all followers with the new entry
4. Each follower appends to its log, replies success=true
5. Once majority (including leader) have appended:
     leader sets commit_index = N
     leader applies entry to state machine
     leader replies OK to client
6. Next heartbeat includes leader_commit = N
7. Followers see leader_commit > their commit_index
     followers apply entries up to N to their state machines
```

**Why commit only after majority?** If only 1 of 5 nodes has an entry and that node crashes, the entry is lost. Once a majority (3 of 5) have it, even if 2 crash the entry survives on at least 1 remaining node — and that node's log will be up-to-date enough to win the next election.

---

## Consistency: How Raft Prevents Gaps

Each AppendEntries carries `prev_log_index` and `prev_log_term` — the index and term of the entry just before the new ones.

A follower only accepts new entries if its log contains an entry at `prev_log_index` with `prev_log_term`. If not, it rejects and the leader backs up one entry and retries.

This ensures logs are always a consistent prefix — no gaps, no out-of-order entries.

---

## The Noop Entry

When a new leader is elected, it doesn't know which entries from the previous term are committed (the previous leader may have died before telling followers). Raft's solution: the new leader immediately appends a **Noop** entry for its own term and replicates it. Once committed, it implicitly commits all preceding entries too.

---

## Safety Guarantee

**Election safety**: at most one leader per term. Guaranteed by the one-vote-per-term rule.

**Log matching**: if two logs have an entry with the same index and term, all preceding entries are identical. Guaranteed by the prev_log consistency check in AppendEntries.

**Leader completeness**: a leader always has all committed entries. Guaranteed by the log up-to-date check in RequestVote — you can only win an election if your log is at least as current as a majority.

**State machine safety**: once a log entry is applied to a state machine at one node, no other node will apply a different command at the same index. Follows from log matching + leader completeness.

---

## Common Interview Questions

**Q: What happens if two nodes become candidates at the same time?**
Both collect votes. If neither gets a majority (split vote), both time out and start a new election with a higher term. Random timeouts make this unlikely to repeat.

**Q: Can a committed entry ever be lost?**
No. A committed entry exists on a majority of nodes. Any future leader must have a log at least as up-to-date as a majority — which means it must have the committed entry.

**Q: Why does Raft need the Noop entry when a new leader is elected?**
A leader can only commit entries from its own term. Entries from previous terms are only indirectly committed when a current-term entry is committed. The Noop forces this to happen immediately.

**Q: What's the difference between commit_index and last_applied?**
`commit_index` — the highest index known to be safely replicated on a majority.
`last_applied` — the highest index actually applied to the state machine.
`last_applied` always lags `commit_index` slightly — entries are committed first, then applied.

**Q: How does Raft handle a network partition?**
If the cluster splits into two groups, the larger group can elect a leader and continue. The smaller group cannot reach majority and stalls. When the partition heals, nodes in the smaller group see a higher term from the majority-side leader and revert to Follower, syncing their logs.

**Q: What is the difference between Raft and Paxos?**
Both solve the same consensus problem. Raft was designed to be understandable — it decomposes the problem into leader election, log replication, and safety, with each piece handled by clear rules. Paxos is often described as harder to implement correctly and teach.
