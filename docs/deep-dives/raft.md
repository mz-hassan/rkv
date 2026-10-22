# Raft deep dive (`crates/raft`, `raft-driver`, `raft-storage`)

## Sans-IO contract
`Raft` is a state machine with no clock, thread, socket or disk. Inputs: `tick()`, `step(msg)`, `propose`, `read_index`,
`transfer_leader`, `propose_conf_change`, `compact`. Output: `ready()` -> `Ready{hard_state, truncate_from, entries,
install_snapshot, save_snapshot, messages, committed, read_states}`. The driver must **persist, then send, then apply, then `advance`**.
* persist-before-send makes votes and AppendEntries acks safe (a node never promises something it could forget);
* the leader counts its own log toward a quorum only after `advance` reports it persisted;
* committed entries are handed out in the same `Ready` as the entries they follow, so the driver persists them before applying.
The same `Raft` runs in the simulator (`sim`, virtual time, `MemFs`) and in production (`raft-driver`, tokio, `OsFs`).

## Elections
Randomised timeout in [T, 2T) ticks. **PreVote**: a node first asks "would you vote for me at term+1?" without bumping its own
term, so a partitioned node cannot force the cluster into a new term when it returns. **CheckQuorum**: a leader steps down if it
has not heard from a quorum within an election timeout. **Leader stickiness**: a node that heard from a leader within the timeout
ignores (does not even answer) vote requests, unless the request is flagged `transfer`. A leader never grants votes in its own term.

## Replication
Per-follower `Probe -> Replicate -> Snapshot` progress. Probe sends one message and waits (rejection carries a hint index to skip
back over a whole conflicting term). Replicate pipelines up to `max_inflight` messages of up to `max_msg_entries` entries; if
acks stop (lost messages) the leader falls back to Probe after a few heartbeats. A leader only commits an entry of **its own term**
by counting replicas (Raft paper section 5.4.2 / Figure 8); older entries commit transitively.

## Membership (joint consensus)
A change is an `EnterJoint{voters, learners}` entry (config `C_old,new`: decisions need a majority of **both** sets). Configs take
effect when appended, not when committed. When the entering entry commits, the leader appends `LeaveJoint` automatically. A change
that only touches learners uses a direct `ConfSet`. A leader removed from the voter set steps down once the new config commits.
New nodes join as **learners** (replicate, never vote) and are promoted by a second change (PD promotes when they are caught up).

## Reads
* **ReadIndex**: leader records `commit`, broadcasts a heartbeat carrying a sequence number, and answers once a quorum echoes it; the
  caller then waits until `applied >= index`. Requires an entry committed in the current term first.
* **Lease reads**: valid while a quorum acked within `election_tick/2` of the leader's own ticks; sound only with bounded clock drift.
* **Follower reads**: served locally if the last leader message was recent (not linearizable).

## Snapshots and log compaction
The state machine hands the node a snapshot at index i (`compact`); the node drops the log through i and keeps the snapshot to send
to followers whose `next` falls below the log. `raft-storage` writes the snapshot file (tmp+rename, CRC) **before** the WAL record that
references it; the WAL is rewritten when it exceeds live data by 4 MiB.

## Where the safety arguments are checked
`crates/invariants`: election safety, leader append-only, log matching, leader completeness, state-machine safety, commit
monotonicity - asserted on every step in the simulator. End-to-end: linearizability of every recorded client history.
