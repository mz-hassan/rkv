# BUGS FOUND

Every entry says **how it was found**, because that is what makes it credible. "Found by review" and "found by compiler" are
listed honestly; they are not claimed as testing wins. Keep this file: it is resume and interview material
(`make resume` counts the `## BUG-` headings).

Not listed: bugs in the *test harness* that were not product bugs (for example, an in-process "crash" that left HTTP/2
connections alive; fixed by running each test node on its own runtime and dropping it).

## BUG-001: compaction resurrects deleted keys (tombstone dropped too early)
- **Found by:** model-based property test (`kv-engine/tests/model.rs`, proptest vs `BTreeMap`, leveled style), failing in a
  release-mode run; proptest shrank it to a short operation sequence saved in `tests/model.proptest-regressions`.
- **Symptom:** a scan returned a key (`key013`) that had been deleted; values of all other keys correct.
- **Root cause:** when compacting L0 -> L1 the engine only drops a tombstone if no *deeper* level overlaps the output. It
  checked overlap using the key range of the **input** files, but the overlapping L1 files pulled into the job can be wider than
  the inputs, so the output covered keys whose older versions still lived in L2. Dropping the tombstone un-hid them.
- **Fix:** `is_bottom_for` computes the range over inputs **and** next-level files (`db.rs`).
- **Regression test:** the saved proptest case is replayed on every run; 300,000 cases per compaction style pass after the fix.
- **Why it matters:** this is the classic LSM "zombie data" bug; it shows the model test catches data-loss bugs that unit
  tests of the happy path cannot.

## BUG-002: Percolator prewrite reports a false write conflict
- **Found by:** code review while writing the transaction unit tests (before the test existed).
- **Symptom (by reasoning):** `prewrite` treated *any* newer write record, including another transaction's rollback tombstone or a
  lock-only record, as a conflict, aborting transactions that had nothing to do with each other.
- **Fix:** only committed Put/Delete records at or after the start timestamp conflict.
- **Regression test:** `other_txns_rollback_tombstone_is_not_a_conflict`.

## BUG-003: PD garbage-collected freshly created learner replicas
- **Found by:** the PD-driven integration test (`pd/tests/cluster.rs`) stalling at 2 of 3 replicas; debug output showed the
  learner store had created its replica and then lost it.
- **Root cause:** a new learner reports its replica to PD *before* the range leader's report lists it as a peer; the replica-GC
  rule ("store reports a replica the leader does not list") therefore told it to self-destruct.
- **Fix:** a replica must stay unlisted for 30 s before PD asks the store to destroy it.

## BUG-004: PD scheduler waited a full 20 s after every finished operation
- **Found by:** the same integration test: each add-replica and promote step took ~20 s.
- **Root cause:** the per-range "operation pending" flag was only cleared by a timeout.
- **Fix:** an operation is considered finished as soon as the range's peer set or version changes.

## BUG-005: linearizability checker could use unbounded memory
- **Found by:** mutation testing (`--bug vote-ignores-log`): the checker's memo table, bounded only by *search nodes*, grew to
  ~5 GB RSS on a corrupted history, heating the author's laptop.
- **Fix:** smaller node budget (200k per key) so a bad history ends as `Inconclusive` instead of exhausting memory.

## BUG-006: simulator livelock and message storm when the protocol is already broken
- **Found by:** mutation testing, seed 77 of `--bug vote-ignores-log`: the run never finished.
- **Root cause (two parts):** (1) `Raft::has_ready()` stayed true when `commit > applied` but the entry was not in the log (a
  state a correct Raft never reaches), so the driver loop spun; (2) with divergent committed logs the leader and a follower
  ping-ponged "already committed that far" / reject forever, with duplicated messages multiplying the chains.
- **Fix:** `has_ready` only reports committed entries that exist; the simulator fails fast on the first violation and flags a
  `message storm` above 500k queued events instead of running forever.

## BUG-007: SSTable reader trusted lengths read from disk
- **Found by:** review while thinking about what fuzzing against the in-memory filesystem *cannot* see (it clamps reads; a
  real file allocates the requested length up front).
- **Symptom (by reasoning):** a corrupt footer claiming a huge block length would attempt a giant allocation.
- **Fix:** every handle must lie inside the file; short or out-of-bounds index entries are rejected.
- **Regression test:** `corrupt_footer_lengths_are_rejected_without_huge_allocation`.

## BUG-008: PD client held a mutex guard across `await` (and re-locked it inside the loop)
- **Found by:** the compiler (`future cannot be sent between threads safely`); it would also have deadlocked on leader redirect.
- **Fix:** read the address count into a local before the retry loop.

## BUG-010: a leader that lost leadership left its pending client requests hanging
- **Found by:** end-to-end fault-injection test on a Linux VM: leader isolated with `iptables` (kernel-level), client-observed
  failover window measured. Every trial came out at ~5.0 s instead of ~0.3 s, i.e. the server-side proposal timeout.
  The earlier local failover test (`kill -9`) could not show it: killing a process drops its connections, a partition does not.
- **Root cause:** `raft-driver` decided "did I just lose leadership?" at the *start* of `drive()`, but a step-down caused by
  CheckQuorum or a higher-term message happens in `tick()`/`step()` *before* `drive()` runs, so by then the node already looked like a
  follower and its pending proposals and ReadIndex requests were never failed. Clients waited for their timeout on a node that
  could never commit.
- **Fix:** remember the role seen at the end of the previous `drive()` (`was_leader`) and fail pending work when it changes.
- **Regression test:** `isolated_leader_fails_pending_proposals_fast` (fails on the old code, passes on the fix; verified both ways).
- **Effect on the measurement:** isolated-leader failover ~5000 ms -> ~280 ms (smoke figure, 5 trials; the real R2/N1 run is separate).

## BUG-011: disk-fault injection silently injected nothing (tooling)
- **Found by:** reading the smoke output critically: the `dm-flakey` run reported 0 client errors and the faulty node's log showed
  no failure. Probing the device directly showed `/mnt/flaky` was not mounted.
- **Root cause:** in the simulated-machines mode every "machine" command runs under `ip netns exec`, which uses a *private mount
  namespace*: the mount made there vanished when the command ended, so the data directory was on the VM's ordinary disk.
- **Fix:** device-mapper/mount setup runs on the real host. After the fix a lone server on the flaky device fail-stops
  (`raft driver fatal storage error`), and in the 3-node run the faulty node panicked with `wal write failed: Input/output error`
  while the other two kept serving (0 client errors).
- **Why it matters:** a fault-injection test that injects nothing looks like a pass. Always verify the fault actually fires.

## BUG-012: benchmark and cloud-script fragility found by real runs (tooling)
- The benchmark load phase panicked on the first error, so a cluster mid-election aborted the run; it now retries.
- A 50 ms delay + 1% loss run failed because the election timeout (200-400 ms) was shorter than a few round trips, so the leader
  kept losing its quorum: correct behaviour for badly tuned timeouts. The script now scales the election timeout with the
  injected delay and records it in the result label.
- `R()` quoted commands with single quotes (breaks on `awk '...'`); the R2 script isolated node 1 instead of the leader; deploy
  overwrote a running binary ("Text file busy") and could not stop root-owned servers; `pkill -f` matched its own shell
  (twice); a `sed` rename made a helper function call itself (infinite recursion, only reachable in ssh mode, caught by dry-run).
- **Found by:** running every step against a real Linux machine at smoke scale before any long run.

## BUG-013: the Kubernetes operator crashed at startup (Rustls crypto provider)
- **Found by:** first run of the operator against a real cluster (kind on an Azure VM): `CrashLoopBackOff`, panic "Could not
  automatically determine the process-level CryptoProvider from Rustls crate features".
- **Fix:** install the `ring` provider explicitly at startup. A dry run and a successful build could never show this.

## BUG-014: the operator's server-side-apply patches erased the StatefulSet's container spec and made it flap
- **Found by:** the same first real run: the StatefulSet generation climbed ~20/s, pods were recreated in a loop, a controller
  revision showed a template with **no image**, and the CR status read `image: ""`.
- **Root cause:** the operator used Kubernetes *server-side apply* with partial bodies. SSA gives the applying manager ownership of
  everything it sends and **deletes owned fields that a later apply omits**, so patches that carried only `replicas`/`partition`
  removed the image (and, once ownership migrated from `kubectl apply`, much more). The two branches of the upgrade logic then
  undid each other.
- **Fix:** adjust only `replicas`, the update `partition` and the image with strategic-merge patches (no ownership semantics), and
  write CR status only when it actually changed (our own status write re-triggers reconciliation).
- **Note:** diagnosis was delayed ~1 h by BUG-017 (the cluster kept running the old operator).

## BUG-015: the operator reported `Ready` before the new pods or stores were healthy
- **Found by:** the K1 script's scale-out wait returning after 0 s. **Fix:** `Ready` now requires every pod ready and every store
  alive in PD and ranges healthy; otherwise phase `Stabilizing`.

## BUG-016: an empty store could never become Ready, so scale-out was impossible on Kubernetes
- **Found by:** K1 scale-out: the two new store pods stayed `0/1` for 10 minutes; PD showed them alive with 0 ranges.
- **Root cause:** `/readyz` required "some range has a known leader". A freshly added store hosts no ranges until PD schedules
  replicas onto it, so its readiness probe failed forever.
- **Fix:** ready = some range has a leader, OR the store hosts no ranges and its last PD heartbeat succeeded. After the fix: 5/5
  pods ready in 8 s.

## BUG-017: the cluster ran stale code because of Docker's legacy build cache (tooling)
- **Found by:** noticing that the image ID never changed across "fixes", then comparing the checksum of the binary inside the image
  with the one on disk. With the legacy builder (no buildx) a `COPY` of a rebuilt binary silently reused an old layer.
- **Fix:** `docker build --no-cache` for prebuilt images in `k1.sh`.
- **Lesson:** verify the artifact that is actually deployed, not the one that was built.

## BUG-018: PD's replica balancer undid store draining, so scale-in never finished
- **Found by:** K1 scale-in: "draining store 5: 2 ranges left" for 9 minutes, with learners repeatedly appearing on store 5.
- **Root cause:** the balance rule chose the "least loaded" store from all stores that hold replicas, including the store marked
  `evict_store`, and kept adding replicas back onto it while other rules removed them.
- **Fix:** only stores that may receive replicas take part in balancing.

## BUG-019: `kvctl presplit` reused a stale range after the first split (tooling)
- **Found by:** K1: "could not split at bank00000050". A split is visible to PD only after the next heartbeat and the new child
  must elect a leader; the retry loop re-sent the old range id forever. **Fix:** re-resolve the range on every attempt.

## BUG-020: the SIGKILL crash-consistency test reported 13,675 false "lost writes" (test harness)
- **Found by:** the overnight run: `S2-sigkill` exited non-zero. Diagnostics showed one impossible id (93369337) reappearing in
  every iteration, always in the *test's ledger*, never a missing database key.
- **Root cause:** `writeln!` on an unbuffered file makes two `write` syscalls (digits, then newline). A `kill -9` between them
  left "9336" with no newline and the next append glued "9337\n" onto it. No data was lost.
- **Fix:** one write per ledger line; ignore a torn final line. The invalid result set is kept under `bench/invalid/` for
  transparency; the corrected re-run is a separate result. 40 kill/recover cycles after the fix: 534,207 acknowledged keys, 0 violations.

## BUG-021: MVCC GC deleted the only committed value under a rollback marker (data loss)
- **Found by:** K1 (rolling upgrade + scale under bank load): the final audit could not read accounts 32, 41, 42 for 60 s; a
  transactional read from a fresh client confirmed they were gone from every replica. Neighbouring accounts were intact.
- **Root cause:** `gc_key` treated the first write record at or below the safe point as "the visible version" even when it was a
  Rollback/Lock marker (a conflicting transaction that gave up). The real Put below it was then deleted as "older".
- **Fix:** only a Put counts as the retained visible version; Rollback/Lock markers are dropped; a Delete drops itself and
  everything older. Regression test `gc_keeps_value_below_rollback_marker`.
- **Why the faster tests missed it:** GC only runs every ~60 s with a 10-minute retention window, so only the 25-minute K1 run
  reached it.

## BUG-022: a store could never rejoin a range it had been removed from
- **Found by:** K1 scale-in stalling for 15 min with the range stuck on "4 voters + learner on store 3".
- **Root cause:** destroying a replica wrote a permanent tombstone and the raft ingress dropped every message for tombstoned
  ranges, so PD adding the store back as a learner (balancing, then draining) never created the replica.
- **Fix:** removal tombstones are marked revivable (persisted); a message from a peer whose current descriptor lists this store
  re-creates the replica (throttled). Merged-away ranges stay permanently tombstoned.

## BUG-023: PD blocked on learners placed on dead or draining stores
- **Found by:** K1 again, draining the second store: PD added a learner to a store whose pod had just been removed and then
  waited for it forever (the scheduler skips ranges that have learners).
- **Fix:** learners on non-alive stores are removed first. Also: the bank benchmark's final audit now retries for 60 s and reports
  "could not read" separately from a conservation violation (the old message "total 0" was misleading).

## BUG-024: destroying a replica left its metadata behind, so a re-added replica started from a stale state
- **Found by:** K1 scale-in (third stall): a revived replica reported `commit=0 applied=141 bytes=0 peers=[2,3,4,5]` and leadership
  transfer to it never completed.
- **Root cause:** `destroy_replica` deleted the range's data but not its per-replica meta keys (applied index, descriptor, frozen
  marker, client sessions, bootstrap membership). Once BUG-022 allowed a store to rejoin, the new replica believed it had applied
  entries whose data was gone.
- **Fix:** destroy removes all of those keys as well.

## BUG-025: a replica being re-added destroyed itself while replaying history
- **Found by:** K1 scale-in (fourth stall): the learner on the re-added store sat at `commit=0 applied=0` and the same range's
  replica on that store was destroyed three times in 8 minutes.
- **Root cause:** a fresh replica replays the group's log from index 1. That history contains the old config entry that removed
  this same store, so for a moment the replica saw itself outside the group while caught up, and the "removed from group" rule
  tore it down, restarting the cycle.
- **Fix:** with a PD present, replicas no longer destroy themselves on a local view of the log; PD's replica GC (which sees the
  leader's authoritative peer list) decides. A first attempt (require the removed state to hold for ~10 s) was not enough: a
  slow catch-up can sit in that state longer, found by the local churn test. Without PD the old rule stays, with the 10 s debounce.

## BUG-026: PD replica GC destroyed freshly added learners using a stale timer
- **Found by:** the local churn test (`bench/scripts/churn_local.sh`), cycle 2: a learner replica was destroyed right after being
  created and the range never finished moving.
- **Root cause:** PD records when a store first reported a replica that the leader does not list ("unlisted"), and only clears
  that when the store is listed. If the replica disappears and the same range is later added again, the new learner inherited the
  old timer, so the 30 s grace period looked expired and PD ordered it destroyed on its first report.
- **Fix:** forget the timer for any replica the store stops reporting.

## BUG-027: short scans copied the whole memtable (performance)
- **Found by:** the first Azure S1 run: YCSB workloads A to D took ~10 s each, workload E (95% short scans) ran for 18+ minutes at
  full CPU and had to be killed.
- **Root cause:** `Memtable::range` copies every matching entry into a Vec, and `scan_at` called it with no upper bound for every
  scan, so each 1-to-100-key scan cost O(memtable). `has_tombstone_for` did the same just to peek at one entry.
- **Fix:** copy only a bounded prefix of each memtable (`range_n`), and redo the scan with a larger bound if the merge reaches the
  point where a truncated memtable stops being complete. Regression test `limited_scan_matches_unbounded_scan` compares
  limited and unlimited scans over keys with five versions each plus deletes.
- **Why tests missed it:** correctness was fine; only a million-entry memtable made it visible.

## BUG-028: benchmark clusters were never wiped between runs (benchmark integrity)
- **Found by:** the first Azure R1 run: a single-node server grew to 14 GB of memory on a 1.6 GB dataset, and `/tmp/rkv-cluster`
  held 3.6 GB. The server was replaying earlier runs' logs.
- **Root cause:** `scripts/cluster.sh up` stopped the old processes but reused the old data directories, so every benchmark in a
  script started on top of the state left by the previous one.
- **Fix:** `up` now starts from empty data directories (`RKV_KEEP_DATA=1` opts out). Cluster results collected before the fix
  were discarded and rerun.
- **Why it matters:** it would have produced slower, noisier and unexplainable cluster numbers without any error.

## BUG-029: benchmark harness race between stopping and starting a cluster
- **Found by:** Azure R1 run stopped with "cluster not ready"; the new server's log said `Address already in use`.
- **Root cause:** `cluster.sh down` sent `kill -9` and returned immediately, and the next `up` started before the old process had
  exited and released its ports.
- **Fix:** `down` waits for the processes to disappear and the ports to free up. The etcd baseline script had the same flaw (its
  5-member cluster failed to start next to the lingering 3-member one and the benchmark client hung); fixed the same way. The
  affected runs were repeated.

## BUG-009b: "non-strict" fsync on macOS was actually a full drive-cache flush
- **Found by:** a sanity check of the RocksDB baseline: with `sync=true` it reported ~109k ops/s against ~0.7k for rkv with the same
  nominal durability. The gap was too big to be an engine difference.
- **Root cause:** Rust's `File::sync_data/sync_all` call `fcntl(F_FULLFSYNC)` on Apple platforms, so the default mode already paid for
  the strictest flush, and `--strict-fsync` meant nothing. Any rkv-vs-RocksDB durability comparison would have been unfair to rkv.
- **Fix:** `OsFile::sync` calls `fsync(2)` (default) or `F_FULLFSYNC` (strict) explicitly; the headline S1 comparison now disables
  fsync on both sides and the fsync runs are labelled separately. Also fixed: the RocksDB wrapper built its `WriteOptions` but
  never applied them (it silently ran without fsync).
- **Why it matters:** a benchmark-integrity bug, the kind that produces an impressive-looking but false resume number.

## BUG-009: benchmark/tooling scripts
- `cluster.sh up` exited non-zero in static mode (last command was a failing `[ ] && echo` under `set -e`), silently aborting R1.
- `kvctl presplit` split a child range before it had elected a leader; now retries.
- S1 smoke run was fsync-bound at ~400 loads/s: bulk load now uses batched writes (one fsync per batch).
- **Found by:** running every script at smoke scale.

## Verification-suite coverage gaps (honest)

These are not bugs in the product; they are bugs the suite did **not** find when deliberately injected (mutation testing of
the verification itself, `make sim`):

| injected bug | result |
|---|---|
| vote without the log up-to-date check, commit on a minority (5 voters), vote not persisted, stale follower reads, ack before commit, no request dedup | **caught** (see RESULTS.md after `make sim`) |
| committing old-term entries by counting replicas (Raft paper Figure 8) | **NOT caught** in 6,000 five-voter seeds: the fault schedules do not reliably produce the required interleaving. Next step: a targeted scenario. |
| skipping the "never truncate committed entries" assertion | equivalent mutant: it only matters once another safety bug exists, so it cannot be caught on its own |
