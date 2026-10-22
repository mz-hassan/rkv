# L2 / L3 runs (Linux hosts, cloud)

These need things a Mac does not have (`tc netem`, `iptables`, `dm-flakey`, `perf`, several machines). Everything is coded and the plan
is validated locally with `DRY_RUN=1` (`make dry-run`); nothing here has been executed against real hosts yet - results for
N1 are `NOT RUN` until you do.

| file | purpose |
|---|---|
| `n1.sh` | orchestrates deploy -> R1 under netem delay/loss -> R2 leader isolation (100 trials) -> dm-flakey disk faults -> flamegraph |
| `netem.sh`, `iptables_partition.sh`, `dmflakey.sh`, `perf_flame.sh` | single-purpose fault/measure wrappers |
| `wrap_windows.py` | converts the R2 raw millisecond list into a `result.json` |
| `terraform/` | 3 servers (+1 client) on AWS, optionally one per AZ |

Cost note: 4 x m6i.xlarge is roughly US$0.8/hour on demand; the whole N1 sequence takes about 4 hours. Destroy afterwards (`terraform destroy`).

Manual quick start on 3 Linux boxes you already have: `HOSTS="a b c" CLIENT=d SSH_USER=you deploy/cloud/n1.sh r1`.
