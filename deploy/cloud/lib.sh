#!/usr/bin/env bash
# Helpers for the Linux runs.
#   MODE=ssh    (default) hosts are real machines: SSH_USER, SSH_KEY, HOSTS="ip1 ip2 ip3", CLIENT=ip4
#   MODE=netns  "hosts" are Linux network namespaces on THIS machine (see netns.sh): 10.77.0.11..13 = servers, 10.77.0.14 = client.
#               Real kernel netem/iptables/dm-flakey/perf, but all "machines" share CPUs and the disk.
#   DRY_RUN=1   print every command, execute nothing (used by `make dry-run`).
set -euo pipefail
DRY_RUN="${DRY_RUN:-0}"
MODE="${MODE:-ssh}"
SSH_USER="${SSH_USER:-ubuntu}"
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10 ${SSH_KEY:+-i $SSH_KEY}"
ns_of() { echo "rkv-n$(( ${1##*.} - 10 ))"; }          # 10.77.0.11 -> rkv-n1
X() { echo "+ $*" >&2; [ "$DRY_RUN" = 1 ] || "$@"; }
# Run a shell snippet as root on a host. The snippet is passed on stdin / as one argument, so quoting inside it is safe.
# stdout of the REMOTE command is the function's stdout; the "+ ..." trace goes to stderr.
R() { local h="$1"; shift; echo "+ [$h] $*" >&2; [ "$DRY_RUN" = 1 ] && return 0
  if [ "$MODE" = netns ]; then sudo ip netns exec "$(ns_of "$h")" bash -c "$*"
  else printf '%s\n' "$*" | ssh $SSH_OPTS "$SSH_USER@$h" "sudo bash -s"; fi; }
# copy a local file to a host
C() { local h="$1" src="$2" dst="$3"; echo "+ copy $src -> [$h]:$dst" >&2; [ "$DRY_RUN" = 1 ] && return 0
  if [ "$MODE" = netns ]; then cp "$src" "$dst.tmp.$$" && mv -f "$dst.tmp.$$" "$dst"; else scp $SSH_OPTS "$src" "$SSH_USER@$h:$dst"; fi; }
