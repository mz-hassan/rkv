#!/usr/bin/env bash
# Helpers for the Linux/cloud runs. DRY_RUN=1 prints every command and executes nothing (used by `make dry-run`
# so the plan is validated on a Mac). On real hosts: SSH_USER, SSH_KEY, HOSTS="ip1 ip2 ip3", CLIENT=ip4.
set -euo pipefail
DRY_RUN="${DRY_RUN:-0}"
SSH_USER="${SSH_USER:-ubuntu}"
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10 ${SSH_KEY:+-i $SSH_KEY}"
X() { echo "+ $*"; [ "$DRY_RUN" = 1 ] || "$@"; }
# run a command on a host (as root)
R() { local h="$1"; shift; echo "+ [$h] $*"; [ "$DRY_RUN" = 1 ] || ssh $SSH_OPTS "$SSH_USER@$h" "sudo bash -c '$*'"; }
C() { local h="$1" src="$2" dst="$3"; echo "+ scp $src -> [$h]:$dst"; [ "$DRY_RUN" = 1 ] || scp $SSH_OPTS "$src" "$SSH_USER@$h:$dst"; }
