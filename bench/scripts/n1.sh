#!/usr/bin/env bash
# N1 (L2, DEFERRED): real-network runs on Linux hosts. See deploy/cloud/README.md. HOSTS="h1 h2 h3". DRY_RUN=1 validates the plan locally.
source "$(dirname "$0")/lib.sh"; banner N1
exec deploy/cloud/n1.sh "$@"
