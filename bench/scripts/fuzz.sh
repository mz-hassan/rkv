#!/usr/bin/env bash
# Fuzz every decoder. SECS per target (default 600). Needs: rustup toolchain install nightly; cargo install cargo-fuzz.
source "$(dirname "$0")/lib.sh"; banner fuzz
SECS="${SECS:-$(pick 5 600)}"
cd fuzz
for t in wal_reader sstable_reader manifest_decoder raft_message repl_command proto_requests; do
  run cargo +nightly fuzz run $t -- -max_total_time=$SECS -timeout=10 -rss_limit_mb=4096
done
