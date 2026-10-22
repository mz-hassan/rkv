#!/usr/bin/env bash
# Reports which tools are present and prints the install command for what is missing.
set -u
export PATH="/opt/homebrew/opt/rustup/bin:$HOME/.cargo/bin:$PATH"
row() { # name, required|optional, check-cmd, install hint
  if command -v "$3" >/dev/null 2>&1; then printf "  %-14s %-9s ok   (%s)\n" "$1" "$2" "$($3 --version 2>&1 | head -1)"; else printf "  %-14s %-9s MISSING  ->  %s\n" "$1" "$2" "$4"; fi
}
echo "Toolchain"
row rustc required rustc "brew install rustup && rustup-init -y -c clippy -c rustfmt"
row cargo required cargo "(comes with rustup)"
row protoc required protoc "brew install protobuf"
row python3 required python3 "brew install python"
echo "Verification / bench extras"
row cargo-nextest optional cargo-nextest "cargo install cargo-nextest --locked"
row cargo-fuzz optional cargo-fuzz "rustup toolchain install nightly --profile minimal && cargo install cargo-fuzz --locked"
echo "L1 (containers / kubernetes / baselines)"
row docker optional docker "brew install docker docker-compose colima   # then: colima start --cpu 4 --memory 6"
row kind optional kind "brew install kind"
row kubectl optional kubectl "brew install kubectl"
row etcd optional etcd "brew install etcd        # baseline for R1"
row go optional go "brew install go             # only for the Porcupine cross-check"
echo "Plots"
if [ -x .venv/bin/python ] && .venv/bin/python -c "import matplotlib" 2>/dev/null; then echo "  matplotlib     ok (.venv)"; else echo "  matplotlib     MISSING -> python3 -m venv .venv && .venv/bin/pip install matplotlib numpy pyyaml"; fi
echo
echo "Not available on macOS (see docs/DEFERRED_RUNS.md): tc netem, iptables, dm-flakey, perf  -> L2 scripts in deploy/cloud/"
