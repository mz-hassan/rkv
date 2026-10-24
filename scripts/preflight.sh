#!/usr/bin/env bash
# Is this machine quiet enough for benchmarking? Exit 0 = go, 1 = no-go (prints why). macOS and Linux.
# Override thresholds: MAX_SWAP_MB=1024 MAX_LOAD=1.5 MIN_FREE_GB=4 ALLOW_BATTERY=1
set -u
fail=0; warn() { echo "  [!] $*"; fail=1; }; ok() { echo "  [ok] $*"; }
MAX_SWAP_MB=${MAX_SWAP_MB:-1024}; MAX_LOAD=${MAX_LOAD:-1.5}; MIN_FREE_GB=${MIN_FREE_GB:-4}
if [ "$(uname)" = Darwin ]; then
  ps_=$(vm_stat | awk '/page size/{print $8}'); free=$(vm_stat | awk -v p=$ps_ '/Pages free/{f=$3} /Pages inactive/{i=$3} /Pages speculative/{s=$3} END{printf "%.1f",(f+i+s)*p/1e9}')
  swap=$(sysctl -n vm.swapusage | awk '{gsub("M","",$6); printf "%d",$6}')
  comp=$(vm_stat | awk -v p=$ps_ '/occupied by compressor/{printf "%.1f",$5*p/1e9}')
  if pmset -g batt | grep -q "AC Power"; then ok "on AC power"; elif [ "${ALLOW_BATTERY:-0}" = 1 ]; then ok "on battery (allowed)"; else warn "running on battery: plug in the charger"; fi
  pmset -g | grep -q "lowpowermode *1" && warn "Low Power Mode is on (System Settings > Battery)"
  heavy=$(ps -axo pcpu,comm | awk '$1>15 && $2 !~ /(rkv|cargo|rustc)/ {print $1"% "$2}' | head -3)
else
  free=$(awk '/MemAvailable/{printf "%.1f",$2/1048576}' /proc/meminfo); swap=$(awk '/SwapTotal/{t=$2} /SwapFree/{f=$2} END{printf "%d",(t-f)/1024}' /proc/meminfo); comp=0
  heavy=$(ps -eo pcpu,comm --sort=-pcpu | awk 'NR>1 && $1>15 {print $1"% "$2}' | head -3)
fi
load=$(uptime | sed 's/.*load averages*: *//' | awk '{gsub(",","",$1); print $1}')
echo "preflight on $(uname -sm): free+reclaimable ${free} GB, swap used ${swap} MB, compressed ${comp} GB, 1-min load ${load}"
awk -v f="$free" -v m="$MIN_FREE_GB" 'BEGIN{exit !(f+0>=m+0)}' && ok "enough free memory" || warn "less than ${MIN_FREE_GB} GB free: quit apps (or reboot)"
[ "$swap" -le "$MAX_SWAP_MB" ] && ok "swap use low" || warn "swap in use (${swap} MB > ${MAX_SWAP_MB} MB): memory pressure will distort latency; reboot recommended"
awk -v l="$load" -v m="$MAX_LOAD" 'BEGIN{exit !(l+0<=m+0)}' && ok "machine is idle" || warn "load ${load} > ${MAX_LOAD}: something is using the CPU"
[ -n "${heavy:-}" ] && warn "busy processes: $heavy"
if [ "$fail" = 0 ]; then echo "GO: quiet enough to benchmark"; else echo "NO-GO: fix the [!] items above, then re-run scripts/preflight.sh"; fi
exit $fail
