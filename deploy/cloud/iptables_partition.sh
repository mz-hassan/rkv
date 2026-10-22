#!/usr/bin/env bash
# Network partitions with iptables (Linux only). usage:
#   iptables_partition.sh HOST isolate "IP1 IP2"   drop all traffic between HOST and the listed peers (both directions)
#   iptables_partition.sh HOST heal
source "$(dirname "$0")/lib.sh"
host="$1"; act="$2"
case "$act" in
  isolate) for ip in $3; do R "$host" "iptables -A INPUT -s $ip -j DROP; iptables -A OUTPUT -d $ip -j DROP"; done ;;
  heal)    R "$host" "iptables -F INPUT; iptables -F OUTPUT" ;;
  *) echo "usage"; exit 2 ;;
esac
