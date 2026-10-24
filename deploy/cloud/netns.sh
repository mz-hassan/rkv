#!/usr/bin/env bash
# Simulate N machines on one Linux box with network namespaces joined by a bridge. Needs root (sudo).
#   netns.sh up [N=4]      namespaces rkv-n1..rkv-nN at 10.77.0.11..  (n1-n3 servers, n4 client); each has eth0 and its own iptables/tc state
#   netns.sh down
#   netns.sh ping          connectivity check between all pairs
set -euo pipefail
cmd="${1:-}"; N="${2:-4}"
case "$cmd" in
  up)
    sudo ip link add br-rkv type bridge 2>/dev/null || true
    sudo ip addr add 10.77.0.1/24 dev br-rkv 2>/dev/null || true
    sudo ip link set br-rkv up
    for i in $(seq 1 "$N"); do
      ns="rkv-n$i"; ip="10.77.0.$((10+i))"
      sudo ip netns add "$ns" 2>/dev/null || true
      if ! sudo ip netns exec "$ns" ip link show eth0 >/dev/null 2>&1; then
        sudo ip link add "vh$i" type veth peer name "vn$i"
        sudo ip link set "vn$i" netns "$ns"
        sudo ip netns exec "$ns" ip link set "vn$i" name eth0
        sudo ip link set "vh$i" master br-rkv; sudo ip link set "vh$i" up
      fi
      sudo ip netns exec "$ns" ip addr replace "$ip/24" dev eth0
      sudo ip netns exec "$ns" ip link set eth0 up
      sudo ip netns exec "$ns" ip link set lo up
      sudo ip netns exec "$ns" ip route replace default via 10.77.0.1
    done
    echo "namespaces up: $(ip netns list | tr '\n' ' ')" ;;
  down)
    for i in $(seq 1 "$N"); do sudo ip netns del "rkv-n$i" 2>/dev/null || true; sudo ip link del "vh$i" 2>/dev/null || true; done
    sudo ip link del br-rkv 2>/dev/null || true; echo "namespaces removed" ;;
  ping)
    for i in $(seq 1 "$N"); do for j in $(seq 1 "$N"); do [ "$i" = "$j" ] && continue
      sudo ip netns exec "rkv-n$i" ping -c1 -W1 "10.77.0.$((10+j))" >/dev/null 2>&1 && echo "n$i -> n$j ok" || { echo "n$i -> n$j FAILED"; exit 1; }
    done; done ;;
  *) echo "usage: $0 up|down|ping [N]"; exit 2 ;;
esac
