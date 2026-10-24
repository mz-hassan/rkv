#!/usr/bin/env bash
# Disk fault injection with device-mapper "flakey" (Linux only, root). The data directory lives on the flaky device.
#   dmflakey.sh HOST up  LOOPFILE SIZE_MB UP_SEC DOWN_SEC   create loop device + flakey target mounted at /mnt/flaky
#   dmflakey.sh HOST drop                                  switch to error mode (all IO fails) until `restore`
#   dmflakey.sh HOST restore | down
source "$(dirname "$0")/lib.sh"
host="$1"; act="$2"
# Device-mapper and mounts are host-wide. In MODE=netns a "host" is only a network namespace, and `ip netns exec` gives
# commands a PRIVATE mount namespace (a mount made there vanishes), so disk setup must run on the real host.
H() { if [ "$MODE" = netns ]; then echo "+ [host] $*" >&2; [ "$DRY_RUN" = 1 ] || sudo bash -c "$*"; else H "$*"; fi; }
case "$act" in
  up) f="$3"; mb="$4"; up="$5"; down="$6"
      H "umount /mnt/flaky 2>/dev/null || true; dmsetup remove flaky 2>/dev/null || true; losetup -j $f 2>/dev/null | cut -d: -f1 | xargs -r losetup -d; rm -f $f"   # idempotent: clean up any previous run
      H "truncate -s ${mb}M $f && L=\$(losetup -f --show $f) && S=\$(blockdev --getsz \$L) && echo \"0 \$S flakey \$L 0 $up $down\" | dmsetup create flaky && mkfs.ext4 -q /dev/mapper/flaky && mkdir -p /mnt/flaky && mount /dev/mapper/flaky /mnt/flaky" ;;
  drop)    H "dmsuspend=1; S=\$(blockdev --getsz /dev/mapper/flaky); L=\$(dmsetup table flaky | awk '{print \$4}'); dmsetup suspend flaky && echo \"0 \$S error\" | dmsetup load flaky && dmsetup resume flaky" ;;
  restore) H "dmsetup suspend flaky; dmsetup reload flaky --table \"\$(dmsetup table flaky | sed 's/error/flakey/')\" 2>/dev/null || true; dmsetup resume flaky" ;;
  down)    H "umount /mnt/flaky; dmsetup remove flaky; losetup -D" ;;
  *) echo "usage"; exit 2 ;;
esac
