#!/usr/bin/env bash
# Disk fault injection with device-mapper "flakey" (Linux only, root). The data directory lives on the flaky device.
#   dmflakey.sh HOST up  LOOPFILE SIZE_MB UP_SEC DOWN_SEC   create loop device + flakey target mounted at /mnt/flaky
#   dmflakey.sh HOST drop                                  switch to error mode (all IO fails) until `restore`
#   dmflakey.sh HOST restore | down
source "$(dirname "$0")/lib.sh"
host="$1"; act="$2"
case "$act" in
  up) f="$3"; mb="$4"; up="$5"; down="$6"
      R "$host" "truncate -s ${mb}M $f && L=\$(losetup -f --show $f) && S=\$(blockdev --getsz \$L) && echo \"0 \$S flakey \$L 0 $up $down\" | dmsetup create flaky && mkfs.ext4 -q /dev/mapper/flaky && mkdir -p /mnt/flaky && mount /dev/mapper/flaky /mnt/flaky" ;;
  drop)    R "$host" "dmsuspend=1; S=\$(blockdev --getsz /dev/mapper/flaky); L=\$(dmsetup table flaky | awk '{print \$4}'); dmsetup suspend flaky && echo \"0 \$S error\" | dmsetup load flaky && dmsetup resume flaky" ;;
  restore) R "$host" "dmsetup suspend flaky; dmsetup reload flaky --table \"\$(dmsetup table flaky | sed 's/error/flakey/')\" 2>/dev/null || true; dmsetup resume flaky" ;;
  down)    R "$host" "umount /mnt/flaky; dmsetup remove flaky; losetup -D" ;;
  *) echo "usage"; exit 2 ;;
esac
