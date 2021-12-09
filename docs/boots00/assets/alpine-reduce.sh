# the root is alpine
set -e
mkdir out
cd out
for i in /*; do
  case "$i" in
    (/out|/home|/tmp|/yzix*|/mnt) ;;
    (*) ln -sT "$ROOTFS$i" ".$i" ;;
  esac
done
