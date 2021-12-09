[ $# -eq 2 ] || exit 1

set -e
gzip -cd "$1" > /dat.tar
mkdir /out
cd /out
tar -C /out -xvf /dat.tar

shift
[ $# -eq 0 ] && exit 0

ls -las

for i in *; do
  found=false
  for j; do
    if [ "$i" = "$j" ]; then
      found=true
      break
    fi
  done
  "$found" || rm -rf "$i"
done
