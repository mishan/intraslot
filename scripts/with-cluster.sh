#!/usr/bin/env bash
# Bring up a throwaway 3-node Redis Cluster on 7001-7003, run "$@", tear it down.
#   scripts/with-cluster.sh node --test test/cluster.test.js
# Requires redis-server >= 7 on PATH.
set -e
BASE="${INTRASLOT_CLUSTER_DIR:-/tmp/intraslot-cluster}"
PORTS=(7001 7002 7003)

cleanup() {
  for p in "${PORTS[@]}"; do redis-cli -p "$p" shutdown nosave 2>/dev/null || true; done
}
trap cleanup EXIT

rm -rf "$BASE"; mkdir -p "$BASE"
for p in "${PORTS[@]}"; do
  mkdir -p "$BASE/$p"
  cat > "$BASE/$p/redis.conf" <<EOF
port $p
cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 5000
appendonly no
save ""
dir $BASE/$p
daemonize yes
logfile $BASE/$p/redis.log
protected-mode no
EOF
  redis-server "$BASE/$p/redis.conf"
done
sleep 1

redis-cli --cluster create \
  127.0.0.1:7001 127.0.0.1:7002 127.0.0.1:7003 \
  --cluster-yes >/dev/null 2>&1

# Wait for cluster_state:ok rather than sleeping blindly.
for _ in $(seq 1 30); do
  state=$(redis-cli -p 7001 cluster info 2>/dev/null | grep -c '^cluster_state:ok' || true)
  [ "$state" = "1" ] && break
  sleep 0.5
done
[ "$state" = "1" ] || { echo "cluster failed to form" >&2; exit 1; }

"$@"
