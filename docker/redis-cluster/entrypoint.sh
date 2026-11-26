#!/bin/sh
# Start a single-node Redis Cluster for the test suite.
#
# One node that owns every hash slot is enough: the client-side checks the
# result backend has to satisfy (CROSSSLOT, no pipelined PUBLISH, no shared
# connection pool) are enforced by redis-py's RedisCluster client and by
# cluster mode itself, regardless of how many nodes there are.
#
# A fresh node is a cluster with no slots, so once the server accepts
# connections, claim all 16384 slots in the background. Any extra arguments
# are passed on to redis-server.
set -eu

port=${REDIS_CLUSTER_PORT:-7000}

(
  until redis-cli -p "$port" ping 2>/dev/null | grep -q PONG; do
    sleep 0.2
  done
  if ! redis-cli -p "$port" cluster info | grep -q '^cluster_slots_assigned:16384'; then
    redis-cli -p "$port" cluster addslotsrange 0 16383
  fi
) &

exec redis-server \
  --port "$port" \
  --cluster-enabled yes \
  --cluster-node-timeout 5000 \
  --appendonly no \
  --save '' \
  "$@"
