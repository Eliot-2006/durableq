#!/usr/bin/env bash
# POST N demo.effect jobs through the API, wait for the queue to drain, run `durableq check` in-cluster.
set -euo pipefail

CTX=kind-durableq
NS=durableq
N=${N:-200}
TIMEOUT=${TIMEOUT:-300}
PORT=${PORT:-18080}
k() { kubectl --context "$CTX" -n "$NS" "$@"; }

k port-forward svc/durableq-api "$PORT:8080" >/dev/null &
PF=$!
trap 'kill $PF 2>/dev/null || true' EXIT
for _ in $(seq 30); do curl -fsS "localhost:$PORT/healthz" >/dev/null 2>&1 && break; sleep 1; done

echo "enqueueing $N jobs"
seq "$N" | xargs -P 16 -I{} curl -fsS -o /dev/null -XPOST "localhost:$PORT/v1/jobs" \
  -d '{"kind":"demo.effect","payload":{"n":{}}}'

echo "waiting for drain (timeout ${TIMEOUT}s)"
deadline=$((SECONDS + TIMEOUT))
while :; do
  s=$(curl -fsS "localhost:$PORT/v1/stats")
  open=$(echo "$s" | sed -E 's/.*"pending":([0-9]+).*"scheduled":([0-9]+).*"running":([0-9]+).*/\1+\2+\3/' | bc)
  echo "  $s"
  [ "$open" -eq 0 ] && break
  [ $SECONDS -ge $deadline ] && { echo "FAIL: queue did not drain" >&2; exit 1; }
  sleep 3
done

echo "durableq check (in-cluster)"
k run durableq-check --rm -i --restart=Never --image=durableq:dev --image-pull-policy=Never \
  --overrides='{"spec":{"containers":[{"name":"durableq-check","image":"durableq:dev","imagePullPolicy":"Never","args":["check"],"env":[{"name":"DATABASE_URL","valueFrom":{"secretKeyRef":{"name":"durableq-db","key":"DATABASE_URL"}}}]}]}}'
echo "smoke OK"
