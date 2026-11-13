#!/usr/bin/env bash
# POST N demo.effect jobs through the API, wait for the queue to drain, run `durableq check` in-cluster.
set -euo pipefail

CTX=kind-durableq
NS=durableq
N=${N:-200}
TIMEOUT=${TIMEOUT:-300}
PORT=${PORT:-18080}
k() { kubectl --context "$CTX" -n "$NS" "$@"; }

# Not via k(): backgrounding a function forks a subshell, and killing it would orphan kubectl.
kubectl --context "$CTX" -n "$NS" port-forward svc/durableq-api "$PORT:8080" >/dev/null &
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
  open=$(echo "$s" | sed -E 's/.*"pending":([0-9]+).*"scheduled":([0-9]+).*"running":([0-9]+).*"lease_expired":([0-9]+).*/\1+\2+\3+\4/' | bc)
  echo "  $s"
  [ "$open" -eq 0 ] && break
  [ $SECONDS -ge $deadline ] && { echo "FAIL: queue did not drain" >&2; exit 1; }
  sleep 3
done

echo "durableq check (in-cluster)"
# exec into the API pod: it already has DATABASE_URL, and exec reliably returns output and exit code.
k exec deploy/durableq-api -c api -- /durableq check
echo "smoke OK"
