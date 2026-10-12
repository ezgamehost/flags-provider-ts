#!/usr/bin/env bash
# End to end: both providers against the real flags-stack (`flags-stack all`: flags-api, the
# compiler worker, the change relay and flags-data in one process) on Postgres and Redis, with
# org-api faked by flags-stack's cmd/fake-orgapi.
#   scripts/e2e.sh <flags-stack checkout>
# Needs DATABASE_URL (a database of its own: it migrates and fills it) and REDIS_URL (changes reach
# flags-data only through Redis pub/sub). CI checks flags-stack out at .flags-stack-rev.
set -euo pipefail
stack=$(cd "${1:?usage: scripts/e2e.sh <flags-stack checkout>}" && pwd)
: "${DATABASE_URL:?DATABASE_URL is required}" "${REDIS_URL:?REDIS_URL is required}"
root=$(pwd)
mkdir -p .bin
(cd "$stack" && go build -o "$root/.bin/flags-stack" ./cmd/flags-stack && go build -o "$root/.bin/fake-orgapi" ./cmd/fake-orgapi)

# fakeorgapi's fixed organization, project and credentials (internal/fakeorgapi).
export E2E_ORG=01920000-0000-7000-8000-00000000a001 E2E_PROJECT=01920000-0000-7000-8000-00000000b001
export E2E_API_KEY=ezgh_0123456789abcdefghijABCDEFGHIJklmnopqrst
export E2E_API_URL=http://127.0.0.1:6500 E2E_DATA_URL=http://127.0.0.1:6501
export APP_ENV=local REGION=us-local-1 PORT=6500 DATA_PORT=6501
export ORG_API_URL=http://127.0.0.1:4999 INTERNAL_API_TOKEN=internal-token-for-tests
export ORG_API_SERVICE_CREDENTIAL=ok_0123456789abcdef.0123456789ABCDEFGHIJabcdefghij0123456789abc

pids=()
# cleanup stops everything and waits for it to exit (flags-stack drains after SIGTERM); whatever
# is still up after 30 s is killed.
cleanup() {
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  for _ in $(seq 1 150); do
    alive=0
    for p in "${pids[@]}"; do kill -0 "$p" 2>/dev/null && alive=1; done
    [ "$alive" = 0 ] && return 0
    sleep 0.2
  done
  for p in "${pids[@]}"; do kill -9 "$p" 2>/dev/null || true; done
}
trap cleanup EXIT
# wait_for <url> [<pid>] [any]: until the URL answers 2xx (any HTTP status with "any"), failing at
# once if the process exits.
wait_for() {
  local f=-fsS; [ "${3:-}" = any ] && f=-sS
  for _ in $(seq 1 150); do
    curl $f -o /dev/null "$1" 2>/dev/null && return 0
    if [ -n "${2:-}" ] && ! kill -0 "$2" 2>/dev/null; then echo "the process serving $1 exited" >&2; return 1; fi
    sleep 0.2
  done
  echo "timed out waiting for $1" >&2; return 1
}
for port in 4999 6500 6501; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    echo "port $port is in use (a previous run still shutting down?); free it and retry" >&2; exit 1
  fi
done

PORT=4999 FAKE_ORG_ID=$E2E_ORG FAKE_PROJECT_ID=$E2E_PROJECT .bin/fake-orgapi > .bin/fake-orgapi.log 2>&1 & fake_pid=$!; pids+=($fake_pid)
wait_for http://127.0.0.1:4999/ "$fake_pid" any || { cat .bin/fake-orgapi.log; exit 1; }
.bin/flags-stack migrate
.bin/flags-stack all > .bin/stack.log 2>&1 & stack_pid=$!; pids+=($stack_pid)
wait_for "$E2E_API_URL/ready" "$stack_pid" || { tail -n 200 .bin/stack.log; exit 1; }
wait_for "$E2E_DATA_URL/health" "$stack_pid" || { tail -n 200 .bin/stack.log; exit 1; }

bun test test/e2e/e2e.test.ts || { echo "--- stack log"; tail -n 300 .bin/stack.log; exit 1; }
