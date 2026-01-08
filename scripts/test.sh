#!/bin/sh
set -eu

export NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
MON_URL="${NATS_MON_URL:-http://127.0.0.1:8222/varz}"

# wait for NATS
i=0
while [ "$i" -lt 80 ]; do
  if curl -fsS "$MON_URL" >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 0.25
done

if ! curl -fsS "$MON_URL" >/dev/null 2>&1; then
  echo "NATS did not become ready" >&2
  exit 1
fi

go test ./... -count=1 -race -coverprofile=coverage.out
go tool cover -func=coverage.out
