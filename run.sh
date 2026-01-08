#!/bin/sh
set -eu
clear

echo "→ Tidying Go modules (host)"
go mod tidy

echo "→ Starting NATS"
docker compose up -d nats

cleanup() {
  echo "→ Cleaning up (keeping caches)"
  # IMPORTANT: no -v, so gocache/gomodcache persist across runs
  docker compose down
}
trap cleanup EXIT INT TERM

echo "→ Running tests (pass 1)"
docker compose run --rm test

echo "→ CHAOS: restarting NATS"
docker compose stop nats
docker compose up -d nats

echo "→ Running tests (pass 2)"
docker compose run --rm test

echo "✔ All done"
#docker compose down -v # uncomment to remove caches as well
