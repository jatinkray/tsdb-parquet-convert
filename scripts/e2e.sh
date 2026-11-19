#!/usr/bin/env bash
# End-to-end check against a real Parqtel container.
#
# Files mode: convert -> clone into a host data directory -> copy into the
# container -> restart -> live reconcile.
# OTLP mode: clean volume -> clone over HTTP -> live reconcile -> re-run ->
# reconcile again -> compact -> reconcile again.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT/deploy/e2e/docker-compose.yml"
COMPOSE=(docker compose -f "$COMPOSE_FILE")
CONVERTER="$ROOT/target/release/tsdb2parqtel"
FIXTURE="$ROOT/crates/tsdb-reader/tests/fixtures/testblock"
WORK="$ROOT/.e2e"
BASE_URL="${PARQTEL_E2E_URL:-http://127.0.0.1:18080}"
if [[ -z "${PARQTEL_IMAGE:-}" ]]; then
  if docker image inspect parqtel-s3-qa:95048c6 >/dev/null 2>&1; then
    PARQTEL_IMAGE=parqtel-s3-qa:95048c6
  else
    PARQTEL_IMAGE=ghcr.io/parqtel/parqtel-oss:0.3.1
  fi
fi
export PARQTEL_IMAGE

cleanup() {
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_ready() {
  for _ in $(seq 1 60); do
    if curl -fsS "$BASE_URL/ready" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "Parqtel did not become ready at $BASE_URL" >&2
  "${COMPOSE[@]}" logs --tail=80 parqtel >&2 || true
  return 1
}

restart() {
  "${COMPOSE[@]}" restart parqtel >/dev/null
  wait_ready
}

verify_live() {
  "$CONVERTER" verify "$FIXTURE" "$BASE_URL" \
    --min-time 1650005000000 --max-time 1650005120000 "$@"
}

cargo build --release --locked --manifest-path "$ROOT/Cargo.toml"
rm -rf "$WORK"
mkdir -p "$WORK"

"${COMPOSE[@]}" up -d
wait_ready

echo "== files mode =="
"$CONVERTER" convert "$FIXTURE" --out "$WORK/files" --sidecar
"$CONVERTER" clone "$FIXTURE" --target "$WORK/data"
"${COMPOSE[@]}" cp "$WORK/data/." parqtel:/data/
restart
verify_live

echo "== files re-run is idempotent =="
"$CONVERTER" clone "$FIXTURE" --target "$WORK/data"
"${COMPOSE[@]}" cp "$WORK/data/." parqtel:/data/
restart
verify_live

echo "== otlp mode =="
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null
"${COMPOSE[@]}" up -d
wait_ready
"$CONVERTER" clone "$FIXTURE" \
  --target "$BASE_URL" \
  --checkpoint "$WORK/checkpoint.json" \
  --matcher '{__name__="up"}'
verify_live --matcher '{__name__="up"}'

echo "== otlp checkpoint resume =="
"$CONVERTER" clone "$FIXTURE" \
  --target "$BASE_URL" \
  --checkpoint "$WORK/checkpoint.json" \
  --matcher '{__name__="up"}'
verify_live --matcher '{__name__="up"}'

echo "== otlp protobuf =="
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null
"${COMPOSE[@]}" up -d
wait_ready
"$CONVERTER" clone "$FIXTURE" \
  --target "$BASE_URL" \
  --checkpoint "$WORK/checkpoint-protobuf.json" \
  --otlp-protocol protobuf \
  --matcher '{__name__="up"}'
verify_live --matcher '{__name__="up"}'

echo "== compaction =="
"${COMPOSE[@]}" stop parqtel >/dev/null
"${COMPOSE[@]}" run --rm parqtel compact
"${COMPOSE[@]}" up -d
wait_ready
verify_live --matcher '{__name__="up"}'

echo "e2e: pass"
