#!/usr/bin/env bash
# Shared helpers for the scenario scripts. Each script runs against the Docker Compose stack,
# prints PASS/FAIL per acceptance criterion and exits non-zero if any failed.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"
BASE_URL="${BASE_URL:-http://localhost:8000}"
API="$BASE_URL/api/v1"
WORK="$ROOT/.demo/scenarios"
REL=".demo/scenarios"   # the same directory relative to the project: what ./oscli (in the client container) can see
mkdir -p "$WORK"
MIB=1048576
# Docker names follow the Compose project (COMPOSE_PROJECT_NAME, default "objectstore", the name in
# docker-compose.yml), so a run under another project never touches the default project's volumes.
PROJECT="${COMPOSE_PROJECT_NAME:-objectstore}"
NETWORK="${PROJECT}_objectstore-net"
FAILED=0
PASSED=0

if [ -t 1 ]; then GREEN=$'\033[32m'; RED=$'\033[31m'; BOLD=$'\033[1m'; OFF=$'\033[0m'; else GREEN=""; RED=""; BOLD=""; OFF=""; fi
command -v jq > /dev/null || { echo "the scenario scripts need jq" >&2; exit 2; }

say() { printf '%s\n' "$*"; }
section() { printf '\n%s== %s%s\n' "$BOLD" "$*" "$OFF"; }
ok() { PASSED=$((PASSED + 1)); printf '  %sPASS%s %s\n' "$GREEN" "$OFF" "$*"; }
bad() { FAILED=$((FAILED + 1)); printf '  %sFAIL%s %s\n' "$RED" "$OFF" "$*"; }
# check "<criterion>" command...   -> PASS if the command succeeds
check() { local what="$1"; shift; if "$@" > /dev/null 2>&1; then ok "$what"; else bad "$what"; fi; }
summary() {
    printf '\n%s%d passed, %d failed%s\n' "$BOLD" "$PASSED" "$FAILED" "$OFF"
    [ "$FAILED" -eq 0 ]
}

status() { curl -fsS -m 5 "$API/cluster/status"; }
meta() { curl -fsS -m 5 "$API/objects/$1/metadata"; }
worker_field() { status | jq -r --arg w "$1" ".workers[] | select(.worker_id == \$w) | .$2"; }
counter() { status | jq -r ".counters.$1 // 0"; }
now() { python3 -c 'import time; print(f"{time.time():.1f}")'; }
since() { python3 -c "import sys,time; print(f'{time.time()-float(sys.argv[1]):.1f}')" "$1"; }

# wait_for <timeout s> <command...>: poll every 0.5 s; returns non-zero on time-out.
wait_for() {
    local timeout="$1" started=$SECONDS
    shift
    until "$@" > /dev/null 2>&1; do
        [ $((SECONDS - started)) -ge "$timeout" ] && return 1
        sleep 0.5
    done
}

healthy() {
    status | jq -e '.safe_mode == false
        and ([.workers[] | select(.state == "ALIVE" and .has_reported)] | length) == 4
        and .under_replicated == 0 and .over_replicated == 0 and .missing_chunks == 0
        and .pending_replications == 0 and .replicas.corrupt == 0 and .replicas.excess == 0'
}

# Start the stack if needed and wait until it is healthy.
ensure_up() {
    if ! curl -fsS -m 2 "$BASE_URL/health" > /dev/null 2>&1; then
        docker compose up -d --wait > /dev/null 2>&1
    fi
    for w in controller worker1 worker2 worker3 worker4; do
        [ "$(docker inspect -f '{{.State.Running}}' "$w" 2>/dev/null)" = "true" ] || docker start "$w" > /dev/null
        [ "$(docker inspect -f '{{.State.Paused}}' "$w" 2>/dev/null)" = "true" ] && docker unpause "$w" > /dev/null
    done
    wait_for 120 healthy || { echo "the cluster did not become healthy" >&2; status | jq . >&2; exit 2; }
}

# A fresh, empty cluster.
fresh_cluster() {
    docker compose down -v > /dev/null 2>&1
    docker compose up -d --wait > /dev/null 2>&1
    wait_for 120 healthy || { echo "the cluster did not become healthy" >&2; exit 2; }
}

# upload <file> [extra curl args...] -> prints the object id
upload() {
    local file="$1"; shift
    curl -fsS -m 120 -F "file=@$file" "$@" "$API/objects" | jq -r .object_id
}

random_file() { head -c "$2" /dev/urandom > "$1"; }
sha() { shasum -a 256 "$1" | cut -d' ' -f1; }
download_matches() { curl -fsS -m 120 -o "$WORK/dl.bin" "$API/objects/$1" && [ "$(sha "$WORK/dl.bin")" = "$(sha "$2")" ]; }

# Overwrite 4 bytes inside a chunk file on a worker (size unchanged; the .meta is untouched).
corrupt_chunk() { docker exec "$1" sh -c "printf 'XXXX' | dd of=/data/chunks/$2 bs=1 seek=100 conv=notrunc status=none"; }
file_exists() { docker exec "$1" test -f "/data/chunks/$2"; }
location_state() { meta "$1" | jq -r --arg c "$2" --arg w "$3" '.chunks[] | select(.chunk_id == $c) | .locations[] | select(.worker_id == $w) | .status'; }
no_invariant_violations() { [ "$(counter invariant_violations)" = "0" ]; }
