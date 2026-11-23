#!/usr/bin/env bash
#
# showcase.sh - a guided tour of the store's inner state (start it with "make showcase").
#
#   0. starts the stack with docker-compose.showcase.yml: the controller prints every message it
#      receives and sends, and every 10 s the cluster; every worker prints its own state every 10 s
#   1. shows where to watch that, with the latest lines
#   2. uploads three files and shows where every copy of every chunk went (chunk x worker grid)
#   3. docker stop worker2: STALE, then DEAD; every chunk it held is copied again from the surviving
#      copy, until each has two live copies; all files still download intact
#   4. docker start worker2: it registers again, adopts a new fence and resyncs (lists the chunks on
#      its disk); the controller has the survivors VERIFY themselves, deletes the surplus copies, and
#      every chunk ends with exactly two copies
#   5. a new upload after the recovery; fsck confirms every disk matches the controller's map
#
# Between the steps it waits for Enter, so you can look at the logs in another terminal
# ("make showcase-logs"). Every wait polls the REST API until the state is reached. With a store
# that is already full of data (e.g. after "make chaos") re-copying a worker's chunks takes minutes;
# "make down-v" first gives a quick run.
#
#   ./showcase.sh               guided, pauses before each step
#   ./showcase.sh --no-pause    runs straight through
#
# Needs: bash, docker, curl, python3. Environment: BASE_URL (default http://localhost:$CONTROLLER_PORT, i.e. 9000),
# VICTIM (the worker to stop, default worker2), ADMIN_EMAIL / ADMIN_PASSWORD (the admin account).

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

BASE_URL="${BASE_URL:-http://localhost:${CONTROLLER_PORT:-9000}}"
API="$BASE_URL/api/v1"
VICTIM="${VICTIM:-worker2}"
WORK=".demo/showcase"
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.showcase.yml)
PAUSE=1
[ "${1:-}" = "--no-pause" ] && PAUSE=0
[ -t 0 ] || PAUSE=0
export BASE_URL
# Every request is made as the admin account, which sees every object: curl reads $CURL_HOME/.curlrc,
# ./oscli reads OSCLI_EMAIL and OSCLI_PASSWORD (README "Accounts").
ADMIN_EMAIL="${ADMIN_EMAIL:-admin@gmail.com}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-admin123}"
export OSCLI_EMAIL="$ADMIN_EMAIL" OSCLI_PASSWORD="$ADMIN_PASSWORD"
export CURL_HOME="$PWD/.demo/curl"
mkdir -p "$CURL_HOME"
printf 'user = "%s:%s"\n' "$ADMIN_EMAIL" "$ADMIN_PASSWORD" > "$CURL_HOME/.curlrc"

for tool in docker curl python3; do
    command -v "$tool" > /dev/null 2>&1 || { echo "showcase.sh needs '$tool' on the PATH" >&2; exit 2; }
done
if [ -t 1 ]; then
    BOLD=$'\033[1m'; GREEN=$'\033[32m'; RED=$'\033[31m'; YELLOW=$'\033[33m'; DIM=$'\033[2m'; OFF=$'\033[0m'
else
    BOLD=""; GREEN=""; RED=""; YELLOW=""; DIM=""; OFF=""
fi

# ---- output ----------------------------------------------------------------------------------

FAILED=0
banner() {
    printf '\n%s══════════════════════════════════════════════════════════════════════════════════%s\n' "$BOLD" "$OFF"
    printf '%s %s%s\n' "$BOLD" "$*" "$OFF"
    printf '%s══════════════════════════════════════════════════════════════════════════════════%s\n' "$BOLD" "$OFF"
}
step() { printf '\n%s▸ %s%s\n' "$BOLD" "$*" "$OFF"; }
note() { printf '  %s%s%s\n' "$DIM" "$*" "$OFF"; }
tip() { printf '  %s%s%s\n' "$YELLOW" "$*" "$OFF"; }
indent() { sed 's/^/  /'; }
ok() { printf '  %s✓%s %s\n' "$GREEN" "$OFF" "$*"; }
bad() { printf '  %s✗ %s%s\n' "$RED" "$*" "$OFF"; FAILED=$((FAILED + 1)); }
pause() {
    [ "$PAUSE" = 1 ] || return 0
    printf '\n  %s[ press Enter to %s ]%s ' "$YELLOW" "$*" "$OFF"
    read -r _
}

view() { python3 tests/showcase_view.py "$@"; }
# now, to the microsecond (docker logs --since takes RFC 3339 with fractions)
now() { python3 -c 'from datetime import datetime, timezone; print(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"))'; }
json_field() { python3 -c 'import json, sys; print(json.load(sys.stdin)[sys.argv[1]])' "$1"; }
sha256_of() {
    if command -v sha256sum > /dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

# log lines without the date and the level, chunk ids shortened as the trace prints them:
# "10:42:07.123  [monitor] copied chunk=3f1c9a52/1 src=worker4 dst=worker3 (12 ms)"
tidy() {
    sed -E -e 's/^[0-9]{4}-[0-9]{2}-[0-9]{2} ([0-9:.]+) +[A-Z]+ +/\1  /' \
        -e 's/([0-9a-f]{8})-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-0*([0-9]+)/\1\/\2/g' \
        -e 's/ cmd=[0-9a-f-]+//' | cut -c1-150
}
# the showcase's objects as a grep -E alternative: full ids (monitor lines) or 8-char prefixes (trace)
ids_pattern() { local IFS='|'; echo "${IDS[*]}"; }
prefix_pattern() { local id out=""; for id in "${IDS[@]}"; do out+="${out:+|}${id:0:8}"; done; echo "$out"; }

# show <container> <since> <first grep -F filter> <grep -E pattern> <max lines> [head|tail]
show() {
    local lines
    lines=$(docker logs --since "$2" "$1" 2>&1 | grep -F -- "$3" | grep -E -- "$4" | "${6:-head}" -n "$5" | tidy || true)
    if [ -n "$lines" ]; then printf '%s\n' "$lines" | indent; else note "(none in the log)"; fi
}
count() { docker logs --since "$2" "$1" 2>&1 | grep -cF -- "$3" || true; }

# last_state <container> [since]: the newest state block it printed (each starts with a "═══" header)
last_state() {
    docker logs --since "${2:-40s}" "$1" 2>&1 | awk '
        index($0, "[state] ═══") > 0 { n = 0 }
        index($0, "[state]") > 0 { sub(/^.*\[state\] /, ""); block[n++] = $0 }
        END { if (n == 0) print "(no state printed yet)"
              for (i = 0; i < n; i++) print block[i] }' | cut -c1-150 | indent
}
printed_state() { docker logs --since "$2" "$1" 2>&1 | grep -qF '[state] ═══'; }

# wait_until <timeout seconds> "what we wait for" command [args...]
wait_until() {
    local timeout="$1" what="$2" started=$SECONDS
    shift 2
    printf '  waiting until %s ' "$what"
    until "$@" > /dev/null 2>&1; do
        if [ $((SECONDS - started)) -ge "$timeout" ]; then
            printf ' %stimed out after %ss%s\n' "$RED" "$timeout" "$OFF"
            FAILED=$((FAILED + 1))
            return 0
        fi
        printf '.'
        sleep 1
    done
    printf ' %sdone after %ss%s\n' "$GREEN" "$((SECONDS - started))" "$OFF"
}

verify_downloads() {
    local i out="$WORK/download.out"
    for i in "${!IDS[@]}"; do
        if curl -fsS -o "$out" "$API/objects/${IDS[$i]}" && [ "$(sha256_of "$out")" = "$(sha256_of "${FILES[$i]}")" ]; then
            ok "$(basename "${FILES[$i]}") downloads with the same SHA-256 as the original"
        else
            bad "$(basename "${FILES[$i]}") did not download intact"
        fi
    done
    rm -f "$out"
}

upload() {   # upload <file>: prints the object id
    curl -fsS -F "file=@$1" "$API/objects" | json_field object_id
}

# ---- 0. start ------------------------------------------------------------------------------

banner "0  Starting the cluster with the showcase settings"
note "docker-compose.showcase.yml turns on: TRACE_MESSAGES=all on the controller (every message in"
note "and out) and STATE_PRINT_INTERVAL=10 on every container (the state, every 10 seconds)."
step "${COMPOSE[*]} up --build -d --wait"
"${COMPOSE[@]}" up --build -d --wait 2>&1 | tail -n 6 | indent
wait_until 300 "every worker is ALIVE and has completed a resync" view check healthy
STORED=$(view objects)
if [ "$STORED" -gt 50 ]; then
    tip "The store already holds $STORED objects (from earlier runs). That works, but re-copying a stopped"
    tip "worker's chunks then takes minutes. For a quick showcase:  make down-v  and run this again."
fi

# ---- 1. where to look ----------------------------------------------------------------------

banner "1  Where the inner state is printed"
step "Watch it live in a second terminal:"
printf '  %smake showcase-logs%s          all five containers, interleaved\n' "$BOLD" "$OFF"
printf '  %sdocker logs -f controller%s   every message + the whole cluster every 10 s\n' "$BOLD" "$OFF"
printf '  %sdocker logs -f %s%s      what %s holds and how its link to the controller is doing\n' "$BOLD" "$VICTIM" "$OFF" "$VICTIM"
step "The controller logs each message: ← received from a worker, → sent to a worker. The latest:"
show controller 10s '[msg]' '.' 8 tail
note "(a heartbeat carries the worker's new changes; the reply acknowledges them and carries commands)"
wait_until 25 "the containers print their state" printed_state "$VICTIM" 12s
step "Every 10 s the controller prints the cluster as it sees it:"
last_state controller 12s
step "Every 10 s each worker prints its own state. $VICTIM's latest:"
last_state "$VICTIM" 12s
pause "upload three files"

# ---- 2. upload -----------------------------------------------------------------------------

banner "2  Uploading three files (1 MB chunks, two copies of each chunk)"
rm -rf "$WORK"
mkdir -p "$WORK"
cp samples/sample.jpg "$WORK/photo.jpg"
head -c 3670016 /dev/urandom > "$WORK/dataset.bin"    # 3.5 MB -> 4 chunks
head -c 1258291 /dev/urandom > "$WORK/notes.bin"      # 1.2 MB -> 2 chunks
FILES=("$WORK/photo.jpg" "$WORK/dataset.bin" "$WORK/notes.bin")
IDS=()
SINCE=$(now)
for file in "${FILES[@]}"; do
    id=$(upload "$file")
    IDS+=("$id")
    ok "$(basename "$file") → object ${id:0:8}…   (its chunks are ${id:0:8}/0, ${id:0:8}/1, … in the logs)"
done
step "The controller sent each chunk to two workers; each answered with the change it logged:"
show controller "$SINCE" '[msg]' 'PUT (CHUNK|ANSWER)' 12
note "(the full list: docker logs controller | grep 'PUT ')"
step "Where every copy of every chunk is now (from GET /api/v1/objects/{id}/metadata):"
view grid "${IDS[@]}"
BEFORE=$(view copies "$VICTIM")
pause "stop $VICTIM"

# ---- 3. a worker fails -----------------------------------------------------------------------

banner "3  A worker fails: docker stop $VICTIM"
note "$VICTIM holds $BEFORE chunk copies. Once it stops, the controller hears nothing from it: after 6 s"
note "of silence it is STALE (new data avoids it, its copies still count), after 12 s DEAD: its copies"
note "are forgotten and every chunk it held is copied from the surviving copy to another worker."
WAIT_FOR=repaired
if [ "$BEFORE" -gt 100 ]; then
    WAIT_FOR=repaired-files
    tip "Re-copying all $BEFORE copies at the configured rate (MAX_STREAMS=2 copies in flight per source,"
    tip "a planner tick every MONITOR_INTERVAL seconds) takes about $((BEFORE / 80 + 1)) minutes, so this step goes on once the"
    tip "showcase files have two live copies again; the rest keeps being copied in the background."
fi
SINCE=$(now)
step "docker stop $VICTIM"
docker stop "$VICTIM" > /dev/null
view follow "$WAIT_FOR" "$VICTIM" $((120 + BEFORE)) "${IDS[@]}" || FAILED=$((FAILED + 1))
step "The controller's view of $VICTIM:"
show controller "$SINCE" "[registry] $VICTIM" '.' 3
step "What the replication monitor did for the showcase files ($(count controller "$SINCE" '[monitor] copied') copies made in all):"
show controller "$SINCE" '[monitor]' "$(ids_pattern)" 16
step "The REPLICATE commands went out in heartbeat replies; the new copies came back as RECEIVED changes:"
show controller "$SINCE" '[msg]' "REPLY .*REPLICATE ($(prefix_pattern))/|HEARTBEAT .*RECEIVED ($(prefix_pattern))/" 8
step "Where every copy is now ($VICTIM's copies are gone from the map, the others hold two of each):"
view grid "${IDS[@]}"
step "Every file still downloads intact, with $VICTIM down:"
verify_downloads
pause "start $VICTIM again"

# ---- 4. the worker comes back ----------------------------------------------------------------

banner "4  The worker comes back: docker start $VICTIM"
note "$VICTIM still has its old copies on disk. It registers again (a new process id), adopts a new"
note "fence and resyncs: it lists every chunk it holds. Chunks that were re-copied meanwhile now have"
note "three copies, one too many: the controller asks the two that would stay to VERIFY themselves,"
note "then sends a versioned DELETE for the surplus. Chunks not re-copied yet just count $VICTIM's copy"
note "again. Either way every chunk ends with exactly two copies."
SINCE=$(now)
step "docker start $VICTIM"
docker start "$VICTIM" > /dev/null
view follow healthy "$VICTIM" 300 "${IDS[@]}" || FAILED=$((FAILED + 1))
step "The registration with its fence, the resync listing on a heartbeat, VERIFY and DELETE commands in the replies:"
show controller "$SINCE" '[msg]' "REGISTER|resync listing|segment .* (ok|done)|VERIFY|DELETE" 12
step "What the replication monitor did for the showcase files ($(count controller "$SINCE" '[monitor] trimmed') surplus copies deleted in all):"
show controller "$SINCE" '[monitor]' "$(ids_pattern)" 16
step "$VICTIM's own view of itself, as it printed it after the restart:"
wait_until 30 "$VICTIM prints its state" printed_state "$VICTIM" "$SINCE"
last_state "$VICTIM" "$SINCE"
step "Where every copy is now:"
view grid "${IDS[@]}"
verify_downloads
pause "upload one more file and run fsck"

# ---- 5. new data and fsck -------------------------------------------------------------------

banner "5  New data after the recovery, and fsck checks every disk"
head -c 2306867 /dev/urandom > "$WORK/after.bin"      # 2.2 MB -> 3 chunks
FILES+=("$WORK/after.bin")
SINCE=$(now)
id=$(upload "$WORK/after.bin")
IDS+=("$id")
ok "after.bin → object ${id:0:8}…"
show controller "$SINCE" '[msg]' 'PUT CHUNK' 6
NEW_ON_VICTIM=$(curl -fsS "$API/objects/$id/metadata" | python3 -c '
import json, sys
print(sum(loc["worker_id"] == sys.argv[1] for c in json.load(sys.stdin)["chunks"] for loc in c["locations"]))' "$VICTIM")
if [ "$NEW_ON_VICTIM" -gt 0 ]; then
    note "$NEW_ON_VICTIM of its copies went to $VICTIM: it is a full member of the cluster again"
else
    note "none of its copies went to $VICTIM this time; it is ALIVE and eligible for new copies"
fi
view grid "${IDS[@]}"
step "fsck: every worker lists its disk and the controller compares it with its map"
wait_until 120 "fsck reports HEALTHY (no missing, extra or orphan copies)" view check fsck
verify_downloads

banner "Done"
if [ "$FAILED" = 0 ]; then
    ok "losing $VICTIM and getting it back broke nothing: every chunk has two copies and every file is intact"
else
    bad "$FAILED check(s) failed (see above)"
fi
note "The containers keep printing their state. Back to quiet logs:  docker compose up -d"
note "The uploaded objects stay; see them in the web client at $BASE_URL/app"
[ "$FAILED" = 0 ]
