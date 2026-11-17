#!/usr/bin/env bash
#
# demo.sh - runs the required demonstrations against a running stack (start it with "make up")
# and checks every claim it makes. Prints PASS/FAIL per demonstration and "N/7 PASS" at the end;
# the exit code is 0 only if all seven passed.
#
#   1. Object storage            4. Metadata         7. Controller crash (docker kill mid-upload)
#   2. Chunking and distribution 5. Worker failure   (docker stop / docker start worker2)
#   3. Replication               6. Data integrity   (a stored chunk is corrupted on purpose)
#
# Needs: bash, docker, curl. jq is used from the host if installed, otherwise from the client
# container. Nothing waits for a fixed time: every wait polls until the expected state is reached
# (or a generous time-out expires), so the script is as fast as the system and never flaky.
#
# Environment: BASE_URL (default http://localhost:8000), VICTIM (worker to stop, default worker2).

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

BASE_URL="${BASE_URL:-http://localhost:8000}"
API="$BASE_URL/api/v1"
VICTIM="${VICTIM:-worker2}"
WORKERS=(worker1 worker2 worker3 worker4)
WORK=".demo"            # inside the project directory, so the client container can see it too
MIB=1048576

# ---- tools ---------------------------------------------------------------------------------

for tool in docker curl; do
    command -v "$tool" > /dev/null 2>&1 || { echo "demo.sh needs '$tool' on the PATH" >&2; exit 2; }
done
if ! command -v jq > /dev/null 2>&1; then
    jq() { docker exec -i client jq "$@"; }
fi
sha256_of() {
    if command -v sha256sum > /dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi
}
if [ -t 1 ]; then BOLD=$'\033[1m'; GREEN=$'\033[32m'; RED=$'\033[31m'; DIM=$'\033[2m'; OFF=$'\033[0m'; else BOLD=""; GREEN=""; RED=""; DIM=""; OFF=""; fi

# ---- reporting -----------------------------------------------------------------------------

PASSED=0
TOTAL=0
DEMO_OK=1
RESULTS=()

banner() {
    TOTAL=$((TOTAL + 1))
    DEMO_OK=1
    CURRENT="$*"
    printf '\n%s================================================================================%s\n' "$BOLD" "$OFF"
    printf '%s DEMO %s%s\n' "$BOLD" "$*" "$OFF"
    printf '%s================================================================================%s\n' "$BOLD" "$OFF"
}
step() { printf '\n%s--> %s%s\n' "$BOLD" "$*" "$OFF"; }
note() { printf '    %s%s%s\n' "$DIM" "$*" "$OFF"; }
indent() { sed 's/^/    /'; }

# check "what is being claimed" command [args...]   -- the claim holds if the command succeeds
check() {
    local claim="$1"
    shift
    if "$@" > /dev/null 2>&1; then
        printf '    %s[ok]%s   %s\n' "$GREEN" "$OFF" "$claim"
    else
        printf '    %s[FAIL]%s %s\n' "$RED" "$OFF" "$claim"
        DEMO_OK=0
    fi
}
equal() { [ "$1" = "$2" ]; }

finish() {
    if [ "$DEMO_OK" = 1 ]; then
        PASSED=$((PASSED + 1))
        RESULTS+=("${GREEN}PASS${OFF}  $CURRENT")
        printf '\n%sPASS%s  %s\n' "$GREEN" "$OFF" "$CURRENT"
    else
        RESULTS+=("${RED}FAIL${OFF}  $CURRENT")
        printf '\n%sFAIL%s  %s\n' "$RED" "$OFF" "$CURRENT"
    fi
}

# wait_until <timeout seconds> "what we wait for" command [args...]
wait_until() {
    local timeout="$1" what="$2" started=$SECONDS
    shift 2
    printf '    waiting until %s ' "$what"
    until "$@" > /dev/null 2>&1; do
        if [ $((SECONDS - started)) -ge "$timeout" ]; then
            printf ' %stimed out after %ss%s\n' "$RED" "$timeout" "$OFF"
            DEMO_OK=0
            return 0
        fi
        printf '.'
        sleep 1
    done
    printf ' %sdone after %ss%s\n' "$GREEN" "$((SECONDS - started))" "$OFF"
}

# ---- talking to the system -------------------------------------------------------------------

oscli() { ./oscli "$@"; }
status() { curl -fsS "$API/cluster/status"; }
meta() { curl -fsS "$API/objects/$1/metadata"; }
controller_log() { docker logs controller 2>&1; }

status_has() { status | jq -e "$@"; }
meta_has() { local object_id="$1"; shift; meta "$object_id" | jq -e "$@"; }
file_has() { local file="$1"; shift; jq -e "$@" < "$file"; }
listing_has() { curl -fsS "$API/objects?limit=500" | jq -e "$@"; }
field() { jq -r "$2" < "$1"; }   # field <file> <jq expression>

worker_is() { [ "$(status | jq -r --arg w "$1" '.workers[] | select(.worker_id == $w) | .status')" = "$2" ]; }
no_chunk_under_replicated() { status | jq -e '.under_replicated == 0'; }
cluster_healthy() {
    status | jq -e '.safe_mode == false
        and ([.workers[] | select(.state == "ALIVE" and .has_reported)] | length) == 4
        and .under_replicated == 0 and .over_replicated == 0 and .missing_chunks == 0
        and .pending_replications == 0 and .replicas.corrupt == 0 and .replicas.excess == 0'
}
# every chunk of the object has exactly two copies, both intact, on two different live workers
exactly_two_good_copies() {
    meta "$1" | jq -e '.replication_ok and ([.chunks[]
        | (.locations | length) == 2
          and ([.locations[].worker_id] | unique | length) == 2
          and ([.locations[] | select(.status == "OK" and .worker_status == "ALIVE")] | length) == 2] | all)'
}
# Chunk data files of an object on a worker (each also has a <chunk_id>.meta sidecar, not counted).
files_on_worker() { docker exec "$1" sh -c "ls /data/chunks | grep -v '\.meta\$' | grep -c '^$2' || true"; }
disk_sha256() { docker exec "$1" sha256sum "/data/chunks/$2" 2> /dev/null | cut -d' ' -f1; }
# the file of every recorded copy of every chunk hashes to the checksum recorded at upload
copies_on_disk_are_intact() {
    local object_id="$1" chunk_id checksum worker
    while read -r chunk_id checksum worker; do
        [ "$(disk_sha256 "$worker" "$chunk_id")" = "$checksum" ] || return 1
    done < <(meta "$object_id" | jq -r '.chunks[] | . as $c | .locations[] | "\($c.chunk_id) \($c.checksum) \(.worker_id)"')
}
corrupt_chunk() {
    docker exec "$1" sh -c "printf 'XXXX' | dd of=/data/chunks/$2 bs=1 seek=100 conv=notrunc status=none"
}
show_workers() {
    status | jq -r '.workers[] | "\(.worker_id)  \(.state)  copies=\(.chunk_count)  last heartbeat \(.last_seen_s_ago) s ago"' | indent
    status | jq -r '"under_replicated=\(.under_replicated)  over_replicated=\(.over_replicated)  pending=\(.pending_replications)  missing=\(.missing_chunks)  copies: ok=\(.replicas.ok) corrupt=\(.replicas.corrupt) excess=\(.replicas.excess)"' | indent
}
repair_events() { # repair_events <event> <object_id>
    status | jq -r --arg e "$1" --arg o "$2" '.repair_log[] | select(.event == $e and (.chunk_id | startswith($o)))
        | "\(.time)  \(.event)  \(.chunk_id)  \(.source // "-") -> \(.target // "-")  (\(.reason))"'
}
count_repair_events() { repair_events "$1" "$2" | grep -c . || true; }

# ---- preparation ---------------------------------------------------------------------------

mkdir -p "$WORK"
printf '%sMini Distributed Cloud Object Store - required demonstrations%s\n' "$BOLD" "$OFF"
printf 'controller: %s   dashboard: %s/dashboard\n' "$BASE_URL" "$BASE_URL"

if ! curl -fsS "$BASE_URL/health" > /dev/null 2>&1; then
    echo "The controller does not answer at $BASE_URL. Start the stack first:  make up" >&2
    exit 2
fi
step "The deployment: 1 controller, 4 workers, 1 client, each worker with its own persistent volume"
docker ps --filter network=objectstore_objectstore-net --format '{{.Names}}\t{{.Status}}\t{{.Ports}}' | sort | indent
docker volume ls --filter name=objectstore_ --format '{{.Name}}' | sort | tr '\n' ' ' | indent
echo
if [ -s "$WORK/objects.txt" ]; then
    step "Removing the objects left behind by the previous run of this script"
    while read -r previous; do
        curl -sS -o /dev/null -X DELETE "$API/objects/$previous" || true
    done < "$WORK/objects.txt"
fi
: > "$WORK/objects.txt"
remember() { echo "$1" >> "$WORK/objects.txt"; }   # this run's objects stay, for inspection on the dashboard
wait_until 90 "all four workers are ALIVE and every chunk has two good copies" cluster_healthy
if [ "$DEMO_OK" != 1 ]; then
    echo "The cluster is not healthy; see 'make status' and 'make logs'." >&2
    exit 2
fi

# ============================================================================================
banner "1/7  Object storage - upload an object, retrieve it, verify it is identical"

head -c 307200 /dev/urandom > "$WORK/small.bin"
step "Upload a 300 KB file:  POST $API/objects  (multipart/form-data, field 'file')"
CODE=$(curl -sS -o "$WORK/upload.json" -w '%{http_code}' -F "file=@$WORK/small.bin" "$API/objects")
jq '{object_id, name, size, content_type, checksum, chunk_count, created_at}' < "$WORK/upload.json" | indent
SMALL_ID=$(field "$WORK/upload.json" .object_id)
check "the upload answered 201 Created and assigned an object id" equal "$CODE" 201
check "the stored checksum is the SHA-256 of the original file" equal "$(field "$WORK/upload.json" .checksum)" "$(sha256_of "$WORK/small.bin")"

step "Download it:  GET $API/objects/$SMALL_ID"
CODE=$(curl -sS -D "$WORK/small.headers" -o "$WORK/small.out" -w '%{http_code}' "$API/objects/$SMALL_ID")
grep -iE '^(HTTP/|content-type|content-length|content-disposition|etag)' "$WORK/small.headers" | tr -d '\r' | indent
note "original   sha256 $(sha256_of "$WORK/small.bin")"
note "downloaded sha256 $(sha256_of "$WORK/small.out")"
check "the download answered 200" equal "$CODE" 200
check "the retrieved object is byte-for-byte identical to the original" cmp -s "$WORK/small.bin" "$WORK/small.out"

step "A real image: upload samples/sample.jpg with the CLI and fetch it back"
cp samples/sample.jpg "$WORK/sample.jpg"
oscli upload "$WORK/sample.jpg" > "$WORK/jpeg.json"
JPEG_ID=$(field "$WORK/jpeg.json" .object_id)
remember "$JPEG_ID"
jq '{object_id, name, size, content_type}' < "$WORK/jpeg.json" | indent
curl -fsS -D "$WORK/jpeg.headers" -o "$WORK/jpeg.out" "$API/objects/$JPEG_ID"
check "the image is stored with content type image/jpeg" equal "$(field "$WORK/jpeg.json" .content_type)" "image/jpeg"
check "the download is served with Content-Type: image/jpeg" grep -qi '^content-type: image/jpeg' "$WORK/jpeg.headers"
check "the retrieved image is identical to the original" cmp -s samples/sample.jpg "$WORK/jpeg.out"

step "List and delete:  GET $API/objects?limit=3   then   DELETE $API/objects/$SMALL_ID"
curl -fsS "$API/objects?limit=3" | jq -c '{total, items: [.items[] | {object_id, name, size, content_type}]}' | indent
check "the listing contains the object (id, name, size, content type)" \
    listing_has --arg id "$SMALL_ID" '.items[] | select(.object_id == $id and .name == "small.bin" and .size == 307200)'
CODE=$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE "$API/objects/$SMALL_ID")
check "DELETE answered 204 No Content" equal "$CODE" 204
CODE=$(curl -sS -o "$WORK/gone.json" -w '%{http_code}' "$API/objects/$SMALL_ID")
note "GET after delete -> $CODE $(cat "$WORK/gone.json")"
check "the object is gone: 404 with a JSON error" equal "$CODE $(field "$WORK/gone.json" .error)" "404 object_not_found"
finish

# ============================================================================================
banner "2/7  Chunking and distribution - an object larger than 1 MB"

head -c 3500000 /dev/urandom > "$WORK/big.bin"
LAST_CHUNK=$((3500000 - 3 * MIB))   # 354272
step "Upload a 3.5 MB file (3,500,000 bytes = 3 x 1,048,576 + $LAST_CHUNK bytes)"
oscli upload "$WORK/big.bin" > "$WORK/big.json"
BIG_ID=$(field "$WORK/big.json" .object_id)
remember "$BIG_ID"
jq '{object_id, size, chunk_count, chunks: [.chunks[] | {index, chunk_id, size, checksum: (.checksum[0:16] + "...")}]}' < "$WORK/big.json" | indent
check "the object was divided into 4 chunks" equal "$(field "$WORK/big.json" .chunk_count)" 4
check "chunks are 1 MiB each, only the last one is smaller" equal "$(jq -c '[.chunks[].size]' < "$WORK/big.json")" "[$MIB,$MIB,$MIB,$LAST_CHUNK]"
check "every chunk has a unique id that carries its position (…-00000 to …-00003)" \
    equal "$(jq -c '[.chunks[].chunk_id]' < "$WORK/big.json")" "[\"$BIG_ID-00000\",\"$BIG_ID-00001\",\"$BIG_ID-00002\",\"$BIG_ID-00003\"]"

step "Where the chunks went:  oscli where $BIG_ID"
oscli where "$BIG_ID" | indent
check "every chunk is stored on two different workers" \
    file_has "$WORK/big.json" '[.chunks[] | ([.locations[].worker_id] | unique | length) == 2] | all'

step "The chunk files as they exist on each worker's volume (each also has a .meta sidecar with its checksum)"
TOTAL_FILES=0
SPREAD_OK=1
for worker in "${WORKERS[@]}"; do
    count=$(files_on_worker "$worker" "$BIG_ID")
    TOTAL_FILES=$((TOTAL_FILES + count))
    [ "$count" = 2 ] || SPREAD_OK=0
    note "$worker: $count chunk files of this object: $(docker exec "$worker" sh -c "ls /data/chunks | grep -v '\.meta\$' | grep '^$BIG_ID' | sed 's/.*-/…-/' | tr '\n' ' '")"
done
check "8 chunk files exist in total (4 chunks x 2 copies)" equal "$TOTAL_FILES" 8
check "they are spread evenly: every worker holds exactly 2" equal "$SPREAD_OK" 1

step "Reassembly: download and compare"
oscli download "$BIG_ID" "$WORK/big.out" | jq -c '{bytes, etag_match}' | indent
check "the reassembled object is identical to the original" cmp -s "$WORK/big.bin" "$WORK/big.out"
finish

# ============================================================================================
banner "3/7  Replication - every chunk has two copies; where they are"

step "Copies of each chunk of $BIG_ID, with the SHA-256 of the file on each worker's disk"
while read -r chunk_id checksum worker_a worker_b; do
    note "$chunk_id  recorded sha256 ${checksum:0:16}..."
    note "    $worker_a: ${DIM}$(disk_sha256 "$worker_a" "$chunk_id" | cut -c1-16)...${OFF}    $worker_b: ${DIM}$(disk_sha256 "$worker_b" "$chunk_id" | cut -c1-16)...${OFF}"
done < <(jq -r '.chunks[] | "\(.chunk_id) \(.checksum) \(.locations[0].worker_id) \(.locations[1].worker_id)"' < "$WORK/big.json")
check "each chunk has exactly two copies, on two different live workers, both in state OK" exactly_two_good_copies "$BIG_ID"
check "both files of every chunk hash to the checksum recorded at upload" copies_on_disk_are_intact "$BIG_ID"

step "Cluster-wide:  GET $API/cluster/status"
status | jq -c '{objects, chunks, replicas, under_replicated, over_replicated}' | indent
check "copies in state OK = 2 x number of chunks" status_has '.replicas.ok == 2 * .chunks'
check "no chunk is under-replicated or over-replicated" status_has '.under_replicated == 0 and .over_replicated == 0'
check "the object reports replication_ok = true" meta_has "$BIG_ID" '.replication_ok'
finish

# ============================================================================================
banner "4/7  Metadata - custom metadata on upload; read and modify it without re-uploading"

head -c 1500000 /dev/urandom > "$WORK/report.bin"
step "Upload with custom metadata:  -F 'metadata={\"owner\":\"ansh\",\"course\":\"cloud-computing\"}'"
curl -fsS -F "file=@$WORK/report.bin;type=application/pdf;filename=report.pdf" \
    -F 'metadata={"owner":"ansh","course":"cloud-computing"}' "$API/objects" > "$WORK/meta0.json"
META_ID=$(field "$WORK/meta0.json" .object_id)
remember "$META_ID"
jq -c '{object_id, custom_metadata}' < "$WORK/meta0.json" | indent

step "Retrieve metadata only:  GET $API/objects/$META_ID/metadata"
meta "$META_ID" > "$WORK/meta1.json"
jq 'del(.chunks) + {chunks: "(\(.chunks | length) chunks with their locations, omitted here)"}' < "$WORK/meta1.json" | indent
check "system metadata is complete (name, size, type, times, checksum, chunk count, chunk locations)" \
    file_has "$WORK/meta1.json" '.name == "report.pdf" and .size == 1500000 and .content_type == "application/pdf"
        and (.created_at | length) > 0 and (.updated_at | length) > 0 and (.checksum | length) == 64
        and .chunk_count == 2 and ([.chunks[] | (.locations | length) == 2] | all)'
check "custom metadata is returned as uploaded" \
    equal "$(jq -cS .custom_metadata < "$WORK/meta1.json")" '{"course":"cloud-computing","owner":"ansh"}'

step "Modify it:  PATCH $API/objects/$META_ID/metadata   body {\"owner\":\"team\",\"tag\":\"v1\"}"
curl -fsS -X PATCH -H 'Content-Type: application/json' -d '{"owner":"team","tag":"v1"}' \
    "$API/objects/$META_ID/metadata" > "$WORK/meta2.json"
jq -c '{custom_metadata, created_at, updated_at}' < "$WORK/meta2.json" | indent
check "keys were added and updated, untouched keys kept" \
    equal "$(jq -cS .custom_metadata < "$WORK/meta2.json")" '{"course":"cloud-computing","owner":"team","tag":"v1"}'

step "Remove a key with the CLI:  oscli patch $META_ID --remove tag"
oscli patch "$META_ID" --remove tag > "$WORK/meta3.json"
jq -c '{custom_metadata, created_at, updated_at}' < "$WORK/meta3.json" | indent
check "the key was removed" equal "$(jq -cS .custom_metadata < "$WORK/meta3.json")" '{"course":"cloud-computing","owner":"team"}'
check "last-modified time moved forward, creation time did not" \
    file_has "$WORK/meta3.json" --arg updated "$(field "$WORK/meta1.json" .updated_at)" --arg created "$(field "$WORK/meta1.json" .created_at)" \
        '.updated_at > $updated and .created_at == $created'
check "size, checksum and chunk layout are unchanged: the data was not re-uploaded" \
    equal "$(jq -cS '{size, checksum, chunk_count, chunks}' < "$WORK/meta3.json")" "$(jq -cS '{size, checksum, chunk_count, chunks}' < "$WORK/meta1.json")"
curl -fsS -o "$WORK/report.out" "$API/objects/$META_ID"
check "the object itself is still intact" cmp -s "$WORK/report.bin" "$WORK/report.out"

step "Invalid metadata is refused:  PATCH with body [1,2]"
CODE=$(curl -sS -o "$WORK/bad.json" -w '%{http_code}' -X PATCH -H 'Content-Type: application/json' -d '[1,2]' "$API/objects/$META_ID/metadata")
note "$CODE $(cat "$WORK/bad.json")"
check "400 with a JSON error message" equal "$CODE $(field "$WORK/bad.json" .error)" "400 invalid_metadata"
finish

# ============================================================================================
banner "5/7  Worker failure - docker stop $VICTIM, then docker start $VICTIM"

head -c 3500000 /dev/urandom > "$WORK/failure.bin"
oscli upload "$WORK/failure.bin" > "$WORK/failure.json"
FAIL_ID=$(field "$WORK/failure.json" .object_id)
remember "$FAIL_ID"
AFFECTED=$(jq --arg w "$VICTIM" '[.chunks[] | select(any(.locations[]; .worker_id == $w))] | length' < "$WORK/failure.json")

step "BEFORE the failure"
show_workers
oscli where "$FAIL_ID" | indent
check "$VICTIM holds a copy of $AFFECTED chunks of the test object" test "$AFFECTED" -gt 0
check "the replication factor is maintained (2 good copies of every chunk)" exactly_two_good_copies "$FAIL_ID"

step "docker stop $VICTIM"
STOPPED_AT=$SECONDS
docker stop "$VICTIM" > /dev/null
# Poll quickly so that each phase is observed: STALE (silent > 6 s: avoided, but its copies still
# count, nothing is repaired) and DEAD (silent > 12 s: its copies are forgotten and re-replicated).
UNDER_SEEN=0
DETECTED=0
STALE_SEEN=0
PENDING_WHILE_STALE=0
while [ $((SECONDS - STOPPED_AT)) -lt 40 ]; do
    SNAPSHOT=$(status)
    STATE=$(echo "$SNAPSHOT" | jq -r --arg w "$VICTIM" '.workers[] | select(.worker_id == $w) | .state')
    if [ "$STATE" = "STALE" ] && [ "$STALE_SEEN" = 0 ]; then
        STALE_SEEN=1
        note "$((SECONDS - STOPPED_AT)) s: $VICTIM is STALE - avoided for reads and writes, its copies still count, no repair"
        [ "$(echo "$SNAPSHOT" | jq .pending_replications)" = 0 ] || PENDING_WHILE_STALE=1
    fi
    if [ "$STATE" = "DEAD" ]; then
        DETECTED=1
        UNDER_SEEN=$(echo "$SNAPSHOT" | jq -r .under_replicated)
        break
    fi
    sleep 0.3
done

step "DURING the failure"
note "$VICTIM was declared DEAD $((SECONDS - STOPPED_AT)) s after it stopped (STALE after 6 s, DEAD after 12 s of silence)"
controller_log | grep -F "[registry] $VICTIM DEAD after" | tail -1 | cut -c1-160 | indent
show_workers
check "while only STALE, nothing was re-replicated" equal "$PENDING_WHILE_STALE" 0
check "the controller detected that $VICTIM is unavailable" equal "$DETECTED" 1
REPAIRED_ALREADY=$(count_repair_events copied "$FAIL_ID")
note "under-replicated chunks seen at the moment of detection: $UNDER_SEEN (re-replicated so far: $REPAIRED_ALREADY)"
check "the controller detected that the replication factor is no longer maintained" \
    test "$UNDER_SEEN" -gt 0 -o "$REPAIRED_ALREADY" -gt 0
oscli download "$FAIL_ID" "$WORK/failure.during" | jq -c '{bytes, etag_match}' | indent
check "the object is still served, from the remaining copies, and is identical" cmp -s "$WORK/failure.bin" "$WORK/failure.during"

step "Automatic re-replication: the monitor (every 3 s) orders a surviving holder to copy each chunk directly to another worker"
wait_until 60 "no chunk is under-replicated any more" no_chunk_under_replicated
repair_events copied "$FAIL_ID" | indent
SAMPLE=$(status | jq -r --arg p "$FAIL_ID" '[.repair_log[] | select(.event == "copied" and (.chunk_id | startswith($p)))][0] | "\(.chunk_id) \(.source)"')
docker logs "${SAMPLE#* }" 2>&1 | grep -F "replicate ok chunk=${SAMPLE% *}" | tail -1 | cut -c1-160 | indent
oscli where "$FAIL_ID" | indent
check "the copies went worker to worker (the source logged '[cmd] replicate ok')" \
    sh -c "docker logs ${SAMPLE#* } 2>&1 | grep -q 'replicate ok chunk=${SAMPLE% *}'"
check "one new copy was made for each of the $AFFECTED affected chunks" equal "$(count_repair_events copied "$FAIL_ID")" "$AFFECTED"
check "the replication factor is restored while $VICTIM is still down" \
    meta_has "$FAIL_ID" --arg w "$VICTIM" '.replication_ok and ([.chunks[]
        | ([.locations[] | select(.status == "OK" and .worker_status == "ALIVE" and .worker_id != $w)] | length) == 2] | all)'

step "docker start $VICTIM"
docker start "$VICTIM" > /dev/null
wait_until 90 "$VICTIM is ALIVE again" worker_is "$VICTIM" ALIVE
wait_until 90 "the surplus copies are trimmed and every chunk has exactly two copies" exactly_two_good_copies "$FAIL_ID"
wait_until 60 "the whole cluster is healthy" cluster_healthy

step "AFTER the failure"
controller_log | grep -F "[registry] $VICTIM is back" | tail -1 | indent
repair_events trimmed "$FAIL_ID" | indent
show_workers
oscli where "$FAIL_ID" | indent
check "$VICTIM's copies survived on its volume and came back in its block report; the surplus was trimmed" \
    test "$(count_repair_events trimmed "$FAIL_ID")" -eq "$AFFECTED"
check "the replication factor is exactly 2 again: no chunk under- or over-replicated" cluster_healthy
check "the files on the workers' disks match the database" copies_on_disk_are_intact "$FAIL_ID"
oscli download "$FAIL_ID" "$WORK/failure.after" > /dev/null
check "the object is still identical" cmp -s "$WORK/failure.bin" "$WORK/failure.after"
finish

# ============================================================================================
banner "6/7  Data integrity - corrupt stored data on purpose"

head -c 3500000 /dev/urandom > "$WORK/integrity.bin"
oscli upload "$WORK/integrity.bin" > "$WORK/integrity.json"
INT_ID=$(field "$WORK/integrity.json" .object_id)
remember "$INT_ID"

# --- A: found by a download ---------------------------------------------------------------
CHUNK_A=$(field "$WORK/integrity.json" '.chunks[0].chunk_id')
SUM_A=$(field "$WORK/integrity.json" '.chunks[0].checksum')
# Corrupt the copy the controller reads first (the least loaded holder), so the read path meets it.
WORKER_A=$(jq -r --argjson s "$(status)" '.chunks[0].locations | map(.worker_id) as $h
    | [$s.workers[] | select(.worker_id as $id | $h | index($id))] | sort_by(.chunk_count, .worker_id) | .[0].worker_id' < "$WORK/integrity.json")
step "A) Corrupt chunk 0 on $WORKER_A (overwrite 4 bytes inside the file), then download immediately"
note "before: sha256 on disk $(disk_sha256 "$WORKER_A" "$CHUNK_A")"
corrupt_chunk "$WORKER_A" "$CHUNK_A"
note "after:  sha256 on disk $(disk_sha256 "$WORKER_A" "$CHUNK_A")"
note "recorded at upload     $SUM_A"
oscli download "$INT_ID" "$WORK/integrity.out" | jq -c '{bytes, etag_match}' | indent
check "the corrupted copy was NOT returned: the download is identical to the original" cmp -s "$WORK/integrity.bin" "$WORK/integrity.out"
# The worker re-hashes the chunk before serving it, refuses (409 chunk_corrupt) and reports it bad;
# the controller marks the copy CORRUPT and reads the other copy instead.
DETECT_A="\[integrity\] chunk=$CHUNK_A (worker=|on )$WORKER_A"
wait_until 30 "the controller log shows the detection" sh -c "docker logs controller 2>&1 | grep -qE '$DETECT_A'"
docker logs "$WORKER_A" 2>&1 | grep -F "get chunk=$CHUNK_A" | tail -1 | cut -c1-160 | indent
controller_log | grep -E "$DETECT_A" | tail -1 | cut -c1-160 | indent
check "the corruption was detected and logged" sh -c "docker logs controller 2>&1 | grep -qE '$DETECT_A'"

step "Restoration: replace first, then delete the corrupt copy (as HDFS does)"
wait_until 60 "the corrupt copy is replaced and invalidated" \
    sh -c "[ \$(curl -fsS $API/cluster/status | jq '[.repair_log[] | select(.event == \"invalidated\" and .chunk_id == \"$CHUNK_A\")] | length') -ge 1 ]"
wait_until 60 "chunk 0 has two good copies again" exactly_two_good_copies "$INT_ID"
status | jq -r --arg c "$CHUNK_A" '.repair_log[] | select(.chunk_id == $c) | "\(.time)  \(.event)  \(.source // "-") -> \(.target // "-")  (\(.reason))"' | indent
SEQ_A=$(status | jq -r --arg c "$CHUNK_A" '[.repair_log[] | select(.chunk_id == $c) | .event] | join(" ")')
check "a new copy was made from the valid one before the corrupt copy was deleted" sh -c "echo '$SEQ_A' | grep -q 'copied.*invalidated'"
check "every copy on disk hashes to the recorded checksum again" copies_on_disk_are_intact "$INT_ID"

# --- B: found by the scrubber, nobody reads the object ------------------------------------
CHUNK_B=$(meta "$INT_ID" | jq -r '.chunks[2].chunk_id')
WORKER_B=$(meta "$INT_ID" | jq -r '.chunks[2].locations[1].worker_id')
step "B) Corrupt chunk 2 on $WORKER_B and do NOT read the object: the worker's block scanner must find it (every 10 s)"
corrupt_chunk "$WORKER_B" "$CHUNK_B"
wait_until 40 "the block scanner reports the corrupt copy" \
    sh -c "docker logs controller 2>&1 | grep -qF '[integrity] chunk=$CHUNK_B on $WORKER_B CORRUPT (reported by worker)'"
docker logs "$WORKER_B" 2>&1 | grep -F "[scanner] CORRUPT chunk=$CHUNK_B" | tail -1 | cut -c1-140 | indent
controller_log | grep -F "[integrity] chunk=$CHUNK_B" | tail -1 | cut -c1-160 | indent
check "the block scanner detected the corruption without any download" \
    sh -c "docker logs $WORKER_B 2>&1 | grep -qF '[scanner] CORRUPT chunk=$CHUNK_B'"
wait_until 60 "the chunk has two good copies again" exactly_two_good_copies "$INT_ID"
wait_until 60 "the whole cluster is healthy" cluster_healthy
repair_events copied "$INT_ID" | tail -1 | indent
oscli where "$INT_ID" | indent
check "replication_ok is true again" meta_has "$INT_ID" '.replication_ok'
check "every copy on disk hashes to the recorded checksum" copies_on_disk_are_intact "$INT_ID"
oscli download "$INT_ID" "$WORK/integrity.final" > /dev/null
check "the object is still identical to the original" cmp -s "$WORK/integrity.bin" "$WORK/integrity.final"
finish

# ============================================================================================
banner "7/7  Controller crash - docker kill controller in the middle of an upload"

head -c $((100 * MIB)) /dev/urandom > "$WORK/crash.bin"
KEY="demo7-$(date +%s)-$RANDOM"
OBJECTS_BEFORE=$(curl -fsS "$API/objects?limit=500" | jq '[.items[] | select(.name == "crash.bin")] | length')
step "Upload 100 MB with Idempotency-Key $KEY, and kill the controller while chunks are being written"
FILES_BEFORE=$(docker exec worker1 sh -c 'ls /data/chunks | wc -l')
OSCLI_RETRY_SECONDS=120 oscli upload "$WORK/crash.bin" --key "$KEY" > "$WORK/crash.json" 2> "$WORK/crash.err" &
UPLOAD_PID=$!
# Wait until the controller is writing this upload's chunks to the workers, then kill it.
for _ in $(seq 1 300); do
    [ "$(docker exec worker1 sh -c 'ls /data/chunks | wc -l')" -gt "$FILES_BEFORE" ] && break
    sleep 0.05
done
docker kill controller > /dev/null
note "docker kill controller: the upload was in progress (the client sees its connection drop)"
sleep 2
step "docker start controller: it comes back in safe mode"
docker start controller > /dev/null
wait_until 60 "the controller answers" curl -fsS -m 2 "$BASE_URL/health"
SAFE=$(status | jq .safe_mode)
note "safe_mode = $SAFE  (no writes, no repairs, no deletions until the workers have reported their chunks)"
check "the restarted controller is in safe mode" equal "$SAFE" true
wait_until 60 "safe mode is over" sh -c "[ \$(curl -fsS $API/cluster/status | jq .safe_mode) = false ]"
controller_log | grep -F "[safemode] OFF" | tail -1 | cut -c1-140 | indent

step "The client retried with the same Idempotency-Key"
if wait "$UPLOAD_PID"; then UPLOAD_OK=1; else UPLOAD_OK=0; fi
grep -E '^note:' "$WORK/crash.err" | sort | uniq -c | sed 's/^ */  /' | indent
CRASH_ID=$(jq -r .object_id "$WORK/crash.json" 2>/dev/null || echo "")
remember "$CRASH_ID"
OBJECTS_AFTER=$(curl -fsS "$API/objects?limit=500" | jq '[.items[] | select(.name == "crash.bin")] | length')
check "the upload eventually succeeded" equal "$UPLOAD_OK" 1
check "exactly one object exists for it (no duplicate, no half-written object)" equal "$((OBJECTS_AFTER - OBJECTS_BEFORE))" 1
curl -fsS -o "$WORK/crash.out" "$API/objects/$CRASH_ID"
check "it downloads byte-identical" cmp -s "$WORK/crash.bin" "$WORK/crash.out"

step "fsck: the chunks the killed attempt had written are orphans; workers report them and they are deleted"
note "(orphan deletion waits ORPHAN_GC_DELAY = 60 s after a controller start, then the next full block report, every 30 s)"
fsck_healthy() { curl -fsS -m 60 "$API/fsck" | jq -e '.status == "HEALTHY" and .quiescent and .orphans_on_disk == 0
    and (.disk_vs_map | [.[] | length] | add) == 0' > /dev/null; }
wait_until 180 "fsck reports HEALTHY with no orphans" fsck_healthy
curl -fsS "$API/fsck" | jq -c '{status, quiescent, objects, orphans_on_disk, unreachable_workers, invariant_violations}' | indent
check "fsck is HEALTHY: every disk matches the controller's map" fsck_healthy
check "no invariant was ever violated" sh -c "[ \$(curl -fsS $API/cluster/status | jq .counters.invariant_violations) = 0 ]"
finish

# ============================================================================================
printf '\n%s================================================================================%s\n' "$BOLD" "$OFF"
for line in "${RESULTS[@]}"; do
    printf ' %s\n' "$line"
done
printf '%s %d/%d PASS%s\n' "$BOLD" "$PASSED" "$TOTAL" "$OFF"
[ "$PASSED" = "$TOTAL" ]
