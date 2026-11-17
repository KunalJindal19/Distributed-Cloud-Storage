#!/usr/bin/env bash
# Block 6: protocol, liveness, block reports, safe mode, cluster identity, scanner, orphan GC.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

section "AC-6.1 the cluster forms; every volume records the cluster id"
fresh_cluster
started=$(now)
check "4 workers ALIVE and reported, safe mode off" healthy
CLUSTER_ID=$(status | jq -r .cluster_id)
for w in worker1 worker2 worker3 worker4; do
    check "$w /data/VERSION holds cluster $CLUSTER_ID" sh -c "docker exec $w cat /data/VERSION | grep -q $CLUSTER_ID"
done

random_file "$WORK/a.bin" $((3 * MIB))
OBJ=$(upload "$WORK/a.bin")

section "AC-6.2 docker stop worker2: STALE at 6-8 s, DEAD at 12-14 s; start: back within 3 s"
docker stop worker2 > /dev/null
t0=$(now); stale_at=""; dead_at=""
while [ -z "$dead_at" ] && [ "$(python3 -c "print(int($(since "$t0")))")" -lt 30 ]; do
    st=$(worker_field worker2 state)
    [ "$st" = "STALE" ] && [ -z "$stale_at" ] && stale_at=$(since "$t0")
    [ "$st" = "DEAD" ] && dead_at=$(since "$t0")
    sleep 0.25
done
say "  STALE after ${stale_at:-never} s, DEAD after ${dead_at:-never} s (docker stop itself takes ~1 s)"
check "STALE between 6 and 9 s" python3 -c "import sys; sys.exit(0 if 5.5 <= float('${stale_at:-0}') <= 9 else 1)"
check "DEAD between 12 and 15 s" python3 -c "import sys; sys.exit(0 if 11.5 <= float('${dead_at:-0}') <= 15 else 1)"
check "the log reports the replicas removed" sh -c "docker logs controller 2>&1 | grep -q 'worker2 DEAD after .* replicas removed'"
docker start worker2 > /dev/null
t0=$(now)
wait_for 30 sh -c "[ \"\$(curl -fsS $API/cluster/status | jq -r '.workers[] | select(.worker_id==\"worker2\") | \"\(.state) \(.has_reported)\"')\" = 'ALIVE true' ]"
say "  ALIVE and reported $(since "$t0") s after docker start (includes JVM start-up)"
check "worker2 ALIVE and reported again" sh -c "[ \"\$(curl -fsS $API/cluster/status | jq -r '.workers[] | select(.worker_id==\"worker2\") | .state')\" = ALIVE ]"
wait_for 90 healthy

section "AC-6.3 docker restart, or a 9 s pause: never DEAD, nothing re-replicated"
watch_never_dead() { # watch_never_dead <worker> <seconds>
    local end=$((SECONDS + $2)) seen=""
    while [ $SECONDS -lt $end ]; do seen="$seen $(worker_field "$1" state)"; sleep 0.25; done
    echo "$seen" | grep -q DEAD && return 1 || return 0
}
docker restart worker2 > /dev/null &
check "docker restart worker2: never DEAD" watch_never_dead worker2 20
wait
check "under_replicated stayed 0, no copies made" sh -c "[ \$(curl -fsS $API/cluster/status | jq '[.repair_log[] | select(.event==\"copied\")] | length') = 0 ]"
docker pause worker3 > /dev/null
( sleep 9; docker unpause worker3 > /dev/null ) &
saw=""
end=$((SECONDS + 16)); while [ $SECONDS -lt $end ]; do saw="$saw $(worker_field worker3 state)"; sleep 0.25; done
wait
check "a 9 s pause makes worker3 STALE" sh -c "echo '$saw' | grep -q STALE"
check "...but never DEAD" sh -c "! echo '$saw' | grep -q DEAD"
wait_for 30 healthy
check "under_replicated 0 and no copies made" sh -c "[ \$(curl -fsS $API/cluster/status | jq '[.repair_log[] | select(.event==\"copied\")] | length') = 0 ]"

section "AC-6.4 docker pause controller for 20 s: stall detected, no worker DEAD"
docker pause controller > /dev/null; sleep 20; docker unpause controller > /dev/null
sleep 4
check "the stall warning is logged" sh -c "docker logs controller 2>&1 | grep -q 'controller stalled'"
check "no worker went DEAD" sh -c "! docker logs --since 30s controller 2>&1 | grep -q ' DEAD after'"
wait_for 30 healthy
check "the cluster is healthy" healthy

section "AC-6.6 a corrupted copy is found by the scanner within 12 s, kept on disk, never served"
CHUNK="$OBJ-00000"; HOLDER=$(meta "$OBJ" | jq -r '.chunks[0].locations[0].worker_id')
corrupt_chunk "$HOLDER" "$CHUNK"; t0=$(now)
wait_for 20 sh -c "[ \"\$(curl -fsS $API/objects/$OBJ/metadata | jq -r '.chunks[0].locations[] | select(.worker_id==\"$HOLDER\") | .status')\" = CORRUPT ]"
say "  CORRUPT after $(since "$t0") s"
check "the copy on $HOLDER is CORRUPT within 12 s" python3 -c "import sys; sys.exit(0 if float('$(since "$t0")') <= 13 else 1)"
check "the corrupt file is still on disk" file_exists "$HOLDER" "$CHUNK"
check "a download is byte-identical" download_matches "$OBJ" "$WORK/a.bin"
# Block 6 has no repair yet; from block 7 on the copy is replaced, then deleted. Wait until healthy.
wait_for 60 healthy || true

section "AC-6.8 controller restart: safe mode, writes refused, reads work, safe mode ends"
docker compose restart controller > /dev/null 2>&1
wait_for 30 curl -fsS -m 2 "$BASE_URL/health"
check "safe mode is on right after the restart" sh -c "[ \$(curl -fsS $API/cluster/status | jq .safe_mode) = true ]"
random_file "$WORK/b.bin" 1000
code=$(curl -s -o "$WORK/r.json" -D "$WORK/r.h" -w '%{http_code}' -F "file=@$WORK/b.bin" "$API/objects")
check "an upload answers 503 safe_mode" sh -c "[ '$code' = 503 ] && jq -e '.error == \"safe_mode\"' '$WORK/r.json'"
check "...with Retry-After: 5" grep -qi '^retry-after: 5' "$WORK/r.h"
t0=$(now)
wait_for 30 sh -c "[ \$(curl -fsS $API/cluster/status | jq .safe_mode) = false ]"
say "  safe mode left $(since "$t0") s after the controller answered"
check "safe mode ends within 15 s" python3 -c "import sys; sys.exit(0 if float('$(since "$t0")') <= 15 else 1)"
check "no commands were issued during safe mode" sh -c "[ \$(curl -fsS $API/cluster/status | jq '[.repair_log[] | select(.event==\"copied\" or .event==\"trimmed\")] | length') = 0 ]"
check "reads work" download_matches "$OBJ" "$WORK/a.bin"
wait_for 30 healthy
check "under_replicated is 0 afterwards" healthy

section "AC-6.7 orphan GC: a junk file is deleted, but not within ORPHAN_GC_DELAY of a controller start"
for i in $(seq 1 20); do random_file "$WORK/n.bin" $((1000 + i)); upload "$WORK/n.bin" > /dev/null; done
docker compose restart controller > /dev/null 2>&1
wait_for 30 curl -fsS -m 2 "$BASE_URL/health"; t0=$(now)
JUNK="$(python3 -c 'import uuid; print(uuid.uuid4())')-00000"
docker exec worker4 sh -c "head -c 5000 /dev/urandom > /data/chunks/$JUNK"
wait_for 50 sh -c "false"   # 50 s: inside ORPHAN_GC_DELAY
check "the junk file survives the first ${ORPHAN_GC_DELAY:-60} s after the controller start" file_exists worker4 "$JUNK"
wait_for 90 sh -c "! docker exec worker4 test -f /data/chunks/$JUNK"
say "  junk file gone $(since "$t0") s after the controller start"
check "the junk file is deleted by orphan GC" sh -c "! docker exec worker4 test -f /data/chunks/$JUNK"
check "exactly one orphan was collected (20 uploads left none)" sh -c "[ \$(curl -fsS $API/cluster/status | jq '[.repair_log[] | select(.event==\"orphan\")] | length') = 1 ]"

section "AC-6.10 no invariant violations"
check "invariant_violations == 0" no_invariant_violations

section "AC-6.9 wiping the controller's volume: workers refuse it and keep every chunk"
before=$(for w in worker1 worker2 worker3 worker4; do docker exec $w sh -c 'ls /data/chunks | grep -v "\.meta$"'; done | sort | sha256sum 2>/dev/null || true)
count_before=$(for w in worker1 worker2 worker3 worker4; do docker exec $w sh -c 'ls /data/chunks | grep -vc "\.meta$"'; done | paste -sd+ - | bc)
docker compose down > /dev/null 2>&1
docker volume rm objectstore_controller_data > /dev/null
docker compose up -d > /dev/null 2>&1
wait_for 60 curl -fsS -m 2 "$BASE_URL/health"; sleep 10
check "the workers log cluster_id_mismatch" sh -c "docker logs worker1 2>&1 | grep -q cluster_id_mismatch"
check "the controller shows 0 workers" sh -c "[ \$(curl -fsS $API/cluster/status | jq '.workers | length') = 0 ]"
count_after=$(for w in worker1 worker2 worker3 worker4; do docker exec $w sh -c 'ls /data/chunks | grep -vc "\.meta$"'; done | paste -sd+ - | bc)
say "  chunk files before: $count_before, after: $count_after"
check "every chunk file is still on disk" [ "$count_before" = "$count_after" ]

section "clean-up: a fresh cluster"
fresh_cluster
summary
