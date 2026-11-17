#!/usr/bin/env bash
# Block 9: fsck. (AC-9.2, the chaos test, is tests/chaos.py.)
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

fsck() { curl -fsS -m 60 "$API/fsck"; }

ensure_up
random_file "$WORK/a.bin" $((4 * MIB))
OBJ=$(upload "$WORK/a.bin")
wait_for 60 healthy

section "AC-9.1 fsck: HEALTHY on a quiet cluster"
wait_for 60 sh -c "curl -fsS $API/fsck | jq -e '.quiescent' > /dev/null"
fsck | jq -c '{status, quiescent, objects, orphans_on_disk}' | sed 's/^/  /'
check "HEALTHY" sh -c "curl -fsS $API/fsck | jq -e '.status == \"HEALTHY\" and .quiescent and .orphans_on_disk == 0' > /dev/null"

section "AC-9.1 fsck: DEGRADED while a worker is DEAD and not yet repaired"
docker stop worker2 > /dev/null
result=""
for _ in $(seq 1 160); do
    if [ "$(worker_field worker2 state)" = DEAD ]; then result=$(fsck); break; fi
    sleep 0.2
done
echo "$result" | jq -c '{status, objects, unreachable_workers}' | sed 's/^/  /'
check "DEGRADED, with under-replicated objects" sh -c "echo '$result' | jq -e '.status == \"DEGRADED\" and .objects.under_replicated > 0' > /dev/null"
docker start worker2 > /dev/null; wait_for 120 healthy

section "AC-9.1 fsck lists a planted orphan and a corrupted chunk"
JUNK="$(python3 -c 'import uuid; print(uuid.uuid4())')-00000"
docker exec worker4 sh -c "head -c 1000 /dev/urandom > /data/chunks/$JUNK && sha256sum /data/chunks/$JUNK | cut -d' ' -f1 | tr -d '\n' > /data/chunks/$JUNK.meta && echo ' 1000' >> /data/chunks/$JUNK.meta"
C="$OBJ-00001"; W=$(meta "$OBJ" | jq -r '.chunks[1].locations[0].worker_id')
corrupt_chunk "$W" "$C"
result=$(fsck)
echo "$result" | jq -c '{status, orphans_on_disk, checksum_mismatch: .disk_vs_map.checksum_mismatch}' | sed 's/^/  /'
check "the orphan is counted" sh -c "echo '$result' | jq -e '.orphans_on_disk >= 1' > /dev/null"
check "the corrupted copy is listed as $C@$W" sh -c "echo '$result' | jq -e --arg x '$C@$W' '.disk_vs_map.checksum_mismatch | index(\$x) != null' > /dev/null"
check "the status is not HEALTHY" sh -c "echo '$result' | jq -e '.status != \"HEALTHY\"' > /dev/null"
check "the cluster repairs itself and fsck is HEALTHY again (orphan GC included)" \
    wait_for 180 sh -c "curl -fsS -m 60 $API/fsck | jq -e '.status == \"HEALTHY\" and .quiescent and .orphans_on_disk == 0 and ([.disk_vs_map[] | length] | add) == 0' > /dev/null"
check "invariant_violations == 0" no_invariant_violations
summary
