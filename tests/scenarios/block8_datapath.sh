#!/usr/bin/env bash
# Block 8: upload pipeline recovery, idempotency, durable commit, download classification,
# delete via commands, client retries.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

files_of() { # files_of <object-id prefix>: chunk files of an object on all workers
    for w in worker1 worker2 worker3 worker4; do
        docker exec "$w" sh -c "ls /data/chunks | grep -v '\.meta$' | grep '^$1' || true" 2>/dev/null
    done | grep -c . || true
}
fsck_orphans() { curl -fsS -m 60 "$API/fsck" | jq .orphans_on_disk; }
objects_named() { curl -fsS "$API/objects?limit=500" | jq --arg n "$1" '[.items[] | select(.name == $n)] | length'; }
# Wait until the controller writes an upload's chunks (file count on worker1 grows), then run a command.
when_writing() {
    local before; before=$(docker exec worker1 sh -c 'ls /data/chunks | wc -l')
    for _ in $(seq 1 400); do
        [ "$(docker exec worker1 sh -c 'ls /data/chunks | wc -l')" -gt "$before" ] && break
        sleep 0.05
    done
    "$@"
}

fresh_cluster
random_file "$WORK/50.bin" $((50 * MIB))

section "AC-8.1 a target is stopped during a 50 MB upload: still 201, every chunk ends with 2 copies"
curl -s -m 180 -o "$WORK/r1.json" -w '%{http_code}' -F "file=@$WORK/50.bin;filename=recovery.bin" "$API/objects" > "$WORK/r1.code" &
CURL=$!
when_writing docker stop worker2 > /dev/null
wait $CURL
say "  upload answered $(cat "$WORK/r1.code")"
check "the upload answered 201 (failed target replaced)" [ "$(cat "$WORK/r1.code")" = 201 ]
ID1=$(jq -r .object_id "$WORK/r1.json")
check "the controller logged the replacement" sh -c "docker logs controller 2>&1 | grep -q 'replacing it for this upload'"
check "the object downloads intact" download_matches "$ID1" "$WORK/50.bin"
check "after worker2 is DEAD and repairs finish, every chunk has 2 copies" \
    wait_for 120 sh -c "curl -fsS $API/objects/$ID1/metadata | jq -e '.replication_ok and ([.chunks[] | [.locations[] | select(.status==\"OK\" and .worker_status==\"ALIVE\")] | length == 2] | all)' > /dev/null"
docker start worker2 > /dev/null; wait_for 120 healthy

section "AC-8.2 only 2 workers alive and one stops mid-upload: 503 upload_failed, nothing committed, leftovers collected"
docker stop worker3 worker4 > /dev/null
wait_for 30 sh -c "[ \$(curl -fsS $API/cluster/status | jq '[.workers[] | select(.state==\"DEAD\")] | length') = 2 ]"
curl -s -m 180 -o "$WORK/r2.json" -w '%{http_code}' -F "file=@$WORK/50.bin;filename=doomed.bin" "$API/objects" > "$WORK/r2.code" &
CURL=$!
when_writing docker stop worker2 > /dev/null
wait $CURL
code=$(cat "$WORK/r2.code")
say "  upload answered $code: $(jq -c . "$WORK/r2.json" 2>/dev/null | cut -c1-120)"
check "503 upload_failed" sh -c "[ '$code' = 503 ] && jq -e '.error == \"upload_failed\"' '$WORK/r2.json' > /dev/null"
check "no object was recorded" [ "$(objects_named doomed.bin)" = 0 ]
docker start worker2 worker3 worker4 > /dev/null
wait_for 120 healthy
t0=$(now)
check "leftover chunks are garbage-collected (ORPHAN_GC_DELAY + one resync)" wait_for 150 sh -c "[ \$(curl -fsS -m 60 $API/fsck | jq .orphans_on_disk) = 0 ]"
say "  orphans gone $(since "$t0") s after the workers were back"

section "AC-8.3 Idempotency-Key: 201, then 200 replay with the same id, then 409 for another size"
random_file "$WORK/k.bin" 3000; random_file "$WORK/k2.bin" 4000; KEY="scenario-$RANDOM-$RANDOM"
c1=$(curl -s -o "$WORK/k1.json" -w '%{http_code}' -H "Idempotency-Key: $KEY" -F "file=@$WORK/k.bin" "$API/objects")
c2=$(curl -s -D "$WORK/k2.h" -o "$WORK/k2.json" -w '%{http_code}' -H "Idempotency-Key: $KEY" -F "file=@$WORK/k.bin" "$API/objects")
c3=$(curl -s -o "$WORK/k3.json" -w '%{http_code}' -H "Idempotency-Key: $KEY" -F "file=@$WORK/k2.bin" "$API/objects")
say "  answers: $c1, $c2, $c3"
check "first upload 201" [ "$c1" = 201 ]
check "same key again: 200 with Idempotent-Replay: true and the same object id" sh -c "[ '$c2' = 200 ] && grep -qi '^idempotent-replay: true' '$WORK/k2.h' && [ \$(jq -r .object_id '$WORK/k1.json') = \$(jq -r .object_id '$WORK/k2.json') ]"
check "same key, different size: 409 conflict" sh -c "[ '$c3' = 409 ] && jq -e '.error == \"conflict\"' '$WORK/k3.json' > /dev/null"

section "AC-8.4 docker kill controller during a 30 MB upload; the client retries with the same key: one object"
random_file "$WORK/30.bin" $((30 * MIB))
OSCLI_RETRY_SECONDS=120 ./oscli upload "$REL/30.bin" --name crash30.bin --key "crash-$RANDOM-$RANDOM" > "$WORK/c.json" 2> "$WORK/c.err" &
CLIENT=$!
when_writing docker kill controller > /dev/null
sleep 2; docker start controller > /dev/null
wait $CLIENT; rc=$?
say "  client exit code $rc; its notes: $(grep -c '^note:' "$WORK/c.err") retries"
check "the client succeeded after retrying" [ "$rc" = 0 ]
check "exactly one object exists" [ "$(objects_named crash30.bin)" = 1 ]
check "it downloads intact" download_matches "$(jq -r .object_id "$WORK/c.json")" "$WORK/30.bin"
check "no orphans remain after GC" wait_for 150 sh -c "[ \$(curl -fsS -m 60 $API/fsck | jq .orphans_on_disk) = 0 ]"

section "AC-8.5 the controller is killed right after a 201: the object survives"
wait_for 120 healthy
random_file "$WORK/5.bin" $((5 * MIB))
ID5=$(upload "$WORK/5.bin")
docker kill controller > /dev/null; docker start controller > /dev/null
check "after the restart the object downloads byte-identical" wait_for 60 download_matches "$ID5" "$WORK/5.bin"

section "AC-8.7 delete while a holder is DEAD: 204; live files gone within 3 s; the dead holder's after it returns"
wait_for 120 healthy   # AC-8.5 restarted the controller: wait until it has left safe mode
random_file "$WORK/d.bin" $((3 * MIB))
ID7=$(upload "$WORK/d.bin"); DOWN=$(meta "$ID7" | jq -r '.chunks[0].locations[0].worker_id')
docker stop "$DOWN" > /dev/null
wait_for 30 sh -c "[ \"\$(curl -fsS $API/cluster/status | jq -r '.workers[]|select(.worker_id==\"$DOWN\")|.state')\" = DEAD ]"
code=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$API/objects/$ID7"); t0=$(now)
check "DELETE answered 204" [ "$code" = 204 ]
wait_for 5 sh -c "[ \$(for w in worker1 worker2 worker3 worker4; do [ \$w = $DOWN ] || docker exec \$w sh -c 'ls /data/chunks | grep -c ^$ID7 || true'; done | paste -sd+ - | bc) = 0 ]"
say "  live holders' files gone after $(since "$t0") s"
check "the live holders' files are gone within 3 s" python3 -c "import sys; sys.exit(0 if float('$(since "$t0")') <= 3.5 else 1)"
docker start "$DOWN" > /dev/null
check "the returning worker's files are collected as orphans" wait_for 150 sh -c "[ \$(docker exec $DOWN sh -c 'ls /data/chunks | grep -c ^$ID7 || true') = 0 ]"

section "AC-8.8 the client retries through safe mode and verifies downloads"
wait_for 120 healthy
docker compose restart controller > /dev/null 2>&1
random_file "$WORK/s.bin" 2000
check "an upload issued during safe mode succeeds after retrying" sh -c "OSCLI_RETRY_SECONDS=60 ./oscli upload '$REL/s.bin' > '$WORK/s.json' 2> '$WORK/s.err' && grep -q 'answered 503' '$WORK/s.err'"
check "the download is verified against the ETag" sh -c "./oscli download \$(jq -r .object_id '$WORK/s.json') '$REL/s.out' | jq -e .etag_match > /dev/null"

section "AC-8.6 network faults never make a download report a copy for quarantine"
docker compose -f docker-compose.yml -f docker-compose.chaos.yml up -d --wait > /dev/null 2>&1
wait_for 120 healthy
ids=(); for i in $(seq 1 8); do random_file "$WORK/f$i.bin" $((i * 300000)); ids+=("$(OSCLI_RETRY_SECONDS=60 ./oscli upload "$REL/f$i.bin" 2>/dev/null | jq -r .object_id)"); done
bad=0; for round in 1 2 3; do for i in $(seq 1 8); do
    OSCLI_RETRY_SECONDS=60 ./oscli download "${ids[$((i-1))]}" "$REL/f.out" > /dev/null 2>&1 && cmp -s "$WORK/f.out" "$WORK/f$i.bin" || bad=$((bad+1))
done; done
check "24 downloads under faults are all intact" [ "$bad" = 0 ]
check "no copy was reported for quarantine by the read path" sh -c "! docker logs controller 2>&1 | grep -q -- '-> quarantine'"
check "replicas.corrupt is 0" sh -c "[ \$(curl -fsS $API/cluster/status | jq .replicas.corrupt) = 0 ]"
docker compose up -d --wait > /dev/null 2>&1

section "invariants"
wait_for 60 healthy
check "invariant_violations == 0" no_invariant_violations
summary
