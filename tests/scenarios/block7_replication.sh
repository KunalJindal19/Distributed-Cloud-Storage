#!/usr/bin/env bash
# Block 7: the replication monitor and worker-to-worker copies.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

ok_copies_min() { # fewest OK copies of any chunk of an object
    meta "$1" | jq '[.chunks[] | [.locations[] | select(.status == "OK")] | length] | min // 0'
}
event_count() { status | jq --arg e "$1" --arg p "${2:-}" '[.repair_log[] | select(.event == $e and (.chunk_id | startswith($p)))] | length'; }

fresh_cluster   # the timelines below assume no repair backlog from earlier runs
random_file "$WORK/a.bin" $((3 * MIB + 99))
OBJ=$(upload "$WORK/a.bin")
AFFECTED=$(meta "$OBJ" | jq '[.chunks[] | select(any(.locations[]; .worker_id == "worker2"))] | length')

section "AC-7.1 timeline after docker stop worker2 (object $OBJ, $AFFECTED chunks affected)"
min_ok=99; all_identical=1
# t=0 is the moment "docker stop" is issued: the worker stops heartbeating as it begins to shut down.
t0=$(now); docker stop worker2 > /dev/null
stale=""; dead=""; pend=""; repaired=""; pending_while_stale=0; stale_silence=0; dead_silence=0
while [ -z "$repaired" ] && [ $(python3 -c "print(int($(since "$t0")))") -lt 60 ]; do
    s=$(status); st=$(echo "$s" | jq -r '.workers[] | select(.worker_id=="worker2") | .state')
    ago=$(echo "$s" | jq -r '.workers[] | select(.worker_id=="worker2") | .last_seen_s_ago')
    [ "$st" = STALE ] && [ -z "$stale" ] && stale=$(since "$t0") && stale_silence=$ago
    [ "$st" = STALE ] && [ "$(echo "$s" | jq .pending_replications)" != 0 ] && pending_while_stale=1
    [ "$st" = DEAD ] && [ -z "$dead" ] && dead=$(since "$t0") && dead_silence=$ago
    [ -n "$dead" ] && [ -z "$pend" ] && [ "$(echo "$s" | jq '[.repair_log[] | select(.event=="replicate")] | length')" != 0 ] && pend=$(since "$t0")
    [ -n "$dead" ] && [ "$(echo "$s" | jq .under_replicated)" = 0 ] && [ "$(echo "$s" | jq .pending_replications)" = 0 ] && repaired=$(since "$t0")
    m=$(ok_copies_min "$OBJ"); [ "$m" -lt "$min_ok" ] && min_ok=$m
    sleep 0.5
done
download_matches "$OBJ" "$WORK/a.bin" || all_identical=0
say "  after docker stop: STALE ${stale:-never} s, DEAD ${dead:-never} s, copies ordered ${pend:-never} s, repaired ${repaired:-never} s"
say "  silence since the last heartbeat when declared: STALE ${stale_silence} s, DEAD ${dead_silence} s"
# The worker's last heartbeat came 0-2 s before "docker stop", so the transitions are judged by
# the silence the controller measured, which is what the liveness rule is defined on.
check "STALE after 6-8 s of silence, with no pending copies" sh -c "python3 -c 'import sys; sys.exit(0 if 6 <= float(\"$stale_silence\") <= 8 else 1)' && [ $pending_while_stale = 0 ]"
check "DEAD after 12-14 s of silence" python3 -c "import sys; sys.exit(0 if 12 <= float('$dead_silence') <= 14 else 1)"
check "copies ordered by 17 s" python3 -c "import sys; sys.exit(0 if 0 < float('${pend:-0}') <= 17.5 else 1)"
check "under_replicated 0 by 25 s" python3 -c "import sys; sys.exit(0 if 0 < float('${repaired:-0}') <= 25 else 1)"
check "downloads stayed byte-identical" [ "$all_identical" = 1 ]

section "AC-7.2 copies flow worker to worker"
C=$(status | jq -r '[.repair_log[] | select(.event=="copied")][0].chunk_id')
SRC=$(status | jq -r --arg c "$C" '[.repair_log[] | select(.event=="copied" and .chunk_id==$c)][0].source')
check "the monitor ordered REPLICATE for $C" sh -c "docker logs controller 2>&1 | grep -q 'REPLICATE chunk=$C'"
check "the source $SRC logged '[cmd] replicate ok'" sh -c "docker logs $SRC 2>&1 | grep -q 'replicate ok chunk=$C'"
check "the controller recorded the copy as confirmed" sh -c "docker logs controller 2>&1 | grep -q 'copied chunk=$C'"
check "one copy per affected chunk" [ "$(event_count copied "$OBJ")" = "$AFFECTED" ]

section "AC-7.3 docker start worker2: surplus trimmed within 10 s, never fewer than 1 OK copy"
docker start worker2 > /dev/null; t0=$(now)
wait_for 30 sh -c "[ \"\$(curl -fsS $API/cluster/status | jq -r '.workers[]|select(.worker_id==\"worker2\")|.has_reported')\" = true ]"; t1=$(now)
min_ok=99
end=$((SECONDS + 30)); trimmed=""
while [ $SECONDS -lt $end ] && [ -z "$trimmed" ]; do
    m=$(ok_copies_min "$OBJ"); [ "$m" -lt "$min_ok" ] && min_ok=$m
    status | jq -e '.over_replicated == 0 and .pending_replications == 0' > /dev/null \
        && [ "$(event_count trimmed "$OBJ")" = "$AFFECTED" ] && trimmed=$(since "$t1")
    sleep 0.5
done
say "  reported again $(python3 -c "print(round($t1-$t0,1))") s after docker start; trimmed ${trimmed:-never} s after that"
check "over_replicated 0 within 10 s of the worker reporting" python3 -c "import sys; sys.exit(0 if 0 <= float('${trimmed:-99}') <= 10 else 1)"
check "never fewer than 1 OK copy" [ "$min_ok" -ge 1 ]
check "every chunk has exactly 2 copies" sh -c "curl -fsS $API/objects/$OBJ/metadata | jq -e '[.chunks[] | (.locations|length) == 2] | all'"
wait_for 60 healthy

section "AC-7.4 40 MB object, a worker stopped: at most MAX_STREAMS copies per source, repaired within 60 s"
random_file "$WORK/big.bin" $((40 * MIB))
BIG=$(upload "$WORK/big.bin")
docker stop worker3 > /dev/null; t0=$(now); worst=0
until [ "$(worker_field worker3 state)" = DEAD ] && status | jq -e '.under_replicated == 0 and .pending_replications == 0' > /dev/null; do
    w=$(status | jq '[.workers[].pending_as_source] | max'); [ "$w" -gt "$worst" ] && worst=$w
    [ $(python3 -c "print(int($(since "$t0")))") -gt 90 ] && break
    sleep 0.3
done
say "  repaired $(since "$t0") s after docker stop; most copies in flight from one source: $worst"
check "pending_as_source never above 2" [ "$worst" -le 2 ]
check "fully repaired within 60 s" python3 -c "import sys; sys.exit(0 if float('$(since "$t0")') <= 60 else 1)"
check "the 40 MB object is intact" download_matches "$BIG" "$WORK/big.bin"
docker start worker3 > /dev/null; wait_for 90 healthy

section "AC-7.5 a corrupt copy is replaced first, invalidated only afterwards"
C="$OBJ-00001"; BAD=$(meta "$OBJ" | jq -r '.chunks[1].locations[0].worker_id')
corrupt_chunk "$BAD" "$C"; min_ok=99
wait_for 40 sh -c "[ \$(curl -fsS $API/cluster/status | jq '[.repair_log[] | select(.event==\"invalidated\" and .chunk_id==\"$C\")] | length') -ge 1 ]" &
WAITER=$!
while kill -0 $WAITER 2>/dev/null; do m=$(ok_copies_min "$OBJ"); [ "$m" -lt "$min_ok" ] && min_ok=$m; sleep 0.3; done
seq=$(status | jq -r --arg c "$C" '[.repair_log[] | select(.chunk_id==$c) | .event] | join(" ")')
say "  events for $C: $seq"
check "copied before invalidated" sh -c "echo '$seq' | grep -q 'copied.*invalidated'"
check "the chunk never had fewer than 1 OK copy" [ "$min_ok" -ge 1 ]
check "the corrupt file is gone from $BAD" sh -c "! docker exec $BAD test -f /data/chunks/$C"
wait_for 60 healthy

section "AC-7.6 docker network disconnect worker3: DEAD, repaired; reconnect: re-registers, trimmed"
docker network disconnect objectstore_objectstore-net worker3
wait_for 25 sh -c "[ \"\$(curl -fsS $API/cluster/status | jq -r '.workers[]|select(.worker_id==\"worker3\")|.state')\" = DEAD ]"
check "worker3 DEAD while partitioned" [ "$(worker_field worker3 state)" = DEAD ]
# worker3 held ~25 copies (half of the 40 MB object from AC-7.4); at MAX_STREAMS per source this takes ~30 s.
check "repaired while partitioned" wait_for 90 sh -c "curl -fsS $API/cluster/status | jq -e '.under_replicated == 0 and .pending_replications == 0' > /dev/null"
docker network connect objectstore_objectstore-net worker3
check "re-registers, reports, surplus trimmed: healthy again" wait_for 90 healthy
check "re-registration logged" sh -c "docker logs worker3 2>&1 | grep -q 'registering again'"

section "AC-7.7 both holders down: missing, 503, 'unrecoverable' once, low CPU; one returns: repaired"
random_file "$WORK/m.bin" 1000
M=$(upload "$WORK/m.bin"); H1=$(meta "$M" | jq -r '.chunks[0].locations[0].worker_id'); H2=$(meta "$M" | jq -r '.chunks[0].locations[1].worker_id')
docker stop "$H1" "$H2" > /dev/null
wait_for 30 sh -c "[ \$(curl -fsS $API/cluster/status | jq .missing_chunks) -ge 1 ]"
check "missing_chunks >= 1" sh -c "[ \$(curl -fsS $API/cluster/status | jq .missing_chunks) -ge 1 ]"
check "the download answers 503 chunk_unavailable" sh -c "[ \$(curl -s -o /dev/null -w '%{http_code}' $API/objects/$M) = 503 ]"
sleep 10
check "'unrecoverable' recorded exactly once" [ "$(event_count unrecoverable "$M")" = 1 ]
cpu=$(docker stats --no-stream --format '{{.CPUPerc}}' controller | tr -d '%')
say "  controller CPU: $cpu %"
check "controller CPU below 10 %" python3 -c "import sys; sys.exit(0 if float('$cpu') < 10 else 1)"
docker start "$H1" > /dev/null
check "the chunk is repaired once one holder returns" wait_for 60 sh -c "curl -fsS $API/objects/$M/metadata | jq -e .replication_ok > /dev/null"
check "the object downloads intact" download_matches "$M" "$WORK/m.bin"
docker start "$H2" > /dev/null; wait_for 90 healthy

section "AC-7.8 the target of an in-flight copy dies: another target is used"
random_file "$WORK/t.bin" $((2 * MIB))
T=$(upload "$WORK/t.bin")
# A holder of chunk 0 that does not hold chunk 1: stopping it (and then the repair target) never
# leaves a chunk without any copy.
LOST=$(meta "$T" | jq -r '([.chunks[1].locations[].worker_id]) as $c1 | [.chunks[0].locations[].worker_id | select(. as $w | ($c1 | map(select(. == $w)) | length) == 0)][0]')
before=$(event_count replicate "$T")
docker stop "$LOST" > /dev/null
wait_for 90 sh -c "[ \$(curl -fsS $API/cluster/status | jq --arg p $T '[.repair_log[] | select(.event==\"replicate\" and (.chunk_id|startswith(\$p)))] | length') -gt $before ]"
TARGET=$(status | jq -r --arg p "$T" '[.repair_log[] | select(.event=="replicate" and (.chunk_id|startswith($p)))][0].target')
check "a copy was ordered after $LOST died" sh -c "[ '$TARGET' != null ] && [ -n '$TARGET' ]"
[ "$TARGET" != null ] && docker kill "$TARGET" > /dev/null && say "  killed $TARGET right after a copy to it was ordered"
check "converges with two copies on other workers" wait_for 90 sh -c "curl -fsS $API/objects/$T/metadata | jq -e --arg a $LOST --arg b $TARGET '[.chunks[] | [.locations[] | select(.status==\"OK\" and .worker_status==\"ALIVE\" and .worker_id != \$a and .worker_id != \$b)] | length == 2] | all' > /dev/null"
check "the object is intact" download_matches "$T" "$WORK/t.bin"
docker start "$LOST" > /dev/null; [ "$TARGET" != null ] && docker start "$TARGET" > /dev/null; wait_for 90 healthy

section "AC-7.9 no invariant violations"
check "invariant_violations == 0" no_invariant_violations
summary
