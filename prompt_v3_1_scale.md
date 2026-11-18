# Claude Code Prompt — v3.1: HDFS-style scaling of replication, reporting and scanning

> Paste everything below this line into Claude Code, in the root of the `Distributed-Cloud-Storage` repository.
> This prompt **replaces** `prompt_v3_scale.md` completely.

---

## 0. Context and rules

This repository is a Java 21 / Spring Boot 4.1 multi-module Maven project (`common`, `worker`, `controller`, `client`, `system-tests`). It already implements the HDFS-style design of blocks 5.1–10:
- in-memory `ClusterState` behind one lock;
- ALIVE / STALE / DEAD liveness;
- ordered, acknowledged full and incremental reports;
- safe mode;
- worker-to-worker `REPLICATE` / `DELETE` commands in heartbeat replies;
- excess and corrupt handling;
- idempotent uploads;
- fsck, fault injection and the chaos test.

**The design is correct but does not scale.** These are the places where work grows with the total number of chunks, or with the size of an object, verified against the current code:

| # | Where (current code) | Cost |
|---|---|---|
| 1 | `ReplicationPlanner.plan()` | every 3 s: `live()` for **every** chunk, a sort over all chunks, a walk over all of them, under the cluster lock |
| 2 | `Reporter.sendFullReport()` → `Reports.fullReport()` | every 30 s: every chunk id + size + checksum in one message, applied in one lock hold |
| 3 | `BlockScanner.scanOnce()` | every 10 s: re-hashes every chunk on the worker |
| 4 | `ClusterStatusService.health()`, `SafeMode.reportedChunks()`, `Monitor.tick()` → `checkInvariants()` | O(n) under the lock on every dashboard poll and **every monitor tick** (`INVARIANT_CHECKS=1` today runs the full check each tick) |
| 5 | `ChunkStore.stats()`, called by `Lifeline.beat()` **every heartbeat** | `list()` reads **every sidecar file** every 2 s |
| 6 | `UploadService.commit()` and `ObjectService.delete()` | **SQLite transactions with `synchronous=FULL` (fsync) run inside `cluster.locked(...)`**, and the pool has one connection, so the cluster lock can wait up to the pool timeout (10 s) behind any other query. Both are also O(chunks of the object) in one lock hold. |
| 7 | `Registry.healthPass()` → `removeWorkerReplicas()` | a DEAD worker's replicas are all removed in one lock hold: O(k), and k can be hundreds of thousands |
| 8 | `ClusterState.queueCommand()` | the dedupe scans the whole command queue (up to 1000) per enqueue. Object deletion queues one DELETE per replica, so a big object overflows the 1000-command cap and **silently drops DELETEs** (`commands_dropped`). |

This prompt fixes all eight with HDFS mechanisms, without changing the public API or the failure-handling guarantees.

### Rules
1. Read `README.md`, the code of every class named here, and this whole prompt. Then post a short plan (files to add, change or delete, in block order) and start. Stop and ask only if the code contradicts this prompt in a way you cannot resolve.
2. Build in block order (§10). One commit per block (`block N: <title>`). After each block:
   - run `mvn -B test` (all modules) and the block's scenario script;
   - fix every failing acceptance criterion;
   - if one still fails after two serious attempts, stop and report.
3. **Never weaken a test.** If a v2 test encodes a rule this prompt changes, rewrite it to the new rule and say so in the commit message.
4. **The concurrency rule, tightened:** every read-then-modify of cluster state is one synchronous block under `ClusterState`'s lock, with **no network I/O and no database I/O inside**. Every lock-held block must be **bounded** by the batch limits in §1. Unbounded work is split into batches across separate lock acquisitions.
5. No code outside `ClusterState` may assign `Replica.state`, modify `pending`, or modify a worker's replica or foreign sets (§2.2).
6. Don't rename existing env vars, endpoints, JSON keys, error codes or log prefixes, except the wire-format changes listed in §5.1 and §6. Worker and controller change together, so no compatibility shim is needed.
7. Record every decision you make yourself in `README.md → Design decisions`.

---

## 1. Configuration

Add each variable to `ControllerProperties` / `WorkerProperties`, `controller.yml` / `worker.yml` (production default), `docker-compose.yml` (demo value, with a comment), and the README table.

| Variable | Svc | Default | Demo | Meaning |
|---|---|---|---|---|
| `MONITOR_INTERVAL` | C | 3 | 3 | **changed:** fallback wake-up period only |
| `MONITOR_MIN_GAP` | C | 0.1 | 0.1 | after a wake-up, wait this long to batch events |
| `WORK_MULTIPLIER` | C | 2 | 2 | new copies per tick = `WORK_MULTIPLIER × write-eligible workers` |
| `REPL_EXAMINE_P0` / `REPL_EXAMINE_P1` | C | 2000 / 1000 | same | max chunks examined per tick in each priority level (separate budgets) |
| `EXCESS_EXAMINE` / `CLEANUP_EXAMINE` | C | 2000 / 2000 | same | max chunks examined per tick for over-replication / cleanup |
| `DELETE_BUDGET` | C | 1000 | 1000 | max DELETE commands queued per tick, all reasons together |
| `MAX_OUTSTANDING_DELETES` | C | 200 | 200 | per worker: DELETEs queued but not yet confirmed by IBR |
| `SWEEP_BATCH` | C | 5000 | 5000 | replicas (or chunks) handled per lock hold by sweeps, dead-worker removal, object deletion, upload commit |
| `INVARIANT_CHECKS` | C | true | true | **changed:** enables the *rolling* check; it never runs the full O(n) check in a tick |
| `INVARIANT_SLICE` | C | 5000 | 5000 | chunks verified per tick by the rolling check |
| `FULL_REPORT_INTERVAL` | C+W | **21600** | **300** | periodic full report (safety net) |
| `MAX_CONCURRENT_REPORTS` | C | 2 | 2 | full-report leases at once |
| `REPORT_START_TIMEOUT` | C | 10 | 10 | a granted lease with no first page is withdrawn after this |
| `REPORT_SESSION_TIMEOUT` | C | 60 | 60 | an open session with no page for this long is closed |
| `FINGERPRINT_GRACE` | C | 30 | 30 | a fingerprint mismatch must persist this long before a full report is wanted |
| `REPORT_PAGE_SIZE` | W | 1000 | 1000 | chunks per report page |
| `REPORT_PAGE_PACING` | W | 0.2 | 0.2 | pause between pages of a periodic report (catch-up: none) |
| `SCAN_INTERVAL` | W | **86400** | **10** | **changed:** minimum time between the *starts* of two scanner passes |
| `SCAN_BYTES_PER_SECOND` | W | 1048576 | 67108864 | scanner read rate (HDFS default 1 MB/s) |
| `SCAN_SUSPECT_SHARE` | W | 0.5 | 0.5 | max share of scanner bytes spent on suspects while normal work remains |

All other settings are unchanged.

---

## 2. Shared foundations (Block 11, before anything else)

### 2.1 Lock instrumentation
`ClusterState.locked(...)` measures **wait time** (from the request to acquiring the lock) and **hold time** with `System.nanoTime()`.
- Record both in two small lock-free histograms: fixed log₂ buckets from 1 µs to 16 s in an `AtomicLongArray`, plus a max.
- Expose them in `GET /api/v1/cluster/status` as `lock: {wait_p50_ms, wait_p99_ms, wait_max_ms, hold_p50_ms, hold_p99_ms, hold_max_ms, window_s}`, computed over a rolling 60 s window: two alternating histograms swapped every 30 s.
- Log `WARN [lock] held <n> ms by <caller>` whenever a hold exceeds 100 ms. The caller is a short label passed to `locked(label, …)`.

### 2.2 Per-worker on-disk view, ordered by `lastSeen`
In `ClusterState`, replace `Map<String, Set<String>> workerBlocks` with a per-worker `WorkerDisk`:

```java
final class WorkerDisk {
    final LinkedHashSet<String> replicas = new LinkedHashSet<>();         // chunk ids, ordered by Replica.lastSeen (oldest first)
    final LinkedHashMap<String, Foreign> foreign = new LinkedHashMap<>();  // files on disk that are NOT replicas, ordered by lastSeen
    int outstandingDeletes;                                                // DELETEs queued/delivered, not yet confirmed
    long fpCount, fpBytes, fpXor, fpSum;                                   // controller-side fingerprint (§5.3)
}
record Foreign(long size, String cs64, String reason, boolean approved, long lastSeen, long deleteQueuedAt) {}
// reason: "orphan" (unknown chunk), "garbage" (failed upload), "object_deleted"
```

- `Replica` gains `long lastSeen` and `String reportedCs64` (null = equals canonical).
- **Stamping a replica or foreign entry** (`touchSeen(workerId, chunkId)`) sets `lastSeen = now` and moves the id to the end of its ordered set (`remove` + `add`, O(1)). The order is therefore always oldest-seen first.
- Only `ClusterState` methods modify `WorkerDisk`. Keep `chunksOn(w)` working (it returns `replicas`).

### 2.3 Sweep tasks (bounded removal of stale entries)
`ClusterState` keeps `Map<String, Long> sweepWatermark` (worker → watermark nanos; a higher watermark replaces a lower one).

A **sweep step** (§3.4) walks a worker's `replicas`, then `foreign`, **from the head**, and stops at the first entry with `lastSeen ≥ watermark` or after `SWEEP_BATCH` entries. For each entry it passes:

- **Replica**:
  - if the worker is DEAD, or the replica is settled (`confirmed`, or `now - addedAt > PENDING_TIMEOUT`): `removeReplica` and log `[report] missing` (or count it under a DEAD summary line);
  - otherwise move it to the end without restamping, as "kept unsettled". This is rare: only upload-added replicas within `PENDING_TIMEOUT`.
- **Foreign**: remove it.

When the head entry is at or after the watermark, the sweep for that worker is done: remove the watermark, and if it came from a report, log `[report] <w> sweep done removed=<n>`. Watermarks are created by:
- **DEAD:** `Registry.healthPass()` marking a worker DEAD sets `watermark = now` **instead of** calling `removeWorkerReplicas()`. `live()`, `readable()`, placement and source selection already ignore DEAD workers, so the replicas are invisible at once; the sweep only reclaims them.
- **The last page of a full report** sets `watermark = session.startedAt` (§6.3).

If a DEAD worker re-registers while its sweep is still running, the sweep continues. Replicas the worker re-reports are stamped after the watermark and survive; the others are removed.

### 2.4 No database I/O under the cluster lock (fixes items 6 and 8)

**Upload commit (`UploadService`):**
1. The PUTs stay as today, outside the lock.
2. **The DB transaction runs outside the lock:** insert object, metadata, chunks, idempotency key.
   - Idempotency races are decided by the database: the key column is a primary key, so a constraint violation means another request won. Look up the winner and handle it as today's `LostIdempotencyRace`; delete *our* copies by moving them to `foreign` (reason `garbage`, approved) under the lock.
3. **Memory update under the lock, in batches of `SWEEP_BATCH` chunks.**
   - If the object is in `garbageObjects` (it was deleted meanwhile), move every written copy to `foreign` as `object_deleted`, approved.
   - Otherwise `putChunk` + `addReplica(confirmed = hasUploadReceipt, expectBoot)` per copy.
4. `endUpload(objectId)` under the lock, **after** the last batch.
   - If the upload did not commit, move its upload receipts to `foreign` as `garbage`, approved, before clearing them.
   - The `PENDING_UPLOADS` entry protects the chunks from orphan handling until this point.
5. An idempotent replay whose object is still in `PENDING_UPLOADS` waits, polling every 50 ms for up to 5 s, before describing it.

**Object delete (`ObjectService.delete`):**
1. The DB delete runs **outside the lock** (404 if absent) and returns the object's `chunk_count`.
2. Then, under the lock: `addGarbage(objectId)` and enqueue an `ObjectDeletion(objectId, chunkCount)` task.
3. Return 204.
4. The monitor drains the task in batches of `SWEEP_BATCH` chunk indexes (§3.4). Chunk ids are `ChunkIds.of(objectId, i)` for `i < chunkCount`, so no scan is needed. For each chunk:
   - move every replica to its worker's `foreign` as `object_deleted`, approved;
   - then `removeChunk`.

   Downloads return 404 as soon as the DB row is gone.

**`PATCH …/metadata`:** DB only, never under the cluster lock.

**`ClusterState.queueCommand`:**
- dedupe through a per-worker `HashMap<key, Command>` (O(1));
- deletes go through §3.5, so `MAX_COMMANDS_PER_WORKER` is never reached by DELETEs;
- keep the cap only as a last-resort guard;
- `commands_dropped` must stay 0 in all tests.

---

## 3. Block A — Incremental replication queue and event-driven monitor

### 3.1 `controller/.../replication/NeededReplications.java` (HDFS `LowRedundancyBlocks`)
Owned by `ClusterState`; every method runs under the lock.

```java
enum Level { P0_ONE_LIVE, P1_BELOW_RF, MISSING }
EnumMap<Level, LinkedHashSet<String>> levels;     // FIFO; rotate = remove + add at back
Map<String, Level> levelOf;                       // absent = healthy
LinkedHashSet<String> overReplicated;             // live > RF
LinkedHashSet<String> cleanup;                    // ≥1 replica in state EXCESS or CORRUPT
Map<String, Snapshot> last;                       // per chunk, for O(1) counter deltas
record Snapshot(int live, int ok, int corrupt, int excess, int pending) {}
```

### 3.2 `update(chunkId)`: the single place membership is decided (O(RF))
```
if chunk ∉ chunks: forget(chunkId); return
live    = OK replicas on non-DEAD workers            (== ClusterState.live())
pending = pendingOf(chunkId).size()
level   = live == 0 → MISSING
          live + pending < RF → (live == 1 ? P0_ONE_LIVE : P1_BELOW_RF)
          else none
move between level sets only if the level changed (new entries go to the back)
overReplicated: live > RF ;  cleanup: corrupt + excess > 0
counters: subtract last[chunk], add the new Snapshot
entering MISSING → record("unrecoverable") once; this replaces ReplicationPlanner.missing
entering P0/P1/overReplicated/cleanup → signalMonitor()
```

### 3.3 Hooks: every mutation calls `touched(chunkId)` → `needed.update(chunkId)`
| `ClusterState` method | Hook |
|---|---|
| `putChunk` | `touched` |
| `removeChunk` | `needed.forget` |
| `addReplica` (new, or state change) | `touched` |
| `removeReplica` | `touched` (after `protect`) |
| `markCorrupt` | `touched` |
| `protect` (EXCESS → OK) | `touched` |
| new `setReplicaState(chunk, worker, state)` | `touched` |
| `addPending`, `removePending` | `touched` |
| new `expirePending(now)` (replaces the list surgery in `Monitor.expirePendingCopies`) | `touched` per expired copy |
| `dropPendingForWorker` | `touched` per affected chunk |

Also:
- **DEAD (§2.3):** `live()` changes immediately, but buckets change only as the sweep removes replicas. The rolling invariant check (§4.2) skips chunks with a DEAD holder.
- `ReplicationPlanner`'s direct `replica.state = …` writes become `setReplicaState`.
- `pendingFrom(w)` and `pendingTo(w)` become O(1): keep per-worker counters updated with `pending`.

### 3.4 Event-driven monitor
**Wake-up:**
- `ClusterState` holds `Semaphore wake = new Semaphore(0)` and `signalMonitor()`, which does `if (wake.availablePermits() == 0) wake.release()`. A signal raised while a tick runs leaves a permit, so the next wait returns immediately. **No wake-up can be lost.**
- Signal on:
  - a chunk entering P0, P1, `overReplicated` or `cleanup`;
  - `removePending` / `expirePending` (a slot freed);
  - a worker becoming ALIVE, registering, or completing a report;
  - safe mode turning off;
  - a new sweep or `ObjectDeletion` task;
  - a newly approved foreign entry.
- `MaintenanceLoops` monitor thread:

  ```java
  while (running) {
      wake.tryAcquire(monitorInterval);            // event or fallback
      Thread.sleep(monitorMinGap);                  // batch a burst
      wake.drainPermits();
      boolean moreWork = monitor.tick();            // returns true if any budget ran out with work left
      if (moreWork) signalMonitor();                // continue right away, after the lock was released
  }
  ```

**`Monitor.tick()` is a sequence of separate lock acquisitions,** each bounded, so heartbeats and reports interleave:
1. `locked`: purge expired failures; `expirePending(now)`; `safeMode.evaluate()` (§4.1). If safe mode is on: stop here, return false.
2. `locked`: **sweeps:** up to `SWEEP_BATCH` entries in total, DEAD watermarks first, then report watermarks.
3. `locked`: **object deletions:** up to `SWEEP_BATCH` chunks.
4. `locked`: **replication** (§3.5 a).
5. `locked`: **excess** (§3.5 b).
6. `locked`: **cleanup and foreign GC** (§3.5 c, d).
7. `locked`: every 10 ticks `requestOverdueReports()`; pending-only invariants I4/I8; the rolling slice (§4.2) if `INVARIANT_CHECKS`.

Return true if any step stopped because of a budget while work remained.

### 3.5 Planning with separate budgets (no queue can starve another)
```
a) Replication: copyBudget = WORK_MULTIPLIER × write-eligible workers
   for level, examineLimit in [(P0, REPL_EXAMINE_P0), (P1, REPL_EXAMINE_P1)]:
       examine at most min(examineLimit, |level| at step start) chunks, FIFO:
         - an EXCESS replica with !deleteDelivered → setReplicaState(OK), cancel its DELETE, record "restored"
         - else if copyBudget == 0 → stop this level (work remains)
         - src = chooseSource, dst = chooseTarget; none → blocked++, rotate to back
         - else queue REPLICATE, addPending (update() removes it from the level), copyBudget--
   P0 has strict priority for copyBudget (as HDFS), but P1 always has its own examine budget,
   so a large *blocked* P0 backlog can never stop P1 from being examined.
b) Excess: examine at most EXCESS_EXAMINE of overReplicated, FIFO:
       skip (rotate) if pending or postponed (holder STALE / !hasReported / replica !confirmed / worker has an active sweep)
       else choose victims; setReplicaState(EXCESS); requestDelete(victim, chunk, "excess")
c) Cleanup: examine at most CLEANUP_EXAMINE of cleanup, FIFO, rotating each examined chunk:
       resend an EXCESS DELETE after PENDING_TIMEOUT; DELETE a CORRUPT replica only when live ≥ RF and not postponed
d) Foreign GC (only if !safeMode and now - startedAt ≥ ORPHAN_GC_DELAY): round-robin over workers,
       for approved foreign entries with deleteQueuedAt == 0 or older than PENDING_TIMEOUT: requestDelete(…, reason)
```

`requestDelete(worker, chunk, reason)` is the **only** way to queue a DELETE. It:
- refuses (and leaves the entry for a later tick) when the tick's `DELETE_BUDGET` is used up, or when `outstandingDeletes(worker) ≥ MAX_OUTSTANDING_DELETES`;
- otherwise queues the command, sets `deleteQueuedAt`, and increments `outstandingDeletes`.

`outstandingDeletes` is decremented when an IBR `deleted` confirms the delete, when the entry disappears by a sweep, and when the worker re-registers or goes DEAD (reset to 0).

Every examined-but-skipped entry rotates to the back, so every eligible entry is examined within ⌈size / examine limit⌉ ticks. While anything is blocked, the fallback wake-up (`MONITOR_INTERVAL`) guarantees progress even without events.

---

## 4. Block B — O(1) status and safe mode; rolling invariants

### 4.1 Counters and safe mode
- **`NeededReplications.counters()`**, kept by Snapshot deltas:
  - `underReplicated` (live < RF), `overReplicated`, `missing` (live == 0);
  - `replicasOk`, `replicasCorrupt`, `replicasExcess`;
  - `chunksWithLiveCopy` (live ≥ 1).
- `ClusterStatusService.health()` returns these counters; `status()` becomes O(workers + events).
- `SafeMode.reportedChunks()` = `chunksWithLiveCopy`.
- **Safe-mode exit condition** (decides the "is an IBR enough?" question): leave safe mode when **either**:
  - the threshold has been met for `SAFE_MODE_EXTENSION` **and** every registered non-DEAD worker has `hasReported` (completed a full report since registering); **or**
  - `SAFE_MODE_TIMEOUT` has passed.

  Replicas learned from IBRs count toward the threshold (they are real copies), but exit also waits for every worker's catch-up full report. Without that, the monitor would schedule needless copies for chunks whose second holder simply hasn't finished reporting. (HDFS uses a min-datanodes rule for the same reason.)

### 4.2 Rolling invariant check (replaces the per-tick full check)
- Every tick (step 7), verify `INVARIANT_SLICE` chunks from a rotating cursor. At the start of each rotation, copy the chunk-id key set once: an O(n) reference copy, documented.
- For each chunk: recompute its level, set memberships and Snapshot from scratch and compare (**I9**); verify the replica ↔ `WorkerDisk` index (**I2**); skip chunks with a DEAD holder.
- Global counters (**I10**) are compared with the sums of all Snapshots once per completed rotation.
- **On a mismatch:**
  1. log `ERROR [invariant]`;
  2. increment `invariant_violations`;
  3. repair that chunk with `update()`;
  4. on an I10 mismatch, recompute the counters from Snapshots.
- `checkInvariants()` (the full check) remains, but is only called by tests and by `/api/v1/fsck`. fsck copies the key list once, then checks in slices of `SWEEP_BATCH` per lock hold.
- Relax **I1**: a holder may be DEAD only while that worker has a DEAD sweep watermark.

---

## 5. Block C — Worker chunk index and fingerprint

### 5.1 `worker/.../storage/ChunkIndex.java`
- A `ConcurrentSkipListMap<String, Entry>` (sorted, so report pages resume from a cursor), with `record Entry(long size, String checksum /* null = no valid sidecar */, long metaMtime)`.
- **Aggregates**, kept under a single `synchronized` block together with every map change:
  - `count`, `totalBytes`;
  - fingerprint `xor`, `sum` (wrapping 64-bit).
- **Fingerprint element** for one chunk:

  ```
  cs64 = checksum == null ? "0000000000000000" : first 16 hex chars of checksum
  e    = first 8 bytes of SHA-256(UTF-8 of chunkId + ":" + size + ":" + cs64) as long   // Checksums.fingerprintElement(...)
  add:    count++, totalBytes += size, xor ^= e, sum += e
  remove: count--, totalBytes -= size, xor ^= e, sum -= e
  ```

  Including size and the checksum prefix makes the fingerprint sensitive to content-identity changes, not just ids. Two independent accumulators make accidental cancellation negligible.
- **Built once at startup**, in `ChunkStore` initialisation after the existing cleanup: one directory walk plus sidecar reads. A missing or invalid sidecar gives `checksum = null`, and `ibr.bad` is queued after the worker registers.
- **Kept consistent with disk** (the rule: the index changes only after the disk change is durable, and the IBR event is recorded after the index change):

| Operation | Index | IBR |
|---|---|---|
| `write` succeeds (both renames + dir fsync) | put | `received` |
| `write` fails at any point | unchanged | none |
| `delete` succeeds (data file gone; meta removed or absent) | remove | `deleted` |
| `delete` fails with `IOException` | unchanged | none (the controller resends after `PENDING_TIMEOUT`) |
| `read` finds no file but the index has it | remove | `deleted` |
| `read` `IOException` | unchanged | none; add to scanner suspects |

- `ChunkStore.list()`, `chunkIds()` and **`stats()` read the index**:
  - `stats()` = `count`, `totalBytes`, `freeBytes()`, active transfers, in O(1). This fixes item 5.
  - `listVerified()` (fsck) still reads disk.
- **Restart:** the index is rebuilt from disk. Lost IBRs don't matter, because registration is followed by a full report.

### 5.2 Wire changes
- `HeartbeatRequest` gains:
  - `@JsonProperty("fingerprint") Fingerprint fingerprint`, with `record Fingerprint(long count, long totalBytes, String xor, String sum)`; xor and sum are 16 lowercase hex chars;
  - `@JsonProperty("wants_full_report") boolean wantsFullReport`.
- `WorkerStats` is filled from the index aggregates.

### 5.3 Controller-side fingerprint: exact accounting
The controller keeps, per worker in `WorkerDisk`, the same four aggregates over **everything it believes is on that worker's disk**:
- **replicas**, every state, using `reportedCs64 != null ? reportedCs64 : canonical cs64` and the canonical size;
- **foreign** entries, with their reported size and cs64;
- **upload receipts** for that worker, using the size and cs64 from the IBR.

Update the aggregates in O(1) on every add, remove and state change of those three. `ChunkInfo` caches `cs64` and `size`.

Because receipts and foreign files are counted, **a long-running upload, a queued orphan, or a refused orphan GC does not cause a mismatch.** A mismatch means real drift.

**In `Registry.heartbeat()`, after acceptance:** skip the comparison if the worker has an open report session, a sweep watermark, or `!hasReported`. Otherwise:
```
match → fpMismatchSince = 0, fpBackoff = FINGERPRINT_GRACE
first mismatch → fpMismatchSince = now
mismatch for ≥ fpBackoff and lastReportAt older than fpBackoff → want a full report ("fingerprint"),
    increment fingerprint_mismatch, fpBackoff = min(2 × fpBackoff, FULL_REPORT_INTERVAL), fpMismatchSince = now
```
Log one INFO line per transition, with both fingerprints.

---

## 6. Block D — Paged full reports, leases, mark-and-sweep

### 6.1 Wire format (replaces the `chunks` list of `BlockReportRequest`)
```java
public record BlockReportRequest(String workerId, String bootId, long seq, double sentAt,
        @JsonProperty("report_id") String reportId,          // assigned by the controller's grant
        @JsonProperty("page") int page,
        @JsonProperty("last") boolean last,
        @JsonProperty("objects") List<ObjectChunks> objects,
        @JsonProperty("unverified") List<UnverifiedChunk> unverified)
public record ObjectChunks(@JsonProperty("object_id") String objectId,
                           @JsonProperty("chunks") List<ReportedChunk> chunks)
public record ReportedChunk(int index, long size, String cs64)   // JSON: [index, size, "cs64"]
public record UnverifiedChunk(String chunkId, long size)          // no valid sidecar
```
- At most `REPORT_PAGE_SIZE` chunks per page (including unverified).
- Each chunk carries **cs64** (16 hex chars of its sidecar checksum). It costs about 18 bytes and keeps a canonical-checksum comparison in every full report.
- Grant fields replace `request_full_report` in `HeartbeatResponse` and are added to `RegisterResponse`: `"full_report": {"report_id": "…"}` (absent = no grant).
- New `ErrorCodes`: `report_session_unknown`, `report_page_gap`.

### 6.2 Leases and sessions (controller)
- **Wanting a report:**
  - the worker sends `wants_full_report` (after registration, when its periodic report is due, on IBR overflow, after an abandoned report);
  - the controller sets `fullReportRequested` (fingerprint mismatch, overdue, `!hasReported`, unknown chunk in an IBR).
- **Granting** (in a heartbeat reply or the register reply), under the lock:
  - if the worker wants a report, has no session, and fewer than `MAX_CONCURRENT_REPORTS` sessions are open, open `ReportSession{reportId=uuid, grantedAt=now, startedAt=0, nextPage=0, lastPageAt=0, totals}` and return the grant;
  - priority: `!hasReported` workers first, then the longest-waiting.
  - **While a session has no page yet, every heartbeat reply repeats the same grant,** so a lost reply doesn't lose the lease.
- **Closing a session** releases its lease immediately and calls `signalMonitor()`. It happens on:
  - the last page applied;
  - no first page within `REPORT_START_TIMEOUT`;
  - no page within `REPORT_SESSION_TIMEOUT`;
  - a gap;
  - an unknown report id;
  - re-registration;
  - DEAD.

  Closing for any reason other than completion means **no sweep and no orphan approval**, and sets `fullReportRequested` so the worker is granted again.
- **Accepting a page** (after the existing seq rules):

| Condition | Answer |
|---|---|
| no session, or `reportId` ≠ session's | 409 `report_session_unknown` (also closes a mismatched open session) |
| `page < nextPage` | 200 `"ignored": "duplicate_page"`, nothing applied |
| `page > nextPage` | close the session; 409 `report_page_gap` |
| `page == nextPage` | apply; `nextPage++`; `lastPageAt = now`; on page 0, `startedAt = now` |

### 6.3 Applying a page (under the lock, O(page))
For each reported chunk:

| Case | Action |
|---|---|
| object in `PENDING_UPLOADS` | record the upload receipt (fingerprint accounting) |
| chunk unknown, object in `garbageObjects` | `foreign` (reason `garbage`, **approved**), stamp |
| chunk unknown otherwise | `foreign` (reason `orphan`, not approved), stamp; count in `session.orphans` |
| `size` ≠ canonical, or cs64 ≠ canonical cs64, or unverified | replica CORRUPT (create or mark), `reportedCs64 = cs64`, `confirmed = true`, stamp |
| known, matches | create the replica (`confirmed`) or set `confirmed = true`; clear `reportedCs64`; stamp; complete a matching pending copy |

**On the last page:**
1. `sweepWatermark[w] = max(existing, session.startedAt)`. **Removal is done by sweep steps (§2.3), never here.**
2. **Orphan approval (safety valve over the whole session):**
   - if `session.orphans > 10` and `session.orphans / session.reportedTotal > ORPHAN_GC_MAX_FRACTION`: approve nothing, log ERROR, increment `orphan_gc_refused`;
   - else mark this session's unapproved orphans approved. Iterate `foreign` from the tail for at most `session.orphans` entries: they were all stamped during the session.
3. Set `hasReported = true`, `lastReportAt = now`, `fpMismatchSince = 0`; close the session (releasing the lease); `signalMonitor()`.
4. Log one summary line: `report_id`, `pages`, `chunks`, `added`, `corrupt`, `orphans`, `refused`.

**IBRs during a report are safe:**
- `received` stamps the replica (or receipt or foreign entry), so it survives the sweep;
- `deleted` removes the entry and decrements `outstandingDeletes`;
- the worker's single reporter thread orders IBRs and pages.

### 6.4 IBR changes (`Reports.ibr`)
- `received`: stamp `lastSeen`; set `reportedCs64` if the checksum ≠ canonical. For an **unknown** chunk not in `PENDING_UPLOADS`:
  - if the object is garbage: `foreign` (garbage, approved);
  - else: `foreign` (orphan, unapproved) and `fullReportRequested = true`, so the valve decides.
- `deleted`: remove the replica or foreign entry; `outstandingDeletes--`; record `trimmed` / `invalidated` / `deleted` as today.
- `bad`: as today.

### 6.5 Worker `Reporter`
- **Delete** "snapshot everything + clear the IBR buffer". **The IBR buffer is never cleared because of a report.**
- `Lifeline` passes `wants_full_report` and the fingerprint up, and hands any `full_report` grant to the reporter through a volatile field.
- On a grant with `report_id`: run `runPagedReport(reportId, catchUp = firstReportSinceRegister)`:

  ```
  cursor = ""; page = 0
  loop:
     entries = index.tailMap(cursor, false) — first REPORT_PAGE_SIZE           // O(page · log n)
     last    = no entry after these
     build ObjectChunks (sorted ids ⇒ one object's chunks are adjacent) + UnverifiedChunk
     sendUntilAcked (existing seq / retry rules)
        409 report_session_unknown or report_page_gap → abandon; wantsFullReport = true; return
        404 / stale_boot → as today
     if last: wantsFullReport = false; lastFullReport = now; scanner.afterFullReport(); return
     cursor = last id; page++
     if ibr.hasEvents(): sendIncrementalReport()          // IBRs flow between pages
     if !catchUp: sleep REPORT_PAGE_PACING
  ```

- IBR buffer overflow: clear it, abandon any running report, set `wantsFullReport`.

---

## 7. Block E — Rate-limited scanner with reconciliation

### 7.1 Token bucket (exact rules)
- `capacity = max(CHUNK_SIZE, SCAN_BYTES_PER_SECOND)` bytes; **starts full**; refills continuously at `SCAN_BYTES_PER_SECOND` using `System.nanoTime()` (never wall-clock).
- `acquire(n)`: refill, then if `tokens ≥ n` subtract `n`; otherwise sleep `(n − tokens) / rate`, refill, and subtract.
  - `n > capacity` is allowed: the bucket goes to 0 after sleeping for the deficit.
  - This guarantees bytes read ≤ `capacity + rate × t` over any window `t`.

### 7.2 Pass structure
- **A pass starts** when `SCAN_INTERVAL` has elapsed since the previous pass *started*. The real period is `max(SCAN_INTERVAL, totalBytes / SCAN_BYTES_PER_SECOND)`; document it, for example 1 TB at 1 MB/s ≈ 11.6 days.
- **Reconciliation, first, names only:** one `DirectoryStream` over the chunk directory, ignoring `.tmp` and anything younger than `SETTLE_TIME`.

| Disk vs index | Action |
|---|---|
| file, not in index | read its sidecar → add (`checksum` null if missing or invalid); `received` |
| index entry, no file | remove; `deleted` |
| `Files.size` ≠ entry size | update the entry; `bad` |
| sidecar mtime ≠ `metaMtime` | re-read the sidecar → update the entry (the fingerprint changes); if missing or invalid → `checksum = null`; if the checksum changed → `bad` |

  Log `[scanner] reconciled +a -b ~c` only when something changed.
- **Verification:** iterate the index from a cursor (`tailMap`), resuming after pauses. `acquire(size)`, hash, compare with the entry checksum.
  - Mismatch, or `checksum == null`: `bad` (once per chunk per pass, plus once after each full report).
  - File gone: remove it from the index; `deleted`.
- **Suspects** (from read `IOException`s and executor read failures): a deduplicated `LinkedHashSet` with a cap of 10,000.
  - A suspect is taken before the cursor only while `suspectBytes ≤ SCAN_SUSPECT_SHARE × passBytes`, so normal scanning can't starve.
  - A missing suspect file → remove it from the index; `deleted`, immediately.
- **Never delete data files.**

### 7.3 Detection guarantees (put these in the README)
| Damage | Detected by | Time |
|---|---|---|
| any damage to a chunk that is read | download / REPLICATE verification against the canonical checksum | immediately |
| data bit rot, sidecar intact | scanner | next verification pass: ≤ `SCAN_INTERVAL` + pass time |
| data and sidecar both altered consistently | sidecar mtime → index cs64 changes → fingerprint mismatch → full report cs64 ≠ canonical → CORRUPT | ≤ reconciliation period + `FINGERPRINT_GRACE` + report time |
| file deleted or added by hand | reconciliation | ≤ `SCAN_INTERVAL` |

---

## 8. Edge cases (implement exactly this)

| Situation | Behaviour |
|---|---|
| Large P0 backlog all blocked, P1 schedulable | P1 has its own examine budget → served in the same tick |
| Large blocked cleanup queue | Its own examine budget; replication, excess and foreign GC unaffected |
| Huge P0 backlog, all schedulable | Strict priority for `copyBudget`; `moreWork` re-wakes the monitor immediately; each lock hold stays bounded |
| Worker with 300k replicas goes DEAD | DEAD set in O(1); the sweep removes 5000 per tick with immediate re-wake; repairs start with the first batch |
| Worker returns after DEAD while its sweep runs | Re-reported replicas are stamped after the watermark and survive; the rest are removed |
| Controller restart | Sessions are lost; workers get 404 → re-register → catch-up reports under ≤ `MAX_CONCURRENT_REPORTS` leases; safe mode waits for every worker's report or the time-out |
| Worker restart mid-report | Register closes the session (no sweep) and releases the lease |
| Grant lost with a heartbeat reply | The grant is repeated in every reply until page 0 or `REPORT_START_TIMEOUT` |
| Late page from an abandoned session | 409 `report_session_unknown`; nothing applied |
| Report session applied, sweep not finished, new report starts | The watermark rises to the new `startedAt`; one continuing sweep covers both |
| 200k orphans on a worker (valve passes) | Approved in one pass over the session's tail entries; deleted at ≤ `MAX_OUTSTANDING_DELETES` per worker and ≤ `DELETE_BUDGET` per tick |
| Orphans exceed the valve | Nothing approved; `orphan_gc_refused`; the fingerprint still matches (foreign entries are counted) |
| Object with 100k chunks deleted | 204 at once; the deletion task moves replicas to `foreign` in batches; deletes are paced; `commands_dropped` stays 0 |
| Upload lasting minutes | Receipts are in the controller fingerprint → no mismatch → no reports triggered |
| Upload commit races with an object delete (idempotent replay + delete) | The commit's memory step sees the object in garbage → copies go to `foreign` (object_deleted) |
| A DELETE command lost or failed | `deleteQueuedAt` + `PENDING_TIMEOUT` → resent; `outstandingDeletes` freed when the worker re-registers or goes DEAD |

---

## 9. Tests

### 9.1 Unit and property tests (controller)
1. `NeededReplicationsTest`: every transition, FIFO, rotation, `forget`, counters.
2. **Starvation:** 5,000 blocked P0 chunks (no target) + 100 schedulable P1 → P1 is scheduled on the first tick. 10,000 blocked cleanup entries + over-replicated chunks → excess handled on the first tick.
3. **Randomized state machine:** 20,000 random ops on 8 workers and 3,000 chunks (add/remove replica, corrupt, excess, pending add/remove/expire, DEAD and re-register, report pages, IBRs, object delete, upload commit). After every 100 ops: `checkInvariants()` returns zero violations, and the counters equal a from-scratch recount.
4. **Report sessions:** in order, duplicate, gap, unknown id, start timeout, session timeout, re-register mid-report, controller "restart" (fresh `ClusterState`). Each releases its lease immediately (open sessions ≤ max at all times). IBR `received` mid-report survives the sweep; IBR `deleted` mid-report is honoured.
5. **Bounded finalization:** a worker reporting 300k chunks in pages, with 100k stale replicas → the last page's lock hold is < 5 ms, and the stale replicas are removed by sweep steps of ≤ `SWEEP_BATCH`.
6. **Bounded orphan GC:** 200k orphans approved → DELETE queue per worker never exceeds `MAX_OUTSTANDING_DELETES`, ≤ `DELETE_BUDGET` per tick, `commands_dropped == 0`.
7. **Fingerprint:** worker index aggregates equal controller aggregates after random sequences that include uploads in progress (receipts), orphans, garbage and corrupt replicas with a forged cs64. An upload spanning 3 × `FINGERPRINT_GRACE` triggers no report.
8. **Safe mode:** the threshold met by IBRs alone does not exit while a registered worker hasn't completed its report; the time-out still exits.
9. **DB outside the lock:** a test DB wrapper that sleeps 2 s in every write → heartbeats are still accepted with lock wait < 50 ms during an upload commit and an object delete.

### 9.2 Worker tests
1. `ChunkIndexTest`: rebuild equals disk; the write/delete/read failure rules of §5.1; O(1) `stats()`.
2. `BlockScannerTest` (fake clock and fake file system acceptable):
   - rate within ±10% over 10 s;
   - bucket starts full and caps bursts;
   - suspects capped at their share;
   - a missing suspect is removed and reported `deleted` immediately;
   - reconciliation handles added, removed, size-changed, sidecar-missing, sidecar-invalid and sidecar-changed files;
   - **data + `.meta` both rewritten consistently** → scanner silent, index cs64 changes → fingerprint differs.
3. `ReporterPagingTest`: page size, grouping, cs64 present, IBRs interleaved, abandon on 409, grant id used.

### 9.3 Scale test (`@Tag("scale")`, `mvn -Pscale test`, `make test-scale`)
Run in memory, with no HTTP: `ClusterState`, `Registry`, `Reports`, `Monitor`, `ClusterStatusService`, `SafeMode`, 8 workers, **500,000 chunks × 2 replicas** loaded through pages.

Run this mixed workload for 60 s, with simulated worker threads:
- heartbeats from 8 workers every 100 ms;
- IBR traffic;
- catch-up reports for 2 workers (300k chunks each);
- one worker DEAD (125k replicas) at t=10 s;
- copy completions simulated by IBRs;
- 100k approved orphans;
- a 50k-chunk object delete;
- the monitor running continuously.

Assert, and print:

| Metric (from §2.1 instrumentation) | Limit |
|---|---|
| lock hold p99 / max | < 10 ms / < 50 ms |
| lock wait p99 seen by heartbeats | < 20 ms |
| heartbeat handler latency p95 / p99 | < 10 ms / < 25 ms |
| P0 backlog after the DEAD worker fully scheduled | within 30 s |
| `status()` | < 5 ms |
| invariant violations, `commands_dropped` | 0 |

Record the numbers and the machine in `README.md → Scalability`.

### 9.4 Scenarios and chaos (against `make up`)
New `tests/scenarios/block11_scale.sh`:
1. Repair starts ≤ 1 s after DEAD.
2. A hand-deleted chunk file is noticed within `SCAN_INTERVAL + 5 s` and repaired.
3. A junk file is GC'd (reconcile → IBR → report → valve → approved → deleted).
4. Controller restart: safe mode exits after all reports; `fingerprint_mismatch` stays 0 for 2 min idle.
5. `REPORT_PAGE_SIZE=10` with 100+ chunks: multi-page reports with ≤ 2 sessions at once.
6. `make corrupt` together with a matching rewrite of the `.meta` → replica CORRUPT within `SCAN_INTERVAL + FINGERPRINT_GRACE + 10 s`.

Every existing test and scenario must still pass, updated only where this prompt changed a rule. `python tests/chaos.py` must pass three times in a row, then once more with `REPORT_PAGE_SIZE=10 FULL_REPORT_INTERVAL=60`.

---

## 10. Build order and acceptance criteria

**Block 11 — Foundations (§2):** lock instrumentation, `WorkerDisk`, sweep tasks with batched DEAD removal, DB I/O out of the lock, `requestDelete` / `outstandingDeletes`, O(1) command dedupe.
- AC-11.1 Test 9.1-9 passes.
- AC-11.2 No `cluster.locked` block contains a repository call. Check with a review grep plus an assertion in a test wrapper.
- AC-11.3 `commands_dropped == 0` after deleting a 5,000-chunk object in the system tests.
- AC-11.4 All existing tests and the chaos test pass.

**Block 12 — Replication queue and monitor (§3, §4):**
- AC-12.1 Tests 9.1-1, -2, -3, -8 pass.
- AC-12.2 No loop over `cluster.chunks()` remains in `ReplicationPlanner`, `ClusterStatusService`, `SafeMode` or `Monitor`, outside the rolling slice's per-rotation key copy.
- AC-12.3 Scenario 1 passes.

**Block 13 — Index and fingerprint (§5) + scanner (§7):**
- AC-13.1 Tests 9.1-7 and 9.2-1, -2 pass.
- AC-13.2 `stats()` and heartbeats are O(1). No worker directory walk except the startup build, reconciliation and `listVerified()`.
- AC-13.3 Scenarios 2 and 6 pass.

**Block 14 — Paged reports (§6):**
- AC-14.1 Tests 9.1-4, -5, -6 and 9.2-3 pass.
- AC-14.2 Scenarios 3, 4 and 5 pass. With `FULL_REPORT_INTERVAL=21600`, the v2 failure, integrity and controller-crash demos pass.

**Block 15 — Scale test and docs:**
- AC-15.1 `make test-scale` passes within the limits of §9.3.
- AC-15.2 README:
  - the HDFS table gains split reports, leases, LowRedundancyBlocks, VolumeScanner and min-datanodes safe mode;
  - the configuration table shows production vs demo values;
  - the protocol section documents fingerprints, grants, pages, cs64 and the new error codes;
  - the detection-time table (§7.3) is added;
  - Limitations lists what remains O(n): fsck, the startup index build, one key copy per invariant rotation.
- AC-15.3 Status and dashboard show the lock metrics, open sessions, sweep backlog, foreign backlog, `outstanding_deletes`, `fingerprint_mismatch` and `orphan_gc_refused`.
- AC-15.4 `make down-v && make up && make test && make demo && python tests/chaos.py` pass twice from a clean clone.

---

## 11. Done checklist
- [ ] Every lock-held block is bounded (§1 limits). No DB or network I/O under the lock. Lock hold max < 50 ms in the scale test.
- [ ] All replica, pending, foreign and sweep mutations go through `ClusterState` and keep the buckets, counters and fingerprints exact. The rolling check finds zero drift in every test.
- [ ] No queue can starve another. Every eligible entry is examined within a bounded number of ticks.
- [ ] Upload consistency, crash recovery, report ordering and replication guarantees from v2 are unchanged. All v2 tests and the chaos test pass.
- [ ] Report back: changes per block, any deviations and why, the scale-test table, and the last chaos summary.
