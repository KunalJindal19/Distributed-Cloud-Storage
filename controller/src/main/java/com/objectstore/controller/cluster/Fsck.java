package com.objectstore.controller.cluster;

import com.objectstore.common.ChunkIds;
import com.objectstore.common.dto.ChunkInfo;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.ClusterState.WorkerState;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.config.WorkerIo;
import com.objectstore.controller.domain.Chunk;
import com.objectstore.controller.repository.ChunkRepository;
import com.objectstore.controller.repository.ObjectRepository;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * The equivalent of {@code hdfs fsck}: an end-to-end consistency check.
 *
 * <ol>
 *   <li>Every ALIVE worker is asked, in parallel, for the chunks on its disk with checksums
 *       recomputed from the data right now.</li>
 *   <li>That is compared with the block map (where the controller believes copies are) and with
 *       the namespace (what each chunk's checksum must be).</li>
 *   <li>The structural invariants are checked, plus I7: the in-memory chunk table equals the
 *       {@code chunks} table in the database.</li>
 * </ol>
 *
 * Besides, it reports (v5 §8): chunks with fewer than RF available (ALIVE) copies and unrecoverable
 * ones, chunks with surplus copies, quarantined copies on each worker, orphan copies, and workers
 * that are STALE, DEAD or have not completed a resync. It never deletes or changes anything.
 *
 * <p>Differences between disks and the block map are only meaningful while the cluster is
 * quiescent: no op pending, no command queued, not in safe mode, nothing in the apply queue, no
 * resync open and no worker being purged.
 *
 * <p>Order (v4 §5.10): take the namespace lock; queue a {@code Barrier} and wait for it (every
 * namespace task queued before is applied); read the database's chunk table and snapshot the
 * in-memory state (read lock); release the namespace lock; list the disks; compare; run the full
 * invariant check under the read lock.
 */
@Service
public class Fsck {

    private static final Logger log = LoggerFactory.getLogger(Fsck.class);

    public record ObjectCounts(long total, long healthy, long underReplicated, long corruptReplicas, long missing) {
    }

    public record DiskVsMap(List<String> onDiskNotInMap, List<String> inMapNotOnDisk, List<String> checksumMismatch) {
    }

    /** Chunk-level findings: fewer than RF available copies, none at all, and surplus copies. */
    public record ChunkFindings(long underReplicated, List<String> unrecoverable, long excess, long pendingQuarantines) {
    }

    /** A worker that is not a full member right now, and why; or a worker with quarantined copies. */
    public record WorkerFinding(String workerId, String state, boolean reported, boolean resyncing,
                                long quarantinedChunks, long quarantinedBytes) {
    }

    public record Result(
            String status,
            ObjectCounts objects,
            DiskVsMap diskVsMap,
            long orphansOnDisk,
            List<String> unreachableWorkers,
            List<String> invariantViolations,
            boolean quiescent,
            ChunkFindings chunks,
            List<WorkerFinding> workers) {
    }

    private record Snapshot(
            Map<String, String> aliveUrls,
            Map<String, ClusterState.ChunkInfo> chunks,
            Map<String, Set<String>> blocks,
            Set<String> pendingUploads) {
    }

    private final ClusterState cluster;
    private final WorkerClient workers;
    private final WorkerIo io;
    private final ChunkRepository chunkRepository;
    private final ObjectRepository objects;
    private final ControllerProperties properties;
    private final NamespaceLock namespaceLock;
    private final Writer writer;
    private final ApplyQueue queue;

    public Fsck(ClusterState cluster, WorkerClient workers, WorkerIo io, ChunkRepository chunkRepository,
                ObjectRepository objects, ControllerProperties properties, NamespaceLock namespaceLock, Writer writer,
                ApplyQueue queue) {
        this.cluster = cluster;
        this.workers = workers;
        this.io = io;
        this.chunkRepository = chunkRepository;
        this.objects = objects;
        this.properties = properties;
        this.namespaceLock = namespaceLock;
        this.writer = writer;
        this.queue = queue;
    }

    public Result run() {
        Snapshot snapshot;
        List<Chunk> database;
        long totalObjects;
        namespaceLock.lock();
        try {
            try {
                writer.barrier(properties.uploadCommitWait());
            } catch (java.util.concurrent.TimeoutException e) {
                log.warn("[fsck] the writer did not reach the barrier within {} s; comparing anyway",
                        properties.uploadCommitWaitSeconds());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            database = chunkRepository.findAll();
            totalObjects = objects.count();
            snapshot = cluster.read(this::snapshot);
        } finally {
            namespaceLock.unlock();
        }
        List<String> i7 = checkI7(database, snapshot.chunks());

        // 1. What is really on each live worker's disk (no lock held during network calls).
        List<String> workerIds = new ArrayList<>(snapshot.aliveUrls().keySet());
        List<Map<String, ChunkInfo>> listings = io.mapAll(workerIds, id -> {
            try {
                return workers.listChunks(snapshot.aliveUrls().get(id), id);
            } catch (WorkerException e) {
                return null;
            }
        });

        // 2. Compare disks with the map and the namespace.
        List<String> unreachable = new ArrayList<>();
        Set<String> onDiskNotInMap = new TreeSet<>();
        Set<String> inMapNotOnDisk = new TreeSet<>();
        Set<String> checksumMismatch = new TreeSet<>();
        long orphans = 0;
        for (int i = 0; i < workerIds.size(); i++) {
            String workerId = workerIds.get(i);
            Map<String, ChunkInfo> disk = listings.get(i);
            if (disk == null) {
                unreachable.add(workerId);
                continue;
            }
            for (ChunkInfo file : disk.values()) {
                ClusterState.ChunkInfo known = snapshot.chunks().get(file.chunkId());
                if (known == null) {
                    if (!snapshot.pendingUploads().contains(ChunkIds.objectIdOf(file.chunkId()))) {
                        orphans++;
                    }
                    continue;
                }
                if (!known.checksum().equals(file.checksum())) {
                    checksumMismatch.add(file.chunkId() + "@" + workerId);
                }
                if (!snapshot.blocks().getOrDefault(file.chunkId(), Set.of()).contains(workerId)) {
                    onDiskNotInMap.add(file.chunkId() + "@" + workerId);
                }
            }
            for (Map.Entry<String, Set<String>> entry : snapshot.blocks().entrySet()) {
                if (entry.getValue().contains(workerId) && !disk.containsKey(entry.getKey())) {
                    inMapNotOnDisk.add(entry.getKey() + "@" + workerId);
                }
            }
        }

        // 3. Invariants (I7 was compared above, from the snapshot), and the per-object picture.
        long orphanCount = orphans;
        return cluster.read(() -> {
            List<String> violations = new ArrayList<>(cluster.checkInvariants());
            violations.addAll(i7);
            ObjectCounts counts = countObjects(totalObjects);
            boolean quiescent = !cluster.isSafeMode() && cluster.ops().isEmpty() && cluster.queuedCommands() == 0
                    && queue.depth() == 0 && cluster.openSessions() == 0 && !cluster.anyPurging()
                    && cluster.resyncingWorkers() == 0;
            long[] health = cluster.health();
            ChunkFindings findings = new ChunkFindings(health[1] + health[0], cluster.unrecoverable(100), health[2],
                    cluster.ops().count(PendingOps.Reason.CORRUPT));
            List<WorkerFinding> workerFindings = new ArrayList<>();
            for (WorkerInfo worker : cluster.workersSorted()) {
                long quarantined = worker.stats == null ? 0 : worker.stats.quarantinedChunks();
                if (worker.state != WorkerState.ALIVE || !worker.hasReported || worker.resyncing || quarantined > 0) {
                    workerFindings.add(new WorkerFinding(worker.workerId, worker.state.name(), worker.hasReported,
                            worker.resyncing, quarantined, worker.stats == null ? 0 : worker.stats.quarantinedBytes()));
                }
            }
            DiskVsMap diskVsMap = new DiskVsMap(List.copyOf(onDiskNotInMap), List.copyOf(inMapNotOnDisk),
                    List.copyOf(checksumMismatch));
            String status;
            if (counts.missing() > 0) {
                status = "CORRUPT";
            } else if (counts.underReplicated() > 0 || counts.corruptReplicas() > 0 || !unreachable.isEmpty()
                    || !violations.isEmpty() || !checksumMismatch.isEmpty()
                    || (quiescent && (!onDiskNotInMap.isEmpty() || !inMapNotOnDisk.isEmpty() || orphanCount > 0))) {
                status = "DEGRADED";
            } else {
                status = "HEALTHY";
            }
            Result result = new Result(status, counts, diskVsMap, orphanCount, unreachable, violations, quiescent, findings,
                    workerFindings);
            log.info("[fsck] {} objects={} under={} corrupt_replicas={} missing={} orphans={} quiescent={}", status,
                    counts.total(), counts.underReplicated(), counts.corruptReplicas(), counts.missing(),
                    result.orphansOnDisk(), quiescent);
            return result;
        });
    }

    private Snapshot snapshot() {
        Map<String, String> alive = new LinkedHashMap<>();
        for (WorkerInfo worker : cluster.workersSorted()) {
            if (worker.state == WorkerState.ALIVE) {
                alive.put(worker.workerId, worker.url);
            }
        }
        Map<String, Set<String>> blocks = new HashMap<>();
        for (Map.Entry<String, Map<String, ClusterState.Replica>> entry : cluster.blocks().entrySet()) {
            blocks.put(entry.getKey(), Set.copyOf(entry.getValue().keySet()));
        }
        return new Snapshot(alive, Map.copyOf(cluster.chunks()), blocks, Set.copyOf(cluster.pendingUploads()));
    }

    /** I7: the in-memory chunk table mirrors the database exactly (both read at the barrier). */
    private List<String> checkI7(List<Chunk> rows, Map<String, ClusterState.ChunkInfo> memory) {
        List<String> violations = new ArrayList<>();
        Map<String, Chunk> database = new HashMap<>();
        for (Chunk chunk : rows) {
            database.put(chunk.chunkId(), chunk);
        }
        for (Chunk chunk : database.values()) {
            ClusterState.ChunkInfo mirror = memory.get(chunk.chunkId());
            if (mirror == null || !mirror.checksum().equals(chunk.checksum()) || mirror.size() != chunk.size()) {
                violations.add("I7: chunk " + chunk.chunkId() + " is in the database but not mirrored correctly in memory");
            }
        }
        for (String chunkId : memory.keySet()) {
            if (!database.containsKey(chunkId)) {
                violations.add("I7: chunk " + chunkId + " is in memory but not in the database");
            }
        }
        for (String violation : violations) {
            log.error("[invariant] {}", violation);
            cluster.increment("invariant_violations");
        }
        return violations;
    }

    private ObjectCounts countObjects(long totalObjects) {
        int rf = properties.replicationFactor();
        Map<String, int[]> perObject = new HashMap<>(); // [chunks under-replicated, chunks missing, corrupt replicas]
        long corruptReplicas = 0;
        for (Map.Entry<String, ClusterState.ChunkInfo> entry : cluster.chunks().entrySet()) {
            int[] counters = perObject.computeIfAbsent(entry.getValue().objectId(), id -> new int[3]);
            int live = cluster.live(entry.getKey()).size();
            if (live == 0) {
                counters[1]++;
            } else if (live < rf) {
                counters[0]++;
            }
            for (String workerId : cluster.replicasOf(entry.getKey()).keySet()) {
                if (cluster.ops().deleteReason(entry.getKey(), workerId) == PendingOps.Reason.CORRUPT) {
                    counters[2]++;
                    corruptReplicas++;
                }
            }
        }
        long missing = perObject.values().stream().filter(c -> c[1] > 0).count();
        long under = perObject.values().stream().filter(c -> c[1] == 0 && c[0] > 0).count();
        long unhealthy = perObject.values().stream().filter(c -> c[0] + c[1] + c[2] > 0).count();
        return new ObjectCounts(totalObjects, totalObjects - unhealthy, under, corruptReplicas, missing);
    }
}
