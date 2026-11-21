package com.objectstore.controller.objects;

import com.objectstore.controller.cluster.ClusterState;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.ClusterState.WorkerState;
import com.objectstore.controller.config.ControllerProperties;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.springframework.stereotype.Component;

/**
 * Decides which workers receive copies (the NameNode's block placement policy, minus racks).
 * Every method must be called under the cluster lock; all are deterministic.
 */
@Component
public class PlacementPolicy {

    private final ClusterState cluster;
    private final ControllerProperties properties;

    public PlacementPolicy(ClusterState cluster, ControllerProperties properties) {
        this.cluster = cluster;
        this.properties = properties;
    }

    /**
     * A worker may receive new data if it is ALIVE (or STALE while more than STALE_RATIO of the
     * non-DEAD workers are STALE, HDFS's stale ratio: otherwise a network blip would stop all
     * writes), has completed a resync and is not resyncing now (v5 §5.2), and has at least
     * MIN_FREE_BYTES free.
     */
    public boolean isWriteEligible(WorkerInfo worker) {
        if (worker.isDead() || !worker.hasReported || worker.resyncing) {
            return false;
        }
        if (worker.state == WorkerState.STALE && !staleRatioExceeded()) {
            return false;
        }
        return worker.stats == null || worker.stats.freeBytes() >= properties.minFreeBytes();
    }

    public boolean staleRatioExceeded() {
        long nonDead = cluster.workers().values().stream().filter(w -> !w.isDead()).count();
        long stale = cluster.workers().values().stream().filter(w -> w.state == WorkerState.STALE).count();
        return nonDead > 0 && (double) stale / nonDead > properties.staleRatio();
    }

    public List<WorkerInfo> writeEligibleWorkers() {
        List<WorkerInfo> eligible = new ArrayList<>();
        for (WorkerInfo worker : cluster.workersSorted()) {
            if (isWriteEligible(worker)) {
                eligible.add(worker);
            }
        }
        return eligible;
    }

    /**
     * Where the copies of one chunk go at upload time: the write-eligible workers minus
     * {@code exclude}, as a ring sorted by id. Chunk {@code i} starts at position
     * {@code (offset + i) mod n} where {@code offset} comes from the object id, so copies of a
     * chunk are always on different workers, consecutive chunks rotate around the ring, and
     * different objects start at different positions.
     *
     * @return all candidates in preference order: the first RF are the intended holders, the
     *         rest are the replacements if one of them fails (pipeline recovery)
     */
    public List<WorkerInfo> ringForUpload(String objectId, int chunkIndex, Set<String> exclude) {
        List<WorkerInfo> ring = new ArrayList<>();
        for (WorkerInfo worker : writeEligibleWorkers()) {
            if (!exclude.contains(worker.workerId)) {
                ring.add(worker);
            }
        }
        int n = ring.size();
        if (n == 0) {
            return ring;
        }
        int start = (int) ((ringOffset(objectId) + chunkIndex) % n);
        List<WorkerInfo> ordered = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            ordered.add(ring.get((start + i) % n));
        }
        return ordered;
    }

    /** A stable, evenly distributed number taken from the first 32 bits of the object's UUID. */
    static long ringOffset(String objectId) {
        return Long.parseLong(objectId.substring(0, 8), 16);
    }

    /**
     * Which holder of a chunk should send the repair copy: a copy with no pending DELETE on an ALIVE,
     * reported worker (a resyncing one may still send) with a free stream and room in its queue, that
     * did not recently fail with this chunk. Prefer the least busy source, then one without a recent
     * data error, then by id.
     */
    public Optional<String> chooseSource(String chunkId) {
        return chooseSource(chunkId, worker -> true);
    }

    /** {@link #chooseSource(String)} among the workers {@code allowed} accepts (the planner's free streams). */
    public Optional<String> chooseSource(String chunkId, java.util.function.Predicate<String> allowed) {
        long recent = properties.targetBackoff().toNanos();
        long now = cluster.now();
        List<String> candidates = new ArrayList<>();
        for (String workerId : cluster.replicasOf(chunkId).keySet()) {
            WorkerInfo worker = cluster.worker(workerId);
            if (worker != null && worker.state == WorkerState.ALIVE && worker.hasReported
                    && !cluster.ops().hasDelete(chunkId, workerId)
                    && cluster.ops().streamsOf(workerId) < properties.maxStreams()
                    && worker.commands.size() < properties.maxCommandsPerWorker()
                    && !cluster.recentlyFailed(chunkId, workerId) && allowed.test(workerId)) {
                candidates.add(workerId);
            }
        }
        return candidates.stream().min(Comparator
                .comparingInt((String w) -> cluster.ops().streamsOf(w))
                .thenComparingInt(w -> cluster.worker(w).lastDataError != 0 && now - cluster.worker(w).lastDataError < recent ? 1 : 0)
                .thenComparing(w -> w));
    }

    /**
     * Where a repair copy goes (v5 §6.3): an ALIVE, reported, non-resyncing, write-eligible worker
     * that does not hold the chunk, has no op on the pair and did not recently fail with it. Prefer
     * the worker with the fewest copies (held plus inbound), then by id.
     */
    public Optional<String> chooseTarget(String chunkId) {
        List<String> candidates = new ArrayList<>();
        for (WorkerInfo worker : writeEligibleWorkers()) {
            String id = worker.workerId;
            if (worker.state == WorkerState.ALIVE && cluster.replica(chunkId, id) == null
                    && cluster.ops().get(chunkId, id) == null && !cluster.recentlyFailed(chunkId, id)) {
                candidates.add(id);
            }
        }
        return candidates.stream().min(Comparator
                .comparingInt((String w) -> cluster.chunksOn(w).size() + cluster.ops().insertsTo(w))
                .thenComparing(w -> w));
    }

    /**
     * Which surplus copy an EXCESS delete removes (v5 §6.4): preferably one without fresh VERIFY
     * credit; among those the busiest worker (highest id breaks ties), which pulls the load back
     * towards even.
     */
    public Optional<String> chooseVictim(String chunkId) {
        List<String> holders = new ArrayList<>(cluster.replicasOf(chunkId).keySet());
        return holders.stream().min(Comparator
                .comparingInt((String w) -> cluster.freshForDelete(cluster.replica(chunkId, w), cluster.worker(w)) ? 1 : 0)
                .thenComparing(Comparator.comparingInt((String w) -> cluster.chunksOn(w).size()).reversed())
                .thenComparing(Comparator.<String>naturalOrder().reversed()));
    }

    /** Alive workers, for the insufficient-workers pre-check of an upload. */
    public int writeEligibleCount(Collection<WorkerInfo> workers) {
        return (int) workers.stream().filter(this::isWriteEligible).count();
    }
}
