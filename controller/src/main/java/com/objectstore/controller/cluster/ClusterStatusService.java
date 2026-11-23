package com.objectstore.controller.cluster;

import com.objectstore.controller.api.dto.ClusterStatusResponse;
import com.objectstore.controller.api.dto.ClusterStatusResponse.Config;
import com.objectstore.controller.api.dto.ClusterStatusResponse.ReplicaCounts;
import com.objectstore.controller.api.dto.ClusterStatusResponse.WorkerView;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.repository.ObjectRepository;
import com.objectstore.controller.util.Timestamps;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Service;

/** Assembles the cluster overview from the in-memory state (one short pass under the lock). */
@Service
public class ClusterStatusService {

    private final ClusterState cluster;
    private final ControllerProperties properties;
    private final ObjectRepository objects;
    private final Clock clock;
    private final ApplyQueue queue;
    private final Writer writer;

    public ClusterStatusService(ClusterState cluster, ControllerProperties properties, ObjectRepository objects, Clock clock,
                                ApplyQueue queue, Writer writer) {
        this.cluster = cluster;
        this.properties = properties;
        this.objects = objects;
        this.clock = clock;
        this.queue = queue;
        this.writer = writer;
    }

    /**
     * Replication health: under-replicated = fewer than RF live copies (including missing chunks),
     * over-replicated = more than RF, missing = none. Live = OK copies on non-DEAD workers.
     */
    public record Health(int underReplicated, int overReplicated, int missing, long ok, long corrupt, long excess) {
    }

    /** O(1): every number comes from the priority index's counters. */
    public Health health() {
        long[] h = cluster.health();          // missing, under (1 <= live < RF), over, ok, corrupt, excess
        return new Health((int) (h[1] + h[0]), (int) h[2], (int) h[0], h[3], h[4], h[5]);
    }

    /**
     * What any signed-in client may know about the cluster (the web client's status chip): whether
     * writes are possible and whether data is unavailable. {@code objects}: the caller's own count.
     */
    public record HealthSummary(boolean safeMode, int workersReady, int workersTotal, int replicationFactor,
                                int missingChunks, int underReplicated, long objects) {
    }

    public HealthSummary summary(long visibleObjects) {
        return cluster.read(() -> {
            Health health = health();
            int ready = 0;
            for (WorkerInfo worker : cluster.workers().values()) {
                if (worker.state == ClusterState.WorkerState.ALIVE && worker.hasReported) {
                    ready++;
                }
            }
            return new HealthSummary(cluster.isSafeMode(), ready, cluster.workers().size(),
                    properties.replicationFactor(), health.missing(), health.underReplicated(), visibleObjects);
        });
    }

    public ClusterStatusResponse status() {
        long objectCount = objects.count();
        Map<String, Double> lockMaxHold = cluster.takeLockMaxHoldMs();   // reset by reading
        return cluster.read(() -> {
            Health health = health();
            long onStale = 0;
            for (WorkerInfo worker : cluster.workers().values()) {
                if (worker.state == ClusterState.WorkerState.STALE) {
                    onStale += cluster.chunksOn(worker.workerId).size();     // O(workers): set sizes are O(1)
                }
            }
            List<WorkerView> workers = new ArrayList<>();
            for (WorkerInfo worker : cluster.workersSorted()) {
                double ago = cluster.secondsSince(worker.lastSeen);
                workers.add(new WorkerView(
                        worker.workerId,
                        worker.url,
                        worker.state.name(),
                        worker.state.name(),
                        Registry.shortId(worker.bootId),
                        worker.hasReported,
                        Timestamps.format(clock.instant().minus(Duration.ofMillis((long) (ago * 1000)))),
                        Math.round(ago * 10) / 10.0,
                        cluster.chunksOn(worker.workerId).size(),
                        worker.commands.size(),
                        cluster.ops().streamsOf(worker.workerId),
                        worker.stats,
                        worker.resyncing,
                        worker.fenceFloor,
                        worker.attempt,
                        worker.verifyEpoch,
                        cluster.ops().opsOn(worker.workerId).size()));
            }
            Map<String, Long> counters = cluster.counters();
            for (String name : List.of("invariant_violations", "orphan_gc_refused", "stale_messages", "duplicates", "blocked",
                    "changes_gap", "apply_queue_full", "writer_task_failures", "namespace_reconciled",
                    "index_mismatches", "upload_commit_wait_timeouts", "stale_rejections", "ops_resent", "resyncs",
                    "quarantines", "excess_postponed", "verify_requests")) {
                counters.putIfAbsent(name, 0L);
            }
            return new ClusterStatusResponse(
                    new Config(properties.chunkSize(), properties.replicationFactor(), properties.staleAfterSeconds(),
                            properties.deadAfterSeconds(), properties.monitorIntervalSeconds(),
                            properties.opResendInitialSeconds(), properties.resyncIntervalSeconds()),
                    cluster.clusterId(),
                    cluster.isSafeMode(),
                    workers,
                    objectCount,
                    cluster.chunks().size(),
                    new ReplicaCounts(health.ok(), health.corrupt(), health.excess(), onStale),
                    health.underReplicated(),
                    health.overReplicated(),
                    health.missing(),
                    cluster.ops().inserts(),
                    cluster.blockedLastTick(),
                    counters,
                    cluster.events(),
                    lockMaxHold.values().stream().mapToDouble(Double::doubleValue).max().orElse(0),
                    lockMaxHold,
                    queue.depth(),
                    queue.oldestAgeMillis(),
                    writer.takeBusyRatio(),
                    cluster.openSessions(),
                    cluster.ops().deleteCounts(),
                    cluster.levels().sizes(),
                    cluster.cleanupSize(),
                    cluster.postponedSize(),
                    cluster.ops().counts(),
                    cluster.verifyOutstanding(),
                    cluster.resyncingWorkers(),
                    cluster.versions().term(),
                    cluster.versions().current());
        });
    }

}
