package com.objectstore.controller.cluster;

import com.objectstore.controller.api.dto.ClusterStatusResponse;
import com.objectstore.controller.api.dto.ClusterStatusResponse.Config;
import com.objectstore.controller.api.dto.ClusterStatusResponse.ReplicaCounts;
import com.objectstore.controller.api.dto.ClusterStatusResponse.WorkerView;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.ReplicaState;
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

    public ClusterStatusResponse status() {
        long objectCount = objects.count();
        Map<String, Double> lockMaxHold = cluster.takeLockMaxHoldMs();   // reset by reading
        return cluster.read(() -> {
            Health health = health();
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
                        cluster.pendingFrom(worker.workerId),
                        worker.stats));
            }
            Map<String, Long> counters = cluster.counters();
            for (String name : List.of("invariant_violations", "orphan_gc_refused", "stale_messages", "duplicates", "blocked",
                    "changes_gap", "apply_queue_full", "writer_task_failures", "namespace_reconciled",
                    "index_mismatches", "upload_commit_wait_timeouts")) {
                counters.putIfAbsent(name, 0L);
            }
            return new ClusterStatusResponse(
                    new Config(properties.chunkSize(), properties.replicationFactor(), properties.staleAfterSeconds(),
                            properties.deadAfterSeconds(), properties.monitorIntervalSeconds(),
                            properties.pendingTimeoutSeconds(), properties.fullReportIntervalSeconds()),
                    cluster.clusterId(),
                    cluster.isSafeMode(),
                    workers,
                    objectCount,
                    cluster.chunks().size(),
                    new ReplicaCounts(health.ok(), health.corrupt(), health.excess()),
                    health.underReplicated(),
                    health.overReplicated(),
                    health.missing(),
                    cluster.pendingTotal(),
                    cluster.blockedLastTick(),
                    counters,
                    cluster.events(),
                    lockMaxHold.values().stream().mapToDouble(Double::doubleValue).max().orElse(0),
                    lockMaxHold,
                    queue.depth(),
                    queue.oldestAgeMillis(),
                    writer.takeBusyRatio(),
                    cluster.openSessions(),
                    cluster.ledger().counts(),
                    cluster.levels().sizes(),
                    cluster.cleanupSize(),
                    cluster.postponedSize());
        });
    }

    /** Copy states on a chunk, as the status endpoint names them. */
    static String stateName(ReplicaState state) {
        return state.name();
    }
}
