package com.objectstore.controller.cluster;

import com.objectstore.controller.api.dto.ClusterStatusResponse;
import com.objectstore.controller.api.dto.ClusterStatusResponse.Config;
import com.objectstore.controller.api.dto.ClusterStatusResponse.ReplicaCounts;
import com.objectstore.controller.api.dto.ClusterStatusResponse.WorkerView;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.domain.ReplicaStatus;
import com.objectstore.controller.replication.RepairLog;
import com.objectstore.controller.repository.ChunkRepository;
import com.objectstore.controller.repository.ObjectRepository;
import com.objectstore.controller.util.Timestamps;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Service;

/** Assembles the cluster overview. Reads the database and the in-memory repair log only. */
@Service
public class ClusterStatusService {

    private final ControllerProperties properties;
    private final WorkerRegistry registry;
    private final ObjectRepository objects;
    private final ChunkRepository chunks;
    private final RepairLog repairLog;

    public ClusterStatusService(ControllerProperties properties, WorkerRegistry registry, ObjectRepository objects,
                                ChunkRepository chunks, RepairLog repairLog) {
        this.properties = properties;
        this.registry = registry;
        this.objects = objects;
        this.chunks = chunks;
        this.repairLog = repairLog;
    }

    public ClusterStatusResponse status() {
        Map<String, Integer> replicaCounts = registry.replicaCounts();
        List<WorkerView> workers = registry.allWorkers().stream()
                .map(worker -> new WorkerView(
                        worker.workerId(),
                        worker.url(),
                        worker.status().name(),
                        Timestamps.format(worker.lastSeen()),
                        replicaCounts.getOrDefault(worker.workerId(), 0)))
                .toList();
        Map<ReplicaStatus, Long> byStatus = chunks.countReplicasByStatus();
        int replicationFactor = properties.replicationFactor();
        return new ClusterStatusResponse(
                new Config(
                        properties.chunkSize(),
                        replicationFactor,
                        properties.heartbeatTimeoutSeconds(),
                        properties.repairIntervalSeconds(),
                        properties.scrubIntervalSeconds()),
                workers,
                objects.count(),
                chunks.countChunks(),
                new ReplicaCounts(
                        byStatus.get(ReplicaStatus.OK),
                        byStatus.get(ReplicaStatus.CORRUPT),
                        byStatus.get(ReplicaStatus.MISSING)),
                chunks.findUnderReplicated(replicationFactor).size(),
                chunks.findOverReplicated(replicationFactor).size(),
                repairLog.snapshot());
    }
}
