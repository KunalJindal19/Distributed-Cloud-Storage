package com.objectstore.controller.api.dto;

import java.util.List;

/** Response of {@code GET /api/v1/cluster/status}: the whole cluster at a glance. */
public record ClusterStatusResponse(
        Config config,
        List<WorkerView> workers,
        long objects,
        long chunks,
        ReplicaCounts replicas,
        int underReplicated,
        int overReplicated,
        List<RepairEvent> repairLog) {

    public record Config(
            int chunkSize,
            int replicationFactor,
            double heartbeatTimeout,
            double repairInterval,
            double scrubInterval) {
    }

    /** {@code chunkCount} is the number of copies recorded on the worker, whatever their state. */
    public record WorkerView(String workerId, String url, String status, String lastSeen, int chunkCount) {
    }

    public record ReplicaCounts(long ok, long corrupt, long missing) {
    }

    /**
     * One action of the repair loop.
     *
     * @param event  {@code copied}, {@code trimmed}, {@code purged} or {@code unrecoverable}
     * @param source for {@code copied}: the worker the bytes were read from; for {@code trimmed}
     *               and {@code purged}: the worker the copy was removed from
     * @param target for {@code copied}: the worker that received the new copy
     */
    public record RepairEvent(String time, String event, String chunkId, String source, String target, String reason) {
    }
}
