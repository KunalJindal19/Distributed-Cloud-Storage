package com.objectstore.controller.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.objectstore.common.dto.WorkerStats;
import java.util.List;
import java.util.Map;

/** Response of {@code GET /api/v1/cluster/status}: the whole cluster at a glance. */
public record ClusterStatusResponse(
        Config config,
        String clusterId,
        boolean safeMode,
        List<WorkerView> workers,
        long objects,
        long chunks,
        ReplicaCounts replicas,
        int underReplicated,
        int overReplicated,
        int missingChunks,
        int pendingReplications,
        long blocked,
        Map<String, Long> counters,
        List<RepairEvent> repairLog,
        /** Longest continuous write-lock hold since the last status read (ms), and per holder. */
        double lockMaxHoldMs,
        Map<String, Double> lockMaxHoldBy,
        int applyQueueDepth,
        long applyOldestTaskAgeMs,
        /** Fraction of the time since the last status read that the writer held the lock. */
        double writerBusyRatio,
        long reportSessionsOpen,
        /** The deletion ledger: {waiting, queued, delivered}. */
        Map<String, Integer> ledger) {

    public record Config(
            int chunkSize,
            int replicationFactor,
            double staleAfter,
            double deadAfter,
            double monitorInterval,
            double pendingTimeout,
            double fullReportInterval) {
    }

    /**
     * One worker. {@code status} is kept as an alias of {@code state} for the v2 shape;
     * {@code chunk_count} is the number of copies recorded on the worker, whatever their state.
     */
    public record WorkerView(
            String workerId,
            String url,
            String status,
            String state,
            String bootId,
            boolean hasReported,
            String lastSeen,
            // Named explicitly: the snake_case strategy would render "lastSeenSAgo" as "last_seen_sago".
            @JsonProperty("last_seen_s_ago") double lastSeenSAgo,
            int chunkCount,
            int queuedCommands,
            int pendingAsSource,
            WorkerStats stats) {
    }

    public record ReplicaCounts(long ok, long corrupt, long excess) {
    }

    /**
     * One action of the replication monitor or one event it observed.
     *
     * @param event {@code copied}, {@code trimmed}, {@code invalidated}, {@code deleted},
     *              {@code timeout}, {@code unrecoverable}, {@code orphan}, {@code replicate}
     */
    public record RepairEvent(String time, String event, String chunkId, String source, String target, String reason) {
    }
}
