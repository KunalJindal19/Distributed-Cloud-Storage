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
        /** INSERT ops (repair and upload copies on their way). */
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
        /** DELETE ops by state, under the v4 names {waiting, queued, delivered}. */
        Map<String, Integer> ledger,
        /** Chunks needing copies by priority level {"L,P": size} (L live, P pending). */
        Map<String, Integer> levels,
        int cleanupSize,
        /** Chunks whose surplus copy waits: holders not current, or survivors still being verified. */
        int postponedSize,
        /** v5: pending ops {by_reason, by_state}. */
        Map<String, Map<String, Integer>> ops,
        /** v5: VERIFY requests outstanding. */
        int verifyOutstanding,
        /** v5: workers resyncing now. */
        int resyncing,
        /** v5: the controller term, and the highest operation version issued. */
        long term,
        long version) {

    public record Config(
            int chunkSize,
            int replicationFactor,
            double staleAfter,
            double deadAfter,
            double monitorInterval,
            double opResendInitial,
            double resyncInterval) {
    }

    /**
     * One worker. {@code status} is kept as an alias of {@code state} for the v2 shape;
     * {@code chunk_count} is the number of copies recorded on the worker.
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
            WorkerStats stats,
            boolean resyncing,
            long fence,
            int attempt,
            long verifyEpoch,
            int pendingOps) {
    }

    /**
     * Copies by the state the API shows: {@code ok}, {@code corrupt} (a quarantine is pending),
     * {@code excess} (an EXCESS delete is pending); {@code on_stale_workers} are uncertain (their worker
     * is STALE), so a chunk never looks fully available because of them.
     */
    public record ReplicaCounts(long ok, long corrupt, long excess, long onStaleWorkers) {
    }

    /**
     * One action of the replication monitor or one event it observed.
     *
     * @param event {@code replicate}, {@code copied}, {@code trimmed}, {@code quarantined}, {@code deleted},
     *              {@code missing}, {@code timeout}, {@code failed}, {@code source failed}, {@code stale},
     *              {@code unrecoverable}, {@code recoverable}, {@code orphan}
     */
    public record RepairEvent(String time, String event, String chunkId, String source, String target, String reason) {
    }
}
