package com.objectstore.controller.domain;

import java.time.Instant;

/** One copy of a chunk, joined with the worker that holds it. */
public record ReplicaLocation(
        String chunkId,
        String workerId,
        String workerUrl,
        ReplicaStatus status,
        WorkerStatus workerStatus,
        Instant updatedAt) {

    /**
     * The one definition everything shares: a <em>good copy</em> is a copy believed intact on a
     * worker that is alive. Replication health, the download pre-check, repair and trimming all
     * count copies with this rule, so they can never disagree with each other.
     */
    public boolean isGood() {
        return status == ReplicaStatus.OK && workerStatus == WorkerStatus.ALIVE;
    }

    /** A known-bad copy on a reachable worker: something the repair loop can clean up. */
    public boolean isStale() {
        return status != ReplicaStatus.OK && workerStatus == WorkerStatus.ALIVE;
    }
}
