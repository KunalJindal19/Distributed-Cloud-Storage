package com.objectstore.controller.domain;

import java.time.Instant;

/** A copy as recorded for one worker, with what the file on that worker is expected to contain. */
public record WorkerReplica(
        String chunkId,
        ReplicaStatus status,
        Instant updatedAt,
        String expectedChecksum,
        long expectedSize) {
}
