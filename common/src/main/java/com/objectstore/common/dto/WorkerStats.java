package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Storage statistics a worker sends with every heartbeat and registration. */
public record WorkerStats(
        @JsonProperty("chunks") long chunks,
        @JsonProperty("used_bytes") long usedBytes,
        @JsonProperty("free_bytes") long freeBytes,
        @JsonProperty("active_transfers") int activeTransfers,
        @JsonProperty("quarantined_chunks") long quarantinedChunks,
        @JsonProperty("quarantined_bytes") long quarantinedBytes,
        @JsonProperty("tombstones") long tombstones) {

    public WorkerStats(long chunks, long usedBytes, long freeBytes, int activeTransfers) {
        this(chunks, usedBytes, freeBytes, activeTransfers, 0, 0, 0);
    }
}
