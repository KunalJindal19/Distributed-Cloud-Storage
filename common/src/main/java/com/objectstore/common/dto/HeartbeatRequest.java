package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import java.util.List;

/**
 * Body of {@code POST /internal/workers/heartbeat}: the envelope, storage statistics, the
 * worker's next changes (a contiguous run, possibly empty) and at most one full-report segment.
 */
public record HeartbeatRequest(
        @JsonProperty("worker_id") @NotBlank(message = "worker_id is required") String workerId,
        @JsonProperty("boot_id") @NotBlank(message = "boot_id is required") String bootId,
        @JsonProperty("seq") long seq,
        @JsonProperty("sent_at") double sentAt,
        @JsonProperty("stats") WorkerStats stats,
        @JsonProperty("changes") List<Change> changes,
        @JsonProperty("segment") ReportSegment segment) {

    /** A heartbeat with no changes and no segment. */
    public HeartbeatRequest(String workerId, String bootId, long seq, double sentAt, WorkerStats stats) {
        this(workerId, bootId, seq, sentAt, stats, List.of(), null);
    }
}
