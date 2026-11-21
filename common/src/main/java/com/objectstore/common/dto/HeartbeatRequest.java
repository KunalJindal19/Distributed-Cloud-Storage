package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import java.util.List;

/**
 * Body of {@code POST /internal/workers/heartbeat}: the envelope, storage statistics, the
 * worker's next changes (a contiguous run, possibly empty), at most one resync segment, and
 * {@code resync_wanted} ({@code overflow} or {@code tombstones}) when the worker asks for a resync.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record HeartbeatRequest(
        @JsonProperty("worker_id") @NotBlank(message = "worker_id is required") String workerId,
        @JsonProperty("boot_id") @NotBlank(message = "boot_id is required") String bootId,
        @JsonProperty("seq") long seq,
        @JsonProperty("sent_at") double sentAt,
        @JsonProperty("stats") WorkerStats stats,
        @JsonProperty("changes") List<Change> changes,
        @JsonProperty("segment") ReportSegment segment,
        @JsonProperty("resync_wanted") String resyncWanted) {

    /** A heartbeat with no changes, no segment and no resync request. */
    public HeartbeatRequest(String workerId, String bootId, long seq, double sentAt, WorkerStats stats) {
        this(workerId, bootId, seq, sentAt, stats, List.of(), null, null);
    }
}
