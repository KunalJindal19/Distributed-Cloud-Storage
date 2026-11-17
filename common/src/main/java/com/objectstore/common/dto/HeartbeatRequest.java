package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

/** Body of {@code POST /internal/workers/heartbeat}: the envelope plus storage statistics. */
public record HeartbeatRequest(
        @JsonProperty("worker_id") @NotBlank(message = "worker_id is required") String workerId,
        @JsonProperty("boot_id") @NotBlank(message = "boot_id is required") String bootId,
        @JsonProperty("seq") long seq,
        @JsonProperty("sent_at") double sentAt,
        @JsonProperty("stats") WorkerStats stats) {
}
