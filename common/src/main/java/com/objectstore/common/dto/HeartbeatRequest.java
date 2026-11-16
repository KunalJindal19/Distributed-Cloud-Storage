package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

/** Body of {@code POST /internal/workers/heartbeat}. */
public record HeartbeatRequest(
        @JsonProperty("worker_id")
        @NotBlank(message = "worker_id is required")
        String workerId) {
}
