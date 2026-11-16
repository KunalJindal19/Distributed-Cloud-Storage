package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/** The controller's answer to a registration or a heartbeat. */
public record AckResponse(
        @JsonProperty("ok") boolean ok,
        @JsonProperty("worker_id") String workerId,
        @JsonProperty("status") String status) {
}
