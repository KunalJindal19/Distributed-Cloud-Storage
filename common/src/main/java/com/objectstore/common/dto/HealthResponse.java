package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Response of {@code GET /health} on both services. {@code worker_id} is present on workers only. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record HealthResponse(
        @JsonProperty("status") String status,
        @JsonProperty("service") String service,
        @JsonProperty("worker_id") String workerId) {

    public static HealthResponse controller() {
        return new HealthResponse("ok", "controller", null);
    }

    public static HealthResponse worker(String workerId) {
        return new HealthResponse("ok", "worker", workerId);
    }
}
