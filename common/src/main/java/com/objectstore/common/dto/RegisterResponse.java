package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Answer to a registration: the cluster id, and the registration resync: the fence {@code floor}
 * (t0) the worker adopts before it accepts any operation, and the report {@code attempt}.
 */
public record RegisterResponse(
        @JsonProperty("ok") boolean ok,
        @JsonProperty("cluster_id") String clusterId,
        @JsonProperty("state") String state,
        @JsonProperty("floor") long floor,
        @JsonProperty("attempt") int attempt) {
}
