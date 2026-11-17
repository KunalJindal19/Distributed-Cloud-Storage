package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record RegisterResponse(
        @JsonProperty("ok") boolean ok,
        @JsonProperty("cluster_id") String clusterId,
        @JsonProperty("state") String state) {
}
