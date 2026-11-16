package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/** The body of every non-2xx response: a stable machine-readable code plus a human sentence. */
public record ApiError(
        @JsonProperty("error") String error,
        @JsonProperty("detail") String detail) {
}
