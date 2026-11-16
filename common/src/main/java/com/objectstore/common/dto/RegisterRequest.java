package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.objectstore.common.InternalApi;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/** Body of {@code POST /internal/workers/register}. */
public record RegisterRequest(
        @JsonProperty("worker_id")
        @NotBlank(message = "worker_id is required")
        @Pattern(regexp = InternalApi.WORKER_ID_PATTERN, message = "worker_id has an invalid format")
        String workerId,

        @JsonProperty("url")
        @NotBlank(message = "url is required")
        @Pattern(regexp = "^https?://[^\\s/]+(:\\d+)?/?$", message = "url must look like http://host:port")
        String url) {
}
