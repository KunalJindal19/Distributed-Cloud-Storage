package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Acknowledgement of a full or incremental report; {@code ignored} explains a no-op. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ReportResponse(
        @JsonProperty("ok") boolean ok,
        @JsonProperty("ignored") String ignored) {
}
