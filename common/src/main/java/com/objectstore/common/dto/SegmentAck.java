package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * What the controller did with a resync segment: {@code ok} (accepted, or a duplicate of the current
 * attempt), {@code done} (that whole attempt already completed) or {@code stale} (not applied).
 */
public record SegmentAck(
        @JsonProperty("report_id") String reportId,
        @JsonProperty("index") int index,
        @JsonProperty("status") String status) {
}
