package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/** The controller acknowledges having received a report segment. */
public record SegmentAck(
        @JsonProperty("report_id") String reportId,
        @JsonProperty("index") int index) {
}
