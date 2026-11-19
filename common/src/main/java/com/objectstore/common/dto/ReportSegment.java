package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * One segment of a full report, carried by a heartbeat. Segments of one report share its
 * {@code report_id} and {@code baseline} (the last change number the listing includes) and are
 * sent stop-and-wait: segment {@code index + 1} only after {@code index} was acknowledged.
 */
public record ReportSegment(
        @JsonProperty("report_id") String reportId,
        @JsonProperty("index") int index,
        @JsonProperty("last") boolean last,
        @JsonProperty("baseline") long baseline,
        @JsonProperty("entries") List<ReportEntry> entries) {
}
