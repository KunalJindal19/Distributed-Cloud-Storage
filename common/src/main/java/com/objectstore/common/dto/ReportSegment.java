package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * One segment of a resync listing, carried by a heartbeat (v5 §5.3). Segments of one listing share
 * its {@code report_id}, the fence {@code floor} (t0) and {@code attempt} they answer, and
 * {@code baseline} M (the last change number the listing covers); they are sent stop-and-wait:
 * segment {@code index + 1} only after {@code index} was acknowledged.
 */
public record ReportSegment(
        @JsonProperty("report_id") String reportId,
        @JsonProperty("floor") long floor,
        @JsonProperty("attempt") int attempt,
        @JsonProperty("index") int index,
        @JsonProperty("last") boolean last,
        @JsonProperty("baseline") long baseline,
        @JsonProperty("entries") List<ReportEntry> entries) {
}
