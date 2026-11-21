package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * The controller's answer to a heartbeat: the worker's state, its commands, a resync request
 * ({@code resync}, until the attempt's first segment has arrived), and the acknowledgements:
 * {@code changes_received_up_to} (the worker may forget every change up to that number) and
 * {@code segment_ack} (what became of the segment it sent).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record HeartbeatResponse(
        @JsonProperty("ok") boolean ok,
        @JsonProperty("state") String state,
        @JsonProperty("commands") List<Command> commands,
        @JsonProperty("resync") Resync resync,
        @JsonProperty("ignored") String ignored,
        @JsonProperty("changes_received_up_to") long changesReceivedUpTo,
        @JsonProperty("segment_ack") SegmentAck segmentAck) {
}
