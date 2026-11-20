package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * The controller's answer to a heartbeat: the worker's state, its commands, and the
 * acknowledgements: {@code changes_received_up_to} (the worker may forget every change up to that
 * number) and {@code segment_ack} (the report segment that was received).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record HeartbeatResponse(
        @JsonProperty("ok") boolean ok,
        @JsonProperty("state") String state,
        @JsonProperty("commands") List<Command> commands,
        @JsonProperty("request_full_report") boolean requestFullReport,
        @JsonProperty("ignored") String ignored,
        @JsonProperty("changes_received_up_to") long changesReceivedUpTo,
        @JsonProperty("segment_ack") SegmentAck segmentAck) {
}
