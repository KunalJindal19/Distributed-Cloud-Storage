package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** The controller's answer to a heartbeat: the worker's state and the commands for it. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record HeartbeatResponse(
        @JsonProperty("ok") boolean ok,
        @JsonProperty("state") String state,
        @JsonProperty("commands") List<Command> commands,
        @JsonProperty("request_full_report") boolean requestFullReport,
        @JsonProperty("ignored") String ignored) {
}
