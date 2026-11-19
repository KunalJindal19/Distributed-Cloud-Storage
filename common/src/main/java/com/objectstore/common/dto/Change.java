package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One numbered change to a worker's disk ({@link ChangeType}). Numbers start at 1 per worker
 * process and are consecutive; the worker keeps a change until the controller acknowledges having
 * received it.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Change(
        @JsonProperty("seq") long seq,
        @JsonProperty("type") ChangeType type,
        @JsonProperty("chunk_id") String chunkId,
        @JsonProperty("size") Long size,
        @JsonProperty("checksum") String checksum,
        @JsonProperty("cmd_id") String cmdId,
        @JsonProperty("target") String target,
        @JsonProperty("reason") String reason) {
}
