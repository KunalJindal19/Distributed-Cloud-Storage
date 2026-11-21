package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * An instruction from the controller to a worker, delivered in a heartbeat reply. Every command
 * carries the {@code version} of the operation it belongs to (v5 §3); the worker's version check
 * decides whether it is still current. There is no expiry time: versions alone decide what is stale.
 *
 * <ul>
 *   <li>{@code REPLICATE} (HDFS {@code DNA_TRANSFER}): send your copy of {@code chunk_id} to the
 *       worker at {@code target_url}, to be installed there as {@code version}.</li>
 *   <li>{@code DELETE} (HDFS {@code DNA_INVALIDATE}): remove your copy as of {@code version};
 *       {@code reason} is {@code excess}, {@code corrupt} (quarantine), {@code orphan},
 *       {@code object_deleted} or {@code upload_failed}.</li>
 *   <li>{@code VERIFY}: hash your copy now and compare it with its sidecar and
 *       {@code expected_checksum}; answer with {@code RECEIVED(cmd_id)} if it is good.</li>
 * </ul>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Command(
        @JsonProperty("cmd_id") String cmdId,
        @JsonProperty("type") String type,
        @JsonProperty("chunk_id") String chunkId,
        @JsonProperty("expected_checksum") String expectedChecksum,
        @JsonProperty("target_url") String targetUrl,
        @JsonProperty("reason") String reason,
        @JsonProperty("version") long version) {
}
