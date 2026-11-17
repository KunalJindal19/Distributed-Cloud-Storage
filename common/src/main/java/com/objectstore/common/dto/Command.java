package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * An instruction from the controller to a worker, delivered in a heartbeat reply.
 *
 * <ul>
 *   <li>{@code REPLICATE} (HDFS {@code DNA_TRANSFER}): send your copy of {@code chunk_id}
 *       directly to the worker at {@code target_url}.</li>
 *   <li>{@code DELETE} (HDFS {@code DNA_INVALIDATE}): delete your copy; {@code reason} is
 *       {@code excess}, {@code corrupt}, {@code orphan} or {@code object_deleted}.</li>
 * </ul>
 *
 * A worker ignores a command once {@code expires_at} (wall-clock seconds) has passed, and
 * executes a given {@code cmd_id} at most once.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Command(
        @JsonProperty("cmd_id") String cmdId,
        @JsonProperty("type") String type,
        @JsonProperty("chunk_id") String chunkId,
        @JsonProperty("checksum") String checksum,
        @JsonProperty("target_url") String targetUrl,
        @JsonProperty("reason") String reason,
        @JsonProperty("expires_at") double expiresAt) {
}
