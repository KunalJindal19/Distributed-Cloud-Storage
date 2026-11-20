package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Answer of {@code PUT /internal/chunks/{chunk_id}}. When the controller's request carried
 * {@code X-Change-Ack} for this worker process, {@code changes} holds the worker's next changes
 * (so an upload learns its own copies without waiting for a heartbeat).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ChunkPutResponse(
        @JsonProperty("chunk_id") String chunkId,
        @JsonProperty("size") long size,
        @JsonProperty("checksum") String checksum,
        @JsonProperty("changes") ChangeBatch changes) {
}
