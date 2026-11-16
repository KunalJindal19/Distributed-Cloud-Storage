package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** Response of {@code GET /internal/workers/{worker_id}/chunks}. */
public record ChunkListResponse(
        @JsonProperty("worker_id") String workerId,
        @JsonProperty("chunks") List<ChunkInfo> chunks) {
}
