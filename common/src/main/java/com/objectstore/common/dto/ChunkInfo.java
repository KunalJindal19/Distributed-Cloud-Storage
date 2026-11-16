package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/** One chunk as it exists on a worker's disk. The checksum is computed from the stored bytes. */
public record ChunkInfo(
        @JsonProperty("chunk_id") String chunkId,
        @JsonProperty("size") long size,
        @JsonProperty("checksum") String checksum) {
}
