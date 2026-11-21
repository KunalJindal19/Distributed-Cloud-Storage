package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/** One chunk in a resync listing: its size, sidecar checksum and record version on the worker. */
public record ReportEntry(
        @JsonProperty("chunk_id") String chunkId,
        @JsonProperty("size") long size,
        @JsonProperty("checksum") String checksum,
        @JsonProperty("version") long version) {
}
