package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/** One chunk in a full-report segment; {@code bad} = its data failed verification in this process. */
public record ReportEntry(
        @JsonProperty("chunk_id") String chunkId,
        @JsonProperty("size") long size,
        @JsonProperty("checksum") String checksum,
        @JsonProperty("bad") boolean bad) {
}
