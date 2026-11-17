package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import java.util.List;

/** A full block report: every chunk the worker holds, with the checksum from its {@code .meta}. */
public record BlockReportRequest(
        @JsonProperty("worker_id") @NotBlank(message = "worker_id is required") String workerId,
        @JsonProperty("boot_id") @NotBlank(message = "boot_id is required") String bootId,
        @JsonProperty("seq") long seq,
        @JsonProperty("sent_at") double sentAt,
        @JsonProperty("chunks") List<ChunkInfo> chunks) {
}
