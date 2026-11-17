package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import java.util.List;

/**
 * An incremental block report (IBR): what changed on the worker since its previous report.
 *
 * @param received chunks newly stored
 * @param deleted  chunks removed
 * @param bad      chunks whose bytes no longer match their {@code .meta} checksum
 */
public record IncrementalReportRequest(
        @JsonProperty("worker_id") @NotBlank(message = "worker_id is required") String workerId,
        @JsonProperty("boot_id") @NotBlank(message = "boot_id is required") String bootId,
        @JsonProperty("seq") long seq,
        @JsonProperty("sent_at") double sentAt,
        @JsonProperty("received") List<ChunkInfo> received,
        @JsonProperty("deleted") List<String> deleted,
        @JsonProperty("bad") List<String> bad) {
}
