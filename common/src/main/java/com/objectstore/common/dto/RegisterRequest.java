package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.objectstore.common.InternalApi;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/**
 * Body of {@code POST /internal/workers/register} (HDFS {@code registerDatanode}).
 *
 * @param bootId    uuid of this worker <em>process</em>; a new one on every start
 * @param clusterId the cluster id stored in the worker's {@code /data/VERSION}, or {@code null}
 *                  for a fresh volume
 * @param hbSeq     the last heartbeat sequence number this process used (-1 if none yet)
 * @param reportSeq the last report sequence number this process used (-1 if none yet)
 */
public record RegisterRequest(
        @JsonProperty("worker_id")
        @NotBlank(message = "worker_id is required")
        @Pattern(regexp = InternalApi.WORKER_ID_PATTERN, message = "worker_id has an invalid format")
        String workerId,

        @JsonProperty("url")
        @NotBlank(message = "url is required")
        @Pattern(regexp = "^https?://[^\\s/]+(:\\d+)?/?$", message = "url must look like http://host:port")
        String url,

        @JsonProperty("boot_id") @NotBlank(message = "boot_id is required") String bootId,
        @JsonProperty("cluster_id") String clusterId,
        @JsonProperty("hb_seq") long hbSeq,
        @JsonProperty("report_seq") long reportSeq,
        @JsonProperty("sent_at") double sentAt,
        @JsonProperty("stats") WorkerStats stats) {
}
