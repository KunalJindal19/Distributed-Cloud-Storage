package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One numbered change to a worker's disk ({@link ChangeType}). Numbers start at 1 per worker
 * process and are consecutive; the worker keeps a change until the controller acknowledges having
 * received it. {@code version} is the version of the record the change produced (or, for
 * {@code CMD_FAILED}, of the operation that failed).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Change(
        @JsonProperty("seq") long seq,
        @JsonProperty("type") ChangeType type,
        @JsonProperty("chunk_id") String chunkId,
        @JsonProperty("size") Long size,
        @JsonProperty("checksum") String checksum,
        @JsonProperty("version") long version,
        @JsonProperty("cmd_id") String cmdId,
        @JsonProperty("target") String target,
        @JsonProperty("reason") String reason,
        @JsonProperty("detail") String detail) {

    public static Change received(long seq, String chunkId, long size, String checksum, long version, String cmdId) {
        return new Change(seq, ChangeType.RECEIVED, chunkId, size, checksum, version, cmdId, null, null, null);
    }

    public static Change deleted(long seq, String chunkId, long version, String detail) {
        return new Change(seq, ChangeType.DELETED, chunkId, null, null, version, null, null, null, detail);
    }

    public static Change cmdFailed(long seq, String chunkId, long version, String reason, String target, String cmdId,
                                   String detail) {
        return new Change(seq, ChangeType.CMD_FAILED, chunkId, null, null, version, cmdId, target, reason, detail);
    }
}
