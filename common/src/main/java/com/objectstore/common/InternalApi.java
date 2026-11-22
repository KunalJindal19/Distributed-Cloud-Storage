package com.objectstore.common;

/** Names shared by both ends of the controller &harr; worker protocol (the HDFS NameNode/DataNode protocol). */
public final class InternalApi {

    /**
     * On a chunk PUT: the SHA-256 the body must have (the namespace checksum). On a GET answer: the
     * checksum in the worker's sidecar.
     */
    public static final String CHECKSUM_HEADER = "X-Checksum";
    /**
     * On a chunk PUT or DELETE (required): the operation version the copy is installed or deleted at
     * (v5 §4.3, §4.4).
     */
    public static final String OP_VERSION_HEADER = "X-Op-Version";
    /** On a chunk DELETE (optional): the controller's command id, reported back in a {@code CMD_FAILED}. */
    public static final String CMD_ID_HEADER = "X-Cmd-Id";
    /** On a chunk DELETE (optional): the delete reason; {@code corrupt} quarantines the copy instead. */
    public static final String REASON_HEADER = "X-Reason";
    /**
     * On the controller's chunk PUTs: {@code <boot_id>:<n>}, the last change of that worker process
     * the controller has received. The worker forgets changes up to {@code n} and answers with the
     * next ones.
     */
    public static final String CHANGE_ACK_HEADER = "X-Change-Ack";

    // Worker -> controller (DatanodeProtocol)
    public static final String REGISTER_PATH = "/internal/workers/register";
    /** Heartbeats carry the change stream and resync segments: there are no report endpoints. */
    public static final String HEARTBEAT_PATH = "/internal/workers/heartbeat";

    // Controller -> worker and worker -> worker (DataTransferProtocol)
    public static final String CHUNK_PATH = "/internal/chunks/{chunkId}";
    public static final String WORKER_CHUNKS_PATH = "/internal/workers/{workerId}/chunks";

    /** Worker ids double as Docker host names, so keep them to a conservative alphabet. */
    public static final String WORKER_ID_PATTERN = "^[A-Za-z0-9][A-Za-z0-9_.-]{0,31}$";

    // Command types.
    public static final String COMMAND_REPLICATE = "REPLICATE";
    public static final String COMMAND_DELETE = "DELETE";
    public static final String COMMAND_VERIFY = "VERIFY";

    // DELETE reasons (Command.reason); "corrupt" quarantines instead of deleting.
    public static final String REASON_EXCESS = "excess";
    public static final String REASON_CORRUPT = "corrupt";
    public static final String REASON_ORPHAN = "orphan";
    public static final String REASON_OBJECT_DELETED = "object_deleted";
    public static final String REASON_UPLOAD_FAILED = "upload_failed";

    // CMD_FAILED reasons (Change.reason), v5 §3.4.
    public static final String FAILED_STALE = "stale";
    public static final String FAILED_BUSY = "busy";
    public static final String FAILED_IO_ERROR = "io_error";
    public static final String FAILED_NO_LOCAL_COPY = "no_local_copy";
    public static final String FAILED_LOCAL_COPY_CORRUPT = "local_copy_corrupt";
    public static final String FAILED_TRANSFER = "transfer_failed";
    public static final String FAILED_BAD_COMMAND = "bad_command";

    // DELETED details (Change.detail).
    public static final String DELETED_DELETED = "deleted";
    public static final String DELETED_CORRUPT = "corrupt";
    public static final String DELETED_MISSING = "missing";

    // resync_wanted values (HeartbeatRequest).
    public static final String RESYNC_OVERFLOW = "overflow";
    public static final String RESYNC_TOMBSTONES = "tombstones";

    // Segment acknowledgement statuses (SegmentAck.status).
    public static final String SEGMENT_OK = "ok";
    public static final String SEGMENT_DONE = "done";
    public static final String SEGMENT_STALE = "stale";

    private InternalApi() {
    }

    /** Wall-clock time in seconds, the unit of {@code sent_at}. */
    public static double nowSeconds() {
        return System.currentTimeMillis() / 1000.0;
    }

    /** Controller versions are even and positive; worker observations are odd (v5 §3.2). */
    public static boolean isControllerVersion(long version) {
        return version > 0 && (version & 1) == 0;
    }
}
