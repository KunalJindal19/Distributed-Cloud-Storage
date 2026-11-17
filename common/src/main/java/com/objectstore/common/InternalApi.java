package com.objectstore.common;

/** Names shared by both ends of the controller &harr; worker protocol (the HDFS NameNode/DataNode protocol). */
public final class InternalApi {

    /** Carries the SHA-256 of a chunk body, on PUT requests and GET responses. */
    public static final String CHECKSUM_HEADER = "X-Checksum";

    // Worker -> controller (DatanodeProtocol)
    public static final String REGISTER_PATH = "/internal/workers/register";
    public static final String HEARTBEAT_PATH = "/internal/workers/heartbeat";
    public static final String BLOCK_REPORT_PATH = "/internal/workers/blockreport";
    public static final String IBR_PATH = "/internal/workers/ibr";

    // Controller -> worker and worker -> worker (DataTransferProtocol)
    public static final String CHUNK_PATH = "/internal/chunks/{chunkId}";
    public static final String WORKER_CHUNKS_PATH = "/internal/workers/{workerId}/chunks";

    /** Worker ids double as Docker host names, so keep them to a conservative alphabet. */
    public static final String WORKER_ID_PATTERN = "^[A-Za-z0-9][A-Za-z0-9_.-]{0,31}$";

    public static final String COMMAND_REPLICATE = "REPLICATE";
    public static final String COMMAND_DELETE = "DELETE";

    private InternalApi() {
    }

    /** Wall-clock time in seconds, the unit of {@code sent_at} and {@code expires_at}. */
    public static double nowSeconds() {
        return System.currentTimeMillis() / 1000.0;
    }
}
