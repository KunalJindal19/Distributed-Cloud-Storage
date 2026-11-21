package com.objectstore.common.web;

/** The closed set of machine-readable values used in the {@code error} field of an error body. */
public final class ErrorCodes {

    public static final String BAD_REQUEST = "bad_request";
    public static final String INVALID_METADATA = "invalid_metadata";
    public static final String CHECKSUM_MISMATCH = "checksum_mismatch";
    public static final String NOT_FOUND = "not_found";
    public static final String OBJECT_NOT_FOUND = "object_not_found";
    public static final String CHUNK_NOT_FOUND = "chunk_not_found";
    public static final String UNKNOWN_WORKER = "unknown_worker";
    public static final String METHOD_NOT_ALLOWED = "method_not_allowed";
    public static final String NOT_ACCEPTABLE = "not_acceptable";
    public static final String CONFLICT = "conflict";
    public static final String PAYLOAD_TOO_LARGE = "payload_too_large";
    public static final String UNSUPPORTED_MEDIA_TYPE = "unsupported_media_type";
    public static final String INTERNAL_ERROR = "internal_error";
    public static final String INSUFFICIENT_WORKERS = "insufficient_workers";
    public static final String UPLOAD_FAILED = "upload_failed";
    public static final String CHUNK_UNAVAILABLE = "chunk_unavailable";
    public static final String SERVICE_UNAVAILABLE = "service_unavailable";
    public static final String SAFE_MODE = "safe_mode";
    public static final String INSUFFICIENT_STORAGE = "insufficient_storage";
    public static final String CHUNK_CORRUPT = "chunk_corrupt";
    /** A chunk PUT or DELETE whose version is not newer than the worker's record or fence (v5 §4.2). */
    public static final String STALE_VERSION = "stale_version";
    /** The same (chunk, version) is being received already. */
    public static final String IN_PROGRESS = "in_progress";

    // Controller answers to worker messages (see the protocol acceptance rules).
    public static final String STALE_BOOT = "stale_boot";
    public static final String TOO_OLD = "too_old";
    public static final String STALE_SEQ = "stale_seq";
    public static final String CLUSTER_ID_MISMATCH = "cluster_id_mismatch";
    public static final String DUPLICATE_WORKER = "duplicate_worker";
    /** A registration attempt older than one already accepted from the same process. */
    public static final String STALE_REGISTRATION = "stale_registration";

    private ErrorCodes() {
    }

    /** The generic code for a status that was not raised through an {@link ApiException}. */
    public static String forStatus(int status) {
        return switch (status) {
            case 404 -> NOT_FOUND;
            case 405 -> METHOD_NOT_ALLOWED;
            case 406 -> NOT_ACCEPTABLE;
            case 409 -> CONFLICT;
            case 413 -> PAYLOAD_TOO_LARGE;
            case 415 -> UNSUPPORTED_MEDIA_TYPE;
            case 507 -> INSUFFICIENT_STORAGE;
            case 503 -> SERVICE_UNAVAILABLE;
            default -> status >= 500 ? INTERNAL_ERROR : BAD_REQUEST;
        };
    }
}
