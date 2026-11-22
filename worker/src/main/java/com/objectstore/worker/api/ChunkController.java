package com.objectstore.worker.api;

import com.objectstore.common.Checksums;
import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.ApiError;
import com.objectstore.common.dto.ChangeBatch;
import com.objectstore.common.dto.ChunkListResponse;
import com.objectstore.common.dto.ChunkPutResponse;
import com.objectstore.common.web.ApiException;
import com.objectstore.common.web.ErrorCodes;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.reporting.ChangeLog;
import com.objectstore.worker.reporting.WorkerIdentity;
import com.objectstore.worker.storage.ChunkStore;
import com.objectstore.worker.storage.ChunkStore.DeleteOutcome;
import com.objectstore.worker.storage.ChunkStore.PutOutcome;
import com.objectstore.worker.storage.ChunkStore.StoredChunk;
import jakarta.servlet.http.HttpServletRequest;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/**
 * The worker side of the internal data path: store a chunk (the controller's upload PUT and the
 * PUT of a transfer from another worker), retrieve a chunk, delete a chunk (the controller's direct
 * DELETE when a client deletes an object), and list the disk for fsck. PUT and DELETE are both
 * versioned: they go through the same version check as the commands in heartbeat replies.
 */
@RestController
public class ChunkController {

    private static final Logger log = LoggerFactory.getLogger(ChunkController.class);

    private final ChunkStore store;
    private final WorkerProperties properties;
    private final ChangeLog changes;
    private final WorkerIdentity identity;

    public ChunkController(ChunkStore store, WorkerProperties properties, ChangeLog changes, WorkerIdentity identity) {
        this.store = store;
        this.properties = properties;
        this.changes = changes;
        this.identity = identity;
    }

    /**
     * Installs a copy at the version in {@code X-Op-Version} (v5 §4.3). 201 when written, 200 when
     * the same bytes were already here (no write) or the PUT repeats the current version, 400 on a
     * bad id, a missing or wrong checksum or version, 409 {@code stale_version} when the version is
     * not newer than the record or the fence, 409 {@code in_progress} when the same (chunk, version)
     * is being received already, 413 when the body is larger than one chunk, 503 while shutting down.
     *
     * <p>A PUT from the controller carries {@code X-Change-Ack: <boot_id>:<n>}: the change log
     * forgets everything up to {@code n} and the answer carries the next changes, so the controller
     * learns about the copy at once.
     */
    @PutMapping(InternalApi.CHUNK_PATH)
    public ResponseEntity<ChunkPutResponse> putChunk(
            @PathVariable String chunkId,
            @RequestHeader(value = InternalApi.CHECKSUM_HEADER, required = false) String checksumHeader,
            @RequestHeader(value = InternalApi.OP_VERSION_HEADER, required = false) String versionHeader,
            @RequestHeader(value = InternalApi.CHANGE_ACK_HEADER, required = false) String changeAck,
            HttpServletRequest request) {
        if (checksumHeader == null || checksumHeader.isBlank()) {
            throw ApiException.badRequest(InternalApi.CHECKSUM_HEADER + " header is required");
        }
        String checksum = checksumHeader.trim().toLowerCase(Locale.ROOT);
        if (!Checksums.isValidSha256Hex(checksum)) {
            throw ApiException.badRequest(InternalApi.CHECKSUM_HEADER + " must be 64 hex characters (SHA-256)");
        }
        long version = parseVersion(versionHeader);
        if (store.isClosing()) {
            throw ApiException.unavailable(ErrorCodes.SERVICE_UNAVAILABLE, "this worker is shutting down");
        }
        int limit = properties.maxChunkSize();
        if (request.getContentLengthLong() > limit) {
            throw ApiException.payloadTooLarge("a chunk may be at most " + limit + " bytes");
        }
        if (!store.beginIncoming(chunkId, version)) {
            throw ApiException.conflict(ErrorCodes.IN_PROGRESS, "chunk " + chunkId + " version " + version
                    + " is being received already");
        }
        try {
            byte[] body = readBody(request, limit);
            PutOutcome outcome = store.put(chunkId, version, body, checksum);
            log.info("[store] put chunk={} v={} size={} ({})", chunkId, version, body.length,
                    outcome.name().toLowerCase(Locale.ROOT));
            if (outcome == PutOutcome.STALE) {
                throw ApiException.conflict(ErrorCodes.STALE_VERSION, "version " + version + " of chunk " + chunkId
                        + " is not newer than this worker's record or fence");
            }
            HttpStatus status = outcome == PutOutcome.CREATED ? HttpStatus.CREATED : HttpStatus.OK;
            return ResponseEntity.status(status).body(new ChunkPutResponse(chunkId, body.length, checksum,
                    piggyback(changeAck)));
        } finally {
            store.endIncoming(chunkId, version);
        }
    }

    /**
     * Deletes the copy as of the version in {@code X-Op-Version}: exactly the versioned delete the
     * DELETE command runs ({@link ChunkStore#delete}), so a delayed or repeated request can never
     * delete a newer copy. 204 when deleted, or when this repeats the delete at the current version;
     * 409 {@code stale_version} when the version is not newer than the copy or the fence (nothing
     * changes); 400 without a valid version or chunk id; 500 when a disk error stopped it before
     * anything was removed; 503 while shutting down or after a fail-stop. {@code X-Reason: corrupt}
     * quarantines the bytes instead of deleting them. Every result also goes to the change stream
     * ({@code DELETED(v)} or {@code CMD_FAILED}, with {@code X-Cmd-Id}), exactly as for the command,
     * so the controller learns it the same way whichever path delivered the delete.
     */
    @DeleteMapping(InternalApi.CHUNK_PATH)
    public ResponseEntity<Void> deleteChunk(
            @PathVariable String chunkId,
            @RequestHeader(value = InternalApi.OP_VERSION_HEADER, required = false) String versionHeader,
            @RequestHeader(value = InternalApi.CMD_ID_HEADER, required = false) String cmdId,
            @RequestHeader(value = InternalApi.REASON_HEADER, required = false) String reason) {
        long version = parseVersion(versionHeader);
        boolean corrupt = reason != null && InternalApi.REASON_CORRUPT.equals(reason.trim());
        DeleteOutcome outcome = store.delete(chunkId, version, corrupt, cmdId);
        log.info("[store] delete chunk={} v={} reason={} via DELETE endpoint ({})", chunkId, version,
                reason == null ? "-" : reason.trim(), outcome.name().toLowerCase(Locale.ROOT));
        return switch (outcome) {
            case DELETED, REPEAT -> ResponseEntity.noContent().build();
            case STALE -> throw ApiException.conflict(ErrorCodes.STALE_VERSION, "version " + version + " of chunk "
                    + chunkId + " is not newer than this worker's record or fence");
            case IO_ERROR -> throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, ErrorCodes.INTERNAL_ERROR,
                    "a disk error stopped the delete of " + chunkId + " before anything was removed", null);
            case REFUSED -> throw ApiException.unavailable(ErrorCodes.SERVICE_UNAVAILABLE,
                    "this worker is shutting down or has stopped after a disk error");
        };
    }

    static long parseVersion(String header) {
        if (header == null || header.isBlank()) {
            throw ApiException.badRequest(InternalApi.OP_VERSION_HEADER + " header is required");
        }
        long version;
        try {
            version = Long.parseLong(header.trim());
        } catch (NumberFormatException e) {
            throw ApiException.badRequest(InternalApi.OP_VERSION_HEADER + " must be a number");
        }
        if (!InternalApi.isControllerVersion(version)) {
            throw ApiException.badRequest(InternalApi.OP_VERSION_HEADER + " must be a positive even controller version");
        }
        return version;
    }

    /**
     * Reads at most one chunk, bounded by SERVER_READ_TIMEOUT: every read blocks at most the
     * socket timeout (the server's connection timeout is set to the same value) and the loop gives up
     * once the deadline has passed.
     */
    private byte[] readBody(HttpServletRequest request, int limit) {
        long deadline = System.nanoTime() + properties.serverReadTimeout().toNanos();
        ByteArrayOutputStream body = new ByteArrayOutputStream(Math.min(limit, Math.max(0,
                (int) Math.min(Integer.MAX_VALUE, request.getContentLengthLong()))));
        byte[] buffer = new byte[64 * 1024];
        try (InputStream in = request.getInputStream()) {
            int read;
            while ((read = in.read(buffer)) != -1) {
                body.write(buffer, 0, read);
                if (body.size() > limit) {
                    throw ApiException.payloadTooLarge("a chunk may be at most " + limit + " bytes");
                }
                if (System.nanoTime() > deadline) {
                    throw ApiException.badRequest("the chunk body did not arrive within "
                            + properties.serverReadTimeoutSeconds() + " s");
                }
            }
        } catch (IOException e) {
            throw ApiException.badRequest("the chunk body could not be read: " + e.getClass().getSimpleName());
        }
        return body.toByteArray();
    }

    /** The changes for a PUT answer, or null if the header is absent, malformed or from another boot. */
    private ChangeBatch piggyback(String changeAck) {
        if (changeAck == null) {
            return null;
        }
        int colon = changeAck.lastIndexOf(':');
        if (colon <= 0 || !changeAck.substring(0, colon).trim().equals(identity.bootId())) {
            return null;
        }
        long received;
        try {
            received = Long.parseLong(changeAck.substring(colon + 1).trim());
        } catch (NumberFormatException e) {
            return null;
        }
        changes.ack(received);
        return new ChangeBatch(identity.bootId(), changes.suffix(received, properties.maxChangesPerPutAnswer()));
    }

    /**
     * Returns the bytes after verifying them against the sidecar; {@code X-Checksum} is their hash.
     * A copy that fails is quarantined and never served: 409 {@code chunk_corrupt}.
     */
    @GetMapping(InternalApi.CHUNK_PATH)
    public ResponseEntity<byte[]> getChunk(@PathVariable String chunkId) {
        StoredChunk chunk = store.read(chunkId).orElseThrow(() -> ApiException.notFound(ErrorCodes.CHUNK_NOT_FOUND,
                "chunk " + chunkId + " is not stored on this worker"));
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header(InternalApi.CHECKSUM_HEADER, chunk.checksum())
                .body(chunk.data());
    }

    /** Everything on this worker's disk, with checksums recomputed from the files right now (used by fsck). */
    @GetMapping(InternalApi.WORKER_CHUNKS_PATH)
    public ChunkListResponse listChunks(@PathVariable String workerId) {
        if (!properties.id().equals(workerId)) {
            throw ApiException.notFound(ErrorCodes.UNKNOWN_WORKER,
                    "this is worker '" + properties.id() + "', not '" + workerId + "'");
        }
        return new ChunkListResponse(properties.id(), store.listVerified());
    }

    // The store reports problems with its own exception types; translate them to the wire contract.

    @ExceptionHandler(ChunkStore.InvalidChunkIdException.class)
    ResponseEntity<ApiError> onInvalidChunkId(ChunkStore.InvalidChunkIdException ex) {
        return error(HttpStatus.BAD_REQUEST, ErrorCodes.BAD_REQUEST, ex.getMessage());
    }

    @ExceptionHandler(ChunkStore.ChecksumMismatchException.class)
    ResponseEntity<ApiError> onChecksumMismatch(ChunkStore.ChecksumMismatchException ex) {
        return error(HttpStatus.BAD_REQUEST, ErrorCodes.CHECKSUM_MISMATCH, ex.getMessage());
    }

    @ExceptionHandler(ChunkStore.CorruptChunkException.class)
    ResponseEntity<ApiError> onCorrupt(ChunkStore.CorruptChunkException ex) {
        log.warn("[store] {}; refusing to serve it", ex.getMessage());
        return error(HttpStatus.CONFLICT, ErrorCodes.CHUNK_CORRUPT, ex.getMessage());
    }

    @ExceptionHandler(ChunkStore.UnavailableException.class)
    ResponseEntity<ApiError> onUnavailable(ChunkStore.UnavailableException ex) {
        return error(HttpStatus.SERVICE_UNAVAILABLE, ErrorCodes.SERVICE_UNAVAILABLE, ex.getMessage());
    }

    @ExceptionHandler(ChunkStore.StoreFailedException.class)
    ResponseEntity<ApiError> onStoreFailed(ChunkStore.StoreFailedException ex) {
        log.error("[store] {}: {}", ex.getMessage(), ex.getCause() == null ? "-" : ex.getCause().toString());
        return error(HttpStatus.INTERNAL_SERVER_ERROR, ErrorCodes.INTERNAL_ERROR, ex.getMessage());
    }

    @ExceptionHandler(ChunkStore.InsufficientStorageException.class)
    ResponseEntity<ApiError> onInsufficientStorage(ChunkStore.InsufficientStorageException ex) {
        return error(HttpStatus.INSUFFICIENT_STORAGE, ErrorCodes.INSUFFICIENT_STORAGE, ex.getMessage());
    }

    private static ResponseEntity<ApiError> error(HttpStatus status, String code, String detail) {
        return ResponseEntity.status(status).contentType(MediaType.APPLICATION_JSON).body(new ApiError(code, detail));
    }
}
