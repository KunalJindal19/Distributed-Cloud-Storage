package com.objectstore.worker.api;

import com.objectstore.common.Checksums;
import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.ApiError;
import com.objectstore.common.dto.ChunkInfo;
import com.objectstore.common.dto.ChunkListResponse;
import com.objectstore.common.web.ApiException;
import com.objectstore.common.web.ErrorCodes;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.storage.ChunkStore;
import com.objectstore.worker.storage.ChunkStore.StoredChunk;
import com.objectstore.worker.storage.ChunkStore.WriteResult;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
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

/** The worker side of the internal API: the four chunk operations called by the controller. */
@RestController
public class ChunkController {

    private static final Logger log = LoggerFactory.getLogger(ChunkController.class);

    private final ChunkStore store;
    private final WorkerProperties properties;

    public ChunkController(ChunkStore store, WorkerProperties properties) {
        this.store = store;
        this.properties = properties;
    }

    /**
     * Stores a chunk. 201 when written, 200 when identical content was already there (so the
     * controller can safely retry), 400 on a bad id / missing or wrong checksum, 409 when the id
     * exists with different content, 413 when the body is larger than one chunk.
     */
    @PutMapping(InternalApi.CHUNK_PATH)
    public ResponseEntity<ChunkInfo> putChunk(
            @PathVariable String chunkId,
            @RequestHeader(value = InternalApi.CHECKSUM_HEADER, required = false) String checksumHeader,
            HttpServletRequest request) throws IOException {
        if (checksumHeader == null || checksumHeader.isBlank()) {
            throw ApiException.badRequest(InternalApi.CHECKSUM_HEADER + " header is required");
        }
        String checksum = checksumHeader.trim().toLowerCase(Locale.ROOT);
        if (!Checksums.isValidSha256Hex(checksum)) {
            throw ApiException.badRequest(InternalApi.CHECKSUM_HEADER + " must be 64 hex characters (SHA-256)");
        }
        int limit = properties.maxChunkSize();
        if (request.getContentLengthLong() > limit) {
            throw ApiException.payloadTooLarge("a chunk may be at most " + limit + " bytes");
        }
        // Read one byte past the limit so an oversized body without Content-Length is caught too.
        byte[] body = request.getInputStream().readNBytes(limit + 1);
        if (body.length > limit) {
            throw ApiException.payloadTooLarge("a chunk may be at most " + limit + " bytes");
        }

        WriteResult result = store.write(chunkId, body, checksum);
        log.info("[store] put chunk={} size={} ({})", chunkId, body.length, result.name().toLowerCase(Locale.ROOT));
        HttpStatus status = result == WriteResult.CREATED ? HttpStatus.CREATED : HttpStatus.OK;
        return ResponseEntity.status(status).body(new ChunkInfo(chunkId, body.length, checksum));
    }

    /** Returns the raw bytes; {@code X-Checksum} is the hash of exactly the bytes being sent. */
    @GetMapping(InternalApi.CHUNK_PATH)
    public ResponseEntity<byte[]> getChunk(@PathVariable String chunkId) {
        StoredChunk chunk = store.read(chunkId).orElseThrow(() -> chunkNotFound(chunkId));
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header(InternalApi.CHECKSUM_HEADER, chunk.checksum())
                .body(chunk.data());
    }

    @DeleteMapping(InternalApi.CHUNK_PATH)
    public ResponseEntity<Void> deleteChunk(@PathVariable String chunkId) {
        boolean removed = store.delete(chunkId);
        log.info("[store] delete chunk={} ({})", chunkId, removed ? "ok" : "missing");
        if (!removed) {
            throw chunkNotFound(chunkId);
        }
        return ResponseEntity.noContent().build();
    }

    /** Everything on this worker's disk, hashed from the files as they are now. */
    @GetMapping(InternalApi.WORKER_CHUNKS_PATH)
    public ChunkListResponse listChunks(@PathVariable String workerId) {
        if (!properties.id().equals(workerId)) {
            throw ApiException.notFound(ErrorCodes.UNKNOWN_WORKER,
                    "this is worker '" + properties.id() + "', not '" + workerId + "'");
        }
        return new ChunkListResponse(properties.id(), store.list());
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

    @ExceptionHandler(ChunkStore.ChunkConflictException.class)
    ResponseEntity<ApiError> onConflict(ChunkStore.ChunkConflictException ex) {
        return error(HttpStatus.CONFLICT, ErrorCodes.CONFLICT, ex.getMessage());
    }

    private static ResponseEntity<ApiError> error(HttpStatus status, String code, String detail) {
        return ResponseEntity.status(status).contentType(MediaType.APPLICATION_JSON).body(new ApiError(code, detail));
    }

    private static ApiException chunkNotFound(String chunkId) {
        return ApiException.notFound(ErrorCodes.CHUNK_NOT_FOUND, "chunk " + chunkId + " is not stored on this worker");
    }
}
