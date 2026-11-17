package com.objectstore.controller.api;

import com.objectstore.common.web.ApiException;
import com.objectstore.controller.api.dto.ObjectListResponse;
import com.objectstore.controller.api.dto.ObjectResponse;
import com.objectstore.controller.domain.Chunk;
import com.objectstore.controller.domain.StoredObject;
import com.objectstore.controller.objects.DownloadService;
import com.objectstore.controller.objects.DownloadService.DownloadPlan;
import com.objectstore.controller.objects.ObjectService;
import com.objectstore.controller.objects.UploadService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

/**
 * The client-facing REST API: the six endpoints under {@code /api/v1/objects}.
 *
 * <p>Structured data is JSON. Object data travels as binary: multipart/form-data on the way in,
 * the object's own content type on the way out. Clients never learn or need worker addresses.
 */
@RestController
@RequestMapping("/api/v1/objects")
public class ObjectsController {

    private static final Logger log = LoggerFactory.getLogger(ObjectsController.class);
    public static final String OBJECT_ID_HEADER = "X-Object-Id";
    public static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";
    public static final String IDEMPOTENT_REPLAY_HEADER = "Idempotent-Replay";

    private final UploadService uploads;
    private final DownloadService downloads;
    private final ObjectService objects;

    public ObjectsController(UploadService uploads, DownloadService downloads, ObjectService objects) {
        this.uploads = uploads;
        this.downloads = downloads;
        this.objects = objects;
    }

    /**
     * Upload an object. multipart/form-data with a required {@code file} part and an optional
     * {@code metadata} field holding a JSON object of custom key/value pairs.
     *
     * <p>An optional {@code Idempotency-Key} header makes retries safe: the same key again
     * returns the original object with 200 and {@code Idempotent-Replay: true}; the same key with
     * a file of a different size is 409. Multipart parsing is lazy: safe mode and the number of
     * writable workers are checked before the request body is read.
     */
    @PostMapping
    public ResponseEntity<ObjectResponse> upload(HttpServletRequest request) {
        String key = UploadService.validateIdempotencyKey(request.getHeader(IDEMPOTENCY_KEY_HEADER));
        // A retry of an upload that already succeeded is answered from the namespace, even in safe mode.
        Optional<UploadService.UploadResult> replay = uploads.replay(key, () -> filePart(request).getSize());
        if (replay.isPresent()) {
            return respond(replay.get());
        }
        uploads.checkWritable();
        MultipartFile file = filePart(request);
        String metadata = ((MultipartHttpServletRequest) request).getParameter("metadata");
        return respond(uploads.upload(file, metadata, key));
    }

    private static MultipartFile filePart(HttpServletRequest request) {
        if (!(request instanceof MultipartHttpServletRequest multipart)) {
            throw ApiException.badRequest("expected a multipart/form-data request with a 'file' part");
        }
        MultipartFile file = multipart.getFile("file");
        if (file == null) {
            throw ApiException.badRequest("the multipart request has no 'file' part");
        }
        return file;
    }

    private static ResponseEntity<ObjectResponse> respond(UploadService.UploadResult result) {
        ObjectResponse object = result.object();
        if (result.replay()) {
            return ResponseEntity.ok().header(IDEMPOTENT_REPLAY_HEADER, "true").body(object);
        }
        return ResponseEntity.created(URI.create("/api/v1/objects/" + object.objectId())).body(object);
    }

    /** List stored objects, newest first. {@code limit} 1-500 (default 50), {@code offset} &ge; 0. */
    @GetMapping
    public ObjectListResponse list(
            @RequestParam(defaultValue = "" + ObjectService.DEFAULT_PAGE_SIZE) int limit,
            @RequestParam(defaultValue = "0") long offset) {
        return objects.list(limit, offset);
    }

    /**
     * Download an object: the complete original bytes with the content type it was uploaded with.
     * Chunks are fetched, verified and written one at a time. Everything that can fail cleanly
     * is checked before the response is committed, so those cases are proper JSON errors.
     */
    @GetMapping("/{objectId}")
    public void download(@PathVariable String objectId, HttpServletResponse response) throws IOException {
        long startedNanos = System.nanoTime();
        DownloadPlan plan = downloads.plan(objectId);
        StoredObject object = plan.object();
        List<Chunk> chunks = plan.chunks();

        // Fetch the first chunk before any header is set: a failure here is still a clean 503.
        byte[] first = chunks.isEmpty() ? null : downloads.fetchVerified(chunks.get(0));

        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType(object.contentType());
        response.setContentLengthLong(object.size());
        response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
                "attachment; filename=\"" + asciiFileName(object.name()) + "\"");
        response.setHeader(HttpHeaders.ETAG, "\"" + object.checksum() + "\"");
        response.setHeader(OBJECT_ID_HEADER, object.objectId());

        OutputStream out = response.getOutputStream();
        if (first != null) {
            out.write(first);
        }
        for (int i = 1; i < chunks.size(); i++) {
            byte[] data;
            try {
                data = downloads.fetchVerified(chunks.get(i));
            } catch (ApiException e) {
                // Headers and earlier chunks are on the wire. Dropping the connection makes the
                // client see fewer bytes than Content-Length, never a silently truncated success.
                log.error("[download] aborted object={} chunk={}", objectId, chunks.get(i).chunkId());
                throw e;
            }
            out.write(data);
        }
        out.flush();
        log.info("[download] object={} size={} chunks={} ms={}", objectId, object.size(), chunks.size(),
                (System.nanoTime() - startedNanos) / 1_000_000);
    }

    /** System-maintained and custom metadata, without downloading the object's data. */
    @GetMapping("/{objectId}/metadata")
    public ObjectResponse getMetadata(@PathVariable String objectId) {
        return objects.getMetadata(objectId);
    }

    /** Add or update custom metadata; a {@code null} value removes a key. The data is not touched. */
    @PatchMapping("/{objectId}/metadata")
    public ObjectResponse patchMetadata(@PathVariable String objectId, @RequestBody(required = false) byte[] body) {
        return objects.patchMetadata(objectId, body);
    }

    /** Delete an object and its stored data. 204 on success. */
    @DeleteMapping("/{objectId}")
    public ResponseEntity<Void> delete(@PathVariable String objectId) {
        objects.delete(objectId);
        return ResponseEntity.noContent().build();
    }

    /** Keeps {@code A-Z a-z 0-9 . _ -}; everything else (quotes, spaces, non-ASCII) becomes {@code _}. */
    static String asciiFileName(String name) {
        String safe = name.replaceAll("[^A-Za-z0-9._-]", "_");
        return safe.isEmpty() ? "download" : safe;
    }
}
