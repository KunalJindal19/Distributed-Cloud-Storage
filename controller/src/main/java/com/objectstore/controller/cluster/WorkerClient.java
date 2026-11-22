package com.objectstore.controller.cluster;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.ChunkInfo;
import com.objectstore.common.dto.ChunkListResponse;
import com.objectstore.common.dto.ChunkPutResponse;
import com.objectstore.common.http.FaultSettings;
import com.objectstore.common.http.HttpClients;
import com.objectstore.controller.config.ControllerProperties;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * The controller's direct HTTP calls to workers: writing chunks during an upload, reading them
 * for a download, and listing a worker's disk for fsck. Repairs and deletions do <em>not</em> go
 * through here: workers copy and delete chunks on command.
 */
@Component
public class WorkerClient {

    /** Bytes of a chunk together with the checksum the worker computed from its sidecar. */
    public record FetchedChunk(byte[] data, String checksum) {
    }

    private final RestClient chunks;
    private final RestClient listing;

    public WorkerClient(RestClient.Builder builder, ControllerProperties properties) {
        FaultSettings faults = FaultSettings.fromEnvironment();
        Duration connect = properties.connectTimeout();
        Duration read = properties.readTimeout();
        this.chunks = HttpClients.restClient(builder, connect, read, faults);
        // Listing makes the worker hash every file it holds, which takes longer than one chunk call.
        Duration listingTimeout = read.multipliedBy(6).compareTo(Duration.ofMinutes(1)) > 0
                ? read.multipliedBy(6) : Duration.ofMinutes(1);
        this.listing = HttpClients.restClient(builder, connect, listingTimeout, faults);
    }

    /**
     * {@code PUT /internal/chunks/{chunk_id}} at operation {@code version} ({@code X-Op-Version}).
     * 200 (already there, or a repeat) and 201 (written) both succeed; anything else (409
     * {@code stale_version} / {@code in_progress}, 503, 500, ...) is a failed target.
     * {@code changeAck} ({@code <boot_id>:<received>}, may be null) goes in {@code X-Change-Ack}; the
     * answer may carry the worker's next changes.
     */
    public ChunkPutResponse putChunk(String workerUrl, String chunkId, byte[] data, String checksum, long version,
                                     String changeAck) {
        return call(workerUrl, () -> {
            RestClient.RequestBodySpec spec = chunks.put()
                    .uri(workerUrl + InternalApi.CHUNK_PATH, chunkId)
                    .header(InternalApi.CHECKSUM_HEADER, checksum)
                    .header(InternalApi.OP_VERSION_HEADER, Long.toString(version))
                    .contentType(MediaType.APPLICATION_OCTET_STREAM);
            if (changeAck != null) {
                spec = spec.header(InternalApi.CHANGE_ACK_HEADER, changeAck);
            }
            return spec.body(data).exchange((request, response) -> {
                int status = response.getStatusCode().value();
                if (status != 200 && status != 201) {
                    throw failure(workerUrl, response);
                }
                try {
                    return response.bodyTo(ChunkPutResponse.class);
                } catch (RuntimeException unreadable) {
                    return null;                      // the chunk is stored; only the piggyback is lost
                }
            });
        });
    }

    /**
     * {@code DELETE /internal/chunks/{chunk_id}} at operation {@code version}, with the command's id and
     * reason. Returns the HTTP status the worker answered with (204 deleted or a repeat, 409 stale,
     * 400, 500, 503, ...); throws {@link WorkerException} without a status when no answer came.
     */
    public int deleteChunk(String workerUrl, String chunkId, long version, String cmdId, String reason) {
        return call(workerUrl, () -> {
            RestClient.RequestHeadersSpec<?> spec = chunks.delete()
                    .uri(workerUrl + InternalApi.CHUNK_PATH, chunkId)
                    .header(InternalApi.OP_VERSION_HEADER, Long.toString(version));
            if (cmdId != null) {
                spec = spec.header(InternalApi.CMD_ID_HEADER, cmdId);
            }
            if (reason != null) {
                spec = spec.header(InternalApi.REASON_HEADER, reason);
            }
            return spec.exchange((request, response) -> response.getStatusCode().value());
        });
    }

    /** {@code GET /internal/chunks/{chunk_id}}. The caller classifies and verifies the result. */
    public FetchedChunk getChunk(String workerUrl, String chunkId) {
        return call(workerUrl, () -> chunks.get()
                .uri(workerUrl + InternalApi.CHUNK_PATH, chunkId)
                .accept(MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON)
                .exchange((request, response) -> {
                    if (response.getStatusCode().value() != 200) {
                        throw failure(workerUrl, response);
                    }
                    String checksum = response.getHeaders().getFirst(InternalApi.CHECKSUM_HEADER);
                    return new FetchedChunk(response.getBody().readAllBytes(), checksum);
                }));
    }

    /** {@code GET /internal/workers/{worker_id}/chunks}: the disk as it is now, hashes recomputed. */
    public Map<String, ChunkInfo> listChunks(String workerUrl, String workerId) {
        ChunkListResponse response = call(workerUrl, () -> listing.get()
                .uri(workerUrl + InternalApi.WORKER_CHUNKS_PATH, workerId)
                .accept(MediaType.APPLICATION_JSON)
                .exchange((request, httpResponse) -> {
                    if (httpResponse.getStatusCode().value() != 200) {
                        throw failure(workerUrl, httpResponse);
                    }
                    return httpResponse.bodyTo(ChunkListResponse.class);
                }));
        Map<String, ChunkInfo> byId = new LinkedHashMap<>();
        if (response != null && response.chunks() != null) {
            for (ChunkInfo chunk : response.chunks()) {
                byId.put(chunk.chunkId(), chunk);
            }
        }
        return byId;
    }

    private static <T> T call(String workerUrl, Supplier<T> request) {
        try {
            return request.get();
        } catch (WorkerException e) {
            throw e;
        } catch (RestClientException e) {
            throw new WorkerException(workerUrl, null, describe(e), e);
        }
    }

    private static WorkerException failure(String workerUrl, ClientHttpResponse response) throws IOException {
        int status = response.getStatusCode().value();
        String body = new String(response.getBody().readNBytes(2048), StandardCharsets.UTF_8).strip();
        return new WorkerException(workerUrl, status, "HTTP " + status + (body.isEmpty() ? "" : " " + body), null);
    }

    private static String describe(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        return root.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }
}
