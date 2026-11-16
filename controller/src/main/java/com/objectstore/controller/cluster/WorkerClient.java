package com.objectstore.controller.cluster;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.ChunkInfo;
import com.objectstore.common.dto.ChunkListResponse;
import com.objectstore.controller.config.ControllerProperties;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * The controller side of the internal API: every HTTP call to a worker goes through here.
 *
 * <p>All four operations are idempotent by design of the protocol (a repeated PUT of the same
 * content is a 200, a repeated DELETE is a 404), so a call that fails because a pooled connection
 * turned out to be stale is transparently tried once more. Time-outs are not retried.
 */
@Component
public class WorkerClient {

    /** Bytes of a chunk together with the checksum the worker computed while reading it. */
    public record FetchedChunk(byte[] data, String checksum) {
    }

    private final RestClient chunks;
    private final RestClient listing;

    public WorkerClient(RestClient.Builder builder, ControllerProperties properties) {
        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(2))
                .build();
        Duration timeout = properties.workerTimeout();
        this.chunks = builder.clone().requestFactory(requestFactory(httpClient, timeout)).build();
        // Listing makes the worker hash every file it holds, which takes longer than one chunk call.
        Duration listingTimeout = timeout.multipliedBy(6).compareTo(Duration.ofMinutes(1)) > 0
                ? timeout.multipliedBy(6)
                : Duration.ofMinutes(1);
        this.listing = builder.clone().requestFactory(requestFactory(httpClient, listingTimeout)).build();
    }

    private static JdkClientHttpRequestFactory requestFactory(HttpClient httpClient, Duration readTimeout) {
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
        factory.setReadTimeout(readTimeout);
        return factory;
    }

    /** {@code PUT /internal/chunks/{chunk_id}}. 200 (already there) and 201 (written) both succeed. */
    public void putChunk(String workerUrl, String chunkId, byte[] data, String checksum) {
        call(workerUrl, () -> chunks.put()
                .uri(workerUrl + InternalApi.CHUNK_PATH, chunkId)
                .header(InternalApi.CHECKSUM_HEADER, checksum)
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .body(data)
                .exchange((request, response) -> {
                    int status = response.getStatusCode().value();
                    if (status != 200 && status != 201) {
                        throw failure(workerUrl, response);
                    }
                    return Boolean.TRUE;
                }));
    }

    /** {@code GET /internal/chunks/{chunk_id}}. The caller is responsible for verifying the bytes. */
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

    /**
     * {@code DELETE /internal/chunks/{chunk_id}}.
     *
     * @return {@code true} if the worker removed a file, {@code false} if it had none
     */
    public boolean deleteChunk(String workerUrl, String chunkId) {
        return call(workerUrl, () -> chunks.delete()
                .uri(workerUrl + InternalApi.CHUNK_PATH, chunkId)
                .exchange((request, response) -> {
                    int status = response.getStatusCode().value();
                    if (status == 204) {
                        return Boolean.TRUE;
                    }
                    if (status == 404) {
                        return Boolean.FALSE;
                    }
                    throw failure(workerUrl, response);
                }));
    }

    /**
     * {@code GET /internal/workers/{worker_id}/chunks}: what is really on the worker's disk.
     *
     * @return chunk id &rarr; size and checksum as computed by the worker from the file
     */
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
            try {
                return request.get();
            } catch (ResourceAccessException e) {
                if (isTimeout(e)) {
                    throw e;
                }
                return request.get();
            }
        } catch (WorkerException e) {
            throw e;
        } catch (RestClientException e) {
            throw new WorkerException(workerUrl, null, describe(e), e);
        }
    }

    private static boolean isTimeout(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof HttpTimeoutException || t instanceof java.net.SocketTimeoutException) {
                return true;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return false;
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
