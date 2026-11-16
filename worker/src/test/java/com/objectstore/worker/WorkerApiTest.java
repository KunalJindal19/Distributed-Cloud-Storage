package com.objectstore.worker;

import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.Checksums;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** Drives the worker's internal API over real HTTP, exactly as the controller does. */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"worker.id=worker1", "worker.registration-enabled=false"})
class WorkerApiTest {

    private static final String OBJECT_ID = "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a";

    @TempDir
    static Path dataDir;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("worker.data-dir", () -> dataDir.toString());
    }

    @Value("${local.server.port}")
    int port;

    private final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

    @BeforeEach
    void emptyTheStore() throws IOException {
        try (Stream<Path> files = Files.list(dataDir.resolve("chunks"))) {
            for (Path file : files.toList()) {
                Files.delete(file);
            }
        }
    }

    @Test
    void healthIdentifiesTheWorker() throws Exception {
        HttpResponse<String> response = get("/health");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"status\":\"ok\"", "\"service\":\"worker\"", "\"worker_id\":\"worker1\"");
    }

    @Test
    void putStoresTheChunkAndIsIdempotent() throws Exception {
        byte[] data = bytes(1_048_576, 1);
        String checksum = Checksums.sha256Hex(data);
        String id = chunkId(0);

        HttpResponse<String> created = put(id, data, checksum);
        HttpResponse<String> repeated = put(id, data, checksum);

        assertThat(created.statusCode()).isEqualTo(201);
        assertThat(created.body()).contains("\"chunk_id\":\"" + id + "\"", "\"size\":1048576", "\"checksum\":\"" + checksum + "\"");
        assertThat(repeated.statusCode()).isEqualTo(200);
        assertThat(Files.readAllBytes(dataDir.resolve("chunks").resolve(id))).isEqualTo(data);
    }

    @Test
    void putWithAWrongChecksumIsRejectedAndWritesNothing() throws Exception {
        HttpResponse<String> response = put(chunkId(1), bytes(100, 1), "0".repeat(64));

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("\"error\":\"checksum_mismatch\"");
        try (Stream<Path> files = Files.list(dataDir.resolve("chunks"))) {
            assertThat(files).isEmpty();
        }
    }

    @Test
    void putWithoutChecksumHeaderIsABadRequest() throws Exception {
        HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(uri("/internal/chunks/" + chunkId(0)))
                        .PUT(HttpRequest.BodyPublishers.ofByteArray(bytes(10, 1)))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("\"error\":\"bad_request\"");
    }

    @Test
    void putOfDifferentContentToAnExistingIdIsAConflict() throws Exception {
        byte[] original = bytes(100, 1);
        byte[] other = bytes(100, 2);
        put(chunkId(0), original, Checksums.sha256Hex(original));

        HttpResponse<String> response = put(chunkId(0), other, Checksums.sha256Hex(other));

        assertThat(response.statusCode()).isEqualTo(409);
        assertThat(response.body()).contains("\"error\":\"conflict\"");
    }

    @Test
    void putLargerThanOneChunkIsRejected() throws Exception {
        byte[] tooBig = bytes(1_048_577, 1);

        HttpResponse<String> response = put(chunkId(0), tooBig, Checksums.sha256Hex(tooBig));

        assertThat(response.statusCode()).isEqualTo(413);
        assertThat(response.body()).contains("\"error\":\"payload_too_large\"");
    }

    @Test
    void putWithAnInvalidChunkIdIsRejected() throws Exception {
        byte[] data = bytes(10, 1);

        HttpResponse<String> response = put("test-00000", data, Checksums.sha256Hex(data));

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("\"error\":\"bad_request\"");
    }

    @Test
    void getReturnsBytesWithAChecksumComputedFromDisk() throws Exception {
        byte[] data = bytes(4096, 9);
        String id = chunkId(2);
        put(id, data, Checksums.sha256Hex(data));

        HttpResponse<byte[]> intact = getBytes("/internal/chunks/" + id);
        assertThat(intact.statusCode()).isEqualTo(200);
        assertThat(intact.body()).isEqualTo(data);
        assertThat(intact.headers().firstValue("X-Checksum")).contains(Checksums.sha256Hex(data));
        assertThat(intact.headers().firstValue("Content-Type")).contains("application/octet-stream");

        // Damage the file behind the worker's back: the header must describe the damaged bytes.
        byte[] damaged = data.clone();
        damaged[100] ^= 0x5A;
        Files.write(dataDir.resolve("chunks").resolve(id), damaged);

        HttpResponse<byte[]> afterDamage = getBytes("/internal/chunks/" + id);
        assertThat(afterDamage.headers().firstValue("X-Checksum")).contains(Checksums.sha256Hex(damaged));
    }

    @Test
    void getOfAnUnknownChunkIs404() throws Exception {
        HttpResponse<String> response = get("/internal/chunks/" + chunkId(7));

        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(response.body()).contains("\"error\":\"chunk_not_found\"");
    }

    @Test
    void deleteReturns204Then404() throws Exception {
        byte[] data = bytes(10, 1);
        put(chunkId(0), data, Checksums.sha256Hex(data));

        assertThat(delete(chunkId(0)).statusCode()).isEqualTo(204);
        HttpResponse<String> second = delete(chunkId(0));
        assertThat(second.statusCode()).isEqualTo(404);
        assertThat(second.body()).contains("\"error\":\"chunk_not_found\"");
    }

    @Test
    void listReportsEveryChunkAndOnlyForThisWorker() throws Exception {
        byte[] a = bytes(300, 1);
        byte[] b = bytes(700, 2);
        put(chunkId(1), b, Checksums.sha256Hex(b));
        put(chunkId(0), a, Checksums.sha256Hex(a));

        HttpResponse<String> mine = get("/internal/workers/worker1/chunks");
        assertThat(mine.statusCode()).isEqualTo(200);
        assertThat(mine.body()).isEqualTo(
                "{\"worker_id\":\"worker1\",\"chunks\":["
                        + "{\"chunk_id\":\"" + chunkId(0) + "\",\"size\":300,\"checksum\":\"" + Checksums.sha256Hex(a) + "\"},"
                        + "{\"chunk_id\":\"" + chunkId(1) + "\",\"size\":700,\"checksum\":\"" + Checksums.sha256Hex(b) + "\"}]}");

        HttpResponse<String> someoneElse = get("/internal/workers/worker9/chunks");
        assertThat(someoneElse.statusCode()).isEqualTo(404);
        assertThat(someoneElse.body()).contains("\"error\":\"unknown_worker\"");
    }

    @Test
    void unknownRoutesAnswerWithJsonNotHtml() throws Exception {
        HttpResponse<String> response = get("/nope");

        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(response.headers().firstValue("Content-Type").orElse("")).startsWith("application/json");
        assertThat(response.body()).isEqualTo("{\"error\":\"not_found\",\"detail\":\"route not found\"}");
    }

    private static String chunkId(int index) {
        return "%s-%05d".formatted(OBJECT_ID, index);
    }

    private static byte[] bytes(int length, int seed) {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = (byte) (i * 31 + seed);
        }
        return data;
    }

    private URI uri(String path) {
        return URI.create("http://localhost:" + port + path);
    }

    private HttpResponse<String> put(String chunkId, byte[] body, String checksum) throws Exception {
        return http.send(
                HttpRequest.newBuilder(uri("/internal/chunks/" + chunkId))
                        .header("X-Checksum", checksum)
                        .header("Content-Type", "application/octet-stream")
                        .PUT(HttpRequest.BodyPublishers.ofByteArray(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(uri(path)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<byte[]> getBytes(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(uri(path)).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    private HttpResponse<String> delete(String chunkId) throws Exception {
        return http.send(
                HttpRequest.newBuilder(uri("/internal/chunks/" + chunkId)).DELETE().build(),
                HttpResponse.BodyHandlers.ofString());
    }
}
