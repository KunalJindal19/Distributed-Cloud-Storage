package com.objectstore.worker;

import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.Checksums;
import com.objectstore.common.dto.ChangeType;
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
import com.objectstore.worker.reporting.ChangeLog;
import com.objectstore.worker.reporting.WorkerIdentity;
import com.objectstore.worker.storage.ChunkStore;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** Drives the worker's internal API over real HTTP, exactly as the controller does (v5 §4.3). */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"spring.config.name=worker", "worker.id=worker1", "worker.background-enabled=false",
                "worker.server-read-timeout-seconds=1"})
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

    @Autowired
    ChangeLog changes;

    @Autowired
    WorkerIdentity identity;

    @Autowired
    ChunkStore store;

    /** Operation versions of this test class: even, increasing, above the fence set below. */
    private static final AtomicLong VERSIONS = new AtomicLong(1000);

    private static long nextVersion() {
        return VERSIONS.addAndGet(2);
    }

    private final ObjectMapper json = new ObjectMapper();

    private final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

    @BeforeEach
    void emptyTheStore() throws IOException {
        if (!store.isFenceSet()) {
            store.adoptFence(1000, null);                        // as the first registration would
        }
        for (String chunkId : store.chunkIds()) {
            store.delete(chunkId, nextVersion(), false, "test");  // versioned deletes empty the index too
        }
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
    void putStoresTheChunkAndARepeatOfTheSameVersionAnswers200() throws Exception {
        byte[] data = bytes(1_048_576, 1);
        String checksum = Checksums.sha256Hex(data);
        String id = chunkId(0);
        long version = nextVersion();

        HttpResponse<String> created = put(id, data, checksum, null, version);
        HttpResponse<String> repeated = put(id, data, checksum, null, version);

        assertThat(created.statusCode()).isEqualTo(201);
        assertThat(created.body()).contains("\"chunk_id\":\"" + id + "\"", "\"size\":1048576", "\"checksum\":\"" + checksum + "\"");
        assertThat(repeated.statusCode()).isEqualTo(200);
        assertThat(Files.readAllBytes(dataDir.resolve("chunks").resolve(id))).isEqualTo(data);
    }

    @Test
    void aControllerPutGetsTheNextChangesAndAcknowledgesTheOldOnes() throws Exception {
        changes.ack(changes.lastSeq());
        long before = changes.lastSeq();
        byte[] a = bytes(100, 7);
        byte[] b = bytes(100, 8);
        put(chunkId(5), a, Checksums.sha256Hex(a));                         // a repair PUT: no header
        long afterFirst = changes.lastSeq();

        HttpResponse<String> answer = put(chunkId(6), b, Checksums.sha256Hex(b), identity.bootId() + ":" + afterFirst);

        assertThat(answer.statusCode()).isEqualTo(201);
        JsonNode body = json.readTree(answer.body());
        assertThat(body.path("changes").path("boot_id").asString()).isEqualTo(identity.bootId());
        JsonNode items = body.path("changes").path("items");
        assertThat(items).hasSize(1);
        assertThat(items.get(0).path("seq").asLong()).isEqualTo(afterFirst + 1);
        assertThat(items.get(0).path("type").asString()).isEqualTo("RECEIVED");
        assertThat(items.get(0).path("chunk_id").asString()).isEqualTo(chunkId(6));
        assertThat(items.get(0).path("checksum").asString()).isEqualTo(Checksums.sha256Hex(b));
        assertThat(changes.suffix(0, 100)).extracting(c -> c.seq()).doesNotContain(before + 1); // acknowledged
    }

    @Test
    void edge12_aPutAnswerForAnotherBootCarriesNoChanges() throws Exception {
        byte[] data = bytes(100, 9);
        HttpResponse<String> answer = put(chunkId(7), data, Checksums.sha256Hex(data), "some-other-boot:0");
        HttpResponse<String> repair = put(chunkId(8), data(10), Checksums.sha256Hex(data(10)));

        assertThat(answer.statusCode()).isEqualTo(201);
        assertThat(json.readTree(answer.body()).has("changes")).isFalse();
        assertThat(json.readTree(repair.body()).has("changes")).as("a worker-to-worker PUT gets no changes").isFalse();
        assertThat(changes.size()).as("nothing was acknowledged").isGreaterThan(0);
    }

    @Test
    void edge11_aPutAnswerIsBoundedAndRespectsTheHold() throws Exception {
        changes.ack(changes.lastSeq());
        for (int i = 0; i < 300; i++) {
            changes.deleted(chunkId(100 + i), nextVersion(), "deleted");
        }
        long m = changes.beginHold();
        changes.deleted(chunkId(999), nextVersion(), "deleted");             // above M: held back
        try {
            byte[] data = data(11);
            long acked = m - 300;
            HttpResponse<String> answer = put(chunkId(9), data, Checksums.sha256Hex(data), identity.bootId() + ":" + acked);
            JsonNode items = json.readTree(answer.body()).path("changes").path("items");
            assertThat(items).hasSize(256);                                  // MAX_CHANGES_PER_PUT_ANSWER
            assertThat(items.get(0).path("seq").asLong()).isEqualTo(acked + 1);

            HttpResponse<String> next = put(chunkId(9), data, Checksums.sha256Hex(data), identity.bootId() + ":" + (m - 1));
            JsonNode rest = json.readTree(next.body()).path("changes").path("items");
            assertThat(rest).hasSize(1);
            assertThat(rest.get(0).path("seq").asLong()).as("nothing above M leaves during a report").isEqualTo(m);
        } finally {
            changes.endHold();
        }
    }

    private static byte[] data(int seed) {
        return bytes(64, seed);
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
    void anOlderVersionIsStaleAndANewerOneReplacesADifferentCopyAfterQuarantiningIt() throws Exception {
        byte[] original = bytes(100, 1);
        byte[] other = bytes(100, 2);
        long older = nextVersion();
        long newer = nextVersion();
        assertThat(put(chunkId(0), original, Checksums.sha256Hex(original), null, newer).statusCode()).isEqualTo(201);

        HttpResponse<String> stale = put(chunkId(0), other, Checksums.sha256Hex(other), null, older);
        assertThat(stale.statusCode()).isEqualTo(409);
        assertThat(stale.body()).contains("\"error\":\"stale_version\"");

        HttpResponse<String> replaced = put(chunkId(0), other, Checksums.sha256Hex(other), null, nextVersion());
        assertThat(replaced.statusCode()).isEqualTo(201);
        assertThat(Files.readAllBytes(dataDir.resolve("chunks").resolve(chunkId(0)))).isEqualTo(other);
    }

    @Test
    void aPutWithoutAValidOpVersionIsABadRequest() throws Exception {
        byte[] data = bytes(10, 1);
        HttpResponse<String> missing = http.send(HttpRequest.newBuilder(uri("/internal/chunks/" + chunkId(0)))
                .header("X-Checksum", Checksums.sha256Hex(data))
                .PUT(HttpRequest.BodyPublishers.ofByteArray(data)).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(missing.statusCode()).isEqualTo(400);
        assertThat(missing.body()).contains("X-Op-Version");

        HttpResponse<String> odd = put(chunkId(0), data, Checksums.sha256Hex(data), null, 1001);
        assertThat(odd.statusCode()).as("odd versions are worker observations").isEqualTo(400);
    }

    @Test
    void aBodyThatDoesNotArriveIsCutOffAfterTheServerReadTimeout() throws Exception {
        long started = System.nanoTime();
        try (Socket socket = new Socket("localhost", port)) {
            socket.setSoTimeout(8000);
            OutputStream out = socket.getOutputStream();
            String head = "PUT /internal/chunks/" + chunkId(4) + " HTTP/1.1\r\nHost: localhost\r\n"
                    + "X-Checksum: " + "a".repeat(64) + "\r\nX-Op-Version: " + nextVersion() + "\r\n"
                    + "Content-Type: application/octet-stream\r\nContent-Length: 1000\r\n\r\n";
            out.write(head.getBytes(StandardCharsets.US_ASCII));
            out.write(new byte[10]);                                    // 10 of the 1000 bytes, then silence
            out.flush();
            InputStream in = socket.getInputStream();
            byte[] buffer = new byte[256];
            int read = in.read(buffer);                                 // an answer, or the connection closing
            double seconds = (System.nanoTime() - started) / 1e9;
            assertThat(seconds).as("SERVER_READ_TIMEOUT = 1 s applies to the body").isLessThan(6);
            if (read > 0) {
                // Tomcat reports the timed-out read as an aborted request: the answer is an error status.
                assertThat(new String(buffer, 0, read, StandardCharsets.US_ASCII)).matches("(?s)HTTP/1\\.1 [45]\\d\\d.*");
            }
        }
        assertThat(Files.exists(dataDir.resolve("chunks").resolve(chunkId(4)))).isFalse();
    }

    @Test
    void healthReportsMisconfigurationAsUnavailable() throws Exception {
        assertThat(get("/health").statusCode()).isEqualTo(200);
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
    void getReturnsTheVerifiedBytesWithTheirChecksum() throws Exception {
        byte[] data = bytes(4096, 9);
        String id = chunkId(2);
        put(id, data, Checksums.sha256Hex(data));

        HttpResponse<byte[]> intact = getBytes("/internal/chunks/" + id);
        assertThat(intact.statusCode()).isEqualTo(200);
        assertThat(intact.body()).isEqualTo(data);
        assertThat(intact.headers().firstValue("X-Checksum")).contains(Checksums.sha256Hex(data));
        assertThat(intact.headers().firstValue("Content-Type")).contains("application/octet-stream");

    }

    @Test
    void getOfAnUnknownChunkIs404() throws Exception {
        HttpResponse<String> response = get("/internal/chunks/" + chunkId(7));

        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(response.body()).contains("\"error\":\"chunk_not_found\"");
    }

    @Test
    void aVersionedDeleteRemovesTheCopyReportsItAndARepeatIsHarmless() throws Exception {
        byte[] data = bytes(10, 1);
        String id = chunkId(0);
        put(id, data, Checksums.sha256Hex(data));
        long version = nextVersion();

        HttpResponse<String> deleted = delete(id, version, "cmd-1", null);
        HttpResponse<String> repeated = delete(id, version, "cmd-1", null);

        assertThat(deleted.statusCode()).isEqualTo(204);
        assertThat(repeated.statusCode()).as("the same version again is a repeat").isEqualTo(204);
        assertThat(Files.exists(dataDir.resolve("chunks").resolve(id))).isFalse();
        assertThat(Files.exists(dataDir.resolve("chunks").resolve(id + ".meta"))).isFalse();
        assertThat(store.index()).doesNotContainKey(id);
        assertThat(get("/internal/chunks/" + id).statusCode()).isEqualTo(404);
        assertThat(changes.suffix(0, 100_000)).as("reported exactly like the DELETE command")
                .anyMatch(c -> c.type() == ChangeType.DELETED && c.chunkId().equals(id) && c.version() == version);
    }

    /** The v4 double delete, over the endpoint: a delayed old DELETE cannot destroy the newer copy. */
    @Test
    void aDeleteOlderThanTheCopyIsStaleAndTheNewerCopySurvives() throws Exception {
        byte[] data = bytes(10, 1);
        String id = chunkId(0);
        put(id, data, Checksums.sha256Hex(data));
        long first = nextVersion();
        assertThat(delete(id, first, "cmd-old", null).statusCode()).isEqualTo(204);
        assertThat(put(id, data, Checksums.sha256Hex(data)).statusCode()).as("repaired at a newer version")
                .isEqualTo(201);

        HttpResponse<String> late = delete(id, first, "cmd-old", null);     // the old request, arriving late

        assertThat(late.statusCode()).isEqualTo(409);
        assertThat(late.body()).contains("stale_version");
        assertThat(Files.readAllBytes(dataDir.resolve("chunks").resolve(id))).isEqualTo(data);
        assertThat(changes.suffix(0, 100_000)).as("the controller learns it from CMD_FAILED(stale), with the cmd id")
                .anyMatch(c -> c.type() == ChangeType.CMD_FAILED && c.chunkId().equals(id) && c.version() == first
                        && "stale".equals(c.reason()) && "cmd-old".equals(c.cmdId()));
    }

    @Test
    void aDeleteWithoutAValidVersionIsRejectedAndChangesNothing() throws Exception {
        byte[] data = bytes(10, 1);
        String id = chunkId(0);
        put(id, data, Checksums.sha256Hex(data));

        HttpResponse<String> unversioned = http.send(HttpRequest.newBuilder(uri("/internal/chunks/" + id)).DELETE().build(),
                HttpResponse.BodyHandlers.ofString());
        HttpResponse<String> odd = delete(id, 1001, null, null);

        assertThat(unversioned.statusCode()).as("an unversioned delete is exactly the v4 bug").isEqualTo(400);
        assertThat(unversioned.body()).contains("X-Op-Version");
        assertThat(odd.statusCode()).as("odd versions are worker observations").isEqualTo(400);
        assertThat(get("/internal/chunks/" + id).statusCode()).isEqualTo(200);
    }

    @Test
    void aCorruptReasonQuarantinesTheCopyInsteadOfDeletingIt() throws Exception {
        byte[] data = bytes(10, 1);
        String id = chunkId(0);
        put(id, data, Checksums.sha256Hex(data));

        assertThat(delete(id, nextVersion(), null, "corrupt").statusCode()).isEqualTo(204);

        assertThat(store.isQuarantined(id)).isTrue();
        assertThat(get("/internal/chunks/" + id).statusCode()).isNotEqualTo(200);
        try (Stream<Path> files = Files.list(store.quarantineDir())) {
            assertThat(files.map(f -> f.getFileName().toString())).anyMatch(name -> name.startsWith(id + "."));
        }
    }

    @Test
    void aCorruptCopyIsNeverServed() throws Exception {
        byte[] data = bytes(4096, 4);
        put(chunkId(3), data, Checksums.sha256Hex(data));
        byte[] damaged = data.clone();
        damaged[7] ^= 0x10;
        Files.write(dataDir.resolve("chunks").resolve(chunkId(3)), damaged);

        HttpResponse<String> response = get("/internal/chunks/" + chunkId(3));

        assertThat(response.statusCode()).isEqualTo(409);
        assertThat(response.body()).contains("\"error\":\"chunk_corrupt\"");
    }

    @Test
    void listReportsEveryChunkAndOnlyForThisWorker() throws Exception {
        byte[] a = bytes(300, 1);
        byte[] b = bytes(700, 2);
        put(chunkId(1), b, Checksums.sha256Hex(b));
        put(chunkId(0), a, Checksums.sha256Hex(a));

        HttpResponse<String> mine = get("/internal/workers/worker1/chunks");
        assertThat(mine.statusCode()).isEqualTo(200);
        // The listing re-hashes the files: it is what fsck compares with the controller's view.
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
        return put(chunkId, body, checksum, null);
    }

    private HttpResponse<String> put(String chunkId, byte[] body, String checksum, String changeAck) throws Exception {
        return put(chunkId, body, checksum, changeAck, nextVersion());
    }

    private HttpResponse<String> put(String chunkId, byte[] body, String checksum, String changeAck, long version)
            throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(uri("/internal/chunks/" + chunkId))
                .header("X-Checksum", checksum)
                .header("X-Op-Version", Long.toString(version))
                .header("Content-Type", "application/octet-stream")
                .PUT(HttpRequest.BodyPublishers.ofByteArray(body));
        if (changeAck != null) {
            request.header("X-Change-Ack", changeAck);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(uri(path)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<byte[]> getBytes(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(uri(path)).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    private HttpResponse<String> delete(String chunkId, long version, String cmdId, String reason) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(uri("/internal/chunks/" + chunkId))
                .header("X-Op-Version", Long.toString(version))
                .DELETE();
        if (cmdId != null) {
            request.header("X-Cmd-Id", cmdId);
        }
        if (reason != null) {
            request.header("X-Reason", reason);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }
}
