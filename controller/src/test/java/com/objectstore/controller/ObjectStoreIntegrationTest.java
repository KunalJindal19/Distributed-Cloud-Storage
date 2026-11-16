package com.objectstore.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.objectstore.common.Checksums;
import com.objectstore.controller.support.FakeWorker;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * End-to-end tests of the controller: the real application (HTTP layer, SQLite, background loops)
 * running against four in-memory workers that speak the internal API. The timers are shortened so
 * that failure detection and repair happen in a second or two instead of the production 6/10/15 s.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "objectstore.heartbeat-timeout-seconds=1.5",
                "objectstore.health-interval-seconds=0.2",
                "objectstore.startup-grace-seconds=0",
                "objectstore.repair-interval-seconds=0.5",
                "objectstore.scrub-interval-seconds=0.5",
                "objectstore.worker-timeout-seconds=3"
        })
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ObjectStoreIntegrationTest {

    private static final int MIB = 1_048_576;
    private static final String UNKNOWN_ID = "00000000-0000-0000-0000-000000000000";
    private static final Duration PATIENCE = Duration.ofSeconds(20);

    /** Created eagerly: the Spring context (and with it the database) starts before any JUnit callback. */
    private static final Path dataDir = createDataDir();

    private static Path createDataDir() {
        try {
            Path dir = Files.createTempDirectory("objectstore-controller-test");
            dir.toFile().deleteOnExit();
            return dir;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("objectstore.db-path", () -> dataDir.resolve("meta.db").toString());
    }

    @Value("${local.server.port}")
    int port;

    private final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    private final JsonMapper json = JsonMapper.builder().build();
    private final Map<String, FakeWorker> workers = new LinkedHashMap<>();
    private ScheduledExecutorService heartbeats;

    @BeforeAll
    void startCluster() throws Exception {
        for (String id : List.of("worker1", "worker2", "worker3", "worker4")) {
            FakeWorker worker = new FakeWorker(id);
            worker.start();
            workers.put(id, worker);
            register(worker);
        }
        heartbeats = Executors.newSingleThreadScheduledExecutor();
        heartbeats.scheduleWithFixedDelay(this::beat, 100, 300, TimeUnit.MILLISECONDS);
    }

    @AfterAll
    void stopCluster() {
        heartbeats.shutdownNow();
        workers.values().forEach(FakeWorker::stop);
    }

    /** Every test starts from, and therefore must be able to return to, a fully healthy cluster. */
    @BeforeEach
    void healthyCluster() throws Exception {
        for (FakeWorker worker : workers.values()) {
            worker.heal();
            if (!worker.isRunning()) {
                restart(worker);
            }
        }
        await().atMost(PATIENCE).untilAsserted(() -> {
            JsonNode status = status();
            assertThat(aliveWorkers(status)).hasSize(4);
            assertThat(status.get("under_replicated").asInt()).isZero();
            assertThat(status.get("over_replicated").asInt()).isZero();
            assertThat(status.get("replicas").get("corrupt").asInt()).isZero();
            assertThat(status.get("replicas").get("missing").asInt()).isZero();
        });
    }

    // ---- Object storage, chunking, distribution, replication ---------------------------------

    @Test
    void anObjectLargerThanOneChunkIsSplitReplicatedAndReturnedByteForByte() throws Exception {
        byte[] original = bytes(3_500_000, 1);

        HttpResponse<String> response = upload("big.bin", "application/octet-stream", original, null);

        assertThat(response.statusCode()).isEqualTo(201);
        JsonNode object = json.readTree(response.body());
        String id = object.get("object_id").asString();
        assertThat(UUID.fromString(id).toString()).isEqualTo(id);
        assertThat(response.headers().firstValue("Location")).contains("/api/v1/objects/" + id);
        assertThat(object.get("name").asString()).isEqualTo("big.bin");
        assertThat(object.get("size").asLong()).isEqualTo(3_500_000);
        assertThat(object.get("checksum").asString()).isEqualTo(Checksums.sha256Hex(original));
        assertThat(object.get("created_at").asString()).matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z");
        assertThat(object.get("updated_at").asString()).isEqualTo(object.get("created_at").asString());
        assertThat(object.get("replication_ok").asBoolean()).isTrue();

        // Chunking: 1 MiB pieces, only the last one smaller, ids carry the order.
        assertThat(object.get("chunk_count").asInt()).isEqualTo(4);
        JsonNode chunks = object.get("chunks");
        assertThat(chunks.size()).isEqualTo(4);
        Map<String, Integer> copiesPerWorker = new LinkedHashMap<>();
        for (int i = 0; i < 4; i++) {
            JsonNode chunk = chunks.get(i);
            int expectedSize = i < 3 ? MIB : 3_500_000 - 3 * MIB;
            byte[] range = Arrays.copyOfRange(original, i * MIB, i * MIB + expectedSize);
            assertThat(chunk.get("chunk_id").asString()).isEqualTo("%s-%05d".formatted(id, i));
            assertThat(chunk.get("index").asInt()).isEqualTo(i);
            assertThat(chunk.get("size").asInt()).isEqualTo(expectedSize);
            assertThat(chunk.get("checksum").asString()).isEqualTo(Checksums.sha256Hex(range));

            // Replication: exactly two copies, on two different workers, both good.
            List<String> holders = holders(chunk);
            assertThat(holders).hasSize(2).doesNotHaveDuplicates();
            for (JsonNode location : chunk.get("locations")) {
                assertThat(location.get("status").asString()).isEqualTo("OK");
                assertThat(location.get("worker_status").asString()).isEqualTo("ALIVE");
            }
            // ...and the bytes really are on those workers, and nowhere else.
            for (FakeWorker worker : workers.values()) {
                byte[] stored = worker.files().get(chunk.get("chunk_id").asString());
                if (holders.contains(worker.id())) {
                    assertThat(stored).isEqualTo(range);
                    copiesPerWorker.merge(worker.id(), 1, Integer::sum);
                } else {
                    assertThat(stored).isNull();
                }
            }
        }
        // Distribution: four chunks x two copies over four workers = two each.
        assertThat(copiesPerWorker).containsOnly(
                Map.entry("worker1", 2), Map.entry("worker2", 2), Map.entry("worker3", 2), Map.entry("worker4", 2));

        HttpResponse<byte[]> download = download(id);
        assertThat(download.statusCode()).isEqualTo(200);
        assertThat(download.body()).isEqualTo(original);
        assertThat(download.headers().firstValue("Content-Type")).contains("application/octet-stream");
        assertThat(download.headers().firstValue("Content-Length")).contains("3500000");
        assertThat(download.headers().firstValue("Content-Disposition")).contains("attachment; filename=\"big.bin\"");
        assertThat(download.headers().firstValue("ETag")).contains("\"" + Checksums.sha256Hex(original) + "\"");
        assertThat(download.headers().firstValue("X-Object-Id")).contains(id);
    }

    @Test
    void objectsOfAwkwardSizesRoundTrip() throws Exception {
        for (int size : new int[] {0, 1, 10, MIB - 1, MIB, MIB + 1, 2 * MIB}) {
            byte[] original = bytes(size, size);

            JsonNode object = uploadOk("size-" + size + ".bin", "application/octet-stream", original, null);

            int expectedChunks = (size + MIB - 1) / MIB;
            assertThat(object.get("chunk_count").asInt()).as("chunks of %d bytes", size).isEqualTo(expectedChunks);
            assertThat(object.get("chunks").size()).isEqualTo(expectedChunks);
            assertThat(object.get("checksum").asString()).isEqualTo(Checksums.sha256Hex(original));
            assertThat(object.get("replication_ok").asBoolean()).isTrue();
            for (JsonNode chunk : object.get("chunks")) {
                assertThat(chunk.get("size").asInt()).isPositive(); // never an empty trailing chunk
            }
            HttpResponse<byte[]> download = download(object.get("object_id").asString());
            assertThat(download.statusCode()).isEqualTo(200);
            assertThat(download.body()).isEqualTo(original);
            assertThat(download.headers().firstValue("Content-Length")).contains(String.valueOf(size));
        }
    }

    @Test
    void theContentTypeIsStoredAndUsedForTheDownload() throws Exception {
        byte[] data = bytes(2000, 3);

        JsonNode declared = uploadOk("picture.bin", "image/png", data, null);
        JsonNode inferred = uploadOk("my photo é.jpg", null, data, null);

        assertThat(declared.get("content_type").asString()).isEqualTo("image/png");
        assertThat(inferred.get("content_type").asString()).isEqualTo("image/jpeg");
        assertThat(inferred.get("name").asString()).isEqualTo("my photo é.jpg");

        HttpResponse<byte[]> download = download(inferred.get("object_id").asString());
        assertThat(download.headers().firstValue("Content-Type")).contains("image/jpeg");
        assertThat(download.headers().firstValue("Content-Disposition")).contains("attachment; filename=\"my_photo__.jpg\"");
    }

    // ---- Metadata ---------------------------------------------------------------------------

    @Test
    void customMetadataCanBeReadAndChangedWithoutTouchingTheData() throws Exception {
        byte[] data = bytes(MIB + 500, 4);
        JsonNode created = uploadOk("doc.pdf", "application/pdf", data, "{\"owner\":\"ansh\",\"project\":\"cloud\"}");
        String id = created.get("object_id").asString();
        assertThat(metadataMap(created)).containsOnly(Map.entry("owner", "ansh"), Map.entry("project", "cloud"));
        int workerCallsBefore = totalChunkRequests();

        JsonNode fetched = json.readTree(get("/api/v1/objects/" + id + "/metadata").body());
        assertThat(fetched).isEqualTo(created);

        Thread.sleep(5);
        HttpResponse<String> patched = patch(id, "{\"owner\":\"team\",\"tag\":\"v1\"}");
        assertThat(patched.statusCode()).isEqualTo(200);
        JsonNode afterPatch = json.readTree(patched.body());
        assertThat(metadataMap(afterPatch)).containsOnly(
                Map.entry("owner", "team"), Map.entry("project", "cloud"), Map.entry("tag", "v1"));
        assertThat(afterPatch.get("updated_at").asString()).isGreaterThan(created.get("created_at").asString());
        assertThat(afterPatch.get("created_at")).isEqualTo(created.get("created_at"));

        JsonNode afterRemoval = json.readTree(patch(id, "{\"tag\":null,\"missing-key\":null}").body());
        assertThat(metadataMap(afterRemoval)).containsOnly(Map.entry("owner", "team"), Map.entry("project", "cloud"));
        assertThat(afterRemoval.get("updated_at").asString()).isGreaterThan(afterPatch.get("updated_at").asString());

        // System metadata and chunk layout are exactly as they were, and no worker was involved.
        for (String field : List.of("size", "checksum", "chunk_count", "chunks", "name", "content_type")) {
            assertThat(afterRemoval.get(field)).as(field).isEqualTo(created.get(field));
        }
        assertThat(totalChunkRequests()).isEqualTo(workerCallsBefore);
        assertThat(download(id).body()).isEqualTo(data);
    }

    @Test
    void invalidMetadataIsRejectedAndChangesNothing() throws Exception {
        JsonNode created = uploadOk("a.txt", "text/plain", bytes(10, 5), "{\"owner\":\"ansh\"}");
        String id = created.get("object_id").asString();
        String sixtyFourMoreKeys = IntStream.range(0, 64)
                .mapToObj(i -> "\"k" + i + "\":\"v\"")
                .collect(Collectors.joining(",", "{", "}"));

        for (String body : List.of("[1,2]", "\"text\"", "{broken", "{\"owner\":5}", "{\"bad key\":\"x\"}",
                "{\"k\":\"" + "v".repeat(1025) + "\"}", sixtyFourMoreKeys, "")) {
            HttpResponse<String> response = patch(id, body);
            assertError(response, 400, "invalid_metadata");
        }
        assertThat(json.readTree(get("/api/v1/objects/" + id + "/metadata").body())).isEqualTo(created);

        // On upload: 400, and nothing reaches a worker or the database.
        long objectsBefore = status().get("objects").asLong();
        int workerCallsBefore = totalChunkRequests();
        assertError(upload("b.txt", "text/plain", bytes(10, 6), "[1,2]"), 400, "invalid_metadata");
        assertError(upload("b.txt", "text/plain", bytes(10, 6), "{\"n\":1}"), 400, "invalid_metadata");
        assertThat(status().get("objects").asLong()).isEqualTo(objectsBefore);
        assertThat(totalChunkRequests()).isEqualTo(workerCallsBefore);
    }

    // ---- Listing, deletion, errors ----------------------------------------------------------

    @Test
    void listingIsPagedNewestFirst() throws Exception {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            ids.add(uploadOk("list-" + i + ".txt", "text/plain", bytes(100 + i, 7), null).get("object_id").asString());
            Thread.sleep(5);
        }

        JsonNode firstPage = json.readTree(get("/api/v1/objects?limit=2").body());
        JsonNode secondPage = json.readTree(get("/api/v1/objects?limit=2&offset=2").body());

        assertThat(firstPage.get("limit").asInt()).isEqualTo(2);
        assertThat(firstPage.get("offset").asInt()).isZero();
        assertThat(firstPage.get("total").asLong()).isGreaterThanOrEqualTo(3).isEqualTo(status().get("objects").asLong());
        assertThat(firstPage.get("items").size()).isEqualTo(2);
        assertThat(firstPage.get("items").get(0).get("object_id").asString()).isEqualTo(ids.get(2));
        assertThat(firstPage.get("items").get(1).get("object_id").asString()).isEqualTo(ids.get(1));
        assertThat(secondPage.get("items").get(0).get("object_id").asString()).isEqualTo(ids.get(0));
        JsonNode newest = firstPage.get("items").get(0);
        assertThat(newest.get("name").asString()).isEqualTo("list-2.txt");
        assertThat(newest.get("size").asLong()).isEqualTo(102);
        assertThat(newest.get("content_type").asString()).isEqualTo("text/plain");

        JsonNode defaults = json.readTree(get("/api/v1/objects").body());
        assertThat(defaults.get("limit").asInt()).isEqualTo(50);
    }

    @Test
    void deletingAnObjectRemovesItsDataFromEveryWorker() throws Exception {
        JsonNode created = uploadOk("gone.bin", "application/octet-stream", bytes(2 * MIB + 5, 8), null);
        String id = created.get("object_id").asString();
        long chunksBefore = status().get("chunks").asLong();
        assertThat(workers.values().stream().mapToInt(w -> w.chunkIdsOf(id).size()).sum()).isEqualTo(6);

        HttpResponse<String> deleted = delete(id);

        assertThat(deleted.statusCode()).isEqualTo(204);
        assertThat(deleted.body()).isEmpty();
        assertThat(workers.values()).allSatisfy(worker -> assertThat(worker.chunkIdsOf(id)).isEmpty());
        assertThat(status().get("chunks").asLong()).isEqualTo(chunksBefore - 3);
        assertError(get("/api/v1/objects/" + id), 404, "object_not_found");
        assertError(get("/api/v1/objects/" + id + "/metadata"), 404, "object_not_found");
        assertError(delete(id), 404, "object_not_found");
    }

    @Test
    void errorsAreAlwaysJsonWithTheRightStatus() throws Exception {
        assertError(get("/api/v1/objects/" + UNKNOWN_ID), 404, "object_not_found");
        assertError(get("/api/v1/objects/" + UNKNOWN_ID + "/metadata"), 404, "object_not_found");
        assertError(patch(UNKNOWN_ID, "{\"k\":\"v\"}"), 404, "object_not_found");
        assertError(delete(UNKNOWN_ID), 404, "object_not_found");

        assertError(get("/api/v1/objects?limit=0"), 400, "bad_request");
        assertError(get("/api/v1/objects?limit=501"), 400, "bad_request");
        assertError(get("/api/v1/objects?offset=-1"), 400, "bad_request");
        assertError(get("/api/v1/objects?limit=abc"), 400, "bad_request");

        // Upload without the required "file" part, and a POST that is not multipart at all.
        assertError(upload(null, null, null, "{\"owner\":\"ansh\"}"), 400, "bad_request");
        assertError(send(HttpRequest.newBuilder(uri("/api/v1/objects"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{}"))), 400, "bad_request");

        assertError(get("/api/v1/nope"), 404, "not_found");
        assertError(send(HttpRequest.newBuilder(uri("/api/v1/objects/" + UNKNOWN_ID))
                .PUT(HttpRequest.BodyPublishers.ofString("x"))), 405, "method_not_allowed");
    }

    @Test
    void aSecondRequestForAnObjectThatIsBeingDeletedIsAConflict() throws Exception {
        JsonNode created = uploadOk("contested.bin", "application/octet-stream", bytes(MIB + 10, 18), null);
        String id = created.get("object_id").asString();
        int deletesBefore = workers.values().stream().mapToInt(FakeWorker::deleteRequests).sum();
        workers.values().forEach(worker -> worker.delayDeletes(1500));

        CompletableFuture<HttpResponse<String>> firstDelete = http.sendAsync(
                HttpRequest.newBuilder(uri("/api/v1/objects/" + id)).DELETE().build(),
                HttpResponse.BodyHandlers.ofString());
        // The first delete is under way once the workers have been asked to remove the chunks.
        await().atMost(PATIENCE).until(
                () -> workers.values().stream().mapToInt(FakeWorker::deleteRequests).sum() > deletesBefore);

        assertError(delete(id), 409, "conflict");
        assertError(patch(id, "{\"owner\":\"late\"}"), 409, "conflict");

        assertThat(firstDelete.get(10, TimeUnit.SECONDS).statusCode()).isEqualTo(204);
        assertError(delete(id), 404, "object_not_found");
    }

    @Test
    void theScrubberLeavesAnUploadInProgressAlone() throws Exception {
        byte[] original = bytes(3 * MIB + 99, 19);
        // Each chunk write takes longer than a scrub interval, so the scrubber passes several times
        // while chunk files already exist on the workers but the object is not recorded yet.
        workers.values().forEach(worker -> worker.delayWrites(700));

        JsonNode created = uploadOk("slow.bin", "application/octet-stream", original, null);

        workers.values().forEach(FakeWorker::heal);
        String id = created.get("object_id").asString();
        Thread.sleep(1500); // a few more scrub and repair passes over the finished object
        JsonNode object = metadata(id);
        assertThat(object.get("replication_ok").asBoolean()).isTrue();
        for (JsonNode chunk : object.get("chunks")) {
            String chunkId = chunk.get("chunk_id").asString();
            // Nothing was collected as an orphan, flagged as missing, purged or re-copied.
            assertThat(workersStoring(chunkId)).containsExactlyInAnyOrderElementsOf(holders(chunk));
            assertThat(goodHolders(chunk)).hasSize(2);
            assertThat(holders(chunk)).isEqualTo(holders(created.get("chunks").get(chunk.get("index").asInt())));
            assertThat(repairEvents("purged", chunkId)).isEmpty();
            assertThat(repairEvents("copied", chunkId)).isEmpty();
        }
        assertThat(download(id).body()).isEqualTo(original);
    }

    // ---- Internal API -----------------------------------------------------------------------

    @Test
    void workersRegisterAndHeartbeatAndUnknownWorkersAreToldToRegister() throws Exception {
        HttpResponse<String> unknown = postJson("/internal/workers/heartbeat", "{\"worker_id\":\"worker99\"}");
        assertError(unknown, 404, "unknown_worker");

        HttpResponse<String> beat = postJson("/internal/workers/heartbeat", "{\"worker_id\":\"worker1\"}");
        assertThat(beat.statusCode()).isEqualTo(200);
        assertThat(beat.body()).isEqualTo("{\"ok\":true,\"worker_id\":\"worker1\",\"status\":\"ALIVE\"}");

        assertError(postJson("/internal/workers/register", "{\"worker_id\":\"../x\",\"url\":\"http://h:1\"}"), 400, "bad_request");
        assertError(postJson("/internal/workers/register", "{\"worker_id\":\"worker1\"}"), 400, "bad_request");
        assertError(postJson("/internal/workers/register", "not json"), 400, "bad_request");

        JsonNode status = status();
        assertThat(status.get("workers").size()).isEqualTo(4);
        assertThat(status.get("config").get("chunk_size").asInt()).isEqualTo(MIB);
        assertThat(status.get("config").get("replication_factor").asInt()).isEqualTo(2);
        assertThat(get("/health").body()).isEqualTo("{\"status\":\"ok\",\"service\":\"controller\"}");
    }

    // ---- Worker failure ---------------------------------------------------------------------

    @Test
    void whenAWorkerDiesItsChunksAreReReplicatedAndTrimmedBackToTwoWhenItReturns() throws Exception {
        byte[] original = bytes(3_500_000, 9);
        JsonNode created = uploadOk("survivor.bin", "application/octet-stream", original, null);
        String id = created.get("object_id").asString();
        FakeWorker victim = workers.get("worker2");
        Set<String> chunksOnVictim = victim.chunkIdsOf(id);
        assertThat(chunksOnVictim).hasSize(2);

        victim.stop();

        // Detected: no heartbeat for longer than the time-out.
        await().atMost(PATIENCE).untilAsserted(() -> assertThat(workerStatus("worker2")).isEqualTo("DEAD"));
        // Still served, from the surviving copies.
        assertThat(download(id).body()).isEqualTo(original);
        // Restored: every chunk has two good copies again, none of them on the dead worker.
        await().atMost(PATIENCE).untilAsserted(() -> {
            assertThat(status().get("under_replicated").asInt()).isZero();
            JsonNode object = metadata(id);
            assertThat(object.get("replication_ok").asBoolean()).isTrue();
            for (JsonNode chunk : object.get("chunks")) {
                assertThat(goodHolders(chunk)).hasSize(2).doesNotContain("worker2");
            }
        });
        for (String chunkId : chunksOnVictim) {
            assertThat(repairEvents("copied", chunkId))
                    .hasSize(1)
                    .allSatisfy(event -> assertThat(event.get("target").asString()).isNotEqualTo("worker2"));
        }
        assertThat(download(id).body()).isEqualTo(original);

        restart(victim);

        // Back, verified by the scrubber, and the surplus copies trimmed: exactly two per chunk.
        await().atMost(PATIENCE).untilAsserted(() -> {
            JsonNode status = status();
            assertThat(workerStatus("worker2")).isEqualTo("ALIVE");
            assertThat(status.get("over_replicated").asInt()).isZero();
            assertThat(status.get("under_replicated").asInt()).isZero();
            for (JsonNode chunk : metadata(id).get("chunks")) {
                assertThat(holders(chunk)).hasSize(2).doesNotHaveDuplicates();
                assertThat(goodHolders(chunk)).hasSize(2);
            }
        });
        for (String chunkId : chunksOnVictim) {
            assertThat(repairEvents("trimmed", chunkId)).hasSize(1);
        }
        // What is on the workers' disks matches the database exactly: two files per chunk.
        for (JsonNode chunk : metadata(id).get("chunks")) {
            String chunkId = chunk.get("chunk_id").asString();
            await().atMost(PATIENCE).untilAsserted(() -> assertThat(workersStoring(chunkId))
                    .containsExactlyInAnyOrderElementsOf(holders(chunk)));
        }
        assertThat(download(id).body()).isEqualTo(original);
    }

    @Test
    void anUploadSurvivesAWorkerThatDiedButHasNotBeenDeclaredDeadYet() throws Exception {
        byte[] original = bytes(3_500_000, 10);
        workers.get("worker3").stop();

        // Straight away: the controller still believes worker3 is alive and will try to use it.
        JsonNode created = uploadOk("resilient.bin", "application/octet-stream", original, null);

        assertThat(created.get("chunk_count").asInt()).isEqualTo(4);
        for (JsonNode chunk : created.get("chunks")) {
            assertThat(holders(chunk)).hasSize(2).doesNotHaveDuplicates().doesNotContain("worker3");
        }
        assertThat(download(created.get("object_id").asString()).body()).isEqualTo(original);
    }

    @Test
    void uploadsNeedAtLeastTwoLiveWorkers() throws Exception {
        workers.get("worker2").stop();
        workers.get("worker3").stop();
        workers.get("worker4").stop();
        await().atMost(PATIENCE).untilAsserted(() -> assertThat(aliveWorkers(status())).containsExactly("worker1"));
        int workerCallsBefore = workers.get("worker1").chunkRequests();

        assertError(upload("x.bin", "application/octet-stream", bytes(5000, 11), null), 503, "insufficient_workers");
        assertThat(workers.get("worker1").chunkRequests()).isEqualTo(workerCallsBefore);

        restart(workers.get("worker2"));
        await().atMost(PATIENCE).untilAsserted(
                () -> assertThat(aliveWorkers(status())).containsExactly("worker1", "worker2"));

        JsonNode created = uploadOk("x.bin", "application/octet-stream", bytes(3 * MIB, 11), null);
        for (JsonNode chunk : created.get("chunks")) {
            assertThat(holders(chunk)).containsExactlyInAnyOrder("worker1", "worker2");
        }
    }

    @Test
    void aChunkWithNoReachableCopyIsReportedAndRepairedAsSoonAsOneHolderReturns() throws Exception {
        byte[] original = bytes(4096, 12);
        JsonNode created = uploadOk("fragile.bin", "application/octet-stream", original, null);
        String id = created.get("object_id").asString();
        String chunkId = created.get("chunks").get(0).get("chunk_id").asString();
        List<String> holders = holders(created.get("chunks").get(0));

        holders.forEach(holder -> workers.get(holder).stop());
        await().atMost(PATIENCE).untilAsserted(
                () -> assertThat(aliveWorkers(status())).doesNotContainAnyElementsOf(holders).hasSize(2));

        // Both copies are out of reach: a clean 503, not a broken download, and the gap is visible.
        assertError(get("/api/v1/objects/" + id), 503, "chunk_unavailable");
        await().atMost(PATIENCE).untilAsserted(() -> {
            assertThat(status().get("under_replicated").asInt()).isPositive();
            assertThat(repairEvents("unrecoverable", chunkId)).isNotEmpty();
        });
        assertThat(metadata(id).get("replication_ok").asBoolean()).isFalse();

        restart(workers.get(holders.get(0)));

        await().atMost(PATIENCE).untilAsserted(() -> {
            JsonNode chunk = metadata(id).get("chunks").get(0);
            assertThat(goodHolders(chunk)).hasSize(2).contains(holders.get(0)).doesNotContain(holders.get(1));
        });
        assertThat(download(id).body()).isEqualTo(original);
        assertThat(repairEvents("unrecoverable", chunkId)).hasSize(1); // reported once, not every pass
    }

    @Test
    void anObjectDeletedWhileAWorkerWasDownLeavesNoOrphansOnceItReturns() throws Exception {
        JsonNode created = uploadOk("orphan.bin", "application/octet-stream", bytes(2 * MIB, 13), null);
        String id = created.get("object_id").asString();
        FakeWorker victim = workers.get(holders(created.get("chunks").get(0)).get(0));
        assertThat(victim.chunkIdsOf(id)).isNotEmpty();

        victim.stop();
        await().atMost(PATIENCE).untilAsserted(() -> assertThat(workerStatus(victim.id())).isEqualTo("DEAD"));
        assertThat(delete(id).statusCode()).isEqualTo(204);
        assertThat(victim.chunkIdsOf(id)).isNotEmpty(); // could not be reached

        restart(victim);

        await().atMost(PATIENCE).untilAsserted(() -> assertThat(victim.chunkIdsOf(id)).isEmpty());
    }

    // ---- Data integrity ---------------------------------------------------------------------

    @Test
    void aCorruptedCopyIsNeverServedAndIsReplaced() throws Exception {
        byte[] original = bytes(2 * MIB + 700, 14);
        JsonNode created = uploadOk("precious.bin", "application/octet-stream", original, null);
        String id = created.get("object_id").asString();
        // Damage, for every chunk, the copy the controller will try first.
        Map<String, String> damaged = new LinkedHashMap<>();
        JsonNode status = status();
        for (JsonNode chunk : created.get("chunks")) {
            String firstChoice = holders(chunk).stream()
                    .min(Comparator.<String>comparingInt(holder -> chunkCount(status, holder)).thenComparing(h -> h))
                    .orElseThrow();
            workers.get(firstChoice).corrupt(chunk.get("chunk_id").asString());
            damaged.put(chunk.get("chunk_id").asString(), firstChoice);
        }

        HttpResponse<byte[]> download = download(id);

        assertThat(download.statusCode()).isEqualTo(200);
        assertThat(download.body()).isEqualTo(original);
        assertHealed(id, original);
        damaged.forEach((chunkId, worker) -> {
            assertThat(repairEvents("purged", chunkId)).hasSize(1)
                    .allSatisfy(event -> assertThat(event.get("source").asString()).isEqualTo(worker));
            assertThat(repairEvents("copied", chunkId)).hasSize(1);
        });
    }

    @Test
    void theScrubberFindsCorruptionAndMissingFilesWithoutAnyoneReadingThem() throws Exception {
        byte[] original = bytes(2 * MIB + 700, 15);
        JsonNode created = uploadOk("quiet.bin", "application/octet-stream", original, null);
        String id = created.get("object_id").asString();
        JsonNode first = created.get("chunks").get(0);
        JsonNode second = created.get("chunks").get(1);
        String corruptedOn = holders(first).get(0);
        String removedFrom = holders(second).get(1);

        workers.get(corruptedOn).corrupt(first.get("chunk_id").asString());
        workers.get(removedFrom).files().remove(second.get("chunk_id").asString());

        // No download here: detection is the scrubber's doing alone.
        assertHealed(id, original);
        assertThat(repairEvents("purged", first.get("chunk_id").asString())).hasSize(1);
        assertThat(repairEvents("purged", second.get("chunk_id").asString())).hasSize(1);
        assertThat(download(id).body()).isEqualTo(original);
    }

    @Test
    void filesTheDatabaseDoesNotKnowAreGarbageCollected() throws Exception {
        String stray = UUID.randomUUID() + "-00000";
        workers.get("worker4").files().put(stray, bytes(100, 16));

        await().atMost(PATIENCE).untilAsserted(() -> assertThat(workers.get("worker4").files()).doesNotContainKey(stray));
    }

    @Test
    void aDownloadThatLosesEveryCopyMidStreamIsCutOffNotSilentlyTruncated() throws Exception {
        byte[] original = bytes(2 * MIB + 10, 17);
        JsonNode created = uploadOk("unlucky.bin", "application/octet-stream", original, null);
        String id = created.get("object_id").asString();
        JsonNode lastChunk = created.get("chunks").get(2);
        holders(lastChunk).forEach(holder -> workers.get(holder).failReadsOf(lastChunk.get("chunk_id").asString()));

        // The first chunks are already on the wire when the last one turns out to be unreadable.
        assertThatThrownBy(() -> download(id)).isInstanceOf(IOException.class);

        // Once the failed reads have been recorded the problem is known up front, and the answer
        // is a clean 503. (The files are intact on disk, so the scrubber keeps restoring the
        // copies and a later attempt may be cut off again; hence "eventually".)
        await().atMost(PATIENCE).ignoreExceptionsInstanceOf(IOException.class).untilAsserted(
                () -> assertError(get("/api/v1/objects/" + id), 503, "chunk_unavailable"));

        // The files were intact all along. Once readable again the scrubber restores the copies.
        workers.values().forEach(FakeWorker::heal);
        assertHealed(id, original);
        assertThat(download(id).body()).isEqualTo(original);
    }

    // ---- helpers ----------------------------------------------------------------------------

    /** Waits until every chunk of the object has exactly two good copies holding the right bytes. */
    private void assertHealed(String objectId, byte[] original) {
        await().atMost(PATIENCE).untilAsserted(() -> {
            JsonNode object = metadata(objectId);
            assertThat(object.get("replication_ok").asBoolean()).isTrue();
            for (JsonNode chunk : object.get("chunks")) {
                String chunkId = chunk.get("chunk_id").asString();
                int from = chunk.get("index").asInt() * MIB;
                byte[] expected = Arrays.copyOfRange(original, from, from + chunk.get("size").asInt());
                assertThat(holders(chunk)).hasSize(2).doesNotHaveDuplicates();
                assertThat(goodHolders(chunk)).hasSize(2);
                assertThat(workersStoring(chunkId)).containsExactlyInAnyOrderElementsOf(holders(chunk));
                for (String holder : holders(chunk)) {
                    assertThat(workers.get(holder).files().get(chunkId)).isEqualTo(expected);
                }
            }
        });
    }

    private List<String> workersStoring(String chunkId) {
        return workers.values().stream().filter(w -> w.files().containsKey(chunkId)).map(FakeWorker::id).toList();
    }

    private static List<String> holders(JsonNode chunk) {
        List<String> holders = new ArrayList<>();
        chunk.get("locations").forEach(location -> holders.add(location.get("worker_id").asString()));
        return holders;
    }

    private static List<String> goodHolders(JsonNode chunk) {
        List<String> holders = new ArrayList<>();
        for (JsonNode location : chunk.get("locations")) {
            if (location.get("status").asString().equals("OK") && location.get("worker_status").asString().equals("ALIVE")) {
                holders.add(location.get("worker_id").asString());
            }
        }
        return holders;
    }

    private static Map<String, String> metadataMap(JsonNode object) {
        Map<String, String> map = new LinkedHashMap<>();
        object.get("custom_metadata").properties().forEach(e -> map.put(e.getKey(), e.getValue().asString()));
        return map;
    }

    private static List<String> aliveWorkers(JsonNode status) {
        List<String> alive = new ArrayList<>();
        for (JsonNode worker : status.get("workers")) {
            if (worker.get("status").asString().equals("ALIVE")) {
                alive.add(worker.get("worker_id").asString());
            }
        }
        return alive;
    }

    private static int chunkCount(JsonNode status, String workerId) {
        for (JsonNode worker : status.get("workers")) {
            if (worker.get("worker_id").asString().equals(workerId)) {
                return worker.get("chunk_count").asInt();
            }
        }
        throw new IllegalArgumentException(workerId);
    }

    private String workerStatus(String workerId) throws Exception {
        for (JsonNode worker : status().get("workers")) {
            if (worker.get("worker_id").asString().equals(workerId)) {
                return worker.get("status").asString();
            }
        }
        return "UNKNOWN";
    }

    private List<JsonNode> repairEvents(String event, String chunkId) {
        List<JsonNode> matching = new ArrayList<>();
        try {
            for (JsonNode entry : status().get("repair_log")) {
                if (entry.get("event").asString().equals(event) && entry.get("chunk_id").asString().equals(chunkId)) {
                    matching.add(entry);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return matching;
    }

    private int totalChunkRequests() {
        return workers.values().stream().mapToInt(FakeWorker::chunkRequests).sum();
    }

    private JsonNode status() throws Exception {
        return json.readTree(get("/api/v1/cluster/status").body());
    }

    private JsonNode metadata(String objectId) throws Exception {
        HttpResponse<String> response = get("/api/v1/objects/" + objectId + "/metadata");
        assertThat(response.statusCode()).isEqualTo(200);
        return json.readTree(response.body());
    }

    private void assertError(HttpResponse<String> response, int status, String code) {
        assertThat(response.statusCode()).as(response.body()).isEqualTo(status);
        assertThat(response.headers().firstValue("Content-Type").orElse("")).startsWith("application/json");
        JsonNode body = json.readTree(response.body());
        assertThat(body.get("error").asString()).isEqualTo(code);
        assertThat(body.get("detail").asString()).isNotBlank();
        Set<String> fields = new HashSet<>(body.propertyNames());
        assertThat(fields).containsExactlyInAnyOrder("error", "detail");
    }

    private void restart(FakeWorker worker) throws Exception {
        worker.start();
        register(worker);
    }

    private void register(FakeWorker worker) throws Exception {
        HttpResponse<String> response = postJson("/internal/workers/register",
                "{\"worker_id\":\"%s\",\"url\":\"%s\"}".formatted(worker.id(), worker.url()));
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    }

    private void beat() {
        for (FakeWorker worker : workers.values()) {
            if (worker.isRunning()) {
                try {
                    postJson("/internal/workers/heartbeat", "{\"worker_id\":\"%s\"}".formatted(worker.id()));
                } catch (Exception ignored) {
                    // the next beat is 300 ms away
                }
            }
        }
    }

    private static byte[] bytes(int length, long seed) {
        byte[] data = new byte[length];
        new Random(seed).nextBytes(data);
        return data;
    }

    private URI uri(String path) {
        return URI.create("http://localhost:" + port + path);
    }

    private HttpResponse<String> send(HttpRequest.Builder request) throws Exception {
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(String path) throws Exception {
        return send(HttpRequest.newBuilder(uri(path)).GET());
    }

    private HttpResponse<byte[]> download(String objectId) throws Exception {
        return http.send(HttpRequest.newBuilder(uri("/api/v1/objects/" + objectId)).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
    }

    private HttpResponse<String> delete(String objectId) throws Exception {
        return send(HttpRequest.newBuilder(uri("/api/v1/objects/" + objectId)).DELETE());
    }

    private HttpResponse<String> patch(String objectId, String body) throws Exception {
        return send(HttpRequest.newBuilder(uri("/api/v1/objects/" + objectId + "/metadata"))
                .header("Content-Type", "application/json")
                .method("PATCH", HttpRequest.BodyPublishers.ofString(body)));
    }

    private HttpResponse<String> postJson(String path, String body) throws Exception {
        return send(HttpRequest.newBuilder(uri(path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)));
    }

    private JsonNode uploadOk(String fileName, String contentType, byte[] data, String metadata) throws Exception {
        HttpResponse<String> response = upload(fileName, contentType, data, metadata);
        assertThat(response.statusCode()).as(response.body()).isEqualTo(201);
        return json.readTree(response.body());
    }

    /** A multipart/form-data POST, built by hand so the test controls every byte of it. */
    private HttpResponse<String> upload(String fileName, String contentType, byte[] data, String metadata)
            throws Exception {
        String boundary = "----test" + UUID.randomUUID().toString().replace("-", "");
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        if (metadata != null) {
            body.write(("--" + boundary + "\r\nContent-Disposition: form-data; name=\"metadata\"\r\n\r\n")
                    .getBytes(StandardCharsets.UTF_8));
            body.write(metadata.getBytes(StandardCharsets.UTF_8));
            body.write("\r\n".getBytes(StandardCharsets.UTF_8));
        }
        if (data != null) {
            body.write(("--" + boundary + "\r\nContent-Disposition: form-data; name=\"file\"; filename=\"" + fileName
                    + "\"\r\n" + (contentType == null ? "" : "Content-Type: " + contentType + "\r\n") + "\r\n")
                    .getBytes(StandardCharsets.UTF_8));
            body.write(data);
            body.write("\r\n".getBytes(StandardCharsets.UTF_8));
        }
        body.write(("--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));
        return send(HttpRequest.newBuilder(uri("/api/v1/objects"))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body.toByteArray())));
    }
}
