package com.objectstore.worker.reporting;

import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.Checksums;
import com.objectstore.worker.commands.CommandExecutor;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.storage.ChunkStore;
import com.objectstore.worker.storage.FailStop;
import com.objectstore.worker.storage.MutationGuard;
import com.objectstore.worker.storage.VersionFile;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.unit.DataSize;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * The worker's link against a stub controller over real HTTP (v5 §5.3): the registration fence is
 * adopted before anything; a resync of the same fence and attempt is ignored; a newer attempt or a
 * new fence starts a new listing; nothing above a listing's baseline leaves while it is in progress;
 * a change-log overflow or too many tombstones ask for a resync; a registration fence that went
 * backwards fail-stops the worker.
 */
class ControllerLinkTest {

    private static final String OBJECT = "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a";
    private static final long FLOOR = 1000;
    private final ObjectMapper json = new ObjectMapper();

    @TempDir
    Path dataDir;

    /** One heartbeat as the stub saw it. */
    record Seen(String reportId, long floor, int attempt, int index, long baseline, long maxChange, String resyncWanted) {
    }

    /** A minimal controller: registration with a fence, heartbeats acknowledged as told. */
    final class Stub {
        final HttpServer server;
        final List<Seen> seen = Collections.synchronizedList(new ArrayList<>());
        volatile boolean ackSegments;
        volatile long registerFloor = FLOOR;
        volatile String resync;              // e.g. {"t0":1000,"attempt":2}, sent with every reply while set
        volatile String ackStatus = "ok";
        long received;

        Stub() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/internal/workers/register", this::register);
            server.createContext("/internal/workers/heartbeat", this::heartbeat);
            server.start();
        }

        String url() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        private synchronized void register(HttpExchange exchange) throws IOException {
            JsonNode request = json.readTree(exchange.getRequestBody().readAllBytes());
            received = request.path("change_seq").asLong();
            reply(exchange, "{\"ok\":true,\"cluster_id\":\"cluster-1\",\"state\":\"ALIVE\",\"floor\":" + registerFloor
                    + ",\"attempt\":1}");
        }

        private synchronized void heartbeat(HttpExchange exchange) throws IOException {
            JsonNode request = json.readTree(exchange.getRequestBody().readAllBytes());
            long maxChange = 0;
            for (JsonNode change : request.path("changes")) {
                long seq = change.path("seq").asLong();
                maxChange = Math.max(maxChange, seq);
                if (seq == received + 1) {
                    received = seq;
                }
            }
            String wanted = request.path("resync_wanted").isMissingNode() || request.path("resync_wanted").isNull()
                    ? null : request.path("resync_wanted").asString();
            JsonNode segment = request.path("segment");
            String ack = "";
            if (!segment.isMissingNode() && !segment.isNull()) {
                seen.add(new Seen(segment.path("report_id").asString(), segment.path("floor").asLong(),
                        segment.path("attempt").asInt(), segment.path("index").asInt(), segment.path("baseline").asLong(),
                        maxChange, wanted));
                if (ackSegments) {
                    if (segment.path("last").asBoolean()) {
                        received = Math.max(received, segment.path("baseline").asLong());
                    }
                    ack = ",\"segment_ack\":{\"report_id\":\"" + segment.path("report_id").asString()
                            + "\",\"index\":" + segment.path("index").asInt() + ",\"status\":\"" + ackStatus + "\"}";
                }
            } else {
                seen.add(new Seen(null, 0, 0, -1, 0, maxChange, wanted));
            }
            reply(exchange, "{\"ok\":true,\"state\":\"ALIVE\",\"commands\":[]" + (resync == null ? "" : ",\"resync\":" + resync)
                    + ",\"changes_received_up_to\":" + received + ack + "}");
        }

        private void reply(HttpExchange exchange, String body) throws IOException {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        }

        List<String> reportIds() {
            synchronized (seen) {
                return seen.stream().map(Seen::reportId).filter(id -> id != null).distinct().toList();
            }
        }

        Seen first(String reportId) {
            synchronized (seen) {
                return seen.stream().filter(s -> reportId.equals(s.reportId())).findFirst().orElseThrow();
            }
        }
    }

    private Stub stub;
    private Thread linkThread;
    private ChunkStore store;
    private FailStop failStop;
    private int nextVersion = 1;

    private void start(int changeLogMax, int tombstoneMax) throws IOException {
        stub = new Stub();
        WorkerProperties properties = new WorkerProperties("worker1", dataDir, stub.url(), "http://127.0.0.1:1", 1_048_576,
                0.05, 1, 10, changeLogMax, 2000, 256, 1, 10, 1L << 30, 30, 1, 2, tombstoneMax, 500, 20, 3,
                DataSize.ofBytes(0), false, false);
        // Three chunks already on disk: start-up recovery indexes them at version 0.
        Files.createDirectories(properties.chunkDir());
        for (int i = 0; i < 3; i++) {
            byte[] data = ("chunk " + i).getBytes(StandardCharsets.US_ASCII);
            String id = "%s-%05d".formatted(OBJECT, i);
            Files.write(properties.chunkDir().resolve(id), data);
            Files.writeString(properties.chunkDir().resolve(id + ".meta"), Checksums.sha256Hex(data) + " " + data.length + "\n");
        }
        ChangeLog changes = new ChangeLog(properties);
        failStop = new FailStop();
        store = new ChunkStore(properties, changes, failStop, new MutationGuard(0));
        ReflectionTestUtils.invokeMethod(store, "initialise");
        VersionFile version = new VersionFile(properties, json);
        ReflectionTestUtils.invokeMethod(version, "load");
        ControllerClients clients = new ControllerClients(RestClient.builder(), properties);
        ControllerLink link = new ControllerLink(properties, new WorkerIdentity(), clients, store, changes, version,
                new CommandExecutor(store, changes, clients, new MutationGuard(0), properties), failStop,
                new MockEnvironment(), json);
        linkThread = Thread.ofPlatform().name("link").daemon(true).start(link);
    }

    /** A new copy at the next version above the fence (a change in the log). */
    private void write(int i) {
        byte[] data = ("chunk " + i).getBytes(StandardCharsets.US_ASCII);
        store.put("%s-%05d".formatted(OBJECT, i), FLOOR + 2L * nextVersion++, data, Checksums.sha256Hex(data));
    }

    @AfterEach
    void stop() {
        if (linkThread != null) {
            linkThread.interrupt();
        }
        if (stub != null) {
            stub.server.stop(0);
        }
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (!condition.getAsBoolean()) {
            assertThat(System.nanoTime()).as(what).isLessThan(deadline);
            Thread.sleep(10);
        }
    }

    @Test
    void theRegistrationFenceIsAdoptedAndARepeatedResyncOfTheSameAttemptIsIgnored() throws Exception {
        start(10_000, 100_000);
        stub.resync = "{\"t0\":" + FLOOR + ",\"attempt\":1}";      // repeated in every reply while the listing runs
        await("the registration listing runs", () -> stub.seen.stream().filter(s -> s.reportId() != null).count() >= 5);

        assertThat(store.isFenceSet()).isTrue();
        assertThat(store.fence()).isEqualTo(FLOOR);
        Seen opening = stub.first(stub.reportIds().get(0));
        assertThat(opening.floor()).isEqualTo(FLOOR);
        assertThat(opening.attempt()).isEqualTo(1);

        stub.resync = null;
        stub.ackSegments = true;
        await("the listing completes", () -> stub.seen.stream().anyMatch(s -> s.index() == 2));
        Thread.sleep(300);
        assertThat(stub.reportIds()).as("no second listing was started").hasSize(1);
    }

    @Test
    void edge34_anOverflowAsksForAResyncAndANewerAttemptListsAgainWhileNothingNewerLeaves() throws Exception {
        start(5, 100_000);
        await("the registration listing runs", () -> !stub.reportIds().isEmpty());
        String first = stub.reportIds().get(0);
        long firstBaseline = stub.first(first).baseline();

        for (int i = 3; i < 13; i++) {                 // 10 more changes than the log keeps
            write(i);
        }
        await("the worker asks for a resync", () -> stub.seen.stream().anyMatch(s -> "overflow".equals(s.resyncWanted())));
        stub.resync = "{\"t0\":" + FLOOR + ",\"attempt\":2}";     // the controller authorizes the next attempt
        await("a new listing starts", () -> stub.reportIds().size() == 2);
        stub.resync = null;
        String second = stub.reportIds().get(1);
        Seen opening = stub.first(second);

        assertThat(opening.attempt()).isEqualTo(2);
        assertThat(opening.index()).isZero();
        assertThat(opening.baseline()).as("a new baseline").isGreaterThanOrEqualTo(firstBaseline + 6);
        synchronized (stub.seen) {
            for (Seen s : stub.seen) {
                if (first.equals(s.reportId())) {
                    assertThat(s.maxChange()).as("hold-back during listing %s", first).isLessThanOrEqualTo(firstBaseline);
                }
            }
        }
        stub.ackSegments = true;
        await("the new listing completes and the held changes follow", () -> {
            synchronized (stub) {
                return stub.received == firstBaseline + 10;
            }
        });
    }

    @Test
    void aNewFenceFromAHeartbeatIsAdoptedAndClearsTheTombstonesItCovers() throws Exception {
        start(10_000, 100_000);
        stub.ackSegments = true;
        await("the registration listing completes", () -> stub.seen.stream().anyMatch(s -> s.index() == 2));
        store.delete("%s-%05d".formatted(OBJECT, 0), FLOOR + 2, false, "d");
        assertThat(store.tombstoneCount()).isEqualTo(1);

        stub.resync = "{\"t0\":" + (FLOOR + 100) + ",\"attempt\":1}";
        await("the new fence is adopted", () -> store.fence() == FLOOR + 100);
        stub.resync = null;

        assertThat(store.tombstoneCount()).isZero();
        await("a listing for the new fence", () -> stub.seen.stream().anyMatch(s -> s.floor() == FLOOR + 100));
    }

    @Test
    void tooManyTombstonesAskForAResync() throws Exception {
        start(10_000, 1);
        stub.ackSegments = true;
        await("the registration listing completes", () -> stub.seen.stream().anyMatch(s -> s.index() == 2));
        store.delete("%s-%05d".formatted(OBJECT, 0), FLOOR + 2, false, "d");
        store.delete("%s-%05d".formatted(OBJECT, 1), FLOOR + 4, false, "d");

        await("resync_wanted = tombstones", () -> stub.seen.stream().anyMatch(s -> "tombstones".equals(s.resyncWanted())));
    }

    @Test
    void aRegistrationFenceThatWentBackwardsFailStopsTheWorker() throws Exception {
        start(10_000, 100_000);
        await("registered", store::isFenceSet);
        stub.registerFloor = FLOOR - 10;                // a controller whose history went backwards
        stub.server.removeContext("/internal/workers/heartbeat");
        stub.server.createContext("/internal/workers/heartbeat", exchange -> {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            byte[] body = "{\"error\":\"unknown_worker\",\"detail\":\"register\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(404, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });

        await("the worker fail-stops", failStop::failed);
        assertThat(store.fence()).isEqualTo(FLOOR);
    }
}
