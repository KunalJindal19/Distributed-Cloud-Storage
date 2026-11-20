package com.objectstore.worker.reporting;

import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.Checksums;
import com.objectstore.worker.commands.CommandExecutor;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.storage.ChunkStore;
import com.objectstore.worker.storage.VersionFile;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
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
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Block 16 (v4 §4.3, §10 rows 1, 33, 34): the worker's link against a stub controller over real
 * HTTP. A report request during a report is ignored; a change-log overflow during a report abandons
 * it and starts a new one; nothing above a report's baseline leaves while it is in progress.
 */
class ControllerLinkTest {

    private static final String OBJECT = "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a";
    private final ObjectMapper json = new ObjectMapper();

    @TempDir
    Path dataDir;

    /** One heartbeat as the stub saw it. */
    record Seen(String reportId, int index, long baseline, long maxChange) {
    }

    /** A minimal controller: registration ok, heartbeats acknowledged as told. */
    final class Stub {
        final HttpServer server;
        final List<Seen> seen = Collections.synchronizedList(new ArrayList<>());
        volatile boolean ackSegments;
        volatile boolean requestFullReport;
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
            reply(exchange, "{\"ok\":true,\"cluster_id\":\"cluster-1\",\"state\":\"ALIVE\"}");
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
            JsonNode segment = request.path("segment");
            String ack = "";
            if (!segment.isMissingNode() && !segment.isNull()) {
                seen.add(new Seen(segment.path("report_id").asString(), segment.path("index").asInt(),
                        segment.path("baseline").asLong(), maxChange));
                if (ackSegments) {
                    if (segment.path("last").asBoolean()) {
                        received = Math.max(received, segment.path("baseline").asLong());
                    }
                    ack = ",\"segment_ack\":{\"report_id\":\"" + segment.path("report_id").asString()
                            + "\",\"index\":" + segment.path("index").asInt() + "}";
                }
            } else {
                seen.add(new Seen(null, -1, 0, maxChange));
            }
            reply(exchange, "{\"ok\":true,\"state\":\"ALIVE\",\"commands\":[],\"request_full_report\":" + requestFullReport
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
    }

    private Stub stub;
    private Thread linkThread;
    private ChunkStore store;

    private void start(int changeLogMax) throws IOException {
        stub = new Stub();
        WorkerProperties properties = new WorkerProperties("worker1", dataDir, stub.url(), "http://127.0.0.1:1", 1_048_576,
                0.05, 1, 3600, 10, changeLogMax, 2000, 256, 1, 10, 1L << 30, 30, 1, 2, false);
        ChangeLog changes = new ChangeLog(properties);
        store = new ChunkStore(properties, changes);
        ReflectionTestUtils.invokeMethod(store, "initialise");
        for (int i = 0; i < 3; i++) {
            write(i);
        }
        VersionFile version = new VersionFile(properties, json);
        ReflectionTestUtils.invokeMethod(version, "load");
        ControllerClients clients = new ControllerClients(RestClient.builder(), properties);
        ControllerLink link = new ControllerLink(properties, new WorkerIdentity(), clients, store, changes, version,
                new CommandExecutor(store, changes, clients), new MockEnvironment(), json);
        linkThread = Thread.ofPlatform().name("link").daemon(true).start(link);
    }

    private void write(int i) {
        byte[] data = ("chunk " + i).getBytes(StandardCharsets.US_ASCII);
        store.write("%s-%05d".formatted(OBJECT, i), data, Checksums.sha256Hex(data));
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
    void edge33_aReportRequestArrivingDuringAReportIsIgnored() throws Exception {
        start(10_000);
        stub.requestFullReport = true;                 // asked again and again while the report runs
        await("the registration report runs", () -> stub.seen.stream().filter(s -> s.reportId() != null).count() >= 5);
        stub.requestFullReport = false;
        stub.ackSegments = true;
        await("the report completes", () -> stub.seen.stream().anyMatch(s -> s.index() == 2));
        Thread.sleep(500);

        assertThat(stub.reportIds()).as("no second report was started").hasSize(1);
    }

    @Test
    void edge34_aChangeLogOverflowDuringAReportStartsANewReportAndNothingNewerLeaves() throws Exception {
        start(5);
        await("the registration report runs", () -> !stub.reportIds().isEmpty());
        String first = stub.reportIds().get(0);
        long firstBaseline = stub.seen.stream().filter(s -> first.equals(s.reportId())).findFirst().orElseThrow().baseline();

        for (int i = 3; i < 13; i++) {                 // 10 more changes than the log keeps
            write(i);
        }
        await("a new report starts", () -> stub.reportIds().size() == 2);
        String second = stub.reportIds().get(1);
        Seen opening = stub.seen.stream().filter(s -> second.equals(s.reportId())).findFirst().orElseThrow();

        assertThat(opening.index()).isZero();
        // The log overflowed at the 6th held change; the new report's baseline is at least there.
        assertThat(opening.baseline()).as("a new baseline").isGreaterThanOrEqualTo(firstBaseline + 6);
        synchronized (stub.seen) {
            for (Seen s : stub.seen) {
                if (first.equals(s.reportId())) {
                    assertThat(s.maxChange()).as("hold-back during report %s", first).isLessThanOrEqualTo(firstBaseline);
                }
            }
        }
        stub.ackSegments = true;
        await("the new report completes and the held changes follow", () -> {
            synchronized (stub) {
                return stub.received == firstBaseline + 10;
            }
        });
    }
}
