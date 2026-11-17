package com.objectstore.systemtest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

/** Block 6: protocol, liveness, block reports, safe mode, cluster identity, scanner, orphan GC. */
class ProtocolSystemTest extends ClusterTest {

    @Test
    void theClusterFormsAndEveryVolumeRecordsTheClusterId() throws Exception {
        JsonNode status = api.status();
        String clusterId = status.get("cluster_id").asString();
        assertThat(status.get("workers")).hasSize(4);
        for (String workerId : cluster.workerIds()) {
            String version = Files.readString(cluster.dataDir(workerId).resolve("VERSION"));
            assertThat(version).contains("\"cluster_id\":\"" + clusterId + "\"", "\"worker_id\":\"" + workerId + "\"");
        }
    }

    @Test
    void aStoppedWorkerGoesStaleThenDeadAndRejoinsWithItsData() {
        String id = api.uploadOk("a.bin", Api.randomBytes(3 * MIB, 1));
        int copiesOnWorker2 = api.worker("worker2").get("chunk_count").asInt();
        assertThat(copiesOnWorker2).isPositive();

        cluster.stopWorker("worker2");
        await().atMost(PATIENCE).pollInterval(Duration.ofMillis(50)).until(() -> api.workerState("worker2").equals("STALE"));
        await().atMost(PATIENCE).pollInterval(Duration.ofMillis(50)).until(() -> api.workerState("worker2").equals("DEAD"));
        assertThat(api.worker("worker2").get("chunk_count").asInt()).as("replicas forgotten at DEAD").isZero();
        assertThat(api.download(id).statusCode()).isEqualTo(200);

        cluster.startWorker("worker2");
        await().atMost(PATIENCE).untilAsserted(() -> {
            JsonNode worker = api.worker("worker2");
            assertThat(worker.get("state").asString()).isEqualTo("ALIVE");
            assertThat(worker.get("has_reported").asBoolean()).isTrue();
        });
    }

    @Test
    void aQuickRestartIsNeverDeclaredDead() {
        api.uploadOk("b.bin", Api.randomBytes(2 * MIB, 2));
        List<String> states = new ArrayList<>();

        cluster.stopWorker("worker3");
        cluster.startWorker("worker3"); // a new process (new boot id) well within DEAD_AFTER
        long end = System.nanoTime() + Duration.ofSeconds(3).toNanos();
        while (System.nanoTime() < end) {
            states.add(api.workerState("worker3"));
        }

        assertThat(states).doesNotContain("DEAD");
        awaitHealthy();
        assertThat(api.status().get("under_replicated").asInt()).isZero();
    }

    @Test
    void replayedReorderedAndForeignReportsChangeNothing() {
        // A fake worker speaking the protocol over raw HTTP.
        String boot = UUID.randomUUID().toString();
        double now = System.currentTimeMillis() / 1000.0;
        HttpResponse<String> register = api.postJson("/internal/workers/register",
                "{\"worker_id\":\"worker9\",\"url\":\"http://127.0.0.1:1\",\"boot_id\":\"%s\",\"cluster_id\":null,\"hb_seq\":-1,\"report_seq\":-1,\"sent_at\":%f}"
                        .formatted(boot, now));
        assertThat(register.statusCode()).isEqualTo(200);
        assertThat(api.json(register.body()).get("cluster_id").asString()).isEqualTo(api.status().get("cluster_id").asString());

        assertThat(report(boot, 3, now).statusCode()).isEqualTo(200);
        HttpResponse<String> duplicate = report(boot, 3, now);
        assertThat(duplicate.statusCode()).isEqualTo(200);
        assertThat(duplicate.body()).contains("\"ignored\":\"duplicate\"");
        assertError(report(boot, 2, now), 409, "stale_seq");
        assertError(report("another-boot", 4, now), 409, "stale_boot");
        assertError(report(boot, 4, now - 20), 409, "too_old");
        assertError(api.postJson("/internal/workers/heartbeat",
                "{\"worker_id\":\"nobody\",\"boot_id\":\"x\",\"seq\":0,\"sent_at\":%f}".formatted(now)), 404, "unknown_worker");
        assertThat(api.status().get("counters").get("stale_messages").asLong()).isGreaterThanOrEqualTo(2);
    }

    @Test
    void theScannerFindsACorruptCopyWhichIsKeptAndNeverServed() throws Exception {
        byte[] data = Api.randomBytes(2 * MIB + 5, 3);
        String id = api.uploadOk("c.bin", data);
        String chunkId = id + "-00000";
        String holder = api.holders(id, 0).get(0);
        Path file = cluster.chunkFile(holder, chunkId);
        Files.write(file, "XXXX".getBytes(), StandardOpenOption.WRITE);

        await().atMost(PATIENCE).untilAsserted(() -> assertThat(locationState(id, 0, holder)).isEqualTo("CORRUPT"));
        assertThat(Files.exists(file)).as("the corrupt file is kept").isTrue();
        assertThat(api.download(id).body()).isEqualTo(data);
    }

    @Test
    void anOrphanIsCollectedButNotBeforeTheDelayAndNormalUploadsLeaveNone() throws Exception {
        for (int i = 0; i < 20; i++) {
            api.uploadOk("n" + i + ".bin", Api.randomBytes(1000 + i, 100 + i));
        }
        String orphan = UUID.randomUUID() + "-00000";
        Path file = cluster.chunkFile("worker4", orphan);
        byte[] junk = Api.randomBytes(100, 4);
        Files.write(file, junk);
        Files.writeString(file.resolveSibling(orphan + ".meta"),
                com.objectstore.common.Checksums.sha256Hex(junk) + " 100\n");

        await().atMost(PATIENCE).until(() -> !Files.exists(file));
        assertThat(api.status().get("repair_log").findValues("event").stream()
                .filter(e -> e.asString().equals("orphan")).count()).as("orphan deletions").isEqualTo(1);
    }

    @Test
    void aRestartedControllerStartsInSafeModeAndLeavesItOnceWorkersReport() {
        byte[] data = Api.randomBytes(MIB + 77, 5);
        String id = api.uploadOk("d.bin", data);

        cluster.stopController();
        cluster.startController();

        JsonNode status = api.status();
        assertThat(status.get("safe_mode").asBoolean()).isTrue();
        HttpResponse<String> refused = api.upload("e.bin", new byte[10], null, null);
        assertError(refused, 503, "safe_mode");
        assertThat(refused.headers().firstValue("Retry-After")).contains("5");
        assertError(api.delete(id), 503, "safe_mode");

        await().atMost(PATIENCE).until(() -> api.download(id).statusCode() == 200);
        awaitHealthy();
        assertThat(api.download(id).body()).isEqualTo(data);
        assertThat(api.status().get("under_replicated").asInt()).isZero();
    }

    @Test
    void workersRefuseAControllerWithAWipedDatabaseAndKeepTheirData() {
        api.uploadOk("f.bin", Api.randomBytes(3 * MIB, 6));
        List<String> before = new ArrayList<>();
        cluster.workerIds().forEach(w -> before.addAll(cluster.chunkFilesOn(w)));
        assertThat(before).hasSize(6);

        cluster.stopController();
        cluster.wipeControllerDatabase();
        cluster.startController();

        await().during(Duration.ofSeconds(3)).atMost(PATIENCE).untilAsserted(
                () -> assertThat(api.status().get("workers")).isEmpty());
        List<String> after = new ArrayList<>();
        cluster.workerIds().forEach(w -> after.addAll(cluster.chunkFilesOn(w)));
        assertThat(after).containsExactlyInAnyOrderElementsOf(before);
        api = null; // the base class's invariant check would query an empty cluster: nothing to check
    }

    private HttpResponse<String> report(String boot, long seq, double sentAt) {
        return api.postJson("/internal/workers/blockreport",
                "{\"worker_id\":\"worker9\",\"boot_id\":\"%s\",\"seq\":%d,\"sent_at\":%f,\"chunks\":[]}".formatted(boot, seq, sentAt));
    }

    private String locationState(String objectId, int index, String workerId) {
        for (JsonNode location : api.metadata(objectId).get("chunks").get(index).get("locations")) {
            if (location.get("worker_id").asString().equals(workerId)) {
                return location.get("status").asString();
            }
        }
        return "ABSENT";
    }

    private void assertError(HttpResponse<String> response, int status, String code) {
        assertThat(response.statusCode()).as(response.body()).isEqualTo(status);
        assertThat(api.json(response.body()).get("error").asString()).isEqualTo(code);
    }
}
