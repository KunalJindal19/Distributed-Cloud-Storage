package com.objectstore.systemtest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import tools.jackson.databind.JsonNode;

/** Block 7: the replication monitor and worker-to-worker copies. */
@ExtendWith(OutputCaptureExtension.class)
class ReplicationSystemTest extends ClusterTest {

    @Test
    void aDeadWorkersChunksAreCopiedWorkerToWorkerAndTrimmedWhenItReturns(CapturedOutput output) {
        byte[] data = Api.randomBytes(3 * MIB + 99, 1);
        String id = api.uploadOk("a.bin", data);
        int affected = (int) java.util.stream.IntStream.range(0, 4)
                .filter(i -> api.holders(id, i).contains("worker2")).count();
        assertThat(affected).isPositive();
        Watchdog watchdog = Watchdog.start(api, id);

        cluster.stopWorker("worker2");
        // STALE: copies still count, nothing is repaired yet.
        await().atMost(PATIENCE).pollInterval(Duration.ofMillis(20)).until(() -> api.workerState("worker2").equals("STALE"));
        assertThat(api.status().get("pending_replications").asInt()).isZero();
        assertThat(api.download(id).body()).isEqualTo(data);
        // DEAD: copies forgotten, worker-to-worker copies ordered, then confirmed.
        await().atMost(PATIENCE).until(() -> api.workerState("worker2").equals("DEAD"));
        await().atMost(PATIENCE).untilAsserted(() -> {
            JsonNode status = api.status();
            assertThat(status.get("under_replicated").asInt()).isZero();
            assertThat(status.get("pending_replications").asInt()).isZero();
        });
        assertThat(events("copied")).hasSize(affected);
        assertThat(api.download(id).body()).isEqualTo(data);
        // The worker's own log shows it copied the chunk; the controller never fetched it.
        assertThat(output.getOut()).contains("[cmd] replicate ok chunk=" + id);

        cluster.startWorker("worker2");
        await().atMost(PATIENCE).untilAsserted(() -> {
            JsonNode status = api.status();
            assertThat(status.get("over_replicated").asInt()).isZero();
            assertThat(status.get("under_replicated").asInt()).isZero();
            for (JsonNode chunk : api.metadata(id).get("chunks")) {
                assertThat(chunk.get("locations")).hasSize(2);
            }
        });
        assertThat(events("trimmed")).hasSize(affected);
        watchdog.stop();
        assertThat(watchdog.fewestOkCopies.get()).as("never fewer than one OK copy").isPositive();
        assertThat(api.download(id).body()).isEqualTo(data);
        awaitDisksMatchMap(id);
    }

    @Test
    void repairRespectsMaxStreamsPerSource() {
        byte[] data = Api.randomBytes(40 * MIB, 2);
        String id = api.uploadOk("big.bin", data);
        AtomicInteger worst = new AtomicInteger();
        AtomicBoolean stop = new AtomicBoolean();
        Thread poller = Thread.ofVirtual().start(() -> {
            while (!stop.get()) {
                for (JsonNode worker : api.status().get("workers")) {
                    worst.accumulateAndGet(worker.get("pending_as_source").asInt(), Math::max);
                }
            }
        });

        cluster.stopWorker("worker3");
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            JsonNode status = api.status();
            assertThat(status.get("workers").findValues("state").stream().map(JsonNode::asString)).contains("DEAD");
            assertThat(status.get("under_replicated").asInt()).isZero();
            assertThat(status.get("pending_replications").asInt()).isZero();
        });
        stop.set(true);

        assertThat(worst.get()).isBetween(1, 2);
        assertThat(api.download(id).body()).isEqualTo(data);
    }

    @Test
    void aCorruptCopyIsReplacedBeforeItIsDeleted() throws Exception {
        byte[] data = Api.randomBytes(2 * MIB + 5, 3);
        String id = api.uploadOk("c.bin", data);
        String chunkId = id + "-00000";
        List<String> holders = api.holders(id, 0);
        String bad = holders.get(0);
        Watchdog watchdog = Watchdog.start(api, id);
        Files.write(cluster.chunkFile(bad, chunkId), "XXXX".getBytes(), StandardOpenOption.WRITE);

        await().atMost(PATIENCE).until(() -> !cluster.chunkFilesOn(bad).contains(chunkId));
        // The worker's DELETED change reaches the controller with its next heartbeat.
        await().atMost(PATIENCE).until(() -> repairOrder(chunkId).contains("invalidated@" + bad));
        watchdog.stop();

        assertThat(repairOrder(chunkId)).as("copy confirmed before the corrupt copy is invalidated")
                .containsSubsequence("copied@" + newHolder(id, holders), "invalidated@" + bad);
        assertThat(watchdog.fewestOkCopies.get()).isPositive();
        await().atMost(PATIENCE).untilAsserted(() -> assertThat(api.metadata(id).get("replication_ok").asBoolean()).isTrue());
        assertThat(api.download(id).body()).isEqualTo(data);
    }

    private List<String> repairOrder(String chunkId) {
        List<String> order = new ArrayList<>();
        for (JsonNode event : api.status().get("repair_log")) {
            if (event.get("chunk_id").asString().equals(chunkId)) {
                order.add(event.get("event").asString() + "@" + event.path("target").asString(event.path("source").asString()));
            }
        }
        return order;
    }

    @Test
    void aChunkWhoseHoldersAreAllDownIsMissingUntilOneReturns() {
        byte[] data = Api.randomBytes(1000, 4);
        String id = api.uploadOk("d.bin", data);
        List<String> holders = api.holders(id, 0);
        holders.forEach(cluster::stopWorker);

        await().atMost(PATIENCE).until(() -> api.status().get("missing_chunks").asInt() >= 1);
        assertThat(api.download(id).statusCode()).isEqualTo(503);
        await().during(Duration.ofSeconds(1)).atMost(PATIENCE).until(() -> events("unrecoverable").size() == 1);

        cluster.startWorker(holders.get(0));
        await().atMost(PATIENCE).untilAsserted(() -> {
            assertThat(api.status().get("missing_chunks").asInt()).isZero();
            assertThat(api.metadata(id).get("replication_ok").asBoolean()).isTrue();
        });
        assertThat(api.download(id).body()).isEqualTo(data);
        assertThat(events("unrecoverable")).hasSize(1);
        cluster.startWorker(holders.get(1));
        awaitHealthy();
    }

    @Test
    void whenTheTargetOfACopyDiesAnotherTargetIsUsed() {
        byte[] data = Api.randomBytes(1000, 5);
        String id = api.uploadOk("e.bin", data);
        String lost = api.holders(id, 0).get(0);

        cluster.stopWorker(lost);
        AtomicReference<String> target = new AtomicReference<>();
        await().atMost(PATIENCE).pollInterval(Duration.ofMillis(10)).until(() -> {
            List<JsonNode> scheduled = events("replicate");
            if (scheduled.isEmpty()) {
                return false;
            }
            target.set(scheduled.getFirst().get("target").asString());
            return true;
        });
        cluster.stopWorker(target.get()); // right after the copy was ordered

        await().atMost(PATIENCE).untilAsserted(() -> {
            JsonNode chunk = api.metadata(id).get("chunks").get(0);
            List<String> okHolders = new ArrayList<>();
            chunk.get("locations").forEach(l -> {
                if (l.get("status").asString().equals("OK") && l.get("worker_status").asString().equals("ALIVE")) {
                    okHolders.add(l.get("worker_id").asString());
                }
            });
            assertThat(okHolders).hasSize(2).doesNotContain(lost, target.get());
        });
        assertThat(api.download(id).body()).isEqualTo(data);
    }

    // ---- helpers ------------------------------------------------------------------------------

    private List<JsonNode> events(String event) {
        List<JsonNode> matching = new ArrayList<>();
        for (JsonNode entry : api.status().get("repair_log")) {
            if (entry.get("event").asString().equals(event)) {
                matching.add(entry);
            }
        }
        return matching;
    }

    private String newHolder(String id, List<String> before) {
        for (JsonNode location : api.metadata(id).get("chunks").get(0).get("locations")) {
            String worker = location.get("worker_id").asString();
            if (!before.contains(worker)) {
                return worker;
            }
        }
        return "?";
    }

    /** Waits until each chunk's files on disk are exactly where the controller says they are. */
    private void awaitDisksMatchMap(String id) {
        await().atMost(PATIENCE).untilAsserted(() -> {
            for (JsonNode chunk : api.metadata(id).get("chunks")) {
                String chunkId = chunk.get("chunk_id").asString();
                List<String> mapped = new ArrayList<>();
                chunk.get("locations").forEach(l -> mapped.add(l.get("worker_id").asString()));
                List<String> onDisk = cluster.workerIds().stream().filter(w -> cluster.chunkFilesOn(w).contains(chunkId)).toList();
                assertThat(onDisk).containsExactlyInAnyOrderElementsOf(mapped);
            }
        });
    }

    /** Polls an object in the background and remembers the fewest OK copies any chunk had. */
    static final class Watchdog {
        final AtomicInteger fewestOkCopies = new AtomicInteger(Integer.MAX_VALUE);
        private final AtomicBoolean running = new AtomicBoolean(true);
        private Thread thread;

        static Watchdog start(Api api, String objectId) {
            Watchdog watchdog = new Watchdog();
            watchdog.thread = Thread.ofVirtual().start(() -> {
                while (watchdog.running.get()) {
                    for (JsonNode chunk : api.metadata(objectId).get("chunks")) {
                        int ok = 0;
                        for (JsonNode location : chunk.get("locations")) {
                            if (location.get("status").asString().equals("OK")) {
                                ok++;
                            }
                        }
                        watchdog.fewestOkCopies.accumulateAndGet(ok, Math::min);
                    }
                }
            });
            return watchdog;
        }

        void stop() {
            running.set(false);
            try {
                thread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @SuppressWarnings("unused")
    private static Path unused() {
        return null;
    }
}
