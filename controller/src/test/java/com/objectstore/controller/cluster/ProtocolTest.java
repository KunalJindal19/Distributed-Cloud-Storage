package com.objectstore.controller.cluster;

import static com.objectstore.controller.cluster.TestSupport.chunk;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.ReportEntry;
import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.dto.RegisterRequest;
import com.objectstore.common.web.ApiException;
import com.objectstore.controller.cluster.ClusterState.ReplicaState;
import com.objectstore.controller.cluster.ClusterState.WorkerState;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The acceptance rules for worker messages, registration cases and liveness transitions. */
class ProtocolTest {

    private final TestSupport t = new TestSupport();
    private final ClusterState cluster = t.cluster;

    @Test
    void aMessageFromAnUnknownWorkerIsAnsweredWithUnknownWorker() {
        assertCode(() -> t.heartbeat("worker1", "boot", 0), 404, "unknown_worker");
    }

    @Test
    void heartbeatsFromAnotherProcessOrSentLongAgoAreRejectedWithoutEffect() {
        t.knowChunk(chunk(0));
        String boot = t.join("worker1");
        java.util.List<com.objectstore.common.dto.Change> changes = java.util.List.of(
                new com.objectstore.common.dto.Change(1, com.objectstore.common.dto.ChangeType.RECEIVED, chunk(0), 10L,
                        TestSupport.SUM, null, null, null));

        assertCode(() -> t.registry.heartbeat(new HeartbeatRequest("worker1", "other-boot", 5, InternalApi.nowSeconds(),
                TestSupport.STATS, changes, null)), 409, "stale_boot");
        assertCode(() -> t.registry.heartbeat(new HeartbeatRequest("worker1", boot, 5, InternalApi.nowSeconds() - 20,
                TestSupport.STATS, changes, null)), 409, "too_old");

        cluster.locked(() -> {
            assertThat(cluster.chunksOn("worker1")).isEmpty();
            assertThat(cluster.worker("worker1").received).isZero();
        });
    }

    @Test
    void aDuplicateHeartbeatDeliversNoCommands() {
        t.knowChunk(chunk(0));
        String boot = t.join("worker1", chunk(0));
        cluster.locked(() -> cluster.queueCommand("worker1", InternalApi.COMMAND_DELETE, chunk(0), null, null, "orphan"));

        HeartbeatResponse first = t.registry.heartbeat(new HeartbeatRequest("worker1", boot, 3, InternalApi.nowSeconds(), TestSupport.STATS));
        cluster.locked(() -> cluster.queueCommand("worker1", InternalApi.COMMAND_DELETE, chunk(0), null, null, "orphan"));
        HeartbeatResponse duplicate = t.registry.heartbeat(new HeartbeatRequest("worker1", boot, 3, InternalApi.nowSeconds(), TestSupport.STATS));

        assertThat(first.commands()).hasSize(1);
        assertThat(duplicate.ignored()).isEqualTo("duplicate");
        assertThat(duplicate.commands()).isEmpty();
        assertThat(cluster.locked(() -> cluster.worker("worker1").commands)).hasSize(1);
    }

    @Test
    void aRestartedProcessKeepsItsCopiesButLosesItsQueueAndItsOutgoingCopies() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2");
        cluster.locked(() -> {
            cluster.queueCommand("worker1", InternalApi.COMMAND_REPLICATE, chunk(0), TestSupport.SUM, "http://worker2:8001", null);
            cluster.addPending(chunk(0), "worker1", "worker2", "cmd");
        });

        t.register("worker1"); // a new boot id

        cluster.locked(() -> {
            assertThat(cluster.worker("worker1").commands).isEmpty();
            assertThat(cluster.pendingTotal()).isZero();
            assertThat(cluster.worker("worker1").hasReported).isFalse();
            assertThat(cluster.replica(chunk(0), "worker1")).isNotNull();
        });
    }

    @Test
    void aWorkerFromAnotherClusterIsRefused() {
        assertCode(() -> t.registry.register(new RegisterRequest("worker1", "http://w:1", "boot", "other-cluster",
                1, 0, -1, InternalApi.nowSeconds(), TestSupport.STATS)), 409, "cluster_id_mismatch");
        assertThat(cluster.locked(() -> cluster.workers())).isEmpty();
    }

    @Test
    void threeBootIdsInAMinuteLookLikeADuplicateWorkerId() {
        t.register("worker1");
        t.register("worker1");
        t.register("worker1");
        assertCode(() -> t.register("worker1"), 409, "duplicate_worker");
    }

    @Test
    void silenceMakesAWorkerStaleThenDeadAndDeadForgetsItsCopies() throws InterruptedException {
        t.knowChunk(chunk(0));
        String boot = t.join("worker1", chunk(0));
        t.registry.healthPass();

        tickHealth(250); // silent for > STALE_AFTER (0.2 s), with the health loop running normally
        assertThat(cluster.locked(() -> cluster.worker("worker1").state)).isEqualTo(WorkerState.STALE);
        assertThat(cluster.locked(() -> cluster.live(chunk(0)))).containsExactly("worker1"); // still counts

        t.send("worker1", List.of(), null);
        assertThat(cluster.locked(() -> cluster.worker("worker1").state)).isEqualTo(WorkerState.ALIVE);

        tickHealth(500); // silent for > DEAD_AFTER (0.4 s)
        assertThat(cluster.locked(() -> cluster.worker("worker1").state)).isEqualTo(WorkerState.DEAD);
        assertThat(cluster.locked(() -> cluster.replicasOf(chunk(0)))).isEmpty();
        assertCode(() -> t.send("worker1", List.of(), null), 404, "unknown_worker");
        assertThat(cluster.locked(cluster::checkInvariants)).isEmpty();
    }

    @Test
    void aControllerStallDoesNotKillTheCluster() throws InterruptedException {
        t.join("worker1");
        t.join("worker2");
        t.registry.healthPass();

        Thread.sleep(600); // the controller "freezes": no health pass for longer than DEAD_AFTER

        t.registry.healthPass();
        cluster.locked(() -> cluster.workers().values()
                .forEach(worker -> assertThat(worker.state).isEqualTo(WorkerState.ALIVE)));
    }

    @Test
    void aFullReportReconcilesTheBlockMap() {
        t.knowChunk(chunk(0));
        t.knowChunk(chunk(1));
        t.knowChunk(chunk(2));
        String boot = t.join("worker1", chunk(0), chunk(1));

        // chunk 1 is gone from the disk; chunk 2 appeared; chunk 0 now has a wrong checksum.
        t.report("worker1", List.of(
                new ReportEntry(chunk(0), 10, "b".repeat(64), false),
                new ReportEntry(chunk(2), 10, TestSupport.SUM, false)));

        cluster.locked(() -> {
            assertThat(cluster.chunksOn("worker1")).containsExactlyInAnyOrder(chunk(0), chunk(2));
            assertThat(cluster.replica(chunk(0), "worker1").state).isEqualTo(ReplicaState.CORRUPT);
            assertThat(cluster.worker("worker1").hasReported).isTrue();
            assertThat(cluster.worker("worker1").session).isNull();
        });
        t.assertNoViolations();
    }

    @Test
    void edge19_aReportEntryMarkedBadMakesTheCopyCorrupt() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));

        t.report("worker1", List.of(new ReportEntry(chunk(0), 10, TestSupport.SUM, true)));

        assertThat(cluster.locked(() -> cluster.replica(chunk(0), "worker1").state)).isEqualTo(ReplicaState.CORRUPT);
    }

    @Test
    void orphansAreOnlyCollectedAfterTheDelayAndNotWhenTooManyAtOnce() throws InterruptedException {
        TestSupport delayed = new TestSupport(TestSupport.properties(5, 10, 0.3));
        String boot = delayed.join("worker1", chunk(7));
        assertThat(delayed.cluster.locked(() -> delayed.cluster.worker("worker1").commands)).isEmpty();

        Thread.sleep(350);
        // Completion queues the DELETE; the same heartbeat's reply hands it over.
        assertThat(delayed.fullReport("worker1", boot, chunk(7)).commands())
                .singleElement().satisfies(c -> assertThat(c.reason()).isEqualTo("orphan"));

        String[] many = java.util.stream.IntStream.range(10, 30).mapToObj(TestSupport::chunk).toArray(String[]::new);
        delayed.fullReport("worker1", boot, many);
        assertThat(delayed.cluster.counter("orphan_gc_refused")).isEqualTo(1);
    }

    @Test
    void chunksOfFailedUploadsAndDeletedObjectsAreCollectedEvenInBulk() throws InterruptedException {
        TestSupport delayed = new TestSupport(TestSupport.properties(5, 10, 0.1));
        Thread.sleep(150);
        String failedUpload = "aaaaaaaa-7d0e-4c1b-9a54-1f2e3d4c5b6a";
        String unknown = "bbbbbbbb-7d0e-4c1b-9a54-1f2e3d4c5b6a";
        delayed.cluster.locked(() -> delayed.cluster.addGarbage(failedUpload));
        String[] reported = java.util.stream.Stream.concat(
                        java.util.stream.IntStream.range(0, 10).mapToObj(i -> "%s-%05d".formatted(failedUpload, i)),
                        java.util.stream.IntStream.range(0, 20).mapToObj(i -> "%s-%05d".formatted(unknown, i)))
                .toArray(String[]::new);

        delayed.register("worker1");
        HeartbeatResponse reply = delayed.fullReport("worker1", delayed.fake("worker1").boot, reported);

        // All 10 known-garbage chunks are deleted; the 20 unaccountable ones (2/3 > 1/2) trip the valve.
        assertThat(reply.commands())
                .hasSize(10)
                .allSatisfy(c -> assertThat(c.chunkId()).startsWith(failedUpload));
        assertThat(delayed.cluster.counter("orphan_gc_refused")).isEqualTo(1);
    }

    @Test
    void changesAddDeleteAndFlagCopies() {
        t.knowChunk(chunk(0));
        t.knowChunk(chunk(1));
        String boot = t.join("worker1", chunk(0));

        t.ibr("worker1", boot, List.of(chunk(1)), List.of(chunk(0)), List.of());
        cluster.locked(() -> assertThat(cluster.chunksOn("worker1")).containsExactly(chunk(1)));

        t.ibr("worker1", boot, List.of(), List.of(), List.of(chunk(1)));
        cluster.locked(() -> assertThat(cluster.replica(chunk(1), "worker1").state).isEqualTo(ReplicaState.CORRUPT));
    }

    /** Runs health passes every 40 ms (well under STALE_AFTER / 2, so no stall is detected). */
    private void tickHealth(long millis) throws InterruptedException {
        long end = System.nanoTime() + millis * 1_000_000;
        while (System.nanoTime() < end) {
            Thread.sleep(40);
            t.registry.healthPass();
        }
    }

    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable call, int status, String code) {
        assertThatThrownBy(call).isInstanceOfSatisfying(ApiException.class, e -> {
            assertThat(e.status().value()).isEqualTo(status);
            assertThat(e.code()).isEqualTo(code);
        });
    }
}
