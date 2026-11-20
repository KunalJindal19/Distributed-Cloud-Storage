package com.objectstore.controller.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.dto.ReportEntry;
import com.objectstore.controller.cluster.ClusterState.WorkerState;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Block 13 (v4 §5.5, §5.11, §10 rows 4 and 6): DEAD as a queued, re-checked, stepped task. */
class MarkDeadTest {

    private final TestSupport t = new TestSupport();
    private final ClusterState cluster = t.cluster;

    private void silence(String workerId) {
        cluster.write(() -> cluster.worker(workerId).lastSeen = cluster.now() - 10_000_000_000L);
    }

    @Test
    void edge4_aHeartbeatBetweenQueuingAndApplyingMarkDeadCancelsIt() {
        t.knowChunk(TestSupport.chunk(0));
        t.join("worker1", TestSupport.chunk(0));
        silence("worker1");
        t.registry.healthPass();                       // queues MarkDead
        assertThat(cluster.read(() -> cluster.worker("worker1").deadQueued)).isTrue();

        // The worker speaks before the writer gets to it (no sync in between).
        t.registry.heartbeat(new com.objectstore.common.dto.HeartbeatRequest("worker1", t.fake("worker1").boot,
                ++t.fake("worker1").hbSeq, com.objectstore.common.InternalApi.nowSeconds(), TestSupport.STATS));
        t.sync();

        cluster.read(() -> {
            assertThat(cluster.worker("worker1").state).isEqualTo(WorkerState.ALIVE);
            assertThat(cluster.worker("worker1").deadQueued).isFalse();
            assertThat(cluster.chunksOn("worker1")).containsExactly(TestSupport.chunk(0));
        });
    }

    @Test
    void aSilentWorkerIsPurgedInStepsAndARegistrationQueuedDuringThePurgeRunsAfterIt() throws Exception {
        int copies = 5 * NamespaceTasks.STEP_ITEMS + 17;
        List<ReportEntry> entries = new ArrayList<>();
        for (int i = 0; i < copies; i++) {
            t.knowChunk(TestSupport.chunk(i));
            entries.add(new ReportEntry(TestSupport.chunk(i), 10, TestSupport.SUM, false));
        }
        t.register("worker1");
        t.report("worker1", entries);
        long epoch = cluster.read(() -> cluster.worker("worker1").epoch);
        silence("worker1");

        t.writer.start();
        try {
            t.registry.healthPass();                   // queues MarkDead (several steps)
            t.reRegister("worker1");                    // queued behind it; waits for its result

            cluster.read(() -> {
                ClusterState.WorkerInfo worker = cluster.worker("worker1");
                assertThat(worker.state).isEqualTo(WorkerState.ALIVE);
                assertThat(worker.epoch).as("DEAD, then registered").isEqualTo(epoch + 2);
                assertThat(worker.purging).isFalse();
                assertThat(cluster.chunksOn("worker1")).as("the purge finished first").isEmpty();
            });
            assertThat(cluster.counter("writer_task_failures")).isZero();
            t.writer.barrier(Duration.ofSeconds(10));
            t.assertNoViolations();
        } finally {
            t.writer.stop();
        }
    }

    @Test
    void edge32_theInvariantsHoldBetweenThePurgeSteps() {
        int copies = 3 * NamespaceTasks.STEP_ITEMS;
        List<ReportEntry> entries = new ArrayList<>();
        for (int i = 0; i < copies; i++) {
            t.knowChunk(TestSupport.chunk(i));
            entries.add(new ReportEntry(TestSupport.chunk(i), 10, TestSupport.SUM, false));
        }
        t.register("worker1");
        t.report("worker1", entries);
        long epoch = cluster.read(() -> cluster.worker("worker1").epoch);
        silence("worker1");
        Task markDead = t.registry.markDead("worker1", epoch);

        List<List<String>> violations = new ArrayList<>();
        Task.Step step;
        do {
            step = cluster.write(() -> markDead.body.run(System.nanoTime() + 1_000_000_000L));
            violations.add(cluster.read(cluster::checkInvariants));
        } while (step == Task.Step.MORE);

        assertThat(violations).hasSizeGreaterThan(1).allSatisfy(v -> assertThat(v).isEmpty());
        assertThat(cluster.read(() -> cluster.worker("worker1").purging)).isFalse();
    }
}
