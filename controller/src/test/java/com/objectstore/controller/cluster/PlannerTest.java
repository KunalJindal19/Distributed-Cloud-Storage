package com.objectstore.controller.cluster;

import static com.objectstore.controller.cluster.TestSupport.chunk;
import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.dto.ReportEntry;
import com.objectstore.controller.cluster.ClusterState.WorkerState;
import com.objectstore.controller.replication.Monitor;
import com.objectstore.controller.replication.ReplicationPlanner;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Block 15 (v4 §7.3, §7.6, §10 rows 23-25, 41): the planner on the priority levels. */
class PlannerTest {

    private static void report(TestSupport t, String workerId, int from, int to) {
        List<ReportEntry> entries = new ArrayList<>();
        for (int i = from; i < to; i++) {
            entries.add(new ReportEntry(chunk(i), 10, TestSupport.SUM, false));
        }
        t.report(workerId, entries);
    }

    private static ReplicationPlanner.TickResult tick(TestSupport t) {
        return t.cluster.write(() -> t.planner.plan());
    }

    private static int pendingOf(TestSupport t, int chunk) {
        return t.cluster.read(() -> t.cluster.pendingOf(chunk(chunk)).size());
    }

    @Test
    void theMostUrgentLevelIsServedFirst() {
        TestSupport t = TestSupport.withRf(3, 1);                    // one stream per source
        t.knowChunk(chunk(0));                                       // (1,0): only on worker1
        t.knowChunk(chunk(1));                                       // (2,0): on worker1 and worker2
        t.register("worker1");
        report(t, "worker1", 0, 2);
        t.register("worker2");
        report(t, "worker2", 1, 2);
        for (String id : List.of("worker3", "worker4", "worker5")) {
            t.join(id);
        }
        t.cluster.write(() -> t.cluster.worker("worker2").state = WorkerState.STALE);   // worker1 is the only source

        tick(t);

        assertThat(pendingOf(t, 0)).as("(1,0) first").isEqualTo(1);
        assertThat(pendingOf(t, 1)).isZero();
        t.assertNoViolations();
    }

    @Test
    void edge23_tenThousandBlockedChunksDoNotStarveALowerPriorityLevelAndTheyRotate() {
        TestSupport t = TestSupport.withRf(3, 2);
        int blocked = 10_000;
        for (int i = 0; i < blocked + 10; i++) {
            t.knowChunk(chunk(i));
        }
        t.register("worker1");
        report(t, "worker1", 0, blocked);                           // (1,0), but worker1 will be STALE: no source
        t.register("worker2");
        report(t, "worker2", blocked, blocked + 10);
        t.register("worker3");
        report(t, "worker3", blocked, blocked + 10);                 // (2,0) on worker2 and worker3
        t.join("worker4");
        t.join("worker5");
        t.cluster.write(() -> t.cluster.worker("worker1").state = WorkerState.STALE);
        NeededReplication levels = t.cluster.levels();
        int oneZero = levels.levelFor(1, 0);
        String headBefore = t.cluster.read(() -> levels.head(oneZero));

        int scheduledLow = 0;
        for (int tick = 0; tick < 5 && scheduledLow == 0; tick++) {
            ReplicationPlanner.TickResult result = tick(t);
            assertThat(result.visited()).isLessThanOrEqualTo(t.properties.plannerVisitBudget());
            for (int i = blocked; i < blocked + 10; i++) {
                scheduledLow += pendingOf(t, i);
            }
        }

        assertThat(scheduledLow).as("(2,0) chunks scheduled despite 10,000 blocked (1,0) chunks").isPositive();
        String headAfter = t.cluster.read(() -> levels.head(oneZero));
        assertThat(headAfter).as("visited blocked chunks went to the tail").isNotEqualTo(headBefore);
        assertThat(t.cluster.blockedLastTick()).isPositive();
    }

    @Test
    void thePlannerStopsEarlyWhenNoStreamIsFree() {
        TestSupport t = TestSupport.withRf(2, 1);                    // one stream per worker, three workers
        for (int i = 0; i < 150; i++) {
            t.knowChunk(chunk(i));
        }
        for (int w = 1; w <= 3; w++) {                               // 50 chunks at (1,0) on each worker
            t.register("worker" + w);
            report(t, "worker" + w, (w - 1) * 50, w * 50);
        }

        ReplicationPlanner.TickResult result = tick(t);

        assertThat(result.freeTotal()).isEqualTo(3);
        assertThat(result.scheduled()).isEqualTo(3);
        // chunk 0 (worker1's stream), 49 blocked, chunk 50 (worker2's), 49 blocked, chunk 100 (the last
        // stream): then nothing is free and the remaining 49 chunks are not visited at all.
        assertThat(result.visited()).as("it stopped once no stream was free").isEqualTo(101);
    }

    @Test
    void aChunkCanGetSeveralCopiesInOneTick() {
        TestSupport t = TestSupport.withRf(3, 2);
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2");
        t.join("worker3");

        tick(t);

        assertThat(t.cluster.read(() -> t.cluster.pendingOf(chunk(0))))
                .extracting(ClusterState.PendingCopy::target).containsExactlyInAnyOrder("worker2", "worker3");
        t.assertNoViolations();
    }

    @Test
    void edge24_chunksWithoutALiveCopyAreNeverScheduled() {
        TestSupport t = TestSupport.withRf(2, 2);
        t.knowChunk(chunk(0));
        t.join("worker1");
        t.join("worker2");

        ReplicationPlanner.TickResult result = tick(t);

        assertThat(t.cluster.levels().levelOf(chunk(0))).isEqualTo(t.cluster.levels().levelFor(0, 0));
        assertThat(result.visited()).isZero();
        assertThat(pendingOf(t, 0)).isZero();
    }

    @Test
    void edge25_rfLargerThanTheWorkersBlocksWithOneWarningPerMinuteAndNoSpin() {
        TestSupport t = TestSupport.withRf(3, 2);
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2", chunk(0));
        Monitor monitor = new Monitor(t.cluster, new SafeMode(t.cluster, t.properties), t.planner, t.properties);

        for (int i = 0; i < 5; i++) {
            monitor.tick();
        }

        assertThat(monitor.lastTick().blocked()).isEqualTo(1);
        assertThat(t.cluster.counter("blocked_warnings")).isEqualTo(1);
        assertThat(pendingOf(t, 0)).isZero();
    }

    @Test
    void edge41_withRfOneNothingIsEverScheduledButSurplusCopiesAreTrimmed() {
        TestSupport t = TestSupport.withRf(1, 2);
        t.knowChunk(chunk(0));
        t.knowChunk(chunk(1));
        t.join("worker1", chunk(0), chunk(1));
        t.join("worker2", chunk(1));                                 // chunk 1 has a surplus copy

        tick(t);

        assertThat(t.cluster.levels().count()).isEqualTo(1);         // only (0,0)
        assertThat(pendingOf(t, 0)).isZero();
        assertThat(t.cluster.read(() -> t.cluster.live(chunk(1)))).hasSize(1);
        assertThat(t.cluster.read(() -> t.cluster.ledger().entriesFor(chunk(1)))).hasSize(1);
        t.assertNoViolations();
    }

    @Test
    void edge28_anUploadCopyAndARepairThatBothLandAreTrimmedBackToRf() {
        TestSupport t = new TestSupport();
        t.knowChunk(chunk(0));
        t.knowChunk(chunk(1));
        t.join("worker1");
        t.join("worker2", chunk(1));                                 // busier: the repair goes to worker3
        t.join("worker3");
        t.join("worker4", chunk(1));                                 // (chunk 1 is fully replicated)
        t.cluster.write(() -> {
            t.cluster.addUploadPending(chunk(0), "worker1");
            t.cluster.addUploadPending(chunk(0), "worker2");
        });
        t.ibr("worker1", t.fake("worker1").boot, List.of(chunk(0)), List.of(), List.of());   // worker1's RECEIVED
        // worker2's never arrives in time: the upload copy expires into normal repair.
        t.cluster.write(() -> t.cluster.expireDue(t.cluster.now() + t.properties.pendingTimeout().toNanos() + 1));
        tick(t);
        String repairTarget = t.cluster.read(() -> t.cluster.pendingOf(chunk(0)).get(0).target());
        assertThat(repairTarget).isEqualTo("worker3");

        // Both land: the late upload copy and the repair.
        t.ibr("worker2", t.fake("worker2").boot, List.of(chunk(0)), List.of(), List.of());
        t.ibr(repairTarget, t.fake(repairTarget).boot, List.of(chunk(0)), List.of(), List.of());
        tick(t);

        assertThat(t.cluster.read(() -> t.cluster.live(chunk(0)))).hasSize(2);
        assertThat(t.cluster.read(() -> t.cluster.ledger().entriesFor(chunk(0)))).hasSize(1);
        t.assertNoViolations();
    }

    @Test
    void edge38_aTargetRestartingBetweenItsPutAndItsReceivedOnlyCausesASurplusThatIsTrimmed() {
        TestSupport t = new TestSupport();
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2");
        t.join("worker3");
        tick(t);
        String target = t.cluster.read(() -> t.cluster.pendingOf(chunk(0)).get(0).target());

        t.register(target);                                        // a new process: its pending copy is dropped
        assertThat(pendingOf(t, 0)).isZero();
        tick(t);                                                   // repaired elsewhere meanwhile
        String other = t.cluster.read(() -> t.cluster.pendingOf(chunk(0)).get(0).target());
        assertThat(other).isNotEqualTo(target);

        t.fullReport(target, t.fake(target).boot, chunk(0));        // its report lists the file: an OK copy
        t.ibr(other, t.fake(other).boot, List.of(chunk(0)), List.of(), List.of());
        assertThat(t.cluster.read(() -> t.cluster.live(chunk(0)))).hasSize(3);
        tick(t);

        assertThat(t.cluster.read(() -> t.cluster.live(chunk(0)))).hasSize(2);
        t.assertNoViolations();
    }
}
