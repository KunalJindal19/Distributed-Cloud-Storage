package com.objectstore.controller.cluster;

import static com.objectstore.controller.cluster.TestSupport.chunk;
import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.dto.ReportSegment;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.PendingOps.Op;
import com.objectstore.controller.cluster.PendingOps.Reason;
import com.objectstore.controller.config.ControllerProperties;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Fenced resync on the controller (v5 §5.2–§5.8, §6.4). */
class ResyncTest {

    private final TestSupport t = new TestSupport();
    private final ClusterState cluster = t.cluster;

    private WorkerInfo worker(String id) {
        return cluster.worker(id);
    }

    @Test
    void aRegistrationStartsAResyncWithAFreshFenceAboveEveryEarlierOp() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        Op before = cluster.write(() -> cluster.ops().ensure(chunk(0), "worker1", Reason.OBJECT_DELETED));

        t.register("worker1");

        cluster.read(() -> {
            assertThat(worker("worker1").resyncing).isTrue();
            assertThat(worker("worker1").fenceFloor).isGreaterThan(before.version());
            assertThat(worker("worker1").attempt).isEqualTo(1);
        });
        assertThat(t.fake("worker1").fence).isEqualTo(cluster.read(() -> worker("worker1").fenceFloor));
    }

    @Test
    void anOpTargetingAResyncingWorkerIsNotHandedOutAndAnOpAtOrBelowTheFenceIsDroppedAtCompletion() {
        t.knowChunk(chunk(0));
        t.knowChunk(chunk(1));
        t.join("worker1", chunk(0), chunk(1));
        Op old = cluster.write(() -> cluster.ops().ensure(chunk(0), "worker1", Reason.OBJECT_DELETED));
        t.send("worker1", List.of(), null, InternalApi.RESYNC_TOMBSTONES);         // a resync starts
        Op fresh = cluster.write(() -> cluster.ops().ensure(chunk(1), "worker1", Reason.OBJECT_DELETED));
        assertThat(fresh.version()).isGreaterThan(cluster.read(() -> worker("worker1").fenceFloor));
        assertThat(old.version()).isLessThan(cluster.read(() -> worker("worker1").fenceFloor));

        assertThat(t.handOut("worker1")).as("frozen while resyncing").isEmpty();

        t.report("worker1", List.of(TestSupport.entry(chunk(0), 0), TestSupport.entry(chunk(1), 0)));

        assertThat(t.op(chunk(0), "worker1")).as("at or below the fence: dropped").isNull();
        assertThat(t.op(chunk(1), "worker1")).as("above the fence: kept").isSameAs(fresh);
        assertThat(t.handOut("worker1")).extracting(Command::chunkId).containsExactly(chunk(1));
        t.assertNoViolations();
    }

    @Test
    void aPendingRepairTargetingAResyncingWorkerLeavesPSoTheRepairCanGoElsewhere() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2");
        t.join("worker3");
        t.plan();
        String target = cluster.read(() -> cluster.ops().opsFor(chunk(0)).getFirst().workerId);

        t.send(target, List.of(), null, InternalApi.RESYNC_TOMBSTONES);
        assertThat(cluster.read(() -> cluster.levels().levelOf(chunk(0)))).isEqualTo(cluster.levels().levelFor(1, 0));
        assertThat(t.handOut("worker1")).as("the REPLICATE to the resyncing target stays queued").isEmpty();
        t.plan();

        assertThat(cluster.read(() -> cluster.ops().opsFor(chunk(0)))).hasSize(2);
    }

    @Test
    void aPostMQuarantineIsReplayedAfterCompletionAndTheViewConverges() {
        t.knowChunk(chunk(0));
        t.knowChunk(chunk(1));
        t.join("worker1", chunk(0), chunk(1));
        t.send("worker1", List.of(), null, InternalApi.RESYNC_TOMBSTONES);
        // The listing (taken after M) still shows chunk 1; the scanner then quarantined it: seq M + 1, held back.
        ReportSegment listing = t.segment("worker1", List.of(TestSupport.entry(chunk(0), 0), TestSupport.entry(chunk(1), 0)));
        t.send("worker1", List.of(), listing);
        assertThat(t.holders(chunk(1))).as("the sweep keeps it").contains("worker1");

        t.ibr("worker1", t.deleted("worker1", chunk(1), t.local("worker1"), "corrupt"));   // the held-back change

        assertThat(t.holders(chunk(1))).isEmpty();
        assertThat(t.holders(chunk(0))).containsExactly("worker1");
        t.assertNoViolations();
    }

    @Test
    void aResyncWithoutSegmentProgressGetsANewAttempt() throws Exception {
        TestSupport stalled = new TestSupport(new ControllerProperties(1_048_576, 2, Path.of("/tmp/unused.db"), 5, 10,
                0.5, 0.1, 0.3, 3, 2, 1000, 10, 0.999, 0.1, 5, 0, 0.5, 16 * 1024 * 1024, 2, 10, true, 5, 10_000, 5, 2,
                2000, 2000, 1000, 1000, 2, 240, 5, 3600, 0.1, 1, 0.2, 3600, 60));          // RESYNC_STALL_TIMEOUT 0.2 s
        stalled.join("worker1");
        stalled.send("worker1", List.of(), null, InternalApi.RESYNC_TOMBSTONES);
        Thread.sleep(300);

        stalled.registry.healthPass();

        assertThat(stalled.cluster.read(() -> stalled.cluster.worker("worker1").attempt)).isEqualTo(2);
        HeartbeatResponse reply = stalled.send("worker1", List.of(), null);
        assertThat(reply.resync().attempt()).isEqualTo(2);
    }

    @Test
    void aWorkerThatGoesDeadDuringItsResyncEndsItAndLosesItsCredit() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.send("worker1", List.of(), null, InternalApi.RESYNC_TOMBSTONES);
        long epoch = cluster.read(() -> worker("worker1").verifyEpoch);
        cluster.write(() -> worker("worker1").lastSeen = cluster.now() - (long) 1e10);

        cluster.write(() -> t.registry.markDead("worker1", worker("worker1").epoch).body.run(Long.MAX_VALUE));

        cluster.read(() -> {
            assertThat(worker("worker1").resyncing).isFalse();
            assertThat(worker("worker1").verifyEpoch).isGreaterThan(epoch);
            assertThat(cluster.replicasOf(chunk(0))).isEmpty();
        });
    }

    @Test
    void theResyncExampleAQuarantinedListedCopyCanNeverBeASurvivorOfAnExcessDelete() {
        // v5 §6.4: W's copy had fresh credit; a periodic resync starts and lists it; the scanner then
        // quarantines it (held back, > M); completion ends the resync before that DELETED is applied.
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2", chunk(0));
        t.join("worker3", chunk(0));
        cluster.write(() -> {
            for (String w : List.of("worker1", "worker2", "worker3")) {
                Replica replica = cluster.replica(chunk(0), w);
                replica.verifiedEpoch = worker(w).verifyEpoch;
                replica.verifiedAt = cluster.now();
            }
        });
        cluster.write(() -> cluster.startResync(worker("worker1"), "periodic", true));   // verifyEpoch++
        t.handOut("worker1");                                                            // the fake adopts the fence
        t.report("worker1", List.of(TestSupport.entry(chunk(0), 0)));
        assertThat(cluster.read(() -> worker("worker1").resyncing)).isFalse();
        assertThat(cluster.read(() -> cluster.freshForDelete(cluster.replica(chunk(0), "worker1"), worker("worker1"))))
                .as("its credit was voided at resync start").isFalse();

        t.plan();

        cluster.read(() -> {
            for (Op op : cluster.ops().opsFor(chunk(0))) {
                assertThat(op.survivors()).extracting(PendingOps.Survivor::workerId).doesNotContain("worker1");
            }
        });
        // The held-back quarantine arrives: the copy is gone, and nothing relied on it.
        t.ibr("worker1", t.deleted("worker1", chunk(0), t.local("worker1"), "corrupt"));
        assertThat(t.holders(chunk(0))).containsExactlyInAnyOrder("worker2", "worker3");
        t.assertNoViolations();
    }
}
