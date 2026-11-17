package com.objectstore.controller.cluster;

import static com.objectstore.controller.cluster.TestSupport.chunk;
import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.controller.cluster.ClusterState.ReplicaState;
import com.objectstore.controller.cluster.ClusterState.WorkerState;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The monitor's planning decisions: repair, trim, invalidate, postpone, protect. */
class ReplicationPlannerTest {

    private final TestSupport t = new TestSupport();
    private final ClusterState cluster = t.cluster;

    private List<Command> queued(String workerId) {
        return cluster.locked(() -> List.copyOf(cluster.worker(workerId).commands));
    }

    @Test
    void anUnderReplicatedChunkIsCopiedWorkerToWorkerAndTheCopyIsTrackedUntilConfirmed() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        String boot2 = t.join("worker2");
        t.join("worker3");

        t.plan();

        Command replicate = queued("worker1").getFirst();
        assertThat(replicate.type()).isEqualTo(InternalApi.COMMAND_REPLICATE);
        assertThat(replicate.chunkId()).isEqualTo(chunk(0));
        assertThat(replicate.targetUrl()).isEqualTo("http://worker2:8001"); // least loaded, then lowest id
        assertThat(cluster.locked(() -> cluster.pendingOf(chunk(0)))).hasSize(1);

        t.plan(); // pending counts: nothing more is scheduled
        assertThat(cluster.locked(() -> cluster.pendingTotal())).isEqualTo(1);

        t.ibr("worker2", boot2, List.of(chunk(0)), List.of(), List.of()); // the target reports the receipt
        assertThat(cluster.locked(() -> cluster.pendingTotal())).isZero();
        assertThat(cluster.locked(() -> cluster.live(chunk(0)))).containsExactlyInAnyOrder("worker1", "worker2");
        assertThat(cluster.events()).anyMatch(e -> e.event().equals("copied"));
    }

    @Test
    void aSourceRunsAtMostMaxStreamsCopiesAtOnce() {
        for (int i = 0; i < 5; i++) {
            t.knowChunk(chunk(i));
        }
        t.join("worker1", chunk(0), chunk(1), chunk(2), chunk(3), chunk(4));
        t.join("worker2");
        t.join("worker3");

        t.plan();

        assertThat(cluster.locked(() -> cluster.pendingFrom("worker1"))).isEqualTo(2);
        assertThat(cluster.locked(() -> cluster.blockedLastTick())).isEqualTo(3);
    }

    @Test
    void aTimedOutCopyAvoidsThatTargetForAWhile() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2");
        t.join("worker3");
        t.plan();
        cluster.locked(() -> {
            cluster.removePending(chunk(0), "worker2");
            cluster.recordFailure(chunk(0), "worker2");
        });

        t.plan();

        assertThat(cluster.locked(() -> cluster.pendingOf(chunk(0)).getFirst().target())).isEqualTo("worker3");
    }

    @Test
    void surplusCopiesAreTrimmedOnlyWhenEveryHolderIsCurrent() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2", chunk(0));
        t.join("worker3", chunk(0));
        cluster.locked(() -> cluster.worker("worker3").state = WorkerState.STALE);

        t.plan();
        assertThat(cluster.locked(() -> cluster.live(chunk(0)))).hasSize(3); // postponed: a holder is STALE

        cluster.locked(() -> cluster.worker("worker3").state = WorkerState.ALIVE);
        t.plan();

        cluster.locked(() -> {
            assertThat(cluster.live(chunk(0))).hasSize(2);
            assertThat(cluster.replica(chunk(0), "worker3").state).isEqualTo(ReplicaState.EXCESS); // highest id on a tie
            assertThat(cluster.worker("worker3").commands).singleElement()
                    .satisfies(c -> assertThat(c.reason()).isEqualTo("excess"));
        });
    }

    @Test
    void anUndeliveredExcessDeleteIsCancelledWhenAnotherHolderDies() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2", chunk(0));
        t.join("worker3", chunk(0));
        t.plan(); // worker3's copy becomes EXCESS, DELETE queued but not delivered

        cluster.locked(() -> {
            cluster.worker("worker1").state = WorkerState.DEAD;
            cluster.removeWorkerReplicas("worker1");
        });

        cluster.locked(() -> {
            assertThat(cluster.replica(chunk(0), "worker3").state).isEqualTo(ReplicaState.OK);
            assertThat(cluster.worker("worker3").commands).isEmpty();
            assertThat(cluster.live(chunk(0))).containsExactlyInAnyOrder("worker2", "worker3");
        });
        t.assertNoViolations();
    }

    @Test
    void aDeliveredExcessDeleteIsNeverTakenBack() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2", chunk(0));
        t.join("worker3", chunk(0));
        t.join("worker4");
        t.plan();
        cluster.locked(() -> cluster.takeCommands(cluster.worker("worker3"))); // the DELETE is handed over

        cluster.locked(() -> {
            cluster.worker("worker1").state = WorkerState.DEAD;
            cluster.removeWorkerReplicas("worker1");
        });
        t.plan();

        cluster.locked(() -> {
            assertThat(cluster.replica(chunk(0), "worker3").state).isEqualTo(ReplicaState.EXCESS);
            assertThat(cluster.pendingOf(chunk(0))).singleElement()
                    .satisfies(p -> assertThat(p.target()).isEqualTo("worker4"));
        });
    }

    @Test
    void aCorruptCopyIsReplacedFirstAndDeletedOnlyAfterwards() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        String boot2 = t.join("worker2", chunk(0));
        String boot3 = t.join("worker3");
        t.ibr("worker2", boot2, List.of(), List.of(), List.of(chunk(0))); // the scanner found damage

        t.plan();
        cluster.locked(() -> {
            assertThat(cluster.worker("worker2").commands).isEmpty(); // not deleted while only 1 good copy
            assertThat(cluster.pendingOf(chunk(0))).singleElement()
                    .satisfies(p -> assertThat(p.target()).isEqualTo("worker3"));
        });

        t.ibr("worker3", boot3, List.of(chunk(0)), List.of(), List.of());
        t.plan();
        cluster.locked(() -> assertThat(cluster.worker("worker2").commands).singleElement()
                .satisfies(c -> assertThat(c.reason()).isEqualTo("corrupt")));

        t.ibr("worker2", boot2, List.of(), List.of(chunk(0)), List.of());
        assertThat(cluster.events()).anyMatch(e -> e.event().equals("invalidated"));
        assertThat(cluster.locked(() -> cluster.chunksOn("worker2"))).isEmpty();
    }

    @Test
    void aChunkWithoutAnyGoodCopyIsReportedOnceAndItsCorruptCopyIsKept() {
        t.knowChunk(chunk(0));
        String boot = t.join("worker1", chunk(0));
        t.join("worker2");
        t.ibr("worker1", boot, List.of(), List.of(), List.of(chunk(0)));

        t.plan();
        t.plan();
        t.plan();

        assertThat(cluster.events().stream().filter(e -> e.event().equals("unrecoverable"))).hasSize(1);
        assertThat(cluster.locked(() -> cluster.worker("worker1").commands)).isEmpty();
        assertThat(cluster.locked(() -> cluster.pendingTotal())).isZero();
    }

    @Test
    void withNoEligibleTargetTheChunkIsBlockedAndNothingSpins() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));

        t.plan();
        t.plan();

        assertThat(cluster.locked(() -> cluster.blockedLastTick())).isEqualTo(1);
        assertThat(cluster.locked(() -> cluster.queuedCommands())).isZero();
    }

    @Test
    void theRepairQueueStartsWithTheChunksThatHaveFewestCopies() {
        TestSupport single = new TestSupport();
        single.knowChunk(chunk(5));
        single.knowChunk(chunk(6));
        single.join("worker1", chunk(5));
        single.join("worker2", chunk(6));
        single.join("worker3");
        single.cluster.locked(() -> {
            single.cluster.replica(chunk(5), "worker1").state = ReplicaState.CORRUPT; // chunk 5: 0 live copies
        });
        single.plan();
        assertThat(single.cluster.events()).anyMatch(e -> e.event().equals("unrecoverable") && e.chunkId().equals(chunk(5)));
        assertThat(single.cluster.locked(() -> single.cluster.pendingOf(chunk(6)))).hasSize(1);
    }
}
