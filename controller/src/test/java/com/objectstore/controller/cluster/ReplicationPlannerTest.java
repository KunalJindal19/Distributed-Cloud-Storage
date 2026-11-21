package com.objectstore.controller.cluster;

import static com.objectstore.controller.cluster.TestSupport.chunk;
import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.controller.cluster.ClusterState.WorkerState;
import com.objectstore.controller.cluster.PendingOps.Op;
import com.objectstore.controller.cluster.PendingOps.Reason;
import com.objectstore.controller.cluster.PendingOps.State;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The planner's decisions (v5 §6.3–§6.6): repair with versioned ops, source and target failures,
 * quarantine and repair of corrupt copies, and the safe EXCESS gate with controller-timed VERIFY.
 */
class ReplicationPlannerTest {

    private final TestSupport t = new TestSupport();
    private final ClusterState cluster = t.cluster;

    private List<Command> queued(String workerId) {
        return cluster.locked(() -> cluster.worker(workerId).commands.stream().toList());
    }

    private void kill(String workerId) {
        cluster.write(() -> cluster.worker(workerId).lastSeen = cluster.now() - (long) 1e10);
        cluster.write(() -> t.registry.markDead(workerId, cluster.worker(workerId).epoch).body.run(Long.MAX_VALUE));
    }

    @Test
    void anUnderReplicatedChunkIsCopiedAndTheRepairResolvesOnlyWithItsOwnVersion() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2");
        t.join("worker3");

        t.plan();

        Command replicate = queued("worker1").getFirst();
        assertThat(replicate.type()).isEqualTo(InternalApi.COMMAND_REPLICATE);
        assertThat(replicate.targetUrl()).isEqualTo("http://worker2:8001");   // least loaded, then lowest id
        Op op = t.op(chunk(0), "worker2");
        assertThat(op.reason()).isEqualTo(Reason.REPAIR);
        assertThat(replicate.version()).isEqualTo(op.version());
        t.plan();                                                              // P counts: nothing more
        assertThat(cluster.read(() -> cluster.ops().inserts())).isEqualTo(1);

        t.ibr("worker2", t.received("worker2", chunk(0), t.local("worker2")));   // a copy found by hand: odd
        assertThat(t.op(chunk(0), "worker2")).as("an odd RECEIVED completes nothing").isNotNull();
        t.ibr("worker2", t.received("worker2", chunk(0), op.version()));
        assertThat(t.op(chunk(0), "worker2")).isNull();
        assertThat(cluster.read(() -> cluster.live(chunk(0)))).containsExactlyInAnyOrder("worker1", "worker2");
        assertThat(cluster.events()).as("the copied event names the source and the target")
                .anyMatch(e -> e.event().equals("copied") && "worker1".equals(e.source()) && "worker2".equals(e.target()));
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

        assertThat(cluster.read(() -> cluster.ops().streamsOf("worker1"))).isEqualTo(2);
        assertThat(cluster.read(cluster::blockedLastTick)).isEqualTo(3);
    }

    @Test
    void aTransferFailureDropsTheRepairAndAvoidsThatTargetForAWhile() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2");
        t.join("worker3");
        t.plan();
        Op op = t.op(chunk(0), "worker2");

        t.ibr("worker1", t.cmdFailed("worker1", chunk(0), op.version(), InternalApi.FAILED_TRANSFER));
        assertThat(t.op(chunk(0), "worker2")).isNull();
        t.plan();

        assertThat(t.op(chunk(0), "worker3")).isNotNull();
        assertThat(t.op(chunk(0), "worker2")).isNull();
    }

    @Test
    void aSourceThatLostItsCopyIsReplacedAtTheSameVersionWithoutBlamingTheTarget() {
        TestSupport rf3 = TestSupport.withRf(3, 2);
        rf3.knowChunk(chunk(0));
        rf3.join("worker1", chunk(0));
        rf3.join("worker2", chunk(0));
        rf3.join("worker3");
        rf3.plan();
        Op op = rf3.op(chunk(0), "worker3");
        String firstSource = op.source();
        long version = op.version();

        rf3.ibr(firstSource, rf3.deleted(firstSource, chunk(0), rf3.local(firstSource), "missing"),
                rf3.cmdFailed(firstSource, chunk(0), version, InternalApi.FAILED_NO_LOCAL_COPY));

        assertThat(op.state()).isEqualTo(State.WAITING);
        assertThat(op.version()).as("same version").isEqualTo(version);
        assertThat(rf3.cluster.read(() -> rf3.cluster.recentlyFailed(chunk(0), "worker3"))).as("not blamed").isFalse();
        rf3.plan();
        assertThat(op.source()).isNotNull().isNotEqualTo(firstSource);
        assertThat(op.state()).isEqualTo(State.QUEUED);
        rf3.assertNoViolations();
    }

    @Test
    void edge27_surplusIsPostponedWhileAHolderIsNotCurrentAndTrimmedOnlyWithVerifiedSurvivors() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2", chunk(0));
        t.join("worker3", chunk(0));
        cluster.write(() -> cluster.worker("worker3").state = WorkerState.STALE);

        t.plan();
        assertThat(cluster.read(() -> cluster.postponedSet())).contains(chunk(0));
        assertThat(cluster.read(cluster::verifyOutstanding)).as("not while a holder is STALE").isZero();

        t.handOut("worker3");                                   // ALIVE again: its postponed chunks are released
        t.plan();
        assertThat(cluster.read(() -> cluster.ops().countFor(chunk(0)))).as("no survivor is verified yet").isZero();
        assertThat(cluster.read(cluster::verifyOutstanding)).isEqualTo(2);
        for (String survivor : List.of("worker1", "worker2")) {
            List<Command> verify = t.handOut(survivor);
            assertThat(verify).singleElement().extracting(Command::type).isEqualTo(InternalApi.COMMAND_VERIFY);
            t.perform(survivor, verify.getFirst());
        }

        t.plan();

        Op excess = t.op(chunk(0), "worker3");
        assertThat(excess.reason()).isEqualTo(Reason.EXCESS);
        assertThat(excess.survivors()).extracting(PendingOps.Survivor::workerId).containsExactlyInAnyOrder("worker1", "worker2");
        List<Command> delete = t.handOut("worker3");
        assertThat(delete).singleElement().satisfies(c -> assertThat(c.reason()).isEqualTo("excess"));
        t.perform("worker3", delete.getFirst());
        assertThat(cluster.read(() -> cluster.live(chunk(0)))).containsExactlyInAnyOrder("worker1", "worker2");
        assertThat(cluster.events()).anyMatch(e -> e.event().equals("trimmed"));
        t.assertNoViolations();
    }

    @Test
    void aDeliveredExcessDeleteIsNotTakenBackAndNormalRepairRestoresRf() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2", chunk(0));
        t.join("worker3", chunk(0));
        t.join("worker4");
        t.settle("worker1", "worker2");                         // the survivors answer their VERIFYs
        t.plan();
        List<Command> delete = t.handOut("worker3");            // the DELETE is handed out
        assertThat(delete).hasSize(1);

        kill("worker1");
        t.perform("worker3", delete.getFirst());
        t.plan();

        assertThat(cluster.read(() -> cluster.live(chunk(0)))).containsExactly("worker2");
        assertThat(cluster.read(() -> cluster.ops().opsFor(chunk(0)))).singleElement()
                .satisfies(op -> assertThat(op.reason()).isEqualTo(Reason.REPAIR));
    }

    @Test
    void aQuarantinedCopyIsGoneAtOnceAndTheChunkIsRepairedFromTheGoodOne() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2", chunk(0));
        t.join("worker3");

        t.ibr("worker2", t.deleted("worker2", chunk(0), t.local("worker2"), "corrupt"));   // the scanner quarantined it
        t.plan();

        assertThat(cluster.events()).anyMatch(e -> e.event().equals("quarantined"));
        assertThat(t.holders(chunk(0))).containsExactly("worker1");
        assertThat(cluster.read(() -> cluster.ops().opsFor(chunk(0)))).singleElement().satisfies(op -> {
            assertThat(op.reason()).isEqualTo(Reason.REPAIR);
            assertThat(op.source()).isEqualTo("worker1");
        });
    }

    @Test
    void aCopyThatDoesNotMatchTheNamespaceIsQuarantinedByACorruptDelete() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2");

        t.ibr("worker2", t.receivedWithChecksum("worker2", chunk(0), "b".repeat(64), t.local("worker2")));

        Op op = t.op(chunk(0), "worker2");
        assertThat(op.reason()).isEqualTo(Reason.CORRUPT);
        assertThat(t.holders(chunk(0))).containsExactly("worker1");
        List<Command> commands = t.handOut("worker2");
        assertThat(commands).singleElement().satisfies(c -> assertThat(c.reason()).isEqualTo("corrupt"));
        t.perform("worker2", commands.getFirst());
        assertThat(t.op(chunk(0), "worker2")).isNull();
    }

    @Test
    void rf2With3CopiesOf2SecretlyCorruptQuarantinesBothTrimsNothingAndRepairsFromTheGoodCopy() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2", chunk(0));
        t.join("worker3", chunk(0));                            // the good one, and the first victim candidate
        t.join("worker4");
        t.plan();                                                // VERIFY the survivors first
        for (String survivor : List.of("worker1", "worker2")) {
            List<Command> verify = t.handOut(survivor);
            assertThat(verify).singleElement().extracting(Command::type).isEqualTo(InternalApi.COMMAND_VERIFY);
            // The VERIFY finds the bytes rotten: the worker quarantines them instead of answering.
            t.ibr(survivor, t.deleted(survivor, chunk(0), t.local(survivor), "corrupt"));
        }
        t.plan();

        assertThat(cluster.read(() -> cluster.ops().count(Reason.EXCESS))).as("nothing is trimmed").isZero();
        assertThat(t.holders(chunk(0))).containsExactly("worker3");
        Op repair = cluster.read(() -> cluster.ops().opsFor(chunk(0)).stream()
                .filter(o -> o.reason() == Reason.REPAIR).findFirst().orElseThrow());
        assertThat(repair.source()).isEqualTo("worker3");
        t.assertNoViolations();
    }

    @Test
    void aChunkWithoutAnyLiveCopyIsReportedOnceAndNeverScheduled() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2");
        t.ibr("worker1", t.deleted("worker1", chunk(0), t.local("worker1"), "corrupt"));

        t.plan();
        t.plan();
        t.plan();

        assertThat(cluster.events().stream().filter(e -> e.event().equals("unrecoverable"))).hasSize(1);
        assertThat(cluster.read(() -> cluster.ops().size())).isZero();
        assertThat(cluster.read(() -> cluster.unrecoverable(10))).containsExactly(chunk(0));
    }

    @Test
    void withNoEligibleTargetTheChunkIsBlockedAndNothingSpins() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));

        t.plan();
        t.plan();

        assertThat(cluster.read(cluster::blockedLastTick)).isEqualTo(1);
        assertThat(cluster.read(cluster::queuedCommands)).isZero();
    }

    @Test
    void theRepairQueueStartsWithTheChunksThatHaveFewestCopies() {
        t.knowChunk(chunk(5));
        t.knowChunk(chunk(6));
        t.join("worker1", chunk(5));
        t.join("worker2", chunk(6));
        t.join("worker3");
        t.ibr("worker1", t.deleted("worker1", chunk(5), t.local("worker1"), "corrupt"));   // chunk 5: no live copy

        t.plan();

        assertThat(cluster.events()).anyMatch(e -> e.event().equals("unrecoverable") && e.chunkId().equals(chunk(5)));
        assertThat(cluster.read(() -> cluster.ops().opsFor(chunk(6)))).hasSize(1);
    }
}
