package com.objectstore.controller.cluster;

import static com.objectstore.controller.cluster.TestSupport.chunk;
import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.WorkerState;
import com.objectstore.controller.cluster.PendingOps.Op;
import com.objectstore.controller.cluster.PendingOps.Reason;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The block map, accounting, reads and the hand-out filter (v5 §5.2, §6.1, §6.2). */
class ClusterStateTest {

    private final TestSupport t = new TestSupport();
    private final ClusterState cluster = t.cluster;

    @Test
    void aReplicaIsOnlyRecordedOnAKnownLiveWorkerForAKnownChunk() {
        t.join("worker1");
        cluster.locked(() -> {
            assertThat(cluster.addReplica(chunk(0), "worker1", 2)).isNull(); // unknown chunk
            cluster.putChunk(chunk(0), new ClusterState.ChunkInfo(TestSupport.OBJECT, 0, 10, TestSupport.SUM));
            assertThat(cluster.addReplica(chunk(0), "worker9", 2)).isNull(); // unknown worker
            assertThat(cluster.addReplica(chunk(0), "worker1", 2)).isNotNull();
            cluster.worker("worker1").state = WorkerState.DEAD;
            cluster.removeWorkerReplicas("worker1");
            assertThat(cluster.addReplica(chunk(0), "worker1", 2)).isNull(); // DEAD
            assertThat(cluster.checkInvariants()).isEmpty();
        });
    }

    @Test
    void liveCountsCopiesOnStaleWorkersAndAPendingDeleteButReadsUseOnlyAliveCopiesNotBeingQuarantined() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2", chunk(0));
        t.join("worker3", chunk(0));
        cluster.locked(() -> {
            cluster.worker("worker1").state = WorkerState.STALE;
            cluster.ops().ensure(chunk(0), "worker2", Reason.CORRUPT);
            assertThat(cluster.live(chunk(0))).as("L (v5 §6.2)").containsExactlyInAnyOrder("worker1", "worker2", "worker3");
            assertThat(cluster.available(chunk(0))).containsExactlyInAnyOrder("worker2", "worker3");
            assertThat(cluster.readable(chunk(0))).as("no STALE source, never a copy being quarantined")
                    .containsExactly("worker3");
            assertThat(cluster.replicaStatus(chunk(0), "worker2")).isEqualTo("CORRUPT");
        });
    }

    @Test
    void readsPreferCopiesWithoutRecentErrorsAndPendingDeletes() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2", chunk(0));
        t.join("worker3", chunk(0));
        cluster.locked(() -> {
            cluster.worker("worker2").lastDataError = cluster.now();
            cluster.ops().setDelete(chunk(0), "worker1", List.of());   // not verified: refused, nothing created
            assertThat(cluster.ops().get(chunk(0), "worker1")).isNull();
            cluster.ops().ensure(chunk(0), "worker1", Reason.OBJECT_DELETED);
            assertThat(cluster.readable(chunk(0))).containsExactly("worker3", "worker2", "worker1");
        });
    }

    @Test
    void aNewVersionGivesTheReplicaANewGenerationAndVoidsItsCredit() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        cluster.locked(() -> {
            Replica replica = cluster.replica(chunk(0), "worker1");
            long gen = replica.gen;
            replica.verifiedEpoch = cluster.worker("worker1").verifyEpoch;
            replica.verifiedAt = cluster.now();
            assertThat(cluster.freshForDelete(replica, cluster.worker("worker1"))).isTrue();

            cluster.addReplica(chunk(0), "worker1", 0);                    // same version: nothing changes
            assertThat(replica.gen).isEqualTo(gen);
            cluster.addReplica(chunk(0), "worker1", 44);                   // a changed copy
            assertThat(replica.gen).isGreaterThan(gen);
            assertThat(cluster.freshForDelete(replica, cluster.worker("worker1"))).isFalse();
        });
    }

    @Test
    void theHandOutFilterHoldsWorkTargetingAResyncingWorkerButLetsItSendCopies() {
        t.knowChunk(chunk(0));
        t.knowChunk(chunk(1));
        t.join("worker1", chunk(0));
        t.join("worker2", chunk(1));
        t.join("worker3");
        cluster.locked(() -> {
            Op toResyncing = cluster.ops().ensure(chunk(0), "worker2", Reason.REPAIR);
            cluster.ops().setSource(toResyncing, "worker1");
            assertThat(cluster.ops().tryQueue(toResyncing)).isTrue();
            Op fromResyncing = cluster.ops().ensure(chunk(1), "worker3", Reason.REPAIR);
            cluster.ops().setSource(fromResyncing, "worker2");
            assertThat(cluster.ops().tryQueue(fromResyncing)).isTrue();
            Op delete = cluster.ops().ensure(chunk(1), "worker2", Reason.OBJECT_DELETED);   // supersedes nothing: own pair
            cluster.startResync(cluster.worker("worker2"), "test", false);

            assertThat(cluster.takeCommands(cluster.worker("worker1"))).as("its target is resyncing").isEmpty();
            assertThat(cluster.worker("worker1").commands).hasSize(1);
            List<Command> fromWorker2 = cluster.takeCommands(cluster.worker("worker2"));
            assertThat(fromWorker2).extracting(Command::type).as("only the copy it sends goes out")
                    .containsExactly(InternalApi.COMMAND_REPLICATE);
            assertThat(delete.state()).isNotEqualTo(PendingOps.State.SENT);
            assertThat(cluster.checkInvariants()).isEmpty();
        });
    }

    @Test
    void theInvariantCheckerReportsABrokenReverseIndexAndAnOpOnADeadWorker() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2");
        cluster.locked(() -> {
            cluster.ops().ensure(chunk(0), "worker2", Reason.REPAIR);
            cluster.chunksOn("worker1").clear();                          // break I2
            cluster.worker("worker2").state = WorkerState.DEAD;           // an op left on a DEAD worker
            List<String> violations = cluster.checkInvariants();
            assertThat(violations).anyMatch(v -> v.startsWith("I2")).anyMatch(v -> v.contains("DEAD worker"));
            assertThat(cluster.counter("invariant_violations")).isEqualTo(violations.size());
        });
    }

    @Test
    void removingAChunkForgetsItsCopiesButKeepsItsDeletes() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2");
        cluster.locked(() -> {
            cluster.ops().ensure(chunk(0), "worker1", Reason.OBJECT_DELETED);
            cluster.removeChunk(chunk(0));
            assertThat(cluster.replicasOf(chunk(0))).isEmpty();
            assertThat(cluster.chunksOn("worker1")).isEmpty();
            assertThat(cluster.ops().get(chunk(0), "worker1")).as("the DELETE still has to be delivered").isNotNull();
            assertThat(cluster.checkInvariants()).isEmpty();
        });
    }

    @Test
    void edge26_theRollingCheckFindsCountsAndRepairsADriftedIndexEntry() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2", chunk(0));
        // Drift: a change that bypasses markDirty (what a bug elsewhere would look like).
        cluster.write(() -> cluster.worker("worker2").state = WorkerState.DEAD);
        cluster.write(() -> cluster.worker("worker2").state = WorkerState.ALIVE);
        cluster.write(() -> {
            cluster.replicasOf(chunk(0)).remove("worker2");
            cluster.chunksOn("worker2").remove(chunk(0));
        });

        List<String> problems = cluster.write(() -> cluster.rollingCheck(100));

        assertThat(problems).anyMatch(p -> p.startsWith("I10"));
        assertThat(cluster.counter("index_mismatches")).isEqualTo(1);
        assertThat(cluster.write(cluster::checkInvariants)).as("repaired").isEmpty();
        assertThat(cluster.read(() -> cluster.levels().levelOf(chunk(0)))).isEqualTo(cluster.levels().levelFor(1, 0));
    }

    /**
     * Found by the chaos test (index_mismatches = 1): the planner tick starts resyncs and then runs the
     * rolling check in the same lock hold. Starting a resync takes the worker's INSERT ops out of P and
     * marks their chunks dirty; the check must compare against flushed entries, not count that as drift.
     */
    @Test
    void theRollingCheckFlushesFirstSoAResyncStartedInTheSameHoldIsNotCountedAsDrift() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2");
        cluster.write(() -> cluster.ops().ensure(chunk(0), "worker2", Reason.REPAIR));     // L = 1, P = 1

        List<String> problems = cluster.write(() -> {
            cluster.startResync(cluster.worker("worker2"), "periodic", true);              // P = 0; the chunk is dirty
            return cluster.rollingCheck(100);
        });

        assertThat(problems).isEmpty();
        assertThat(cluster.counter("index_mismatches")).isZero();
        assertThat(cluster.read(() -> cluster.levels().levelOf(chunk(0)))).isEqualTo(cluster.levels().levelFor(1, 0));
    }
}
