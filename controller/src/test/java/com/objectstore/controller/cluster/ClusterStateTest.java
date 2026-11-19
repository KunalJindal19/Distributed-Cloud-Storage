package com.objectstore.controller.cluster;

import static com.objectstore.controller.cluster.TestSupport.chunk;
import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.controller.cluster.ClusterState.ReplicaState;
import com.objectstore.controller.cluster.ClusterState.WorkerState;
import java.util.List;
import org.junit.jupiter.api.Test;

class ClusterStateTest {

    private final TestSupport t = new TestSupport();
    private final ClusterState cluster = t.cluster;

    @Test
    void aReplicaIsOnlyRecordedOnAKnownLiveWorkerForAKnownChunk() {
        t.join("worker1");
        cluster.locked(() -> {
            assertThat(cluster.addReplica(chunk(0), "worker1", ReplicaState.OK)).isNull(); // unknown chunk
            cluster.putChunk(chunk(0), new ClusterState.ChunkInfo(TestSupport.OBJECT, 0, 10, TestSupport.SUM));
            assertThat(cluster.addReplica(chunk(0), "worker9", ReplicaState.OK)).isNull(); // unknown worker
            assertThat(cluster.addReplica(chunk(0), "worker1", ReplicaState.OK)).isNotNull();
            cluster.worker("worker1").state = WorkerState.DEAD;
            cluster.removeWorkerReplicas("worker1");
            assertThat(cluster.addReplica(chunk(0), "worker1", ReplicaState.OK)).isNull(); // DEAD
            assertThat(cluster.checkInvariants()).isEmpty();
        });
    }

    @Test
    void liveCountsOkCopiesOnStaleWorkersButNotCorruptOrExcessOnes() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2", chunk(0));
        t.join("worker3", chunk(0));
        cluster.locked(() -> {
            cluster.worker("worker1").state = WorkerState.STALE;
            cluster.replica(chunk(0), "worker2").state = ReplicaState.CORRUPT;
            cluster.replica(chunk(0), "worker3").state = ReplicaState.EXCESS;
            assertThat(cluster.live(chunk(0))).containsExactly("worker1");
        });
    }

    @Test
    void readsPreferAliveWorkersWithoutRecentErrors() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2", chunk(0));
        t.join("worker3", chunk(0));
        cluster.locked(() -> {
            cluster.worker("worker1").state = WorkerState.STALE;
            cluster.worker("worker2").lastDataError = cluster.now();
            assertThat(cluster.readable(chunk(0))).containsExactly("worker3", "worker2", "worker1");
        });
    }

    @Test
    void identicalCommandsAreQueuedOnceAndADeliveredDeleteIsMarked() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        cluster.locked(() -> {
            Command first = cluster.queueCommand("worker1", InternalApi.COMMAND_DELETE, chunk(0), null, null, "excess");
            Command again = cluster.queueCommand("worker1", InternalApi.COMMAND_DELETE, chunk(0), null, null, "excess");
            assertThat(again.cmdId()).isEqualTo(first.cmdId());
            assertThat(cluster.worker("worker1").commands).hasSize(1);
            cluster.replica(chunk(0), "worker1").state = ReplicaState.EXCESS;
            cluster.replica(chunk(0), "worker1").deleteCmdId = first.cmdId();

            List<Command> delivered = cluster.takeCommands(cluster.worker("worker1"));

            assertThat(delivered).containsExactly(first);
            assertThat(cluster.replica(chunk(0), "worker1").deleteDelivered).isTrue();
            assertThat(cluster.worker("worker1").commands).isEmpty();
        });
    }

    @Test
    void theInvariantCheckerReportsABrokenReverseIndexAndADeliveredDeleteOnAnOkCopy() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        cluster.locked(() -> {
            cluster.chunksOn("worker1").clear();                          // break I2
            cluster.replica(chunk(0), "worker1").deleteDelivered = true;  // break I5
            List<String> violations = cluster.checkInvariants();
            assertThat(violations).anyMatch(v -> v.startsWith("I2")).anyMatch(v -> v.startsWith("I5"));
            assertThat(cluster.counter("invariant_violations")).isEqualTo(violations.size());
        });
    }

    @Test
    void removingAChunkForgetsItsCopiesAndPendingCopies() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2");
        cluster.locked(() -> {
            cluster.addPending(chunk(0), "worker1", "worker2", "cmd");
            cluster.removeChunk(chunk(0));
            assertThat(cluster.replicasOf(chunk(0))).isEmpty();
            assertThat(cluster.chunksOn("worker1")).isEmpty();
            assertThat(cluster.pendingTotal()).isZero();
            assertThat(cluster.checkInvariants()).isEmpty();
        });
    }
}
