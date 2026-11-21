package com.objectstore.controller.objects;

import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.dto.WorkerStats;
import com.objectstore.controller.cluster.ClusterState;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.ClusterState.WorkerState;
import com.objectstore.controller.cluster.TestSupport;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PlacementPolicyTest {

    private static final String OBJECT_AT_OFFSET_0 = "00000000-7d0e-4c1b-9a54-1f2e3d4c5b6a";
    private static final String OBJECT_AT_OFFSET_1 = "00000001-7d0e-4c1b-9a54-1f2e3d4c5b6a";

    private final TestSupport t = new TestSupport();
    private final ClusterState cluster = t.cluster;
    private final PlacementPolicy placement = t.placement;

    private void joinAll(String... ids) {
        for (String id : ids) {
            t.join(id);
        }
    }

    private List<String> holders(String objectId, int chunk, Set<String> exclude) {
        return cluster.locked(() -> placement.ringForUpload(objectId, chunk, exclude).stream()
                .limit(2).map(w -> w.workerId).toList());
    }

    @Test
    void consecutiveChunksRotateAroundTheRingOfEligibleWorkers() {
        joinAll("worker3", "worker1", "worker4", "worker2");

        assertThat(holders(OBJECT_AT_OFFSET_0, 0, Set.of())).containsExactly("worker1", "worker2");
        assertThat(holders(OBJECT_AT_OFFSET_0, 1, Set.of())).containsExactly("worker2", "worker3");
        assertThat(holders(OBJECT_AT_OFFSET_0, 3, Set.of())).containsExactly("worker4", "worker1");
        assertThat(holders(OBJECT_AT_OFFSET_1, 0, Set.of())).containsExactly("worker2", "worker3");
    }

    @Test
    void excludedStaleUnreportedAndFullWorkersReceiveNoNewData() {
        joinAll("worker1", "worker2", "worker3", "worker4");
        t.register("worker5"); // registered, but no full report yet
        cluster.locked(() -> {
            cluster.worker("worker2").state = WorkerState.STALE;
            cluster.worker("worker3").stats = new WorkerStats(0, 0, 1024, 0);
        });

        assertThat(holders(OBJECT_AT_OFFSET_0, 0, Set.of("worker4"))).containsExactly("worker1");
    }

    @Test
    void whenMostWorkersAreStaleTheyAreUsedAnyway() {
        joinAll("worker1", "worker2", "worker3", "worker4");
        cluster.locked(() -> {
            cluster.worker("worker1").state = WorkerState.STALE;
            cluster.worker("worker2").state = WorkerState.STALE;
            cluster.worker("worker3").state = WorkerState.STALE;
        });

        assertThat(cluster.locked(() -> placement.writeEligibleWorkers())).extracting(w -> w.workerId)
                .containsExactly("worker1", "worker2", "worker3", "worker4");
    }

    @Test
    void theRingNeverPlacesTwoCopiesOfAChunkOnOneWorker() {
        joinAll("worker1", "worker2", "worker3", "worker4");
        for (int i = 0; i < 200; i++) {
            String objectId = UUID.randomUUID().toString();
            for (int chunk = 0; chunk < 6; chunk++) {
                assertThat(holders(objectId, chunk, Set.of())).doesNotHaveDuplicates().hasSize(2);
            }
        }
    }

    @Test
    void manySmallObjectsSpreadOverAllWorkers() {
        joinAll("worker1", "worker2", "worker3", "worker4");
        Map<String, Integer> copies = new HashMap<>();
        for (int i = 0; i < 4000; i++) {
            holders(UUID.randomUUID().toString(), 0, Set.of()).forEach(w -> copies.merge(w, 1, Integer::sum));
        }
        assertThat(copies.values()).allSatisfy(count -> assertThat(count).isBetween(1700, 2300));
    }

    @Test
    void theExcessVictimIsAnUnverifiedCopyFirstThenTheBusiestWorker() {
        t.knowChunk(TestSupport.chunk(0));
        t.knowChunk(TestSupport.chunk(1));
        t.join("worker1", TestSupport.chunk(0));
        t.join("worker2", TestSupport.chunk(0), TestSupport.chunk(1));
        t.join("worker3", TestSupport.chunk(0));

        assertThat(cluster.locked(() -> placement.chooseVictim(TestSupport.chunk(0)))).contains("worker2");
        cluster.locked(() -> {
            var replica = cluster.replica(TestSupport.chunk(0), "worker2");
            replica.verifiedEpoch = cluster.worker("worker2").verifyEpoch;      // worker2's copy is verified now
            replica.verifiedAt = cluster.now();
        });
        assertThat(cluster.locked(() -> placement.chooseVictim(TestSupport.chunk(0)))).contains("worker3");
    }

    @Test
    void aWorkerThatAlreadyHoldsTheChunkInAnyStateIsNeverATarget() {
        t.knowChunk(TestSupport.chunk(0));
        t.join("worker1", TestSupport.chunk(0));
        t.join("worker2", TestSupport.chunk(0));
        t.join("worker3");
        cluster.locked(() -> cluster.ops().ensure(TestSupport.chunk(0), "worker2", com.objectstore.controller.cluster.PendingOps.Reason.CORRUPT));

        assertThat(cluster.locked(() -> placement.chooseTarget(TestSupport.chunk(0)))).contains("worker3");
        assertThat(cluster.locked(() -> placement.chooseSource(TestSupport.chunk(0)))).contains("worker1");
    }

    @SuppressWarnings("unused")
    private static WorkerInfo unused() {
        return null;
    }
}
