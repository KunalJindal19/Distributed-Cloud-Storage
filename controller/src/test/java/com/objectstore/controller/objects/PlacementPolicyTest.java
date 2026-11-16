package com.objectstore.controller.objects;

import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.controller.domain.ReplicaLocation;
import com.objectstore.controller.domain.ReplicaStatus;
import com.objectstore.controller.domain.Worker;
import com.objectstore.controller.domain.WorkerStatus;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PlacementPolicyTest {

    /** First 8 hex digits are 0, so this object's ring offset is 0 and the examples read naturally. */
    private static final String OBJECT_AT_OFFSET_0 = "00000000-7d0e-4c1b-9a54-1f2e3d4c5b6a";
    private static final String OBJECT_AT_OFFSET_1 = "00000001-7d0e-4c1b-9a54-1f2e3d4c5b6a";

    private final PlacementPolicy placement = new PlacementPolicy();

    @Test
    void consecutiveChunksRotateAroundTheRing() {
        List<Worker> alive = workers("worker3", "worker1", "worker4", "worker2");

        assertThat(holders(alive, OBJECT_AT_OFFSET_0, 0)).containsExactly("worker1", "worker2");
        assertThat(holders(alive, OBJECT_AT_OFFSET_0, 1)).containsExactly("worker2", "worker3");
        assertThat(holders(alive, OBJECT_AT_OFFSET_0, 2)).containsExactly("worker3", "worker4");
        assertThat(holders(alive, OBJECT_AT_OFFSET_0, 3)).containsExactly("worker4", "worker1");
    }

    @Test
    void differentObjectsStartAtDifferentRingPositions() {
        List<Worker> alive = workers("worker1", "worker2", "worker3", "worker4");

        assertThat(holders(alive, OBJECT_AT_OFFSET_0, 0)).containsExactly("worker1", "worker2");
        assertThat(holders(alive, OBJECT_AT_OFFSET_1, 0)).containsExactly("worker2", "worker3");
    }

    @Test
    void theRingShrinksToTheWorkersThatAreAlive() {
        List<Worker> threeAlive = workers("worker1", "worker3", "worker4");
        assertThat(holders(threeAlive, OBJECT_AT_OFFSET_0, 0)).containsExactly("worker1", "worker3");
        assertThat(holders(threeAlive, OBJECT_AT_OFFSET_0, 1)).containsExactly("worker3", "worker4");
        assertThat(holders(threeAlive, OBJECT_AT_OFFSET_0, 2)).containsExactly("worker4", "worker1");

        List<Worker> twoAlive = workers("worker1", "worker3");
        assertThat(holders(twoAlive, OBJECT_AT_OFFSET_0, 0)).containsExactly("worker1", "worker3");
        assertThat(holders(twoAlive, OBJECT_AT_OFFSET_0, 1)).containsExactly("worker3", "worker1");
    }

    @Test
    void theRingListsEveryWorkerExactlyOnceSoCopiesNeverShareAWorker() {
        List<Worker> alive = workers("worker1", "worker2", "worker3", "worker4");

        for (int i = 0; i < 200; i++) {
            String objectId = UUID.randomUUID().toString();
            for (int chunk = 0; chunk < 6; chunk++) {
                assertThat(placement.ringFor(alive, objectId, chunk))
                        .extracting(Worker::workerId)
                        .containsExactlyInAnyOrder("worker1", "worker2", "worker3", "worker4");
            }
        }
    }

    @Test
    void fourChunksOnFourWorkersGiveEveryWorkerExactlyTwoCopies() {
        List<Worker> alive = workers("worker1", "worker2", "worker3", "worker4");

        for (int i = 0; i < 50; i++) {
            String objectId = UUID.randomUUID().toString();
            Map<String, Integer> copies = new HashMap<>();
            for (int chunk = 0; chunk < 4; chunk++) {
                holders(alive, objectId, chunk).forEach(id -> copies.merge(id, 1, Integer::sum));
            }
            assertThat(copies).containsOnly(
                    Map.entry("worker1", 2), Map.entry("worker2", 2), Map.entry("worker3", 2), Map.entry("worker4", 2));
        }
    }

    @Test
    void manySingleChunkObjectsSpreadOverAllWorkers() {
        List<Worker> alive = workers("worker1", "worker2", "worker3", "worker4");
        Map<String, Integer> copies = new HashMap<>();

        for (int i = 0; i < 4000; i++) {
            holders(alive, UUID.randomUUID().toString(), 0).forEach(id -> copies.merge(id, 1, Integer::sum));
        }

        assertThat(copies).hasSize(4);
        assertThat(copies.values()).allSatisfy(count -> assertThat(count).isBetween(1700, 2300));
    }

    @Test
    void anEmptyClusterHasAnEmptyRing() {
        assertThat(placement.ringFor(List.of(), OBJECT_AT_OFFSET_0, 0)).isEmpty();
    }

    @Test
    void repairTargetIsTheLeastLoadedLiveWorkerThatHoldsNoCopyYet() {
        List<Worker> alive = workers("worker1", "worker3", "worker4");
        Map<String, Integer> counts = Map.of("worker1", 2, "worker3", 5, "worker4", 3);

        assertThat(placement.chooseRepairTarget(alive, Set.of("worker1", "worker2"), counts))
                .map(Worker::workerId).contains("worker4");
        assertThat(placement.chooseRepairTarget(alive, Set.of("worker2"), counts))
                .map(Worker::workerId).contains("worker1");
    }

    @Test
    void repairTargetTiesGoToTheLowestId() {
        List<Worker> alive = workers("worker4", "worker3", "worker1");

        assertThat(placement.chooseRepairTarget(alive, Set.of("worker1"), Map.of()))
                .map(Worker::workerId).contains("worker3");
    }

    @Test
    void thereIsNoRepairTargetWhenEveryLiveWorkerAlreadyHoldsACopy() {
        List<Worker> alive = workers("worker1", "worker3");

        assertThat(placement.chooseRepairTarget(alive, Set.of("worker1", "worker2", "worker3"), Map.of())).isEmpty();
    }

    @Test
    void trimVictimIsTheCopyOnTheBusiestWorkerTiesToTheHighestId() {
        List<ReplicaLocation> good = List.of(copy("worker1"), copy("worker2"), copy("worker3"));

        assertThat(placement.chooseTrimVictim(good, Map.of("worker1", 4, "worker2", 9, "worker3", 4)).workerId())
                .isEqualTo("worker2");
        assertThat(placement.chooseTrimVictim(good, Map.of("worker1", 4, "worker2", 4, "worker3", 4)).workerId())
                .isEqualTo("worker3");
    }

    @Test
    void readsPreferTheLeastLoadedWorkerTiesToTheLowestId() {
        List<ReplicaLocation> good = List.of(copy("worker3"), copy("worker1"), copy("worker2"));

        assertThat(placement.readOrder(good, Map.of("worker1", 7, "worker2", 2, "worker3", 2)))
                .extracting(ReplicaLocation::workerId)
                .containsExactly("worker2", "worker3", "worker1");
    }

    private List<String> holders(List<Worker> alive, String objectId, int chunkIndex) {
        return placement.ringFor(alive, objectId, chunkIndex).subList(0, 2).stream().map(Worker::workerId).toList();
    }

    private static List<Worker> workers(String... ids) {
        return java.util.Arrays.stream(ids)
                .map(id -> new Worker(id, "http://" + id + ":8001", WorkerStatus.ALIVE, Instant.EPOCH, Instant.EPOCH))
                .toList();
    }

    private static ReplicaLocation copy(String workerId) {
        return new ReplicaLocation("chunk", workerId, "http://" + workerId, ReplicaStatus.OK, WorkerStatus.ALIVE, Instant.EPOCH);
    }
}
