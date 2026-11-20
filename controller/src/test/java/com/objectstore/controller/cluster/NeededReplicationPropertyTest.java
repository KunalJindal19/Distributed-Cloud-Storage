package com.objectstore.controller.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.controller.cluster.ClusterState.PendingCopy;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.ReplicaState;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.ClusterState.WorkerState;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Block 15 (v4 §7.6, §10 row 26): 10⁴ random operation sequences for RF ∈ {1, 2, 3, 4}. After
 * every operation, the levels, the cleanup set and every counter must equal a from-scratch
 * recomputation (and every other invariant must hold).
 */
class NeededReplicationPropertyTest {

    private static final int SEQUENCES = 10_000;
    private static final int OPERATIONS = 25;
    private static final List<String> WORKERS = List.of("worker1", "worker2", "worker3", "worker4", "worker5");

    private static String chunk(int i) {
        return TestSupport.chunk(i);
    }

    @Test
    void edge26_theIndexAndEveryCounterEqualARecomputationAfterEveryOperation() {
        for (int sequence = 0; sequence < SEQUENCES; sequence++) {
            run(new Random(sequence), 1 + sequence % 4, sequence);
        }
    }

    private void run(Random random, int rf, int sequence) {
        TestSupport t = TestSupport.withRf(rf, 2);
        ClusterState cluster = t.cluster;
        WORKERS.forEach(t::join);
        for (int i = 0; i < 5; i++) {
            t.knowChunk(chunk(i));
        }
        List<String> ops = new ArrayList<>();
        for (int op = 0; op < OPERATIONS; op++) {
            String chunkId = chunk(random.nextInt(8));
            String workerId = WORKERS.get(random.nextInt(WORKERS.size()));
            int kind = random.nextInt(16);
            ops.add(kind + ":" + chunkId.substring(chunkId.length() - 1) + "@" + workerId);
            if (kind == 7) {
                markDead(t, workerId);
            } else if (kind == 8) {
                if (t.fake(workerId).boot != null) {
                    t.reRegister(workerId);
                    t.fullReport(workerId, t.fake(workerId).boot, List.copyOf(cluster.read(() -> cluster.chunksOn(workerId)))
                            .toArray(String[]::new));
                }
            } else {
                cluster.write(() -> apply(t, kind, chunkId, workerId, random));
            }
            List<String> violations = cluster.write(cluster::checkInvariants);
            assertThat(violations).as("sequence %d (RF %d), operations %s", sequence, rf, ops).isEmpty();
        }
    }

    private static void apply(TestSupport t, int kind, String chunkId, String workerId, Random random) {
        ClusterState cluster = t.cluster;
        WorkerInfo worker = cluster.worker(workerId);
        boolean known = cluster.chunk(chunkId) != null;
        Replica replica = cluster.replica(chunkId, workerId);
        switch (kind) {
            case 0 -> {                                            // a copy arrives (RECEIVED)
                if (known && !worker.isDead() && replica == null) {
                    cluster.removePending(chunkId, workerId);
                    cluster.addReplica(chunkId, workerId, ReplicaState.OK);
                }
            }
            case 1 -> {                                            // the copy is gone (DELETED)
                cluster.removeReplica(chunkId, workerId);
                cluster.ledger().completed(chunkId, workerId);
            }
            case 2 -> cluster.markCorrupt(chunkId, workerId);       // BAD
            case 3 -> {                                            // freshly verified again
                if (replica != null && replica.state == ReplicaState.CORRUPT && !replica.deleteDelivered) {
                    cluster.ledger().cancel(chunkId, workerId);
                    cluster.setReplicaState(chunkId, workerId, ReplicaState.OK);
                }
            }
            case 4 -> {                                            // a repair is ordered
                List<String> sources = cluster.live(chunkId);
                boolean pendingTarget = cluster.pendingOf(chunkId).stream().anyMatch(p -> p.target().equals(workerId));
                if (known && !sources.isEmpty() && !worker.isDead() && replica == null && !pendingTarget) {
                    String source = sources.get(random.nextInt(sources.size()));
                    if (!cluster.worker(source).isDead() && cluster.pendingFrom(source) < t.properties.maxStreams()) {
                        cluster.addPending(chunkId, source, workerId, "cmd-" + random.nextInt());
                    }
                }
            }
            case 5 -> {                                            // an upload copy awaits its RECEIVED
                boolean pendingTarget = cluster.pendingOf(chunkId).stream().anyMatch(p -> p.target().equals(workerId));
                if (known && !worker.isDead() && replica == null && !pendingTarget) {
                    cluster.addUploadPending(chunkId, workerId);
                }
            }
            case 6 -> {                                            // a pending copy ends
                List<PendingCopy> copies = cluster.pendingOf(chunkId);
                if (!copies.isEmpty()) {
                    cluster.removePendingCopy(copies.get(random.nextInt(copies.size())));
                }
            }
            case 9 -> {                                            // a chunk is committed
                if (!known) {
                    cluster.putChunk(chunkId, new ClusterState.ChunkInfo(TestSupport.OBJECT, 0, 10, TestSupport.SUM));
                }
            }
            case 10 -> {                                           // a chunk is deleted
                if (known) {
                    cluster.ledger().chunkRemoved(chunkId, DeletionLedger.OBJECT_DELETED);
                    cluster.removeChunk(chunkId);
                }
            }
            case 11 -> {                                           // a surplus copy is trimmed
                List<String> live = cluster.live(chunkId);
                if (live.size() > t.properties.replicationFactor() && cluster.pendingOf(chunkId).isEmpty()) {
                    String victim = live.get(random.nextInt(live.size()));
                    cluster.setReplicaState(chunkId, victim, ReplicaState.EXCESS);
                    cluster.sendDelete(victim, chunkId, "excess");
                }
            }
            case 12 -> cluster.takeCommands(worker);                // a heartbeat takes its commands
            case 13 -> {                                           // STALE / ALIVE
                if (!worker.isDead()) {
                    worker.state = worker.state == WorkerState.ALIVE ? WorkerState.STALE : WorkerState.ALIVE;
                }
            }
            default -> t.planner.plan();                            // a planner pass
        }
    }

    private static void markDead(TestSupport t, String workerId) {
        ClusterState cluster = t.cluster;
        WorkerInfo worker = cluster.read(() -> cluster.worker(workerId));
        if (worker.isDead()) {
            return;
        }
        Task task = cluster.write(() -> {
            worker.lastSeen = cluster.now() - 10_000_000_000L;
            return t.registry.markDead(workerId, worker.epoch);
        });
        while (cluster.write(() -> task.body.run(Long.MAX_VALUE)) == Task.Step.MORE) {
            // all steps
        }
    }
}
