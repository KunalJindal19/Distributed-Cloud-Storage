package com.objectstore.controller.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.ClusterState.WorkerState;
import com.objectstore.controller.cluster.PendingOps.Kind;
import com.objectstore.controller.cluster.PendingOps.Op;
import com.objectstore.controller.cluster.PendingOps.Reason;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * 10⁴ random operation sequences for RF ∈ {1, 2, 3, 4}. After every operation the levels, the
 * cleanup set, every counter, the pending-op indexes and counters, and every other invariant must
 * equal a from-scratch recomputation (v5 §6.2: L, P, at most one op per pair).
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
            int kind = random.nextInt(19);
            ops.add(kind + ":" + chunkId.substring(chunkId.length() - 1) + "@" + workerId);
            if (kind == 7) {
                markDead(t, workerId);
            } else if (kind == 8) {
                if (t.fake(workerId).boot != null) {
                    List<String> held = List.copyOf(cluster.read(() -> cluster.chunksOn(workerId)));
                    t.reRegister(workerId);
                    t.fullReport(workerId, t.fake(workerId).boot, held.toArray(String[]::new));
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
        PendingOps pending = cluster.ops();
        WorkerInfo worker = cluster.worker(workerId);
        boolean known = cluster.chunk(chunkId) != null;
        Replica replica = cluster.replica(chunkId, workerId);
        Op op = pending.get(chunkId, workerId);
        switch (kind) {
            case 0 -> {                                            // RECEIVED(v): physical part, then completion
                long version = random.nextBoolean() ? cluster.versions().current() : cluster.versions().current() | 1;
                if (known && !worker.isDead()) {
                    cluster.addReplica(chunkId, workerId, version);
                }
                if (op != null && op.kind() == Kind.INSERT && (version & 1) == 0 && version >= op.version()) {
                    pending.complete(op);
                }
            }
            case 1 -> {                                            // DELETED(v)
                cluster.removeReplica(chunkId, workerId);
                if (op != null && op.kind() == Kind.DELETE && random.nextBoolean()) {
                    pending.complete(op);
                }
            }
            case 2 -> pending.ensure(chunkId, workerId, Reason.CORRUPT);
            case 3 -> {                                            // a repair is ordered
                List<String> sources = cluster.live(chunkId);
                if (known && !sources.isEmpty() && replica == null && op == null) {
                    Op repair = pending.ensure(chunkId, workerId, Reason.REPAIR);
                    if (repair != null) {
                        pending.setSource(repair, sources.get(random.nextInt(sources.size())));
                        pending.tryQueue(repair);
                    }
                }
            }
            case 4 -> {                                            // an upload copy awaits its RECEIVED
                if (known && replica == null && op == null) {
                    pending.upload(chunkId, workerId, cluster.versions().next());
                }
            }
            case 5 -> {                                            // an op ends (dropped)
                List<Op> onChunk = pending.opsFor(chunkId);
                if (!onChunk.isEmpty()) {
                    pending.drop(onChunk.get(random.nextInt(onChunk.size())), "test");
                }
            }
            case 6 -> {                                            // a resend deadline passes
                if (op != null) {
                    pending.deadline(chunkId, workerId, op.version(), cluster.now() + (1L << 50));
                }
            }
            case 9 -> {                                            // a chunk is committed
                if (!known) {
                    cluster.putChunk(chunkId, new ClusterState.ChunkInfo(TestSupport.OBJECT, 0, 10, TestSupport.SUM));
                }
            }
            case 10 -> {                                           // a chunk is deleted
                if (known) {
                    for (String holder : List.copyOf(cluster.replicasOf(chunkId).keySet())) {
                        pending.ensure(chunkId, holder, Reason.OBJECT_DELETED);
                    }
                    cluster.removeChunk(chunkId);
                }
            }
            case 11 -> {                                           // a surplus copy, with verified survivors
                List<String> live = cluster.live(chunkId);
                if (live.size() > t.properties.replicationFactor() && pending.countFor(chunkId) == 0) {
                    String victim = live.get(random.nextInt(live.size()));
                    List<PendingOps.Survivor> survivors = new ArrayList<>();
                    for (String other : live) {
                        if (!other.equals(victim)) {
                            Replica r = cluster.replica(chunkId, other);
                            r.verifiedEpoch = cluster.worker(other).verifyEpoch;
                            r.verifiedAt = cluster.now();
                            survivors.add(new PendingOps.Survivor(other, r.gen));
                        }
                    }
                    pending.setDelete(chunkId, victim, survivors);
                }
            }
            case 12 -> cluster.takeCommands(worker);                // a heartbeat takes its commands
            case 13 -> {                                           // STALE / ALIVE
                if (!worker.isDead()) {
                    worker.state = worker.state == WorkerState.ALIVE ? WorkerState.STALE : WorkerState.ALIVE;
                    cluster.markInsertTargetsDirty(workerId);
                }
            }
            case 14 -> {                                           // a resync starts, or (a short-cut) completes
                if (!worker.isDead()) {
                    if (!worker.resyncing) {
                        cluster.startResync(worker, "test", false);
                    } else {
                        pending.dropTargeting(workerId, worker.fenceFloor, "test completion");
                        worker.resyncing = false;
                        worker.hasReported = true;
                        cluster.markInsertTargetsDirty(workerId);
                    }
                }
            }
            case 15 -> {                                           // a VERIFY, answered
                if (cluster.requestVerify(chunkId, workerId)) {
                    cluster.takeCommands(worker);
                    cluster.verifyAnswered(chunkId, workerId, cluster.replica(chunkId, workerId).verifyCmdId);
                }
            }
            case 16 -> pending.nextWaiting(5).forEach(pending::tryQueue);
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
