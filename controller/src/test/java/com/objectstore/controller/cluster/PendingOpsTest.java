package com.objectstore.controller.cluster;

import static com.objectstore.controller.cluster.TestSupport.chunk;
import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.dto.ReportEntry;
import com.objectstore.controller.cluster.PendingOps.Op;
import com.objectstore.controller.cluster.PendingOps.Reason;
import com.objectstore.controller.cluster.PendingOps.State;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Pending operations (v5 §3.3, §6.6): one op per pair, idempotent {@code ensure}, superseding with a
 * fresh version, the WAITING → QUEUED → SENT life, completion only by the op's own result, resends
 * with the same version, guards, and counters that equal a recount (checked after every step).
 */
class PendingOpsTest {

    private final TestSupport t = new TestSupport();
    private final ClusterState cluster = t.cluster;
    private final PendingOps ops = cluster.ops();

    @Test
    void aDeleteGoesWaitingQueuedSentAndOnlyItsOwnDeletedCompletesIt() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        Op op = cluster.write(() -> ops.ensure(chunk(0), "worker1", Reason.OBJECT_DELETED));
        assertThat(op.state()).as("queued at once: the worker can take it").isEqualTo(State.QUEUED);
        assertThat(InternalApi.isControllerVersion(op.version())).isTrue();

        List<Command> commands = t.handOut("worker1");
        assertThat(commands).singleElement().satisfies(command -> {
            assertThat(command.type()).isEqualTo(InternalApi.COMMAND_DELETE);
            assertThat(command.version()).isEqualTo(op.version());
            assertThat(command.reason()).isEqualTo("object_deleted");
        });
        assertThat(op.state()).isEqualTo(State.SENT);

        t.ibr("worker1", t.deleted("worker1", chunk(0), t.local("worker1"), "missing"));   // an odd observation
        assertThat(t.op(chunk(0), "worker1")).as("an odd version never completes an op").isNotNull();
        t.ibr("worker1", t.deleted("worker1", chunk(0), op.version() - 2, "deleted"));    // an older version
        assertThat(t.op(chunk(0), "worker1")).isNotNull();
        t.ibr("worker1", t.deleted("worker1", chunk(0), op.version(), "deleted"));
        assertThat(t.op(chunk(0), "worker1")).isNull();
        t.assertNoViolations();
    }

    @Test
    void thereIsOneOpPerPairEnsureIsIdempotentAndAStrongerDeleteSupersedesWithAFreshVersion() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        cluster.write(() -> {
            Op corrupt = ops.ensure(chunk(0), "worker1", Reason.CORRUPT);
            long version = corrupt.version();
            String cmd = corrupt.cmdId();
            assertThat(ops.ensure(chunk(0), "worker1", Reason.CORRUPT)).isSameAs(corrupt);
            assertThat(ops.ensure(chunk(0), "worker1", Reason.EXCESS)).as("weaker: unchanged").isSameAs(corrupt);
            assertThat(corrupt.version()).isEqualTo(version);
            assertThat(ops.ensure(chunk(0), "worker1", Reason.REPAIR)).as("no INSERT over an op").isSameAs(corrupt);

            Op deleted = ops.ensure(chunk(0), "worker1", Reason.OBJECT_DELETED);

            assertThat(deleted).isSameAs(corrupt);
            assertThat(deleted.reason()).isEqualTo(Reason.OBJECT_DELETED);
            assertThat(deleted.version()).isGreaterThan(version);
            assertThat(ops.byCommand(cmd)).as("the superseded command was withdrawn").isNull();
            assertThat(cluster.worker("worker1").commands).singleElement()
                    .satisfies(command -> assertThat(command.version()).isEqualTo(deleted.version()));
            assertThat(ops.size()).isEqualTo(1);
            assertThat(cluster.checkInvariants()).isEmpty();
        });
    }

    @Test
    void noOpIsCreatedForAnUnknownOrDeadWorker() {
        t.knowChunk(chunk(0));
        t.join("worker1");
        cluster.write(() -> {
            assertThat(ops.ensure(chunk(0), "worker9", Reason.ORPHAN)).isNull();
            cluster.worker("worker1").state = ClusterState.WorkerState.DEAD;
            assertThat(ops.ensure(chunk(0), "worker1", Reason.ORPHAN)).isNull();
        });
    }

    @Test
    void edge14_anUnansweredDeleteIsResentWithTheSameVersionAndBackoffAndIsNeverDropped() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        Op op = cluster.write(() -> ops.ensure(chunk(0), "worker1", Reason.OBJECT_DELETED));
        long version = op.version();
        for (int attempt = 0; attempt < 8; attempt++) {
            assertThat(t.handOut("worker1")).singleElement().extracting(Command::version).isEqualTo(version);
            long backoff = ops.backoffNanos(op.attempts());
            assertThat(backoff).isLessThanOrEqualTo(t.properties.opResendMax().toNanos());
            cluster.write(() -> assertThat(ops.deadline(chunk(0), "worker1", version, cluster.now() + backoff + 1)).isTrue());
        }
        assertThat(op.attempts()).isEqualTo(8);
        assertThat(t.op(chunk(0), "worker1")).as("a DELETE is never dropped for lack of an answer").isSameAs(op);
        assertThat(ops.backoffNanos(1000)).isEqualTo(t.properties.opResendMax().toNanos());   // no overflow
        t.assertNoViolations();
    }

    @Test
    void aStaleDeadlineDoesNothing() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        Op op = cluster.write(() -> ops.ensure(chunk(0), "worker1", Reason.OBJECT_DELETED));
        t.handOut("worker1");
        cluster.write(() -> {
            assertThat(ops.deadline(chunk(0), "worker1", op.version() + 2, cluster.now() + (1L << 50))).isFalse();
            assertThat(ops.deadline(chunk(0), "worker1", op.version(), cluster.now())).as("not due yet").isFalse();
        });
        assertThat(op.state()).isEqualTo(State.SENT);
    }

    @Test
    void anInsertIsDroppedAfterItsLastAttemptAndItsTargetAvoided() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2");
        Op op = cluster.write(() -> {
            Op repair = ops.ensure(chunk(0), "worker2", Reason.REPAIR);
            ops.setSource(repair, "worker1");
            ops.tryQueue(repair);
            return repair;
        });
        for (int attempt = 0; attempt < t.properties.opMaxInsertAttempts(); attempt++) {
            assertThat(t.handOut("worker1")).hasSize(1);
            long due = cluster.now() + ops.backoffNanos(op.attempts()) + 1;
            cluster.write(() -> ops.deadline(chunk(0), "worker2", op.version(), due));
        }
        assertThat(t.op(chunk(0), "worker2")).isNull();
        assertThat(cluster.read(() -> cluster.recentlyFailed(chunk(0), "worker2"))).isTrue();
        t.assertNoViolations();
    }

    @Test
    void anExcessDeleteNeedsVerifiedSurvivorsAndIsDroppedAtHandOutIfOneFailsMeanwhile() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2", chunk(0));
        t.join("worker3", chunk(0));
        cluster.write(() -> {
            for (String w : List.of("worker1", "worker2")) {
                ClusterState.Replica replica = cluster.replica(chunk(0), w);
                replica.verifiedEpoch = cluster.worker(w).verifyEpoch;
                replica.verifiedAt = cluster.now();
            }
            List<PendingOps.Survivor> survivors = List.of(
                    new PendingOps.Survivor("worker1", cluster.replica(chunk(0), "worker1").gen),
                    new PendingOps.Survivor("worker2", cluster.replica(chunk(0), "worker2").gen));
            assertThat(ops.setDelete(chunk(0), "worker3", survivors)).isNotNull();
            assertThat(ops.get(chunk(0), "worker3").state()).isEqualTo(State.QUEUED);
            // A survivor becomes invalid before hand-out (its worker went STALE): nothing reaches the victim.
            cluster.worker("worker1").state = ClusterState.WorkerState.STALE;
            assertThat(cluster.takeCommands(cluster.worker("worker3"))).isEmpty();
            assertThat(ops.get(chunk(0), "worker3")).isNull();
            assertThat(cluster.worker("worker3").commands).isEmpty();
            assertThat(cluster.counter("excess_dropped_at_handout")).isEqualTo(1);
            assertThat(cluster.checkInvariants()).isEmpty();
        });
    }

    @Test
    void deadDropsEveryOpTargetingTheWorkerAndResourcesTheRepairsItWasSending() {
        t.knowChunk(chunk(0));
        t.knowChunk(chunk(1));
        t.join("worker1", chunk(0));
        t.join("worker2", chunk(1));
        t.join("worker3");
        cluster.write(() -> {
            ops.ensure(chunk(1), "worker2", Reason.OBJECT_DELETED);
            Op repair = ops.ensure(chunk(0), "worker3", Reason.REPAIR);
            ops.setSource(repair, "worker1");
            ops.tryQueue(repair);
        });
        cluster.write(() -> cluster.worker("worker1").lastSeen = cluster.now() - (long) 1e10);
        cluster.write(() -> cluster.worker("worker2").lastSeen = cluster.now() - (long) 1e10);
        cluster.write(() -> t.registry.markDead("worker2", cluster.worker("worker2").epoch).body.run(Long.MAX_VALUE));
        cluster.write(() -> t.registry.markDead("worker1", cluster.worker("worker1").epoch).body.run(Long.MAX_VALUE));

        assertThat(t.op(chunk(1), "worker2")).isNull();
        Op repair = t.op(chunk(0), "worker3");
        assertThat(repair).as("its source is gone, the target still wants the copy").isNotNull();
        assertThat(repair.source()).isNull();
        assertThat(repair.state()).isEqualTo(State.WAITING);
        t.assertNoViolations();
    }

    @Test
    void edge15_deletingAnObjectWith50000ChunksNeverOverfillsAQueueAndDropsNothing() {
        int chunks = 50_000;
        List<ReportEntry> entries = new ArrayList<>(chunks);
        List<String> ids = new ArrayList<>(chunks);
        for (int i = 0; i < chunks; i++) {
            t.knowChunk(chunk(i));
            entries.add(TestSupport.entry(chunk(i), 0));
            ids.add(chunk(i));
        }
        t.register("worker1");
        t.report("worker1", entries);
        cluster.write(() -> t.queue.offer(t.namespaceTasks.deleteObject(TestSupport.OBJECT, ids)));
        t.sync();

        int max = t.properties.maxCommandsPerWorker();
        Set<String> deletes = new HashSet<>();
        int heartbeats = 0;
        List<Command> confirm = new ArrayList<>();
        while (!cluster.read(ops::isEmpty)) {
            assertThat(cluster.read(() -> cluster.worker("worker1").commands.size())).isLessThanOrEqualTo(max);
            List<com.objectstore.common.dto.Change> changes = new ArrayList<>();
            for (Command command : confirm) {
                changes.add(t.deleted("worker1", command.chunkId(), command.version(), "deleted"));
            }
            HeartbeatResponse reply = t.send("worker1", changes, null);
            confirm.clear();
            for (Command command : reply.commands()) {
                assertThat(command.type()).isEqualTo(InternalApi.COMMAND_DELETE);
                deletes.add(command.chunkId());
                confirm.add(command);
            }
            if (++heartbeats % 20 == 0) {
                cluster.write(() -> t.planner.retryWaiting(t.properties.cleanupBudget(),
                        com.objectstore.controller.replication.ReplicationPlanner.Slicer.NONE));
            }
            assertThat(heartbeats).isLessThan(5000);
        }
        assertThat(deletes).hasSize(chunks);
        t.assertNoViolations();
    }

    @Test
    void edge13_aCopyLandingForADeletedObjectGetsADeleteAtOnce() {
        t.join("worker1");
        String deletedObject = "dddddddd-7d0e-4c1b-9a54-1f2e3d4c5b6a";
        String late = deletedObject + "-00003";
        cluster.write(() -> cluster.addGarbage(deletedObject, Reason.OBJECT_DELETED));

        t.ibr("worker1", t.received("worker1", late, cluster.versions().next()));

        Op op = t.op(late, "worker1");
        assertThat(op).isNotNull();
        assertThat(op.reason()).isEqualTo(Reason.OBJECT_DELETED);
        assertThat(op.state()).isEqualTo(State.QUEUED);
        assertThat(cluster.events()).anyMatch(e -> e.event().equals("orphan") && late.equals(e.chunkId()));
        t.assertNoViolations();
    }

    @Test
    void edge16_aFailedUploadDeletesExactlyThePairsItAttempted() {
        t.join("worker1");
        t.join("worker2");
        String failed = "eeeeeeee-7d0e-4c1b-9a54-1f2e3d4c5b6a";
        cluster.write(() -> cluster.beginUpload(failed));
        List<NamespaceTasks.Attempt> attempts = List.of(new NamespaceTasks.Attempt(failed + "-00000", "worker1"),
                new NamespaceTasks.Attempt(failed + "-00000", "worker2"), new NamespaceTasks.Attempt(failed + "-00001", "worker1"));

        cluster.write(() -> t.queue.offer(t.namespaceTasks.endUpload(failed, attempts)));
        t.sync();

        cluster.read(() -> {
            assertThat(cluster.isGarbage(failed)).isTrue();
            assertThat(cluster.pendingUploads()).doesNotContain(failed);
            assertThat(ops.size()).isEqualTo(3);
            for (NamespaceTasks.Attempt attempt : attempts) {
                Op op = ops.get(attempt.chunkId(), attempt.workerId());
                assertThat(op.reason()).isEqualTo(Reason.UPLOAD_FAILED);
                assertThat(op.state()).isEqualTo(State.QUEUED);
            }
        });
        t.assertNoViolations();
    }

    @Test
    void anUndeliveredDeleteGoesBackAtTheSameVersionButOnlyIfNothingChangedMeanwhile() {
        TestSupport t = new TestSupport();
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        Op op = t.cluster.write(() -> t.cluster.ops().ensure(chunk(0), "worker1", Reason.OBJECT_DELETED));
        long version = op.version();
        t.cluster.write(() -> t.cluster.takeDeletes(t.cluster.worker("worker1"), java.util.Set.of(chunk(0)), 50));

        t.cluster.write(() -> t.cluster.ops().undelivered(chunk(0), "worker1", version + 2));   // another version
        assertThat(op.state()).as("not this op: unchanged").isEqualTo(State.SENT);

        t.cluster.write(() -> t.cluster.ops().undelivered(chunk(0), "worker1", version));
        assertThat(op.state()).isEqualTo(State.QUEUED);
        assertThat(op.version()).isEqualTo(version);
        assertThat(t.handOut("worker1")).extracting(Command::version).containsExactly(version);
        t.assertNoViolations();
    }
}
