package com.objectstore.controller.cluster;

import static com.objectstore.controller.cluster.TestSupport.chunk;
import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.ChangeType;
import com.objectstore.common.dto.Command;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.dto.ReportEntry;
import com.objectstore.controller.cluster.ClusterState.ReplicaState;
import com.objectstore.controller.cluster.ClusterState.WorkerState;
import com.objectstore.controller.cluster.DeletionLedger.Entry;
import com.objectstore.controller.cluster.DeletionLedger.State;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Block 14 (v4 §6.1, §6.2, §6.5, §10 rows 14-16): the deletion ledger and the command queues. */
class DeletionLedgerTest {

    private final TestSupport t = new TestSupport();
    private final ClusterState cluster = t.cluster;
    private final DeletionLedger ledger = cluster.ledger();

    private Entry entry(String chunkId, String workerId) {
        return cluster.read(() -> ledger.entry(chunkId, workerId));
    }

    private HeartbeatResponse deleted(String workerId, String... chunkIds) {
        List<Change> changes = new ArrayList<>();
        for (String chunkId : chunkIds) {
            changes.add(t.next(workerId, ChangeType.DELETED, chunkId, null, null, null));
        }
        return t.send(workerId, changes, null);
    }

    @Test
    void anEntryGoesWaitingQueuedDeliveredAndIsCompletedByTheWorkersDeleted() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        cluster.write(() -> {
            cluster.worker("worker1").state = WorkerState.STALE;          // cannot take it now
            ledger.request("aaaaaaaa-7d0e-4c1b-9a54-1f2e3d4c5b6a-00000", "worker1", DeletionLedger.ORPHAN);
        });
        String orphan = "aaaaaaaa-7d0e-4c1b-9a54-1f2e3d4c5b6a-00000";
        assertThat(entry(orphan, "worker1").state).isEqualTo(State.WAITING);

        cluster.write(() -> {
            cluster.worker("worker1").state = WorkerState.ALIVE;
            assertThat(ledger.retryWaiting(10)).isEqualTo(1);
        });
        assertThat(entry(orphan, "worker1").state).isEqualTo(State.QUEUED);
        assertThat(cluster.read(() -> cluster.worker("worker1").commands.stream().toList()))
                .singleElement().satisfies(c -> assertThat(c.cmdId()).isEqualTo(entry(orphan, "worker1").cmdId));

        HeartbeatResponse reply = t.send("worker1", List.of(), null);
        assertThat(reply.commands()).singleElement().satisfies(c -> {
            assertThat(c.type()).isEqualTo(InternalApi.COMMAND_DELETE);
            assertThat(c.reason()).isEqualTo("orphan");
        });
        Entry delivered = entry(orphan, "worker1");
        assertThat(delivered.state).isEqualTo(State.DELIVERED);
        assertThat(delivered.deadline).isGreaterThan(cluster.now());

        deleted("worker1", orphan);
        assertThat(entry(orphan, "worker1")).isNull();
        assertThat(cluster.read(ledger::isEmpty)).isTrue();
        t.assertNoViolations();
    }

    @Test
    void thereIsOneEntryPerPairAndANamespaceReasonReplacesExcess() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        cluster.write(() -> {
            cluster.replica(chunk(0), "worker1").state = ReplicaState.EXCESS;
            cluster.sendDelete("worker1", chunk(0), "excess");
            Entry first = ledger.entry(chunk(0), "worker1");
            assertThat(ledger.request(chunk(0), "worker1", "excess")).isSameAs(first);
            assertThat(ledger.request(chunk(0), "worker1", DeletionLedger.OBJECT_DELETED)).isSameAs(first);
            assertThat(first.reason).isEqualTo(DeletionLedger.OBJECT_DELETED);
            assertThat(ledger.size()).isEqualTo(1);
            assertThat(cluster.worker("worker1").commands).hasSize(1);
        });
    }

    @Test
    void edge15_waitingAndQueuedEntriesCanBeCancelledButADeliveredOneCannot() {
        t.knowChunk(chunk(0));
        t.knowChunk(chunk(1));
        t.join("worker1", chunk(0), chunk(1));
        t.join("worker2", chunk(0), chunk(1));
        t.join("worker3", chunk(0), chunk(1));
        cluster.write(() -> {
            cluster.replica(chunk(0), "worker3").state = ReplicaState.EXCESS;
            cluster.sendDelete("worker3", chunk(0), "excess");             // QUEUED
            assertThat(ledger.cancel(chunk(0), "worker3")).isTrue();
            assertThat(cluster.worker("worker3").commands).as("its command is removed").isEmpty();
            assertThat(cluster.replica(chunk(0), "worker3").deleteCmdId).isNull();

            cluster.replica(chunk(1), "worker3").state = ReplicaState.EXCESS;
            cluster.sendDelete("worker3", chunk(1), "excess");
            cluster.takeCommands(cluster.worker("worker3"));                 // DELIVERED
            assertThat(ledger.cancel(chunk(1), "worker3")).isFalse();
            assertThat(ledger.entry(chunk(1), "worker3").state).isEqualTo(State.DELIVERED);
            assertThat(cluster.replica(chunk(1), "worker3").state).isEqualTo(ReplicaState.EXCESS);
            assertThat(cluster.replica(chunk(1), "worker3").deleteDelivered).isTrue();
        });
        t.assertNoViolations();
    }

    @Test
    void protectCancelsAnUndeliveredExcessDeletionWhenTheChunkFallsBelowRf() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2", chunk(0));
        t.join("worker3", chunk(0));
        cluster.write(() -> {
            cluster.replica(chunk(0), "worker3").state = ReplicaState.EXCESS;
            cluster.sendDelete("worker3", chunk(0), "excess");
            cluster.removeReplica(chunk(0), "worker1");                      // a live copy is lost
            assertThat(ledger.entry(chunk(0), "worker3")).isNull();
            assertThat(cluster.replica(chunk(0), "worker3").state).isEqualTo(ReplicaState.OK);
            assertThat(cluster.worker("worker3").commands).isEmpty();
        });
        t.assertNoViolations();
    }

    @Test
    void edge14_aDeliveredDeleteThatIsNotConfirmedInTimeIsSentAgain() {
        t.knowChunk(chunk(0));
        t.join("worker1");
        String orphan = "bbbbbbbb-7d0e-4c1b-9a54-1f2e3d4c5b6a-00000";
        cluster.write(() -> ledger.request(orphan, "worker1", DeletionLedger.ORPHAN));
        HeartbeatResponse lost = t.send("worker1", List.of(), null);        // the reply is lost on the way
        assertThat(lost.commands()).hasSize(1);
        String firstCmd = lost.commands().get(0).cmdId();

        cluster.write(() -> assertThat(ledger.expireDeadlines(cluster.now())).isZero());   // not yet
        cluster.write(() -> assertThat(ledger.expireDeadlines(cluster.now() + t.properties.pendingTimeout().toNanos() + 1))
                .isEqualTo(1));

        Entry again = entry(orphan, "worker1");
        assertThat(again.state).isEqualTo(State.QUEUED);
        assertThat(again.attempts).isEqualTo(1);
        assertThat(again.cmdId).isNotEqualTo(firstCmd);
        assertThat(t.send("worker1", List.of(), null).commands()).singleElement()
                .satisfies(c -> assertThat(c.chunkId()).isEqualTo(orphan));
        deleted("worker1", orphan);
        assertThat(cluster.read(ledger::isEmpty)).isTrue();
    }

    @Test
    void deadPausesTheEntriesAndTheNextProcessResumesThemAfterItsReport() {
        t.join("worker1");
        String orphan = "cccccccc-7d0e-4c1b-9a54-1f2e3d4c5b6a-00000";
        cluster.write(() -> ledger.request(orphan, "worker1", DeletionLedger.ORPHAN));
        long epoch = cluster.read(() -> cluster.worker("worker1").epoch);
        cluster.write(() -> cluster.worker("worker1").lastSeen = cluster.now() - 10_000_000_000L);
        cluster.write(() -> t.queue.offer(t.registry.markDead("worker1", epoch)));
        t.sync();

        cluster.read(() -> {
            assertThat(cluster.worker("worker1").isDead()).isTrue();
            assertThat(ledger.entry(orphan, "worker1").state).isEqualTo(State.WAITING);
            assertThat(cluster.worker("worker1").commands).isEmpty();
        });

        t.register("worker1");                                                 // a new process
        assertThat(entry(orphan, "worker1").state).as("not before its report").isEqualTo(State.WAITING);
        t.fullReport("worker1", t.fake("worker1").boot);
        assertThat(entry(orphan, "worker1").state).isEqualTo(State.QUEUED);
        t.assertNoViolations();
    }

    @Test
    void edge15_deletingAnObjectWith50000ChunksNeverOverfillsAQueueAndDropsNothing() {
        int chunks = 50_000;
        List<ReportEntry> entries = new ArrayList<>(chunks);
        List<String> ids = new ArrayList<>(chunks);
        for (int i = 0; i < chunks; i++) {
            t.knowChunk(chunk(i));
            entries.add(new ReportEntry(chunk(i), 10, TestSupport.SUM, false));
            ids.add(chunk(i));
        }
        t.register("worker1");
        t.report("worker1", entries);
        cluster.write(() -> t.queue.offer(t.namespaceTasks.deleteObject(TestSupport.OBJECT, ids)));
        t.sync();

        int max = t.properties.maxCommandsPerWorker();
        Set<String> deletes = new HashSet<>();
        int heartbeats = 0;
        List<String> confirm = new ArrayList<>();
        while (!cluster.read(ledger::isEmpty)) {
            assertThat(cluster.read(() -> cluster.worker("worker1").commands.size())).isLessThanOrEqualTo(max);
            HeartbeatResponse reply = deleted("worker1", confirm.toArray(String[]::new));
            confirm.clear();
            for (Command command : reply.commands()) {
                assertThat(command.type()).isEqualTo(InternalApi.COMMAND_DELETE);
                deletes.add(command.chunkId());
                confirm.add(command.chunkId());
            }
            if (++heartbeats % 20 == 0) {
                cluster.write(() -> ledger.retryWaiting(t.properties.cleanupBudget()));   // the monitor tick
            }
            assertThat(heartbeats).isLessThan(5000);
        }
        assertThat(deletes).hasSize(chunks);
        assertThat(cluster.counter("commands_dropped")).isZero();
        assertThat(cluster.counter("commands_expired")).isZero();
        t.assertNoViolations();
    }
}
