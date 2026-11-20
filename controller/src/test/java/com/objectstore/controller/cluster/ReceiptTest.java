package com.objectstore.controller.cluster;

import static com.objectstore.controller.cluster.TestSupport.chunk;
import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.ChangeType;
import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.dto.ReportEntry;
import com.objectstore.common.dto.ReportSegment;
import com.objectstore.controller.cluster.ClusterState.ReplicaState;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.ClusterState.WorkerState;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Block 12 (v4 §4.6, §4.7, §4.11): the receipt step for changes, segments and PUT answers. */
class ReceiptTest {

    private final TestSupport t = new TestSupport();
    private final ClusterState cluster = t.cluster;
    private String boot;

    @BeforeEach
    void setUp() {
        for (int i = 0; i < 10; i++) {
            t.knowChunk(chunk(i));
        }
        boot = t.join("worker1");
    }

    private static Change received(long seq, String chunkId) {
        return new Change(seq, ChangeType.RECEIVED, chunkId, 10L, TestSupport.SUM, null, null, null);
    }

    private static Change deleted(long seq, String chunkId) {
        return new Change(seq, ChangeType.DELETED, chunkId, null, null, null, null, null);
    }

    private WorkerInfo worker() {
        return cluster.worker("worker1");
    }

    @Test
    void duplicatesAndOverlapsAreAppliedOnce() {
        HeartbeatResponse first = t.send("worker1", List.of(received(1, chunk(0)), received(2, chunk(1))), null);
        assertThat(first.changesReceivedUpTo()).isEqualTo(2);

        // An overlap: 1 and 2 again with 3. Only 3 is new.
        HeartbeatResponse overlap = t.send("worker1", List.of(received(1, chunk(0)), deleted(2, chunk(1)), received(3, chunk(2))), null);
        assertThat(overlap.changesReceivedUpTo()).isEqualTo(3);
        cluster.locked(() -> {
            assertThat(cluster.chunksOn("worker1")).containsExactlyInAnyOrder(chunk(0), chunk(1), chunk(2));
            assertThat(worker().applied).isEqualTo(3);
        });

        // The same heartbeat sequence number again: only the acknowledgement is echoed.
        long hb = t.fake("worker1").hbSeq;
        HeartbeatResponse duplicate = t.registry.heartbeat(new HeartbeatRequest("worker1", boot, hb, InternalApi.nowSeconds(),
                TestSupport.STATS, List.of(deleted(4, chunk(0))), null));
        assertThat(duplicate.ignored()).isEqualTo("duplicate");
        assertThat(duplicate.changesReceivedUpTo()).isEqualTo(3);
        assertThat(cluster.locked(() -> cluster.chunksOn("worker1"))).contains(chunk(0));
        t.assertNoViolations();
    }

    @Test
    void edge2_aGapRequestsAReportAndNeverAcknowledgesPastIt() {
        t.send("worker1", List.of(received(1, chunk(0))), null);

        HeartbeatResponse gap = t.send("worker1", List.of(received(3, chunk(2)), received(4, chunk(3))), null);

        assertThat(gap.changesReceivedUpTo()).isEqualTo(1);
        assertThat(gap.requestFullReport()).isTrue();
        assertThat(cluster.counter("changes_gap")).isEqualTo(1);
        assertThat(cluster.locked(() -> cluster.chunksOn("worker1"))).containsExactly(chunk(0));
        // The request is sent once.
        assertThat(t.send("worker1", List.of(), null).requestFullReport()).isFalse();

        // Once the missing change arrives, the run continues.
        HeartbeatResponse filled = t.send("worker1", List.of(received(2, chunk(1)), received(3, chunk(2)), received(4, chunk(3))), null);
        assertThat(filled.changesReceivedUpTo()).isEqualTo(4);
    }

    @Test
    void onlyTheLongestContiguousRunIsAccepted() {
        HeartbeatResponse response = t.send("worker1", List.of(received(1, chunk(0)), received(2, chunk(1)), received(4, chunk(3))), null);

        assertThat(response.changesReceivedUpTo()).isEqualTo(2);
        assertThat(cluster.locked(() -> cluster.chunksOn("worker1"))).containsExactlyInAnyOrder(chunk(0), chunk(1));
    }

    @Test
    void edge30_aFullQueueAcknowledgesNothingButLivenessIsStillUpdated() {
        t.receipts.setSink(tasks -> false);                          // a full queue
        cluster.locked(() -> {
            worker().state = WorkerState.STALE;
            worker().lastSeen = cluster.now() - 10_000_000_000L;
        });

        HeartbeatResponse response = t.send("worker1", List.of(received(1, chunk(0))),
                new ReportSegment("r-full", 0, true, 1, List.of()));

        assertThat(response.changesReceivedUpTo()).isZero();
        assertThat(response.segmentAck()).isNull();
        assertThat(cluster.counter("apply_queue_full")).isEqualTo(2);
        cluster.locked(() -> {
            assertThat(worker().state).isEqualTo(WorkerState.ALIVE);
            assertThat(cluster.now() - worker().lastSeen).isLessThan(1_000_000_000L);
        });
    }

    @Test
    void segmentsAreAppliedOnceInOrderAndResentOnesAreOnlyReacknowledged() {
        List<ReportEntry> first = List.of(new ReportEntry(chunk(0), 10, TestSupport.SUM, false));
        List<ReportEntry> second = List.of(new ReportEntry(chunk(1), 10, TestSupport.SUM, false));
        List<ReportEntry> third = List.of(new ReportEntry(chunk(2), 10, TestSupport.SUM, false));

        assertThat(t.send("worker1", List.of(), new ReportSegment("r2", 0, false, 0, first)).segmentAck().index()).isZero();
        assertThat(t.send("worker1", List.of(), new ReportSegment("r2", 0, false, 0, first)).segmentAck().index()).isZero();
        assertThat(t.send("worker1", List.of(), new ReportSegment("r2", 2, true, 0, third)).segmentAck())
                .as("a segment that skips ahead is ignored").isNull();
        assertThat(cluster.locked(() -> worker().session.listed)).isEqualTo(1);
        assertThat(t.send("worker1", List.of(), new ReportSegment("r2", 1, false, 0, second)).segmentAck().index()).isEqualTo(1);
        assertThat(t.send("worker1", List.of(), new ReportSegment("r2", 2, true, 0, third)).segmentAck().index()).isEqualTo(2);
        cluster.locked(() -> {
            assertThat(worker().session).isNull();
            assertThat(worker().hasReported).isTrue();
            assertThat(cluster.chunksOn("worker1")).containsExactlyInAnyOrder(chunk(0), chunk(1), chunk(2));
        });

        // A resent last segment after completion: re-acknowledged, nothing re-applied.
        long reportedAt = cluster.locked(() -> worker().lastReportAt);
        assertThat(t.send("worker1", List.of(), new ReportSegment("r2", 2, true, 0, third)).segmentAck().index()).isEqualTo(2);
        assertThat(t.send("worker1", List.of(), new ReportSegment("r2", 0, false, 0, first)).segmentAck().index()).isZero();
        cluster.locked(() -> {
            assertThat(worker().session).as("no new session for a completed report").isNull();
            assertThat(worker().lastReportAt).isEqualTo(reportedAt);
        });
        t.assertNoViolations();
    }

    @Test
    void edge1_theLastSegmentMovesReceivedToTheBaseline() {
        t.send("worker1", List.of(received(1, chunk(0))), null);
        // The worker made changes 2..10 (lost on the way) and then listed its disk with baseline 10.
        HeartbeatResponse response = t.send("worker1", List.of(),
                new ReportSegment("r3", 0, true, 10, List.of(new ReportEntry(chunk(5), 10, TestSupport.SUM, false))));

        assertThat(response.changesReceivedUpTo()).isEqualTo(10);
        cluster.locked(() -> {
            assertThat(worker().applied).isEqualTo(10);
            assertThat(cluster.chunksOn("worker1")).containsExactly(chunk(5));
        });
        // Changes up to the baseline are old news; 11 is the next one.
        HeartbeatResponse next = t.send("worker1", List.of(deleted(9, chunk(5)), received(11, chunk(6))), null);
        assertThat(next.changesReceivedUpTo()).isEqualTo(11);
        assertThat(cluster.locked(() -> cluster.chunksOn("worker1"))).containsExactlyInAnyOrder(chunk(5), chunk(6));
    }

    @Test
    void edge12_aPutAnswerFromAnotherBootIsIgnored() {
        t.receipts.acceptPiggyback("worker1", "some-earlier-boot", List.of(received(1, chunk(0))));

        assertThat(cluster.counter("piggyback_ignored")).isEqualTo(1);
        cluster.locked(() -> {
            assertThat(worker().received).isZero();
            assertThat(cluster.chunksOn("worker1")).isEmpty();
        });

        // From the current boot it counts like a heartbeat's changes, without touching liveness.
        long seenBefore = cluster.locked(() -> worker().lastSeen);
        t.receipts.acceptPiggyback("worker1", boot, List.of(received(1, chunk(0))));
        t.sync();
        cluster.locked(() -> {
            assertThat(worker().received).isEqualTo(1);
            assertThat(cluster.replica(chunk(0), "worker1").state).isEqualTo(ReplicaState.OK);
            assertThat(worker().lastSeen).isEqualTo(seenBefore);
        });
    }

    @Test
    void edge37_aHeartbeatThatArrivesTooLateQueuesNothing() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> t.registry.heartbeat(new HeartbeatRequest("worker1", boot,
                        t.fake("worker1").hbSeq + 1, InternalApi.nowSeconds() - 60, TestSupport.STATS,
                        List.of(received(1, chunk(0))), null)))
                .hasMessageContaining("older than");
        assertThat(cluster.locked(() -> worker().received)).isZero();
        // Resent fresh, it is accepted.
        assertThat(t.send("worker1", List.of(received(1, chunk(0))), null).changesReceivedUpTo()).isEqualTo(1);
    }

    @Test
    void edge36_aReorderedOlderHeartbeatOnlyEchoesTheAcknowledgement() {
        t.send("worker1", List.of(received(1, chunk(0)), received(2, chunk(1))), null);
        long old = t.fake("worker1").hbSeq - 1;   // an older heartbeat, delivered late, carrying a delete
        HeartbeatResponse late = t.registry.heartbeat(new HeartbeatRequest("worker1", boot, old, InternalApi.nowSeconds(),
                TestSupport.STATS, List.of(received(1, chunk(0)), deleted(2, chunk(1))), null));

        assertThat(late.ignored()).isEqualTo("duplicate");
        assertThat(late.changesReceivedUpTo()).isEqualTo(2);
        assertThat(cluster.locked(() -> cluster.chunksOn("worker1"))).containsExactlyInAnyOrder(chunk(0), chunk(1));
    }

    @Test
    void edge22_aCommitDuringAnOpenSessionKeepsTheReceiptsMark() {
        String object = "cccccccc-7d0e-4c1b-9a54-1f2e3d4c5b6a";
        String pendingChunk = object + "-00000";
        cluster.locked(() -> cluster.beginUpload(object));
        // Segment 0 of a two-segment report lists the not-yet-committed chunk: a receipt seen in this session.
        t.send("worker1", List.of(), new ReportSegment("r4", 0, false, 0, List.of(new ReportEntry(pendingChunk, 10, TestSupport.SUM, false))));
        long session = cluster.locked(() -> worker().session.number);
        assertThat(cluster.locked(() -> cluster.receipt(pendingChunk, "worker1").seenInSession)).isEqualTo(session);

        // The upload commits (as UploadService does): the receipt becomes a replica with the receipt's mark.
        cluster.locked(() -> {
            cluster.putChunk(pendingChunk, new ClusterState.ChunkInfo(object, 0, 10, TestSupport.SUM));
            ClusterState.Receipt receipt = cluster.removeReceipt(pendingChunk, "worker1");
            cluster.addReplica(pendingChunk, "worker1", ReplicaState.OK).mark = receipt.seenInSession;
            cluster.endUpload(object);
        });
        t.send("worker1", List.of(), new ReportSegment("r4", 1, true, 0, List.of()));

        assertThat(cluster.locked(() -> cluster.replica(pendingChunk, "worker1"))).as("kept by the report").isNotNull();
        t.assertNoViolations();
    }

    @Test
    void edge9_aCommitRecordsACopyWhosePutAnswerWasLostFromItsReceipt() {
        t.join("worker2");
        String object = "abababab-7d0e-4c1b-9a54-1f2e3d4c5b6a";
        String chunkId = object + "-00000";
        cluster.write(() -> cluster.beginUpload(object));
        // worker1's PUT answer was lost, so the upload used worker2 instead; worker1's RECEIVED arrives anyway.
        t.send("worker1", List.of(received(1, chunkId)), null);
        long epoch2 = cluster.read(() -> cluster.worker("worker2").epoch);

        cluster.write(() -> t.queue.offer(t.namespaceTasks.commitObject(object, List.of(new NamespaceTasks.CommittedChunk(
                new com.objectstore.controller.domain.Chunk(chunkId, object, 0, 10, TestSupport.SUM),
                List.of(new NamespaceTasks.Target("worker2", epoch2)))))));
        t.sync();

        cluster.read(() -> {
            assertThat(cluster.replica(chunkId, "worker1")).as("recorded from its receipt").isNotNull();
            assertThat(cluster.pendingOf(chunkId)).extracting(ClusterState.PendingCopy::target).containsExactly("worker2");
            assertThat(cluster.receipts()).doesNotContainKey(chunkId);
        });
        t.assertNoViolations();
    }

    @Test
    void edge33_aReportRequestIsHeldBackWhileTheWorkerIsReporting() {
        cluster.write(() -> worker().fullReportRequested = true);
        HeartbeatResponse during = t.send("worker1", List.of(), new ReportSegment("r9", 0, false, 0, List.of()));
        assertThat(during.requestFullReport()).as("not sent into a report in progress").isFalse();
        HeartbeatResponse last = t.send("worker1", List.of(), new ReportSegment("r9", 1, true, 0, List.of()));
        assertThat(last.requestFullReport()).as("nor with its last segment").isFalse();

        assertThat(t.send("worker1", List.of(), null).requestFullReport()).as("sent once it is over").isTrue();
        assertThat(t.send("worker1", List.of(), null).requestFullReport()).isFalse();
    }
}
