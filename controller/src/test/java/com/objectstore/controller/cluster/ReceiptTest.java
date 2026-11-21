package com.objectstore.controller.cluster;

import static com.objectstore.controller.cluster.TestSupport.chunk;
import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.dto.ReportEntry;
import com.objectstore.common.dto.ReportSegment;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.ClusterState.WorkerState;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The receipt step for changes, segments and PUT answers (v4 §4.6; v5 §5.4 acceptance and resync signals). */
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

    /** A worker observation (odd version), numbered explicitly. */
    private static Change received(long seq, String chunkId) {
        return Change.received(seq, chunkId, 10L, TestSupport.SUM, 2 * seq + 1, null);
    }

    private static Change deleted(long seq, String chunkId) {
        return Change.deleted(seq, chunkId, 2 * seq + 1, "deleted");
    }

    private WorkerInfo worker() {
        return cluster.worker("worker1");
    }

    /** The worker asks for a resync; the fake adopts the fence the reply hands it. */
    private void resync() {
        t.send("worker1", List.of(), null, InternalApi.RESYNC_TOMBSTONES);
        assertThat(cluster.read(() -> worker().resyncing)).isTrue();
    }

    private ReportSegment segment(String id, int index, boolean last, long baseline, List<ReportEntry> entries) {
        TestSupport.Fake fake = t.fake("worker1");
        return new ReportSegment(id, fake.fence, fake.reportAttempt, index, last, baseline, entries);
    }

    @Test
    void duplicatesAndOverlapsAreAppliedOnce() {
        HeartbeatResponse first = t.send("worker1", List.of(received(1, chunk(0)), received(2, chunk(1))), null);
        assertThat(first.changesReceivedUpTo()).isEqualTo(2);

        HeartbeatResponse overlap = t.send("worker1", List.of(received(1, chunk(0)), deleted(2, chunk(1)), received(3, chunk(2))), null);
        assertThat(overlap.changesReceivedUpTo()).isEqualTo(3);
        cluster.locked(() -> {
            assertThat(cluster.chunksOn("worker1")).containsExactlyInAnyOrder(chunk(0), chunk(1), chunk(2));
            assertThat(worker().applied).isEqualTo(3);
        });

        long hb = t.fake("worker1").hbSeq;
        HeartbeatResponse duplicate = t.registry.heartbeat(new HeartbeatRequest("worker1", boot, hb, InternalApi.nowSeconds(),
                TestSupport.STATS, List.of(deleted(4, chunk(0))), null, null));
        assertThat(duplicate.ignored()).isEqualTo("duplicate");
        assertThat(duplicate.changesReceivedUpTo()).isEqualTo(3);
        assertThat(cluster.locked(() -> cluster.chunksOn("worker1"))).contains(chunk(0));
        t.assertNoViolations();
    }

    @Test
    void edge2_aGapStartsAResyncAndNeverAcknowledgesPastIt() {
        t.send("worker1", List.of(received(1, chunk(0))), null);

        HeartbeatResponse gap = t.send("worker1", List.of(received(3, chunk(2)), received(4, chunk(3))), null);

        assertThat(gap.changesReceivedUpTo()).isEqualTo(1);
        assertThat(gap.resync()).isNotNull();
        assertThat(gap.resync().attempt()).isEqualTo(1);
        assertThat(cluster.counter("changes_gap")).isEqualTo(1);
        assertThat(cluster.locked(() -> cluster.chunksOn("worker1"))).containsExactly(chunk(0));
        // The reply repeats the resync until segment 0 arrives; another gap before that changes nothing.
        HeartbeatResponse again = t.send("worker1", List.of(received(3, chunk(2))), null);
        assertThat(again.resync()).isEqualTo(gap.resync());

        // Changes up to the baseline still count while the listing is being made.
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
        resync();
        t.receipts.setSink(tasks -> false);                          // a full queue
        cluster.locked(() -> {
            worker().state = WorkerState.STALE;
            worker().lastSeen = cluster.now() - 10_000_000_000L;
        });

        HeartbeatResponse response = t.send("worker1", List.of(received(1, chunk(0))), segment("r-full", 0, true, 1, List.of()));

        assertThat(response.changesReceivedUpTo()).isZero();
        assertThat(response.segmentAck()).as("not taken: resent").isNull();
        assertThat(cluster.counter("apply_queue_full")).isEqualTo(2);
        cluster.locked(() -> {
            assertThat(worker().state).isEqualTo(WorkerState.ALIVE);
            assertThat(cluster.now() - worker().lastSeen).isLessThan(1_000_000_000L);
        });
    }

    @Test
    void segmentsAreAppliedOnceInOrderAndDuplicatesOrOldOnesApplyNothing() {
        resync();
        List<ReportEntry> first = List.of(TestSupport.entry(chunk(0), 0));
        List<ReportEntry> second = List.of(TestSupport.entry(chunk(1), 0));
        List<ReportEntry> third = List.of(TestSupport.entry(chunk(2), 0));

        assertThat(t.send("worker1", List.of(), segment("r2", 0, false, 0, first)).segmentAck().status()).isEqualTo("ok");
        assertThat(t.send("worker1", List.of(), segment("r2", 0, false, 0, first)).segmentAck().status())
                .as("a duplicate index: acknowledged again").isEqualTo("ok");
        assertThat(t.send("worker1", List.of(), segment("r2", 2, true, 0, third)).segmentAck().status())
                .as("a segment that skips ahead").isEqualTo("stale");
        assertThat(cluster.locked(() -> worker().session.listed)).isEqualTo(1);
        assertThat(t.send("worker1", List.of(), segment("r2", 1, false, 0, second)).segmentAck().status()).isEqualTo("ok");
        assertThat(t.send("worker1", List.of(), segment("r2", 2, true, 0, third)).segmentAck().status()).isEqualTo("ok");
        cluster.locked(() -> {
            assertThat(worker().session).isNull();
            assertThat(worker().resyncing).isFalse();
            assertThat(worker().hasReported).isTrue();
            assertThat(cluster.chunksOn("worker1")).containsExactlyInAnyOrder(chunk(0), chunk(1), chunk(2));
        });

        // Segments of the completed attempt, resent: "done", nothing applied twice.
        long reportedAt = cluster.locked(() -> worker().lastReportAt);
        assertThat(t.send("worker1", List.of(), segment("r2", 2, true, 0, third)).segmentAck().status()).isEqualTo("done");
        assertThat(t.send("worker1", List.of(), segment("r2", 0, false, 0, first)).segmentAck().status()).isEqualTo("done");
        cluster.locked(() -> {
            assertThat(worker().session).as("no new session for a completed attempt").isNull();
            assertThat(worker().lastReportAt).isEqualTo(reportedAt);
        });
        t.assertNoViolations();
    }

    @Test
    void edge1_theLastSegmentMovesReceivedToTheBaseline() {
        t.send("worker1", List.of(received(1, chunk(0))), null);
        resync();
        // The worker made changes 2..10 (lost on the way) and then listed its disk with baseline 10.
        HeartbeatResponse response = t.send("worker1", List.of(),
                segment("r3", 0, true, 10, List.of(TestSupport.entry(chunk(5), 0))));

        assertThat(response.changesReceivedUpTo()).isEqualTo(10);
        cluster.locked(() -> {
            assertThat(worker().applied).isEqualTo(10);
            assertThat(cluster.chunksOn("worker1")).containsExactly(chunk(5));
        });
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

        long seenBefore = cluster.locked(() -> worker().lastSeen);
        t.receipts.acceptPiggyback("worker1", boot, List.of(received(1, chunk(0))));
        t.sync();
        cluster.locked(() -> {
            assertThat(worker().received).isEqualTo(1);
            assertThat(cluster.replica(chunk(0), "worker1")).isNotNull();
            assertThat(worker().lastSeen).isEqualTo(seenBefore);
        });
    }

    @Test
    void edge37_aHeartbeatThatArrivesTooLateQueuesNothing() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> t.registry.heartbeat(new HeartbeatRequest("worker1", boot,
                        t.fake("worker1").hbSeq + 1, InternalApi.nowSeconds() - 60, TestSupport.STATS,
                        List.of(received(1, chunk(0))), null, null)))
                .hasMessageContaining("older than");
        assertThat(cluster.locked(() -> worker().received)).isZero();
        assertThat(t.send("worker1", List.of(received(1, chunk(0))), null).changesReceivedUpTo()).isEqualTo(1);
    }

    @Test
    void edge36_aReorderedOlderHeartbeatOnlyEchoesTheAcknowledgement() {
        t.send("worker1", List.of(received(1, chunk(0)), received(2, chunk(1))), null);
        long old = t.fake("worker1").hbSeq - 1;
        HeartbeatResponse late = t.registry.heartbeat(new HeartbeatRequest("worker1", boot, old, InternalApi.nowSeconds(),
                TestSupport.STATS, List.of(received(1, chunk(0)), deleted(2, chunk(1))), null, null));

        assertThat(late.ignored()).isEqualTo("duplicate");
        assertThat(late.changesReceivedUpTo()).isEqualTo(2);
        assertThat(cluster.locked(() -> cluster.chunksOn("worker1"))).containsExactlyInAnyOrder(chunk(0), chunk(1));
    }

    @Test
    void edge22_aCommitDuringAnOpenSessionKeepsTheReceiptsMark() {
        String object = "cccccccc-7d0e-4c1b-9a54-1f2e3d4c5b6a";
        String pendingChunk = object + "-00000";
        cluster.locked(() -> cluster.beginUpload(object));
        resync();
        t.send("worker1", List.of(), segment("r4", 0, false, 0, List.of(TestSupport.entry(pendingChunk, 0))));
        long session = cluster.locked(() -> worker().session.number);
        assertThat(cluster.locked(() -> cluster.receipt(pendingChunk, "worker1").seenInSession)).isEqualTo(session);

        cluster.locked(() -> {
            cluster.putChunk(pendingChunk, new ClusterState.ChunkInfo(object, 0, 10, TestSupport.SUM));
            ClusterState.Receipt receipt = cluster.removeReceipt(pendingChunk, "worker1");
            cluster.addReplica(pendingChunk, "worker1", receipt.version).mark = receipt.seenInSession;
            cluster.endUpload(object);
        });
        t.send("worker1", List.of(), segment("r4", 1, true, 0, List.of()));

        assertThat(cluster.locked(() -> cluster.replica(pendingChunk, "worker1"))).as("kept by the listing").isNotNull();
        t.assertNoViolations();
    }

    @Test
    void edge9_aCommitRecordsACopyWhosePutAnswerWasLostFromItsReceiptAndTheOtherAsAnUploadOp() {
        t.join("worker2");
        String object = "abababab-7d0e-4c1b-9a54-1f2e3d4c5b6a";
        String chunkId = object + "-00000";
        cluster.write(() -> cluster.beginUpload(object));
        long v1 = t.versions.next();
        long v2 = t.versions.next();
        // worker1's PUT answer was lost, so the upload used worker2 instead; worker1's RECEIVED arrives anyway.
        t.send("worker1", List.of(Change.received(1, chunkId, 10, TestSupport.SUM, v1, null)), null);
        long epoch2 = cluster.read(() -> cluster.worker("worker2").epoch);

        cluster.write(() -> t.queue.offer(t.namespaceTasks.commitObject(object, List.of(new NamespaceTasks.CommittedChunk(
                new com.objectstore.controller.domain.Chunk(chunkId, object, 0, 10, TestSupport.SUM),
                List.of(new NamespaceTasks.Target("worker2", epoch2, v2)))))));
        t.sync();

        cluster.read(() -> {
            assertThat(cluster.replica(chunkId, "worker1")).as("recorded from its receipt").isNotNull();
            assertThat(cluster.replica(chunkId, "worker1").version).isEqualTo(v1);
            PendingOps.Op upload = cluster.ops().get(chunkId, "worker2");
            assertThat(upload.reason()).isEqualTo(PendingOps.Reason.UPLOAD);
            assertThat(upload.version()).as("the PUT's own version").isEqualTo(v2);
            assertThat(cluster.receipts()).doesNotContainKey(chunkId);
        });
        t.assertNoViolations();
    }

    @Test
    void edge33_repeatedResyncSignalsCauseOnlyOneNewAttempt() {
        resync();
        int attempt = cluster.read(() -> worker().attempt);
        t.send("worker1", List.of(), null, InternalApi.RESYNC_OVERFLOW);   // before segment 0: ignored
        assertThat(cluster.read(() -> worker().attempt)).isEqualTo(attempt);

        t.send("worker1", List.of(), segment("r9", 0, false, 0, List.of()));
        t.send("worker1", List.of(), null, InternalApi.RESYNC_OVERFLOW);   // after segment 0: the next attempt
        t.send("worker1", List.of(), null, InternalApi.RESYNC_OVERFLOW);   // repeated: still one
        assertThat(cluster.read(() -> worker().attempt)).isEqualTo(attempt + 1);
        assertThat(t.fake("worker1").reportAttempt).as("the reply asked for it").isEqualTo(attempt + 1);
    }
}
