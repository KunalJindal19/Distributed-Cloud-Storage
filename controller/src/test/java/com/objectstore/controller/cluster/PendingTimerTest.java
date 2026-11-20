package com.objectstore.controller.cluster;

import static com.objectstore.controller.cluster.TestSupport.chunk;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.objectstore.common.dto.ChangeType;
import com.objectstore.controller.cluster.ClusterState.PendingCopy;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Block 15 (v4 §7.4, §7.6, §10 rows 20, 39): pending copies end by success, failure, time-out. */
class PendingTimerTest {

    private static PendingCopy onlyPending(TestSupport t, int chunk) {
        List<PendingCopy> copies = t.cluster.read(() -> List.copyOf(t.cluster.pendingOf(chunk(chunk))));
        assertThat(copies).hasSize(1);
        return copies.get(0);
    }

    @Test
    void aStaleExpiryDoesNothing() {
        TestSupport t = new TestSupport();
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2");
        t.plan();
        PendingCopy copy = onlyPending(t, 0);

        assertThat(t.cluster.write(() -> t.cluster.expirePending(copy.id()))).as("not due yet").isFalse();
        // The copy succeeds (the target's RECEIVED); its old deadline then fires.
        t.ibr("worker2", t.fake("worker2").boot, List.of(chunk(0)), List.of(), List.of());
        assertThat(t.cluster.write(() -> t.cluster.expireDue(Long.MAX_VALUE))).isZero();
        assertThat(t.cluster.read(() -> t.cluster.live(chunk(0)))).hasSize(2);
        t.assertNoViolations();
    }

    @Test
    void aTimedOutCopyIsPlannedAgainWithAnotherTarget() {
        TestSupport t = new TestSupport();
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2");
        t.join("worker3");
        t.plan();
        String first = onlyPending(t, 0).target();

        assertThat(t.cluster.write(() -> t.cluster.expireDue(t.cluster.now() + t.properties.pendingTimeout().toNanos() + 1)))
                .isEqualTo(1);
        assertThat(t.cluster.read(() -> t.cluster.recentlyFailed(chunk(0), first))).isTrue();
        t.plan();

        assertThat(onlyPending(t, 0).target()).isNotEqualTo(first);
        assertThat(t.cluster.events()).anyMatch(e -> e.event().equals("timeout"));
        t.assertNoViolations();
    }

    @Test
    void edge39_cmdFailedEndsAPendingCopyAtOnce() {
        TestSupport t = new TestSupport();
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2");
        t.plan();
        PendingCopy copy = onlyPending(t, 0);

        t.send("worker1", List.of(t.next("worker1", ChangeType.CMD_FAILED, chunk(0), null, null, copy.cmdId())), null);

        assertThat(t.cluster.read(() -> t.cluster.pendingOf(chunk(0)))).isEmpty();
        assertThat(t.cluster.read(() -> t.cluster.recentlyFailed(chunk(0), copy.target()))).isTrue();
        assertThat(t.cluster.events()).anyMatch(e -> e.event().equals("failed"));
        t.assertNoViolations();
    }

    @Test
    void theTimerThreadExpiresACopyThroughTheWriter() {
        TestSupport t = new TestSupport(TestSupport.properties(2, 2, 0.2, 0.2, 0.4, 0, 2000));
        PendingTimer timer = new PendingTimer(t.cluster, t.queue);
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2");
        timer.start();
        t.writer.start();
        try {
            t.cluster.write(() -> t.planner.plan());
            assertThat(t.cluster.read(t.cluster::pendingTotal)).isEqualTo(1);

            await().atMost(Duration.ofSeconds(5)).until(() -> t.cluster.read(t.cluster::pendingTotal) == 0);
            assertThat(t.cluster.events()).anyMatch(e -> e.event().equals("timeout"));
        } finally {
            timer.stop();
            t.writer.stop();
        }
    }

    @Test
    void edge20_aCmdFailedDroppedByTheReportBaselineIsCoveredByThePendingTimeOut() {
        TestSupport t = new TestSupport();
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2");
        t.plan();
        PendingCopy copy = onlyPending(t, 0);
        // worker1's CMD_FAILED (change 1) is lost; its next full report has baseline 1, so the
        // controller never sees it.
        t.next("worker1", ChangeType.CMD_FAILED, chunk(0), null, null, copy.cmdId());
        t.fullReport("worker1", t.fake("worker1").boot, chunk(0));
        assertThat(t.cluster.read(() -> t.cluster.worker("worker1").received)).isEqualTo(1);
        assertThat(onlyPending(t, 0).id()).isEqualTo(copy.id());

        assertThat(t.cluster.write(() -> t.cluster.expireDue(t.cluster.now() + t.properties.pendingTimeout().toNanos() + 1)))
                .isEqualTo(1);
        assertThat(t.cluster.read(() -> t.cluster.pendingOf(chunk(0)))).isEmpty();
        t.assertNoViolations();
    }

    @Test
    void edge39_aCopyThatLandedAfterItsCmdFailedOnlyCausesASurplusThatIsTrimmed() {
        TestSupport t = new TestSupport();
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2");
        t.join("worker3");
        t.plan();
        PendingCopy first = onlyPending(t, 0);
        t.send("worker1", List.of(t.next("worker1", ChangeType.CMD_FAILED, chunk(0), null, null, first.cmdId())), null);
        t.plan();                                                  // planned again, elsewhere
        PendingCopy second = onlyPending(t, 0);
        assertThat(second.target()).isNotEqualTo(first.target());

        // The first PUT had landed after all; then the second copy lands too: three copies.
        t.ibr(first.target(), t.fake(first.target()).boot, List.of(chunk(0)), List.of(), List.of());
        t.ibr(second.target(), t.fake(second.target()).boot, List.of(chunk(0)), List.of(), List.of());
        assertThat(t.cluster.read(() -> t.cluster.live(chunk(0)))).hasSize(3);
        t.plan();

        assertThat(t.cluster.read(() -> t.cluster.live(chunk(0)))).hasSize(2);
        assertThat(t.cluster.read(() -> t.cluster.ledger().entriesFor(chunk(0)))).singleElement()
                .satisfies(e -> assertThat(e.reason).isEqualTo("excess"));
        t.assertNoViolations();
    }
}
