package com.objectstore.controller.cluster;

import static com.objectstore.controller.cluster.TestSupport.chunk;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.RegisterRequest;
import com.objectstore.common.dto.ReportSegment;
import com.objectstore.common.web.ApiException;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Block 12 (v4 §4.8, §4.11, §10 rows 3, 8, 35, 38): registration attempts, age and epochs. */
class RegistrationTest {

    private final TestSupport t = new TestSupport();
    private final ClusterState cluster = t.cluster;

    private RegisterRequest request(String boot, long attempt, long changeSeq, double sentAt) {
        return new RegisterRequest("worker1", "http://worker1:8001", boot, "cluster-1", attempt, changeSeq, -1, sentAt,
                TestSupport.STATS);
    }

    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable call, int status, String code) {
        assertThatThrownBy(call).isInstanceOfSatisfying(ApiException.class, e -> {
            assertThat(e.status().value()).isEqualTo(status);
            assertThat(e.code()).isEqualTo(code);
        });
    }

    @Test
    void edge3_aDelayedOlderAttemptIsRefusedAsStale() {
        t.registry.register(request("boot-a", 1, 0, InternalApi.nowSeconds()));
        t.registry.register(request("boot-a", 2, 7, InternalApi.nowSeconds()));   // after a 404, say
        long epoch = cluster.locked(() -> cluster.worker("worker1").epoch);

        assertCode(() -> t.registry.register(request("boot-a", 1, 0, InternalApi.nowSeconds())), 409, "stale_registration");

        cluster.locked(() -> {
            WorkerInfo worker = cluster.worker("worker1");
            assertThat(worker.epoch).isEqualTo(epoch);
            assertThat(worker.received).isEqualTo(7);
            assertThat(worker.lastAttempt).isEqualTo(2);
        });
    }

    @Test
    void theSameAttemptAgainIsAnsweredWithoutAReset() {
        t.knowChunk(chunk(0));
        t.registry.register(request("boot-a", 1, 0, InternalApi.nowSeconds()));
        t.fake("worker1").boot = "boot-a";
        t.fullReport("worker1", "boot-a", chunk(0));
        cluster.locked(() -> cluster.queueCommand("worker1", InternalApi.COMMAND_DELETE, chunk(0), null, null, "orphan"));
        long epoch = cluster.locked(() -> cluster.worker("worker1").epoch);

        assertThat(t.registry.register(request("boot-a", 1, 0, InternalApi.nowSeconds())).ok()).isTrue();

        cluster.locked(() -> {
            WorkerInfo worker = cluster.worker("worker1");
            assertThat(worker.epoch).isEqualTo(epoch);
            assertThat(worker.hasReported).isTrue();
            assertThat(worker.commands).hasSize(1);
        });
    }

    @Test
    void aRegistrationSentTooLongAgoIsRefused() {
        assertCode(() -> t.registry.register(request("boot-a", 1, 0, InternalApi.nowSeconds() - 60)), 409, "too_old");
        assertThat(cluster.locked(() -> cluster.workers())).isEmpty();
    }

    @Test
    void edge8_acceptingStartsANewEpochAndForgetsWhatWasInFlight() {
        t.knowChunk(chunk(0));
        t.join("worker1", chunk(0));
        t.join("worker2");
        String upload = "dddddddd-7d0e-4c1b-9a54-1f2e3d4c5b6a";
        cluster.locked(() -> {
            cluster.beginUpload(upload);
            cluster.putReceipt(upload + "-00000", "worker1", cluster.worker("worker1").epoch, 10, TestSupport.SUM, 0);
            cluster.addPending(chunk(0), "worker1", "worker2", "cmd-1");
            cluster.queueCommand("worker1", InternalApi.COMMAND_DELETE, chunk(0), null, null, "orphan");
        });
        // A report is in progress when the process restarts.
        t.send("worker1", List.of(), new ReportSegment("r", 0, false, 0, List.of()));
        long epoch = cluster.locked(() -> cluster.worker("worker1").epoch);

        t.register("worker1");   // a new process: change_seq 0

        cluster.locked(() -> {
            WorkerInfo worker = cluster.worker("worker1");
            assertThat(worker.epoch).isEqualTo(epoch + 1);
            assertThat(worker.received).isZero();
            assertThat(worker.applied).isZero();
            assertThat(worker.session).isNull();
            assertThat(worker.hasReported).isFalse();
            assertThat(worker.commands).isEmpty();
            assertThat(cluster.pendingTotal()).isZero();
            assertThat(cluster.receipt(upload + "-00000", "worker1")).isNull();
            assertThat(cluster.replica(chunk(0), "worker1")).as("replicas are kept for the report").isNotNull();
        });
        t.assertNoViolations();
    }

    @Test
    void edge5_workQueuedForAnEndedEpochIsDropped() {
        t.knowChunk(chunk(0));
        t.join("worker1");
        long oldEpoch = cluster.locked(() -> cluster.worker("worker1").epoch);
        t.register("worker1");

        cluster.locked(() -> t.applier.applyChanges("worker1", oldEpoch, List.of(
                new com.objectstore.common.dto.Change(1, com.objectstore.common.dto.ChangeType.RECEIVED, chunk(0), 10L,
                        TestSupport.SUM, null, null, null))));

        assertThat(cluster.locked(() -> cluster.chunksOn("worker1"))).isEmpty();
        assertThat(cluster.counter("stale_tasks_dropped")).isEqualTo(1);
    }

    @Test
    void edge35_aControllerRestartDuringAReportMeansANewRegistrationAndANewReport() {
        t.knowChunk(chunk(0));
        String boot = t.join("worker1", chunk(0));
        // A new controller knows nothing: the worker's next heartbeat is answered 404.
        TestSupport restarted = new TestSupport();
        restarted.knowChunk(chunk(0));
        assertThatThrownBy(() -> restarted.heartbeat("worker1", boot, 5)).isInstanceOf(ApiException.class)
                .satisfies(e -> assertThat(((ApiException) e).status().value()).isEqualTo(404));
        // It registers again (same process, next attempt) and sends a new report.
        restarted.fake("worker1").boot = boot;
        restarted.fake("worker1").attempt = 1;
        restarted.reRegister("worker1");
        restarted.fullReport("worker1", boot, chunk(0));
        assertThat(restarted.cluster.locked(() -> restarted.cluster.chunksOn("worker1"))).containsExactly(chunk(0));
    }
}
