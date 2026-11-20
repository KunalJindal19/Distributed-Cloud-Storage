package com.objectstore.systemtest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.objectstore.controller.objects.UploadService;
import com.objectstore.worker.reporting.ControllerLink;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

/**
 * Block 14 (v4 §6.5, §10 rows 13, 14, 16, 17): deletions complete through the ledger, with no
 * periodic full report at all (FULL_REPORT_INTERVAL = 1 h on both sides).
 */
class LedgerSystemTest extends ClusterTest {

    private static final Duration LONG = Duration.ofSeconds(40);

    @Override
    void configure(InProcessCluster cluster) {
        cluster.controllerSetting("objectstore.full-report-interval-seconds", "3600");
        cluster.workerSetting("worker.full-report-interval-seconds", "3600");
    }

    @AfterEach
    void hooksOff() {
        cluster.workerIds().forEach(id -> ControllerLink.loseRepliesForTests(id, 0));
        UploadService.beforeCommitForTests = null;
    }

    private List<String> filesOf(String objectId) {
        List<String> files = new ArrayList<>();
        for (String workerId : cluster.workerIds()) {
            cluster.chunkFilesOn(workerId).stream().filter(c -> c.startsWith(objectId)).map(c -> c + "@" + workerId)
                    .forEach(files::add);
        }
        return files;
    }

    private void awaitLedgerEmpty() {
        await().atMost(LONG).untilAsserted(() -> {
            JsonNode ledger = api.status().get("ledger");
            assertThat(ledger.get("waiting").asInt() + ledger.get("queued").asInt() + ledger.get("delivered").asInt())
                    .isZero();
        });
    }

    @Test
    void edge14_deletionsCompleteEvenWhenHalfTheHeartbeatRepliesAreLost() {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            ids.add(api.uploadOk("lost-" + i + ".bin", Api.randomBytes(MIB + 1000 * i, 300 + i)));
        }
        cluster.workerIds().forEach(id -> ControllerLink.loseRepliesForTests(id, 0.5));

        for (String id : ids) {
            assertThat(api.delete(id).statusCode()).isEqualTo(204);
        }

        await().atMost(LONG).untilAsserted(() -> {
            for (String id : ids) {
                assertThat(filesOf(id)).as("files of %s", id).isEmpty();
            }
        });
        cluster.workerIds().forEach(id -> ControllerLink.loseRepliesForTests(id, 0));
        awaitLedgerEmpty();
    }

    @Test
    void deletingWhileAHolderIsDownDeletesItsCopiesWhenItReturns() {
        String id = api.uploadOk("down.bin", Api.randomBytes(3 * MIB, 31));
        String holder = api.holders(id, 0).get(0);
        cluster.stopWorker(holder);
        await().atMost(PATIENCE).until(() -> api.workerState(holder).equals("DEAD"));

        assertThat(api.delete(id).statusCode()).isEqualTo(204);
        assertThat(filesOf(id)).anyMatch(f -> f.endsWith("@" + holder));      // still on the stopped holder

        cluster.startWorker(holder);
        await().atMost(LONG).untilAsserted(() -> assertThat(filesOf(id)).isEmpty());
        awaitLedgerEmpty();
    }

    @Test
    void edge17_aControllerKilledDuringAnUploadHasItsLeftoversCollectedAfterTheGcDelay() throws Exception {
        CountDownLatch writing = new CountDownLatch(1);
        CountDownLatch killed = new CountDownLatch(1);
        AtomicReference<String> objectId = new AtomicReference<>();
        UploadService.beforeCommitForTests = (id, targets) -> {
            UploadService.beforeCommitForTests = null;
            objectId.set(id);
            writing.countDown();                       // every chunk is on two workers, nothing committed
            try {
                killed.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("the controller died");
        };
        Thread upload = Thread.ofVirtual().start(() -> api.upload("crash.bin", Api.randomBytes(3 * MIB, 32), null, null));
        assertThat(writing.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(filesOf(objectId.get())).hasSize(6);

        cluster.stopController();                     // "kill": its upload never commits nor cleans up
        killed.countDown();
        upload.join(10_000);
        cluster.startController();

        awaitHealthy();
        // Only after ORPHAN_GC_DELAY (2 s here) does the controller ask every worker for a report,
        // once; those reports list the leftovers, which the ledger deletes.
        await().atMost(LONG).untilAsserted(() -> assertThat(filesOf(objectId.get())).isEmpty());
        awaitLedgerEmpty();
    }
}
