package com.objectstore.systemtest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A client's object delete reaches the holders directly through DELETE /internal/chunks/{id}, like an
 * upload's PUTs, instead of waiting for their heartbeats; heartbeat replies remain the fallback.
 *
 * <p>Workers heartbeat only every 3 s here (the controller's STALE/DEAD limits are raised to match),
 * so files that are gone well within that time were deleted through the endpoint.
 */
class DirectDeleteSystemTest extends ClusterTest {

    @Override
    void configure(InProcessCluster cluster) {
        cluster.workerSetting("worker.heartbeat-interval-seconds", "3");
        cluster.workerSetting("worker.heartbeat-http-timeout-seconds", "1");
        cluster.controllerSetting("objectstore.stale-after-seconds", "8");
        cluster.controllerSetting("objectstore.dead-after-seconds", "16");
        cluster.controllerSetting("objectstore.max-message-age-seconds", "5");
        // A resyncing worker rightly gets no new operations (the hand-out filter), so its deletes wait for its
        // heartbeats. No periodic resyncs here, and the one-time "orphan GC allowed" resyncs run in parallel.
        cluster.controllerSetting("objectstore.resync-interval-seconds", "3600");
        cluster.controllerSetting("objectstore.resync-max-concurrent", "4");
    }

    /** Past the one-time "orphan GC allowed" resyncs (ORPHAN_GC_DELAY = 2 s here), with no worker resyncing. */
    private void awaitNoResync() throws InterruptedException {
        Thread.sleep(2500);
        await().atMost(PATIENCE).pollInterval(Duration.ofMillis(50))
                .until(() -> api.status().get("resyncing").asInt() == 0);
    }

    private List<String> filesOf(String objectId, boolean runningOnly) {
        List<String> files = new ArrayList<>();
        for (String workerId : cluster.workerIds()) {
            if (runningOnly && !cluster.isRunning(workerId)) {
                continue;
            }
            cluster.chunkFilesOn(workerId).stream().filter(c -> c.startsWith(objectId)).map(c -> c + "@" + workerId)
                    .forEach(files::add);
        }
        return files;
    }

    private long counter(String name) {
        return api.status().get("counters").path(name).asLong();
    }

    @Test
    void aClientDeleteRemovesEveryCopyRightAwayWithoutWaitingForHeartbeats() throws Exception {
        String id = api.uploadOk("direct.bin", Api.randomBytes(3 * MIB + 100, 7));        // 4 chunks, 8 copies
        await().atMost(PATIENCE).until(() -> filesOf(id, false).size() == 8);
        awaitNoResync();
        long sentBefore = counter("deletes_sent_direct");

        long started = System.nanoTime();
        assertThat(api.delete(id).statusCode()).isEqualTo(204);
        await().atMost(Duration.ofMillis(1500)).pollInterval(Duration.ofMillis(20))
                .until(() -> filesOf(id, false).isEmpty());
        double seconds = (System.nanoTime() - started) / 1e9;

        assertThat(seconds).as("well inside one 3 s heartbeat interval").isLessThan(1.5);
        assertThat(counter("deletes_sent_direct") - sentBefore).as("every copy's delete went through the endpoint")
                .isEqualTo(8);
        // The workers' DELETED(v) changes complete the ops exactly as before.
        await().atMost(PATIENCE).untilAsserted(() -> {
            var ops = api.status().get("ops").get("by_state");
            assertThat(ops.get("waiting").asInt() + ops.get("queued").asInt() + ops.get("sent").asInt()).isZero();
        });
        assertThat(api.download(id).statusCode()).isEqualTo(404);
    }

    @Test
    void aHolderThatCannotBeReachedGetsItsDeletesLaterAndNothingIsLeftBehind() {
        String id = api.uploadOk("unreachable.bin", Api.randomBytes(2 * MIB, 8));
        String down = api.holders(id, 0).get(0);
        cluster.stopWorker(down);               // still ALIVE to the controller for up to 8 s
        long fallbackBefore = counter("deletes_direct_fallback");

        assertThat(api.delete(id).statusCode()).isEqualTo(204);

        await().atMost(PATIENCE).until(() -> counter("deletes_direct_fallback") > fallbackBefore);
        await().atMost(Duration.ofSeconds(3)).until(() -> filesOf(id, true).isEmpty());
        assertThat(filesOf(id, false)).as("the stopped holder still has its copies").isNotEmpty();

        cluster.startWorker(down);
        awaitHealthy();
        // Back: its resync lists the copies, they belong to a deleted object, and they are deleted.
        await().atMost(PATIENCE).until(() -> filesOf(id, false).isEmpty());
    }
}
