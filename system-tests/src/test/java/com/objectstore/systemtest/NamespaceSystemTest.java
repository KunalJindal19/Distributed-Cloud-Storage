package com.objectstore.systemtest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.objectstore.controller.cluster.ClusterState;
import com.objectstore.controller.cluster.Writer;
import com.objectstore.controller.objects.UploadService;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Block 13 (v4 §5.6-5.9, §5.11, §10 rows 7, 8, 32, 40): the writer, the namespace lock, the settle wait. */
class NamespaceSystemTest extends ClusterTest {

    @Override
    void configure(InProcessCluster cluster) {
        cluster.controllerSetting("objectstore.upload-commit-wait-seconds", "0.3");
    }

    @AfterEach
    void hooksOff() {
        UploadService.afterInsertForTests = null;
        UploadService.beforeCommitForTests = null;
        Writer.delayTasksForTests(0);
    }

    private void barrier() throws Exception {
        cluster.controllerBean(Writer.class).barrier(Duration.ofSeconds(30));
    }

    private List<String> chunksInMemory(String objectId) {
        ClusterState state = cluster.controllerBean(ClusterState.class);
        return state.read(() -> state.chunks().keySet().stream().filter(c -> c.startsWith(objectId)).toList());
    }

    @Test
    void edge7_aDeleteRacingTheCommitOfTheSameObjectNeverLeavesItsChunksInMemory() throws Exception {
        AtomicReference<Thread> deleter = new AtomicReference<>();
        AtomicInteger deleted = new AtomicInteger();
        // Right after the SQLite insert (the object is now visible), another thread deletes it. It
        // must wait for the namespace lock, so its DeleteObject is queued after our CommitObject.
        UploadService.afterInsertForTests = objectId -> deleter.set(Thread.ofVirtual().start(() -> {
            if (api.delete(objectId).statusCode() == 204) {
                deleted.incrementAndGet();
            }
        }));
        List<String> ids = new ArrayList<>();
        for (int run = 0; run < 1000; run++) {
            String id = api.uploadOk("race-" + run + ".bin", Api.randomBytes(200 + run, run));
            deleter.get().join();
            ids.add(id);
        }
        UploadService.afterInsertForTests = null;
        barrier();

        assertThat(deleted.get()).isEqualTo(1000);
        for (String id : ids) {
            assertThat(chunksInMemory(id)).as("chunks of deleted object %s", id).isEmpty();
            assertThat(api.download(id).statusCode()).isEqualTo(404);
        }
        awaitHealthy();
    }

    @Test
    void edge8_anUploadWhoseTargetRestartsBeforeTheCommitIsRepaired() throws Exception {
        AtomicReference<String> restarted = new AtomicReference<>();
        UploadService.beforeCommitForTests = (objectId, targets) -> {
            String target = targets.get(0);
            restarted.set(target);
            String oldBoot = api.worker(target).get("boot_id").asString();
            cluster.stopWorker(target);
            cluster.startWorker(target);                // a new process: a new epoch on the controller
            await().atMost(PATIENCE).until(() -> {
                var worker = api.worker(target);
                return worker != null && !worker.get("boot_id").asString().equals(oldBoot);
            });
        };
        byte[] data = Api.randomBytes(MIB / 2, 8);
        String id = api.uploadOk("restart.bin", data);
        UploadService.beforeCommitForTests = null;

        // The copy on the restarted target was not recorded from the commit; the chunk ends with
        // RF live copies anyway (from the new process's report, or by repair).
        awaitHealthy();
        await().atMost(PATIENCE).untilAsserted(() -> {
            assertThat(api.holders(id, 0)).hasSize(2);
            assertThat(api.status().get("under_replicated").asInt()).isZero();
        });
        assertThat(restarted.get()).isNotNull();
        assertThat(api.download(id).body()).isEqualTo(data);
    }

    @Test
    void edge40_aDownloadRightAfter201WhileTheWriterIsSlowSucceedsWithinTheSettleWait() throws Exception {
        long timeoutsBefore = api.status().get("counters").get("upload_commit_wait_timeouts").asLong();
        Writer.delayTasksForTests(1200);               // every batch waits 1.2 s (outside the lock)
        byte[] data = Api.randomBytes(2 * MIB + 100, 9);
        long started = System.nanoTime();
        String id = api.uploadOk("slow.bin", data);
        var download = api.download(id);
        double seconds = (System.nanoTime() - started) / 1e9;
        Writer.delayTasksForTests(0);

        assertThat(download.statusCode()).isEqualTo(200);
        assertThat(download.body()).isEqualTo(data);
        assertThat(api.status().get("counters").get("upload_commit_wait_timeouts").asLong())
                .as("the upload answered 201 before its CommitObject was applied").isGreaterThan(timeoutsBefore);
        assertThat(seconds).as("upload + download, s").isLessThan(10);
    }
}
