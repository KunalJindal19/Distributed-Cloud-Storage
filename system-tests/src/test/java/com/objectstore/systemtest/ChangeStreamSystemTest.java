package com.objectstore.systemtest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.objectstore.worker.reporting.ControllerLink;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

/** Block 12 (v4 §4.11, §10 rows 1, 9, 11, 22): the change stream end to end. */
class ChangeStreamSystemTest extends ClusterTest {

    private static final long SEGMENT_DELAY_MS = 500;

    @Override
    void configure(InProcessCluster cluster) {
        cluster.workerSetting("worker.report-segment-size", "1");          // one chunk per segment: slow reports
        cluster.controllerSetting("objectstore.resync-interval-seconds", "1.5"); // a periodic resync to land in
        cluster.controllerSetting("objectstore.op-resend-initial-seconds", "15");
        cluster.controllerSetting("objectstore.op-resend-max-seconds", "60");
    }

    @AfterEach
    void noSlowSegments() {
        cluster.workerIds().forEach(id -> ControllerLink.delaySegmentsForTests(id, 0));
    }

    @Test
    void readYourWrites_everyUploadCanBeDownloadedRightAfterItsCreated() {
        for (int i = 0; i < 100; i++) {
            byte[] data = Api.randomBytes(i % 10 == 0 ? MIB + 1000 * i : 500 + 37 * i, 1000 + i);
            String id = api.uploadOk("ryw-" + i + ".bin", data);
            var download = api.download(id);
            assertThat(download.statusCode()).as("download %d right after its 201", i).isEqualTo(200);
            assertThat(download.body()).as("download %d", i).isEqualTo(data);
        }
    }

    @Test
    void edge9_anUploadDuringAResyncSkipsTheResyncingWorkerAndNeedsNoRepair() {
        // Give worker1 several chunks, so that its resync listing has several (slow) segments.
        for (int i = 0; i < 30 && cluster.chunkFilesOn("worker1").size() < 3; i++) {
            api.uploadOk("pre-" + i + ".bin", Api.randomBytes(2 * MIB, 50 + i));
        }
        assertThat(cluster.chunkFilesOn("worker1")).as("worker1's listing needs several segments").hasSizeGreaterThanOrEqualTo(3);
        awaitHealthy();
        ControllerLink link = cluster.workerBean("worker1", ControllerLink.class);
        ControllerLink.delaySegmentsForTests("worker1", SEGMENT_DELAY_MS);
        // Wait for worker1's next periodic resync to start.
        await().atMost(PATIENCE).pollInterval(Duration.ofMillis(5)).until(() -> link.reportInProgress() != null);
        String reportId = link.reportInProgress();

        // Four chunks rotate around the ring of write-eligible workers: a resyncing worker is not one (v5 §5.2).
        byte[] data = Api.randomBytes(4 * MIB, 77);
        String id = api.uploadOk("during.bin", data);
        assertThat(link.reportInProgress()).as("the upload happened during the resync").isEqualTo(reportId);
        for (int index = 0; index < 4; index++) {
            assertThat(cluster.chunkFilesOn("worker1")).doesNotContain(id + "-%05d".formatted(index));
            assertThat(api.holders(id, index)).as("chunk %d has RF copies at once", index).hasSize(2)
                    .doesNotContain("worker1");
        }
        assertThat(api.download(id).body()).as("reads work meanwhile").isEqualTo(data);

        await().atMost(PATIENCE).until(() -> !reportId.equals(link.reportInProgress()));
        awaitHealthy();
        JsonNode log = api.status().get("repair_log");
        for (JsonNode event : log) {
            if (event.get("chunk_id") != null && event.get("chunk_id").asString().startsWith(id)) {
                assertThat(event.get("event").asString()).as("no repair for the uploaded object")
                        .isNotIn("replicate", "copied", "timeout", "trimmed");
            }
        }
        // Once the resync completed, worker1 takes new data again.
        ControllerLink.delaySegmentsForTests("worker1", 0);
        await().atMost(PATIENCE).untilAsserted(() -> {
            String later = api.uploadOk("after.bin", Api.randomBytes(4 * MIB, 78));
            boolean onWorker1 = false;
            for (int index = 0; index < 4; index++) {
                onWorker1 |= api.holders(later, index).contains("worker1");
            }
            assertThat(onWorker1).isTrue();
        });
    }
}
