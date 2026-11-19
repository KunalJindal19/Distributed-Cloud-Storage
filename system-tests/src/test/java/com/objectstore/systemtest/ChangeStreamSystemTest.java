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
        cluster.controllerSetting("objectstore.pending-timeout-seconds", "15");
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
    void edge9_anUploadDuringAFullReportIsRecordedWhenTheReportCompletesWithoutAnExtraCopy() {
        // Give worker1 several chunks, so that its full report has several (slow) segments.
        for (int i = 0; i < 3; i++) {
            api.uploadOk("pre-" + i + ".bin", Api.randomBytes(2 * MIB, 50 + i));
        }
        awaitHealthy();
        ControllerLink link = cluster.workerBean("worker1", ControllerLink.class);
        ControllerLink.delaySegmentsForTests("worker1", SEGMENT_DELAY_MS);
        // Wait for worker1's next periodic report to start.
        await().atMost(PATIENCE).pollInterval(Duration.ofMillis(5)).until(() -> link.reportInProgress() != null);
        String reportId = link.reportInProgress();

        // Four chunks rotate around the four-worker ring: worker1 holds some of them.
        byte[] data = Api.randomBytes(4 * MIB, 77);
        String id = api.uploadOk("during.bin", data);
        assertThat(link.reportInProgress()).as("the upload landed during the report").isEqualTo(reportId);

        List<Integer> onWorker1 = new ArrayList<>();
        for (int index = 0; index < 4; index++) {
            if (cluster.chunkFilesOn("worker1").contains(id + "-%05d".formatted(index))) {
                onWorker1.add(index);
            }
        }
        assertThat(onWorker1).isNotEmpty();
        // Held back: the controller does not list worker1's copies yet, but they are not repaired either.
        for (int index : onWorker1) {
            assertThat(api.holders(id, index)).doesNotContain("worker1");
        }
        assertThat(api.download(id).body()).as("reads work meanwhile").isEqualTo(data);

        // Once the report completes, the held-back RECEIVED records the copies.
        await().atMost(PATIENCE).until(() -> !reportId.equals(link.reportInProgress()));
        await().atMost(PATIENCE).untilAsserted(() -> {
            for (int index : onWorker1) {
                assertThat(api.holders(id, index)).contains("worker1");
            }
        });
        awaitHealthy();
        JsonNode log = api.status().get("repair_log");
        for (JsonNode event : log) {
            if (event.get("chunk_id") != null && event.get("chunk_id").asString().startsWith(id)) {
                assertThat(event.get("event").asString()).as("no repair for the uploaded object")
                        .isNotIn("replicate", "copied", "timeout");
            }
        }
        for (int index = 0; index < 4; index++) {
            assertThat(api.holders(id, index)).as("chunk %d has exactly RF copies", index).hasSize(2);
        }
    }
}
