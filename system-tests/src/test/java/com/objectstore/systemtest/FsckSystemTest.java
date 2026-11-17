package com.objectstore.systemtest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.objectstore.common.Checksums;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

/** Block 9: fsck compares every disk with the block map and the namespace. */
class FsckSystemTest extends ClusterTest {

    private JsonNode fsck() {
        return api.json(api.get("/api/v1/fsck").body());
    }

    @Test
    void aQuietClusterIsHealthyAndPlantedProblemsAreListed() throws Exception {
        String id = api.uploadOk("a.bin", Api.randomBytes(3 * MIB, 1));
        await().atMost(PATIENCE).untilAsserted(() -> {
            JsonNode result = fsck();
            assertThat(result.get("status").asString()).isEqualTo("HEALTHY");
            assertThat(result.get("quiescent").asBoolean()).isTrue();
            assertThat(result.get("objects").get("total").asLong()).isEqualTo(1);
        });

        // A file nobody knows about, and a copy damaged on disk.
        String orphan = UUID.randomUUID() + "-00000";
        byte[] junk = Api.randomBytes(100, 2);
        Path orphanFile = cluster.chunkFile("worker1", orphan);
        Files.write(orphanFile, junk);
        Files.writeString(orphanFile.resolveSibling(orphan + ".meta"), Checksums.sha256Hex(junk) + " 100\n");
        String chunkId = id + "-00001";
        String holder = api.holders(id, 1).get(0);
        Files.write(cluster.chunkFile(holder, chunkId), "XXXX".getBytes(), StandardOpenOption.WRITE);

        JsonNode result = fsck();
        assertThat(result.get("status").asString()).isNotEqualTo("HEALTHY");
        assertThat(result.get("orphans_on_disk").asLong()).isEqualTo(1);
        assertThat(result.get("disk_vs_map").get("checksum_mismatch").toString()).contains(chunkId + "@" + holder);

        // The scanner, the monitor and orphan GC clean both up.
        await().atMost(PATIENCE).untilAsserted(() -> {
            JsonNode after = fsck();
            assertThat(after.get("status").asString()).isEqualTo("HEALTHY");
            assertThat(after.get("orphans_on_disk").asLong()).isZero();
            assertThat(after.get("disk_vs_map").get("checksum_mismatch")).isEmpty();
        });
    }

    @Test
    void aDeadWorkerBeforeRepairMakesTheClusterDegraded() {
        api.uploadOk("b.bin", Api.randomBytes(4 * MIB, 3));
        cluster.stopWorker("worker2");
        await().atMost(PATIENCE).pollInterval(java.time.Duration.ofMillis(20)).until(() -> api.workerState("worker2").equals("DEAD"));

        JsonNode result = fsck();

        assertThat(result.get("status").asString()).isEqualTo("DEGRADED");
        assertThat(result.get("objects").get("under_replicated").asLong()).isPositive();
        cluster.startWorker("worker2");
        awaitHealthy();
    }
}
