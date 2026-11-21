package com.objectstore.systemtest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.objectstore.common.Checksums;
import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.controller.cluster.OpVersions;
import com.objectstore.worker.commands.CommandExecutor;
import com.objectstore.worker.storage.ChunkStore;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Versioned operations end to end (v5 §3, §5): the v4 double delete cannot happen, versions grow
 * across controller restarts, and a restarted worker's fence rejects operations from before it.
 */
class VersionedOpsSystemTest extends ClusterTest {

    private final HttpClient http = HttpClient.newHttpClient();

    /**
     * The v5 §3.1 sequence, against the real controller and workers. On v4 (main) the delayed second
     * DELETE carried no version, so it destroyed the new copy (the ledger entry had been completed by
     * the first DELETE's DELETED). With versions it is STALE on the worker and the copy survives.
     */
    @Test
    void theDoubleDeleteRegressionADelayedOldDeleteCannotDestroyANewerCopy() throws Exception {
        byte[] data = Api.randomBytes(300_000, 41);
        String id = api.uploadOk("dd.bin", data);
        String chunkId = id + "-00000";
        List<String> holders = api.holders(id, 0);
        String victim = holders.get(0);
        String other = holders.get(1);
        // Only the two holders stay up, so the repair has to bring the copy back to the victim.
        for (String workerId : cluster.workerIds()) {
            if (!holders.contains(workerId)) {
                cluster.stopWorker(workerId);
            }
        }
        await().atMost(PATIENCE).until(() -> cluster.workerIds().stream()
                .filter(w -> !holders.contains(w)).allMatch(w -> "DEAD".equals(api.workerState(w))));
        long oldVersion = cluster.controllerBean(OpVersions.class).next();       // a real controller version
        CommandExecutor executor = cluster.workerBean(victim, CommandExecutor.class);
        ChunkStore store = cluster.workerBean(victim, ChunkStore.class);

        // 1. DELETE #1 runs: the copy goes, the controller learns it and repairs the chunk ...
        executor.submit(new Command(UUID.randomUUID().toString(), InternalApi.COMMAND_DELETE, chunkId, null, null,
                InternalApi.REASON_EXCESS, oldVersion));
        // 2. ... so a new copy lands on the same worker, at a newer version.
        await().atMost(PATIENCE).until(() -> store.index().containsKey(chunkId)
                && store.index().get(chunkId).version() > oldVersion);
        await().atMost(PATIENCE).untilAsserted(() -> assertThat(api.holders(id, 0)).containsExactlyInAnyOrder(victim, other));
        long newVersion = store.index().get(chunkId).version();

        // 3. The resent DELETE #2 (same version), delayed in the executor until now, finally runs.
        executor.submit(new Command(UUID.randomUUID().toString(), InternalApi.COMMAND_DELETE, chunkId, null, null,
                InternalApi.REASON_EXCESS, oldVersion));
        await().atMost(PATIENCE).until(() -> api.status().get("counters").path("cmd_failed_ignored").asLong() >= 1);

        assertThat(store.index().get(chunkId).version()).as("the newer copy survives").isEqualTo(newVersion);
        assertThat(Files.exists(cluster.chunkFile(victim, chunkId))).isTrue();
        assertThat(api.holders(id, 0)).containsExactlyInAnyOrder(victim, other);
        assertThat(api.download(id).body()).isEqualTo(data);
    }

    @Test
    void versionsGrowAcrossAControllerRestart() {
        api.uploadOk("v.bin", Api.randomBytes(1000, 42));
        long term = api.status().get("term").asLong();
        long version = api.status().get("version").asLong();

        cluster.stopController();
        cluster.startController();
        awaitHealthy();

        assertThat(api.status().get("term").asLong()).isEqualTo(term + 1);
        assertThat(api.status().get("version").asLong()).isGreaterThan(version);
        assertThat(api.status().get("version").asLong() >>> OpVersions.TERM_SHIFT).isEqualTo(term + 1);
    }

    @Test
    void aRestartedWorkerAdoptsANewFenceAndRejectsAnOperationFromBeforeIt() throws Exception {
        long before = api.worker("worker2").get("fence").asLong();
        cluster.stopWorker("worker2");
        cluster.startWorker("worker2");
        awaitHealthy();
        long after = api.worker("worker2").get("fence").asLong();
        assertThat(after).isGreaterThan(before);

        byte[] data = Api.randomBytes(100, 43);
        HttpResponse<String> old = put("worker2", UUID.randomUUID() + "-00000", data, before + 2 <= after ? before + 2 : before);

        assertThat(old.statusCode()).as(old.body()).isEqualTo(409);
        assertThat(old.body()).contains("stale_version");
    }

    private HttpResponse<String> put(String workerId, String chunkId, byte[] data, long version) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(cluster.workerUrl(workerId) + "/internal/chunks/" + chunkId))
                .timeout(Duration.ofSeconds(5))
                .header(InternalApi.CHECKSUM_HEADER, Checksums.sha256Hex(data))
                .header(InternalApi.OP_VERSION_HEADER, Long.toString(version))
                .PUT(HttpRequest.BodyPublishers.ofByteArray(data))
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
