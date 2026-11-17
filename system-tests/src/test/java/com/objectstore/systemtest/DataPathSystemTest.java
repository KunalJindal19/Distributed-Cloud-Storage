package com.objectstore.systemtest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

/** Block 8: pipeline recovery, idempotency, durable commit, delete via commands. */
class DataPathSystemTest extends ClusterTest {

    @Test
    void anUploadReplacesATargetThatDiedButIsNotYetDeclaredDead() {
        cluster.stopWorker("worker2"); // the controller still believes worker2 is ALIVE
        byte[] data = Api.randomBytes(8 * MIB + 3, 1);

        HttpResponse<String> response = api.upload("a.bin", data, null, null);

        assertThat(response.statusCode()).as(response.body()).isEqualTo(201);
        JsonNode object = api.json(response.body());
        for (JsonNode chunk : object.get("chunks")) {
            List<String> holders = new ArrayList<>();
            chunk.get("locations").forEach(l -> holders.add(l.get("worker_id").asString()));
            assertThat(holders).hasSize(2).doesNotHaveDuplicates().doesNotContain("worker2");
        }
        assertThat(api.download(object.get("object_id").asString()).body()).isEqualTo(data);
        cluster.startWorker("worker2");
        awaitHealthy();
    }

    @Test
    void withTooFewWorkersLeftTheUploadFailsCleanlyAndItsLeftoversAreCollected() {
        cluster.stopWorker("worker3");
        cluster.stopWorker("worker4");
        await().atMost(PATIENCE).until(() -> api.workerState("worker4").equals("DEAD") && api.workerState("worker3").equals("DEAD"));

        // Stop worker2 while the upload is writing to it: its copies can no longer be cleaned up.
        CompletableFuture<HttpResponse<String>> upload =
                CompletableFuture.supplyAsync(() -> api.upload("b.bin", Api.randomBytes(80 * MIB, 2), null, null));
        // Enough leftovers to trip the orphan safety valve (more than 10, and all of worker2's chunks).
        await().atMost(PATIENCE).pollInterval(Duration.ofMillis(2)).until(() -> cluster.chunkFilesOn("worker2").size() >= 12);
        cluster.stopWorker("worker2");
        HttpResponse<String> response = upload.join();

        assertThat(response.statusCode()).as(response.body()).isEqualTo(503);
        assertThat(api.json(response.body()).get("error").asString()).isEqualTo("upload_failed");
        assertThat(api.status().get("objects").asLong()).isZero();
        await().atMost(PATIENCE).untilAsserted(() -> assertThat(cluster.chunkFilesOn("worker1")).isEmpty());
        int leftovers = cluster.chunkFilesOn("worker2").size();
        assertThat(leftovers).as("leftovers on the stopped worker").isGreaterThan(10);

        // When worker2 returns, ALL of its chunks are leftovers of the failed upload: far above the
        // orphan safety valve's 50 %, but known garbage, so they are collected anyway.
        cluster.startWorker("worker2");
        cluster.startWorker("worker3");
        cluster.startWorker("worker4");
        awaitHealthy();
        await().atMost(PATIENCE).untilAsserted(() -> assertThat(cluster.chunkFilesOn("worker2")).isEmpty());
        assertThat(api.status().get("counters").path("orphan_gc_refused").asLong()).isZero();
    }

    @Test
    void anIdempotencyKeyMakesRetriesSafe() {
        byte[] data = Api.randomBytes(MIB + 1, 3);
        String key = "key-" + UUID.randomUUID();

        HttpResponse<String> first = api.upload("c.bin", data, null, key);
        HttpResponse<String> retry = api.upload("c.bin", data, null, key);
        HttpResponse<String> different = api.upload("c.bin", Api.randomBytes(10, 4), null, key);

        assertThat(first.statusCode()).isEqualTo(201);
        assertThat(retry.statusCode()).isEqualTo(200);
        assertThat(retry.headers().firstValue("Idempotent-Replay")).contains("true");
        assertThat(api.json(retry.body()).get("object_id")).isEqualTo(api.json(first.body()).get("object_id"));
        assertThat(different.statusCode()).isEqualTo(409);
        assertThat(api.json(different.body()).get("error").asString()).isEqualTo("conflict");
        assertThat(api.status().get("objects").asLong()).isEqualTo(1);
        assertThat(api.upload("x", new byte[1], null, "bad key with spaces").statusCode()).isEqualTo(400);
    }

    @Test
    void concurrentUploadsWithOneKeyCreateOneObject() {
        byte[] data = Api.randomBytes(2 * MIB, 5);
        String key = "key-" + UUID.randomUUID();
        List<CompletableFuture<HttpResponse<String>>> uploads = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            uploads.add(CompletableFuture.supplyAsync(() -> api.upload("d.bin", data, null, key)));
        }

        List<Integer> codes = uploads.stream().map(CompletableFuture::join).map(HttpResponse::statusCode).toList();
        List<String> ids = uploads.stream().map(CompletableFuture::join)
                .map(r -> api.json(r.body()).get("object_id").asString()).distinct().toList();

        assertThat(codes).containsOnly(200, 201).contains(201);
        assertThat(ids).hasSize(1);
        assertThat(api.status().get("objects").asLong()).isEqualTo(1);
        // The losers' chunks are deleted: only the winner's 2 x 2 copies remain.
        await().atMost(PATIENCE).untilAsserted(() -> {
            int files = cluster.workerIds().stream().mapToInt(w -> cluster.chunkFilesOn(w).size()).sum();
            assertThat(files).isEqualTo(4);
        });
    }

    @Test
    void anObjectSurvivesAControllerCrashRightAfterItsUploadWasAcknowledged() {
        byte[] data = Api.randomBytes(3 * MIB + 7, 6);
        String id = api.uploadOk("e.bin", data);

        cluster.stopController();
        cluster.startController();

        await().atMost(PATIENCE).until(() -> api.download(id).statusCode() == 200);
        assertThat(api.download(id).body()).isEqualTo(data);
        awaitHealthy();
    }

    @Test
    void deletingAnObjectRemovesItsFilesViaCommandsEvenFromAWorkerThatWasDown() {
        byte[] data = Api.randomBytes(3 * MIB, 7);
        String id = api.uploadOk("f.bin", data);
        String down = api.holders(id, 0).get(0);
        cluster.stopWorker(down);
        await().atMost(PATIENCE).until(() -> api.workerState(down).equals("DEAD"));

        assertThat(api.delete(id).statusCode()).isEqualTo(204);
        assertThat(api.download(id).statusCode()).isEqualTo(404);
        // Live holders delete their files when they pick up the DELETE with the next heartbeat.
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            for (String worker : cluster.workerIds()) {
                if (!worker.equals(down)) {
                    assertThat(cluster.chunkFilesOn(worker)).noneMatch(c -> c.startsWith(id));
                }
            }
        });
        assertThat(cluster.chunkFilesOn(down)).anyMatch(c -> c.startsWith(id));

        cluster.startWorker(down);
        // The returning worker's files are orphans now; its block reports get them collected.
        await().atMost(PATIENCE).untilAsserted(() -> assertThat(cluster.chunkFilesOn(down)).noneMatch(c -> c.startsWith(id)));
    }
}
