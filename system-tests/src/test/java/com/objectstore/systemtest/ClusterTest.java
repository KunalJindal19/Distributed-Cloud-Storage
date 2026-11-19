package com.objectstore.systemtest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import tools.jackson.databind.JsonNode;

/**
 * Base class: every test gets a fresh four-worker cluster that has left safe mode, and must leave
 * it with zero invariant violations.
 */
abstract class ClusterTest {

    static final Duration PATIENCE = Duration.ofSeconds(20);
    static final int MIB = 1_048_576;

    InProcessCluster cluster;
    Api api;
    private long violationsBefore;

    /** Hook for subclasses that need non-default controller settings. */
    void configure(InProcessCluster cluster) {
    }

    @BeforeEach
    void startCluster() {
        // No SQLite on the writer thread or under the cluster lock: a violation fails the request.
        com.objectstore.controller.cluster.SqliteGuard.setStrict(true);
        violationsBefore = com.objectstore.controller.cluster.SqliteGuard.violations();
        cluster = new InProcessCluster(4);
        configure(cluster);
        cluster.startAll();
        api = new Api(cluster.controllerUrl());
        awaitHealthy();
    }

    @AfterEach
    void stopCluster() {
        try {
            assertThat(com.objectstore.controller.cluster.SqliteGuard.violations() - violationsBefore)
                    .as("SQLite accessed on the writer or under the cluster lock").isZero();
            if (api != null) {
                assertThat(api.status().get("counters").get("invariant_violations").asLong())
                        .as("invariant violations").isZero();
            }
        } finally {
            cluster.close();
        }
    }

    /** All running workers ALIVE and reported, safe mode off, nothing under/over-replicated or pending. */
    void awaitHealthy() {
        await().atMost(PATIENCE).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
            JsonNode status = api.status();
            assertThat(status.get("safe_mode").asBoolean()).as("safe mode").isFalse();
            for (String workerId : cluster.workerIds()) {
                if (cluster.isRunning(workerId)) {
                    JsonNode worker = api.worker(workerId);
                    assertThat(worker).as(workerId).isNotNull();
                    assertThat(worker.get("state").asString()).as(workerId).isEqualTo("ALIVE");
                    assertThat(worker.get("has_reported").asBoolean()).as(workerId + " reported").isTrue();
                }
            }
        });
    }
}
