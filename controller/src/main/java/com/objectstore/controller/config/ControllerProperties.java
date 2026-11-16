package com.objectstore.controller.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.nio.file.Path;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Controller settings, bound once at start-up from environment variables (see application.yml).
 *
 * @param chunkSize               CHUNK_SIZE, bytes per chunk (1 MiB)
 * @param replicationFactor       REPLICATION_FACTOR, good copies wanted per chunk (2)
 * @param heartbeatTimeoutSeconds HEARTBEAT_TIMEOUT, silence after which a worker is DEAD
 * @param healthIntervalSeconds   HEALTH_INTERVAL, period of the health monitor
 * @param startupGraceSeconds     STARTUP_GRACE, time after a controller start during which no
 *                                worker is declared DEAD (they may still be booting)
 * @param repairIntervalSeconds   REPAIR_INTERVAL, period of the repair loop
 * @param scrubIntervalSeconds    SCRUB_INTERVAL, period of the scrub loop
 * @param workerTimeoutSeconds    WORKER_TIMEOUT_S, time limit of one HTTP call to a worker
 * @param repairConcurrency       REPAIR_CONCURRENCY, chunks repaired in parallel
 * @param dbPath                  DB_PATH, the SQLite metadata file
 */
@Validated
@ConfigurationProperties(prefix = "objectstore")
public record ControllerProperties(
        @Min(1) @Max(64 * 1024 * 1024) int chunkSize,
        @Min(1) int replicationFactor,
        @Positive double heartbeatTimeoutSeconds,
        @Positive double healthIntervalSeconds,
        @PositiveOrZero double startupGraceSeconds,
        @Positive double repairIntervalSeconds,
        @Positive double scrubIntervalSeconds,
        @Positive double workerTimeoutSeconds,
        @Min(1) int repairConcurrency,
        @NotNull Path dbPath) {

    public Duration heartbeatTimeout() {
        return seconds(heartbeatTimeoutSeconds);
    }

    public Duration healthInterval() {
        return seconds(healthIntervalSeconds);
    }

    public Duration startupGrace() {
        return seconds(startupGraceSeconds);
    }

    public Duration repairInterval() {
        return seconds(repairIntervalSeconds);
    }

    public Duration scrubInterval() {
        return seconds(scrubIntervalSeconds);
    }

    public Duration workerTimeout() {
        return seconds(workerTimeoutSeconds);
    }

    private static Duration seconds(double value) {
        return Duration.ofMillis(Math.round(value * 1000));
    }
}
