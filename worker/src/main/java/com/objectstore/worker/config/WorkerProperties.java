package com.objectstore.worker.config;

import com.objectstore.common.InternalApi;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import java.nio.file.Path;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Worker settings, bound once at start-up from environment variables (see worker.yml and the
 * README). The compact constructor rejects inconsistent values with a message naming the setting.
 */
@Validated
@ConfigurationProperties(prefix = "worker")
public record WorkerProperties(
        @NotBlank @Pattern(regexp = InternalApi.WORKER_ID_PATTERN) String id,
        @NotNull Path dataDir,
        @NotBlank String controllerUrl,
        String advertiseUrl,
        @Positive int maxChunkSize,
        @Positive double heartbeatIntervalSeconds,
        @Positive double heartbeatHttpTimeoutSeconds,
        /** Earliest next heartbeat when changes or report segments are waiting. */
        @Positive long heartbeatMinGapMs,
        /** Un-acknowledged changes kept; on overflow all are dropped and a resync is requested. */
        @Positive int changeLogMax,
        @Positive int maxChangesPerMessage,
        @Positive int maxChangesPerPutAnswer,
        /** Entries per resync segment. */
        @Positive int reportSegmentSize,
        @Positive double scanIntervalSeconds,
        /** The block scanner reads at most this many bytes per second. */
        @Positive long scanMaxBytesPerSec,
        @Positive double diskCheckIntervalSeconds,
        @Positive double connectTimeoutSeconds,
        @Positive double readTimeoutSeconds,
        /** Above this many tombstones the worker asks for a resync, whose fence clears them (v5 §4.2). */
        @Positive int tombstoneMax,
        /** Commands accepted but not yet running; beyond it REPLICATE/DELETE answer busy, VERIFY is dropped. */
        @Positive int executorQueueMax,
        /** Longest a chunk PUT may take to deliver its body. */
        @Positive double serverReadTimeoutSeconds,
        /** On shutdown, how long mutations in progress may take to finish. */
        @Positive double shutdownDrainTimeoutSeconds,
        /** Exit the process after a fail-stop (false only in in-process tests). */
        boolean exitOnFailStop,
        /** Switches every background loop off (register, heartbeat, scanner, disk check); tests only. */
        boolean backgroundEnabled) {

    public WorkerProperties {
        if (maxChunkSize <= 0) {
            throw new IllegalArgumentException("CHUNK_SIZE must be positive");
        }
        if (heartbeatHttpTimeoutSeconds <= 0 || tombstoneMax <= 0 || executorQueueMax <= 0
                || serverReadTimeoutSeconds <= 0 || shutdownDrainTimeoutSeconds <= 0) {
            throw new IllegalArgumentException("HEARTBEAT_HTTP_TIMEOUT, TOMBSTONE_MAX, EXECUTOR_QUEUE_MAX, "
                    + "SERVER_READ_TIMEOUT and SHUTDOWN_DRAIN_TIMEOUT must be positive");
        }
    }

    /** One data file and one {@code .meta} sidecar per chunk live directly in this directory. */
    public Path chunkDir() {
        return dataDir.resolve("chunks");
    }

    /** Copies that failed verification, kept for diagnosis (same volume: the move is a rename). */
    public Path quarantineDir() {
        return dataDir.resolve("quarantine");
    }

    public Duration heartbeatInterval() {
        return seconds(heartbeatIntervalSeconds);
    }

    public Duration heartbeatHttpTimeout() {
        return seconds(heartbeatHttpTimeoutSeconds);
    }

    public Duration heartbeatMinGap() {
        return Duration.ofMillis(heartbeatMinGapMs);
    }

    public Duration scanInterval() {
        return seconds(scanIntervalSeconds);
    }

    public Duration diskCheckInterval() {
        return seconds(diskCheckIntervalSeconds);
    }

    public Duration connectTimeout() {
        return seconds(connectTimeoutSeconds);
    }

    public Duration readTimeout() {
        return seconds(readTimeoutSeconds);
    }

    public Duration serverReadTimeout() {
        return seconds(serverReadTimeoutSeconds);
    }

    public Duration shutdownDrainTimeout() {
        return seconds(shutdownDrainTimeoutSeconds);
    }

    private static Duration seconds(double value) {
        return Duration.ofMillis(Math.round(value * 1000));
    }
}
