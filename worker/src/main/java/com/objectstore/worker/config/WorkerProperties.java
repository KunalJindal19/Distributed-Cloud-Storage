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

/** Worker settings, bound once at start-up from environment variables (see application.yml). */
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
        @Positive double fullReportIntervalSeconds,
        @Positive double ibrIntervalSeconds,
        @Positive int ibrBufferMax,
        @Positive double scanIntervalSeconds,
        /** The block scanner reads at most this many bytes per second. */
        @Positive long scanMaxBytesPerSec,
        @Positive double diskCheckIntervalSeconds,
        @Positive double connectTimeoutSeconds,
        @Positive double readTimeoutSeconds,
        /** Switches every background loop off (register, heartbeat, reports, scanner, disk check); tests only. */
        boolean backgroundEnabled) {

    /** One data file and one {@code .meta} sidecar per chunk live directly in this directory. */
    public Path chunkDir() {
        return dataDir.resolve("chunks");
    }

    public Duration heartbeatInterval() {
        return seconds(heartbeatIntervalSeconds);
    }

    public Duration heartbeatHttpTimeout() {
        return seconds(heartbeatHttpTimeoutSeconds);
    }

    public Duration fullReportInterval() {
        return seconds(fullReportIntervalSeconds);
    }

    public Duration ibrInterval() {
        return seconds(ibrIntervalSeconds);
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

    private static Duration seconds(double value) {
        return Duration.ofMillis(Math.round(value * 1000));
    }
}
