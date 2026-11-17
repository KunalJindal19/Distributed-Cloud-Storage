package com.objectstore.controller.config;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.nio.file.Path;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/** Controller settings, bound once at start-up from environment variables (see application.yml and the README). */
@Validated
@ConfigurationProperties(prefix = "objectstore")
public record ControllerProperties(
        @Min(1) @Max(64 * 1024 * 1024) int chunkSize,
        @Min(1) int replicationFactor,
        @NotNull Path dbPath,
        @Positive double staleAfterSeconds,
        @Positive double deadAfterSeconds,
        @DecimalMin("0") @DecimalMax("1") double staleRatio,
        @Positive double healthIntervalSeconds,
        @Positive double monitorIntervalSeconds,
        @Positive double pendingTimeoutSeconds,
        @Positive double targetBackoffSeconds,
        @Min(1) int maxStreams,
        @Positive double commandTtlSeconds,
        @Min(1) int maxCommandsPerWorker,
        @Positive double maxMessageAgeSeconds,
        @DecimalMin("0") @DecimalMax("1") double safeModeThreshold,
        @PositiveOrZero double safeModeExtensionSeconds,
        @PositiveOrZero double safeModeTimeoutSeconds,
        @PositiveOrZero double orphanGcDelaySeconds,
        @DecimalMin("0") @DecimalMax("1") double orphanGcMaxFraction,
        @PositiveOrZero long minFreeBytes,
        @Positive double fullReportIntervalSeconds,
        @Positive double connectTimeoutSeconds,
        @Positive double readTimeoutSeconds,
        boolean invariantChecks) {

    public Duration staleAfter() {
        return seconds(staleAfterSeconds);
    }

    public Duration deadAfter() {
        return seconds(deadAfterSeconds);
    }

    public Duration healthInterval() {
        return seconds(healthIntervalSeconds);
    }

    public Duration monitorInterval() {
        return seconds(monitorIntervalSeconds);
    }

    public Duration pendingTimeout() {
        return seconds(pendingTimeoutSeconds);
    }

    public Duration targetBackoff() {
        return seconds(targetBackoffSeconds);
    }

    public Duration commandTtl() {
        return seconds(commandTtlSeconds);
    }

    public Duration maxMessageAge() {
        return seconds(maxMessageAgeSeconds);
    }

    public Duration safeModeExtension() {
        return seconds(safeModeExtensionSeconds);
    }

    public Duration safeModeTimeout() {
        return seconds(safeModeTimeoutSeconds);
    }

    public Duration orphanGcDelay() {
        return seconds(orphanGcDelaySeconds);
    }

    public Duration fullReportInterval() {
        return seconds(fullReportIntervalSeconds);
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
