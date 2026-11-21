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

/**
 * Controller settings, bound once at start-up from environment variables (see controller.yml and
 * the README). The compact constructor rejects inconsistent values with a message naming the setting.
 */
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
        @Positive double targetBackoffSeconds,
        @Min(1) int maxStreams,
        @Min(1) int maxCommandsPerWorker,
        @Positive double maxMessageAgeSeconds,
        @DecimalMin("0") @DecimalMax("1") double safeModeThreshold,
        @PositiveOrZero double safeModeExtensionSeconds,
        @PositiveOrZero double safeModeTimeoutSeconds,
        @PositiveOrZero double orphanGcDelaySeconds,
        @DecimalMin("0") @DecimalMax("1") double orphanGcMaxFraction,
        @PositiveOrZero long minFreeBytes,
        @Positive double connectTimeoutSeconds,
        @Positive double readTimeoutSeconds,
        boolean invariantChecks,
        /** Longest continuous write-lock hold by the writer before it yields. */
        @Min(1) long maxLockHoldMs,
        /** Tasks the apply queue holds; when full, the receipt step does not acknowledge. */
        @Min(1) int applyQueueCapacity,
        /** How long an upload (or delete) waits for its task before answering anyway. */
        @Positive double uploadCommitWaitSeconds,
        /** How long a download waits for a chunk that is in the database but not yet readable in memory. */
        @PositiveOrZero double downloadSettleWaitSeconds,
        /** Waiting ops re-checked per monitor tick; cleanup chunks visited per tick. */
        @Min(1) int cleanupBudget,
        /** Chunks visited in the under-replication levels per planner tick. */
        @Min(1) int plannerVisitBudget,
        /** Postponed chunks re-checked per tick. */
        @Min(1) int postponedBudget,
        /** Chunks checked by the rolling invariant check per tick. */
        @Min(0) int rollingCheckChunks,
        /** v5 §13: first resend of an unanswered op (replaces PENDING_TIMEOUT). */
        @Positive double opResendInitialSeconds,
        /** v5 §13: the resend backoff never exceeds this. */
        @Positive double opResendMaxSeconds,
        /** v5 §13: an INSERT op is dropped after this many unanswered attempts (DELETEs never are). */
        @Min(1) int opMaxInsertAttempts,
        /** v5 §13: periodic resync of every worker (replaces the worker's FULL_REPORT_INTERVAL). */
        @Positive double resyncIntervalSeconds,
        /** v5 §13: ± this fraction of RESYNC_INTERVAL, so workers do not resync in step. */
        @DecimalMin("0") @DecimalMax("0.9") double resyncJitter,
        /** v5 §13: periodic and GC-triggered resyncs running at once. */
        @Min(1) int resyncMaxConcurrent,
        /** v5 §13: no segment progress for this long → the next attempt. */
        @Positive double resyncStallTimeoutSeconds,
        /** v5 §13: how recent a survivor's VERIFY must be for an EXCESS delete. */
        @Positive double verifyFreshForDeleteSeconds,
        /** v5 §13: minimum gap between VERIFYs for one copy. */
        @Positive double verifyResendSeconds) {

    public ControllerProperties {
        require(chunkSize > 0, "CHUNK_SIZE must be positive");
        require(replicationFactor > 0, "REPLICATION_FACTOR must be positive");
        require(opResendInitialSeconds > 0, "OP_RESEND_INITIAL must be positive");
        require(opResendMaxSeconds > 0, "OP_RESEND_MAX must be positive");
        require(opResendInitialSeconds <= opResendMaxSeconds, "OP_RESEND_INITIAL (" + opResendInitialSeconds
                + " s) must not exceed OP_RESEND_MAX (" + opResendMaxSeconds + " s)");
        require(opMaxInsertAttempts > 0, "OP_MAX_INSERT_ATTEMPTS must be positive");
        require(resyncIntervalSeconds > 0, "RESYNC_INTERVAL must be positive");
        require(resyncJitter >= 0 && resyncJitter < 1, "RESYNC_JITTER must be at least 0 and below 1");
        require(resyncMaxConcurrent > 0, "RESYNC_MAX_CONCURRENT must be positive");
        require(resyncStallTimeoutSeconds > 0, "RESYNC_STALL_TIMEOUT must be positive");
        require(verifyFreshForDeleteSeconds > 0, "VERIFY_FRESH_FOR_DELETE must be positive");
        require(verifyResendSeconds > 0, "VERIFY_RESEND must be positive");
        require(deadAfterSeconds > staleAfterSeconds, "DEAD_AFTER must be larger than STALE_AFTER");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

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

    public Duration targetBackoff() {
        return seconds(targetBackoffSeconds);
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

    public Duration connectTimeout() {
        return seconds(connectTimeoutSeconds);
    }

    public Duration readTimeout() {
        return seconds(readTimeoutSeconds);
    }

    public Duration maxLockHold() {
        return Duration.ofMillis(maxLockHoldMs);
    }

    public Duration uploadCommitWait() {
        return seconds(uploadCommitWaitSeconds);
    }

    public Duration downloadSettleWait() {
        return seconds(downloadSettleWaitSeconds);
    }

    public Duration opResendInitial() {
        return seconds(opResendInitialSeconds);
    }

    public Duration opResendMax() {
        return seconds(opResendMaxSeconds);
    }

    public Duration resyncInterval() {
        return seconds(resyncIntervalSeconds);
    }

    public Duration resyncStallTimeout() {
        return seconds(resyncStallTimeoutSeconds);
    }

    public Duration verifyFreshForDelete() {
        return seconds(verifyFreshForDeleteSeconds);
    }

    public Duration verifyResend() {
        return seconds(verifyResendSeconds);
    }

    private static Duration seconds(double value) {
        return Duration.ofMillis(Math.round(value * 1000));
    }
}
