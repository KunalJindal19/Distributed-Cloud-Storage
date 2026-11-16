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
 * Worker settings, bound once at start-up from environment variables (see application.yml).
 *
 * @param id                       WORKER_ID, e.g. {@code worker1}; also the container's host name
 * @param dataDir                  DATA_DIR, the mount point of the persistent volume
 * @param controllerUrl            CONTROLLER_URL, where to register and send heartbeats
 * @param advertiseUrl             ADVERTISE_URL, the URL the controller should use to reach this
 *                                 worker; blank means {@code http://<id>:<port>}
 * @param heartbeatIntervalSeconds HEARTBEAT_INTERVAL
 * @param maxChunkSize             CHUNK_SIZE, the largest chunk body accepted
 * @param registrationEnabled      switches the register/heartbeat loop off (tests only)
 */
@Validated
@ConfigurationProperties(prefix = "worker")
public record WorkerProperties(
        @NotBlank @Pattern(regexp = InternalApi.WORKER_ID_PATTERN) String id,
        @NotNull Path dataDir,
        @NotBlank String controllerUrl,
        String advertiseUrl,
        @Positive double heartbeatIntervalSeconds,
        @Positive int maxChunkSize,
        boolean registrationEnabled) {

    /** One file per chunk lives directly in this directory. */
    public Path chunkDir() {
        return dataDir.resolve("chunks");
    }

    public Duration heartbeatInterval() {
        return Duration.ofMillis(Math.round(heartbeatIntervalSeconds * 1000));
    }
}
