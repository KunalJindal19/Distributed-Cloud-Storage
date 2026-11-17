package com.objectstore.worker.api;

import com.objectstore.common.dto.HealthResponse;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.storage.VersionFile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Liveness probe. Touches neither the disk nor the network. */
@RestController
public class HealthController {

    private final WorkerProperties properties;
    private final VersionFile version;

    public HealthController(WorkerProperties properties, VersionFile version) {
        this.properties = properties;
        this.version = version;
    }

    /** 200 normally; 503 {@code misconfigured} when the mounted volume belongs to another worker. */
    @GetMapping("/health")
    public ResponseEntity<HealthResponse> health() {
        if (version.isMisconfigured()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(new HealthResponse("misconfigured", "worker", properties.id()));
        }
        return ResponseEntity.ok(HealthResponse.worker(properties.id()));
    }
}
