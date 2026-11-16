package com.objectstore.worker.api;

import com.objectstore.common.dto.HealthResponse;
import com.objectstore.worker.config.WorkerProperties;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Liveness probe used by the Docker health check. Touches neither the disk nor the network. */
@RestController
public class HealthController {

    private final WorkerProperties properties;

    public HealthController(WorkerProperties properties) {
        this.properties = properties;
    }

    @GetMapping("/health")
    public HealthResponse health() {
        return HealthResponse.worker(properties.id());
    }
}
