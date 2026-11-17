package com.objectstore.controller.api;

import com.objectstore.common.dto.HealthResponse;
import com.objectstore.controller.api.dto.ClusterStatusResponse;
import com.objectstore.controller.cluster.ClusterStatusService;
import com.objectstore.controller.cluster.Fsck;
import java.net.URI;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Operational endpoints: health, the cluster overview, and the dashboard page that draws it. */
@RestController
public class ClusterController {

    private final ClusterStatusService status;
    private final Fsck fsck;

    public ClusterController(ClusterStatusService status, Fsck fsck) {
        this.status = status;
        this.fsck = fsck;
    }

    /** Liveness probe used by the Docker health check. Touches neither the database nor a worker. */
    @GetMapping("/health")
    public HealthResponse health() {
        return HealthResponse.controller();
    }

    /** Workers and their state, object/chunk/copy counts, replication health, recent repairs. */
    @GetMapping("/api/v1/cluster/status")
    public ClusterStatusResponse clusterStatus() {
        return status.status();
    }

    /**
     * End-to-end consistency check (HDFS fsck): every live worker's disk, re-hashed, against the
     * block map and the namespace, plus the invariants. Read-only.
     */
    @GetMapping("/api/v1/fsck")
    public Fsck.Result fsck() {
        return fsck.run();
    }

    @GetMapping("/dashboard")
    public ResponseEntity<Resource> dashboard() {
        return ResponseEntity.ok()
                .contentType(MediaType.TEXT_HTML)
                .cacheControl(CacheControl.noCache())
                .body(new ClassPathResource("static/dashboard.html"));
    }

    @GetMapping("/")
    public ResponseEntity<Void> root() {
        return ResponseEntity.status(HttpStatus.FOUND).location(URI.create("/dashboard")).build();
    }
}
