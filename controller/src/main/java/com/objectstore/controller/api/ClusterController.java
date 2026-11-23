package com.objectstore.controller.api;

import com.objectstore.common.dto.HealthResponse;
import com.objectstore.controller.api.dto.ClusterStatusResponse;
import com.objectstore.controller.auth.AuthInterceptor;
import com.objectstore.controller.auth.AuthUser;
import com.objectstore.controller.cluster.ClusterStatusService;
import com.objectstore.controller.cluster.Fsck;
import com.objectstore.controller.repository.ObjectRepository;
import java.net.URI;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RestController;

/**
 * Operational endpoints and pages: health, the cluster overview, the dashboard that draws it, and
 * the web client ({@code /app}) for uploading, browsing, downloading, editing and deleting objects.
 */
@RestController
public class ClusterController {

    private final ClusterStatusService status;
    private final Fsck fsck;
    private final ObjectRepository objects;

    public ClusterController(ClusterStatusService status, Fsck fsck, ObjectRepository objects) {
        this.status = status;
        this.fsck = fsck;
        this.objects = objects;
    }

    /** Liveness probe used by the Docker health check. Touches neither the database nor a worker. */
    @GetMapping("/health")
    public HealthResponse health() {
        return HealthResponse.controller();
    }

    /**
     * Whether the cluster can take writes and whether data is unavailable, for any signed-in client;
     * {@code objects} is the caller's own count.
     */
    @GetMapping("/api/v1/cluster/health")
    public ClusterStatusService.HealthSummary clusterHealth(@RequestAttribute(AuthInterceptor.USER) AuthUser user) {
        return status.summary(objects.count(user.listingOwner()));
    }

    /** Workers and their state, object/chunk/copy counts, replication health, recent repairs. The admin only. */
    @GetMapping("/api/v1/cluster/status")
    public ClusterStatusResponse clusterStatus() {
        return status.status();
    }

    /**
     * End-to-end consistency check (HDFS fsck): every live worker's disk, re-hashed, against the
     * block map and the namespace, plus the invariants. Read-only. The admin only.
     */
    @GetMapping("/api/v1/fsck")
    public Fsck.Result fsck() {
        return fsck.run();
    }

    /** The dashboard page; its data comes from admin-only endpoints, so it shows nothing to anyone else. */
    @GetMapping("/dashboard")
    public ResponseEntity<Resource> dashboard() {
        return ResponseEntity.ok()
                .contentType(MediaType.TEXT_HTML)
                .cacheControl(CacheControl.noCache())
                .body(new ClassPathResource("static/dashboard.html"));
    }

    /** The web client: a single page on top of the public REST API (same origin, no extra server). */
    @GetMapping("/app")
    public ResponseEntity<Resource> app() {
        return ResponseEntity.ok()
                .contentType(MediaType.TEXT_HTML)
                .cacheControl(CacheControl.noCache())
                .body(new ClassPathResource("static/app.html"));
    }

    @GetMapping("/")
    public ResponseEntity<Void> root() {
        return ResponseEntity.status(HttpStatus.FOUND).location(URI.create("/app")).build();
    }
}
