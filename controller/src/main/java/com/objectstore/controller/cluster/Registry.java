package com.objectstore.controller.cluster;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.dto.RegisterRequest;
import com.objectstore.common.dto.RegisterResponse;
import com.objectstore.common.web.ApiException;
import com.objectstore.common.web.ErrorCodes;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.ClusterState.WorkerState;
import com.objectstore.controller.config.ControllerProperties;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Worker registration, heartbeats and liveness (the NameNode's DatanodeManager).
 *
 * <p>A worker is ALIVE while its heartbeats arrive, STALE after STALE_AFTER of silence (still
 * holding its copies, but avoided for reads and writes), and DEAD after DEAD_AFTER, at which
 * point every copy it held is forgotten and gets re-replicated elsewhere. A message from a
 * worker the controller does not know, or from a DEAD one, is answered with 404 so that the
 * worker registers again (HDFS {@code DNA_REGISTER}).
 */
@Service
public class Registry {

    private static final Logger log = LoggerFactory.getLogger(Registry.class);
    private static final long DUPLICATE_REFUSAL_NANOS = 60_000_000_000L;

    private final ClusterState cluster;
    private final ControllerProperties properties;
    private long lastHealthRun;
    private final java.util.Map<String, Long> duplicateRefusedUntil = new java.util.HashMap<>();

    public Registry(ClusterState cluster, ControllerProperties properties) {
        this.cluster = cluster;
        this.properties = properties;
    }

    // ---- registration ---------------------------------------------------------------------

    public RegisterResponse register(RegisterRequest request) {
        return cluster.locked(() -> {
            String workerId = request.workerId();
            String url = request.url().endsWith("/") ? request.url().substring(0, request.url().length() - 1) : request.url();
            if (request.clusterId() != null && !request.clusterId().equals(cluster.clusterId())) {
                log.error("[registry] {} belongs to cluster {} but this is cluster {}; refusing it (its data is left untouched)",
                        workerId, request.clusterId(), cluster.clusterId());
                throw ApiException.conflict(ErrorCodes.CLUSTER_ID_MISMATCH,
                        "worker belongs to cluster " + request.clusterId() + ", this controller is cluster " + cluster.clusterId());
            }
            long now = cluster.now();
            WorkerInfo worker = cluster.worker(workerId);
            if (worker == null) {
                worker = cluster.addWorker(workerId, url, request.bootId());
                log.info("[registry] registered {} url={} boot={}", workerId, url, shortId(request.bootId()));
                reset(worker, request, now);
                return new RegisterResponse(true, cluster.clusterId(), worker.state.name());
            }
            Long refusedUntil = duplicateRefusedUntil.get(workerId);
            if (refusedUntil != null && now < refusedUntil && !request.bootId().equals(worker.bootId)) {
                throw ApiException.conflict(ErrorCodes.DUPLICATE_WORKER,
                        "another process is registered as " + workerId + "; refusing newcomers for a while");
            }
            if (request.bootId().equals(worker.bootId) && !worker.isDead()) {
                // A retried registration from the same process: just refresh it.
                worker.url = url;
                worker.lastSeen = now;
                worker.stats = request.stats();
                worker.state = WorkerState.ALIVE;
                return new RegisterResponse(true, cluster.clusterId(), worker.state.name());
            }
            if (!request.bootId().equals(worker.bootId) && cluster.recordBootFlip(worker)) {
                duplicateRefusedUntil.put(workerId, now + DUPLICATE_REFUSAL_NANOS);
                log.error("[registry] duplicate WORKER_ID {}? three boot ids within a minute; refusing newcomers for 60 s", workerId);
                throw ApiException.conflict(ErrorCodes.DUPLICATE_WORKER,
                        "another process keeps registering as " + workerId);
            }
            boolean wasDead = worker.isDead();
            log.info("[registry] re-registered {} url={} boot={} (was {})", workerId, url, shortId(request.bootId()), worker.state);
            worker.url = url;
            worker.bootId = request.bootId();
            reset(worker, request, now);
            if (wasDead) {
                log.info("[registry] {} is back", workerId);
            }
            return new RegisterResponse(true, cluster.clusterId(), worker.state.name());
        });
    }

    /** A (re)started process: forget what was in flight with the old one; keep its copies for the report to confirm. */
    private void reset(WorkerInfo worker, RegisterRequest request, long now) {
        worker.state = WorkerState.ALIVE;
        worker.lastSeen = now;
        worker.lastHbSeq = request.hbSeq();
        worker.lastReportSeq = request.reportSeq();
        worker.hasReported = false;
        worker.stats = request.stats();
        worker.commands.clear();
        int dropped = cluster.dropPendingForWorker(worker.workerId, true, false);
        if (dropped > 0) {
            log.info("[registry] {} restarted; dropped {} pending copies it was the source of", worker.workerId, dropped);
        }
    }

    // ---- heartbeat ------------------------------------------------------------------------

    public HeartbeatResponse heartbeat(HeartbeatRequest request) {
        return cluster.locked(() -> {
            WorkerInfo worker = accept(request.workerId(), request.bootId(), request.sentAt());
            if (request.seq() <= worker.lastHbSeq) {
                cluster.increment("duplicates");
                return new HeartbeatResponse(true, worker.state.name(), List.of(), false, "duplicate");
            }
            worker.lastHbSeq = request.seq();
            worker.lastSeen = cluster.now();
            worker.stats = request.stats();
            if (worker.state == WorkerState.STALE) {
                worker.state = WorkerState.ALIVE;
                log.info("[registry] {} ALIVE again (was STALE)", worker.workerId);
            }
            List<Command> commands = cluster.takeCommands(worker);
            boolean requestFull = !worker.hasReported || worker.fullReportRequested
                    || cluster.now() - worker.lastReportAt > 2 * properties.fullReportInterval().toNanos();
            worker.fullReportRequested = false;
            return new HeartbeatResponse(true, worker.state.name(), commands, requestFull, null);
        });
    }

    /**
     * The acceptance rules shared by heartbeats and reports: unknown or DEAD → 404 (register
     * again); a boot id that is not the current one → 409 {@code stale_boot}; a message older
     * than MAX_MESSAGE_AGE → 409 {@code too_old}. Must be called under the lock.
     */
    public WorkerInfo accept(String workerId, String bootId, double sentAt) {
        WorkerInfo worker = cluster.worker(workerId);
        if (worker == null || worker.isDead()) {
            throw ApiException.notFound(ErrorCodes.UNKNOWN_WORKER,
                    "worker '" + workerId + "' is not registered; register first");
        }
        if (!bootId.equals(worker.bootId)) {
            throw ApiException.conflict(ErrorCodes.STALE_BOOT, "boot id " + shortId(bootId) + " is not the current process of " + workerId);
        }
        if (InternalApi.nowSeconds() - sentAt > properties.maxMessageAgeSeconds()) {
            cluster.increment("stale_messages");
            throw ApiException.conflict(ErrorCodes.TOO_OLD, "message from " + workerId + " is older than "
                    + properties.maxMessageAgeSeconds() + " s");
        }
        return worker;
    }

    // ---- liveness -------------------------------------------------------------------------

    /**
     * One pass of the health loop. If the controller itself was paused or starved since the last
     * pass, every worker gets a fresh {@code last_seen} first, so a controller stall never turns
     * into a mass declaration of death.
     */
    public void healthPass() {
        cluster.locked(() -> {
            long now = cluster.now();
            long staleAfter = properties.staleAfter().toNanos();
            long deadAfter = properties.deadAfter().toNanos();
            if (lastHealthRun != 0 && now - lastHealthRun > staleAfter / 2) {
                log.warn("[registry] controller stalled {}s - liveness grace applied",
                        String.format("%.1f", (now - lastHealthRun) / 1e9));
                for (WorkerInfo worker : cluster.workers().values()) {
                    if (!worker.isDead()) {
                        worker.lastSeen = now;
                    }
                }
            }
            lastHealthRun = now;
            for (WorkerInfo worker : cluster.workers().values()) {
                long silence = now - worker.lastSeen;
                if (worker.state == WorkerState.ALIVE && silence > staleAfter) {
                    worker.state = WorkerState.STALE;
                    log.warn("[registry] {} STALE after {}s of silence", worker.workerId, String.format("%.1f", silence / 1e9));
                }
                if (!worker.isDead() && silence > deadAfter) {
                    worker.state = WorkerState.DEAD;
                    int removed = cluster.removeWorkerReplicas(worker.workerId);
                    worker.commands.clear();
                    int dropped = cluster.dropPendingForWorker(worker.workerId, true, true);
                    worker.hasReported = false;
                    log.warn("[registry] {} DEAD after {}s - {} replicas removed, {} pending copies dropped",
                            worker.workerId, String.format("%.1f", silence / 1e9), removed, dropped);
                }
            }
        });
    }

    static String shortId(String id) {
        return id == null ? "?" : id.length() > 8 ? id.substring(0, 8) : id;
    }
}
