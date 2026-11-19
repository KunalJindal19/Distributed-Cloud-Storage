package com.objectstore.controller.cluster;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.dto.RegisterRequest;
import com.objectstore.common.dto.RegisterResponse;
import com.objectstore.common.dto.SegmentAck;
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
    private final Receipts receipts;
    private long lastHealthRun;
    private final java.util.Map<String, Long> duplicateRefusedUntil = new java.util.HashMap<>();

    public Registry(ClusterState cluster, ControllerProperties properties, Receipts receipts) {
        this.cluster = cluster;
        this.properties = properties;
        this.receipts = receipts;
    }

    // ---- registration ---------------------------------------------------------------------

    /**
     * Registers a worker process (v4 §4.8). Checks, in order: the cluster id; the age of the
     * request ({@code too_old}); the duplicate-worker refusal; a delayed older attempt of the same
     * process ({@code stale_registration}); a repeat of the last accepted attempt (answered ok,
     * nothing reset: the earlier reply was lost). Anything else is accepted and starts a new epoch.
     */
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
            if (InternalApi.nowSeconds() - request.sentAt() > properties.maxMessageAgeSeconds()) {
                cluster.increment("stale_messages");
                throw ApiException.conflict(ErrorCodes.TOO_OLD, "registration from " + workerId + " is older than "
                        + properties.maxMessageAgeSeconds() + " s");
            }
            long now = cluster.now();
            WorkerInfo worker = cluster.worker(workerId);
            if (worker == null) {
                worker = cluster.addWorker(workerId, url, request.bootId());
                accept(worker, request, url, now);
                log.info("[registry] registered {} url={} boot={} attempt={} change_seq={}", workerId, url,
                        shortId(request.bootId()), request.attempt(), request.changeSeq());
                return new RegisterResponse(true, cluster.clusterId(), worker.state.name());
            }
            Long refusedUntil = duplicateRefusedUntil.get(workerId);
            if (refusedUntil != null && now < refusedUntil && !request.bootId().equals(worker.bootId)) {
                throw ApiException.conflict(ErrorCodes.DUPLICATE_WORKER,
                        "another process is registered as " + workerId + "; refusing newcomers for a while");
            }
            boolean sameProcess = request.bootId().equals(worker.bootId);
            if (sameProcess && request.attempt() < worker.lastAttempt) {
                cluster.increment("stale_registrations");
                log.warn("[registry] {} delayed registration attempt {} refused (attempt {} already accepted)", workerId,
                        request.attempt(), worker.lastAttempt);
                throw ApiException.conflict(ErrorCodes.STALE_REGISTRATION, "registration attempt " + request.attempt()
                        + " is older than the accepted attempt " + worker.lastAttempt);
            }
            if (sameProcess && request.attempt() == worker.lastAttempt) {
                return new RegisterResponse(true, cluster.clusterId(), worker.state.name());   // a lost reply, repeated
            }
            if (!sameProcess && cluster.recordBootFlip(worker)) {
                duplicateRefusedUntil.put(workerId, now + DUPLICATE_REFUSAL_NANOS);
                log.error("[registry] duplicate WORKER_ID {}? three boot ids within a minute; refusing newcomers for 60 s", workerId);
                throw ApiException.conflict(ErrorCodes.DUPLICATE_WORKER,
                        "another process keeps registering as " + workerId);
            }
            WorkerState was = worker.state;
            accept(worker, request, url, now);
            log.info("[registry] re-registered {} url={} boot={} attempt={} change_seq={} (was {}{})", workerId, url,
                    shortId(request.bootId()), request.attempt(), request.changeSeq(), was,
                    sameProcess ? ", same process" : "");
            if (was == WorkerState.DEAD) {
                log.info("[registry] {} is back", workerId);
            }
            return new RegisterResponse(true, cluster.clusterId(), worker.state.name());
        });
    }

    /**
     * An accepted registration: a new epoch whose change stream starts after {@code change_seq}.
     * What was in flight with the old epoch is forgotten (commands, pending copies to or from the
     * worker, upload receipts, an open report session). The copies are kept: the report that
     * follows confirms or removes them.
     */
    private void accept(WorkerInfo worker, RegisterRequest request, String url, long now) {
        worker.url = url;
        worker.bootId = request.bootId();
        worker.state = WorkerState.ALIVE;
        worker.lastSeen = now;
        worker.lastHbSeq = request.hbSeq();
        worker.stats = request.stats();
        worker.epoch++;
        worker.received = request.changeSeq();
        worker.applied = request.changeSeq();
        worker.lastAttempt = request.attempt();
        cluster.closeSession(worker);
        worker.closedReportId = null;
        worker.closedLastSegment = -1;
        worker.hasReported = false;
        worker.fullReportRequested = false;
        worker.commands.clear();
        int dropped = cluster.dropPendingForWorker(worker.workerId, true, true);
        int purged = cluster.purgeReceipts(worker.workerId);
        if (dropped + purged > 0) {
            log.info("[registry] {} new epoch {}: dropped {} pending copies to or from it and {} upload receipts",
                    worker.workerId, worker.epoch, dropped, purged);
        }
    }

    // ---- heartbeat ------------------------------------------------------------------------

    /**
     * The receipt step (v4 §4.6), one brief block under the lock: acceptance; liveness; the change
     * run and the report segment ({@link Receipts}); up to 50 commands; the acknowledgements.
     */
    public HeartbeatResponse heartbeat(HeartbeatRequest request) {
        return cluster.locked(() -> {
            WorkerInfo worker = accept(request.workerId(), request.bootId(), request.sentAt());
            if (request.seq() <= worker.lastHbSeq) {
                cluster.increment("duplicates");
                return new HeartbeatResponse(true, worker.state.name(), List.of(), false, "duplicate", worker.received, null);
            }
            worker.lastHbSeq = request.seq();
            worker.lastSeen = cluster.now();
            worker.stats = request.stats();
            if (worker.state == WorkerState.STALE) {
                worker.state = WorkerState.ALIVE;
                log.info("[registry] {} ALIVE again (was STALE)", worker.workerId);
            }
            receipts.acceptChanges(worker, request.changes());
            SegmentAck segmentAck = receipts.acceptSegment(worker, request.segment());
            List<Command> commands = cluster.takeCommands(worker);
            boolean requestFull = worker.fullReportRequested;
            worker.fullReportRequested = false;
            return new HeartbeatResponse(true, worker.state.name(), commands, requestFull, null, worker.received, segmentAck);
        });
    }

    /**
     * The acceptance rules for heartbeats: unknown or DEAD → 404 (register
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
                    worker.epoch++;                       // whatever was queued for the old epoch is dropped
                    worker.hasReported = false;
                    worker.commands.clear();
                    cluster.closeSession(worker);
                    int dropped = cluster.dropPendingForWorker(worker.workerId, true, true);
                    cluster.purgeReceipts(worker.workerId);
                    int removed = cluster.removeWorkerReplicas(worker.workerId);
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
