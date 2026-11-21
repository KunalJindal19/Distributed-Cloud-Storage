package com.objectstore.controller.cluster;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.dto.RegisterRequest;
import com.objectstore.common.dto.RegisterResponse;
import com.objectstore.common.dto.Resync;
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
 *
 * <p>Registration and DEAD are tasks on the writer's queue ({@code Register}, {@code MarkDead}),
 * so they are ordered with everything else that worker did; the heartbeat's receipt step is one
 * brief block under the write lock that only queues work.
 *
 * <p><b>Fenced resync (v5 §5).</b> Every accepted registration starts a resync: the reply carries the
 * fence {@code floor = t0} and attempt 1. Heartbeat replies carry {@code resync {t0, attempt}} until
 * the attempt's segment 0 has arrived. While a worker is resyncing nothing new is handed to it (the
 * hand-out filter in {@link ClusterState#takeCommands}); a resync without segment progress for
 * RESYNC_STALL_TIMEOUT gets a new attempt.
 */
@Service
public class Registry {

    private static final Logger log = LoggerFactory.getLogger(Registry.class);
    private static final long DUPLICATE_REFUSAL_NANOS = 60_000_000_000L;

    private final ClusterState cluster;
    private final ControllerProperties properties;
    private final Receipts receipts;
    private final ApplyQueue queue;
    private final Writer writer;
    private long lastHealthRun;
    private com.objectstore.controller.observability.MessageTrace trace =
            com.objectstore.controller.observability.MessageTrace.OFF;
    private final java.util.Map<String, Long> duplicateRefusedUntil = new java.util.HashMap<>();

    public Registry(ClusterState cluster, ControllerProperties properties, Receipts receipts, ApplyQueue queue,
                    Writer writer) {
        this.cluster = cluster;
        this.properties = properties;
        this.receipts = receipts;
        this.queue = queue;
        this.writer = writer;
    }

    /** The message trace (TRACE_MESSAGES); off in unit tests. */
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setTrace(com.objectstore.controller.observability.MessageTrace trace) {
        this.trace = trace;
    }

    // ---- registration ---------------------------------------------------------------------

    /**
     * Registers a worker process (v4 §4.8). Checks, in order: the cluster id; the age of the
     * request ({@code too_old}); the duplicate-worker refusal; a delayed older attempt of the same
     * process ({@code stale_registration}); a repeat of the last accepted attempt (answered ok,
     * nothing reset: the earlier reply was lost). Anything else is accepted and starts a new epoch.
     *
     * <p>The registration is a {@code Register} task; the HTTP thread waits for its result.
     */
    public RegisterResponse register(RegisterRequest request) {
        try {
            RegisterResponse response = doRegister(request);
            trace.register(request, response, null);
            return response;
        } catch (RuntimeException refused) {
            trace.register(request, null, refused);
            throw refused;
        }
    }

    private RegisterResponse doRegister(RegisterRequest request) {
        Task task = Task.call("Register", () -> {
            try {
                return applyRegister(request);
            } catch (ApiException refused) {
                return refused;                 // an answer, not a failure of the task
            }
        });
        Object result;
        try {
            queue.put(task);
            result = writer.await(task, properties.uploadCommitWait());
        } catch (InterruptedException | java.util.concurrent.TimeoutException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw ApiException.unavailable(ErrorCodes.SERVICE_UNAVAILABLE, "the controller is busy; register again");
        }
        if (result instanceof ApiException refused) {
            throw refused;
        }
        return (RegisterResponse) result;
    }

    /** The {@code Register} task's body (§4.8); runs on the writer, under the write lock. */
    RegisterResponse applyRegister(RegisterRequest request) {
        {
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
                log.info("[registry] registered {} url={} boot={} attempt={} change_seq={} fence={}", workerId, url,
                        shortId(request.bootId()), request.attempt(), request.changeSeq(), worker.fenceFloor);
                return response(worker);
            }
            if (worker.purging) {
                // Impossible: MarkDead's purge is one task whose steps keep their queue position.
                throw new IllegalStateException("registration of " + workerId + " during its DEAD purge");
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
                return response(worker);                     // a lost reply, repeated: the same fence and attempt
            }
            if (!sameProcess && cluster.recordBootFlip(worker)) {
                duplicateRefusedUntil.put(workerId, now + DUPLICATE_REFUSAL_NANOS);
                log.error("[registry] duplicate WORKER_ID {}? three boot ids within a minute; refusing newcomers for 60 s", workerId);
                throw ApiException.conflict(ErrorCodes.DUPLICATE_WORKER,
                        "another process keeps registering as " + workerId);
            }
            WorkerState was = worker.state;
            accept(worker, request, url, now);
            log.info("[registry] re-registered {} url={} boot={} attempt={} change_seq={} fence={} (was {}{})", workerId,
                    url, shortId(request.bootId()), request.attempt(), request.changeSeq(), worker.fenceFloor, was,
                    sameProcess ? ", same process" : "");
            if (was == WorkerState.DEAD) {
                log.info("[registry] {} is back", workerId);
            }
            return response(worker);
        }
    }

    private RegisterResponse response(WorkerInfo worker) {
        return new RegisterResponse(true, cluster.clusterId(), worker.state.name(), worker.fenceFloor, worker.attempt);
    }

    /**
     * An accepted registration: a new epoch whose change stream starts after {@code change_seq}.
     * What was in flight with the old process is forgotten (its command queue, receipts, an open
     * listing); REPAIR ops it was the source of are re-sourced. The copies and the ops targeting it
     * are kept: the resync that starts here confirms or removes the copies, and its completion drops
     * the ops at or below the fence.
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
        worker.hasReported = false;
        cluster.clearCommands(worker);
        int purged = cluster.purgeReceipts(worker.workerId);
        if (purged > 0) {
            log.info("[registry] {} new epoch {}: dropped {} upload receipts", worker.workerId, worker.epoch, purged);
        }
        cluster.startResync(worker, "registration", false);
    }

    // ---- heartbeat ------------------------------------------------------------------------

    /**
     * The receipt step (v4 §4.6), one brief block under the lock: acceptance; liveness; the change
     * run and the report segment ({@link Receipts}); up to 50 commands; the acknowledgements.
     */
    public HeartbeatResponse heartbeat(HeartbeatRequest request) {
        try {
            HeartbeatResponse response = doHeartbeat(request);
            trace.heartbeat(request, response, null);           // printed after the lock was released
            return response;
        } catch (RuntimeException refused) {
            trace.heartbeat(request, null, refused);
            throw refused;
        }
    }

    private HeartbeatResponse doHeartbeat(HeartbeatRequest request) {
        return cluster.write(() -> {
            WorkerInfo worker = accept(request.workerId(), request.bootId(), request.sentAt());
            if (request.seq() <= worker.lastHbSeq) {
                cluster.increment("duplicates");
                return new HeartbeatResponse(true, worker.state.name(), List.of(), null, "duplicate", worker.received, null);
            }
            worker.lastHbSeq = request.seq();
            worker.lastSeen = cluster.now();
            worker.stats = request.stats();
            if (worker.state == WorkerState.STALE) {
                worker.state = WorkerState.ALIVE;
                log.info("[registry] {} ALIVE again (was STALE)", worker.workerId);
                cluster.markInsertTargetsDirty(worker.workerId);
                cluster.releasePostponed(properties.postponedBudget());
            }
            receipts.acceptChanges(worker, request.changes());
            SegmentAck segmentAck = receipts.acceptSegment(worker, request.segment());
            receipts.resyncWanted(worker, request.resyncWanted());
            List<Command> commands = cluster.takeCommands(worker);
            Resync resync = worker.resyncing && !worker.segmentSeen ? new Resync(worker.fenceFloor, worker.attempt) : null;
            return new HeartbeatResponse(true, worker.state.name(), commands, resync, null, worker.received, segmentAck);
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
     * One pass of the health loop (a brief write-lock block). If the controller itself was paused
     * or starved since the last pass, every worker gets a fresh {@code last_seen} first, so a
     * controller stall never turns into a mass declaration of death. ALIVE → STALE happens here;
     * DEAD is a queued {@code MarkDead} task, which checks again when it runs.
     */
    public void healthPass() {
        cluster.write(() -> {
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
            long stall = properties.resyncStallTimeout().toNanos();
            for (WorkerInfo worker : cluster.workers().values()) {
                long silence = now - worker.lastSeen;
                if (worker.state == WorkerState.ALIVE && silence > staleAfter) {
                    worker.state = WorkerState.STALE;
                    cluster.markInsertTargetsDirty(worker.workerId);
                    log.warn("[registry] {} STALE after {}s of silence", worker.workerId, String.format("%.1f", silence / 1e9));
                }
                if (worker.resyncing && !worker.isDead() && worker.state == WorkerState.ALIVE
                        && now - worker.lastSegmentAt > stall) {
                    cluster.newAttempt(worker, "no segment progress for " + properties.resyncStallTimeoutSeconds() + " s");
                }
                if (!worker.isDead() && !worker.deadQueued && silence > deadAfter
                        && queue.offer(markDead(worker.workerId, worker.epoch))) {
                    worker.deadQueued = true;
                }
            }
        });
    }

    /**
     * {@code MarkDead} (v5 §6.1), a multi-step task. It first checks again: if the epoch changed or
     * a heartbeat arrived since it was queued, nothing happens. Otherwise the worker is DEAD: a new
     * epoch, its commands cleared, receipts purged, every op targeting it dropped, the REPAIR ops it
     * was the source of re-sourced, its resync ended, its VERIFY credit void ({@code verifyEpoch++}),
     * and its replicas removed in steps while it is {@code purging}. Removing them lowers L and the
     * planner schedules repairs.
     */
    Task markDead(String workerId, long epoch) {
        List<String>[] sweep = new List[1];
        int[] cursor = {0};
        int[] dropped = {0};
        double[] silenceSeconds = {0};
        return Task.steps("MarkDead", deadline -> {
            WorkerInfo worker = cluster.worker(workerId);
            if (worker == null) {
                return Task.Step.DONE;
            }
            if (sweep[0] == null) {
                long silence = cluster.now() - worker.lastSeen;
                if (worker.epoch != epoch || worker.isDead() || silence <= properties.deadAfter().toNanos()) {
                    worker.deadQueued = false;             // it spoke (or re-registered) meanwhile
                    return Task.Step.DONE;
                }
                silenceSeconds[0] = silence / 1e9;
                worker.state = WorkerState.DEAD;
                worker.epoch++;                            // whatever was queued for the old epoch is dropped
                worker.purging = true;
                worker.hasReported = false;
                cluster.clearCommands(worker);
                cluster.closeSession(worker);
                worker.resetReceipt();
                dropped[0] = cluster.ops().dropTargeting(workerId, Long.MAX_VALUE, "worker DEAD");
                cluster.ops().resourceFrom(workerId);
                worker.resyncing = false;
                worker.periodicResync = false;
                worker.resyncRequested = null;
                worker.verifyEpoch++;
                cluster.purgeReceipts(workerId);
                sweep[0] = new java.util.ArrayList<>(cluster.chunksOn(workerId));
            }
            int items = 0;
            while (cursor[0] < sweep[0].size()) {
                cluster.removeReplica(sweep[0].get(cursor[0]++), workerId);   // runs protect()
                if (++items >= NamespaceTasks.STEP_ITEMS || System.nanoTime() >= deadline) {
                    if (cursor[0] < sweep[0].size()) {
                        return Task.Step.MORE;
                    }
                }
            }
            worker.purging = false;
            worker.deadQueued = false;
            log.warn("[registry] {} DEAD after {}s - {} replicas removed, {} pending ops dropped", workerId,
                    String.format("%.1f", silenceSeconds[0]), sweep[0].size(), dropped[0]);
            return Task.Step.DONE;
        });
    }

    static String shortId(String id) {
        return id == null ? "?" : id.length() > 8 ? id.substring(0, 8) : id;
    }
}
