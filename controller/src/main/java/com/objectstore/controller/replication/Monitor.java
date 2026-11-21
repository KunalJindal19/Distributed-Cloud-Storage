package com.objectstore.controller.replication;

import com.objectstore.controller.cluster.ClusterState;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.SafeMode;
import com.objectstore.controller.config.ControllerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * The planner tick (thread {@code planner-loop}, every MONITOR_INTERVAL). All its work runs under the
 * write lock in slices of at most MAX_LOCK_HOLD_MS: between slices the lock is released, so
 * heartbeats and the writer are never held up for long.
 *
 * <ol>
 *   <li>safe mode, from the O(1) {@code safeCount}; while it is on, nothing else runs;</li>
 *   <li>the {@link ReplicationPlanner}: free streams, repair by level, safe excess cleanup with
 *       VERIFY, postponed, waiting ops;</li>
 *   <li>periodic resyncs (v5 §5.7): a worker whose last resync is older than RESYNC_INTERVAL ±
 *       RESYNC_JITTER is resynced, at most RESYNC_MAX_CONCURRENT periodic or GC-triggered resyncs at
 *       once;</li>
 *   <li>once orphan GC becomes allowed, every worker is asked for a resync (same limit), so orphans
 *       seen before are collected;</li>
 *   <li>the rolling invariant check on ROLLING_CHECK_CHUNKS chunks (INVARIANT_CHECKS).</li>
 * </ol>
 *
 * Unanswered ops are resent through the resend timer, not here.
 */
@Service
public class Monitor {

    private static final Logger log = LoggerFactory.getLogger(Monitor.class);
    private static final long BLOCKED_WARNING_NANOS = 60_000_000_000L;

    private final ClusterState cluster;
    private final SafeMode safeMode;
    private final ReplicationPlanner planner;
    private final ControllerProperties properties;
    private boolean gcAnnounced;
    private long lastBlockedWarning;
    private volatile ReplicationPlanner.TickResult lastTick;

    public Monitor(ClusterState cluster, SafeMode safeMode, ReplicationPlanner planner, ControllerProperties properties) {
        this.cluster = cluster;
        this.safeMode = safeMode;
        this.planner = planner;
        this.properties = properties;
    }

    public ReplicationPlanner.TickResult lastTick() {
        return lastTick;
    }

    public void tick() {
        long maxHold = properties.maxLockHold().toNanos();
        long[] sliceEnd = {0};
        ReplicationPlanner.Slicer slicer = () -> {
            if (System.nanoTime() >= sliceEnd[0]) {
                cluster.unlockWrite();               // flushes the index, lets everyone else in
                cluster.lockWrite();
                sliceEnd[0] = System.nanoTime() + maxHold;
            }
        };
        cluster.lockWrite();
        sliceEnd[0] = System.nanoTime() + maxHold;
        try {
            cluster.purgeExpiredFailures();
            if (safeMode.evaluate()) {
                return;
            }
            ReplicationPlanner.TickResult result = planner.tick(slicer);
            lastTick = result;
            warnIfBlocked(result);
            slicer.check();
            planner.retryWaiting(properties.cleanupBudget(), slicer);
            slicer.check();
            announceGc();
            startResyncs();
            if (properties.invariantChecks() && properties.rollingCheckChunks() > 0) {
                // A hold of its own: the check (about 1.5 ms for 1,000 chunks at scale) must not be
                // added to whatever is left of the last planner slice (found by the scale test).
                cluster.unlockWrite();
                cluster.lockWrite();
                cluster.rollingCheck(properties.rollingCheckChunks());
            }
        } finally {
            cluster.unlockWrite();
        }
    }

    /** RF larger than the eligible workers (or no free target) blocks repairs: one warning per minute, no spinning. */
    private void warnIfBlocked(ReplicationPlanner.TickResult result) {
        long now = cluster.now();
        if (result.blocked() > 0 && now - lastBlockedWarning > BLOCKED_WARNING_NANOS) {
            lastBlockedWarning = now;
            cluster.increment("blocked_warnings");
            log.warn("[monitor] {} chunks need a copy but no source or target is available now "
                    + "(REPLICATION_FACTOR={}, {} free streams)", result.blocked(), properties.replicationFactor(),
                    result.freeTotal());
        }
    }

    /**
     * When orphan GC becomes allowed (safe mode off and ORPHAN_GC_DELAY since the start), every
     * non-dead worker is asked for a resync, once, so orphans seen before are collected without
     * waiting for RESYNC_INTERVAL.
     */
    private void announceGc() {
        if (gcAnnounced || cluster.now() - cluster.startedAt() < properties.orphanGcDelay().toNanos()) {
            return;
        }
        gcAnnounced = true;
        for (WorkerInfo worker : cluster.workers().values()) {
            if (!worker.isDead()) {
                worker.resyncRequested = "orphan GC allowed";
            }
        }
        log.info("[monitor] orphan GC is allowed now; every worker gets a resync");
    }

    /**
     * Periodic and GC-triggered resyncs, at most RESYNC_MAX_CONCURRENT at once: a requested one first,
     * then any worker whose last resync is older than RESYNC_INTERVAL ± RESYNC_JITTER.
     */
    private void startResyncs() {
        long now = cluster.now();
        int running = cluster.periodicResyncs();
        for (WorkerInfo worker : cluster.workersSorted()) {
            if (worker.isDead() || worker.state != ClusterState.WorkerState.ALIVE || !worker.hasReported
                    || worker.resyncing) {
                continue;
            }
            if (worker.nextPeriodicAt == 0) {
                double jitter = properties.resyncJitter() * (random.nextDouble() * 2 - 1);
                worker.nextPeriodicAt = worker.lastReportAt
                        + (long) (properties.resyncInterval().toNanos() * (1 + jitter));
            }
            boolean due = worker.resyncRequested != null || now >= worker.nextPeriodicAt;
            if (due && running < properties.resyncMaxConcurrent()) {
                String why = worker.resyncRequested != null ? worker.resyncRequested : "periodic";
                cluster.startResync(worker, why, true);
                running++;
            }
        }
    }

    private final java.util.Random random = new java.util.Random();
}
