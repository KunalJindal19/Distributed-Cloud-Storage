package com.objectstore.controller.replication;

import com.objectstore.controller.cluster.ClusterState;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.SafeMode;
import com.objectstore.controller.config.ControllerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * The planner tick (thread {@code planner-loop}, every MONITOR_INTERVAL; v4 §7.3). All its work
 * runs under the write lock in slices of at most MAX_LOCK_HOLD_MS: between slices the lock is
 * released, so heartbeats and the writer are never held up for long.
 *
 * <ol>
 *   <li>safe mode, from the O(1) {@code safeCount}; while it is on, nothing else runs;</li>
 *   <li>to 5. the {@link ReplicationPlanner}: free streams, under-replication by level, cleanup,
 *       postponed;</li>
 *   <li value="6">the deletion ledger: waiting entries are queued (at most CLEANUP_BUDGET);</li>
 *   <li>every 10th tick, workers whose last full report is older than 2 × FULL_REPORT_INTERVAL are
 *       asked for one;</li>
 *   <li>once orphan GC becomes allowed, every worker is asked for a full report (§6.4);</li>
 *   <li>the rolling invariant check on ROLLING_CHECK_CHUNKS chunks (INVARIANT_CHECKS);</li>
 *   <li>the results are published ({@code blocked}; the levels and sets are read from the index).</li>
 * </ol>
 *
 * Pending copies and ledger deadlines expire through the pending-timer, not here.
 */
@Service
public class Monitor {

    private static final Logger log = LoggerFactory.getLogger(Monitor.class);
    private static final long BLOCKED_WARNING_NANOS = 60_000_000_000L;

    private final ClusterState cluster;
    private final SafeMode safeMode;
    private final ReplicationPlanner planner;
    private final ControllerProperties properties;
    private long ticks;
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
            ticks++;
            cluster.purgeExpiredFailures();
            if (safeMode.evaluate()) {
                return;
            }
            ReplicationPlanner.TickResult result = planner.tick(slicer);
            lastTick = result;
            warnIfBlocked(result);
            slicer.check();
            cluster.ledger().retryWaiting(properties.cleanupBudget());
            slicer.check();
            if (ticks % 10 == 0) {
                requestOverdueReports();
            }
            announceGc();
            if (properties.invariantChecks() && properties.rollingCheckChunks() > 0) {
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
     * §6.4: when orphan GC becomes allowed (safe mode off and ORPHAN_GC_DELAY since the start),
     * every non-dead worker is asked for a full report, once, so orphans seen before are collected
     * without waiting for FULL_REPORT_INTERVAL.
     */
    private void announceGc() {
        if (gcAnnounced || cluster.now() - cluster.startedAt() < properties.orphanGcDelay().toNanos()) {
            return;
        }
        gcAnnounced = true;
        for (WorkerInfo worker : cluster.workers().values()) {
            if (!worker.isDead()) {
                worker.fullReportRequested = true;
            }
        }
        log.info("[monitor] orphan GC is allowed now; asking every worker for a full report");
    }

    private void requestOverdueReports() {
        long limit = 2 * properties.fullReportInterval().toNanos();
        long now = cluster.now();
        for (WorkerInfo worker : cluster.workers().values()) {
            boolean overdue = worker.hasReported ? now - worker.lastReportAt > limit
                    : worker.session == null && !worker.reportInProgress();
            if (!worker.isDead() && overdue) {
                worker.fullReportRequested = true;
                log.info("[monitor] asking {} for a full report", worker.workerId);
            }
        }
    }
}
