package com.objectstore.controller.replication;

import com.objectstore.controller.cluster.ClusterState;
import com.objectstore.controller.cluster.ClusterState.PendingCopy;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.SafeMode;
import com.objectstore.controller.config.ControllerProperties;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * The replication monitor (HDFS RedundancyMonitor). Every MONITOR_INTERVAL, as one synchronous
 * block under the cluster lock:
 *
 * <ol start="0">
 *   <li>forget expired (chunk, worker) failure back-offs;</li>
 *   <li>time out pending copies (repairs and upload copies) that were never confirmed, and
 *       remember to avoid that pair for a repair;</li>
 *   <li>evaluate safe mode; while it is on, nothing below runs;</li>
 *   <li>plan repairs and trims, fewest live copies first ({@link ReplicationPlanner});</li>
 *   <li>ask workers that have not reported for a long time for a full report;</li>
 *   <li>check the invariants.</li>
 * </ol>
 *
 * The monitor only decides; the workers do the copying and deleting when they pick up their
 * commands with the next heartbeat.
 */
@Service
public class Monitor {

    private static final Logger log = LoggerFactory.getLogger(Monitor.class);

    private final ClusterState cluster;
    private final SafeMode safeMode;
    private final ReplicationPlanner planner;
    private final ControllerProperties properties;
    private long ticks;

    public Monitor(ClusterState cluster, SafeMode safeMode, ReplicationPlanner planner, ControllerProperties properties) {
        this.cluster = cluster;
        this.safeMode = safeMode;
        this.planner = planner;
        this.properties = properties;
    }

    public void tick() {
        cluster.locked(() -> {
            ticks++;
            cluster.purgeExpiredFailures();
            expirePendingCopies();
            if (safeMode.evaluate()) {
                return;
            }
            planner.plan();
            if (ticks % 10 == 0) {
                requestOverdueReports();
            }
            if (properties.invariantChecks()) {
                cluster.checkInvariants();
            }
        });
    }

    private void expirePendingCopies() {
        long now = cluster.now();
        for (Iterator<Map.Entry<String, List<PendingCopy>>> it = cluster.pending().entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, List<PendingCopy>> entry = it.next();
            for (PendingCopy copy : new ArrayList<>(entry.getValue())) {
                if (copy.deadline() < now) {
                    entry.getValue().remove(copy);
                    if (copy.kind() == ClusterState.PendingKind.UPLOAD) {
                        cluster.record("timeout", entry.getKey(), null, copy.target(),
                                "upload copy not confirmed within " + properties.pendingTimeoutSeconds() + " s; normal repair takes over");
                    } else {
                        cluster.recordFailure(entry.getKey(), copy.target());
                        cluster.record("timeout", entry.getKey(), copy.source(), copy.target(),
                                "copy not confirmed within " + properties.pendingTimeoutSeconds() + " s; avoiding this target for a while");
                    }
                }
            }
            if (entry.getValue().isEmpty()) {
                it.remove();
            }
        }
    }

    private void requestOverdueReports() {
        long limit = 2 * properties.fullReportInterval().toNanos();
        long now = cluster.now();
        for (WorkerInfo worker : cluster.workers().values()) {
            boolean overdue = worker.hasReported ? now - worker.lastReportAt > limit : worker.session == null;
            if (!worker.isDead() && overdue) {
                worker.fullReportRequested = true;
                log.info("[monitor] asking {} for a full report", worker.workerId);
            }
        }
    }
}
