package com.objectstore.controller.cluster;

import com.objectstore.common.web.ApiException;
import com.objectstore.common.web.ErrorCodes;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.ReplicaState;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.config.ControllerProperties;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/**
 * Safe mode (HDFS): after a start the controller knows the namespace but not where any copy is.
 * Until enough chunks have been reported it accepts no writes and issues no commands, so that
 * nothing is deleted or re-replicated on the basis of incomplete information.
 *
 * <p>It leaves safe mode once at least SAFE_MODE_THRESHOLD of the chunks have a reported OK copy
 * and that has held for SAFE_MODE_EXTENSION, or unconditionally after SAFE_MODE_TIMEOUT (a chunk
 * whose holders are gone must not block the cluster for ever).
 */
@Component
public class SafeMode {

    private static final Logger log = LoggerFactory.getLogger(SafeMode.class);

    private final ClusterState cluster;
    private final ControllerProperties properties;

    public SafeMode(ClusterState cluster, ControllerProperties properties) {
        this.cluster = cluster;
        this.properties = properties;
    }

    /** One evaluation; runs under the lock from the monitor. @return true if safe mode is still on */
    public boolean evaluate() {
        if (!cluster.isSafeMode()) {
            return false;
        }
        long now = cluster.now();
        int total = cluster.chunks().size();
        int reported = reportedChunks();
        boolean thresholdMet = total == 0 || (double) reported / total >= properties.safeModeThreshold();
        if (thresholdMet) {
            if (cluster.safeModeThresholdReachedAt() == 0) {
                cluster.setSafeModeThresholdReachedAt(now);
            }
        } else {
            cluster.setSafeModeThresholdReachedAt(0);
        }
        boolean extensionOver = cluster.safeModeThresholdReachedAt() != 0
                && now - cluster.safeModeThresholdReachedAt() >= properties.safeModeExtension().toNanos();
        boolean timedOut = now - cluster.startedAt() >= properties.safeModeTimeout().toNanos();
        if (extensionOver || timedOut) {
            cluster.setSafeMode(false);
            log.info("[safemode] OFF (reported {}/{}, {}s after start{})", reported, total,
                    String.format("%.1f", cluster.secondsSince(cluster.startedAt())),
                    timedOut && !extensionOver ? ", by time-out" : "");
            return false;
        }
        return true;
    }

    /** Chunks with at least one OK copy on a worker whose full report has been processed. */
    public int reportedChunks() {
        int reported = 0;
        for (String chunkId : cluster.chunks().keySet()) {
            for (Map.Entry<String, Replica> entry : cluster.replicasOf(chunkId).entrySet()) {
                WorkerInfo worker = cluster.worker(entry.getKey());
                if (entry.getValue().state == ReplicaState.OK && worker != null && worker.hasReported && !worker.isDead()) {
                    reported++;
                    break;
                }
            }
        }
        return reported;
    }

    /** Writes are refused while safe mode is on. */
    public void requireWritable() {
        if (cluster.isSafeMode()) {
            throw new ApiException(HttpStatus.SERVICE_UNAVAILABLE, ErrorCodes.SAFE_MODE,
                    "the controller is in safe mode while workers report their chunks; retry shortly")
                    .withHeader("Retry-After", "5");
        }
    }
}
