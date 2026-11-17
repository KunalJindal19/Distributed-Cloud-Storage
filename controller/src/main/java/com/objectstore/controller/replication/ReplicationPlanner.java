package com.objectstore.controller.replication;

import com.objectstore.controller.cluster.ClusterState;
import org.springframework.stereotype.Component;

/**
 * Step 3 of the monitor: repairs under-replicated chunks, trims over-replicated ones and cleans
 * up copies that are known to be surplus or corrupt. Added in block 7; until then the monitor
 * only detects, times out and checks invariants.
 */
@Component
public class ReplicationPlanner {

    private final ClusterState cluster;

    public ReplicationPlanner(ClusterState cluster) {
        this.cluster = cluster;
    }

    /** Must be called under the cluster lock. */
    public void plan() {
        // Block 7.
    }
}
