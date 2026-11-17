package com.objectstore.controller.cluster;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.BlockReportRequest;
import com.objectstore.common.dto.ChunkInfo;
import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.IncrementalReportRequest;
import com.objectstore.common.dto.RegisterRequest;
import com.objectstore.common.dto.WorkerStats;
import com.objectstore.controller.config.ControllerProperties;
import java.nio.file.Path;
import java.time.Clock;
import java.util.List;

/** Builds a controller's in-memory core without Spring, for unit tests. */
public final class TestSupport {

    public static final String OBJECT = "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a";
    public static final String SUM = "a".repeat(64);
    public static final WorkerStats STATS = new WorkerStats(0, 0, 1L << 40, 0);

    public final ControllerProperties properties;
    public final ClusterState cluster;
    public final Registry registry;
    public final Reports reports;
    public final com.objectstore.controller.objects.PlacementPolicy placement;
    public final com.objectstore.controller.replication.ReplicationPlanner planner;
    private final long[] seq = new long[16];

    public TestSupport() {
        this(properties(0.2, 0.4, 0));
    }

    public TestSupport(ControllerProperties properties) {
        this.properties = properties;
        this.cluster = new ClusterState(properties, Clock.systemUTC());
        this.registry = new Registry(cluster, properties);
        this.reports = new Reports(cluster, registry, properties);
        this.placement = new com.objectstore.controller.objects.PlacementPolicy(cluster, properties);
        this.planner = new com.objectstore.controller.replication.ReplicationPlanner(cluster, placement, properties);
        cluster.setClusterId("cluster-1");
        cluster.setSafeMode(false);
    }

    public static ControllerProperties properties(double staleAfter, double deadAfter, double orphanGcDelay) {
        return new ControllerProperties(1_048_576, 2, Path.of("/tmp/unused.db"), staleAfter, deadAfter, 0.5,
                0.1, 0.3, 2, 3, 2, 30, 1000, 10, 0.999, 0.1, 5, orphanGcDelay, 0.5, 16 * 1024 * 1024, 30, 2, 10, true);
    }

    public static String chunk(int index) {
        return "%s-%05d".formatted(OBJECT, index);
    }

    /** Adds a chunk to the namespace mirror. */
    public void knowChunk(String chunkId) {
        cluster.locked(() -> cluster.putChunk(chunkId, new ClusterState.ChunkInfo(OBJECT, 0, 10, SUM)));
    }

    public String register(String workerId) {
        String boot = "boot-" + workerId + "-" + System.nanoTime();
        registry.register(new RegisterRequest(workerId, "http://" + workerId + ":8001", boot, "cluster-1",
                -1, -1, InternalApi.nowSeconds(), STATS));
        return boot;
    }

    /** Registers the worker and has it send a full report holding {@code chunks}. */
    public String join(String workerId, String... chunks) {
        String boot = register(workerId);
        fullReport(workerId, boot, chunks);
        return boot;
    }

    public void fullReport(String workerId, String boot, String... chunks) {
        List<ChunkInfo> infos = java.util.Arrays.stream(chunks).map(c -> new ChunkInfo(c, 10, SUM)).toList();
        reports.fullReport(new BlockReportRequest(workerId, boot, nextSeq(workerId), InternalApi.nowSeconds(), infos));
    }

    public void ibr(String workerId, String boot, List<String> received, List<String> deleted, List<String> bad) {
        reports.ibr(new IncrementalReportRequest(workerId, boot, nextSeq(workerId), InternalApi.nowSeconds(),
                received.stream().map(c -> new ChunkInfo(c, 10, SUM)).toList(), deleted, bad));
    }

    /** One planning pass, as the monitor runs it. */
    public void plan() {
        cluster.locked(() -> {
            planner.plan();
            assertNoViolations();
        });
    }

    public void assertNoViolations() {
        java.util.List<String> violations = cluster.locked(cluster::checkInvariants);
        if (!violations.isEmpty()) {
            throw new AssertionError("invariant violations: " + violations);
        }
    }

    public void heartbeat(String workerId, String boot, long hbSeq) {
        registry.heartbeat(new HeartbeatRequest(workerId, boot, hbSeq, InternalApi.nowSeconds(), STATS));
    }

    public long nextSeq(String workerId) {
        int slot = Math.floorMod(workerId.hashCode(), seq.length);
        return seq[slot]++;
    }
}
