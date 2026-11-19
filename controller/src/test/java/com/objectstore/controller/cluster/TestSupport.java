package com.objectstore.controller.cluster;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.ChangeType;
import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.dto.RegisterRequest;
import com.objectstore.common.dto.ReportEntry;
import com.objectstore.common.dto.ReportSegment;
import com.objectstore.common.dto.WorkerStats;
import com.objectstore.controller.config.ControllerProperties;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Builds a controller's in-memory core without Spring, for unit tests, and plays the worker side
 * of the protocol: each fake worker numbers its changes and heartbeats like a real one.
 */
public final class TestSupport {

    public static final String OBJECT = "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a";
    public static final String SUM = "a".repeat(64);
    public static final WorkerStats STATS = new WorkerStats(0, 0, 1L << 40, 0);

    public final ControllerProperties properties;
    public final ClusterState cluster;
    public final ApplyQueue queue;
    public final NamespaceLock namespaceLock = new NamespaceLock();
    public final NamespaceTasks namespaceTasks;
    public final ObjectReconciler reconciler;
    public final Writer writer;
    public final ChangeApplier applier;
    public final Receipts receipts;
    public final Registry registry;
    /** The namespace the reconcile helper reads (tests put rows here). */
    public final Map<String, List<com.objectstore.controller.domain.Chunk>> namespace = new java.util.concurrent.ConcurrentHashMap<>();
    public final com.objectstore.controller.objects.PlacementPolicy placement;
    public final com.objectstore.controller.replication.ReplicationPlanner planner;
    private final Map<String, Fake> fakes = new HashMap<>();

    /** The worker side of one fake worker process. */
    public static final class Fake {
        public String boot;
        public long attempt;
        public long lastChange;            // the last change number this process gave out
        public long hbSeq = -1;
    }

    public TestSupport() {
        this(properties(0.2, 0.4, 0));
    }

    public TestSupport(ControllerProperties properties) {
        this.properties = properties;
        this.cluster = new ClusterState(properties, Clock.systemUTC());
        this.queue = new ApplyQueue(properties.applyQueueCapacity());
        this.namespaceTasks = new NamespaceTasks(cluster);
        this.reconciler = new ObjectReconciler(namespaceLock, queue, namespaceTasks,
                objectId -> namespace.getOrDefault(objectId, List.of()));
        this.writer = new Writer(cluster, queue, properties, reconciler);   // not started: sync() applies the queue
        this.applier = new ChangeApplier(cluster, properties);
        this.receipts = new Receipts(cluster, applier, queue);
        this.registry = new Registry(cluster, properties, receipts, queue, writer);
        this.placement = new com.objectstore.controller.objects.PlacementPolicy(cluster, properties);
        this.planner = new com.objectstore.controller.replication.ReplicationPlanner(cluster, placement, properties);
        cluster.setClusterId("cluster-1");
        cluster.setSafeMode(false);
    }

    public static ControllerProperties properties(double staleAfter, double deadAfter, double orphanGcDelay) {
        return new ControllerProperties(1_048_576, 2, Path.of("/tmp/unused.db"), staleAfter, deadAfter, 0.5,
                0.1, 0.3, 2, 3, 2, 30, 1000, 10, 0.999, 0.1, 5, orphanGcDelay, 0.5, 16 * 1024 * 1024, 30, 2, 10, true,
                5, 10_000, 5, 2);
    }

    public static String chunk(int index) {
        return "%s-%05d".formatted(OBJECT, index);
    }

    /** Adds a chunk to the namespace mirror. */
    public void knowChunk(String chunkId) {
        cluster.locked(() -> cluster.putChunk(chunkId, new ClusterState.ChunkInfo(OBJECT, 0, 10, SUM)));
    }

    public Fake fake(String workerId) {
        return fakes.computeIfAbsent(workerId, id -> new Fake());
    }

    /** A new process registers (new boot id, first attempt, no changes yet). */
    public String register(String workerId) {
        Fake fake = fake(workerId);
        fake.boot = "boot-" + workerId + "-" + System.nanoTime();
        fake.attempt = 0;
        fake.lastChange = 0;
        fake.hbSeq = -1;
        return reRegister(workerId);
    }

    /** The same process registers again (the next attempt; the baseline is its last change). */
    public String reRegister(String workerId) {
        Fake fake = fake(workerId);
        fake.attempt++;
        registry.register(new RegisterRequest(workerId, "http://" + workerId + ":8001", fake.boot, "cluster-1",
                fake.attempt, fake.lastChange, fake.hbSeq, InternalApi.nowSeconds(), STATS));
        return fake.boot;
    }

    /** Registers the worker and has it send a full report holding {@code chunks}. */
    public String join(String workerId, String... chunks) {
        String boot = register(workerId);
        fullReport(workerId, boot, chunks);
        return boot;
    }

    /** A complete full report in one segment, baseline = the worker's last change. */
    public HeartbeatResponse fullReport(String workerId, String boot, String... chunks) {
        return report(workerId, Arrays.stream(chunks).map(c -> new ReportEntry(c, 10, SUM, false)).toList());
    }

    public HeartbeatResponse report(String workerId, List<ReportEntry> entries) {
        Fake fake = fake(workerId);
        return send(workerId, List.of(), new ReportSegment(UUID.randomUUID().toString(), 0, true, fake.lastChange, entries));
    }

    /** The worker's next changes, numbered in order, in one heartbeat. */
    public HeartbeatResponse ibr(String workerId, String boot, List<String> received, List<String> deleted, List<String> bad) {
        List<Change> changes = new ArrayList<>();
        received.forEach(c -> changes.add(next(workerId, ChangeType.RECEIVED, c, 10L, SUM, null)));
        deleted.forEach(c -> changes.add(next(workerId, ChangeType.DELETED, c, null, null, null)));
        bad.forEach(c -> changes.add(next(workerId, ChangeType.BAD, c, null, null, null)));
        return send(workerId, changes, null);
    }

    /** Numbers a new change of this worker (without sending it). */
    public Change next(String workerId, ChangeType type, String chunkId, Long size, String checksum, String cmdId) {
        Fake fake = fake(workerId);
        return new Change(++fake.lastChange, type, chunkId, size, checksum, cmdId, null, null);
    }

    /** A heartbeat with the next sequence number, these changes and this segment; then the queue is applied. */
    public HeartbeatResponse send(String workerId, List<Change> changes, ReportSegment segment) {
        Fake fake = fake(workerId);
        HeartbeatResponse response = registry.heartbeat(new HeartbeatRequest(workerId, fake.boot, ++fake.hbSeq,
                InternalApi.nowSeconds(), STATS, changes, segment));
        sync();
        return response;
    }

    /** Applies everything queued (the writer thread is not running in these tests). */
    public void sync() {
        if (writer.isThreadRunning()) {
            try {
                writer.barrier(java.time.Duration.ofSeconds(30));
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        } else {
            writer.drainInline();
        }
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

    public HeartbeatResponse heartbeat(String workerId, String boot, long hbSeq) {
        Fake fake = fake(workerId);
        fake.hbSeq = Math.max(fake.hbSeq, hbSeq);
        HeartbeatResponse response = registry.heartbeat(new HeartbeatRequest(workerId, boot, hbSeq, InternalApi.nowSeconds(), STATS));
        sync();
        return response;
    }
}
