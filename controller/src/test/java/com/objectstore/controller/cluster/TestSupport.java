package com.objectstore.controller.cluster;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.Command;
import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.dto.RegisterRequest;
import com.objectstore.common.dto.RegisterResponse;
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
 * of the protocol: each fake worker numbers its changes and heartbeats like a real one, adopts the
 * fence its registration (or a heartbeat reply) hands it, and lists its copies for that resync.
 */
public final class TestSupport {

    public static final String OBJECT = "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a";
    public static final String SUM = "a".repeat(64);
    public static final WorkerStats STATS = new WorkerStats(0, 0, 1L << 40, 0);

    public final ControllerProperties properties;
    public final OpVersions versions = new OpVersions(1);
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
        public long attempt;               // registration attempt
        public long lastChange;            // the last change number this process gave out
        public long hbSeq = -1;
        public long fence;                 // the fence (t0) this process adopted
        public int reportAttempt;          // the resync attempt it lists for
    }

    public TestSupport() {
        this(properties(0.2, 0.4, 0));
    }

    public TestSupport(ControllerProperties properties) {
        this.properties = properties;
        this.cluster = new ClusterState(properties, Clock.systemUTC(), versions);
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
        return properties(2, 2, 2, staleAfter, deadAfter, orphanGcDelay, 2000);
    }

    /** Properties with a replication factor, streams per source, first resend, liveness and visit budget. */
    public static ControllerProperties properties(int rf, int maxStreams, double opResendInitial, double staleAfter,
                                                  double deadAfter, double orphanGcDelay, int visitBudget) {
        return new ControllerProperties(1_048_576, rf, Path.of("/tmp/unused.db"), staleAfter, deadAfter, 0.5,
                0.1, 0.3, 3, maxStreams, 1000, 10, 0.999, 0.1, 5, orphanGcDelay, 0.5,
                16 * 1024 * 1024, 2, 10, true, 5, 10_000, 5, 2, 2000, visitBudget, 1000, 1000,
                opResendInitial, Math.max(opResendInitial, 240), 5, 3600, 0.1, 1, 60, 3600, 60);
    }

    /** A cluster with this replication factor and streams per source (other settings as usual). */
    public static TestSupport withRf(int rf, int maxStreams) {
        return new TestSupport(properties(rf, maxStreams, 2, 0.2, 0.4, 0, 2000));
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

    // ---- registration and resync listings --------------------------------------------------------

    /** A new process registers (new boot id, first attempt, no changes yet) and adopts its fence. */
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
        RegisterResponse response = registry.register(new RegisterRequest(workerId, "http://" + workerId + ":8001",
                fake.boot, "cluster-1", fake.attempt, fake.lastChange, fake.hbSeq, InternalApi.nowSeconds(), STATS));
        fake.fence = response.floor();
        fake.reportAttempt = response.attempt();
        return fake.boot;
    }

    /** Registers the worker and has it list {@code chunks} (version 0, as after start-up recovery). */
    public String join(String workerId, String... chunks) {
        String boot = register(workerId);
        fullReport(workerId, boot, chunks);
        return boot;
    }

    /** A complete listing in one segment for the fake's current fence and attempt, baseline = its last change. */
    public HeartbeatResponse fullReport(String workerId, String boot, String... chunks) {
        return report(workerId, Arrays.stream(chunks).map(c -> entry(c, 0)).toList());
    }

    public static ReportEntry entry(String chunkId, long version) {
        return new ReportEntry(chunkId, 10, SUM, version);
    }

    public HeartbeatResponse report(String workerId, List<ReportEntry> entries) {
        return send(workerId, List.of(), segment(workerId, entries));
    }

    /** One-segment listing for the fake's current fence and attempt. */
    public ReportSegment segment(String workerId, List<ReportEntry> entries) {
        Fake fake = fake(workerId);
        return new ReportSegment(UUID.randomUUID().toString(), fake.fence, fake.reportAttempt, 0, true, fake.lastChange,
                entries);
    }

    // ---- changes ----------------------------------------------------------------------------------

    /** Numbers a new change of this worker (without sending it). */
    public Change received(String workerId, String chunkId, long version) {
        return Change.received(++fake(workerId).lastChange, chunkId, 10, SUM, version, null);
    }

    public Change receivedVerify(String workerId, String chunkId, long version, String cmdId) {
        return Change.received(++fake(workerId).lastChange, chunkId, 10, SUM, version, cmdId);
    }

    public Change receivedWithChecksum(String workerId, String chunkId, String checksum, long version) {
        return Change.received(++fake(workerId).lastChange, chunkId, 10, checksum, version, null);
    }

    public Change deleted(String workerId, String chunkId, long version, String detail) {
        return Change.deleted(++fake(workerId).lastChange, chunkId, version, detail);
    }

    public Change cmdFailed(String workerId, String chunkId, long version, String reason) {
        return Change.cmdFailed(++fake(workerId).lastChange, chunkId, version, reason, null, null, null);
    }

    /** An odd version above the fake's fence: a local observation. */
    public long local(String workerId) {
        return fake(workerId).fence | 1;
    }

    /** These changes in one heartbeat. */
    public HeartbeatResponse ibr(String workerId, Change... changes) {
        return send(workerId, List.of(changes), null);
    }

    /** A heartbeat with the next sequence number, these changes and this segment; then the queue is applied. */
    public HeartbeatResponse send(String workerId, List<Change> changes, ReportSegment segment) {
        return send(workerId, changes, segment, null);
    }

    public HeartbeatResponse send(String workerId, List<Change> changes, ReportSegment segment, String resyncWanted) {
        Fake fake = fake(workerId);
        HeartbeatResponse response = registry.heartbeat(new HeartbeatRequest(workerId, fake.boot, ++fake.hbSeq,
                InternalApi.nowSeconds(), STATS, changes, segment, resyncWanted));
        adopt(fake, response);
        sync();
        return response;
    }

    /** What a real worker does with a reply's resync: a new fence or a newer attempt replaces the current one. */
    private static void adopt(Fake fake, HeartbeatResponse response) {
        if (response.resync() != null && (response.resync().t0() > fake.fence
                || (response.resync().t0() == fake.fence && response.resync().attempt() > fake.reportAttempt))) {
            fake.fence = response.resync().t0();
            fake.reportAttempt = response.resync().attempt();
        }
    }

    /** A plain heartbeat; returns the commands handed out. */
    public List<Command> handOut(String workerId) {
        HeartbeatResponse response = send(workerId, List.of(), null);
        return response.commands() == null ? List.of() : response.commands();
    }

    /**
     * What a worker does with a handed-out command, reported as the change it would log: a DELETE
     * logs {@code DELETED(v)} on the holder, a REPLICATE makes the target log {@code RECEIVED(v)}, a
     * VERIFY logs {@code RECEIVED(current version, cmd_id)}.
     */
    public void perform(String holder, Command command) {
        switch (command.type()) {
            case InternalApi.COMMAND_DELETE -> ibr(holder, deleted(holder, command.chunkId(), command.version(),
                    InternalApi.REASON_CORRUPT.equals(command.reason()) ? "corrupt" : "deleted"));
            case InternalApi.COMMAND_REPLICATE -> {
                String target = command.targetUrl().replaceFirst("^http://", "").replaceFirst(":\\d+$", "");
                ibr(target, received(target, command.chunkId(), command.version()));
            }
            case InternalApi.COMMAND_VERIFY -> {
                ClusterState.Replica replica = cluster.read(() -> cluster.replica(command.chunkId(), holder));
                ibr(holder, receivedVerify(holder, command.chunkId(), replica.version, command.cmdId()));
            }
            default -> throw new IllegalArgumentException(command.type());
        }
    }

    /** Hands out and performs every command of these workers until none is left (at most 20 rounds). */
    public void settle(String... workerIds) {
        for (int round = 0; round < 20; round++) {
            boolean any = false;
            plan();
            for (String workerId : workerIds) {
                for (Command command : handOut(workerId)) {
                    perform(workerId, command);
                    any = true;
                }
            }
            if (!any) {
                return;
            }
        }
        throw new AssertionError("the cluster did not settle");
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
        List<String> violations = cluster.locked(cluster::checkInvariants);
        if (!violations.isEmpty()) {
            throw new AssertionError("invariant violations: " + violations);
        }
    }

    public HeartbeatResponse heartbeat(String workerId, String boot, long hbSeq) {
        Fake fake = fake(workerId);
        fake.hbSeq = Math.max(fake.hbSeq, hbSeq);
        HeartbeatResponse response = registry.heartbeat(new HeartbeatRequest(workerId, boot, hbSeq, InternalApi.nowSeconds(), STATS));
        adopt(fake, response);
        sync();
        return response;
    }

    /** The pending op on a pair, read under the lock. */
    public PendingOps.Op op(String chunkId, String workerId) {
        return cluster.read(() -> cluster.ops().get(chunkId, workerId));
    }

    /** The holders of a chunk, read under the lock. */
    public List<String> holders(String chunkId) {
        return cluster.read(() -> new ArrayList<>(cluster.replicasOf(chunkId).keySet()));
    }
}
