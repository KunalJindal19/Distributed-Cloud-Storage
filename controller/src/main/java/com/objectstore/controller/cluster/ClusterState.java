package com.objectstore.controller.cluster;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.common.dto.WorkerStats;
import com.objectstore.controller.api.dto.ClusterStatusResponse.RepairEvent;
import com.objectstore.controller.cluster.PendingOps.Kind;
import com.objectstore.controller.cluster.PendingOps.Op;
import com.objectstore.controller.cluster.PendingOps.Reason;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.util.Timestamps;
import java.time.Clock;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * The controller's in-memory view of the cluster: which workers exist and how alive they are,
 * where every copy of every chunk is, the pending operations ({@link PendingOps}) and the commands
 * each worker still has to pick up. Like the HDFS NameNode's block map it is <em>not</em> persisted:
 * after a restart it is rebuilt from the workers' resyncs.
 *
 * <p><b>The concurrency rule.</b> One fair read-write lock guards all of it: {@link #read} for
 * readers and {@link #write} for brief writers (the receipt step, the health loop, the planner) and
 * the single {@code writer} thread, which applies every queued task. No network I/O, no SQLite access
 * and no sleeping happens while it is held.
 *
 * <p><b>The main design principle (v5).</b> The controller decides the desired state; workers execute
 * versioned instructions and report physical facts. Every replica carries the version the worker
 * reported and a generation {@code gen} that changes whenever the controller learns of a new or
 * changed copy; every worker has a {@code verifyEpoch}, bumped at every resync start and at DEAD,
 * that voids its VERIFY credit at once.
 *
 * <p>Times used for liveness and time-outs are monotonic ({@link System#nanoTime()}).
 */
@Component
public class ClusterState {

    private static final Logger log = LoggerFactory.getLogger(ClusterState.class);
    private static final int EVENT_CAPACITY = 200;
    private static final int BOOT_FLIP_WINDOW_SECONDS = 60;
    private static final int BOOT_FLIPS_FOR_DUPLICATE = 3;
    private static final int MAX_COMMANDS_PER_HEARTBEAT = 50;

    public enum WorkerState { ALIVE, STALE, DEAD }

    /** One worker as the controller knows it (HDFS DatanodeDescriptor). */
    public static final class WorkerInfo {
        public final String workerId;
        public String url;
        public String bootId;
        public WorkerState state = WorkerState.ALIVE;
        public volatile long lastSeen;              // monotonic nanos of the last accepted heartbeat
        public long lastHbSeq = -1;
        public long epoch;                          // +1 on every accepted registration and on DEAD
        public long received;                       // changes up to this number were accepted (acknowledged)
        public long applied;                        // changes up to this number were applied to the block map
        public long lastAttempt;                    // the last accepted registration attempt of bootId
        public ReportSession session;               // the resync listing being applied (writer side), or null
        // The listing as the receipt step sees it: which segments were accepted (and acknowledged).
        public String rxReportId;
        public long rxSession;
        public long rxBaseline;
        public int rxLastSegment = -1;
        public boolean rxDone;
        public boolean deadQueued;                  // a MarkDead task is queued
        public boolean purging;                     // DEAD, and its replicas are still being removed
        public boolean hasReported;                 // a resync completed since (re)registration
        public long lastReportAt;                   // monotonic nanos of the last completed resync
        public String registeredAt = "";
        public WorkerStats stats;
        public final CommandQueue commands = new CommandQueue();
        public final Deque<Long> bootFlips = new ArrayDeque<>();
        public volatile long lastDataError;         // monotonic nanos of the last failed data-path call (no lock)
        // Fenced resync (v5 §5.1).
        public boolean resyncing;
        public long fenceFloor;                     // t0 of the current or last resync
        public int attempt;                         // the current report attempt of that fence
        public boolean segmentSeen;                 // segment 0 of the current attempt has arrived
        public long lastCompletedFloor = -1;
        public int lastCompletedAttempt = -1;
        public long verifyEpoch;                    // +1 at every resync start and at DEAD
        public long lastSegmentAt;                  // monotonic nanos: resync start, then each accepted segment
        public boolean periodicResync;              // the current resync counts against RESYNC_MAX_CONCURRENT
        public String resyncRequested;              // a periodic or GC resync waiting for its turn
        public long nextPeriodicAt;                 // monotonic nanos: when the next periodic resync is due

        WorkerInfo(String workerId) {
            this.workerId = workerId;
        }

        public boolean isDead() {
            return state == WorkerState.DEAD;
        }

        /** A listing is being received (its last segment has not been accepted yet). */
        public boolean reportInProgress() {
            return rxReportId != null && !rxDone;
        }

        /** Forgets the receipt-side listing state (new attempt, re-registration, DEAD). */
        void resetReceipt() {
            rxReportId = null;
            rxSession = 0;
            rxBaseline = 0;
            rxLastSegment = -1;
            rxDone = false;
        }
    }

    /**
     * One copy of one chunk on one worker, as the worker reported it: at {@code version}, with the
     * generation {@code gen} the controller gave it when it learnt of it. VERIFY credit
     * ({@code verifiedEpoch}, {@code verifiedAt}) and the outstanding VERIFY request belong to one
     * generation: a new generation clears both (v5 §6.4).
     */
    public static final class Replica {
        public long version;
        public long gen;
        public long mark;                           // the report session that last listed it; 0 = none
        public long addedAt;                        // monotonic nanos
        public long verifiedEpoch = -1;             // credit: the worker's verifyEpoch it was earned in
        public long verifiedAt;                     // credit: when that VERIFY was handed out (monotonic nanos)
        public String verifyCmdId;                  // the outstanding VERIFY request, or null
        public long verifyGen;
        public long verifyEpoch;
        public long verifySentAt;                   // 0 until handed out
        public long verifyRequestedAt;
    }

    /**
     * A resync listing being applied for one worker (the writer's side). {@code number} is unique
     * across all sessions; a segment sets {@code replica.mark = number} on every copy it lists, and
     * completion removes the worker's copies whose mark differs.
     */
    public static final class ReportSession {
        public final long baseline;
        public final long number;
        public final long floor;
        public final int attempt;
        public long listed;
        public final List<String> garbage = new ArrayList<>();
        public final List<String> orphans = new ArrayList<>();
        public boolean closed;

        ReportSession(long number, long baseline, long floor, int attempt) {
            this.baseline = baseline;
            this.number = number;
            this.floor = floor;
            this.attempt = attempt;
        }
    }

    /**
     * Proof that worker {@code W}, in {@code epoch}, holds bytes of a chunk whose upload has not
     * committed yet, at {@code version}. The commit turns it into a replica; {@code seenInSession}
     * becomes its mark.
     */
    public static final class Receipt {
        public final long epoch;
        public long size;
        public String checksum;
        public long version;
        public long seenInSession;

        Receipt(long epoch, long size, String checksum, long version, long seenInSession) {
            this.epoch = epoch;
            this.size = size;
            this.checksum = checksum;
            this.version = version;
            this.seenInSession = seenInSession;
        }
    }

    /** Mirror of one row of the {@code chunks} table. */
    public record ChunkInfo(String objectId, int idx, int size, String checksum) {
    }

    private record FailureKey(String chunkId, String workerId) {
    }

    /** Where resend deadlines go (the resend timer); without one (unit tests) nothing expires by itself. */
    public interface DeadlineScheduler {
        void opDeadline(String chunkId, String workerId, long version, long atNanos);

        DeadlineScheduler NONE = (chunkId, workerId, version, atNanos) -> { };
    }

    private final ControllerProperties properties;
    private final Clock clock;
    private final OpVersions versions;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Map<String, Long> maxHoldNanos = new ConcurrentHashMap<>();
    private long writeStartedAt;
    private static volatile ReentrantReadWriteLock latestLock;
    private final long startedAt = System.nanoTime();

    private volatile String clusterId = "";
    private final Map<String, WorkerInfo> workers = new ConcurrentHashMap<>();
    private final PendingOps ops = new PendingOps(this);
    private final ReplicationIndex index;
    private volatile DeadlineScheduler deadlines = DeadlineScheduler.NONE;
    private final Map<String, ChunkInfo> chunks = new HashMap<>();
    private final Map<String, Map<String, Replica>> blocks = new HashMap<>();
    private final Map<String, Set<String>> workerBlocks = new HashMap<>();
    private final Set<String> pendingUploads = new HashSet<>();
    /** Objects whose chunks are certainly garbage, and the DELETE reason for them. */
    private final Map<String, Reason> garbageObjects = new HashMap<>();
    private final Map<String, Map<String, Receipt>> uploadReceipts = new HashMap<>();
    private long sessionNumbers;
    private long generations;
    private int verifyOutstanding;
    private final Map<FailureKey, Long> recentFailures = new HashMap<>();
    private final Deque<RepairEvent> events = new ArrayDeque<>(EVENT_CAPACITY);
    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private volatile boolean safeMode = true;
    private volatile int blockedLastTick;
    private long safeModeThresholdReachedAt;

    @Autowired
    public ClusterState(ControllerProperties properties, Clock clock, OpVersions versions) {
        this.properties = properties;
        this.clock = clock;
        this.versions = versions;
        this.index = new ReplicationIndex(this, properties.replicationFactor());
        latestLock = lock;
    }

    /** For tests: versions of term 1. */
    public ClusterState(ControllerProperties properties, Clock clock) {
        this(properties, clock, new OpVersions(1));
    }

    public void setDeadlineScheduler(DeadlineScheduler scheduler) {
        this.deadlines = scheduler;
    }

    DeadlineScheduler deadlines() {
        return deadlines;
    }

    public OpVersions versions() {
        return versions;
    }

    public ControllerProperties properties() {
        return properties;
    }

    public PendingOps ops() {
        return ops;
    }

    // ---- the lock -------------------------------------------------------------------------

    /** Runs {@code action} under the read lock. It must not modify any state. */
    public <T> T read(Supplier<T> action) {
        lock.readLock().lock();
        try {
            return action.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void read(Runnable action) {
        read(() -> {
            action.run();
            return null;
        });
    }

    /** Runs {@code action} as one brief block under the write lock. */
    public <T> T write(Supplier<T> action) {
        lockWrite();
        try {
            return action.get();
        } finally {
            unlockWrite();
        }
    }

    public void write(Runnable action) {
        write(() -> {
            action.run();
            return null;
        });
    }

    /** The same as {@link #write(Supplier)}; kept for tests. */
    public <T> T locked(Supplier<T> action) {
        return write(action);
    }

    public void locked(Runnable action) {
        write(action);
    }

    /** Takes the write lock (re-entrant); the outermost hold is timed. */
    public void lockWrite() {
        lock.writeLock().lock();
        if (lock.getWriteHoldCount() == 1) {
            writeStartedAt = System.nanoTime();
        }
    }

    /** Releases the write lock; the outermost release runs {@link #flushDirty()} and records the hold. */
    public void unlockWrite() {
        try {
            if (lock.getWriteHoldCount() == 1) {
                flushDirty();
                long held = System.nanoTime() - writeStartedAt;
                maxHoldNanos.merge(holderName(), held, Math::max);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Brings the priority index up to date: every chunk marked dirty since the last flush is recomputed. */
    public void flushDirty() {
        index.flush();
    }

    /** The one way to tell the index that something about a chunk changed. */
    public void markDirty(String chunkId) {
        index.markDirty(chunkId);
    }

    private static String holderName() {
        String name = Thread.currentThread().getName();
        return name == null || name.isEmpty() ? (Thread.currentThread().isVirtual() ? "http" : "unnamed") : name;
    }

    /** The longest continuous write hold per holder since the last call, in ms; resets. */
    public Map<String, Double> takeLockMaxHoldMs() {
        Map<String, Double> snapshot = new java.util.TreeMap<>();
        for (String holder : List.copyOf(maxHoldNanos.keySet())) {
            Long nanos = maxHoldNanos.remove(holder);
            if (nanos != null) {
                snapshot.put(holder, Math.round(nanos / 10_000.0) / 100.0);
            }
        }
        return snapshot;
    }

    /** The longest write hold of one holder since the last call, in ms, without resetting the others. */
    public double peekLockMaxHoldMs(String holder) {
        Long nanos = maxHoldNanos.get(holder);
        return nanos == null ? 0 : nanos / 1e6;
    }

    public boolean isLocked() {
        return lock.isWriteLockedByCurrentThread() || lock.getReadHoldCount() > 0;
    }

    public boolean isWriteLocked() {
        return lock.isWriteLockedByCurrentThread();
    }

    /** True if the current thread holds the (latest) cluster lock: no SQLite access is allowed then. */
    public static boolean lockHeldByCurrentThread() {
        ReentrantReadWriteLock current = latestLock;
        return current != null && (current.isWriteLockedByCurrentThread() || current.getReadHoldCount() > 0);
    }

    // ---- time -----------------------------------------------------------------------------

    public long now() {
        return System.nanoTime();
    }

    public double secondsSince(long nanos) {
        return (System.nanoTime() - nanos) / 1e9;
    }

    public long startedAt() {
        return startedAt;
    }

    // ---- identity and safe mode -----------------------------------------------------------

    public String clusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public int blockedLastTick() {
        return blockedLastTick;
    }

    public void setBlockedLastTick(int blocked) {
        this.blockedLastTick = blocked;
    }

    public boolean isSafeMode() {
        return safeMode;
    }

    public void setSafeMode(boolean on) {
        this.safeMode = on;
    }

    public long safeModeThresholdReachedAt() {
        return safeModeThresholdReachedAt;
    }

    public void setSafeModeThresholdReachedAt(long nanos) {
        this.safeModeThresholdReachedAt = nanos;
    }

    // ---- workers --------------------------------------------------------------------------

    public Map<String, WorkerInfo> workers() {
        return workers;
    }

    public WorkerInfo worker(String workerId) {
        return workerId == null ? null : workers.get(workerId);
    }

    public WorkerInfo addWorker(String workerId, String url, String bootId) {
        WorkerInfo info = new WorkerInfo(workerId);
        info.url = url;
        info.bootId = bootId;
        info.registeredAt = Timestamps.format(clock.instant());
        workers.put(workerId, info);
        workerBlocks.computeIfAbsent(workerId, id -> new HashSet<>());
        return info;
    }

    public List<WorkerInfo> workersSorted() {
        List<WorkerInfo> sorted = new ArrayList<>(workers.values());
        sorted.sort(Comparator.comparing(w -> w.workerId));
        return sorted;
    }

    /** Three or more boot id changes inside one minute look like two processes sharing one WORKER_ID. */
    public boolean recordBootFlip(WorkerInfo worker) {
        long now = now();
        worker.bootFlips.addLast(now);
        while (worker.bootFlips.size() > 10) {
            worker.bootFlips.removeFirst();
        }
        long inWindow = worker.bootFlips.stream()
                .filter(t -> now - t <= BOOT_FLIP_WINDOW_SECONDS * 1_000_000_000L)
                .count();
        return inWindow >= BOOT_FLIPS_FOR_DUPLICATE;
    }

    /** A worker may be handed DELETE and VERIFY commands: ALIVE, reported, not resyncing, with room. */
    boolean canTakeCommands(WorkerInfo worker) {
        return worker.state == WorkerState.ALIVE && worker.hasReported && !worker.resyncing
                && worker.commands.size() < properties.maxCommandsPerWorker();
    }

    /** ALIVE, reported and not resyncing: a usable target, survivor and P-counted INSERT target. */
    public boolean isCurrent(WorkerInfo worker) {
        return worker != null && worker.state == WorkerState.ALIVE && worker.hasReported && !worker.resyncing;
    }

    // ---- fenced resync (v5 §5.2, §5.4) ---------------------------------------------------------------

    /**
     * Starts a resync of {@code worker} (O(1), under the write lock): a fresh {@code t0}, so every
     * resync has its own identity and t0 is above every op version issued before; attempt 1; all
     * VERIFY credit of the worker is void. Replies carry {@code resync {t0, 1}} until segment 0
     * arrives. Any listing in progress is closed without completing.
     */
    public long startResync(WorkerInfo worker, String why, boolean periodic) {
        long t0 = versions.next();
        worker.fenceFloor = t0;
        worker.resyncing = true;
        worker.attempt = 1;
        worker.segmentSeen = false;
        worker.verifyEpoch++;
        worker.lastSegmentAt = now();
        worker.periodicResync = periodic;
        worker.resyncRequested = null;
        closeSession(worker);
        worker.resetReceipt();
        markInsertTargetsDirty(worker.workerId);
        increment("resyncs");
        log.info("[report] {} resync started (fence {}, {})", worker.workerId, t0, why);
        return t0;
    }

    /** Authorizes the next report attempt of the current resync (v5 §5.4). */
    public void newAttempt(WorkerInfo worker, String why) {
        worker.attempt++;
        worker.segmentSeen = false;
        worker.lastSegmentAt = now();
        closeSession(worker);
        worker.resetReceipt();
        increment("resync_attempts");
        log.info("[report] {} resync fence {}: new attempt {} ({})", worker.workerId, worker.fenceFloor, worker.attempt, why);
    }

    /** P counts only INSERT ops whose target is current: their chunks need recomputing when that changes. */
    public void markInsertTargetsDirty(String workerId) {
        for (Op op : ops.opsOn(workerId)) {
            if (op.kind() == Kind.INSERT) {
                markDirty(op.chunkId);
            }
        }
    }

    /** Resyncs counting against RESYNC_MAX_CONCURRENT (periodic and GC-triggered) in progress now. */
    public int periodicResyncs() {
        int count = 0;
        for (WorkerInfo worker : workers.values()) {
            if (worker.resyncing && worker.periodicResync) {
                count++;
            }
        }
        return count;
    }

    public int resyncingWorkers() {
        int count = 0;
        for (WorkerInfo worker : workers.values()) {
            if (worker.resyncing) {
                count++;
            }
        }
        return count;
    }

    // ---- chunks (mirror of the DB) --------------------------------------------------------

    public Map<String, ChunkInfo> chunks() {
        return chunks;
    }

    public ChunkInfo chunk(String chunkId) {
        return chunks.get(chunkId);
    }

    public void putChunk(String chunkId, ChunkInfo info) {
        chunks.put(chunkId, info);
        markDirty(chunkId);
    }

    /**
     * Forgets a chunk: its mirror row and every replica. Its pending ops stay (a chunk that left the
     * namespace still has DELETEs to deliver); they are keyed by chunk id, not by the mirror.
     */
    public void removeChunk(String chunkId) {
        chunks.remove(chunkId);
        markDirty(chunkId);
        Map<String, Replica> holders = blocks.remove(chunkId);
        if (holders != null) {
            for (Map.Entry<String, Replica> holder : holders.entrySet()) {
                clearVerify(holder.getValue(), holder.getKey());
                Set<String> set = workerBlocks.get(holder.getKey());
                if (set != null) {
                    set.remove(chunkId);
                }
            }
        }
        uploadReceipts.remove(chunkId);
    }

    public Set<String> pendingUploads() {
        return pendingUploads;
    }

    /** Remembers that every chunk of this object is garbage, and which DELETE reason it gets. */
    public void addGarbage(String objectId, Reason reason) {
        garbageObjects.putIfAbsent(objectId, reason);
    }

    public boolean isGarbage(String objectId) {
        return garbageObjects.containsKey(objectId);
    }

    public Reason garbageReason(String objectId) {
        return garbageObjects.getOrDefault(objectId, Reason.OBJECT_DELETED);
    }

    public void beginUpload(String objectId) {
        pendingUploads.add(objectId);
    }

    /** The upload ended (committed or not): it is no longer pending and its receipts go. */
    public void endUpload(String objectId) {
        pendingUploads.remove(objectId);
        String prefix = objectId + "-";
        uploadReceipts.keySet().removeIf(chunkId -> chunkId.startsWith(prefix));
    }

    // ---- upload receipts --------------------------------------------------------------------

    public Receipt receipt(String chunkId, String workerId) {
        Map<String, Receipt> byWorker = uploadReceipts.get(chunkId);
        return byWorker == null ? null : byWorker.get(workerId);
    }

    /** Records (or refreshes) a receipt; only report segments set {@code seenInSession}. */
    public Receipt putReceipt(String chunkId, String workerId, long epoch, long size, String checksum, long version,
                              long seenInSession) {
        Map<String, Receipt> byWorker = uploadReceipts.computeIfAbsent(chunkId, id -> new HashMap<>());
        Receipt existing = byWorker.get(workerId);
        if (existing != null && existing.epoch == epoch) {
            existing.size = size;
            existing.checksum = checksum;
            existing.version = version;
            if (seenInSession != 0) {
                existing.seenInSession = seenInSession;
            }
            return existing;
        }
        Receipt receipt = new Receipt(epoch, size, checksum, version, seenInSession);
        byWorker.put(workerId, receipt);
        return receipt;
    }

    public Receipt removeReceipt(String chunkId, String workerId) {
        Map<String, Receipt> byWorker = uploadReceipts.get(chunkId);
        if (byWorker == null) {
            return null;
        }
        Receipt removed = byWorker.remove(workerId);
        if (byWorker.isEmpty()) {
            uploadReceipts.remove(chunkId);
        }
        return removed;
    }

    /** Forgets every receipt of a worker (it re-registered or is DEAD: its old epoch is over). */
    public int purgeReceipts(String workerId) {
        int purged = 0;
        for (Iterator<Map.Entry<String, Map<String, Receipt>>> it = uploadReceipts.entrySet().iterator(); it.hasNext(); ) {
            Map<String, Receipt> byWorker = it.next().getValue();
            if (byWorker.remove(workerId) != null) {
                purged++;
            }
            if (byWorker.isEmpty()) {
                it.remove();
            }
        }
        return purged;
    }

    public Map<String, Map<String, Receipt>> receipts() {
        return uploadReceipts;
    }

    // ---- report sessions --------------------------------------------------------------------

    public long nextSessionNumber() {
        return ++sessionNumbers;
    }

    public ReportSession openSession(WorkerInfo worker, long number, long baseline, long floor, int attempt) {
        if (worker.session != null) {
            worker.session.closed = true;
        }
        worker.session = new ReportSession(number, baseline, floor, attempt);
        return worker.session;
    }

    public void closeSession(WorkerInfo worker) {
        if (worker.session != null) {
            worker.session.closed = true;
            worker.session = null;
        }
    }

    /** Workers with a listing being received or applied. */
    public long openSessions() {
        return workers.values().stream().filter(w -> w.session != null || w.reportInProgress()).count();
    }

    public boolean anyPurging() {
        return workers.values().stream().anyMatch(w -> w.purging);
    }

    // ---- replicas (the block map) ---------------------------------------------------------

    public Map<String, Map<String, Replica>> blocks() {
        return blocks;
    }

    public Map<String, Replica> replicasOf(String chunkId) {
        return blocks.getOrDefault(chunkId, Collections.emptyMap());
    }

    public Replica replica(String chunkId, String workerId) {
        Map<String, Replica> holders = blocks.get(chunkId);
        return holders == null ? null : holders.get(workerId);
    }

    public Set<String> chunksOn(String workerId) {
        return workerBlocks.getOrDefault(workerId, Collections.emptySet());
    }

    /**
     * Records that the worker holds the chunk at {@code version} (the physical part of a RECEIVED or
     * a listed entry). A new replica, or one whose version changed, gets a new {@code gen}: its VERIFY
     * credit and outstanding request are cleared. Refused (null) for an unknown or DEAD worker or an
     * unknown chunk.
     */
    public Replica addReplica(String chunkId, String workerId, long version) {
        WorkerInfo worker = workers.get(workerId);
        if (worker == null || worker.isDead() || !chunks.containsKey(chunkId)) {
            return null;
        }
        Map<String, Replica> holders = blocks.computeIfAbsent(chunkId, id -> new LinkedHashMap<>());
        Replica replica = holders.get(workerId);
        if (replica == null) {
            replica = new Replica();
            replica.addedAt = now();
            replica.version = version;
            replica.gen = ++generations;
            holders.put(workerId, replica);
            workerBlocks.computeIfAbsent(workerId, id -> new HashSet<>()).add(chunkId);
            markDirty(chunkId);
        } else if (replica.version != version) {
            replica.version = version;
            replica.gen = ++generations;
            replica.verifiedEpoch = -1;
            replica.verifiedAt = 0;
            clearVerify(replica, workerId);
            markDirty(chunkId);
        }
        return replica;
    }

    public Replica removeReplica(String chunkId, String workerId) {
        Map<String, Replica> holders = blocks.get(chunkId);
        if (holders == null) {
            return null;
        }
        Replica removed = holders.remove(workerId);
        if (holders.isEmpty()) {
            blocks.remove(chunkId);
        }
        Set<String> set = workerBlocks.get(workerId);
        if (set != null) {
            set.remove(chunkId);
        }
        if (removed != null) {
            clearVerify(removed, workerId);
            markDirty(chunkId);
        }
        return removed;
    }

    /** Forgets every copy on a worker (it is DEAD: nothing it holds can be counted on). */
    public int removeWorkerReplicas(String workerId) {
        Set<String> held = workerBlocks.get(workerId);
        if (held == null || held.isEmpty()) {
            return 0;
        }
        int removed = 0;
        for (String chunkId : new ArrayList<>(held)) {
            if (removeReplica(chunkId, workerId) != null) {
                removed++;
            }
        }
        return removed;
    }

    /** L (v5 §6.2): holders on ALIVE or STALE workers. A pending DELETE does not remove a copy from L. */
    public List<String> live(String chunkId) {
        List<String> live = new ArrayList<>();
        for (String workerId : replicasOf(chunkId).keySet()) {
            WorkerInfo worker = workers.get(workerId);
            if (worker != null && !worker.isDead()) {
                live.add(workerId);
            }
        }
        return live;
    }

    /** Copies on ALIVE workers: the available ones (a STALE worker's copies are uncertain). */
    public List<String> available(String chunkId) {
        List<String> available = new ArrayList<>();
        for (String workerId : replicasOf(chunkId).keySet()) {
            WorkerInfo worker = workers.get(workerId);
            if (worker != null && worker.state == WorkerState.ALIVE) {
                available.add(workerId);
            }
        }
        return available;
    }

    /**
     * Where to read a chunk, best first (v5 §7): copies on ALIVE workers only, never one with a
     * pending DELETE CORRUPT; a copy with any other pending DELETE last; then no recent data-path
     * error before a recent one, fewer copies held first, then by id.
     */
    public List<String> readable(String chunkId) {
        long now = now();
        long recent = properties.targetBackoff().toNanos();
        List<String> candidates = new ArrayList<>();
        for (String workerId : available(chunkId)) {
            if (ops.deleteReason(chunkId, workerId) != Reason.CORRUPT) {
                candidates.add(workerId);
            }
        }
        candidates.sort(Comparator
                .comparingInt((String w) -> ops.hasDelete(chunkId, w) ? 1 : 0)
                .thenComparingInt(w -> now - workers.get(w).lastDataError < recent && workers.get(w).lastDataError != 0 ? 1 : 0)
                .thenComparingInt(w -> chunksOn(w).size())
                .thenComparing(w -> w));
        return candidates;
    }

    /**
     * {@link #readable}, then the ALIVE targets of UPLOAD ops (written and fsynced, but their
     * RECEIVED has not been applied yet), last.
     */
    public List<String> readCandidates(String chunkId) {
        List<String> candidates = readable(chunkId);
        for (Op op : ops.opsFor(chunkId)) {
            WorkerInfo worker = workers.get(op.workerId);
            if (op.reason() == Reason.UPLOAD && worker != null && worker.state == WorkerState.ALIVE
                    && !candidates.contains(op.workerId)) {
                candidates.add(op.workerId);
            }
        }
        return candidates;
    }

    /** The state of a copy as the public API shows it: OK, or the reason of its pending DELETE. */
    public String replicaStatus(String chunkId, String workerId) {
        Reason reason = ops.deleteReason(chunkId, workerId);
        if (reason == Reason.CORRUPT) {
            return "CORRUPT";
        }
        return reason == Reason.EXCESS ? "EXCESS" : "OK";
    }

    // ---- VERIFY credit and the EXCESS gate (v5 §6.4) -------------------------------------------------

    /** Fresh credit: earned in the worker's current verify epoch, from a VERIFY sent recently enough. */
    public boolean freshForDelete(Replica replica, WorkerInfo worker) {
        return replica != null && worker != null && replica.verifiedEpoch == worker.verifyEpoch
                && now() - replica.verifiedAt <= properties.verifyFreshForDelete().toNanos();
    }

    /**
     * At least RF survivors of the EXCESS op still exist with the same {@code gen}, have no pending
     * DELETE, are on an ALIVE, reported, non-resyncing worker and have fresh credit.
     */
    public boolean survivorsValid(Op op) {
        int valid = 0;
        for (PendingOps.Survivor survivor : op.survivors()) {
            Replica replica = replica(op.chunkId, survivor.workerId());
            WorkerInfo worker = workers.get(survivor.workerId());
            if (replica != null && replica.gen == survivor.gen() && !ops.hasDelete(op.chunkId, survivor.workerId())
                    && isCurrent(worker) && freshForDelete(replica, worker)) {
                valid++;
            }
        }
        return valid >= properties.replicationFactor();
    }

    /**
     * Asks the holder to VERIFY its copy now, unless a request for this generation went out less than
     * VERIFY_RESEND ago. A new request replaces the old one (its command is withdrawn if still queued).
     *
     * @return true if a VERIFY was queued
     */
    public boolean requestVerify(String chunkId, String workerId) {
        Replica replica = replica(chunkId, workerId);
        WorkerInfo worker = workers.get(workerId);
        ChunkInfo chunk = chunks.get(chunkId);
        if (replica == null || chunk == null || !canTakeCommands(worker)) {
            return false;
        }
        long now = now();
        if (replica.verifyCmdId != null && replica.verifyGen == replica.gen
                && now - replica.verifyRequestedAt < properties.verifyResend().toNanos()) {
            return false;
        }
        clearVerify(replica, workerId);
        Command command = enqueue(worker, InternalApi.COMMAND_VERIFY, chunkId, chunk.checksum(), null, null,
                replica.version);
        replica.verifyCmdId = command.cmdId();
        replica.verifyGen = replica.gen;
        replica.verifyEpoch = worker.verifyEpoch;
        replica.verifySentAt = 0;
        replica.verifyRequestedAt = now;
        verifyOutstanding++;
        increment("verify_requests");
        return true;
    }

    /** True if a VERIFY request for this copy's generation is outstanding and not due for a resend. */
    public boolean verifyPending(String chunkId, String workerId) {
        Replica replica = replica(chunkId, workerId);
        return replica != null && replica.verifyCmdId != null && replica.verifyGen == replica.gen
                && now() - replica.verifyRequestedAt < properties.verifyResend().toNanos();
    }

    /**
     * A RECEIVED carrying a VERIFY's {@code cmd_id}, after its physical part: credit if the request is
     * still the replica's, for the same generation and verify epoch, and was handed out.
     *
     * @return true if credit was given
     */
    public boolean verifyAnswered(String chunkId, String workerId, String cmdId) {
        Replica replica = replica(chunkId, workerId);
        WorkerInfo worker = workers.get(workerId);
        if (replica == null || worker == null || cmdId == null || !cmdId.equals(replica.verifyCmdId)) {
            return false;
        }
        boolean credit = replica.verifyGen == replica.gen && replica.verifyEpoch == worker.verifyEpoch
                && replica.verifySentAt != 0;
        if (credit) {
            replica.verifiedEpoch = replica.verifyEpoch;
            replica.verifiedAt = replica.verifySentAt;
        }
        clearVerify(replica, workerId);
        if (credit) {
            index.unpostpone(chunkId);
            markDirty(chunkId);
        }
        return credit;
    }

    /** Forgets the replica's outstanding VERIFY request (its queued command is withdrawn). */
    void clearVerify(Replica replica, String workerId) {
        if (replica.verifyCmdId == null) {
            return;
        }
        WorkerInfo worker = workers.get(workerId);
        if (worker != null) {
            worker.commands.remove(replica.verifyCmdId);
        }
        replica.verifyCmdId = null;
        replica.verifySentAt = 0;
        verifyOutstanding--;
    }

    public int verifyOutstanding() {
        return verifyOutstanding;
    }

    int maxCommandsPerWorker() {
        return properties.maxCommandsPerWorker();
    }

    int maxStreams() {
        return properties.maxStreams();
    }

    // ---- the priority index ------------------------------------------------------------------

    public NeededReplication levels() {
        return index.levels;
    }

    public LinkedHashSet<String> cleanupSet() {
        return index.cleanup;
    }

    public LinkedHashSet<String> postponedSet() {
        return index.postponed;
    }

    public int cleanupSize() {
        return index.cleanup.size();
    }

    public int postponedSize() {
        return index.postponed.size();
    }

    public int releasePostponed(int budget) {
        return index.releasePostponed(budget);
    }

    public long safeCount() {
        return index.safeCount();
    }

    /** {missing, under, over, ok, corrupt, excess}, all O(1). */
    public long[] health() {
        return new long[] {index.missing(), index.under(), index.over(), index.ok(), index.corrupt(), index.excess()};
    }

    /** Chunks with no live copy (unrecoverable until a holder returns), at most {@code limit}. */
    public List<String> unrecoverable(int limit) {
        return index.unrecoverable(limit);
    }

    /**
     * The rolling check on the next {@code count} chunks of the ring: I2 and the chunk's index entry
     * (I9, I10), which is repaired by recomputing. Chunks touching a purging worker are skipped.
     */
    public List<String> rollingCheck(int count) {
        // Compare flushed entries only: a change made earlier in this lock hold (a resync started by
        // the same planner tick, say) is pending in the dirty set, which is not drift.
        flushDirty();
        List<String> problems = new ArrayList<>();
        for (String chunkId : index.nextForRollingCheck(count)) {
            Map<String, Replica> holders = replicasOf(chunkId);
            boolean purging = false;
            for (String workerId : holders.keySet()) {
                WorkerInfo worker = workers.get(workerId);
                purging |= worker != null && worker.purging;
            }
            if (purging) {
                continue;
            }
            for (String workerId : holders.keySet()) {
                if (!chunksOn(workerId).contains(chunkId)) {
                    problems.add("I2: BLOCKS[" + chunkId + "] lists " + workerId + " which WORKER_BLOCKS does not");
                    increment("invariant_violations");
                }
            }
            String indexProblem = index.checkChunk(chunkId);
            if (indexProblem != null) {
                problems.add(indexProblem);
                increment("index_mismatches");
            }
        }
        for (String problem : problems) {
            log.error("[invariant] {}", problem);
        }
        return problems;
    }

    // ---- recent failures ------------------------------------------------------------------

    public void recordFailure(String chunkId, String workerId) {
        recentFailures.put(new FailureKey(chunkId, workerId), now() + properties.targetBackoff().toNanos());
    }

    public boolean recentlyFailed(String chunkId, String workerId) {
        Long until = recentFailures.get(new FailureKey(chunkId, workerId));
        return until != null && until > now();
    }

    public void purgeExpiredFailures() {
        long now = now();
        recentFailures.values().removeIf(until -> until <= now);
    }

    // ---- commands -------------------------------------------------------------------------

    /** Adds a command to a worker's queue (the caller checked the cap). */
    Command enqueue(WorkerInfo worker, String type, String chunkId, String expectedChecksum, String targetUrl,
                    String reason, long version) {
        Command command = new Command(UUID.randomUUID().toString(), type, chunkId, expectedChecksum, targetUrl, reason,
                version);
        worker.commands.add(command);
        return command;
    }

    /**
     * The worker is DEAD or a new process registered: its queue is cleared. Ops whose command was
     * in it go back to WAITING (REPAIR ops it was the source of are re-sourced); VERIFY requests on
     * its copies are forgotten.
     */
    public void clearCommands(WorkerInfo worker) {
        for (Command command : List.copyOf(worker.commands.stream().toList())) {
            if (InternalApi.COMMAND_VERIFY.equals(command.type())) {
                Replica replica = replica(command.chunkId(), worker.workerId);
                if (replica != null && command.cmdId().equals(replica.verifyCmdId)) {
                    clearVerify(replica, worker.workerId);
                }
                continue;
            }
            Op op = ops.byCommand(command.cmdId());
            if (op != null) {
                ops.toWaiting(op);
            }
        }
        worker.commands.clear();
        ops.resourceFrom(worker.workerId);
    }

    /**
     * Takes the commands for a heartbeat reply, in queue order, applying the hand-out filter
     * (v5 §5.2): nothing whose op targets a resyncing worker and no DELETE or VERIFY to a resyncing
     * worker (they stay queued); a REPLICATE where this worker is only the source still goes out. An
     * EXCESS delete is checked once more ({@code survivorsValid}) immediately before hand-out and
     * dropped if it fails. A command whose op or VERIFY request is gone is dropped.
     */
    public List<Command> takeCommands(WorkerInfo worker) {
        long now = now();
        return worker.commands.drain(command -> {
            switch (command.type()) {
                case InternalApi.COMMAND_REPLICATE -> {
                    Op op = ops.byCommand(command.cmdId());
                    if (op == null || op.state() != PendingOps.State.QUEUED) {
                        return CommandQueue.Decision.DROP;
                    }
                    WorkerInfo target = workers.get(op.workerId);
                    if (target == null || target.resyncing) {
                        return CommandQueue.Decision.KEEP;
                    }
                    ops.delivered(op);
                    return CommandQueue.Decision.TAKE;
                }
                case InternalApi.COMMAND_DELETE -> {
                    return handOutDelete(worker, command);
                }
                case InternalApi.COMMAND_VERIFY -> {
                    if (worker.resyncing) {
                        return CommandQueue.Decision.KEEP;
                    }
                    Replica replica = replica(command.chunkId(), worker.workerId);
                    if (replica == null || !command.cmdId().equals(replica.verifyCmdId)) {
                        return CommandQueue.Decision.DROP;
                    }
                    if (replica.verifySentAt == 0) {
                        replica.verifySentAt = now;
                    }
                    return CommandQueue.Decision.TAKE;
                }
                default -> {
                    return CommandQueue.Decision.DROP;
                }
            }
        }, MAX_COMMANDS_PER_HEARTBEAT);
    }

    /**
     * The hand-out rule for one queued DELETE, shared by heartbeat replies and direct delivery: a
     * command whose op is gone or no longer queued is dropped; nothing goes to a resyncing worker
     * (it stays queued); an EXCESS delete is checked once more and dropped if a survivor is no longer
     * verified; otherwise the op becomes SENT and its resend deadline is armed.
     */
    private CommandQueue.Decision handOutDelete(WorkerInfo worker, Command command) {
        Op op = ops.byCommand(command.cmdId());
        if (op == null || op.state() != PendingOps.State.QUEUED) {
            return CommandQueue.Decision.DROP;
        }
        if (worker.resyncing) {
            return CommandQueue.Decision.KEEP;
        }
        if (op.reason() == Reason.EXCESS && !survivorsValid(op)) {
            ops.dropAtHandOut(op, "a survivor is no longer verified at hand-out");
            increment("excess_dropped_at_handout");
            return CommandQueue.Decision.DROP;              // the drain removes the command
        }
        ops.delivered(op);
        return CommandQueue.Decision.TAKE;
    }

    /**
     * Takes up to {@code max} queued DELETE commands for the given chunks from one worker's queue,
     * for direct delivery through {@code DELETE /internal/chunks/{chunk_id}} (a client's object
     * delete). The same rule as a heartbeat reply applies to each ({@link #handOutDelete}), and only
     * a worker that is ALIVE, has completed a resync, is not resyncing and not being purged gets any:
     * for every other worker the commands stay queued for its heartbeat replies. Every other command
     * in the queue is left where it is. Must be called under the write lock.
     */
    public List<Command> takeDeletes(WorkerInfo worker, Set<String> chunkIds, int max) {
        if (worker == null || !isCurrent(worker) || worker.purging || worker.url == null) {
            return List.of();
        }
        return worker.commands.drain(command -> {
            if (!InternalApi.COMMAND_DELETE.equals(command.type()) || !chunkIds.contains(command.chunkId())) {
                return CommandQueue.Decision.KEEP;
            }
            return handOutDelete(worker, command);
        }, max);
    }

    public int queuedCommands() {
        return workers.values().stream().mapToInt(w -> w.commands.size()).sum();
    }

    // ---- events and counters --------------------------------------------------------------

    public void record(String event, String chunkId, String source, String target, String reason) {
        RepairEvent entry = new RepairEvent(Timestamps.format(clock.instant()), event, chunkId, source, target, reason);
        synchronized (events) {
            if (events.size() == EVENT_CAPACITY) {
                events.removeFirst();
            }
            events.addLast(entry);
        }
        String line = "[monitor] {} chunk={} src={} dst={} ({})";
        if ("unrecoverable".equals(event) || "timeout".equals(event)) {
            log.warn(line, event, chunkId, dash(source), dash(target), reason);
        } else if (!"replicate".equals(event)) { // the planner logs its own REPLICATE line
            log.info(line, event, chunkId, dash(source), dash(target), reason);
        }
    }

    public List<RepairEvent> events() {
        synchronized (events) {
            return List.copyOf(events);
        }
    }

    public void increment(String counter) {
        counters.computeIfAbsent(counter, k -> new AtomicLong()).incrementAndGet();
    }

    public long counter(String counter) {
        AtomicLong value = counters.get(counter);
        return value == null ? 0 : value.get();
    }

    public Map<String, Long> counters() {
        Map<String, Long> snapshot = new java.util.TreeMap<>();
        counters.forEach((k, v) -> snapshot.put(k, v.get()));
        return snapshot;
    }

    // ---- invariants -----------------------------------------------------------------------

    /**
     * Checks the structural invariants. A violation is counted and logged, never thrown: the
     * monitor must keep running.
     *
     * <ul>
     *   <li>I1 every holder is a known, non-DEAD worker; I2 the reverse index; I3 only known chunks
     *       have replicas;</li>
     *   <li>I4 a resyncing worker has no handed-out op above its fence (the hand-out filter);</li>
     *   <li>I9/I10 the priority index and its counters; I11 applied ≤ received; I12 the pending ops;
     *       I13 receipts belong to the current epoch of a pending upload;</li>
     *   <li>I14 the outstanding VERIFY counter.</li>
     * </ul>
     */
    public List<String> checkInvariants() {
        if (lock.isWriteLockedByCurrentThread()) {
            flushDirty();
        }
        List<String> violations = new ArrayList<>();
        int verifies = 0;
        for (Map.Entry<String, Map<String, Replica>> entry : blocks.entrySet()) {
            for (Map.Entry<String, Replica> holder : entry.getValue().entrySet()) {
                WorkerInfo worker = workers.get(holder.getKey());
                if (worker == null || (worker.isDead() && !worker.purging)) {
                    violations.add("I1: chunk " + entry.getKey() + " has a replica on " + holder.getKey()
                            + (worker == null ? " (unknown)" : " (DEAD)"));
                }
                if (!chunksOn(holder.getKey()).contains(entry.getKey())) {
                    violations.add("I2: BLOCKS[" + entry.getKey() + "] lists " + holder.getKey() + " which WORKER_BLOCKS does not");
                }
                if (holder.getValue().verifyCmdId != null) {
                    verifies++;
                }
            }
            if (!chunks.containsKey(entry.getKey())) {
                violations.add("I3: BLOCKS holds unknown chunk " + entry.getKey());
            }
        }
        for (Map.Entry<String, Set<String>> entry : workerBlocks.entrySet()) {
            for (String chunkId : entry.getValue()) {
                if (replica(chunkId, entry.getKey()) == null) {
                    violations.add("I2: WORKER_BLOCKS[" + entry.getKey() + "] lists " + chunkId + " which BLOCKS does not");
                }
                if (!chunks.containsKey(chunkId)) {
                    violations.add("I3: WORKER_BLOCKS[" + entry.getKey() + "] holds unknown chunk " + chunkId);
                }
            }
        }
        for (WorkerInfo worker : workers.values()) {
            if (!worker.resyncing) {
                continue;
            }
            for (Op op : ops.opsOn(worker.workerId)) {
                if (op.state() == PendingOps.State.SENT && op.version() > worker.fenceFloor
                        && op.reason() != Reason.UPLOAD) {
                    violations.add("I4: " + op + " was handed out to resyncing " + worker.workerId + " above its fence");
                }
            }
        }
        if (verifies != verifyOutstanding) {
            violations.add("I14: " + verifyOutstanding + " VERIFY requests counted, " + verifies + " found");
        }
        violations.addAll(ops.check());
        Set<String> skip = new HashSet<>();
        for (WorkerInfo worker : workers.values()) {
            if (worker.purging) {
                skip.addAll(chunksOn(worker.workerId));
            }
        }
        violations.addAll(index.checkAll(skip));
        for (WorkerInfo worker : workers.values()) {
            if (worker.applied > worker.received) {
                violations.add("I11: " + worker.workerId + " applied " + worker.applied + " > received " + worker.received);
            }
        }
        for (Map.Entry<String, Map<String, Receipt>> entry : uploadReceipts.entrySet()) {
            String objectId = com.objectstore.common.ChunkIds.objectIdOf(entry.getKey());
            for (Map.Entry<String, Receipt> holder : entry.getValue().entrySet()) {
                WorkerInfo worker = workers.get(holder.getKey());
                if (!pendingUploads.contains(objectId) || worker == null || worker.epoch != holder.getValue().epoch) {
                    violations.add("I13: receipt of " + entry.getKey() + " on " + holder.getKey()
                            + " belongs to an old epoch or an object that is not pending");
                }
            }
        }
        for (String violation : violations) {
            log.error("[invariant] {}", violation);
        }
        if (!violations.isEmpty()) {
            counters.computeIfAbsent("invariant_violations", k -> new AtomicLong()).addAndGet(violations.size());
        }
        return violations;
    }

    private static String dash(String value) {
        return value == null ? "-" : value;
    }
}
