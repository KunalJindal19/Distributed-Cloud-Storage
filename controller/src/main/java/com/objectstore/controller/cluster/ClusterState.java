package com.objectstore.controller.cluster;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.common.dto.WorkerStats;
import com.objectstore.controller.api.dto.ClusterStatusResponse.RepairEvent;
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
import java.util.LinkedHashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import org.springframework.stereotype.Component;

/**
 * The controller's in-memory view of the cluster: which workers exist and how alive they are,
 * where every copy of every chunk is, which copies are on their way, and what commands each
 * worker still has to pick up. Like the HDFS NameNode's block map it is <em>not</em> persisted:
 * after a restart it is rebuilt from the workers' block reports.
 *
 * <p><b>The concurrency rule.</b> One fair read-write lock guards all of it (the NameNode's
 * namesystem lock): {@link #read} for readers (download candidates, metadata views, status,
 * placement, fsck snapshots) and {@link #write} for brief writers (the receipt step, the health
 * loop, the monitor) and the single {@code writer} thread, which applies every queued task. No
 * network I/O, no SQLite access and no sleeping happens while it is held, read or write. The
 * methods on this class are the <em>only</em> code that writes {@code blocks}, {@code workerBlocks}
 * and {@code pending}, which is what keeps the invariants in {@link #checkInvariants()} true.
 *
 * <p>The longest continuous write-lock hold is recorded per holder (thread name) and shown in the
 * status as {@code lock_max_hold_ms}.
 *
 * <p>Times used for liveness and time-outs are monotonic ({@link System#nanoTime()}), so a
 * clock adjustment on the host can neither kill nor revive a worker.
 *
 * <p><b>Change streams (v4).</b> Each worker process numbers the changes to its disk. For each
 * worker the controller keeps {@code received} (every change up to it has been accepted, so the
 * worker may forget them) and {@code applied} (every change up to it has been applied to the block
 * map), both within the current {@code epoch}. The epoch grows on every accepted registration and
 * on DEAD, so work belonging to an earlier life of the worker is recognised and dropped.
 */
@Component
public class ClusterState {

    private static final Logger log = LoggerFactory.getLogger(ClusterState.class);
    private static final int EVENT_CAPACITY = 200;
    private static final int BOOT_FLIP_WINDOW_SECONDS = 60;
    private static final int BOOT_FLIPS_FOR_DUPLICATE = 3;
    private static final int MAX_COMMANDS_PER_HEARTBEAT = 50;

    /** The checksum recorded for a copy whose bytes failed verification: matches nothing. */
    public static final String UNVERIFIED = "unverified";

    public enum WorkerState { ALIVE, STALE, DEAD }

    public enum ReplicaState { OK, CORRUPT, EXCESS }

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
        public ReportSession session;               // the full report being applied (writer side), or null
        // The report as the receipt step sees it: which segments were accepted (and acknowledged).
        public String rxReportId;                   // the report being received, or the last one received
        public long rxSession;                      // its session number
        public long rxBaseline;
        public int rxLastSegment = -1;
        public boolean rxDone;                      // its last segment was accepted
        public boolean deadQueued;                  // a MarkDead task is queued
        public boolean purging;                     // DEAD, and its replicas are still being removed
        public boolean hasReported;                 // full report completed since (re)registration
        public long lastReportAt;                   // monotonic nanos of the last completed full report
        public String registeredAt = "";
        public WorkerStats stats;
        public final CommandQueue commands = new CommandQueue();
        public final Deque<Long> bootFlips = new ArrayDeque<>();  // monotonic nanos of boot id changes
        public volatile long lastDataError;         // monotonic nanos of the last failed data-path call (no lock)
        public boolean fullReportRequested;         // set by the monitor; cleared by the next heartbeat reply

        WorkerInfo(String workerId) {
            this.workerId = workerId;
        }

        public boolean isDead() {
            return state == WorkerState.DEAD;
        }

        /** A full report is being received (its last segment has not been accepted yet). */
        public boolean reportInProgress() {
            return rxReportId != null && !rxDone;
        }

        /** Forgets the receipt-side report state (re-registration, DEAD). */
        void resetReceipt() {
            rxReportId = null;
            rxSession = 0;
            rxBaseline = 0;
            rxLastSegment = -1;
            rxDone = false;
        }
    }

    /**
     * One copy of one chunk on one worker. Every replica comes from the worker's own change
     * stream or full report (or from an upload receipt, which came from them), so all are confirmed.
     */
    public static final class Replica {
        public ReplicaState state = ReplicaState.OK;
        public long mark;                           // the report session that last listed it; 0 = none
        public long addedAt;                        // monotonic nanos
        public long deleteQueuedAt;                 // monotonic nanos; 0 = no delete queued
        public boolean deleteDelivered;             // the DELETE was handed to the worker; it cannot be cancelled
        public String deleteCmdId;
    }

    public enum PendingKind { REPAIR, UPLOAD }

    /**
     * A copy on its way to {@code target}: a REPAIR that {@code source} was told to make (with the
     * REPLICATE's {@code cmdId}), or an UPLOAD the controller wrote itself and whose RECEIVED has
     * not arrived yet ({@code source} and {@code cmdId} are null). It ends by the target's RECEIVED
     * or a report listing the copy, by {@code CMD_FAILED}, by time-out, by its source or target
     * becoming DEAD or re-registering, or by its chunk being deleted.
     */
    public record PendingCopy(long id, String chunkId, PendingKind kind, String source, String target, long deadline,
                              String cmdId, long startedAt) {

        public boolean involves(String workerId) {
            return workerId.equals(source) || workerId.equals(target);
        }
    }

    /**
     * A full report being applied for one worker (the writer's side; the receipt step tracks which
     * segments it accepted in {@code WorkerInfo.rx*}). {@code number} is unique across all
     * sessions; a segment sets {@code replica.mark = number} on every copy it lists, and completion
     * removes the worker's copies whose mark differs.
     */
    public static final class ReportSession {
        public final long baseline;
        public final long number;
        public long listed;
        public final List<String> garbage = new ArrayList<>();
        public final List<String> orphans = new ArrayList<>();
        public boolean closed;

        ReportSession(long number, long baseline) {
            this.baseline = baseline;
            this.number = number;
        }
    }

    /**
     * Proof that worker {@code W}, in {@code epoch}, holds bytes of a chunk whose upload has not
     * committed yet. The commit turns it into a replica; {@code seenInSession} becomes its mark.
     */
    public static final class Receipt {
        public final long epoch;
        public long size;
        public String checksum;
        public long seenInSession;

        Receipt(long epoch, long size, String checksum, long seenInSession) {
            this.epoch = epoch;
            this.size = size;
            this.checksum = checksum;
            this.seenInSession = seenInSession;
        }
    }

    /** Mirror of one row of the {@code chunks} table. */
    public record ChunkInfo(String objectId, int idx, int size, String checksum) {
    }

    private record FailureKey(String chunkId, String workerId) {
    }

    private final ControllerProperties properties;
    private final Clock clock;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Map<String, Long> maxHoldNanos = new ConcurrentHashMap<>();
    private long writeStartedAt;                    // when the current outermost write hold began
    /** The lock of the most recently created cluster state, for {@link #lockHeldByCurrentThread()}. */
    private static volatile ReentrantReadWriteLock latestLock;
    private final long startedAt = System.nanoTime();

    private volatile String clusterId = "";
    private final Map<String, WorkerInfo> workers = new ConcurrentHashMap<>();
    private final DeletionLedger ledger = new DeletionLedger(this);
    private final ReplicationIndex index;
    private final Map<Long, PendingCopy> pendingById = new HashMap<>();
    private final Map<String, Integer> pendingFromCount = new HashMap<>();
    private final Map<String, Integer> pendingToCount = new HashMap<>();
    private long pendingIds;
    private volatile DeadlineScheduler deadlines = DeadlineScheduler.NONE;
    private final Map<String, ChunkInfo> chunks = new HashMap<>();
    private final Map<String, Map<String, Replica>> blocks = new HashMap<>();
    private final Map<String, Set<String>> workerBlocks = new HashMap<>();
    private final Map<String, List<PendingCopy>> pending = new HashMap<>();
    private final Set<String> pendingUploads = new HashSet<>();
    /** Objects whose chunks are certainly garbage: failed or interrupted uploads, deleted objects. */
    private final Set<String> garbageObjects = new HashSet<>();
    /** Upload receipts: chunk id (of an upload in progress) -> worker id -> receipt. */
    private final Map<String, Map<String, Receipt>> uploadReceipts = new HashMap<>();
    private long sessionNumbers;
    private final Map<FailureKey, Long> recentFailures = new HashMap<>();
    private final Deque<RepairEvent> events = new ArrayDeque<>(EVENT_CAPACITY);
    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private volatile boolean safeMode = true;
    private volatile int blockedLastTick;
    private long safeModeThresholdReachedAt;        // monotonic nanos; 0 = not reached

    public ClusterState(ControllerProperties properties, Clock clock) {
        this.properties = properties;
        this.clock = clock;
        this.index = new ReplicationIndex(this, properties.replicationFactor());
        latestLock = lock;
    }

    /** Where deadlines go (the pending-timer); without one (unit tests) nothing expires by itself. */
    public interface DeadlineScheduler {
        void pendingCopy(long id, long atNanos);

        void ledgerEntry(String chunkId, String workerId, long atNanos);

        DeadlineScheduler NONE = new DeadlineScheduler() {
            @Override
            public void pendingCopy(long id, long atNanos) {
            }

            @Override
            public void ledgerEntry(String chunkId, String workerId, long atNanos) {
            }
        };
    }

    public void setDeadlineScheduler(DeadlineScheduler scheduler) {
        this.deadlines = scheduler;
    }

    DeadlineScheduler deadlines() {
        return deadlines;
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

    /**
     * Brings the priority index up to date: every chunk marked dirty since the last flush is
     * recomputed. Runs before every write-lock release and at every writer yield.
     */
    public void flushDirty() {
        index.flush();
    }

    /** The one way to tell the index that something about a chunk changed (v4 §7.2). */
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

    /** For assertions in code that must only run under the lock (read or write). */
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

    /** Chunks that needed a copy in the last monitor tick but had no usable source or target. */
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
        return workers.get(workerId);
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

    /** Workers sorted by id, the order placement and status use. */
    public List<WorkerInfo> workersSorted() {
        List<WorkerInfo> sorted = new ArrayList<>(workers.values());
        sorted.sort(Comparator.comparing(w -> w.workerId));
        return sorted;
    }

    /**
     * Records a change of boot id and reports whether it looks like two processes fighting over
     * one WORKER_ID: three or more flips inside one minute.
     */
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

    /** Forgets a chunk entirely: its row mirror, every replica and every pending copy. */
    public void removeChunk(String chunkId) {
        chunks.remove(chunkId);
        markDirty(chunkId);
        for (PendingCopy copy : List.copyOf(pendingOf(chunkId))) {
            removePendingCopy(copy);
        }
        Map<String, Replica> holders = blocks.remove(chunkId);
        if (holders != null) {
            for (String workerId : holders.keySet()) {
                Set<String> set = workerBlocks.get(workerId);
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

    /** Remembers that every chunk of this object is garbage (its upload failed, or it was deleted). */
    public void addGarbage(String objectId) {
        garbageObjects.add(objectId);
    }

    public boolean isGarbage(String objectId) {
        return garbageObjects.contains(objectId);
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

    /**
     * Records (or refreshes) a receipt. A receipt of the same epoch keeps its {@code seenInSession}
     * unless {@code seenInSession} is given: only report segments set it.
     */
    public Receipt putReceipt(String chunkId, String workerId, long epoch, long size, String checksum,
                              long seenInSession) {
        Map<String, Receipt> byWorker = uploadReceipts.computeIfAbsent(chunkId, id -> new HashMap<>());
        Receipt existing = byWorker.get(workerId);
        if (existing != null && existing.epoch == epoch) {
            existing.size = size;
            existing.checksum = checksum;
            if (seenInSession != 0) {
                existing.seenInSession = seenInSession;
            }
            return existing;
        }
        Receipt receipt = new Receipt(epoch, size, checksum, seenInSession);
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

    /** A new, unique report session number (allocated by the receipt step). */
    public long nextSessionNumber() {
        return ++sessionNumbers;
    }

    /** Opens the writer-side session {@code number} for a worker, replacing any open one. */
    public ReportSession openSession(WorkerInfo worker, long number, long baseline) {
        if (worker.session != null) {
            worker.session.closed = true;
        }
        worker.session = new ReportSession(number, baseline);
        return worker.session;
    }

    /** Closes the worker's open report session, if any (completed, re-registered or DEAD). */
    public void closeSession(WorkerInfo worker) {
        if (worker.session != null) {
            worker.session.closed = true;
            worker.session = null;
        }
    }

    /** Workers with a report being received or applied. */
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
     * Records a copy, or sets the state of an existing one. Refused (returns null) if the worker is
     * unknown or DEAD, or if the chunk is not known.
     */
    public Replica addReplica(String chunkId, String workerId, ReplicaState state) {
        WorkerInfo worker = workers.get(workerId);
        if (worker == null || worker.isDead() || !chunks.containsKey(chunkId)) {
            return null;
        }
        Map<String, Replica> holders = blocks.computeIfAbsent(chunkId, id -> new LinkedHashMap<>());
        Replica replica = holders.get(workerId);
        if (replica == null) {
            replica = new Replica();
            replica.addedAt = now();
            holders.put(workerId, replica);
            workerBlocks.computeIfAbsent(workerId, id -> new HashSet<>()).add(chunkId);
        }
        replica.state = state;
        markDirty(chunkId);
        return replica;
    }

    /** Changes a copy's state (OK, CORRUPT, EXCESS). */
    public void setReplicaState(String chunkId, String workerId, ReplicaState state) {
        Replica replica = replica(chunkId, workerId);
        if (replica != null && replica.state != state) {
            replica.state = state;
            markDirty(chunkId);
        }
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
            markDirty(chunkId);
            protect(chunkId);
        }
        return removed;
    }

    /** Marks a copy CORRUPT (it stays on disk as a last resort until a replacement exists). */
    public boolean markCorrupt(String chunkId, String workerId) {
        Replica replica = replica(chunkId, workerId);
        if (replica == null || replica.state == ReplicaState.CORRUPT) {
            return false;
        }
        replica.state = ReplicaState.CORRUPT;
        markDirty(chunkId);
        protect(chunkId);
        return true;
    }

    /**
     * Keeps invariant I6 true at every instant: if a chunk has fewer than RF live copies, every
     * excess or corrupt deletion of it that has not been handed to a worker yet (WAITING or QUEUED
     * in the ledger) is cancelled, and an EXCESS copy whose deletion was cancelled counts as OK
     * again. A DELETE that was already delivered cannot be called back (invariant I5).
     *
     * @return number of EXCESS copies turned back to OK
     */
    public int protect(String chunkId) {
        if (!chunks.containsKey(chunkId) || live(chunkId).size() >= properties.replicationFactor()) {
            return 0;
        }
        int restored = 0;
        // An EXCESS copy whose deletion is not tracked (dropped when its worker re-registered) and
        // was never delivered counts again too.
        for (Map.Entry<String, Replica> holder : List.copyOf(replicasOf(chunkId).entrySet())) {
            Replica replica = holder.getValue();
            if (replica.state == ReplicaState.EXCESS && !replica.deleteDelivered
                    && ledger.entry(chunkId, holder.getKey()) == null) {
                setReplicaState(chunkId, holder.getKey(), ReplicaState.OK);
                restored++;
                record("restored", chunkId, holder.getKey(), null, "a live copy was lost; the surplus copy counts again");
            }
        }
        for (DeletionLedger.Entry entry : ledger.entriesFor(chunkId)) {
            if (!entry.linkedToReplica() || entry.state == DeletionLedger.State.DELIVERED) {
                continue;
            }
            String workerId = entry.workerId;
            if (ledger.cancel(chunkId, workerId)) {
                Replica replica = replica(chunkId, workerId);
                if (replica != null && replica.state == ReplicaState.EXCESS) {
                    setReplicaState(chunkId, workerId, ReplicaState.OK);
                    restored++;
                    record("restored", chunkId, workerId, null,
                            "a live copy was lost; the surplus copy's undelivered DELETE was cancelled");
                }
            }
        }
        return restored;
    }

    /** Asks the ledger to delete a surplus or corrupt copy ({@code excess} / {@code corrupt}). */
    public void sendDelete(String workerId, String chunkId, String reason) {
        if (replica(chunkId, workerId) != null) {
            ledger.request(chunkId, workerId, reason);
        }
    }

    public DeletionLedger ledger() {
        return ledger;
    }

    int maxCommandsPerWorker() {
        return properties.maxCommandsPerWorker();
    }

    long pendingTimeoutNanos() {
        return properties.pendingTimeout().toNanos();
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

    /** OK copies on non-DEAD workers. A STALE worker's copies still count, as in HDFS. */
    public List<String> live(String chunkId) {
        List<String> live = new ArrayList<>();
        for (Map.Entry<String, Replica> entry : replicasOf(chunkId).entrySet()) {
            WorkerInfo worker = workers.get(entry.getKey());
            if (entry.getValue().state == ReplicaState.OK && worker != null && !worker.isDead()) {
                live.add(entry.getKey());
            }
        }
        return live;
    }

    /**
     * The workers to read a chunk from, best first: ALIVE before STALE, no recent data-path
     * error before a recent one, the worker holding fewer copies first, then by id.
     */
    public List<String> readable(String chunkId) {
        long now = now();
        long recent = properties.targetBackoff().toNanos();
        List<String> live = live(chunkId);
        live.sort(Comparator
                .comparingInt((String w) -> workers.get(w).state == WorkerState.ALIVE ? 0 : 1)
                .thenComparingInt(w -> now - workers.get(w).lastDataError < recent && workers.get(w).lastDataError != 0 ? 1 : 0)
                .thenComparingInt(w -> chunksOn(w).size())
                .thenComparing(w -> w));
        return live;
    }

    /**
     * Where a download may read a chunk: {@link #readable}, then the ALIVE targets of UPLOAD
     * pending copies (written and fsynced, but their RECEIVED has not been applied yet), last.
     */
    public List<String> readCandidates(String chunkId) {
        List<String> candidates = readable(chunkId);
        for (PendingCopy copy : pendingOf(chunkId)) {
            WorkerInfo worker = workers.get(copy.target());
            if (copy.kind() == PendingKind.UPLOAD && worker != null && worker.state == WorkerState.ALIVE
                    && !candidates.contains(copy.target())) {
                candidates.add(copy.target());
            }
        }
        return candidates;
    }

    // ---- pending copies -------------------------------------------------------------------

    public Map<String, List<PendingCopy>> pending() {
        return pending;
    }

    public List<PendingCopy> pendingOf(String chunkId) {
        return pending.getOrDefault(chunkId, Collections.emptyList());
    }

    public PendingCopy pendingById(long id) {
        return pendingById.get(id);
    }

    private PendingCopy addPendingCopy(String chunkId, PendingKind kind, String source, String target, String cmdId) {
        long now = now();
        PendingCopy copy = new PendingCopy(++pendingIds, chunkId, kind, source, target,
                now + properties.pendingTimeout().toNanos(), cmdId, now);
        pending.computeIfAbsent(chunkId, id -> new ArrayList<>()).add(copy);
        pendingById.put(copy.id(), copy);
        if (source != null) {
            pendingFromCount.merge(source, 1, Integer::sum);
        }
        pendingToCount.merge(target, 1, Integer::sum);
        markDirty(chunkId);
        deadlines.pendingCopy(copy.id(), copy.deadline());
        return copy;
    }

    /** Ends a pending copy (success, failure, time-out, DEAD, re-registration, deletion). */
    public boolean removePendingCopy(PendingCopy copy) {
        List<PendingCopy> list = pending.get(copy.chunkId());
        if (list == null || !list.remove(copy)) {
            return false;
        }
        if (list.isEmpty()) {
            pending.remove(copy.chunkId());
        }
        pendingById.remove(copy.id());
        if (copy.source() != null) {
            pendingFromCount.computeIfPresent(copy.source(), (w, n) -> n > 1 ? n - 1 : null);
        }
        pendingToCount.computeIfPresent(copy.target(), (w, n) -> n > 1 ? n - 1 : null);
        markDirty(copy.chunkId());
        return true;
    }

    /** A REPAIR: {@code source} was told (command {@code cmdId}) to copy the chunk to {@code target}. */
    public PendingCopy addPending(String chunkId, String source, String target, String cmdId) {
        return addPendingCopy(chunkId, PendingKind.REPAIR, source, target, cmdId);
    }

    /** An UPLOAD copy whose RECEIVED has not arrived: counted, readable, and expiring into normal repair. */
    public void addUploadPending(String chunkId, String target) {
        for (PendingCopy existing : pendingOf(chunkId)) {
            if (existing.target().equals(target)) {
                return;
            }
        }
        addPendingCopy(chunkId, PendingKind.UPLOAD, null, target, null);
    }

    /** Removes the REPAIR pending copy started by command {@code cmdId}, if it is still pending. */
    public PendingCopy removePendingByCommand(String chunkId, String cmdId) {
        if (cmdId == null) {
            return null;
        }
        for (PendingCopy copy : List.copyOf(pendingOf(chunkId))) {
            if (cmdId.equals(copy.cmdId())) {
                removePendingCopy(copy);
                return copy;
            }
        }
        return null;
    }

    public PendingCopy removePending(String chunkId, String target) {
        PendingCopy removed = null;
        for (PendingCopy copy : List.copyOf(pendingOf(chunkId))) {
            if (copy.target().equals(target)) {
                removePendingCopy(copy);
                removed = copy;
            }
        }
        return removed;
    }

    public int dropPendingForWorker(String workerId, boolean asSource, boolean asTarget) {
        int dropped = 0;
        if (!pendingFromCount.containsKey(workerId) && !pendingToCount.containsKey(workerId)) {
            return 0;
        }
        for (PendingCopy copy : List.copyOf(pendingById.values())) {
            if ((asSource && workerId.equals(copy.source())) || (asTarget && copy.target().equals(workerId))) {
                removePendingCopy(copy);
                dropped++;
            }
        }
        return dropped;
    }

    /** REPAIR copies in flight from this worker (an incremental counter). */
    public int pendingFrom(String workerId) {
        return pendingFromCount.getOrDefault(workerId, 0);
    }

    /** Copies (repair and upload) on their way to this worker (an incremental counter). */
    public int pendingTo(String workerId) {
        return pendingToCount.getOrDefault(workerId, 0);
    }

    public int pendingTotal() {
        return pendingById.size();
    }

    /**
     * {@code ExpirePending} (§7.4): if the pending copy has not ended yet, it does now. A REPAIR
     * also avoids that (chunk, target) pair for TARGET_BACKOFF. Returns true if it expired.
     */
    public boolean expirePending(long id) {
        return expirePending(id, now());
    }

    private boolean expirePending(long id, long now) {
        PendingCopy copy = pendingById.get(id);
        if (copy == null || copy.deadline() > now) {
            return false;                                   // already ended (or a stale deadline)
        }
        removePendingCopy(copy);
        if (copy.kind() == PendingKind.UPLOAD) {
            record("timeout", copy.chunkId(), null, copy.target(), "upload copy not confirmed within "
                    + properties.pendingTimeoutSeconds() + " s; normal repair takes over");
        } else {
            recordFailure(copy.chunkId(), copy.target());
            record("timeout", copy.chunkId(), copy.source(), copy.target(), "copy not confirmed within "
                    + properties.pendingTimeoutSeconds() + " s; avoiding this target for a while");
        }
        return true;
    }

    /** Expires every pending copy and ledger deadline due by {@code now} (tests; the timer does this). */
    public int expireDue(long now) {
        int expired = 0;
        for (PendingCopy copy : List.copyOf(pendingById.values())) {
            if (expirePending(copy.id(), now)) {
                expired++;
            }
        }
        return expired + ledger.expireDeadlines(now);
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

    /** Moves up to {@code budget} postponed chunks back to cleanup (a worker became ALIVE or reported). */
    public int releasePostponed(int budget) {
        return index.releasePostponed(budget);
    }

    /** Chunks with at least one live copy (O(1); safe mode). */
    public long safeCount() {
        return index.safeCount();
    }

    /** {missing, under, over, ok, corrupt, excess}, all O(1). */
    public long[] health() {
        return new long[] {index.missing(), index.under(), index.over(), index.ok(), index.corrupt(), index.excess()};
    }

    /**
     * The rolling check (v4 §8) on the next {@code count} chunks of the ring: I2, I3 and I5 for
     * each, and its index entry (I9, I10), which is repaired by recomputing. Chunks touching a
     * purging worker are skipped. Returns the problems found.
     */
    public List<String> rollingCheck(int count) {
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
            for (Map.Entry<String, Replica> holder : holders.entrySet()) {
                if (!chunksOn(holder.getKey()).contains(chunkId)) {
                    problems.add("I2: BLOCKS[" + chunkId + "] lists " + holder.getKey() + " which WORKER_BLOCKS does not");
                    increment("invariant_violations");
                }
                if (holder.getValue().deleteDelivered && holder.getValue().state == ReplicaState.OK) {
                    problems.add("I5: chunk " + chunkId + " on " + holder.getKey() + " is OK but its DELETE was delivered");
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

    /**
     * Queues a REPLICATE for a worker. Identical commands (type, chunk, target) are queued once
     * (an O(1) lookup). A full queue (MAX_COMMANDS_PER_WORKER) refuses: commands are never dropped.
     * DELETE commands come only from the {@link DeletionLedger}.
     *
     * @return the queued (or the already queued identical) command, or null if refused
     */
    public Command queueCommand(String workerId, String type, String chunkId, String checksum, String targetUrl,
                                String reason) {
        if (InternalApi.COMMAND_DELETE.equals(type)) {
            throw new IllegalArgumentException("DELETE commands are created by the deletion ledger");
        }
        WorkerInfo worker = workers.get(workerId);
        if (worker == null) {
            return null;
        }
        Command existing = worker.commands.find(type, chunkId, targetUrl);
        if (existing != null) {
            return existing;
        }
        if (worker.commands.size() >= properties.maxCommandsPerWorker()) {
            return null;
        }
        return enqueue(worker, type, chunkId, checksum, targetUrl, reason);
    }

    /** Adds a command to a worker's queue (the caller checked the cap and duplicates). */
    Command enqueue(WorkerInfo worker, String type, String chunkId, String checksum, String targetUrl, String reason) {
        Command command = new Command(UUID.randomUUID().toString(), type, chunkId, checksum, targetUrl, reason,
                InternalApi.nowSeconds() + properties.commandTtlSeconds());
        worker.commands.add(command);
        return command;
    }

    /** Removes a queued REPLICATE that was not delivered yet (DELETEs are cancelled through the ledger). */
    public boolean cancelCommand(String workerId, String cmdId) {
        WorkerInfo worker = workers.get(workerId);
        return worker != null && worker.commands.remove(cmdId);
    }

    /** Removes the queued REPLICATE commands for these chunks (the chunks are being forgotten). */
    void cancelReplicatesFor(Set<String> chunkIds) {
        for (WorkerInfo worker : workers.values()) {
            worker.commands.removeIf(command -> !InternalApi.COMMAND_DELETE.equals(command.type())
                    && chunkIds.contains(command.chunkId()));
        }
    }

    /** The worker is DEAD or registers again: its queue is cleared and its ledger entries wait. */
    public void clearCommands(WorkerInfo worker) {
        worker.commands.clear();
        ledger.pause(worker.workerId);
    }

    /**
     * Takes the commands to hand to a worker in a heartbeat reply, dropping expired ones (an
     * expired DELETE's ledger entry waits again). A DELETE handed over is DELIVERED in the ledger
     * and on its replica: from now on it cannot be cancelled.
     */
    public List<Command> takeCommands(WorkerInfo worker) {
        List<Command> delivered = new ArrayList<>();
        double now = InternalApi.nowSeconds();
        while (delivered.size() < MAX_COMMANDS_PER_HEARTBEAT && !worker.commands.isEmpty()) {
            Command command = worker.commands.poll();
            boolean delete = InternalApi.COMMAND_DELETE.equals(command.type());
            if (command.expiresAt() < now) {
                increment("commands_expired");
                if (delete) {
                    ledger.commandLost(command.cmdId());
                }
                continue;
            }
            if (delete) {
                ledger.delivered(command);
            }
            delivered.add(command);
        }
        return delivered;
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
     * Checks the structural invariants of the block map. A violation is counted and logged,
     * never thrown: the monitor must keep running.
     */
    public List<String> checkInvariants() {
        if (lock.isWriteLockedByCurrentThread()) {
            flushDirty();                 // (under the read lock nothing is dirty: every write unlock flushes)
        }
        List<String> violations = new ArrayList<>();
        int rf = properties.replicationFactor();
        // I1: every holder in BLOCKS is a known, non-DEAD worker.
        for (Map.Entry<String, Map<String, Replica>> entry : blocks.entrySet()) {
            for (String workerId : entry.getValue().keySet()) {
                WorkerInfo worker = workers.get(workerId);
                if (worker == null || (worker.isDead() && !worker.purging)) {   // a purge removes them in steps
                    violations.add("I1: chunk " + entry.getKey() + " has a replica on " + workerId
                            + (worker == null ? " (unknown)" : " (DEAD)"));
                }
            }
            // I3: every chunk in BLOCKS is in CHUNKS.
            if (!chunks.containsKey(entry.getKey())) {
                violations.add("I3: BLOCKS holds unknown chunk " + entry.getKey());
            }
            // I5: a delivered DELETE belongs to an EXCESS or CORRUPT replica.
            for (Map.Entry<String, Replica> holder : entry.getValue().entrySet()) {
                if (holder.getValue().deleteDelivered && holder.getValue().state == ReplicaState.OK) {
                    violations.add("I5: chunk " + entry.getKey() + " on " + holder.getKey() + " is OK but its DELETE was delivered");
                }
            }
        }
        // I2: WORKER_BLOCKS is the exact reverse index of BLOCKS.
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
        for (Map.Entry<String, Map<String, Replica>> entry : blocks.entrySet()) {
            for (String workerId : entry.getValue().keySet()) {
                if (!chunksOn(workerId).contains(entry.getKey())) {
                    violations.add("I2: BLOCKS[" + entry.getKey() + "] lists " + workerId + " which WORKER_BLOCKS does not");
                }
            }
        }
        // I4 and I8: pending copies are between live workers, to a worker without the chunk,
        // bounded per source, and unique per target.
        Map<String, Integer> perSource = new HashMap<>();
        for (Map.Entry<String, List<PendingCopy>> entry : pending.entrySet()) {
            String chunkId = entry.getKey();
            if (!chunks.containsKey(chunkId)) {
                violations.add("I3: PENDING holds unknown chunk " + chunkId);
            }
            Set<String> targets = new HashSet<>();
            for (PendingCopy copy : entry.getValue()) {
                WorkerInfo target = workers.get(copy.target());
                if (copy.kind() == PendingKind.UPLOAD) {
                    if (copy.source() != null || target == null || target.isDead()) {
                        violations.add("I4: upload pending copy of " + chunkId + " to " + copy.target()
                                + " has a source or a DEAD target");
                    }
                } else {
                    WorkerInfo source = copy.source() == null ? null : workers.get(copy.source());
                    if (source == null || source.isDead() || target == null || target.isDead()) {
                        violations.add("I4: pending copy of " + chunkId + " " + copy.source() + "->" + copy.target()
                                + " involves a DEAD worker");
                    }
                    perSource.merge(copy.source(), 1, Integer::sum);
                }
                if (replica(chunkId, copy.target()) != null) {
                    violations.add("I4: pending copy of " + chunkId + " targets " + copy.target() + " which already holds it");
                }
                if (!targets.add(copy.target())) {
                    violations.add("I8: chunk " + chunkId + " has two pending copies to " + copy.target());
                }
            }
        }
        for (Map.Entry<String, Integer> entry : perSource.entrySet()) {
            if (entry.getValue() > properties.maxStreams()) {
                violations.add("I4: " + entry.getKey() + " is the source of " + entry.getValue() + " pending copies (max " + properties.maxStreams() + ")");
            }
        }
        // I6: no excess/corrupt DELETE is queued for a chunk that would drop below RF.
        for (WorkerInfo worker : workers.values()) {
            for (Command command : worker.commands) {
                if (InternalApi.COMMAND_DELETE.equals(command.type())
                        && (DeletionLedger.EXCESS.equals(command.reason()) || DeletionLedger.CORRUPT.equals(command.reason()))) {
                    Replica replica = replica(command.chunkId(), worker.workerId);
                    if (replica == null) {
                        continue;
                    }
                    List<String> live = live(command.chunkId());
                    int liveAfter = live.size() - (replica.state == ReplicaState.OK && live.contains(worker.workerId) ? 1 : 0);
                    if (liveAfter < rf) {
                        violations.add("I6: DELETE (" + command.reason() + ") queued on " + worker.workerId + " for "
                                + command.chunkId() + " would leave " + liveAfter + " live copies");
                    }
                }
            }
        }
        // I12: the deletion ledger.
        violations.addAll(ledger.check());
        // I9, I10: the priority index and every counter against a recomputation (skipping chunks of
        // a worker that is being purged: its copies leave in steps).
        Set<String> skip = new HashSet<>();
        for (WorkerInfo worker : workers.values()) {
            if (worker.purging) {
                skip.addAll(chunksOn(worker.workerId));
            }
        }
        violations.addAll(index.checkAll(skip));
        Map<String, Integer> from = new HashMap<>();
        Map<String, Integer> to = new HashMap<>();
        int total = 0;
        for (List<PendingCopy> list : pending.values()) {
            for (PendingCopy copy : list) {
                total++;
                if (copy.source() != null) {
                    from.merge(copy.source(), 1, Integer::sum);
                }
                to.merge(copy.target(), 1, Integer::sum);
                if (pendingById.get(copy.id()) != copy) {
                    violations.add("I10: pending copy " + copy.id() + " is missing from the id index");
                }
            }
        }
        if (!from.equals(pendingFromCount) || !to.equals(pendingToCount) || total != pendingById.size()) {
            violations.add("I10: pendingFrom/pendingTo counters " + pendingFromCount + "/" + pendingToCount
                    + " differ from a recount " + from + "/" + to);
        }
        // I11: applied <= received for every worker.
        for (WorkerInfo worker : workers.values()) {
            if (worker.applied > worker.received) {
                violations.add("I11: " + worker.workerId + " applied " + worker.applied + " > received " + worker.received);
            }
        }
        // I13: no receipt belongs to an old epoch or to an object that is not pending.
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

    /** For log lines and the status page: a short form of the replica's state. */
    public static String letter(ReplicaState state) {
        return switch (state) {
            case OK -> "OK";
            case CORRUPT -> "C";
            case EXCESS -> "E";
        };
    }

    private static String dash(String value) {
        return value == null ? "-" : value;
    }
}
