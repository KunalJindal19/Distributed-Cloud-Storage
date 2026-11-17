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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
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
 * <p><b>The concurrency rule.</b> One lock, {@link #locked}, guards all of it (the NameNode's
 * namesystem lock). Every read-then-modify of this state runs as one synchronous block under the
 * lock, with no network I/O inside. The methods on this class are the <em>only</em> code that
 * writes {@code blocks}, {@code workerBlocks} and {@code pending}, which is what keeps the
 * invariants in {@link #checkInvariants()} true.
 *
 * <p>Times used for liveness and time-outs are monotonic ({@link System#nanoTime()}), so a
 * clock adjustment on the host can neither kill nor revive a worker.
 */
@Component
public class ClusterState {

    private static final Logger log = LoggerFactory.getLogger(ClusterState.class);
    private static final int EVENT_CAPACITY = 200;
    private static final int BOOT_FLIP_WINDOW_SECONDS = 60;
    private static final int BOOT_FLIPS_FOR_DUPLICATE = 3;
    private static final int MAX_COMMANDS_PER_HEARTBEAT = 50;

    public enum WorkerState { ALIVE, STALE, DEAD }

    public enum ReplicaState { OK, CORRUPT, EXCESS }

    /** One worker as the controller knows it (HDFS DatanodeDescriptor). */
    public static final class WorkerInfo {
        public final String workerId;
        public String url;
        public String bootId;
        public WorkerState state = WorkerState.ALIVE;
        public long lastSeen;                       // monotonic nanos of the last accepted heartbeat
        public long lastHbSeq = -1;
        public long lastReportSeq = -1;
        public boolean hasReported;                 // full report processed since (re)registration
        public long lastReportAt;                   // monotonic nanos of the last accepted full report
        public String registeredAt = "";
        public WorkerStats stats;
        public final Deque<Command> commands = new ArrayDeque<>();
        public final Deque<Long> bootFlips = new ArrayDeque<>();  // monotonic nanos of boot id changes
        public long lastDataError;                  // monotonic nanos of the last failed data-path call
        public boolean fullReportRequested;         // set by the monitor; cleared by the next heartbeat reply

        WorkerInfo(String workerId) {
            this.workerId = workerId;
        }

        public boolean isDead() {
            return state == WorkerState.DEAD;
        }
    }

    /** One copy of one chunk on one worker. */
    public static final class Replica {
        public ReplicaState state = ReplicaState.OK;
        public boolean confirmed;                   // seen in this worker's ordered report channel
        public long addedAt;                        // monotonic nanos
        public long deleteQueuedAt;                 // monotonic nanos; 0 = no delete queued
        public boolean deleteDelivered;             // the DELETE was handed to the worker; it cannot be cancelled
        public String deleteCmdId;
    }

    /** A copy that a source worker has been told to make on a target worker. */
    public record PendingCopy(String source, String target, long deadline, String cmdId, long startedAt) {
    }

    /** Mirror of one row of the {@code chunks} table. */
    public record ChunkInfo(String objectId, int idx, int size, String checksum) {
    }

    private record FailureKey(String chunkId, String workerId) {
    }

    private final ControllerProperties properties;
    private final Clock clock;
    private final ReentrantLock lock = new ReentrantLock();
    private final long startedAt = System.nanoTime();

    private volatile String clusterId = "";
    private final Map<String, WorkerInfo> workers = new LinkedHashMap<>();
    private final Map<String, ChunkInfo> chunks = new HashMap<>();
    private final Map<String, Map<String, Replica>> blocks = new HashMap<>();
    private final Map<String, Set<String>> workerBlocks = new HashMap<>();
    private final Map<String, List<PendingCopy>> pending = new HashMap<>();
    private final Set<String> pendingUploads = new HashSet<>();
    /** Objects whose chunks are certainly garbage: failed or interrupted uploads, deleted objects. */
    private final Set<String> garbageObjects = new HashSet<>();
    /** object id of an upload in progress -> "chunkId@workerId" copies already confirmed by a report. */
    private final Map<String, Set<String>> uploadReceipts = new HashMap<>();
    private final Map<FailureKey, Long> recentFailures = new HashMap<>();
    private final Deque<RepairEvent> events = new ArrayDeque<>(EVENT_CAPACITY);
    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private volatile boolean safeMode = true;
    private volatile int blockedLastTick;
    private long safeModeThresholdReachedAt;        // monotonic nanos; 0 = not reached

    public ClusterState(ControllerProperties properties, Clock clock) {
        this.properties = properties;
        this.clock = clock;
    }

    // ---- the lock -------------------------------------------------------------------------

    /** Runs {@code action} as one synchronous block under the namesystem lock. */
    public <T> T locked(Supplier<T> action) {
        lock.lock();
        try {
            return action.get();
        } finally {
            lock.unlock();
        }
    }

    public void locked(Runnable action) {
        lock.lock();
        try {
            action.run();
        } finally {
            lock.unlock();
        }
    }

    /** For assertions in code that must only run under the lock. */
    public boolean isLocked() {
        return lock.isHeldByCurrentThread();
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
    }

    /** Forgets a chunk entirely: its row mirror, every replica and every pending copy. */
    public void removeChunk(String chunkId) {
        chunks.remove(chunkId);
        Map<String, Replica> holders = blocks.remove(chunkId);
        if (holders != null) {
            for (String workerId : holders.keySet()) {
                Set<String> set = workerBlocks.get(workerId);
                if (set != null) {
                    set.remove(chunkId);
                }
            }
        }
        pending.remove(chunkId);
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

    public void endUpload(String objectId) {
        pendingUploads.remove(objectId);
        uploadReceipts.remove(objectId);
    }

    /**
     * A worker's ordered report channel mentioned a chunk of an upload that is not committed yet.
     * Remembered so the copy can be recorded as confirmed when the upload commits.
     */
    public void recordUploadReceipt(String objectId, String chunkId, String workerId) {
        if (pendingUploads.contains(objectId)) {
            uploadReceipts.computeIfAbsent(objectId, id -> new HashSet<>()).add(chunkId + "@" + workerId);
        }
    }

    public boolean hasUploadReceipt(String objectId, String chunkId, String workerId) {
        Set<String> receipts = uploadReceipts.get(objectId);
        return receipts != null && receipts.contains(chunkId + "@" + workerId);
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
     * Records a copy. Refused (returns false) if the worker is unknown or DEAD, if the worker has
     * restarted since {@code expectBoot} was observed, or if the chunk is not known: a copy on a
     * worker that died or restarted after it was written cannot be trusted to exist.
     */
    public boolean addReplica(String chunkId, String workerId, boolean confirmed, ReplicaState state, String expectBoot) {
        WorkerInfo worker = workers.get(workerId);
        if (worker == null || worker.isDead() || !chunks.containsKey(chunkId)) {
            return false;
        }
        if (expectBoot != null && !expectBoot.equals(worker.bootId)) {
            return false;
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
        replica.confirmed |= confirmed;
        return true;
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
        protect(chunkId);
        return true;
    }

    /**
     * Keeps invariant I6 true at every instant: if a chunk has fewer than RF live copies, every
     * DELETE for it (other than object/orphan deletions) that has not been handed to a worker yet
     * is cancelled, and an EXCESS copy whose DELETE was cancelled counts as OK again. A DELETE
     * that was already delivered cannot be called back (invariant I5).
     *
     * @return number of EXCESS copies turned back to OK
     */
    public int protect(String chunkId) {
        if (!chunks.containsKey(chunkId) || live(chunkId).size() >= properties.replicationFactor()) {
            return 0;
        }
        int restored = 0;
        for (Map.Entry<String, Replica> entry : replicasOf(chunkId).entrySet()) {
            Replica replica = entry.getValue();
            if (replica.deleteDelivered || replica.deleteQueuedAt == 0) {
                continue;
            }
            if (replica.deleteCmdId != null) {
                cancelCommand(entry.getKey(), replica.deleteCmdId);
            }
            replica.deleteCmdId = null;
            replica.deleteQueuedAt = 0;
            if (replica.state == ReplicaState.EXCESS) {
                replica.state = ReplicaState.OK;
                restored++;
                record("restored", chunkId, entry.getKey(), null,
                        "a live copy was lost; the surplus copy's undelivered DELETE was cancelled");
            }
        }
        return restored;
    }

    /** Queues a DELETE for a surplus or corrupt copy and remembers when, for the retry rule. */
    public void sendDelete(String workerId, String chunkId, String reason) {
        Replica replica = replica(chunkId, workerId);
        if (replica == null) {
            return;
        }
        Command command = queueCommand(workerId, InternalApi.COMMAND_DELETE, chunkId, null, null, reason);
        if (command == null) {
            return;
        }
        replica.deleteCmdId = command.cmdId();
        replica.deleteQueuedAt = now();
        // delete_delivered stays true once true: a delivered DELETE may still run (I5).
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

    // ---- pending copies -------------------------------------------------------------------

    public Map<String, List<PendingCopy>> pending() {
        return pending;
    }

    public List<PendingCopy> pendingOf(String chunkId) {
        return pending.getOrDefault(chunkId, Collections.emptyList());
    }

    public void addPending(String chunkId, String source, String target, String cmdId) {
        long now = now();
        pending.computeIfAbsent(chunkId, id -> new ArrayList<>())
                .add(new PendingCopy(source, target, now + properties.pendingTimeout().toNanos(), cmdId, now));
    }

    public PendingCopy removePending(String chunkId, String target) {
        List<PendingCopy> list = pending.get(chunkId);
        if (list == null) {
            return null;
        }
        PendingCopy removed = null;
        for (Iterator<PendingCopy> it = list.iterator(); it.hasNext(); ) {
            PendingCopy copy = it.next();
            if (copy.target().equals(target)) {
                it.remove();
                removed = copy;
            }
        }
        if (list.isEmpty()) {
            pending.remove(chunkId);
        }
        return removed;
    }

    public int dropPendingForWorker(String workerId, boolean asSource, boolean asTarget) {
        int dropped = 0;
        for (Iterator<Map.Entry<String, List<PendingCopy>>> it = pending.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, List<PendingCopy>> entry = it.next();
            List<PendingCopy> list = entry.getValue();
            for (Iterator<PendingCopy> copies = list.iterator(); copies.hasNext(); ) {
                PendingCopy copy = copies.next();
                if ((asSource && copy.source().equals(workerId)) || (asTarget && copy.target().equals(workerId))) {
                    copies.remove();
                    dropped++;
                }
            }
            if (list.isEmpty()) {
                it.remove();
            }
        }
        return dropped;
    }

    public int pendingFrom(String workerId) {
        int count = 0;
        for (List<PendingCopy> list : pending.values()) {
            for (PendingCopy copy : list) {
                if (copy.source().equals(workerId)) {
                    count++;
                }
            }
        }
        return count;
    }

    public int pendingTo(String workerId) {
        int count = 0;
        for (List<PendingCopy> list : pending.values()) {
            for (PendingCopy copy : list) {
                if (copy.target().equals(workerId)) {
                    count++;
                }
            }
        }
        return count;
    }

    public int pendingTotal() {
        return pending.values().stream().mapToInt(List::size).sum();
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
     * Queues a command for a worker. Identical commands (type, chunk, target) are queued once.
     * The queue is capped: when full, the oldest DELETE is dropped first (it is re-planned by the
     * monitor), then the oldest command of any kind.
     *
     * @return the queued (or the already queued identical) command, with its id and expiry
     */
    public Command queueCommand(String workerId, String type, String chunkId, String checksum, String targetUrl,
                                String reason) {
        WorkerInfo worker = workers.get(workerId);
        if (worker == null) {
            return null;
        }
        for (Command existing : worker.commands) {
            if (existing.type().equals(type) && existing.chunkId().equals(chunkId)
                    && java.util.Objects.equals(existing.targetUrl(), targetUrl)) {
                return existing;
            }
        }
        while (worker.commands.size() >= properties.maxCommandsPerWorker()) {
            Command victim = null;
            for (Command candidate : worker.commands) {
                if (InternalApi.COMMAND_DELETE.equals(candidate.type())) {
                    victim = candidate;
                    break;
                }
            }
            if (victim == null) {
                victim = worker.commands.peekFirst();
            }
            worker.commands.remove(victim);
            Replica replica = replica(victim.chunkId(), workerId);
            if (replica != null && victim.cmdId().equals(replica.deleteCmdId) && !replica.deleteDelivered) {
                replica.deleteQueuedAt = 0;
                replica.deleteCmdId = null;
            }
            increment("commands_dropped");
        }
        Command command = new Command(UUID.randomUUID().toString(), type, chunkId, checksum, targetUrl, reason,
                InternalApi.nowSeconds() + properties.commandTtlSeconds());
        worker.commands.addLast(command);
        return command;
    }

    /** Removes a queued command that was not delivered yet. */
    public boolean cancelCommand(String workerId, String cmdId) {
        WorkerInfo worker = workers.get(workerId);
        return worker != null && worker.commands.removeIf(c -> c.cmdId().equals(cmdId));
    }

    /**
     * Takes the commands to hand to a worker in a heartbeat reply, dropping expired ones. A
     * DELETE handed over is marked delivered on its replica: from now on it cannot be cancelled.
     */
    public List<Command> takeCommands(WorkerInfo worker) {
        List<Command> delivered = new ArrayList<>();
        double now = InternalApi.nowSeconds();
        while (delivered.size() < MAX_COMMANDS_PER_HEARTBEAT && !worker.commands.isEmpty()) {
            Command command = worker.commands.pollFirst();
            if (command.expiresAt() < now) {
                increment("commands_expired");
                continue;
            }
            if (InternalApi.COMMAND_DELETE.equals(command.type())) {
                Replica replica = replica(command.chunkId(), worker.workerId);
                if (replica != null && command.cmdId().equals(replica.deleteCmdId)) {
                    replica.deleteDelivered = true;
                }
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
        List<String> violations = new ArrayList<>();
        int rf = properties.replicationFactor();
        // I1: every holder in BLOCKS is a known, non-DEAD worker.
        for (Map.Entry<String, Map<String, Replica>> entry : blocks.entrySet()) {
            for (String workerId : entry.getValue().keySet()) {
                WorkerInfo worker = workers.get(workerId);
                if (worker == null || worker.isDead()) {
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
                WorkerInfo source = workers.get(copy.source());
                WorkerInfo target = workers.get(copy.target());
                if (source == null || source.isDead() || target == null || target.isDead()) {
                    violations.add("I4: pending copy of " + chunkId + " " + copy.source() + "->" + copy.target() + " involves a DEAD worker");
                }
                if (replica(chunkId, copy.target()) != null) {
                    violations.add("I4: pending copy of " + chunkId + " targets " + copy.target() + " which already holds it");
                }
                if (!targets.add(copy.target())) {
                    violations.add("I8: chunk " + chunkId + " has two pending copies to " + copy.target());
                }
                perSource.merge(copy.source(), 1, Integer::sum);
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
                        && !"object_deleted".equals(command.reason()) && !"orphan".equals(command.reason())) {
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
