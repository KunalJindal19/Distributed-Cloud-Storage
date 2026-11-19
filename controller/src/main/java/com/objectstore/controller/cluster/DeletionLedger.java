package com.objectstore.controller.cluster;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.ReplicaState;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.ClusterState.WorkerState;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Every DELETE command is created here and tracked until the worker confirms it (v4 §6.1), so a
 * deletion completes without relying on full reports.
 *
 * <ul>
 *   <li><b>Entry:</b> {@code (chunk, worker) → reason, state, cmdId, deadline, attempts}; at most one
 *       per pair. Reasons: {@code excess} and {@code corrupt} (the chunk is in the namespace; the
 *       entry is mirrored in the replica's {@code deleteQueuedAt / deleteDelivered / deleteCmdId}),
 *       {@code orphan}, {@code object_deleted}, {@code upload_failed}.</li>
 *   <li><b>WAITING → QUEUED</b> when the worker is ALIVE, has reported and its command queue has
 *       room: tried when the entry is created, otherwise each monitor tick (at most CLEANUP_BUDGET
 *       entries).</li>
 *   <li><b>QUEUED → DELIVERED</b> when a heartbeat reply hands the command out; then
 *       {@code deadline = now + PENDING_TIMEOUT}.</li>
 *   <li><b>Completed</b> (removed) when that worker's {@code DELETED(chunk)} is applied.</li>
 *   <li><b>Deadline</b> passed without completion: back to WAITING (a new command when queued
 *       again), {@code attempts++}.</li>
 *   <li><b>Worker DEAD or re-registering:</b> its queue is cleared; its entries wait until it is
 *       ALIVE and reported again. Its {@code excess}/{@code corrupt} entries are dropped instead:
 *       its copies are re-learnt from its report and judged again.</li>
 *   <li><b>Cancellation</b> ({@code protect()}, a chunk going below RF): an entry that is WAITING or
 *       QUEUED can be cancelled; a DELIVERED one cannot (its copy stays EXCESS or CORRUPT, I5).</li>
 * </ul>
 *
 * Guarded by the cluster lock; part of {@link ClusterState}.
 */
public final class DeletionLedger {

    private static final Logger log = LoggerFactory.getLogger(DeletionLedger.class);

    public enum State { WAITING, QUEUED, DELIVERED }

    public static final String EXCESS = "excess";
    public static final String CORRUPT = "corrupt";
    public static final String ORPHAN = "orphan";
    public static final String OBJECT_DELETED = "object_deleted";
    public static final String UPLOAD_FAILED = "upload_failed";

    record Key(String chunkId, String workerId) {
    }

    /** One tracked deletion. */
    public static final class Entry {
        public final String chunkId;
        public final String workerId;
        public String reason;
        public State state = State.WAITING;
        public String cmdId;
        public long deadline;
        public int attempts;

        Entry(String chunkId, String workerId, String reason) {
            this.chunkId = chunkId;
            this.workerId = workerId;
            this.reason = reason;
        }

        boolean linkedToReplica() {
            return EXCESS.equals(reason) || CORRUPT.equals(reason);
        }
    }

    private final ClusterState cluster;
    private final Map<Key, Entry> entries = new HashMap<>();
    private final Map<String, Set<Key>> byWorker = new HashMap<>();
    private final Map<String, Set<String>> byChunk = new HashMap<>();
    private final Map<String, Entry> byCmd = new HashMap<>();
    private final LinkedHashSet<Key> waiting = new LinkedHashSet<>();
    private int queued;
    private int delivered;

    DeletionLedger(ClusterState cluster) {
        this.cluster = cluster;
    }

    // ---- reading ----------------------------------------------------------------------------------

    public Entry entry(String chunkId, String workerId) {
        return entries.get(new Key(chunkId, workerId));
    }

    public int size() {
        return entries.size();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    /** {waiting, queued, delivered}. */
    public Map<String, Integer> counts() {
        Map<String, Integer> counts = new java.util.LinkedHashMap<>();
        counts.put("waiting", waiting.size());
        counts.put("queued", queued);
        counts.put("delivered", delivered);
        return counts;
    }

    public List<Entry> entriesOf(String workerId) {
        List<Entry> list = new ArrayList<>();
        for (Key key : byWorker.getOrDefault(workerId, Set.of())) {
            list.add(entries.get(key));
        }
        return list;
    }

    public List<Entry> entriesFor(String chunkId) {
        List<Entry> list = new ArrayList<>();
        for (String workerId : byChunk.getOrDefault(chunkId, Set.of())) {
            list.add(entries.get(new Key(chunkId, workerId)));
        }
        return list;
    }

    public Entry byCommand(String cmdId) {
        return byCmd.get(cmdId);
    }

    // ---- creating ---------------------------------------------------------------------------------

    /**
     * Asks for {@code chunk} to be deleted from {@code worker}. If an entry exists it is kept (one
     * per pair); a namespace reason ({@code object_deleted}, {@code orphan}, {@code upload_failed})
     * replaces {@code excess}/{@code corrupt}, because the chunk is gone from the namespace.
     */
    public Entry request(String chunkId, String workerId, String reason) {
        Key key = new Key(chunkId, workerId);
        Entry entry = entries.get(key);
        if (entry != null) {
            if (entry.linkedToReplica() && !EXCESS.equals(reason) && !CORRUPT.equals(reason)) {
                entry.reason = reason;
            }
            return entry;
        }
        entry = new Entry(chunkId, workerId, reason);
        entries.put(key, entry);
        byWorker.computeIfAbsent(workerId, id -> new LinkedHashSet<>()).add(key);
        byChunk.computeIfAbsent(chunkId, id -> new LinkedHashSet<>()).add(workerId);
        waiting.add(key);
        tryQueue(entry);
        return entry;
    }

    /** WAITING → QUEUED if the worker can take it now. */
    boolean tryQueue(Entry entry) {
        if (entry.state != State.WAITING) {
            return false;
        }
        WorkerInfo worker = cluster.worker(entry.workerId);
        if (worker == null || worker.state != WorkerState.ALIVE || !worker.hasReported
                || worker.commands.size() >= cluster.maxCommandsPerWorker()) {
            return false;
        }
        Command command = cluster.enqueue(worker, InternalApi.COMMAND_DELETE, entry.chunkId, null, null, entry.reason);
        waiting.remove(new Key(entry.chunkId, entry.workerId));
        entry.state = State.QUEUED;
        entry.cmdId = command.cmdId();
        byCmd.put(command.cmdId(), entry);
        queued++;
        if (entry.linkedToReplica()) {
            Replica replica = cluster.replica(entry.chunkId, entry.workerId);
            if (replica != null) {
                replica.deleteCmdId = command.cmdId();
                replica.deleteQueuedAt = cluster.now();
            }
        }
        return true;
    }

    /** Re-checks waiting entries, oldest first, at most {@code budget}; returns how many were queued. */
    public int retryWaiting(int budget) {
        int queuedNow = 0;
        int visited = 0;
        for (Key key : new ArrayList<>(waiting)) {
            if (visited++ >= budget) {
                break;
            }
            Entry entry = entries.get(key);
            if (entry != null && tryQueue(entry)) {
                queuedNow++;
            }
        }
        return queuedNow;
    }

    /** Re-checks one worker's waiting entries (it just became able to take them). */
    public int retryWorker(String workerId) {
        int queuedNow = 0;
        for (Entry entry : entriesOf(workerId)) {
            if (tryQueue(entry)) {
                queuedNow++;
            }
        }
        return queuedNow;
    }

    // ---- the command's life ---------------------------------------------------------------------------

    /** A heartbeat reply hands the DELETE out: QUEUED → DELIVERED. It can no longer be cancelled. */
    void delivered(Command command) {
        Entry entry = byCmd.get(command.cmdId());
        if (entry == null || entry.state != State.QUEUED) {
            return;
        }
        entry.state = State.DELIVERED;
        entry.deadline = cluster.now() + cluster.pendingTimeoutNanos();
        queued--;
        delivered++;
        if (entry.linkedToReplica()) {
            Replica replica = cluster.replica(entry.chunkId, entry.workerId);
            if (replica != null && command.cmdId().equals(replica.deleteCmdId)) {
                replica.deleteDelivered = true;
            }
        }
    }

    /** The queued command was lost (expired in the queue): the entry waits again. */
    void commandLost(String cmdId) {
        Entry entry = byCmd.get(cmdId);
        if (entry != null && entry.state == State.QUEUED) {
            backToWaiting(entry);
        }
    }

    /** The worker confirmed the deletion ({@code DELETED} applied): the entry is done. */
    public void completed(String chunkId, String workerId) {
        Entry entry = entries.get(new Key(chunkId, workerId));
        if (entry != null) {
            remove(entry);
        }
    }

    /** Delivered entries past their deadline go back to WAITING ({@code attempts++}); returns how many. */
    public int expireDeadlines(long now) {
        int expired = 0;
        for (Entry entry : new ArrayList<>(entries.values())) {
            if (entry.state == State.DELIVERED && entry.deadline <= now) {
                entry.attempts++;
                log.info("[ledger] DELETE chunk={} on {} ({}) not confirmed in time; sending it again (attempt {})",
                        entry.chunkId, entry.workerId, entry.reason, entry.attempts + 1);
                backToWaiting(entry);
                expired++;
                tryQueue(entry);
            }
        }
        return expired;
    }

    /** {@code LedgerDeadline} for one entry (the pending-timer path). */
    public boolean deadline(String chunkId, String workerId, long now) {
        Entry entry = entries.get(new Key(chunkId, workerId));
        if (entry == null || entry.state != State.DELIVERED || entry.deadline > now) {
            return false;
        }
        entry.attempts++;
        backToWaiting(entry);
        tryQueue(entry);
        return true;
    }

    // ---- cancelling and pausing -------------------------------------------------------------------------

    /** Cancels a WAITING or QUEUED entry (removing its command); false if DELIVERED or absent. */
    public boolean cancel(String chunkId, String workerId) {
        Entry entry = entries.get(new Key(chunkId, workerId));
        if (entry == null || entry.state == State.DELIVERED) {
            return false;
        }
        if (entry.state == State.QUEUED) {
            WorkerInfo worker = cluster.worker(workerId);
            if (worker != null) {
                worker.commands.remove(entry.cmdId);
            }
        }
        if (entry.linkedToReplica()) {
            Replica replica = cluster.replica(chunkId, workerId);
            if (replica != null && !replica.deleteDelivered) {
                replica.deleteCmdId = null;
                replica.deleteQueuedAt = 0;
            }
        }
        remove(entry);
        return true;
    }

    /**
     * The worker is DEAD or registers again: its command queue was cleared. Its namespace
     * deletions wait until it is ALIVE and reported again; its {@code excess}/{@code corrupt}
     * entries are dropped (its copies are judged again after its report).
     */
    public void pause(String workerId) {
        for (Entry entry : entriesOf(workerId)) {
            if (entry.linkedToReplica()) {
                remove(entry);
            } else if (entry.state != State.WAITING) {
                backToWaiting(entry);
            }
        }
    }

    /** The chunk left the namespace: its excess/corrupt entries become namespace deletions. */
    public void chunkRemoved(String chunkId, String reason) {
        for (Entry entry : entriesFor(chunkId)) {
            if (entry.linkedToReplica()) {
                entry.reason = reason;
            }
        }
    }

    private void backToWaiting(Entry entry) {
        if (entry.state == State.QUEUED) {
            queued--;
        } else if (entry.state == State.DELIVERED) {
            delivered--;
        }
        if (entry.cmdId != null) {
            byCmd.remove(entry.cmdId);
        }
        entry.cmdId = null;
        entry.state = State.WAITING;
        waiting.add(new Key(entry.chunkId, entry.workerId));
    }

    private void remove(Entry entry) {
        Key key = new Key(entry.chunkId, entry.workerId);
        entries.remove(key);
        waiting.remove(key);
        if (entry.state == State.QUEUED) {
            queued--;
        } else if (entry.state == State.DELIVERED) {
            delivered--;
        }
        if (entry.cmdId != null) {
            byCmd.remove(entry.cmdId);
        }
        Set<Key> ofWorker = byWorker.get(entry.workerId);
        if (ofWorker != null) {
            ofWorker.remove(key);
            if (ofWorker.isEmpty()) {
                byWorker.remove(entry.workerId);
            }
        }
        Set<String> ofChunk = byChunk.get(entry.chunkId);
        if (ofChunk != null) {
            ofChunk.remove(entry.workerId);
            if (ofChunk.isEmpty()) {
                byChunk.remove(entry.chunkId);
            }
        }
    }

    // ---- invariants -----------------------------------------------------------------------------------

    /** I12 (§8): one entry per pair; every queued DELETE has a QUEUED entry with its cmdId; excess/corrupt entries point to such a copy or none. */
    List<String> check() {
        List<String> violations = new ArrayList<>();
        Set<String> queuedIds = new HashSet<>();
        for (WorkerInfo worker : cluster.workers().values()) {
            for (Command command : worker.commands) {
                if (!InternalApi.COMMAND_DELETE.equals(command.type())) {
                    continue;
                }
                queuedIds.add(command.cmdId());
                Entry entry = byCmd.get(command.cmdId());
                if (entry == null || entry.state != State.QUEUED || !entry.workerId.equals(worker.workerId)) {
                    violations.add("I12: DELETE " + command.chunkId() + " queued on " + worker.workerId
                            + " has no QUEUED ledger entry with its cmd_id");
                }
            }
        }
        int countedWaiting = 0;
        int countedQueued = 0;
        int countedDelivered = 0;
        for (Entry entry : entries.values()) {
            switch (entry.state) {
                case WAITING -> countedWaiting++;
                case QUEUED -> {
                    countedQueued++;
                    if (!queuedIds.contains(entry.cmdId)) {
                        violations.add("I12: QUEUED ledger entry " + entry.chunkId + "@" + entry.workerId + " has no command");
                    }
                }
                case DELIVERED -> countedDelivered++;
            }
            if (entry.linkedToReplica()) {
                Replica replica = cluster.replica(entry.chunkId, entry.workerId);
                if (replica != null && replica.state != ReplicaState.EXCESS && replica.state != ReplicaState.CORRUPT) {
                    violations.add("I12: " + entry.reason + " ledger entry " + entry.chunkId + "@" + entry.workerId
                            + " points to a " + replica.state + " copy");
                }
            }
        }
        if (countedWaiting != waiting.size() || countedQueued != queued || countedDelivered != delivered) {
            violations.add("I12: ledger counters differ from the entries");
        }
        return violations;
    }

    /** For logs. */
    @Override
    public String toString() {
        return "ledger" + counts();
    }

    Iterator<Entry> iterator() {
        return entries.values().iterator();
    }
}
