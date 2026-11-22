package com.objectstore.controller.cluster;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.ClusterState.WorkerState;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pending operations (v5 §3.3): the controller's desired state for every (chunk, worker) pair that
 * is not yet true. It replaces v4's pending copies and deletion ledger.
 *
 * <ul>
 *   <li><b>At most one op per pair</b>: {@code INSERT} ("the worker should hold this chunk at version
 *       v": {@code REPAIR}, a REPLICATE from a source, or {@code UPLOAD}, the controller's own PUT) or
 *       {@code DELETE} ("the worker should not hold it, as of version v": {@code EXCESS},
 *       {@code CORRUPT} (quarantine), {@code OBJECT_DELETED}, {@code UPLOAD_FAILED}, {@code ORPHAN}).</li>
 *   <li><b>Versions.</b> A new desired state gets a fresh version; resending the same op reuses it.
 *       Superseding an op (DELETE replaces INSERT, a stronger DELETE replaces a weaker one) gives it a
 *       newer version and removes its command if it is still queued.</li>
 *   <li><b>{@link #ensure} is idempotent</b>: nothing happens if an equal or stronger op exists, so a
 *       retried task never creates a second op. DELETE precedence: {@code OBJECT_DELETED} =
 *       {@code UPLOAD_FAILED} = {@code ORPHAN} > {@code CORRUPT} > {@code EXCESS}.</li>
 *   <li><b>Guards</b>: no op for an unknown or DEAD worker (tasks of a stale epoch are dropped before
 *       they get here).</li>
 *   <li><b>States</b>: {@code WAITING} (no command queued), {@code QUEUED}, {@code SENT}. An
 *       unanswered SENT op goes back to WAITING with the same version after
 *       {@code min(OP_RESEND_INITIAL · 2^attempts, OP_RESEND_MAX)} (the {@link ResendTimer}). INSERT
 *       ops are dropped after OP_MAX_INSERT_ATTEMPTS (the planner re-plans); DELETE ops never are.</li>
 *   <li><b>Indexes</b> by pair, chunk, worker, source and command id, and counters by reason and
 *       state, which {@link #check()} recomputes from scratch.</li>
 * </ul>
 *
 * Guarded by the cluster lock; part of {@link ClusterState}.
 */
public final class PendingOps {

    private static final Logger log = LoggerFactory.getLogger(PendingOps.class);

    public enum Kind { INSERT, DELETE }

    public enum Reason {
        REPAIR(Kind.INSERT, 0, "repair"),
        UPLOAD(Kind.INSERT, 0, "upload"),
        EXCESS(Kind.DELETE, 1, InternalApi.REASON_EXCESS),
        CORRUPT(Kind.DELETE, 2, InternalApi.REASON_CORRUPT),
        ORPHAN(Kind.DELETE, 3, InternalApi.REASON_ORPHAN),
        OBJECT_DELETED(Kind.DELETE, 3, InternalApi.REASON_OBJECT_DELETED),
        UPLOAD_FAILED(Kind.DELETE, 3, InternalApi.REASON_UPLOAD_FAILED);

        public final Kind kind;
        final int strength;
        public final String wire;

        Reason(Kind kind, int strength, String wire) {
            this.kind = kind;
            this.strength = strength;
            this.wire = wire;
        }
    }

    public enum State { WAITING, QUEUED, SENT }

    /** A survivor an EXCESS delete relies on: the worker and the copy generation that was verified. */
    public record Survivor(String workerId, long gen) {
    }

    /** One pending operation. {@code workerId} is the worker it concerns (the target of an INSERT). */
    public static final class Op {
        public final String chunkId;
        public final String workerId;
        Reason reason;
        long version;
        State state = State.WAITING;
        String source;                  // REPAIR: who sends the copy (null = to be chosen)
        String cmdId;                   // the queued or handed-out command
        int attempts;
        long deadline;                  // SENT: when it is resent (monotonic nanos)
        long sentAt;
        final long createdAt;
        List<Survivor> survivors = List.of();

        Op(String chunkId, String workerId, Reason reason, long version, long now) {
            this.chunkId = chunkId;
            this.workerId = workerId;
            this.reason = reason;
            this.version = version;
            this.createdAt = now;
        }

        public Reason reason() {
            return reason;
        }

        public Kind kind() {
            return reason.kind;
        }

        public long version() {
            return version;
        }

        public State state() {
            return state;
        }

        public String source() {
            return source;
        }

        public String cmdId() {
            return cmdId;
        }

        public int attempts() {
            return attempts;
        }

        public long createdAt() {
            return createdAt;
        }

        public List<Survivor> survivors() {
            return survivors;
        }

        @Override
        public String toString() {
            return reason + "(" + chunkId + "@" + workerId + " v" + version + " " + state
                    + (source == null ? "" : " from " + source) + ")";
        }
    }

    record Key(String chunkId, String workerId) {
    }

    private final ClusterState cluster;
    private final Map<Key, Op> ops = new HashMap<>();
    private final Map<String, Set<String>> byChunk = new HashMap<>();
    private final Map<String, Set<String>> byWorker = new HashMap<>();
    private final Map<String, Set<Key>> bySource = new HashMap<>();
    private final Map<String, Op> byCmd = new HashMap<>();
    private final LinkedHashSet<Key> waiting = new LinkedHashSet<>();
    /** REPAIR ops with this source that are QUEUED or SENT: the source's busy streams. */
    private final Map<String, Integer> streams = new HashMap<>();
    private final EnumMap<Reason, Integer> perReason = new EnumMap<>(Reason.class);
    private final EnumMap<State, Integer> perState = new EnumMap<>(State.class);

    PendingOps(ClusterState cluster) {
        this.cluster = cluster;
    }

    // ---- reading ----------------------------------------------------------------------------------

    public Op get(String chunkId, String workerId) {
        return ops.get(new Key(chunkId, workerId));
    }

    public int size() {
        return ops.size();
    }

    public boolean isEmpty() {
        return ops.isEmpty();
    }

    /** Every op on a chunk. */
    public List<Op> opsFor(String chunkId) {
        Set<String> workers = byChunk.get(chunkId);
        if (workers == null) {
            return List.of();
        }
        List<Op> list = new ArrayList<>(workers.size());
        for (String workerId : workers) {
            list.add(ops.get(new Key(chunkId, workerId)));
        }
        return list;
    }

    public int countFor(String chunkId) {
        Set<String> workers = byChunk.get(chunkId);
        return workers == null ? 0 : workers.size();
    }

    /** Every op that concerns a worker (as the target or the holder to delete from). */
    public List<Op> opsOn(String workerId) {
        Set<String> chunks = byWorker.get(workerId);
        if (chunks == null) {
            return List.of();
        }
        List<Op> list = new ArrayList<>(chunks.size());
        for (String chunkId : chunks) {
            list.add(ops.get(new Key(chunkId, workerId)));
        }
        return list;
    }

    public Op byCommand(String cmdId) {
        return cmdId == null ? null : byCmd.get(cmdId);
    }

    /** The op on this chunk with exactly this version, or null. */
    public Op find(String chunkId, long version) {
        for (Op op : opsFor(chunkId)) {
            if (op.version == version) {
                return op;
            }
        }
        return null;
    }

    /** A DELETE op is pending on the pair. */
    public boolean hasDelete(String chunkId, String workerId) {
        Op op = get(chunkId, workerId);
        return op != null && op.kind() == Kind.DELETE;
    }

    /** The reason of the pair's DELETE op, or null. */
    public Reason deleteReason(String chunkId, String workerId) {
        Op op = get(chunkId, workerId);
        return op != null && op.kind() == Kind.DELETE ? op.reason : null;
    }

    /** REPAIR copies this worker is sending (queued or handed out): its busy streams. */
    public int streamsOf(String workerId) {
        return streams.getOrDefault(workerId, 0);
    }

    /** INSERT ops targeting this worker. */
    public int insertsTo(String workerId) {
        int count = 0;
        for (Op op : opsOn(workerId)) {
            if (op.kind() == Kind.INSERT) {
                count++;
            }
        }
        return count;
    }

    public int count(Reason reason) {
        return perReason.getOrDefault(reason, 0);
    }

    public int count(State state) {
        return perState.getOrDefault(state, 0);
    }

    public int inserts() {
        return count(Reason.REPAIR) + count(Reason.UPLOAD);
    }

    /** {reason: count} for every reason, and {state: count} for every state. */
    public Map<String, Map<String, Integer>> counts() {
        Map<String, Integer> reasons = new LinkedHashMap<>();
        for (Reason reason : Reason.values()) {
            reasons.put(reason.name().toLowerCase(java.util.Locale.ROOT), count(reason));
        }
        Map<String, Integer> states = new LinkedHashMap<>();
        for (State state : State.values()) {
            states.put(state.name().toLowerCase(java.util.Locale.ROOT), count(state));
        }
        Map<String, Map<String, Integer>> all = new LinkedHashMap<>();
        all.put("by_reason", reasons);
        all.put("by_state", states);
        return all;
    }

    /** DELETE ops by state, under the v4 names {waiting, queued, delivered}. */
    public Map<String, Integer> deleteCounts() {
        int waitingDeletes = 0;
        int queuedDeletes = 0;
        int sentDeletes = 0;
        for (Op op : ops.values()) {
            if (op.kind() != Kind.DELETE) {
                continue;
            }
            switch (op.state) {
                case WAITING -> waitingDeletes++;
                case QUEUED -> queuedDeletes++;
                case SENT -> sentDeletes++;
            }
        }
        Map<String, Integer> counts = new LinkedHashMap<>();
        counts.put("waiting", waitingDeletes);
        counts.put("queued", queuedDeletes);
        counts.put("delivered", sentDeletes);
        return counts;
    }

    // ---- creating and superseding ---------------------------------------------------------------------

    /**
     * Makes sure an op for this desired state exists (v5 §3.3) and returns it; null if the worker is
     * unknown or DEAD. An existing equal or stronger op is returned unchanged; a weaker one is
     * superseded (fresh version, its queued command removed). A new or superseded DELETE is queued at
     * once if its worker can take it. An INSERT is never created over an existing op. EXCESS deletes
     * are created with {@link #setDelete}, which records their survivors first.
     */
    public Op ensure(String chunkId, String workerId, Reason reason) {
        WorkerInfo worker = cluster.worker(workerId);
        if (worker == null || worker.isDead()) {
            return null;
        }
        Op existing = get(chunkId, workerId);
        if (existing != null) {
            if (reason.kind == Kind.INSERT
                    || (existing.kind() == Kind.DELETE && existing.reason.strength >= reason.strength)) {
                return existing;
            }
            supersede(existing, reason);
            tryQueue(existing);
            return existing;
        }
        Op op = new Op(chunkId, workerId, reason, cluster.versions().next(), cluster.now());
        Key key = new Key(chunkId, workerId);
        ops.put(key, op);
        byChunk.computeIfAbsent(chunkId, id -> new LinkedHashSet<>()).add(workerId);
        byWorker.computeIfAbsent(workerId, id -> new LinkedHashSet<>()).add(chunkId);
        perReason.merge(reason, 1, Integer::sum);
        perState.merge(State.WAITING, 1, Integer::sum);
        waiting.add(key);
        cluster.markDirty(chunkId);
        if (reason.kind == Kind.DELETE) {
            tryQueue(op);
        }
        return op;
    }

    /**
     * An UPLOAD op (v5 §7): the controller's own PUT at {@code version} succeeded but its RECEIVED
     * has not been applied yet. It is SENT from the start; if the RECEIVED never comes, the resend
     * turns it into a REPAIR at the same version.
     */
    public Op upload(String chunkId, String workerId, long version) {
        WorkerInfo worker = cluster.worker(workerId);
        if (worker == null || worker.isDead() || get(chunkId, workerId) != null) {
            return get(chunkId, workerId);
        }
        Op op = new Op(chunkId, workerId, Reason.UPLOAD, version, cluster.now());
        ops.put(new Key(chunkId, workerId), op);
        byChunk.computeIfAbsent(chunkId, id -> new LinkedHashSet<>()).add(workerId);
        byWorker.computeIfAbsent(workerId, id -> new LinkedHashSet<>()).add(chunkId);
        perReason.merge(Reason.UPLOAD, 1, Integer::sum);
        perState.merge(State.SENT, 1, Integer::sum);
        op.state = State.SENT;
        arm(op);
        cluster.markDirty(chunkId);
        return op;
    }

    private void supersede(Op op, Reason reason) {
        withdrawCommand(op);
        setState(op, State.WAITING);
        setSource(op, null);
        perReason.merge(op.reason, -1, Integer::sum);
        perReason.merge(reason, 1, Integer::sum);
        Reason old = op.reason;
        op.reason = reason;
        op.version = cluster.versions().next();
        op.attempts = 0;
        op.survivors = List.of();
        cluster.markDirty(op.chunkId);
        log.info("[ops] {} on {} superseded by {} (v{})", old, op.chunkId + "@" + op.workerId, reason, op.version);
    }

    /** The same kind and reason again, with a fresh version ({@code CMD_FAILED(stale)}, v5 §3.4). */
    public void reissue(Op op) {
        withdrawCommand(op);
        setState(op, State.WAITING);
        op.version = cluster.versions().next();
        op.attempts = 0;
        cluster.markDirty(op.chunkId);
        tryQueue(op);
    }

    /**
     * {@code setDelete(EXCESS)} (v5 §6.4): a DELETE EXCESS of the victim's copy that relies on these
     * survivors, which are recorded before the op is queued (its validity is checked at creation,
     * when it is queued and immediately before hand-out). Null if any op is pending on the pair.
     */
    public Op setDelete(String chunkId, String victim, List<Survivor> survivors) {
        WorkerInfo worker = cluster.worker(victim);
        if (worker == null || worker.isDead() || get(chunkId, victim) != null) {
            return null;
        }
        Op op = new Op(chunkId, victim, Reason.EXCESS, cluster.versions().next(), cluster.now());
        op.survivors = List.copyOf(survivors);
        Key key = new Key(chunkId, victim);
        ops.put(key, op);
        byChunk.computeIfAbsent(chunkId, id -> new LinkedHashSet<>()).add(victim);
        byWorker.computeIfAbsent(victim, id -> new LinkedHashSet<>()).add(chunkId);
        perReason.merge(Reason.EXCESS, 1, Integer::sum);
        perState.merge(State.WAITING, 1, Integer::sum);
        waiting.add(key);
        cluster.markDirty(chunkId);
        if (!cluster.survivorsValid(op)) {
            drop(op, "its survivors are not verified");
            return null;
        }
        tryQueue(op);
        return op;
    }

    // ---- queuing and handing out ----------------------------------------------------------------------

    /**
     * WAITING → QUEUED if the op can go out now. DELETE: its worker is ALIVE, reported, not
     * resyncing and has room (an EXCESS also needs valid survivors, or it is dropped). REPAIR: it has
     * a source that is ALIVE, reported, holds a copy, has a free stream and room, and the target is
     * ALIVE, reported and not resyncing. UPLOAD ops are never queued.
     */
    public boolean tryQueue(Op op) {
        if (op.state != State.WAITING || ops.get(new Key(op.chunkId, op.workerId)) != op) {
            return false;
        }
        WorkerInfo target = cluster.worker(op.workerId);
        if (target == null || target.isDead()) {
            return false;
        }
        Command command;
        if (op.kind() == Kind.DELETE) {
            if (!cluster.canTakeCommands(target)) {
                return false;
            }
            if (op.reason == Reason.EXCESS && !cluster.survivorsValid(op)) {
                drop(op, "its survivors are no longer verified");
                return false;
            }
            command = cluster.enqueue(target, InternalApi.COMMAND_DELETE, op.chunkId, null, null, op.reason.wire,
                    op.version);
        } else {
            if (op.reason != Reason.REPAIR || op.source == null) {
                return false;
            }
            WorkerInfo source = cluster.worker(op.source);
            ClusterState.ChunkInfo chunk = cluster.chunk(op.chunkId);
            if (chunk == null || source == null || source.state != WorkerState.ALIVE || !source.hasReported
                    || cluster.replica(op.chunkId, op.source) == null || hasDelete(op.chunkId, op.source)
                    || source.commands.size() >= cluster.maxCommandsPerWorker()
                    || streamsOf(op.source) >= cluster.maxStreams()
                    || target.state != WorkerState.ALIVE || !target.hasReported || target.resyncing) {
                return false;
            }
            command = cluster.enqueue(source, InternalApi.COMMAND_REPLICATE, op.chunkId, chunk.checksum(), target.url,
                    null, op.version);
            streams.merge(op.source, 1, Integer::sum);
        }
        waiting.remove(new Key(op.chunkId, op.workerId));
        setState(op, State.QUEUED);
        op.cmdId = command.cmdId();
        byCmd.put(command.cmdId(), op);
        return true;
    }

    /** A heartbeat reply hands the op's command out: QUEUED → SENT, and the resend timer is armed. */
    void delivered(Op op) {
        if (op.state != State.QUEUED) {
            return;
        }
        setState(op, State.SENT);
        op.sentAt = cluster.now();
        arm(op);
    }

    private void arm(Op op) {
        op.deadline = cluster.now() + backoffNanos(op.attempts);
        cluster.deadlines().opDeadline(op.chunkId, op.workerId, op.version, op.deadline);
    }

    /** {@code min(OP_RESEND_INITIAL · 2^attempts, OP_RESEND_MAX)}, the exponent capped so it cannot overflow. */
    long backoffNanos(int attempts) {
        long initial = cluster.properties().opResendInitial().toNanos();
        long max = cluster.properties().opResendMax().toNanos();
        int exponent = Math.min(Math.max(0, attempts), 30);
        double value = (double) initial * (1L << exponent);
        return (long) Math.min(value, max);
    }

    /**
     * The resend deadline of an op passed (a writer task). If the op is still SENT with this version:
     * an op targeting a resyncing worker is re-armed without counting an attempt; an UPLOAD becomes a
     * REPAIR at the same version; an INSERT past OP_MAX_INSERT_ATTEMPTS is dropped (the target is
     * avoided for a while and the planner re-plans); anything else goes back to WAITING, same version.
     *
     * @return true if something happened
     */
    public boolean deadline(String chunkId, String workerId, long version, long now) {
        Op op = get(chunkId, workerId);
        if (op == null || op.version != version || op.state != State.SENT || op.deadline > now) {
            return false;
        }
        WorkerInfo target = cluster.worker(workerId);
        if (target != null && target.resyncing) {
            arm(op);
            return true;
        }
        if (op.reason == Reason.UPLOAD) {
            perReason.merge(Reason.UPLOAD, -1, Integer::sum);
            perReason.merge(Reason.REPAIR, 1, Integer::sum);
            op.reason = Reason.REPAIR;
            toWaiting(op);
            cluster.markDirty(chunkId);
            cluster.record("timeout", chunkId, null, workerId, "upload copy not confirmed in time; repaired at the same version");
            return true;
        }
        op.attempts++;
        if (op.kind() == Kind.INSERT && op.attempts >= cluster.properties().opMaxInsertAttempts()) {
            cluster.recordFailure(chunkId, workerId);
            cluster.record("timeout", chunkId, op.source, workerId, "copy not confirmed after " + op.attempts
                    + " attempts; dropped, the planner chooses again");
            drop(op, "no answer after " + op.attempts + " attempts");
            return true;
        }
        cluster.increment("ops_resent");
        log.info("[ops] {} not answered in time; sending it again (attempt {})", op, op.attempts + 1);
        toWaiting(op);
        tryQueue(op);
        return true;
    }

    /**
     * A command taken for direct delivery that certainly did not reach the worker (the connection
     * failed, or the worker refused it without acting): the op goes back to WAITING at the same
     * version and is queued again, so the worker's next heartbeat reply carries it. Nothing happens
     * if the op has meanwhile ended, changed version or moved on.
     */
    public void undelivered(String chunkId, String workerId, long version) {
        Op op = get(chunkId, workerId);
        if (op == null || op.version != version || op.state != State.SENT) {
            return;
        }
        toWaiting(op);
        tryQueue(op);
    }

    /** Back to WAITING with the same version: its queued command (if any) is withdrawn. */
    public void toWaiting(Op op) {
        if (op.state == State.WAITING) {
            return;
        }
        withdrawCommand(op);
        setState(op, State.WAITING);
        waiting.add(new Key(op.chunkId, op.workerId));
    }

    /** A REPAIR whose source failed or went away: same version, another source is chosen later. */
    public void resource(Op op) {
        toWaiting(op);
        setSource(op, null);
    }

    /** Re-sources every REPAIR op that uses this worker as its source (it is DEAD or a new process). */
    public int resourceFrom(String workerId) {
        Set<Key> keys = bySource.get(workerId);
        if (keys == null) {
            return 0;
        }
        int count = 0;
        for (Key key : List.copyOf(keys)) {
            Op op = ops.get(key);
            if (op != null) {
                resource(op);
                count++;
            }
        }
        return count;
    }

    /** Assigns the source of a WAITING REPAIR op. */
    public void setSource(Op op, String source) {
        if (op.source != null) {
            Set<Key> keys = bySource.get(op.source);
            if (keys != null) {
                keys.remove(new Key(op.chunkId, op.workerId));
                if (keys.isEmpty()) {
                    bySource.remove(op.source);
                }
            }
        }
        op.source = source;
        if (source != null) {
            bySource.computeIfAbsent(source, id -> new LinkedHashSet<>()).add(new Key(op.chunkId, op.workerId));
        }
    }

    /** Removes the op's command from the queue it is in (if still queued) and frees its stream. */
    private void withdrawCommand(Op op) {
        if (op.cmdId != null) {
            if (op.state == State.QUEUED) {
                String queueOwner = op.kind() == Kind.INSERT ? op.source : op.workerId;
                WorkerInfo owner = queueOwner == null ? null : cluster.worker(queueOwner);
                if (owner != null) {
                    owner.commands.remove(op.cmdId);
                }
            }
            byCmd.remove(op.cmdId);
            op.cmdId = null;
        }
        if (op.reason == Reason.REPAIR && op.source != null && (op.state == State.QUEUED || op.state == State.SENT)) {
            streams.computeIfPresent(op.source, (w, n) -> n > 1 ? n - 1 : null);
        }
    }

    private void setState(Op op, State state) {
        if (op.state == state) {
            return;
        }
        perState.merge(op.state, -1, Integer::sum);
        perState.merge(state, 1, Integer::sum);
        Key key = new Key(op.chunkId, op.workerId);
        if (op.state == State.WAITING) {
            waiting.remove(key);
        }
        if (state == State.WAITING) {
            waiting.add(key);
        }
        op.state = state;
    }

    // ---- ending ---------------------------------------------------------------------------------------

    /** The worker's result resolved the op (v5 §3.4 completion rule). */
    public void complete(Op op) {
        remove(op);
    }

    /** Drops the op and its queued command. */
    public void drop(Op op, String why) {
        if (ops.get(new Key(op.chunkId, op.workerId)) != op) {
            return;
        }
        log.info("[ops] dropped {}: {}", op, why);
        remove(op);
    }

    /** Drops an op while its command is being taken out of the queue (the caller removes the command). */
    void dropAtHandOut(Op op, String why) {
        if (op.cmdId != null) {
            byCmd.remove(op.cmdId);
            op.cmdId = null;
        }
        drop(op, why);
    }

    /** Drops every op concerning this worker with {@code version <= maxVersion}; returns how many. */
    public int dropTargeting(String workerId, long maxVersion, String why) {
        int dropped = 0;
        for (Op op : opsOn(workerId)) {
            if (op.version <= maxVersion) {
                remove(op);
                dropped++;
            }
        }
        if (dropped > 0) {
            log.info("[ops] dropped {} ops targeting {} ({})", dropped, workerId, why);
        }
        return dropped;
    }

    private void remove(Op op) {
        Key key = new Key(op.chunkId, op.workerId);
        if (ops.remove(key) == null) {
            return;
        }
        withdrawCommand(op);
        setSource(op, null);
        waiting.remove(key);
        perReason.merge(op.reason, -1, Integer::sum);
        perState.merge(op.state, -1, Integer::sum);
        Set<String> ofChunk = byChunk.get(op.chunkId);
        if (ofChunk != null) {
            ofChunk.remove(op.workerId);
            if (ofChunk.isEmpty()) {
                byChunk.remove(op.chunkId);
            }
        }
        Set<String> ofWorker = byWorker.get(op.workerId);
        if (ofWorker != null) {
            ofWorker.remove(op.chunkId);
            if (ofWorker.isEmpty()) {
                byWorker.remove(op.workerId);
            }
        }
        cluster.markDirty(op.chunkId);
    }

    // ---- retrying -----------------------------------------------------------------------------------

    /** Up to {@code budget} WAITING ops, oldest first, rotated so the next call starts further on. */
    public List<Op> nextWaiting(int budget) {
        List<Op> batch = new ArrayList<>(Math.min(budget, waiting.size()));
        List<Key> keys = new ArrayList<>(Math.min(budget, waiting.size()));
        for (Iterator<Key> it = waiting.iterator(); it.hasNext() && keys.size() < budget; ) {
            keys.add(it.next());
        }
        for (Key key : keys) {
            waiting.remove(key);
            waiting.add(key);
            Op op = ops.get(key);
            if (op != null) {
                batch.add(op);
            }
        }
        return batch;
    }

    /** Every op, for status and tests. */
    public List<Op> all() {
        return Collections.unmodifiableList(new ArrayList<>(ops.values()));
    }

    // ---- invariants -----------------------------------------------------------------------------------

    /**
     * The op invariants: one per pair (by construction), the indexes and counters equal a
     * recomputation, every QUEUED op has its command in the right queue, every queued DELETE or
     * REPLICATE belongs to a QUEUED op, no op concerns a DEAD worker.
     */
    List<String> check() {
        List<String> violations = new ArrayList<>();
        EnumMap<Reason, Integer> reasons = new EnumMap<>(Reason.class);
        EnumMap<State, Integer> states = new EnumMap<>(State.class);
        Map<String, Integer> recountedStreams = new HashMap<>();
        Set<Key> recountedWaiting = new HashSet<>();
        for (Map.Entry<Key, Op> entry : ops.entrySet()) {
            Op op = entry.getValue();
            reasons.merge(op.reason, 1, Integer::sum);
            states.merge(op.state, 1, Integer::sum);
            if (op.state == State.WAITING) {
                recountedWaiting.add(entry.getKey());
            }
            if (!byChunk.getOrDefault(op.chunkId, Set.of()).contains(op.workerId)
                    || !byWorker.getOrDefault(op.workerId, Set.of()).contains(op.chunkId)) {
                violations.add("I12: op " + op + " is missing from the chunk or worker index");
            }
            if (op.source != null && !bySource.getOrDefault(op.source, Set.of()).contains(entry.getKey())) {
                violations.add("I12: op " + op + " is missing from the source index");
            }
            if (op.reason == Reason.REPAIR && op.source != null && op.state != State.WAITING) {
                recountedStreams.merge(op.source, 1, Integer::sum);
            }
            WorkerInfo worker = cluster.worker(op.workerId);
            if (worker == null || (worker.isDead() && !worker.purging)) {
                violations.add("I12: op " + op + " concerns an unknown or DEAD worker");
            }
            if (op.state == State.QUEUED) {
                String owner = op.kind() == Kind.INSERT ? op.source : op.workerId;
                WorkerInfo queue = owner == null ? null : cluster.worker(owner);
                boolean found = false;
                if (queue != null) {
                    for (Command command : queue.commands) {
                        if (command.cmdId().equals(op.cmdId)) {
                            found = true;
                            break;
                        }
                    }
                }
                if (!found || byCmd.get(op.cmdId) != op) {
                    violations.add("I12: QUEUED op " + op + " has no queued command");
                }
            }
        }
        for (WorkerInfo worker : cluster.workers().values()) {
            for (Command command : worker.commands) {
                if (InternalApi.COMMAND_VERIFY.equals(command.type())) {
                    continue;
                }
                Op op = byCmd.get(command.cmdId());
                if (op == null || op.state != State.QUEUED) {
                    violations.add("I12: " + command.type() + " " + command.chunkId() + " queued on " + worker.workerId
                            + " belongs to no QUEUED op");
                }
            }
        }
        for (Reason reason : Reason.values()) {
            if (!reasons.getOrDefault(reason, 0).equals(perReason.getOrDefault(reason, 0))) {
                violations.add("I12: op counter " + reason + " is " + perReason.getOrDefault(reason, 0) + ", recounted "
                        + reasons.getOrDefault(reason, 0));
            }
        }
        for (State state : State.values()) {
            if (!states.getOrDefault(state, 0).equals(perState.getOrDefault(state, 0))) {
                violations.add("I12: op state counter " + state + " differs from a recount");
            }
        }
        if (!recountedWaiting.equals(waiting)) {
            violations.add("I12: the waiting set differs from the WAITING ops");
        }
        if (!recountedStreams.equals(streams)) {
            violations.add("I12: busy streams " + streams + " differ from a recount " + recountedStreams);
        }
        return violations;
    }

    @Override
    public String toString() {
        return "ops" + perReason;
    }
}
