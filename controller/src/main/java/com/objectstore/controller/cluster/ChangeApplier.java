package com.objectstore.controller.cluster;

import com.objectstore.common.ChunkIds;
import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.ChangeType;
import com.objectstore.common.dto.ReportEntry;
import com.objectstore.controller.cluster.ClusterState.Receipt;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.ReportSession;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.PendingOps.Kind;
import com.objectstore.controller.cluster.PendingOps.Op;
import com.objectstore.controller.cluster.PendingOps.Reason;
import com.objectstore.controller.config.ControllerProperties;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Applies a worker's changes and resync segments (v5 §3.4, §5.5, §5.6). These are the bodies of the
 * writer's {@code ApplyChanges}, {@code ApplySegment} and {@code Completion} tasks: every method runs
 * under the cluster write lock and none does network or database I/O. Each carries the worker's
 * epoch: work that belongs to an earlier life of the worker is dropped.
 *
 * <p><b>Each change does two separate things, in this order.</b>
 * <ol>
 *   <li><b>The physical part, always.</b> {@code RECEIVED} adds or updates the replica (a new
 *       version gives it a new generation), unless its checksum differs from the namespace, in
 *       which case {@code ensure(DELETE, CORRUPT)} is created instead; {@code DELETED} removes the
 *       replica. Never skipped because of a version.</li>
 *   <li><b>Completion, only if</b> the version is even, at least the op's version, and the kind
 *       matches ({@code RECEIVED} ↔ INSERT, {@code DELETED} ↔ DELETE). Odd versions (the worker's own
 *       observations), older versions and the wrong kind leave the op untouched. This is what fixes
 *       the v4 double delete: an old DELETE's {@code DELETED} can no longer complete a newer op.</li>
 * </ol>
 * An even version above {@code versions.current()} is a bug: it is logged and treated as a
 * physical observation only.
 *
 * <p><b>Why a listed chunk skips older changes.</b> A listing is taken after every change up to its
 * baseline M; a change ≤ M for a chunk the open session already listed is older news, so its physical
 * part is skipped (its completion part still runs: it only ends ops whose result the version proves).
 */
@Component
public class ChangeApplier {

    private static final Logger log = LoggerFactory.getLogger(ChangeApplier.class);
    static final int ORPHAN_VALVE_MIN = 10;

    private final ClusterState cluster;
    private final ControllerProperties properties;

    public ChangeApplier(ClusterState cluster, ControllerProperties properties) {
        this.cluster = cluster;
        this.properties = properties;
    }

    /** Orphan GC is allowed once safe mode is off and ORPHAN_GC_DELAY has passed since the start. */
    public boolean gcAllowed() {
        return !cluster.isSafeMode() && cluster.now() - cluster.startedAt() >= properties.orphanGcDelay().toNanos();
    }

    private WorkerInfo current(String workerId, long epoch) {
        WorkerInfo worker = cluster.worker(workerId);
        if (worker == null || worker.epoch != epoch || worker.isDead()) {
            cluster.increment("stale_tasks_dropped");
            return null;
        }
        return worker;
    }

    // ---- changes -------------------------------------------------------------------------------

    /** The {@code ApplyChanges} task: yields at the deadline and resumes where it stopped. */
    public Task applyChangesTask(String workerId, long epoch, List<Change> run) {
        int[] cursor = {0};
        return Task.steps("ApplyChanges", deadline -> {
            cursor[0] = applyChanges(workerId, epoch, run, cursor[0], deadline);
            return cursor[0] < run.size() ? Task.Step.MORE : Task.Step.DONE;
        });
    }

    /** Applies a contiguous run of changes at once, skipping those already applied. */
    public void applyChanges(String workerId, long epoch, List<Change> run) {
        applyChanges(workerId, epoch, run, 0, Long.MAX_VALUE);
    }

    private int applyChanges(String workerId, long epoch, List<Change> run, int from, long deadline) {
        WorkerInfo worker = current(workerId, epoch);
        if (worker == null) {
            return run.size();
        }
        for (int i = from; i < run.size(); i++) {
            if (i > from && System.nanoTime() >= deadline) {
                return i;
            }
            Change change = run.get(i);
            if (change.seq() <= worker.applied) {
                continue;
            }
            try {
                apply(worker, change);
            } catch (RuntimeException e) {
                log.error("[report] {} change {} ({} {}) could not be applied", workerId, change.seq(), change.type(),
                        change.chunkId(), e);
            }
            worker.applied = change.seq();
        }
        return run.size();
    }

    private void apply(WorkerInfo worker, Change change) {
        String chunkId = change.chunkId();
        if (change.type() == null || chunkId == null || !ChunkIds.isValid(chunkId)) {
            return;
        }
        switch (change.type()) {
            case RECEIVED -> received(worker, change);
            case DELETED -> deleted(worker, change);
            case CMD_FAILED -> cmdFailed(worker, change);
        }
    }

    /** True if the open session listed this chunk and the change is older than the listing. */
    private boolean listedInOpenSession(WorkerInfo worker, String chunkId, long seq) {
        ReportSession session = worker.session;
        if (session == null || seq > session.baseline) {
            return false;
        }
        Replica replica = cluster.replica(chunkId, worker.workerId);
        if (replica != null && replica.mark == session.number) {
            return true;
        }
        Receipt receipt = cluster.receipt(chunkId, worker.workerId);
        return receipt != null && receipt.seenInSession == session.number;
    }

    /** An even version above the highest one issued is a bug: only its physical part is applied. */
    private boolean completable(WorkerInfo worker, Change change) {
        long version = change.version();
        if (!InternalApi.isControllerVersion(version)) {
            return false;                                      // a worker observation (odd) or none
        }
        if (version > cluster.versions().current()) {
            cluster.increment("future_versions");
            log.error("[report] BUG {} reported {} {} at version {}, above the highest issued {}; physical only",
                    worker.workerId, change.type(), change.chunkId(), version, cluster.versions().current());
            return false;
        }
        return true;
    }

    private void received(WorkerInfo worker, Change change) {
        String workerId = worker.workerId;
        String chunkId = change.chunkId();
        long size = change.size() == null ? -1 : change.size();
        boolean older = listedInOpenSession(worker, chunkId, change.seq());
        ClusterState.ChunkInfo known = cluster.chunk(chunkId);
        if (known == null) {
            String objectId = ChunkIds.objectIdOf(chunkId);
            if (cluster.pendingUploads().contains(objectId)) {
                if (!older) {
                    cluster.putReceipt(chunkId, workerId, worker.epoch, size, change.checksum(), change.version(), 0);
                }
            } else if (cluster.isGarbage(objectId) && gcAllowed()) {
                deleteUnknown(workerId, chunkId, cluster.garbageReason(objectId),
                        "a copy of a failed upload or a deleted object landed");
            }
            // otherwise: unknown here; the next resync and its safety valve decide
            return;
        }
        // 1. The physical part.
        if (!older) {
            boolean matches = known.checksum().equals(change.checksum()) && known.size() == size;
            if (!matches) {
                log.warn("[report] chunk={} on {} does not match the namespace (received {} expected {}); quarantine",
                        chunkId, workerId, change.checksum(), known.checksum());
                cluster.removeReplica(chunkId, workerId);
                cluster.ops().ensure(chunkId, workerId, Reason.CORRUPT);
                return;                                   // a mismatched copy completes nothing
            }
            cluster.addReplica(chunkId, workerId, change.version());
        }
        if (change.cmdId() != null && cluster.verifyAnswered(chunkId, workerId, change.cmdId())) {
            log.debug("[verify] chunk={} on {} verified", chunkId, workerId);
        }
        // 2. Completion.
        if (completable(worker, change)) {
            Op op = cluster.ops().get(chunkId, workerId);
            if (op != null && op.kind() == Kind.INSERT && change.version() >= op.version()) {
                String source = op.source();                  // completing the op releases its source
                cluster.ops().complete(op);
                if (op.reason() == Reason.REPAIR) {
                    long ms = (cluster.now() - op.createdAt()) / 1_000_000;
                    cluster.record("copied", chunkId, source, workerId, ms + " ms");
                }
            }
        }
    }

    private void deleted(WorkerInfo worker, Change change) {
        String workerId = worker.workerId;
        String chunkId = change.chunkId();
        boolean older = listedInOpenSession(worker, chunkId, change.seq());
        Op op = cluster.ops().get(chunkId, workerId);
        // 1. The physical part.
        if (!older) {
            Replica removed = cluster.removeReplica(chunkId, workerId);
            cluster.removeReceipt(chunkId, workerId);
            if (InternalApi.DELETED_CORRUPT.equals(change.detail())) {
                cluster.increment("quarantines");
                cluster.record("quarantined", chunkId, workerId, null, "the worker quarantined its copy");
            } else if (InternalApi.DELETED_MISSING.equals(change.detail())) {
                cluster.record("missing", chunkId, workerId, null, "the file is gone from the worker's disk");
            } else if (removed != null) {
                boolean excess = op != null && op.reason() == Reason.EXCESS;
                cluster.record(excess ? "trimmed" : "deleted", chunkId, workerId, null, "worker reported the copy deleted");
            }
        }
        // 2. Completion.
        if (completable(worker, change) && op != null && op.kind() == Kind.DELETE && change.version() >= op.version()) {
            cluster.ops().complete(op);
        }
    }

    /** The CMD_FAILED table (v5 §3.4): ignored unless an op on the chunk has exactly this version. */
    private void cmdFailed(WorkerInfo worker, Change change) {
        String chunkId = change.chunkId();
        Op op = cluster.ops().find(chunkId, change.version());
        String reason = change.reason() == null ? "" : change.reason();
        if (op == null) {
            cluster.increment("cmd_failed_ignored");
            return;
        }
        String reporter = worker.workerId;
        switch (reason) {
            case InternalApi.FAILED_STALE -> {
                cluster.increment("stale_rejections");
                if (!reporter.equals(op.workerId)) {
                    return;
                }
                cluster.record("stale", chunkId, null, reporter, op.reason() + " v" + op.version()
                        + " rejected by the worker's version check; reissued");
                cluster.ops().reissue(op);
            }
            case InternalApi.FAILED_BUSY, InternalApi.FAILED_IO_ERROR -> {
                cluster.ops().toWaiting(op);
                cluster.ops().tryQueue(op);
            }
            case InternalApi.FAILED_NO_LOCAL_COPY, InternalApi.FAILED_LOCAL_COPY_CORRUPT -> {
                if (op.reason() != Reason.REPAIR || !reporter.equals(op.source())) {
                    return;
                }
                cluster.record("source failed", chunkId, reporter, op.workerId, reason + "; choosing another source");
                cluster.ops().resource(op);                   // same version; the target is not blamed
            }
            case InternalApi.FAILED_TRANSFER -> {
                cluster.recordFailure(chunkId, op.workerId);
                cluster.record("failed", chunkId, op.source(), op.workerId,
                        "the source could not deliver the copy: " + (change.detail() == null ? reason : change.detail()));
                cluster.ops().drop(op, "transfer failed");
            }
            case InternalApi.FAILED_BAD_COMMAND -> {
                log.error("[report] BUG {} rejected {} as a bad command: {}", reporter, op, change.detail());
                cluster.ops().drop(op, "bad command");
            }
            default -> log.warn("[report] {} CMD_FAILED with unknown reason {} for {}", reporter, reason, op);
        }
    }

    // ---- resync segments -----------------------------------------------------------------------

    /** Applies one segment of a listing at once (tests). */
    public void applySegment(String workerId, long epoch, long sessionNumber, long baseline, boolean open,
                             long floor, int attempt, List<ReportEntry> entries) {
        applySegment(workerId, epoch, sessionNumber, baseline, open, floor, attempt, entries, 0, Long.MAX_VALUE);
    }

    /** The {@code ApplySegment} task: yields at the deadline and resumes where it stopped. */
    public Task applySegmentTask(String workerId, long epoch, long sessionNumber, long baseline, boolean open,
                                 long floor, int attempt, List<ReportEntry> entries) {
        int[] cursor = {0};
        return Task.steps("ApplySegment", deadline -> {
            cursor[0] = applySegment(workerId, epoch, sessionNumber, baseline, open && cursor[0] == 0, floor, attempt,
                    entries, cursor[0], deadline);
            return cursor[0] < entries.size() ? Task.Step.MORE : Task.Step.DONE;
        });
    }

    private int applySegment(String workerId, long epoch, long sessionNumber, long baseline, boolean open, long floor,
                             int attempt, List<ReportEntry> entries, int from, long deadline) {
        WorkerInfo worker = current(workerId, epoch);
        if (worker == null) {
            return entries.size();
        }
        if (open) {
            if (!worker.resyncing || worker.fenceFloor != floor || worker.attempt != attempt) {
                cluster.increment("stale_tasks_dropped");
                return entries.size();
            }
            cluster.openSession(worker, sessionNumber, baseline, floor, attempt);
        }
        ReportSession session = worker.session;
        if (session == null || session.closed || session.number != sessionNumber) {
            cluster.increment("stale_tasks_dropped");
            return entries.size();
        }
        for (int i = from; i < entries.size(); i++) {
            if (i > from && System.nanoTime() >= deadline) {
                return i;
            }
            ReportEntry entry = entries.get(i);
            String chunkId = entry.chunkId();
            if (chunkId == null || !ChunkIds.isValid(chunkId)) {
                continue;
            }
            session.listed++;
            try {
                listed(worker, session, entry);
            } catch (RuntimeException e) {
                log.error("[report] {} entry {} could not be applied", workerId, chunkId, e);
            }
        }
        return entries.size();
    }

    private void listed(WorkerInfo worker, ReportSession session, ReportEntry entry) {
        String workerId = worker.workerId;
        String chunkId = entry.chunkId();
        ClusterState.ChunkInfo known = cluster.chunk(chunkId);
        if (known == null) {
            String objectId = ChunkIds.objectIdOf(chunkId);
            if (cluster.pendingUploads().contains(objectId)) {
                cluster.putReceipt(chunkId, workerId, worker.epoch, entry.size(), entry.checksum(), entry.version(),
                        session.number);
            } else if (cluster.isGarbage(objectId)) {
                session.garbage.add(chunkId);
            } else {
                session.orphans.add(chunkId);
            }
            return;
        }
        if (!known.checksum().equals(entry.checksum()) || known.size() != entry.size()) {
            log.warn("[report] chunk={} on {} does not match the namespace (listed {}, expected {}); quarantine",
                    chunkId, workerId, entry.checksum(), known.checksum());
            cluster.removeReplica(chunkId, workerId);
            cluster.ops().ensure(chunkId, workerId, Reason.CORRUPT);
            return;
        }
        Replica replica = cluster.addReplica(chunkId, workerId, entry.version());
        if (replica != null) {
            replica.mark = session.number;
        }
    }

    // ---- completion (v5 §5.6) ------------------------------------------------------------------

    /** The {@code Completion} task, in slices. */
    public Task completeReportTask(String workerId, long epoch, long sessionNumber, long baseline, long floor,
                                   int attempt) {
        Completion completion = new Completion(workerId, epoch, sessionNumber, baseline, floor, attempt);
        return Task.steps("Completion", completion::step);
    }

    /** Completes a listing at once (tests). */
    public void completeReport(String workerId, long epoch, long sessionNumber, long baseline, long floor, int attempt) {
        Completion completion = new Completion(workerId, epoch, sessionNumber, baseline, floor, attempt);
        while (completion.step(Long.MAX_VALUE) == Task.Step.MORE) {
            // all steps in one go
        }
    }

    /**
     * Completing a resync, in this order: (1) re-check {@code applied ≤ baseline}; (2) drop every op
     * targeting the worker with version ≤ t0 (the fence made them unrunnable; the planner re-derives
     * what is still needed with fresh versions); (3) collect garbage and orphans with {@code ensure}
     * (after 2, so nothing just created is dropped); (4) sweep the worker's unmarked replicas; (5)
     * {@code resyncing = false}, {@code lastCompleted = (t0, attempt)}, reported. Every step re-checks
     * the epoch and the session and leaves the state consistent.
     */
    private final class Completion {
        private final String workerId;
        private final long epoch;
        private final long number;
        private final long baseline;
        private final long floor;
        private final int attempt;
        private int phase;
        private List<String> sweep;
        private int cursor;
        private int dropped;
        private int garbage;
        private int orphans;
        private int missing;

        Completion(String workerId, long epoch, long number, long baseline, long floor, int attempt) {
            this.workerId = workerId;
            this.epoch = epoch;
            this.number = number;
            this.baseline = baseline;
            this.floor = floor;
            this.attempt = attempt;
        }

        Task.Step step(long deadline) {
            WorkerInfo worker = current(workerId, epoch);
            if (worker == null) {
                return Task.Step.DONE;
            }
            ReportSession session = worker.session;
            if (session == null || session.closed || session.number != number || !worker.resyncing
                    || worker.fenceFloor != floor || worker.attempt != attempt) {
                cluster.increment("stale_tasks_dropped");
                return Task.Step.DONE;
            }
            switch (phase) {
                case 0 -> {
                    if (worker.applied > baseline) {             // cannot happen: the queue is FIFO
                        cluster.newAttempt(worker, "applied " + worker.applied + " > baseline " + baseline);
                        return Task.Step.DONE;
                    }
                    dropped = cluster.ops().dropTargeting(workerId, floor, "resync completion: below fence " + floor);
                    phase = 1;
                    return Task.Step.MORE;
                }
                case 1 -> {
                    collectGarbage(session);
                    sweep = new ArrayList<>(cluster.chunksOn(workerId));
                    phase = 2;
                    return Task.Step.MORE;
                }
                default -> {
                    int items = 0;
                    while (cursor < sweep.size()) {
                        String chunkId = sweep.get(cursor++);
                        Replica replica = cluster.replica(chunkId, workerId);
                        if (replica != null && replica.mark != number) {
                            cluster.removeReplica(chunkId, workerId);
                            missing++;
                            log.warn("[report] chunk={} not listed by {}: removed from the map", chunkId, workerId);
                        }
                        if (++items >= NamespaceTasks.STEP_ITEMS || System.nanoTime() >= deadline) {
                            if (cursor < sweep.size()) {
                                return Task.Step.MORE;
                            }
                        }
                    }
                    finish(worker, session);
                    return Task.Step.DONE;
                }
            }
        }

        private void finish(WorkerInfo worker, ReportSession session) {
            worker.resyncing = false;
            worker.lastCompletedFloor = floor;
            worker.lastCompletedAttempt = attempt;
            worker.hasReported = true;
            worker.lastReportAt = cluster.now();
            worker.nextPeriodicAt = 0;                         // the monitor draws the next periodic time
            worker.periodicResync = false;
            worker.applied = Math.max(worker.applied, baseline);
            cluster.closeSession(worker);
            cluster.markInsertTargetsDirty(workerId);
            for (Op op : cluster.ops().opsOn(workerId)) {
                cluster.ops().tryQueue(op);                    // its waiting deletions can go out now
            }
            cluster.releasePostponed(properties.postponedBudget());
            log.info("[report] {} resync fence {} attempt {} complete: listed={} missing={} garbage={} orphans={} "
                    + "ops dropped={} baseline={}", workerId, floor, attempt, session.listed, missing, garbage, orphans,
                    dropped, baseline);
        }

        private void collectGarbage(ReportSession session) {
            if (!gcAllowed()) {
                return;                       // the GC-triggered resync collects it later
            }
            for (String chunkId : session.garbage) {
                if (cluster.chunk(chunkId) == null && deleteUnknown(workerId, chunkId,
                        cluster.garbageReason(ChunkIds.objectIdOf(chunkId)), "a copy of a failed upload or a deleted object")) {
                    garbage++;
                }
            }
            // The orphan safety valve, over the whole listing: a resync that would delete much of a
            // worker's data the controller cannot account for is more likely a bug or a wrong database.
            if (session.orphans.size() > ORPHAN_VALVE_MIN
                    && (double) session.orphans.size() / Math.max(1, session.listed) > properties.orphanGcMaxFraction()) {
                log.error("[report] orphan GC refused on {}: {} of {} chunks unknown", workerId, session.orphans.size(),
                        session.listed);
                cluster.increment("orphan_gc_refused");
            } else {
                for (String chunkId : session.orphans) {
                    if (cluster.chunk(chunkId) == null && deleteUnknown(workerId, chunkId, Reason.ORPHAN,
                            "unknown chunk on disk")) {
                        orphans++;
                    }
                }
            }
        }
    }

    // ---- helpers -----------------------------------------------------------------------------

    /** A DELETE for a chunk the namespace does not know; false if a DELETE was already pending. */
    private boolean deleteUnknown(String workerId, String chunkId, Reason reason, String why) {
        Op existing = cluster.ops().get(chunkId, workerId);
        if (existing != null && existing.kind() == Kind.DELETE) {
            return false;
        }
        Op op = cluster.ops().ensure(chunkId, workerId, reason);
        if (op != null) {
            cluster.record("orphan", chunkId, workerId, null, why + "; DELETE (" + reason.wire + ") queued");
        }
        return op != null;
    }

    /** Copies from a change type to its log name (for tests and logs). */
    static String name(ChangeType type) {
        return type.name();
    }
}
