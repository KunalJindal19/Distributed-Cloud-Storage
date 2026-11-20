package com.objectstore.controller.cluster;

import com.objectstore.common.ChunkIds;
import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.ReportEntry;
import com.objectstore.controller.cluster.ClusterState.PendingCopy;
import com.objectstore.controller.cluster.ClusterState.PendingKind;
import com.objectstore.controller.cluster.ClusterState.Receipt;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.ReplicaState;
import com.objectstore.controller.cluster.ClusterState.ReportSession;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.config.ControllerProperties;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Applies a worker's changes and full-report segments to the block map (v4 §4.9). These are the
 * bodies of the writer's {@code ApplyChanges}, {@code ApplySegment} and {@code CompleteReport}
 * tasks: every method runs under the cluster write lock, and none does network or database I/O.
 *
 * <p>Each method carries the worker's epoch: work that belongs to an earlier life of the worker
 * (before a re-registration or DEAD) is dropped.
 *
 * <p><b>Why marks are only set by segments.</b> A report's listing is taken after every change up
 * to its baseline {@code M} happened, and hold-back guarantees that nothing numbered above
 * {@code M} arrives before the report completes. A replica created from a change {@code <= M} that
 * the listing did not see must have been deleted before the listing, so removing it at completion
 * (its mark is not the session's) is correct. A replica created at commit from a receipt keeps the
 * receipt's {@code seenInSession} as its mark.
 *
 * <p>For the same reason the listing is newer than any change {@code <= M}: once a segment has
 * listed a chunk (its replica or receipt carries this session's mark), a change {@code <= M} for
 * that chunk that arrives later is older news and does not change it. And a listed copy that is
 * neither bad nor mismatched is OK, also when it was CORRUPT from an older change, unless its
 * DELETE was already delivered (I5).
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

    /** Applies {@code run} from index {@code from} until done or the deadline; returns where to resume. */
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
        if (change.type() != com.objectstore.common.dto.ChangeType.CMD_FAILED && listedInOpenSession(worker, chunkId, change.seq())) {
            return; // the report's listing is newer than this change (see the class comment)
        }
        switch (change.type()) {
            case RECEIVED -> received(worker, chunkId, change.size() == null ? -1 : change.size(), change.checksum());
            case DELETED -> deleted(worker, chunkId);
            case BAD -> bad(worker, chunkId);
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

    private void received(WorkerInfo worker, String chunkId, long size, String checksum) {
        String workerId = worker.workerId;
        ClusterState.ChunkInfo known = cluster.chunk(chunkId);
        if (known == null) {
            String objectId = ChunkIds.objectIdOf(chunkId);
            if (cluster.pendingUploads().contains(objectId)) {
                cluster.putReceipt(chunkId, workerId, worker.epoch, size, checksum, 0);
            } else if (cluster.isGarbage(objectId) && gcAllowed()) {
                deleteUnknown(workerId, chunkId, "chunk of a failed upload or a deleted object landed; DELETE queued");
            }
            // otherwise: unknown here; the full report and its safety valve decide
            return;
        }
        boolean matches = known.checksum().equals(checksum) && known.size() == size;
        Replica replica = cluster.replica(chunkId, workerId);
        if (!matches) {
            if (replica == null || replica.state != ReplicaState.CORRUPT) {
                log.warn("[report] chunk={} on {} CORRUPT (received {} expected {})", chunkId, workerId, checksum,
                        known.checksum());
            }
            setCorrupt(chunkId, workerId);
        } else if (replica == null) {
            cluster.addReplica(chunkId, workerId, ReplicaState.OK);
        } else if (replica.state == ReplicaState.CORRUPT) {
            makeOk(chunkId, workerId, replica, "freshly verified bytes received");
        }
        // An EXCESS copy stays EXCESS.
        completePending(chunkId, workerId);
    }

    private void deleted(WorkerInfo worker, String chunkId) {
        String workerId = worker.workerId;
        Replica removed = cluster.removeReplica(chunkId, workerId);   // runs protect()
        if (removed != null) {
            String event = switch (removed.state) {
                case EXCESS -> "trimmed";
                case CORRUPT -> "invalidated";
                case OK -> "deleted";
            };
            cluster.record(event, chunkId, workerId, null, "worker reported the copy deleted");
        }
        cluster.ledger().completed(chunkId, workerId);
        cluster.removeReceipt(chunkId, workerId);
    }

    private void bad(WorkerInfo worker, String chunkId) {
        String workerId = worker.workerId;
        cluster.removeReceipt(chunkId, workerId);
        if (cluster.chunk(chunkId) == null) {
            return;
        }
        Replica replica = cluster.replica(chunkId, workerId);
        if (replica == null || replica.state != ReplicaState.CORRUPT) {
            log.warn("[integrity] chunk={} on {} CORRUPT (reported by worker)", chunkId, workerId);
        }
        setCorrupt(chunkId, workerId);
    }

    private void cmdFailed(WorkerInfo worker, Change change) {
        PendingCopy failed = cluster.removePendingByCommand(change.chunkId(), change.cmdId());
        if (failed == null) {
            return; // already ended (time-out, success, DEAD, re-registration, deletion)
        }
        cluster.recordFailure(change.chunkId(), failed.target());
        cluster.record("failed", change.chunkId(), failed.source(), failed.target(),
                "the source could not complete the copy: " + change.reason());
    }

    // ---- report segments ---------------------------------------------------------------------

    /**
     * Applies one segment of a full report to session {@code sessionNumber}. The first segment
     * ({@code open}) opens the writer-side session, replacing any older one of this worker.
     */
    public void applySegment(String workerId, long epoch, long sessionNumber, long baseline, boolean open,
                             List<ReportEntry> entries) {
        applySegment(workerId, epoch, sessionNumber, baseline, open, entries, 0, Long.MAX_VALUE);
    }

    /** The {@code ApplySegment} task: yields at the deadline and resumes where it stopped. */
    public Task applySegmentTask(String workerId, long epoch, long sessionNumber, long baseline, boolean open,
                                 List<ReportEntry> entries) {
        int[] cursor = {0};
        return Task.steps("ApplySegment", deadline -> {
            cursor[0] = applySegment(workerId, epoch, sessionNumber, baseline, open && cursor[0] == 0, entries,
                    cursor[0], deadline);
            return cursor[0] < entries.size() ? Task.Step.MORE : Task.Step.DONE;
        });
    }

    private int applySegment(String workerId, long epoch, long sessionNumber, long baseline, boolean open,
                             List<ReportEntry> entries, int from, long deadline) {
        WorkerInfo worker = current(workerId, epoch);
        if (worker == null) {
            return entries.size();
        }
        if (open) {
            cluster.openSession(worker, sessionNumber, baseline);
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
                cluster.putReceipt(chunkId, workerId, worker.epoch, entry.size(),
                        entry.bad() ? ClusterState.UNVERIFIED : entry.checksum(), session.number);
            } else if (cluster.isGarbage(objectId)) {
                session.garbage.add(chunkId);
            } else {
                session.orphans.add(chunkId);
            }
            return;
        }
        boolean good = !entry.bad() && known.checksum().equals(entry.checksum()) && known.size() == entry.size();
        Replica replica = cluster.replica(chunkId, workerId);
        if (!good) {
            if (replica == null || replica.state != ReplicaState.CORRUPT) {
                log.warn("[report] chunk={} on {} CORRUPT (listed {}{}, expected {})", chunkId, workerId,
                        entry.checksum(), entry.bad() ? " and known bad" : "", known.checksum());
            }
            replica = setCorrupt(chunkId, workerId);
        } else if (replica == null) {
            replica = cluster.addReplica(chunkId, workerId, ReplicaState.OK);
            completePending(chunkId, workerId);
        } else if (replica.state == ReplicaState.CORRUPT) {
            makeOk(chunkId, workerId, replica, "the full report lists a verified copy");
            completePending(chunkId, workerId);
        }
        if (replica != null) {
            replica.mark = session.number;
        }
    }

    /** The {@code CompleteReport} task: {@link #completeReport} in steps of at most 1000 items. */
    public Task completeReportTask(String workerId, long epoch, long sessionNumber, long baseline) {
        Completion completion = new Completion(workerId, epoch, sessionNumber, baseline);
        return Task.steps("CompleteReport", completion::step);
    }

    /** Completes a report at once (tests). */
    public void completeReport(String workerId, long epoch, long sessionNumber, long baseline) {
        Completion completion = new Completion(workerId, epoch, sessionNumber, baseline);
        while (completion.step(Long.MAX_VALUE) == Task.Step.MORE) {
            // all steps in one go
        }
    }

    /**
     * Completing a report session (a multi-step task): orphan GC (if allowed), then removal of the
     * worker's copies that the report did not list (their mark is not this session's), then the
     * bookkeeping; {@code applied} catches up with the baseline. Every step re-checks the epoch
     * and the session, and leaves the state consistent.
     */
    private final class Completion {
        private final String workerId;
        private final long epoch;
        private final long number;
        private final long baseline;
        private int phase;                    // 0 = GC, 1 = sweep, 2 = done
        private List<String> sweep;
        private int cursor;
        private int garbage;
        private int orphans;
        private int missing;

        Completion(String workerId, long epoch, long number, long baseline) {
            this.workerId = workerId;
            this.epoch = epoch;
            this.number = number;
            this.baseline = baseline;
        }

        Task.Step step(long deadline) {
            WorkerInfo worker = current(workerId, epoch);
            if (worker == null) {
                return Task.Step.DONE;
            }
            ReportSession session = worker.session;
            if (session == null || session.closed || session.number != number) {
                cluster.increment("stale_tasks_dropped");
                return Task.Step.DONE;
            }
            if (phase == 0) {
                collectGarbage(session);
                sweep = new ArrayList<>(cluster.chunksOn(workerId));
                phase = 1;
                return Task.Step.MORE;
            }
            int items = 0;
            while (cursor < sweep.size()) {
                String chunkId = sweep.get(cursor++);
                Replica replica = cluster.replica(chunkId, workerId);
                if (replica != null && replica.mark != number) {
                    cluster.removeReplica(chunkId, workerId);   // runs protect()
                    missing++;
                    log.warn("[report] missing chunk={} on {} (was {})", chunkId, workerId, replica.state);
                }
                if (++items >= NamespaceTasks.STEP_ITEMS || System.nanoTime() >= deadline) {
                    if (cursor < sweep.size()) {
                        return Task.Step.MORE;
                    }
                }
            }
            worker.hasReported = true;
            worker.lastReportAt = cluster.now();
            worker.applied = Math.max(worker.applied, baseline);
            cluster.closeSession(worker);
            cluster.ledger().retryWorker(workerId);        // its deletions can be queued now
            cluster.releasePostponed(properties.postponedBudget());
            log.info("[report] {} full report {} complete: listed={} missing={} garbage={} orphans={} baseline={}",
                    workerId, worker.rxSession == number ? Registry.shortId(worker.rxReportId) : "#" + number,
                    session.listed, missing, garbage, orphans, baseline);
            return Task.Step.DONE;
        }

        private void collectGarbage(ReportSession session) {
            if (!gcAllowed()) {
                return;                       // asked again later (Block 14)
            }
            for (String chunkId : session.garbage) {
                if (cluster.chunk(chunkId) == null && deleteUnknown(workerId, chunkId,
                        "chunk of a failed upload or a deleted object; DELETE queued")) {
                    garbage++;
                }
            }
            // Safety valve, over the whole report: a report that would delete much of a worker's
            // data the controller cannot account for is more likely a bug or a wrong database.
            if (session.orphans.size() > ORPHAN_VALVE_MIN
                    && (double) session.orphans.size() / Math.max(1, session.listed) > properties.orphanGcMaxFraction()) {
                log.error("[report] orphan GC refused on {}: {} of {} chunks unknown", workerId, session.orphans.size(),
                        session.listed);
                cluster.increment("orphan_gc_refused");
            } else {
                for (String chunkId : session.orphans) {
                    if (cluster.chunk(chunkId) == null && deleteUnknown(workerId, chunkId, "unknown chunk on disk; DELETE queued")) {
                        orphans++;
                    }
                }
            }
        }
    }

    // ---- helpers -----------------------------------------------------------------------------

    /** A ledger DELETE for a chunk the namespace does not know (orphan GC); false if one was already tracked. */
    private boolean deleteUnknown(String workerId, String chunkId, String reason) {
        if (cluster.ledger().entry(chunkId, workerId) != null) {
            return false;
        }
        cluster.ledger().request(chunkId, workerId, DeletionLedger.ORPHAN);
        cluster.record("orphan", chunkId, workerId, null, reason);
        return true;
    }

    /** The copy becomes CORRUPT (added as CORRUPT if absent); protect() keeps I6 true. */
    private Replica setCorrupt(String chunkId, String workerId) {
        Replica replica = cluster.replica(chunkId, workerId);
        if (replica == null) {
            replica = cluster.addReplica(chunkId, workerId, ReplicaState.CORRUPT);
            cluster.protect(chunkId);
            return replica;
        }
        cluster.markCorrupt(chunkId, workerId);
        return replica;
    }

    /** CORRUPT → OK on fresh evidence; a delivered DELETE cannot be called back, so that copy stays CORRUPT. */
    private void makeOk(String chunkId, String workerId, Replica replica, String why) {
        if (replica.deleteDelivered) {
            return;
        }
        cluster.ledger().cancel(chunkId, workerId);   // a corrupt copy's undelivered DELETE
        cluster.setReplicaState(chunkId, workerId, ReplicaState.OK);
        log.info("[report] chunk={} on {} OK again ({})", chunkId, workerId, why);
    }

    private void completePending(String chunkId, String workerId) {
        PendingCopy completed = cluster.removePending(chunkId, workerId);
        if (completed == null) {
            return;
        }
        long ms = (cluster.now() - completed.startedAt()) / 1_000_000;
        if (completed.kind() == PendingKind.UPLOAD) {
            log.debug("[report] upload copy of chunk={} on {} confirmed after {} ms", chunkId, workerId, ms);
        } else {
            cluster.record("copied", chunkId, completed.source(), workerId, ms + " ms");
        }
    }
}
