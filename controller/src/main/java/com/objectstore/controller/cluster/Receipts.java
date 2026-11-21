package com.objectstore.controller.cluster;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.ReportEntry;
import com.objectstore.common.dto.ReportSegment;
import com.objectstore.common.dto.SegmentAck;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * The receipt step for a worker's change stream, resync segments and resync requests. It decides
 * what is <em>accepted</em> (and so acknowledged to the worker) and hands the accepted work to the
 * {@link ApplySink}; it applies nothing itself. Every method runs under the cluster lock and is
 * O(message).
 *
 * <ul>
 *   <li><b>Changes.</b> Changes up to the worker's {@code received} are dropped (duplicates and
 *       overlaps). If the first remaining one is not {@code received + 1} this is a gap: nothing is
 *       accepted, and a resync starts (or, while resyncing with segment 0 seen, a new attempt). The
 *       acknowledgement never moves past the gap. Otherwise the longest contiguous run is accepted
 *       and {@code received} moves to its end, provided the sink has room.</li>
 *   <li><b>Segments</b> follow the v5 §5.4 table: the current attempt's next segment is applied
 *       ({@code ok}); a duplicate is acknowledged again without applying anything ({@code ok}); a
 *       segment of the attempt that already completed gets {@code done}; one of the current attempt
 *       that the worker's stream has outrun ({@code received > baseline}) authorizes the next attempt
 *       ({@code stale}); anything else is {@code stale}.</li>
 *   <li><b>{@code resync_wanted}</b> (overflow, tombstones) starts a resync, or a new attempt if
 *       segment 0 of the current one has arrived (repeated signals cause only one).</li>
 * </ul>
 */
@Component
public class Receipts {

    private static final Logger log = LoggerFactory.getLogger(Receipts.class);

    private final ClusterState cluster;
    private final ChangeApplier applier;
    private volatile ApplySink sink;

    public Receipts(ClusterState cluster, ChangeApplier applier, ApplyQueue queue) {
        this.cluster = cluster;
        this.applier = applier;
        this.sink = queue;
    }

    public void setSink(ApplySink sink) {
        this.sink = sink;
    }

    /** Accepts the longest contiguous run after {@code received}. Under the lock. */
    public void acceptChanges(WorkerInfo worker, List<Change> changes) {
        if (changes == null || changes.isEmpty()) {
            return;
        }
        long received = worker.received;
        List<Change> run = new ArrayList<>();
        long expected = received + 1;
        for (Change change : changes) {
            if (change == null || change.seq() <= received) {
                continue;                                   // already received: a duplicate or an overlap
            }
            if (run.isEmpty() && change.seq() != received + 1) {
                cluster.increment("changes_gap");
                log.warn("[report] {} change gap: expected {} but got {}; nothing accepted", worker.workerId,
                        received + 1, change.seq());
                resyncSignal(worker, "change-log gap");
                return;
            }
            if (change.seq() != expected) {
                break;                                      // the longest contiguous run ends here
            }
            run.add(change);
            expected++;
        }
        if (run.isEmpty()) {
            return;
        }
        List<Change> accepted = List.copyOf(run);
        if (!sink.offer(applier.applyChangesTask(worker.workerId, worker.epoch, accepted))) {
            cluster.increment("apply_queue_full");
            return;
        }
        worker.received = accepted.getLast().seq();
    }

    /** The worker asks for a resync ({@code overflow}, {@code tombstones}). Under the lock. */
    public void resyncWanted(WorkerInfo worker, String why) {
        if (why != null) {
            resyncSignal(worker, "the worker asked: " + why);
        }
    }

    /**
     * A gap or a resync request: while resyncing it authorizes a new attempt, but only once segment 0
     * of the current one has arrived (a newer attempt that has not started yet postdates the signal);
     * otherwise it starts a resync.
     */
    private void resyncSignal(WorkerInfo worker, String why) {
        if (worker.resyncing) {
            if (worker.segmentSeen) {
                cluster.newAttempt(worker, why);
            }
        } else {
            cluster.startResync(worker, why, false);
        }
    }

    /** One resync segment (v5 §5.4). Returns the acknowledgement, or null (not taken: resend). Under the lock. */
    public SegmentAck acceptSegment(WorkerInfo worker, ReportSegment segment) {
        if (segment == null || segment.reportId() == null || segment.index() < 0) {
            return null;
        }
        String reportId = segment.reportId();
        int index = segment.index();
        if (segment.floor() == worker.lastCompletedFloor && segment.attempt() == worker.lastCompletedAttempt) {
            return ack(segment, InternalApi.SEGMENT_DONE);
        }
        if (!worker.resyncing || segment.floor() != worker.fenceFloor || segment.attempt() != worker.attempt) {
            return ack(segment, InternalApi.SEGMENT_STALE);
        }
        boolean known = reportId.equals(worker.rxReportId);
        if (known && index <= worker.rxLastSegment) {
            return ack(segment, InternalApi.SEGMENT_OK);          // a duplicate: acknowledge again, apply nothing
        }
        if (worker.received > segment.baseline()) {
            cluster.newAttempt(worker, "received " + worker.received + " > baseline " + segment.baseline());
            return ack(segment, InternalApi.SEGMENT_STALE);
        }
        boolean opening = !known && worker.rxReportId == null && index == 0;
        if (!opening && !(known && !worker.rxDone && index == worker.rxLastSegment + 1)) {
            return ack(segment, InternalApi.SEGMENT_STALE);       // not the segment we expect
        }
        long number = opening ? cluster.nextSessionNumber() : worker.rxSession;
        long baseline = opening ? segment.baseline() : worker.rxBaseline;
        String workerId = worker.workerId;
        long epoch = worker.epoch;
        long floor = segment.floor();
        int attempt = segment.attempt();
        List<ReportEntry> entries = segment.entries() == null ? List.of() : segment.entries();
        Task apply = applier.applySegmentTask(workerId, epoch, number, baseline, opening, floor, attempt, entries);
        boolean accepted = segment.last()
                ? sink.offer(apply, applier.completeReportTask(workerId, epoch, number, baseline, floor, attempt))
                : sink.offer(apply);
        if (!accepted) {
            cluster.increment("apply_queue_full");
            return null;
        }
        if (opening) {
            worker.rxReportId = reportId;
            worker.rxSession = number;
            worker.rxBaseline = baseline;
            worker.rxDone = false;
            worker.segmentSeen = true;
            log.info("[report] {} resync listing {} started (fence {} attempt {}, baseline {})", workerId,
                    Registry.shortId(reportId), floor, attempt, baseline);
        }
        worker.rxLastSegment = index;
        worker.lastSegmentAt = cluster.now();
        if (segment.last()) {
            worker.rxDone = true;
            worker.received = Math.max(worker.received, baseline);
        }
        return ack(segment, InternalApi.SEGMENT_OK);
    }

    private static SegmentAck ack(ReportSegment segment, String status) {
        return new SegmentAck(segment.reportId(), segment.index(), status);
    }

    /**
     * Changes that came back in a worker's answer to the controller's PUT. Accepted like a
     * heartbeat's changes if {@code bootId} is the worker's current process and it is not DEAD.
     * There is no liveness update: only heartbeats count for liveness.
     */
    public void acceptPiggyback(String workerId, String bootId, List<Change> items) {
        if (items == null || items.isEmpty()) {
            return;
        }
        cluster.write(() -> {
            WorkerInfo worker = cluster.worker(workerId);
            if (worker == null || worker.isDead() || bootId == null || !bootId.equals(worker.bootId)) {
                cluster.increment("piggyback_ignored");
                return;
            }
            acceptChanges(worker, items);
        });
    }

    /** The {@code X-Change-Ack} value for a PUT to this worker, or null if it is unknown. Under the (read) lock. */
    public String changeAck(String workerId) {
        WorkerInfo worker = cluster.worker(workerId);
        return worker == null || worker.bootId == null ? null : worker.bootId + ":" + worker.received;
    }
}
