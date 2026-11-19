package com.objectstore.controller.cluster;

import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.ReportEntry;
import com.objectstore.common.dto.ReportSegment;
import com.objectstore.common.dto.SegmentAck;
import com.objectstore.controller.cluster.ClusterState.ReportSession;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * The receipt step for a worker's change stream and full-report segments (v4 §4.6 steps 3-4,
 * §4.7). It decides what is <em>accepted</em> (and so acknowledged to the worker) and hands the
 * accepted work to the {@link ApplySink}; it applies nothing itself. Every method runs under the
 * cluster lock and is O(message).
 *
 * <ul>
 *   <li><b>Changes.</b> Changes up to the worker's {@code received} are dropped (duplicates and
 *       overlaps). If the first remaining one is not {@code received + 1} this is a gap: nothing
 *       from the message is accepted, a full report is requested and {@code changes_gap} counted;
 *       the acknowledgement never moves past the gap. Otherwise the longest contiguous run is
 *       accepted and {@code received} moves to its end, provided the sink has room; if not,
 *       {@code apply_queue_full} is counted and nothing is acknowledged.</li>
 *   <li><b>Segments.</b> Segment 0 of a new report id opens a session. The next expected segment
 *       is applied (with the completion after the last one, when {@code received} catches up
 *       with the baseline in this same step); an earlier one is only re-acknowledged; anything
 *       else is ignored.</li>
 * </ul>
 */
@Component
public class Receipts {

    private static final Logger log = LoggerFactory.getLogger(Receipts.class);

    private final ClusterState cluster;
    private final ChangeApplier applier;
    private volatile ApplySink sink = ApplySink.INLINE;

    public Receipts(ClusterState cluster, ChangeApplier applier) {
        this.cluster = cluster;
        this.applier = applier;
    }

    public void setSink(ApplySink sink) {
        this.sink = sink;
    }

    /** §4.6 step 3: accepts the longest contiguous run after {@code received}. Under the lock. */
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
                worker.fullReportRequested = true;
                cluster.increment("changes_gap");
                log.warn("[report] {} change gap: expected {} but got {}; nothing accepted, full report requested",
                        worker.workerId, received + 1, change.seq());
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
        ApplySink target = sink;
        if (!target.hasRoom(1)) {
            cluster.increment("apply_queue_full");
            return;
        }
        String workerId = worker.workerId;
        long epoch = worker.epoch;
        List<Change> accepted = List.copyOf(run);
        worker.received = accepted.getLast().seq();
        target.submit("ApplyChanges", () -> applier.applyChanges(workerId, epoch, accepted));
    }

    /** §4.6 step 4: one full-report segment. Returns the acknowledgement, or null. Under the lock. */
    public SegmentAck acceptSegment(WorkerInfo worker, ReportSegment segment) {
        if (segment == null || segment.reportId() == null || segment.index() < 0) {
            return null;
        }
        String reportId = segment.reportId();
        ReportSession session = worker.session;
        boolean inOpenSession = session != null && session.reportId.equals(reportId);
        if (!inOpenSession && reportId.equals(worker.closedReportId) && segment.index() <= worker.closedLastSegment) {
            return new SegmentAck(reportId, segment.index());   // a resent segment of a completed report
        }
        if (segment.index() == 0 && !inOpenSession) {
            session = cluster.openSession(worker, reportId, segment.baseline());
            log.info("[report] {} full report {} started (baseline {})", worker.workerId, Registry.shortId(reportId),
                    segment.baseline());
        }
        if (session == null || !session.reportId.equals(reportId)) {
            return null;                                     // a segment of a report we do not follow
        }
        if (segment.index() <= session.lastSegment) {
            return new SegmentAck(reportId, segment.index());   // a duplicate: re-acknowledge only
        }
        if (segment.index() != session.lastSegment + 1) {
            return null;
        }
        ApplySink target = sink;
        if (!target.hasRoom(segment.last() ? 2 : 1)) {
            cluster.increment("apply_queue_full");
            return null;
        }
        String workerId = worker.workerId;
        long epoch = worker.epoch;
        long number = session.number;
        long baseline = session.baseline;
        List<ReportEntry> entries = segment.entries() == null ? List.of() : segment.entries();
        session.lastSegment = segment.index();
        target.submit("ApplySegment", () -> applier.applySegment(workerId, epoch, number, entries));
        if (segment.last()) {
            worker.received = Math.max(worker.received, baseline);
            target.submit("CompleteReport", () -> applier.completeReport(workerId, epoch, number, baseline));
        }
        return new SegmentAck(reportId, segment.index());
    }

    /**
     * §4.7: changes that came back in a worker's answer to the controller's PUT. Accepted like a
     * heartbeat's changes if {@code bootId} is the worker's current process and it is not DEAD.
     * There is no liveness update: only heartbeats count for liveness.
     */
    public void acceptPiggyback(String workerId, String bootId, List<Change> items) {
        if (items == null || items.isEmpty()) {
            return;
        }
        cluster.locked(() -> {
            WorkerInfo worker = cluster.worker(workerId);
            if (worker == null || worker.isDead() || bootId == null || !bootId.equals(worker.bootId)) {
                cluster.increment("piggyback_ignored");
                return;
            }
            acceptChanges(worker, items);
        });
    }

    /** The {@code X-Change-Ack} value for a PUT to this worker, or null if it is unknown. Under the lock. */
    public String changeAck(String workerId) {
        WorkerInfo worker = cluster.worker(workerId);
        return worker == null || worker.bootId == null ? null : worker.bootId + ":" + worker.received;
    }
}
