package com.objectstore.controller.cluster;

import com.objectstore.common.ChunkIds;
import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.BlockReportRequest;
import com.objectstore.common.dto.ChunkInfo;
import com.objectstore.common.dto.IncrementalReportRequest;
import com.objectstore.common.dto.ReportResponse;
import com.objectstore.common.web.ApiException;
import com.objectstore.common.web.ErrorCodes;
import com.objectstore.controller.cluster.ClusterState.PendingCopy;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.ReplicaState;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.config.ControllerProperties;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Processes block reports (HDFS {@code blockReport} and {@code blockReceivedAndDeleted}). This is
 * how the controller learns, and keeps learning, what is really on each worker's disk.
 *
 * <p>Reports from one worker arrive on an ordered, acknowledged channel: each carries a sequence
 * number, the worker sends the next only after the previous was acknowledged, and it resends an
 * unacknowledged one unchanged. So a report with the last seen sequence number is a retry (a
 * no-op, acknowledged again) and one with a lower number is a delayed old copy (ignored).
 */
@Service
public class Reports {

    private static final Logger log = LoggerFactory.getLogger(Reports.class);
    private static final int ORPHAN_VALVE_MIN = 10;

    private final ClusterState cluster;
    private final Registry registry;
    private final ControllerProperties properties;

    public Reports(ClusterState cluster, Registry registry, ControllerProperties properties) {
        this.cluster = cluster;
        this.registry = registry;
        this.properties = properties;
    }

    /** A full block report: everything the worker holds. Reconciles the block map with it. */
    public ReportResponse fullReport(BlockReportRequest request) {
        return cluster.locked(() -> {
            WorkerInfo worker = acceptReport(request.workerId(), request.bootId(), request.sentAt(), request.seq());
            if (worker == null) {
                return new ReportResponse(true, "duplicate");
            }
            String workerId = worker.workerId;
            long now = cluster.now();
            boolean gcAllowed = !cluster.isSafeMode()
                    && now - cluster.startedAt() >= properties.orphanGcDelay().toNanos();
            List<ChunkInfo> reported = request.chunks() == null ? List.of() : request.chunks();
            Set<String> seen = new HashSet<>();
            List<String> orphans = new ArrayList<>();      // chunks of no object the controller can account for
            List<String> garbage = new ArrayList<>();      // chunks of failed uploads and deleted objects
            int corrupt = 0;
            int added = 0;

            for (ChunkInfo chunk : reported) {
                String chunkId = chunk.chunkId();
                if (!ChunkIds.isValid(chunkId)) {
                    continue;
                }
                seen.add(chunkId);
                ClusterState.ChunkInfo known = cluster.chunk(chunkId);
                if (known == null) {
                    if (cluster.pendingUploads().contains(ChunkIds.objectIdOf(chunkId))) {
                        // An upload in progress; its rows are committed at the end.
                        cluster.recordUploadReceipt(ChunkIds.objectIdOf(chunkId), chunkId, workerId);
                        continue;
                    }
                    if (gcAllowed) {
                        (cluster.isGarbage(ChunkIds.objectIdOf(chunkId)) ? garbage : orphans).add(chunkId);
                    } else {
                        log.debug("[report] {} holds unknown chunk {} (orphan GC not yet allowed)", workerId, chunkId);
                    }
                    continue;
                }
                boolean matches = known.checksum().equals(chunk.checksum()) && known.size() == chunk.size();
                Replica existing = cluster.replica(chunkId, workerId);
                if (!matches) {
                    if (existing == null || existing.state != ReplicaState.CORRUPT) {
                        log.warn("[report] chunk={} on {} CORRUPT (reported {} expected {})", chunkId, workerId,
                                chunk.checksum(), known.checksum());
                        corrupt++;
                    }
                    if (existing == null) {
                        cluster.addReplica(chunkId, workerId, true, ReplicaState.CORRUPT, null);
                    } else {
                        existing.confirmed = true;
                        cluster.markCorrupt(chunkId, workerId);
                    }
                } else if (existing == null) {
                    cluster.addReplica(chunkId, workerId, true, ReplicaState.OK, null);
                    added++;
                } else {
                    existing.confirmed = true; // keep its state: CORRUPT stays CORRUPT, EXCESS stays EXCESS
                }
                PendingCopy completed = cluster.removePending(chunkId, workerId);
                if (completed != null) {
                    cluster.record("copied", chunkId, completed.source(), workerId,
                            "confirmed by full report after " + millis(completed) + " ms");
                }
            }

            // Chunks of uploads that never committed and of deleted objects are certainly garbage.
            for (String chunkId : garbage) {
                cluster.queueCommand(workerId, InternalApi.COMMAND_DELETE, chunkId, null, null, "orphan");
                cluster.record("orphan", chunkId, workerId, null, "chunk of a failed upload or a deleted object; DELETE queued");
            }
            // Orphan safety valve: a report that would delete much of a worker's data that the
            // controller cannot account for is far more likely a bug or a wrong database than garbage.
            if (orphans.size() > ORPHAN_VALVE_MIN
                    && (double) orphans.size() / reported.size() > properties.orphanGcMaxFraction()) {
                log.error("[report] orphan GC refused on {}: {} of {} chunks unknown", workerId, orphans.size(), reported.size());
                cluster.increment("orphan_gc_refused");
            } else {
                for (String chunkId : orphans) {
                    cluster.queueCommand(workerId, InternalApi.COMMAND_DELETE, chunkId, null, null, "orphan");
                    cluster.record("orphan", chunkId, workerId, null, "unknown chunk on disk; DELETE queued");
                }
            }

            // Copies we believed this worker had but it did not report.
            int missing = 0;
            for (String chunkId : new ArrayList<>(cluster.chunksOn(workerId))) {
                if (seen.contains(chunkId)) {
                    continue;
                }
                Replica replica = cluster.replica(chunkId, workerId);
                if (replica == null) {
                    continue;
                }
                boolean settled = replica.confirmed || now - replica.addedAt > properties.pendingTimeout().toNanos();
                if (settled) {
                    cluster.removeReplica(chunkId, workerId);
                    missing++;
                    log.warn("[report] missing chunk={} on {} (was {})", chunkId, workerId, replica.state);
                }
                // else: an upload added it after the worker took its snapshot; the next report settles it
            }

            worker.hasReported = true;
            worker.lastReportAt = now;
            log.info("[report] {} seq={} chunks={} added={} corrupt={} missing={} orphans={}", workerId,
                    request.seq(), reported.size(), added, corrupt, missing, orphans.size() + garbage.size());
            return new ReportResponse(true, null);
        });
    }

    /** An incremental block report: what changed since the previous one. */
    public ReportResponse ibr(IncrementalReportRequest request) {
        return cluster.locked(() -> {
            WorkerInfo worker = acceptReport(request.workerId(), request.bootId(), request.sentAt(), request.seq());
            if (worker == null) {
                return new ReportResponse(true, "duplicate");
            }
            String workerId = worker.workerId;
            for (ChunkInfo chunk : nullSafe(request.received())) {
                String chunkId = chunk.chunkId();
                ClusterState.ChunkInfo known = cluster.chunk(chunkId);
                if (known == null) {
                    if (ChunkIds.isValid(chunkId)) {
                        cluster.recordUploadReceipt(ChunkIds.objectIdOf(chunkId), chunkId, workerId);
                    }
                    continue; // unknown here; the next full report decides whether it is an orphan
                }
                boolean matches = known.checksum().equals(chunk.checksum()) && known.size() == chunk.size();
                if (!matches) {
                    log.warn("[ibr] chunk={} on {} CORRUPT (received {} expected {})", chunkId, workerId,
                            chunk.checksum(), known.checksum());
                    if (cluster.replica(chunkId, workerId) == null) {
                        cluster.addReplica(chunkId, workerId, true, ReplicaState.CORRUPT, null);
                    } else {
                        cluster.markCorrupt(chunkId, workerId);
                    }
                } else {
                    Replica existing = cluster.replica(chunkId, workerId);
                    if (existing == null) {
                        cluster.addReplica(chunkId, workerId, true, ReplicaState.OK, null);
                        log.info("[ibr] received chunk={} on {}", chunkId, workerId);
                    } else {
                        existing.confirmed = true;
                    }
                }
                PendingCopy completed = cluster.removePending(chunkId, workerId);
                if (completed != null) {
                    cluster.record("copied", chunkId, completed.source(), workerId, millis(completed) + " ms");
                }
            }
            for (String chunkId : nullSafeIds(request.deleted())) {
                Replica removed = cluster.removeReplica(chunkId, workerId);
                if (removed == null) {
                    continue;
                }
                String event = switch (removed.state) {
                    case EXCESS -> "trimmed";
                    case CORRUPT -> "invalidated";
                    case OK -> "deleted";
                };
                cluster.record(event, chunkId, workerId, null, "worker reported the copy deleted");
            }
            for (String chunkId : nullSafeIds(request.bad())) {
                Replica replica = cluster.replica(chunkId, workerId);
                if (replica != null && cluster.markCorrupt(chunkId, workerId)) {
                    log.warn("[integrity] chunk={} on {} CORRUPT (reported by worker)", chunkId, workerId);
                }
            }
            return new ReportResponse(true, null);
        });
    }

    /**
     * The acceptance rules for reports, on top of the common ones.
     *
     * @return the worker, or {@code null} if the report is a retry of the last one (acknowledge, apply nothing)
     */
    private WorkerInfo acceptReport(String workerId, String bootId, double sentAt, long seq) {
        WorkerInfo worker = registry.accept(workerId, bootId, sentAt);
        if (seq == worker.lastReportSeq) {
            cluster.increment("duplicates");
            return null;
        }
        if (seq < worker.lastReportSeq) {
            cluster.increment("stale_messages");
            throw ApiException.conflict(ErrorCodes.STALE_SEQ,
                    "report seq " + seq + " is older than the last one accepted (" + worker.lastReportSeq + ")");
        }
        worker.lastReportSeq = seq;
        return worker;
    }

    private long millis(PendingCopy copy) {
        return (cluster.now() - copy.startedAt()) / 1_000_000;
    }

    private static List<ChunkInfo> nullSafe(List<ChunkInfo> list) {
        return list == null ? List.of() : list;
    }

    private static List<String> nullSafeIds(List<String> list) {
        return list == null ? List.of() : list;
    }
}
