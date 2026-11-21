package com.objectstore.controller.cluster;

import com.objectstore.controller.cluster.ClusterState.Receipt;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.PendingOps.Kind;
import com.objectstore.controller.cluster.PendingOps.Op;
import com.objectstore.controller.cluster.PendingOps.Reason;
import com.objectstore.controller.cluster.Task.Step;
import com.objectstore.controller.domain.Chunk;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * The tasks that change the namespace mirror and the copies and ops that belong to it (v5 §7):
 * {@code CommitObject}, {@code EndUpload}, {@code DeleteObject}, {@code ApplyObjectMirror}, plus
 * {@code ReportCorrupt} for a download. They run on the writer, under the write lock, and never touch
 * SQLite: the caller did the database work (under the namespace lock) and passed the rows in. Long
 * ones yield ({@link Step#MORE}) every {@link #STEP_ITEMS} items or at the deadline.
 */
@Component
public class NamespaceTasks {

    private static final Logger log = LoggerFactory.getLogger(NamespaceTasks.class);
    static final int STEP_ITEMS = 1000;

    /** A target of an upload: the worker, its epoch when the chunk was written, and the PUT's version. */
    public record Target(String workerId, long epoch, long version) {
    }

    /** A committed chunk and the targets that acknowledged it. */
    public record CommittedChunk(Chunk chunk, List<Target> targets) {
    }

    /** A copy an upload wrote, or may have written (the PUT may have landed even if it timed out). */
    public record Attempt(String chunkId, String workerId) {
    }

    private final ClusterState cluster;

    public NamespaceTasks(ClusterState cluster) {
        this.cluster = cluster;
    }

    private static boolean stepDone(int items, long deadline) {
        return items >= STEP_ITEMS || System.nanoTime() >= deadline;
    }

    // ---- uploads ------------------------------------------------------------------------------

    /**
     * {@code CommitObject} (v5 §7): each chunk goes into the mirror; a target whose RECEIVED is already
     * applied (its receipt) becomes a replica at the receipt's version; a target whose PUT succeeded
     * but whose RECEIVED has not been applied yet gets an INSERT {@code UPLOAD} op at that PUT's
     * version, resolved when the RECEIVED arrives. A target that died or re-registered meanwhile gets
     * nothing (the chunk is repaired). Then every other receipt of a current epoch becomes a replica
     * too (a surplus is trimmed later). Finally the object is no longer pending.
     */
    public Task commitObject(String objectId, List<CommittedChunk> chunks) {
        int[] cursor = {0};
        int[] refused = {0};
        int[] awaited = {0};
        return Task.namespace("CommitObject", objectId, deadline -> {
            int items = 0;
            while (cursor[0] < chunks.size()) {
                CommittedChunk entry = chunks.get(cursor[0]++);
                Chunk chunk = entry.chunk();
                cluster.putChunk(chunk.chunkId(), new ClusterState.ChunkInfo(chunk.objectId(), chunk.index(), chunk.size(),
                        chunk.checksum()));
                for (Target target : entry.targets()) {
                    items++;
                    WorkerInfo worker = cluster.worker(target.workerId());
                    if (recordReceipt(chunk, target.workerId())) {
                        continue;
                    }
                    if (worker == null || worker.isDead() || worker.epoch != target.epoch()) {
                        refused[0]++;
                        continue;
                    }
                    cluster.ops().upload(chunk.chunkId(), target.workerId(), target.version());
                    awaited[0]++;
                }
                Map<String, Receipt> others = cluster.receipts().get(chunk.chunkId());
                for (String workerId : others == null ? List.<String>of() : List.copyOf(others.keySet())) {
                    items++;
                    recordReceipt(chunk, workerId);
                }
                items++;
                if (stepDone(items, deadline) && cursor[0] < chunks.size()) {
                    return Step.MORE;
                }
            }
            cluster.endUpload(objectId);
            if (refused[0] > 0) {
                log.warn("[upload] object={}: {} copies not recorded because their worker died or re-registered meanwhile; "
                        + "the planner will re-replicate", objectId, refused[0]);
            }
            if (awaited[0] > 0) {
                log.debug("[upload] object={}: {} copies await their RECEIVED", objectId, awaited[0]);
            }
            return Step.DONE;
        });
    }

    /** Turns the worker's receipt for a committed chunk into its replica (or a CORRUPT delete). */
    private boolean recordReceipt(Chunk chunk, String workerId) {
        Receipt receipt = cluster.removeReceipt(chunk.chunkId(), workerId);
        WorkerInfo worker = cluster.worker(workerId);
        if (receipt == null || worker == null || worker.isDead() || receipt.epoch != worker.epoch) {
            return false;
        }
        boolean intact = chunk.checksum().equals(receipt.checksum) && chunk.size() == receipt.size;
        if (!intact) {
            cluster.ops().ensure(chunk.chunkId(), workerId, Reason.CORRUPT);
            return true;
        }
        Replica replica = cluster.addReplica(chunk.chunkId(), workerId, receipt.version);
        if (replica != null) {
            replica.mark = receipt.seenInSession;
        }
        Op op = cluster.ops().get(chunk.chunkId(), workerId);
        if (op != null && op.kind() == Kind.INSERT && receipt.version >= op.version()) {
            cluster.ops().complete(op);
        }
        return true;
    }

    /**
     * {@code EndUpload}: an upload that did not commit is never visible. Its chunks are garbage and
     * {@code ensure(DELETE, UPLOAD_FAILED)} goes to exactly the pairs it attempted.
     */
    public Task endUpload(String objectId, List<Attempt> attempts) {
        int[] cursor = {0};
        return Task.steps("EndUpload", deadline -> {
            if (cursor[0] == 0) {
                cluster.addGarbage(objectId, Reason.UPLOAD_FAILED);
            }
            int items = 0;
            while (cursor[0] < attempts.size()) {
                Attempt attempt = attempts.get(cursor[0]++);
                if (cluster.chunk(attempt.chunkId()) == null) {
                    cluster.ops().ensure(attempt.chunkId(), attempt.workerId(), Reason.UPLOAD_FAILED);
                }
                if (stepDone(++items, deadline) && cursor[0] < attempts.size()) {
                    return Step.MORE;
                }
            }
            cluster.endUpload(objectId);
            return Step.DONE;
        });
    }

    // ---- deletes ------------------------------------------------------------------------------

    /**
     * {@code DeleteObject} (v5 §7): the object is garbage; {@code ensure(DELETE, OBJECT_DELETED)} goes
     * to every holder and every INSERT target of each chunk (superseding any other op on the pair),
     * and the chunk is forgotten. Physical deletion is asynchronous; repeated DELETEs are harmless.
     */
    public Task deleteObject(String objectId, List<String> chunkIds) {
        int[] cursor = {0};
        int[] deletions = {0};
        boolean[] started = {false};
        return Task.namespace("DeleteObject", objectId, deadline -> {
            if (!started[0]) {
                cluster.addGarbage(objectId, Reason.OBJECT_DELETED);
                started[0] = true;
            }
            if (cursor[0] < chunkIds.size()) {
                int end = Math.min(chunkIds.size(), cursor[0] + STEP_ITEMS);
                deletions[0] += forget(chunkIds.subList(cursor[0], end));
                cursor[0] = end;
                if (cursor[0] < chunkIds.size()) {
                    return Step.MORE;
                }
            }
            log.info("[api] deleted object={}; {} copies to delete", objectId, deletions[0]);
            return Step.DONE;
        });
    }

    /** Forgets a batch of chunks: a DELETE for every holder and INSERT target, then the mirror rows go. */
    private int forget(List<String> chunkIds) {
        int deletions = 0;
        for (String chunkId : chunkIds) {
            Set<String> holders = new LinkedHashSet<>(cluster.replicasOf(chunkId).keySet());
            for (Op op : cluster.ops().opsFor(chunkId)) {
                if (op.kind() == Kind.INSERT) {
                    holders.add(op.workerId);          // a copy on its way may land
                }
            }
            for (String workerId : holders) {
                if (cluster.ops().ensure(chunkId, workerId, Reason.OBJECT_DELETED) != null) {
                    deletions++;
                }
            }
            cluster.removeChunk(chunkId);
        }
        return deletions;
    }

    // ---- reconciling after a failed namespace task ---------------------------------------------

    /**
     * {@code ApplyObjectMirror}: makes the mirror of one object match the rows read from the
     * database: missing chunks are added (with no copies yet: they get repaired), chunks no longer in
     * the namespace are forgotten as {@code DeleteObject} does.
     */
    public Task applyObjectMirror(String objectId, List<Chunk> rows) {
        return Task.of("ApplyObjectMirror", () -> {
            String prefix = objectId + "-";
            Set<String> wanted = new HashSet<>();
            int added = 0;
            for (Chunk row : rows) {
                wanted.add(row.chunkId());
                if (cluster.chunk(row.chunkId()) == null) {
                    cluster.putChunk(row.chunkId(), new ClusterState.ChunkInfo(row.objectId(), row.index(), row.size(),
                            row.checksum()));
                    added++;
                }
            }
            List<String> stale = new ArrayList<>();
            for (String chunkId : cluster.chunks().keySet()) {
                if (chunkId.startsWith(prefix) && !wanted.contains(chunkId)) {
                    stale.add(chunkId);
                }
            }
            if (rows.isEmpty()) {
                cluster.addGarbage(objectId, Reason.OBJECT_DELETED);
            }
            forget(stale);
            cluster.endUpload(objectId);
            cluster.increment("namespace_reconciled");
            log.warn("[writer] reconciled object={} with the namespace: {} chunks added, {} removed", objectId, added,
                    stale.size());
        });
    }

    // ---- downloads ------------------------------------------------------------------------------

    /**
     * {@code ReportCorrupt}: a download found that the worker's copy (generation {@code gen}, read in
     * {@code epoch}) does not match the namespace checksum: {@code ensure(DELETE, CORRUPT)}, if it is
     * still the same copy. A copy corrupted in transit is retried by the download, never reported.
     */
    public Task reportCorrupt(String workerId, long epoch, String chunkId, long gen, String reason) {
        return Task.of("ReportCorrupt", () -> {
            WorkerInfo worker = cluster.worker(workerId);
            Replica replica = cluster.replica(chunkId, workerId);
            if (worker == null || worker.epoch != epoch || worker.isDead() || replica == null || replica.gen != gen) {
                return;
            }
            if (cluster.ops().ensure(chunkId, workerId, Reason.CORRUPT) != null) {
                log.warn("[integrity] chunk={} worker={} -> quarantine ({})", chunkId, workerId, reason);
            }
        });
    }
}
