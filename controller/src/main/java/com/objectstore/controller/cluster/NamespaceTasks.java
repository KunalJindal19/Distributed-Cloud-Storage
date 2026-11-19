package com.objectstore.controller.cluster;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.controller.cluster.ClusterState.PendingCopy;
import com.objectstore.controller.cluster.ClusterState.Receipt;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.ReplicaState;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.Task.Step;
import com.objectstore.controller.domain.Chunk;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * The tasks that change the namespace mirror and the copies that belong to it (v4 §5.6, §5.7,
 * §5.9): {@code CommitObject}, {@code EndUpload}, {@code DeleteObject}, {@code ApplyObjectMirror},
 * plus {@code MarkCorrupt}. They run on the writer, under the write lock, and never touch SQLite:
 * the caller did the database work (under the namespace lock) and passed the rows in. Long ones
 * yield ({@link Step#MORE}) every {@link #STEP_ITEMS} items or at the deadline; the state
 * satisfies every invariant at each yield.
 */
@Component
public class NamespaceTasks {

    private static final Logger log = LoggerFactory.getLogger(NamespaceTasks.class);
    static final int STEP_ITEMS = 1000;

    /** A target of an upload: the worker and the epoch it had when the chunk was written to it. */
    public record Target(String workerId, long epoch) {
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
     * {@code CommitObject} (§5.6 step 5): each chunk goes into the mirror; for each target still in
     * the epoch it was written in, its receipt becomes the replica (OK, or CORRUPT if the checksum
     * differs, with the receipt's report mark), or it gets an UPLOAD pending copy. A target that
     * died or re-registered gets nothing (the chunk is under-replicated and is repaired). Finally
     * the object is no longer pending and its remaining receipts go.
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
                    if (worker == null || worker.isDead() || worker.epoch != target.epoch()) {
                        refused[0]++;
                        continue;
                    }
                    Receipt receipt = cluster.removeReceipt(chunk.chunkId(), target.workerId());
                    if (receipt != null && receipt.epoch == worker.epoch) {
                        boolean intact = chunk.checksum().equals(receipt.checksum) && chunk.size() == receipt.size;
                        Replica replica = cluster.addReplica(chunk.chunkId(), target.workerId(),
                                intact ? ReplicaState.OK : ReplicaState.CORRUPT);
                        replica.mark = receipt.seenInSession;
                        cluster.removePending(chunk.chunkId(), target.workerId());
                    } else if (cluster.replica(chunk.chunkId(), target.workerId()) == null) {
                        cluster.addUploadPending(chunk.chunkId(), target.workerId());
                        awaited[0]++;
                    }
                }
                items++;
                if (stepDone(items, deadline) && cursor[0] < chunks.size()) {
                    return Step.MORE;
                }
            }
            cluster.endUpload(objectId);
            if (refused[0] > 0) {
                log.warn("[upload] object={}: {} copies not recorded because their worker died or re-registered meanwhile; "
                        + "the monitor will re-replicate", objectId, refused[0]);
            }
            if (awaited[0] > 0) {
                log.debug("[upload] object={}: {} copies await their RECEIVED", objectId, awaited[0]);
            }
            return Step.DONE;
        });
    }

    /**
     * {@code EndUpload} (§5.6 step 6): an upload that did not commit. Its chunks are garbage: a
     * DELETE ({@code upload_failed}) goes to every worker it wrote or may have written to, and
     * its receipts and pending state go.
     */
    public Task endUpload(String objectId, List<Attempt> attempts) {
        int[] cursor = {0};
        return Task.steps("EndUpload", deadline -> {
            if (cursor[0] == 0) {
                cluster.addGarbage(objectId);
            }
            int items = 0;
            while (cursor[0] < attempts.size()) {
                Attempt attempt = attempts.get(cursor[0]++);
                WorkerInfo worker = cluster.worker(attempt.workerId());
                if (worker != null && !worker.isDead() && cluster.chunk(attempt.chunkId()) == null) {
                    cluster.queueCommand(attempt.workerId(), InternalApi.COMMAND_DELETE, attempt.chunkId(), null, null,
                            "upload_failed");
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
     * {@code DeleteObject} (§5.7 step 3): the object is garbage; for each chunk, every queued
     * (undelivered) command for it is cancelled, a DELETE ({@code object_deleted}) goes to every
     * holder (any state) and every pending target, and the chunk is forgotten.
     */
    public Task deleteObject(String objectId, List<String> chunkIds) {
        int[] cursor = {0};
        int[] commands = {0};
        boolean[] started = {false};
        return Task.namespace("DeleteObject", objectId, deadline -> {
            if (!started[0]) {
                cluster.addGarbage(objectId);
                started[0] = true;
            }
            if (cursor[0] < chunkIds.size()) {           // one batch per step
                int end = Math.min(chunkIds.size(), cursor[0] + STEP_ITEMS);
                commands[0] += forget(chunkIds.subList(cursor[0], end), "object_deleted");
                cursor[0] = end;
                if (cursor[0] < chunkIds.size()) {
                    return Step.MORE;
                }
            }
            log.info("[api] deleted object={}; {} DELETE commands queued", objectId, commands[0]);
            return Step.DONE;
        });
    }

    /** Forgets a batch of chunks: cancels their queued commands, queues DELETEs, removes them. */
    private int forget(List<String> chunkIds, String reason) {
        Set<String> batch = new HashSet<>(chunkIds);
        for (WorkerInfo worker : cluster.workers().values()) {
            worker.commands.removeIf(command -> batch.contains(command.chunkId()));
        }
        int commands = 0;
        for (String chunkId : chunkIds) {
            Set<String> holders = new LinkedHashSet<>(cluster.replicasOf(chunkId).keySet());
            for (PendingCopy copy : cluster.pendingOf(chunkId)) {
                holders.add(copy.target());          // a copy on its way may land
            }
            for (String workerId : holders) {
                Command queued = cluster.queueCommand(workerId, InternalApi.COMMAND_DELETE, chunkId, null, null, reason);
                if (queued != null) {
                    commands++;
                }
            }
            cluster.removeChunk(chunkId);
        }
        return commands;
    }

    // ---- reconciling after a failed namespace task ---------------------------------------------

    /**
     * {@code ApplyObjectMirror} (§5.9): makes the mirror of one object match the rows read from
     * the database: missing chunks are added (with no copies yet: they get repaired), chunks no
     * longer in the namespace are removed as {@code DeleteObject} does.
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
            forget(stale, "object_deleted");
            if (rows.isEmpty()) {
                cluster.addGarbage(objectId);
            }
            cluster.endUpload(objectId);
            cluster.increment("namespace_reconciled");
            log.warn("[writer] reconciled object={} with the namespace: {} chunks added, {} removed", objectId, added,
                    stale.size());
        });
    }

    // ---- downloads ------------------------------------------------------------------------------

    /** {@code MarkCorrupt}: a read proved a copy bad; it becomes CORRUPT if it is still OK in the same epoch. */
    public Task markCorrupt(String workerId, long epoch, String chunkId, String reason) {
        return Task.of("MarkCorrupt", () -> {
            WorkerInfo worker = cluster.worker(workerId);
            Replica replica = cluster.replica(chunkId, workerId);
            if (worker == null || worker.epoch != epoch || worker.isDead() || replica == null
                    || replica.state != ReplicaState.OK) {
                return;
            }
            cluster.markCorrupt(chunkId, workerId);
            log.warn("[integrity] chunk={} worker={} -> CORRUPT ({})", chunkId, workerId, reason);
        });
    }
}
