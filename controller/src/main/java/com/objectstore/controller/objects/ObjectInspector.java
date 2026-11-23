package com.objectstore.controller.objects;

import com.objectstore.controller.api.dto.ObjectResponse;
import com.objectstore.controller.cluster.ClusterState;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.NeededReplication;
import com.objectstore.controller.cluster.OpVersions;
import com.objectstore.controller.cluster.PendingOps;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.domain.Chunk;
import com.objectstore.controller.repository.ChunkRepository;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * Everything the controller knows about one object, for the admin's dashboard: its metadata, and
 * for every chunk each copy (worker, state, version, VERIFY credit), the operations pending on it
 * (copies on their way, deletes and their state) and its replication level; plus, per worker, what
 * it holds of the object. Read-only: the database first, then one short read of the cluster state.
 */
@Component
public class ObjectInspector {

    /** One copy of a chunk. {@code status}: OK, or CORRUPT / EXCESS while its delete is pending. */
    public record CopyView(String workerId, String workerState, String status, long version, String versionLabel,
                           boolean verified) {
    }

    /** One pending operation on a chunk: an INSERT (a copy on its way to {@code workerId}) or a DELETE. */
    public record OpView(String workerId, String kind, String reason, String state, long version, String versionLabel,
                         String source, int attempts, double ageSeconds) {
    }

    /**
     * One chunk. {@code live}: copies on workers that are not DEAD; {@code good}: those without a
     * pending CORRUPT delete; {@code health}: OK, UNDER_REPLICATED, SURPLUS or MISSING (no good copy);
     * {@code level}: its priority level "L,P" while it needs copies, else null.
     */
    public record ChunkView(String chunkId, int index, int size, String checksum, int live, int good, String health,
                            String level, List<CopyView> copies, List<OpView> ops) {
    }

    /** What one worker holds of the object, and the operations pending on it for the object. */
    public record WorkerView(String workerId, String state, boolean hasReported, boolean resyncing, int copies,
                             int ok, int corrupt, int excess, long bytes, int incoming, int deleting) {
    }

    public record Inspection(ObjectResponse object, int replicationFactor, Map<String, Integer> chunksByHealth,
                             List<WorkerView> workers, List<ChunkView> chunks) {
    }

    private final ObjectService objects;
    private final ChunkRepository chunks;
    private final ClusterState cluster;
    private final ControllerProperties properties;

    public ObjectInspector(ObjectService objects, ChunkRepository chunks, ClusterState cluster,
                           ControllerProperties properties) {
        this.objects = objects;
        this.chunks = chunks;
        this.cluster = cluster;
        this.properties = properties;
    }

    /** 404 {@code object_not_found} if there is no such object. */
    public Inspection inspect(String objectId) {
        ObjectResponse object = objects.getMetadata(objectId);
        List<Chunk> rows = chunks.findChunksByObject(objectId);
        int rf = properties.replicationFactor();
        return cluster.read(() -> {
            long now = cluster.now();
            Map<String, int[]> perWorker = new LinkedHashMap<>();   // ok, corrupt, excess, incoming, deleting
            Map<String, long[]> bytesPerWorker = new LinkedHashMap<>();
            cluster.workers().keySet().stream().sorted(WORKER_ORDER).forEach(w -> {
                perWorker.put(w, new int[5]);
                bytesPerWorker.put(w, new long[1]);
            });
            Map<String, Integer> byHealth = new LinkedHashMap<>();
            List<ChunkView> views = new ArrayList<>();
            NeededReplication levels = cluster.levels();
            for (Chunk chunk : rows) {
                List<CopyView> copies = new ArrayList<>();
                int live = 0;
                int good = 0;
                for (Map.Entry<String, Replica> entry : cluster.replicasOf(chunk.chunkId()).entrySet()) {
                    String workerId = entry.getKey();
                    WorkerInfo worker = cluster.worker(workerId);
                    String status = cluster.replicaStatus(chunk.chunkId(), workerId);
                    boolean dead = worker == null || worker.isDead();
                    if (!dead) {
                        live++;
                        if (!"CORRUPT".equals(status)) {
                            good++;
                        }
                    }
                    copies.add(new CopyView(workerId, dead ? "DEAD" : worker.state.name(), status,
                            entry.getValue().version, label(entry.getValue().version),
                            cluster.freshForDelete(entry.getValue(), worker)));
                    int[] counts = perWorker.computeIfAbsent(workerId, w -> new int[5]);
                    counts["CORRUPT".equals(status) ? 1 : "EXCESS".equals(status) ? 2 : 0]++;
                    bytesPerWorker.computeIfAbsent(workerId, w -> new long[1])[0] += chunk.size();
                }
                copies.sort(Comparator.comparing(CopyView::workerId, WORKER_ORDER));
                List<OpView> ops = new ArrayList<>();
                for (PendingOps.Op op : cluster.ops().opsFor(chunk.chunkId())) {
                    ops.add(new OpView(op.workerId, op.kind().name(), op.reason().name(), op.state().name(),
                            op.version(), label(op.version()), op.source(), op.attempts(),
                            Math.max(0, now - op.createdAt()) / 1e9));
                    perWorker.computeIfAbsent(op.workerId, w -> new int[5])[op.kind() == PendingOps.Kind.INSERT ? 3 : 4]++;
                }
                ops.sort(Comparator.comparing(OpView::workerId, WORKER_ORDER));
                int level = levels.levelOf(chunk.chunkId());
                String health = good == 0 ? "MISSING" : good < rf ? "UNDER_REPLICATED" : good > rf ? "SURPLUS" : "OK";
                byHealth.merge(health, 1, Integer::sum);
                views.add(new ChunkView(chunk.chunkId(), chunk.index(), chunk.size(), chunk.checksum(), live, good, health,
                        level < 0 ? null : levels.liveOf(level) + "," + levels.pendingOf(level), copies, ops));
            }
            List<WorkerView> workers = new ArrayList<>();
            perWorker.forEach((workerId, c) -> {
                WorkerInfo worker = cluster.worker(workerId);
                boolean dead = worker == null || worker.isDead();
                workers.add(new WorkerView(workerId, dead ? "DEAD" : worker.state.name(), worker != null && worker.hasReported,
                        worker != null && worker.resyncing, c[0] + c[1] + c[2], c[0], c[1], c[2],
                        bytesPerWorker.get(workerId)[0], c[3], c[4]));
            });
            return new Inspection(object, rf, byHealth, workers, views);
        });
    }

    /**
     * {@code t<term>.<seq>}: the controller term and the sequence number inside it ({@code w}: a worker's
     * own observation). Version 0, "from disk": the worker found the copy on its disk when it started
     * (versions live in the worker's memory) and no operation has touched it since.
     */
    static String label(long version) {
        if (version == 0) {
            return "from disk";
        }
        long seq = (version & ((1L << OpVersions.TERM_SHIFT) - 1)) >>> 1;
        return "t" + OpVersions.termOf(version) + "." + seq + ((version & 1) == 1 ? "w" : "");
    }

    /** worker2 before worker10. */
    private static final Comparator<String> WORKER_ORDER = Comparator
            .comparingInt(String::length).thenComparing(Comparator.naturalOrder());
}
