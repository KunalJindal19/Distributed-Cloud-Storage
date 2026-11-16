package com.objectstore.controller.objects;

import com.objectstore.common.Checksums;
import com.objectstore.common.web.ApiException;
import com.objectstore.common.web.ErrorCodes;
import com.objectstore.controller.cluster.WorkerClient;
import com.objectstore.controller.cluster.WorkerClient.FetchedChunk;
import com.objectstore.controller.cluster.WorkerException;
import com.objectstore.controller.cluster.WorkerRegistry;
import com.objectstore.controller.domain.Chunk;
import com.objectstore.controller.domain.ReplicaLocation;
import com.objectstore.controller.domain.StoredObject;
import com.objectstore.controller.replication.IntegrityReporter;
import com.objectstore.controller.repository.ChunkRepository;
import com.objectstore.controller.repository.ObjectRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * The read path. The client names an object; where its chunks live is entirely the controller's
 * business.
 *
 * <p>A download happens in two steps:
 * <ol>
 *   <li>{@link #plan}: load the object, its chunks in order and, for every chunk, the copies that
 *       can be read (good copies only). If any chunk has none, fail with 503 <em>before</em> a
 *       single byte is sent, because the status code cannot be changed once the body has started.</li>
 *   <li>{@link #fetchVerified}, once per chunk: read a copy, hash it, and compare with the
 *       checksum recorded at upload. A copy that does not match is never passed on: it is
 *       reported, and the next copy is used instead.</li>
 * </ol>
 */
@Service
public class DownloadService {

    private static final Logger log = LoggerFactory.getLogger(DownloadService.class);

    /** A chunk and the copies to try, in order. */
    public record PlannedChunk(Chunk chunk, List<ReplicaLocation> candidates) {
    }

    /** Everything needed to stream an object; nothing in it touches the database again. */
    public record DownloadPlan(StoredObject object, List<PlannedChunk> chunks) {
    }

    private final ObjectRepository objects;
    private final ChunkRepository chunks;
    private final WorkerRegistry registry;
    private final PlacementPolicy placement;
    private final WorkerClient workers;
    private final IntegrityReporter integrity;
    private final Clock clock;

    public DownloadService(ObjectRepository objects, ChunkRepository chunks, WorkerRegistry registry,
                           PlacementPolicy placement, WorkerClient workers, IntegrityReporter integrity, Clock clock) {
        this.objects = objects;
        this.chunks = chunks;
        this.registry = registry;
        this.placement = placement;
        this.workers = workers;
        this.integrity = integrity;
        this.clock = clock;
    }

    /**
     * @throws ApiException 404 {@code object_not_found}, or 503 {@code chunk_unavailable} if some
     *                      chunk currently has no intact copy on a live worker
     */
    public DownloadPlan plan(String objectId) {
        StoredObject object = objects.findById(objectId).orElseThrow(() -> ObjectService.notFound(objectId));
        Map<String, List<ReplicaLocation>> locationsByChunk = chunks.findLocationsByObject(objectId).stream()
                .collect(Collectors.groupingBy(ReplicaLocation::chunkId));
        Map<String, Integer> replicaCounts = registry.replicaCounts();

        List<PlannedChunk> planned = new ArrayList<>();
        for (Chunk chunk : chunks.findChunksByObject(objectId)) {
            List<ReplicaLocation> good = locationsByChunk.getOrDefault(chunk.chunkId(), List.of()).stream()
                    .filter(ReplicaLocation::isGood)
                    .toList();
            if (good.isEmpty()) {
                throw unavailable(chunk.chunkId());
            }
            planned.add(new PlannedChunk(chunk, placement.readOrder(good, replicaCounts)));
        }
        return new DownloadPlan(object, planned);
    }

    /**
     * Returns the bytes of a chunk, guaranteed to match the checksum recorded at upload.
     *
     * @throws ApiException 503 {@code chunk_unavailable} if no candidate produced intact bytes
     */
    public byte[] fetchVerified(PlannedChunk planned) {
        Chunk chunk = planned.chunk();
        for (ReplicaLocation candidate : planned.candidates()) {
            Instant observedAt = clock.instant();
            try {
                FetchedChunk fetched = workers.getChunk(candidate.workerUrl(), chunk.chunkId());
                byte[] data = fetched.data();
                if (data.length == chunk.size() && Checksums.sha256Hex(data).equals(chunk.checksum())) {
                    return data;
                }
                integrity.reportCorrupt(candidate, observedAt, "checksum mismatch on read");
            } catch (WorkerException e) {
                integrity.reportMissing(candidate, observedAt, "read failed: " + e.getMessage());
            }
            log.warn("[download] chunk={} worker={} unusable, trying the next copy",
                    chunk.chunkId(), candidate.workerId());
        }
        throw unavailable(chunk.chunkId());
    }

    private static ApiException unavailable(String chunkId) {
        return ApiException.unavailable(ErrorCodes.CHUNK_UNAVAILABLE,
                "chunk " + chunkId + " has no intact copy on a live worker right now");
    }
}
