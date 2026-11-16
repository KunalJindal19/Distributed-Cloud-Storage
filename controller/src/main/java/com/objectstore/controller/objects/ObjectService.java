package com.objectstore.controller.objects;

import com.objectstore.common.web.ApiException;
import com.objectstore.common.web.ErrorCodes;
import com.objectstore.controller.api.dto.ObjectListResponse;
import com.objectstore.controller.api.dto.ObjectResponse;
import com.objectstore.controller.cluster.WorkerClient;
import com.objectstore.controller.cluster.WorkerException;
import com.objectstore.controller.config.WorkerIo;
import com.objectstore.controller.domain.ReplicaLocation;
import com.objectstore.controller.domain.StoredObject;
import com.objectstore.controller.domain.WorkerStatus;
import com.objectstore.controller.replication.ReplicaGuard;
import com.objectstore.controller.repository.ChunkRepository;
import com.objectstore.controller.repository.ObjectRepository;
import com.objectstore.controller.repository.ObjectRepository.PatchOutcome;
import java.time.Clock;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/** Metadata, listing and deletion of objects. */
@Service
public class ObjectService {

    private static final Logger log = LoggerFactory.getLogger(ObjectService.class);
    public static final int DEFAULT_PAGE_SIZE = 50;
    public static final int MAX_PAGE_SIZE = 500;

    private final ObjectRepository objects;
    private final ChunkRepository chunks;
    private final ObjectViewAssembler views;
    private final MetadataValidator metadataValidator;
    private final WorkerClient workers;
    private final WorkerIo io;
    private final ReplicaGuard guard;
    private final Clock clock;

    /** Objects with a delete in progress; a second mutating request for one of them is a conflict. */
    private final Set<String> deleting = ConcurrentHashMap.newKeySet();

    public ObjectService(ObjectRepository objects, ChunkRepository chunks, ObjectViewAssembler views,
                         MetadataValidator metadataValidator, WorkerClient workers, WorkerIo io, ReplicaGuard guard,
                         Clock clock) {
        this.objects = objects;
        this.chunks = chunks;
        this.views = views;
        this.metadataValidator = metadataValidator;
        this.workers = workers;
        this.io = io;
        this.guard = guard;
        this.clock = clock;
    }

    /** System and custom metadata, including chunk locations. Reads the database only. */
    public ObjectResponse getMetadata(String objectId) {
        return views.describe(find(objectId));
    }

    /**
     * Adds, overwrites and removes custom metadata keys. Only the metadata tables are touched: the
     * object's data, checksum and chunks are not read, moved or re-uploaded.
     */
    public ObjectResponse patchMetadata(String objectId, byte[] body) {
        MetadataValidator.Patch patch = metadataValidator.parseForPatch(body);
        if (deleting.contains(objectId)) {
            throw ApiException.conflict("object " + objectId + " is being deleted");
        }
        PatchOutcome outcome = objects.patchMetadata(
                objectId, patch.upserts(), patch.removals(), MetadataValidator.MAX_KEYS, clock.instant());
        switch (outcome) {
            case NOT_FOUND -> throw notFound(objectId);
            case TOO_MANY_KEYS -> MetadataValidator.requireKeyLimit(MetadataValidator.MAX_KEYS + 1L);
            case UPDATED -> log.info("[api] metadata updated object={} set={} removed={}",
                    objectId, patch.upserts().keySet(), patch.removals());
        }
        return views.describe(find(objectId));
    }

    public ObjectListResponse list(int limit, long offset) {
        if (limit < 1 || limit > MAX_PAGE_SIZE) {
            throw ApiException.badRequest("limit must be between 1 and " + MAX_PAGE_SIZE);
        }
        if (offset < 0) {
            throw ApiException.badRequest("offset must be 0 or greater");
        }
        return new ObjectListResponse(
                objects.list(limit, offset).stream().map(views::summarise).toList(),
                objects.count(),
                limit,
                offset);
    }

    /**
     * Deletes an object and its stored data.
     *
     * <p>Chunk files are deleted on every worker that is alive, then the object row is removed
     * (which removes metadata, chunks and copy records with it). Copies on a worker that is
     * currently DEAD cannot be reached; their files become orphans and are removed by the
     * scrubber as soon as that worker returns.
     *
     * <p>Runs under the replica guard so that it cannot interleave with a repair of the same chunks.
     */
    public void delete(String objectId) {
        find(objectId);
        if (!deleting.add(objectId)) {
            throw ApiException.conflict("object " + objectId + " is already being deleted");
        }
        try {
            guard.run(() -> {
                StoredObject object = find(objectId);
                List<ReplicaLocation> reachable = chunks.findLocationsByObject(objectId).stream()
                        .filter(location -> location.workerStatus() == WorkerStatus.ALIVE)
                        .toList();
                List<Boolean> removed = io.mapAll(reachable, this::deleteCopy);
                objects.delete(objectId);
                log.info("[api] deleted object={} name={} chunks={} files_removed={}", objectId, object.name(),
                        object.chunkCount(), removed.stream().filter(Boolean::booleanValue).count());
            });
        } finally {
            deleting.remove(objectId);
        }
    }

    private boolean deleteCopy(ReplicaLocation location) {
        try {
            return workers.deleteChunk(location.workerUrl(), location.chunkId());
        } catch (WorkerException e) {
            // Best effort: the file is now an orphan and the scrubber will collect it.
            log.warn("[api] could not delete chunk={} on worker={}: {}",
                    location.chunkId(), location.workerId(), e.getMessage());
            return false;
        }
    }

    private StoredObject find(String objectId) {
        return objects.findById(objectId).orElseThrow(() -> notFound(objectId));
    }

    static ApiException notFound(String objectId) {
        return ApiException.notFound(ErrorCodes.OBJECT_NOT_FOUND, "object " + objectId + " does not exist");
    }
}
