package com.objectstore.controller.objects;

import com.objectstore.common.InternalApi;
import com.objectstore.common.web.ApiException;
import com.objectstore.common.web.ErrorCodes;
import com.objectstore.controller.api.dto.ObjectListResponse;
import com.objectstore.controller.api.dto.ObjectResponse;
import com.objectstore.controller.cluster.ClusterState;
import com.objectstore.controller.cluster.SafeMode;
import com.objectstore.controller.domain.Chunk;
import com.objectstore.controller.domain.StoredObject;
import com.objectstore.controller.repository.ChunkRepository;
import com.objectstore.controller.repository.ObjectRepository;
import com.objectstore.controller.repository.ObjectRepository.PatchOutcome;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
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
    private final ClusterState cluster;
    private final SafeMode safeMode;
    private final Clock clock;

    public ObjectService(ObjectRepository objects, ChunkRepository chunks, ObjectViewAssembler views,
                         MetadataValidator metadataValidator, ClusterState cluster, SafeMode safeMode, Clock clock) {
        this.objects = objects;
        this.chunks = chunks;
        this.views = views;
        this.metadataValidator = metadataValidator;
        this.cluster = cluster;
        this.safeMode = safeMode;
        this.clock = clock;
    }

    /** System and custom metadata, including chunk locations. No worker is contacted. */
    public ObjectResponse getMetadata(String objectId) {
        return views.describe(find(objectId));
    }

    /**
     * Adds, overwrites and removes custom metadata keys in one transaction. Only the metadata
     * tables are touched: the object's data, checksum and chunks are not read, moved or re-uploaded.
     */
    public ObjectResponse patchMetadata(String objectId, byte[] body) {
        safeMode.requireWritable();
        MetadataValidator.Patch patch = metadataValidator.parseForPatch(body);
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
     * Deletes an object (HDFS {@code delete} followed by {@code DNA_INVALIDATE}).
     *
     * <p>In one synchronous block: the namespace row is deleted durably (cascading to metadata,
     * chunks and idempotency key), then every known copy gets a DELETE command and the chunks
     * are forgotten. The workers delete the files when they pick the commands up with their next
     * heartbeat. Copies the controller does not know about (on a DEAD worker, or a DELETE lost
     * with a heartbeat reply) become orphans, which the workers' block reports clean up.
     */
    public void delete(String objectId) {
        safeMode.requireWritable();
        int queued = cluster.locked(() -> {
            List<Chunk> chunkRows = chunks.findChunksByObject(objectId);
            if (!objects.delete(objectId, clock.instant())) {
                throw notFound(objectId);
            }
            cluster.addGarbage(objectId);
            int commands = 0;
            for (Chunk chunk : chunkRows) {
                java.util.Set<String> holders = new java.util.LinkedHashSet<>(cluster.replicasOf(chunk.chunkId()).keySet());
                // A copy on its way (a repair or an upload copy) may land: delete it there too.
                cluster.pendingOf(chunk.chunkId()).forEach(copy -> holders.add(copy.target()));
                for (String workerId : holders) {
                    cluster.queueCommand(workerId, InternalApi.COMMAND_DELETE, chunk.chunkId(), null, null, "object_deleted");
                    commands++;
                }
                cluster.removeChunk(chunk.chunkId());
            }
            return commands;
        });
        log.info("[api] deleted object={}; {} DELETE commands queued", objectId, queued);
    }

    private StoredObject find(String objectId) {
        return objects.findById(objectId).orElseThrow(() -> notFound(objectId));
    }

    static ApiException notFound(String objectId) {
        return ApiException.notFound(ErrorCodes.OBJECT_NOT_FOUND, "object " + objectId + " does not exist");
    }
}
