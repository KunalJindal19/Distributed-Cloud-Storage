package com.objectstore.controller.objects;

import com.objectstore.common.web.ApiException;
import com.objectstore.common.web.ErrorCodes;
import com.objectstore.controller.api.dto.ObjectListResponse;
import com.objectstore.controller.api.dto.ObjectResponse;
import com.objectstore.controller.auth.AuthUser;
import com.objectstore.controller.cluster.ApplyQueue;
import com.objectstore.controller.cluster.ClusterState;
import com.objectstore.controller.cluster.NamespaceLock;
import com.objectstore.controller.cluster.NamespaceTasks;
import com.objectstore.controller.cluster.Task;
import com.objectstore.controller.cluster.Writer;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.cluster.SafeMode;
import com.objectstore.controller.domain.Chunk;
import com.objectstore.controller.domain.StoredObject;
import com.objectstore.controller.repository.ChunkRepository;
import com.objectstore.controller.repository.ObjectRepository;
import com.objectstore.controller.repository.ObjectRepository.PatchOutcome;
import java.time.Clock;
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
    private final NamespaceLock namespaceLock;
    private final NamespaceTasks namespaceTasks;
    private final ApplyQueue queue;
    private final Writer writer;
    private final ControllerProperties properties;
    private final ObjectDeleteSender deleteSender;

    public ObjectService(ObjectRepository objects, ChunkRepository chunks, ObjectViewAssembler views,
                         MetadataValidator metadataValidator, ClusterState cluster, SafeMode safeMode, Clock clock,
                         NamespaceLock namespaceLock, NamespaceTasks namespaceTasks, ApplyQueue queue, Writer writer,
                         ControllerProperties properties, ObjectDeleteSender deleteSender) {
        this.objects = objects;
        this.chunks = chunks;
        this.views = views;
        this.metadataValidator = metadataValidator;
        this.cluster = cluster;
        this.safeMode = safeMode;
        this.clock = clock;
        this.namespaceLock = namespaceLock;
        this.namespaceTasks = namespaceTasks;
        this.queue = queue;
        this.writer = writer;
        this.properties = properties;
        this.deleteSender = deleteSender;
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

    /**
     * 404 unless {@code user} may see the object: its owner, or the admin. Another account's object
     * is answered exactly like one that does not exist, so object ids reveal nothing.
     */
    public void checkAccess(AuthUser user, String objectId) {
        List<String> owner = objects.ownerOf(objectId);
        if (owner.isEmpty() || !user.mayAccess(owner.get(0))) {
            throw notFound(objectId);
        }
    }

    /** The objects {@code user} may see (the admin: all of them), newest first. */
    public ObjectListResponse list(int limit, long offset, AuthUser user) {
        if (limit < 1 || limit > MAX_PAGE_SIZE) {
            throw ApiException.badRequest("limit must be between 1 and " + MAX_PAGE_SIZE);
        }
        if (offset < 0) {
            throw ApiException.badRequest("offset must be 0 or greater");
        }
        return new ObjectListResponse(
                objects.list(limit, offset, user.listingOwner()).stream().map(views::summarise).toList(),
                objects.count(user.listingOwner()),
                limit,
                offset);
    }

    /**
     * Deletes an object (HDFS {@code delete} followed by {@code DNA_INVALIDATE}), v4 §5.7.
     *
     * <p>Under the namespace lock: the namespace row is deleted durably (cascading to metadata,
     * chunks and idempotency key, leaving a tombstone) and {@code DeleteObject} is queued. Then,
     * with no lock held, a wait of at most UPLOAD_COMMIT_WAIT for it, and {@code 204}. The task
     * gives every holder of a copy (and every target of a copy on its way) a DELETE command and
     * forgets the chunks; the workers delete the files when they pick the commands up with their
     * next heartbeat. Copies the controller does not know about (on a DEAD worker) are garbage,
     * collected from the worker's changes or its next full report.
     */
    public void delete(String objectId) {
        safeMode.requireWritable();
        Task task;
        List<Chunk> chunkRows;
        namespaceLock.lock();
        try {
            chunkRows = chunks.findChunksByObject(objectId);
            if (!objects.delete(objectId, clock.instant())) {
                throw notFound(objectId);
            }
            task = namespaceTasks.deleteObject(objectId, chunkRows.stream().map(Chunk::chunkId).toList());
            boolean interrupted = false;
            while (true) {
                try {
                    queue.put(task);
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        } finally {
            namespaceLock.unlock();
        }
        // Once the task has created the versioned DELETEs, send them straight to the holders through
        // DELETE /internal/chunks/{id} (like the upload's PUTs) rather than waiting for their heartbeats.
        // In the background: the 204 below never waits for the workers.
        deleteSender.sendWhenApplied(task, objectId, chunkRows.stream().map(Chunk::chunkId).toList());
        try {
            writer.await(task, properties.uploadCommitWait());
        } catch (java.util.concurrent.TimeoutException e) {
            cluster.increment("upload_commit_wait_timeouts");
            log.warn("[api] object={} is deleted but its DeleteObject task did not finish within {} s; answering anyway",
                    objectId, properties.uploadCommitWaitSeconds());
        } catch (RuntimeException e) {
            log.warn("[api] object={} is deleted; applying it to memory failed ({}), so it is reconciled", objectId,
                    e.toString());
        }
    }

    private StoredObject find(String objectId) {
        return objects.findById(objectId).orElseThrow(() -> notFound(objectId));
    }

    static ApiException notFound(String objectId) {
        return ApiException.notFound(ErrorCodes.OBJECT_NOT_FOUND, "object " + objectId + " does not exist");
    }
}
