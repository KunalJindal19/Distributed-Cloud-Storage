package com.objectstore.controller.objects;

import com.objectstore.common.Checksums;
import com.objectstore.common.ChunkIds;
import com.objectstore.common.web.ApiException;
import com.objectstore.common.web.ErrorCodes;
import com.objectstore.controller.api.dto.ObjectResponse;
import com.objectstore.controller.cluster.WorkerClient;
import com.objectstore.controller.cluster.WorkerException;
import com.objectstore.controller.cluster.WorkerRegistry;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.config.WorkerIo;
import com.objectstore.controller.domain.Chunk;
import com.objectstore.controller.domain.NewReplica;
import com.objectstore.controller.domain.StoredObject;
import com.objectstore.controller.domain.Worker;
import com.objectstore.controller.repository.ObjectRepository;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.MediaTypeFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

/**
 * The upload path: split the incoming byte stream into chunks, store every chunk on
 * REPLICATION_FACTOR different workers, then record the object.
 *
 * <p>Key properties:
 * <ul>
 *   <li><b>Constant memory.</b> At most one chunk is held in memory at a time, however large the
 *       object is.</li>
 *   <li><b>All or nothing.</b> The database is written once, at the very end, in a single
 *       transaction. If anything fails before that, every chunk copy already written is deleted
 *       again and nothing is recorded: a half-uploaded object can never be observed.</li>
 *   <li><b>End-to-end checksums.</b> Each chunk's SHA-256 travels with it and is re-verified by
 *       the worker before it touches the disk; the SHA-256 of the whole object is accumulated on
 *       the way through.</li>
 * </ul>
 */
@Service
public class UploadService {

    private static final Logger log = LoggerFactory.getLogger(UploadService.class);
    private static final int MAX_NAME_LENGTH = 512;
    private static final int MAX_CONTENT_TYPE_LENGTH = 255;
    private static final String DEFAULT_NAME = "unnamed";

    /** A copy that was (or may have been) written to a worker during the current upload. */
    private record WrittenCopy(Worker worker, String chunkId) {
    }

    private final ControllerProperties properties;
    private final MetadataValidator metadataValidator;
    private final WorkerRegistry registry;
    private final PlacementPolicy placement;
    private final WorkerClient workers;
    private final WorkerIo io;
    private final InFlightUploads inFlight;
    private final ObjectRepository objects;
    private final ObjectViewAssembler views;
    private final Clock clock;

    public UploadService(ControllerProperties properties, MetadataValidator metadataValidator,
                         WorkerRegistry registry, PlacementPolicy placement, WorkerClient workers, WorkerIo io,
                         InFlightUploads inFlight, ObjectRepository objects, ObjectViewAssembler views, Clock clock) {
        this.properties = properties;
        this.metadataValidator = metadataValidator;
        this.registry = registry;
        this.placement = placement;
        this.workers = workers;
        this.io = io;
        this.inFlight = inFlight;
        this.objects = objects;
        this.views = views;
        this.clock = clock;
    }

    public ObjectResponse upload(MultipartFile file, String rawMetadata) {
        // Validate everything that can be validated before a single byte is sent to a worker.
        Map<String, String> metadata = metadataValidator.parseForUpload(rawMetadata);
        String name = objectName(file.getOriginalFilename());
        String contentType = contentType(file.getContentType(), name);
        requireEnoughWorkers(registry.aliveWorkers());

        String objectId = UUID.randomUUID().toString();
        long startedNanos = System.nanoTime();
        Set<WrittenCopy> attempted = ConcurrentHashMap.newKeySet();
        boolean committed = false;

        inFlight.begin(objectId);
        try (InputStream in = file.getInputStream()) {
            int chunkSize = properties.chunkSize();
            MessageDigest objectDigest = Checksums.newSha256();
            List<Chunk> chunks = new ArrayList<>();
            List<NewReplica> replicas = new ArrayList<>();
            long size = 0;

            byte[] piece;
            // readNBytes blocks until it has a full chunk or the stream ends, so only the final
            // piece can be shorter, and an input that is an exact multiple yields no empty piece.
            while ((piece = in.readNBytes(chunkSize)).length > 0) {
                int index = chunks.size();
                if (index > ChunkIds.MAX_INDEX) {
                    throw ApiException.payloadTooLarge("an object can have at most " + (ChunkIds.MAX_INDEX + 1) + " chunks");
                }
                String chunkId = ChunkIds.of(objectId, index);
                String checksum = Checksums.sha256Hex(piece);
                objectDigest.update(piece);
                size += piece.length;

                for (Worker holder : storeChunk(objectId, index, chunkId, piece, checksum, attempted)) {
                    replicas.add(new NewReplica(chunkId, holder.workerId()));
                }
                chunks.add(new Chunk(chunkId, objectId, index, piece.length, checksum));
            }

            Instant now = clock.instant();
            StoredObject object = new StoredObject(
                    objectId, name, size, contentType, Checksums.hex(objectDigest), chunks.size(), now, now);
            objects.insert(object, metadata, chunks, replicas);
            committed = true;

            log.info("[upload] object={} name={} size={} chunks={} ms={}",
                    objectId, name, size, chunks.size(), (System.nanoTime() - startedNanos) / 1_000_000);
            return views.describe(object);
        } catch (IOException e) {
            throw new ApiException(org.springframework.http.HttpStatus.BAD_REQUEST, ErrorCodes.BAD_REQUEST,
                    "the uploaded file could not be read completely", e);
        } finally {
            if (!committed) {
                rollBack(objectId, attempted);
            }
            // Only now may the scrubber treat unrecorded files of this object as orphans.
            inFlight.end(objectId);
        }
    }

    /**
     * Stores one chunk on REPLICATION_FACTOR different workers and returns the workers that have it.
     *
     * <p>The intended holders are written in parallel. If one of them fails (it may have died a
     * moment ago and not been declared dead yet), the next worker on the ring is tried instead, so
     * an upload survives a worker failing underneath it as long as enough workers remain.
     */
    private List<Worker> storeChunk(String objectId, int index, String chunkId, byte[] data, String checksum,
                                    Set<WrittenCopy> attempted) {
        int replicationFactor = properties.replicationFactor();
        List<Worker> alive = registry.aliveWorkers();
        requireEnoughWorkers(alive);
        List<Worker> ring = placement.ringFor(alive, objectId, index);

        List<Worker> intended = ring.subList(0, replicationFactor);
        List<Boolean> outcomes = io.mapAll(intended, worker -> tryStore(worker, chunkId, data, checksum, attempted));
        List<Worker> holders = new ArrayList<>(replicationFactor);
        for (int i = 0; i < intended.size(); i++) {
            if (outcomes.get(i)) {
                holders.add(intended.get(i));
            }
        }

        Iterator<Worker> fallbacks = ring.subList(replicationFactor, ring.size()).iterator();
        while (holders.size() < replicationFactor && fallbacks.hasNext()) {
            Worker fallback = fallbacks.next();
            if (tryStore(fallback, chunkId, data, checksum, attempted)) {
                holders.add(fallback);
            }
        }

        if (holders.size() < replicationFactor) {
            throw ApiException.unavailable(ErrorCodes.UPLOAD_FAILED,
                    "chunk %d could be stored on only %d of the required %d workers; the upload was rolled back"
                            .formatted(index, holders.size(), replicationFactor));
        }
        return holders;
    }

    private boolean tryStore(Worker worker, String chunkId, byte[] data, String checksum, Set<WrittenCopy> attempted) {
        // Recorded before the call: a write that timed out may still have reached the disk.
        attempted.add(new WrittenCopy(worker, chunkId));
        try {
            workers.putChunk(worker.url(), chunkId, data, checksum);
            return true;
        } catch (WorkerException e) {
            log.warn("[upload] chunk={} worker={} write failed: {}", chunkId, worker.workerId(), e.getMessage());
            return false;
        }
    }

    /** Compensation for a failed upload: remove every copy that was or may have been written. */
    private void rollBack(String objectId, Set<WrittenCopy> attempted) {
        if (attempted.isEmpty()) {
            return;
        }
        io.mapAll(attempted, copy -> {
            try {
                return workers.deleteChunk(copy.worker().url(), copy.chunkId());
            } catch (WorkerException unreachable) {
                // Best effort. Whatever cannot be deleted now is an orphan the scrubber removes later.
                return false;
            }
        });
        log.warn("[upload] failed object={}; rolled back {} chunk copies", objectId, attempted.size());
    }

    private void requireEnoughWorkers(List<Worker> alive) {
        int replicationFactor = properties.replicationFactor();
        if (alive.size() < replicationFactor) {
            throw ApiException.unavailable(ErrorCodes.INSUFFICIENT_WORKERS,
                    "%d storage worker(s) alive but %d are required to store an object"
                            .formatted(alive.size(), replicationFactor));
        }
    }

    /** The uploaded file name without any client-side directory, or "unnamed". */
    static String objectName(String originalFilename) {
        if (originalFilename == null) {
            return DEFAULT_NAME;
        }
        String name = originalFilename.substring(
                Math.max(originalFilename.lastIndexOf('/'), originalFilename.lastIndexOf('\\')) + 1);
        name = name.chars()
                .filter(c -> !Character.isISOControl(c))
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString()
                .strip();
        if (name.isEmpty()) {
            return DEFAULT_NAME;
        }
        if (name.length() > MAX_NAME_LENGTH) {
            throw ApiException.badRequest("the file name is longer than " + MAX_NAME_LENGTH + " characters");
        }
        return name;
    }

    /**
     * The content type declared for the file part. If the client declared nothing, or only the
     * generic {@code application/octet-stream}, the type is inferred from the file extension.
     */
    static String contentType(String declared, String name) {
        MediaType type = null;
        if (declared != null && !declared.isBlank() && declared.length() <= MAX_CONTENT_TYPE_LENGTH) {
            try {
                type = MediaType.parseMediaType(declared.trim());
            } catch (IllegalArgumentException malformed) {
                type = null;
            }
        }
        if (type == null || type.isWildcardType() || type.isWildcardSubtype()
                || MediaType.APPLICATION_OCTET_STREAM.equalsTypeAndSubtype(type)) {
            return MediaTypeFactory.getMediaType(name)
                    .map(MediaType::toString)
                    .orElse(MediaType.APPLICATION_OCTET_STREAM_VALUE);
        }
        return type.toString();
    }
}
