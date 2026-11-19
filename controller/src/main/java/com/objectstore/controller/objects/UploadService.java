package com.objectstore.controller.objects;

import com.objectstore.common.Checksums;
import com.objectstore.common.ChunkIds;
import com.objectstore.common.web.ApiException;
import com.objectstore.common.web.ErrorCodes;
import com.objectstore.controller.api.dto.ObjectResponse;
import com.objectstore.common.dto.ChunkPutResponse;
import com.objectstore.controller.cluster.ClusterState;
import com.objectstore.controller.cluster.ClusterState.Receipt;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.ReplicaState;
import com.objectstore.controller.cluster.Receipts;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.SafeMode;
import com.objectstore.controller.cluster.WorkerClient;
import com.objectstore.controller.cluster.WorkerException;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.config.WorkerIo;
import com.objectstore.controller.domain.Chunk;
import com.objectstore.controller.domain.StoredObject;
import com.objectstore.controller.repository.ObjectRepository;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.MediaTypeFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

/**
 * The upload path (HDFS {@code create}, the write pipeline with pipeline recovery, and
 * {@code completeFile}).
 *
 * <ul>
 *   <li><b>Constant memory.</b> At most one chunk is in memory at a time.</li>
 *   <li><b>Pipeline recovery.</b> If a target fails a write (it may have died a moment ago and
 *       not been declared dead yet), it is excluded for the rest of this upload and the next
 *       worker on the ring takes its place.</li>
 *   <li><b>All or nothing.</b> The namespace is written once, at the very end, in one durable
 *       transaction, and only after every chunk is fsynced on two workers. A 201 therefore
 *       survives a controller crash and the loss of any one worker. If the upload fails before
 *       that, nothing is recorded and every copy written so far is deleted (best effort; whatever
 *       is left is garbage, which the workers' streams and reports clean up).</li>
 *   <li><b>Copies come from the workers.</b> A 2xx answer to a PUT never creates a replica by
 *       itself. The PUT carries {@code X-Change-Ack} and the answer usually carries the worker's
 *       {@code RECEIVED} for the chunk, which becomes an upload receipt; at commit a receipt
 *       becomes a replica, and a copy without one becomes an UPLOAD pending copy, which counts
 *       against repair, may be read from, and expires into normal repair.</li>
 * </ul>
 */
@Service
public class UploadService {

    private static final Logger log = LoggerFactory.getLogger(UploadService.class);
    private static final int MAX_NAME_LENGTH = 512;
    private static final int MAX_CONTENT_TYPE_LENGTH = 255;
    private static final String DEFAULT_NAME = "unnamed";

    /** A target worker, with the boot id and epoch it had when we chose it. */
    private record Target(String workerId, String url, String bootId, long epoch) {
    }

    /** A copy that was, or may have been, written during this upload. */
    private record WrittenCopy(Target target, String chunkId) {
    }

    /** A chunk and the workers that confirmed writing it. */
    private record StoredChunk(Chunk chunk, List<Target> holders) {
    }

    /**
     * The outcome of an upload: the object, and whether it was created by this request (201) or
     * is the one an earlier request with the same Idempotency-Key created (200, a replay).
     */
    public record UploadResult(ObjectResponse object, boolean replay) {
    }

    /** Another request with the same Idempotency-Key committed first. */
    private static final class LostIdempotencyRace extends RuntimeException {
        final ObjectRepository.IdempotentUpload winner;

        LostIdempotencyRace(ObjectRepository.IdempotentUpload winner) {
            super(null, null, false, false);
            this.winner = winner;
        }
    }

    private static final int MAX_KEY_LENGTH = 128;

    private final ControllerProperties properties;
    private final MetadataValidator metadataValidator;
    private final ClusterState cluster;
    private final SafeMode safeMode;
    private final PlacementPolicy placement;
    private final WorkerClient workers;
    private final WorkerIo io;
    private final ObjectRepository objects;
    private final ObjectViewAssembler views;
    private final Clock clock;
    private final Receipts receipts;

    public UploadService(ControllerProperties properties, MetadataValidator metadataValidator, ClusterState cluster,
                         SafeMode safeMode, PlacementPolicy placement, WorkerClient workers, WorkerIo io,
                         ObjectRepository objects, ObjectViewAssembler views, Clock clock, Receipts receipts) {
        this.properties = properties;
        this.metadataValidator = metadataValidator;
        this.cluster = cluster;
        this.safeMode = safeMode;
        this.placement = placement;
        this.workers = workers;
        this.io = io;
        this.objects = objects;
        this.views = views;
        this.clock = clock;
        this.receipts = receipts;
    }

    /**
     * Checks that a write can be accepted at all, before the body is read: not in safe mode, and
     * at least REPLICATION_FACTOR write-eligible workers.
     */
    public void checkWritable() {
        safeMode.requireWritable();
        int eligible = cluster.locked(() -> placement.writeEligibleWorkers().size());
        if (eligible < properties.replicationFactor()) {
            throw ApiException.unavailable(ErrorCodes.INSUFFICIENT_WORKERS,
                    "%d storage worker(s) can accept writes but %d are required to store an object"
                            .formatted(eligible, properties.replicationFactor()));
        }
    }

    /** Validates an Idempotency-Key header: 1-128 visible ASCII characters. */
    public static String validateIdempotencyKey(String key) {
        if (key == null) {
            return null;
        }
        if (key.isEmpty() || key.length() > MAX_KEY_LENGTH || !key.chars().allMatch(c -> c > 32 && c < 127)) {
            throw ApiException.badRequest("Idempotency-Key must be 1-" + MAX_KEY_LENGTH + " visible ASCII characters");
        }
        return key;
    }

    /**
     * If an earlier upload already used this key, the object it created, provided the new
     * request is for a file of the same size. A different size under the same key is a client
     * error: 409 {@code conflict}. Only the multipart part's size is consulted: nothing is sent
     * to any worker.
     */
    public java.util.Optional<UploadResult> replay(String idempotencyKey, java.util.function.LongSupplier newSize) {
        if (idempotencyKey == null) {
            return java.util.Optional.empty();
        }
        return objects.findIdempotent(idempotencyKey).map(previous -> {
            long size = newSize.getAsLong();
            if (size != previous.size()) {
                throw ApiException.conflict("Idempotency-Key " + idempotencyKey + " was used for a "
                        + previous.size() + "-byte upload; this one is " + size + " bytes");
            }
            StoredObject object = objects.findById(previous.objectId()).orElseThrow();
            log.info("[upload] replay key={} object={}", idempotencyKey, previous.objectId());
            return new UploadResult(views.describe(object), true);
        });
    }

    public UploadResult upload(MultipartFile file, String rawMetadata, String idempotencyKey) {
        Map<String, String> metadata = metadataValidator.parseForUpload(rawMetadata);
        String name = objectName(file.getOriginalFilename());
        String contentType = contentType(file.getContentType(), name);

        String objectId = UUID.randomUUID().toString();
        long startedNanos = System.nanoTime();
        Set<WrittenCopy> attempted = ConcurrentHashMap.newKeySet();
        Set<String> exclude = ConcurrentHashMap.newKeySet();
        boolean committed = false;

        cluster.locked(() -> cluster.beginUpload(objectId));
        // Durable before the first chunk is written: if this upload never commits (it fails, or
        // the controller crashes), its chunks are known garbage after any restart.
        objects.recordUploadIntent(objectId, clock.instant());
        try (InputStream in = file.getInputStream()) {
            MessageDigest objectDigest = Checksums.newSha256();
            List<StoredChunk> stored = new ArrayList<>();
            long size = 0;
            byte[] piece;
            // readNBytes blocks until it has a full chunk or the stream ends, so only the final
            // piece can be shorter, and an input that is an exact multiple yields no empty piece.
            while ((piece = in.readNBytes(properties.chunkSize())).length > 0) {
                int index = stored.size();
                if (index > ChunkIds.MAX_INDEX) {
                    throw ApiException.payloadTooLarge("an object can have at most " + (ChunkIds.MAX_INDEX + 1) + " chunks");
                }
                String chunkId = ChunkIds.of(objectId, index);
                String checksum = Checksums.sha256Hex(piece);
                objectDigest.update(piece);
                size += piece.length;
                List<Target> holders = storeChunk(objectId, index, chunkId, piece, checksum, exclude, attempted);
                stored.add(new StoredChunk(new Chunk(chunkId, objectId, index, piece.length, checksum), holders));
            }

            Instant now = clock.instant();
            StoredObject object = new StoredObject(objectId, name, size, contentType, Checksums.hex(objectDigest),
                    stored.size(), now, now);
            try {
                commit(object, metadata, stored, idempotencyKey);
            } catch (LostIdempotencyRace race) {
                // A concurrent request with the same key won. Ours is cleaned up below.
                if (race.winner.size() != size) {
                    throw ApiException.conflict("Idempotency-Key " + idempotencyKey + " was used concurrently for a "
                            + race.winner.size() + "-byte upload; this one is " + size + " bytes");
                }
                StoredObject winner = objects.findById(race.winner.objectId()).orElseThrow();
                log.info("[upload] key={} lost the race to object={}; returning that object", idempotencyKey, winner.objectId());
                return new UploadResult(views.describe(winner), true);
            }
            committed = true;
            log.info("[upload] object={} name={} size={} chunks={} ms={}{}", objectId, name, size, stored.size(),
                    (System.nanoTime() - startedNanos) / 1_000_000,
                    exclude.isEmpty() ? "" : " (replaced failed targets " + exclude + ")");
            return new UploadResult(views.describe(object), false);
        } catch (IOException e) {
            throw new ApiException(HttpStatus.BAD_REQUEST, ErrorCodes.BAD_REQUEST,
                    "the uploaded file could not be read completely", e);
        } finally {
            boolean done = committed;
            cluster.locked(() -> {
                cluster.endUpload(objectId);
                if (!done) {
                    cluster.addGarbage(objectId);
                }
            });
            if (!committed) {
                cleanUp(objectId, attempted);
            }
        }
    }

    /**
     * Commits the namespace and records the copies, as one synchronous block under the cluster
     * lock: the durable transaction first, then the in-memory mirror (v4 §5.6 step 5). For each
     * target still in the epoch it was written in: a receipt becomes a replica (OK, or CORRUPT if
     * the receipt's checksum differs) carrying the receipt's report mark; no receipt yet means an
     * UPLOAD pending copy. A copy whose worker died or re-registered since is not recorded at all;
     * the monitor repairs that chunk like any other under-replicated one.
     */
    private void commit(StoredObject object, Map<String, String> metadata, List<StoredChunk> stored, String idempotencyKey) {
        cluster.locked(() -> {
            // All commits run under the cluster lock, so this check and the insert cannot interleave
            // with another commit using the same key.
            if (idempotencyKey != null) {
                objects.findIdempotent(idempotencyKey).ifPresent(winner -> {
                    throw new LostIdempotencyRace(winner);
                });
            }
            objects.insert(object, metadata, stored.stream().map(StoredChunk::chunk).toList(), idempotencyKey);
            int refused = 0;
            int awaited = 0;
            for (StoredChunk entry : stored) {
                Chunk chunk = entry.chunk();
                cluster.putChunk(chunk.chunkId(), new ClusterState.ChunkInfo(
                        chunk.objectId(), chunk.index(), chunk.size(), chunk.checksum()));
                for (Target holder : entry.holders()) {
                    WorkerInfo worker = cluster.worker(holder.workerId());
                    if (worker == null || worker.isDead() || worker.epoch != holder.epoch()) {
                        refused++;
                        continue;
                    }
                    Receipt receipt = cluster.removeReceipt(chunk.chunkId(), holder.workerId());
                    if (receipt != null && receipt.epoch == worker.epoch) {
                        boolean intact = chunk.checksum().equals(receipt.checksum) && chunk.size() == receipt.size;
                        Replica replica = cluster.addReplica(chunk.chunkId(), holder.workerId(),
                                intact ? ReplicaState.OK : ReplicaState.CORRUPT);
                        replica.mark = receipt.seenInSession;
                    } else {
                        cluster.addUploadPending(chunk.chunkId(), holder.workerId());
                        awaited++;
                    }
                }
            }
            cluster.endUpload(object.objectId());
            if (refused > 0) {
                log.warn("[upload] object={}: {} copies not recorded because their worker died or re-registered meanwhile; "
                        + "the monitor will re-replicate", object.objectId(), refused);
            }
            if (awaited > 0) {
                log.debug("[upload] object={}: {} copies await their RECEIVED", object.objectId(), awaited);
            }
        });
    }

    /**
     * Stores one chunk on REPLICATION_FACTOR different workers. The intended holders are written
     * in parallel; a failed target is excluded for the rest of the upload and the next one on the
     * ring is tried instead.
     */
    private List<Target> storeChunk(String objectId, int index, String chunkId, byte[] data, String checksum,
                                    Set<String> exclude, Set<WrittenCopy> attempted) {
        int rf = properties.replicationFactor();
        List<Target> ring = cluster.locked(() -> {
            List<Target> targets = new ArrayList<>();
            for (WorkerInfo worker : placement.ringForUpload(objectId, index, exclude)) {
                targets.add(new Target(worker.workerId, worker.url, worker.bootId, worker.epoch));
            }
            return targets;
        });
        List<Target> holders = new ArrayList<>(rf);
        Set<String> tried = new HashSet<>();
        int next = 0;
        while (holders.size() < rf && next < ring.size()) {
            List<Target> batch = new ArrayList<>();
            while (batch.size() < rf - holders.size() && next < ring.size()) {
                Target candidate = ring.get(next++);
                if (!exclude.contains(candidate.workerId()) && tried.add(candidate.workerId())) {
                    batch.add(candidate);
                }
            }
            List<Boolean> outcomes = io.mapAll(batch, target -> tryStore(target, chunkId, data, checksum, attempted));
            for (int i = 0; i < batch.size(); i++) {
                if (Boolean.TRUE.equals(outcomes.get(i))) {
                    holders.add(batch.get(i));
                } else {
                    exclude.add(batch.get(i).workerId());
                }
            }
        }
        if (holders.size() < rf) {
            throw ApiException.unavailable(ErrorCodes.UPLOAD_FAILED,
                    "chunk %d could be stored on only %d of the required %d workers; the upload was rolled back"
                            .formatted(index, holders.size(), rf));
        }
        return holders;
    }

    private boolean tryStore(Target target, String chunkId, byte[] data, String checksum, Set<WrittenCopy> attempted) {
        // Recorded before the call: a write that timed out may still have reached the disk.
        attempted.add(new WrittenCopy(target, chunkId));
        try {
            String changeAck = cluster.locked(() -> receipts.changeAck(target.workerId()));
            ChunkPutResponse answer = workers.putChunk(target.url(), chunkId, data, checksum, changeAck);
            if (answer != null && answer.changes() != null) {
                receipts.acceptPiggyback(target.workerId(), answer.changes().bootId(), answer.changes().items());
            }
            return true;
        } catch (WorkerException e) {
            log.warn("[upload] chunk={} worker={} write failed ({}); replacing it for this upload",
                    chunkId, target.workerId(), e.getMessage());
            cluster.locked(() -> {
                WorkerInfo worker = cluster.worker(target.workerId());
                if (worker != null) {
                    worker.lastDataError = cluster.now();
                }
            });
            return false;
        }
    }

    /** A failed upload: delete every copy that was or may have been written. Fire and forget. */
    private void cleanUp(String objectId, Set<WrittenCopy> attempted) {
        if (attempted.isEmpty()) {
            return;
        }
        List<WrittenCopy> copies = List.copyOf(attempted);
        io.execute(() -> {
            for (WrittenCopy copy : copies) {
                try {
                    workers.deleteChunk(copy.target().url(), copy.chunkId());
                } catch (WorkerException unreachable) {
                    // An orphan now; block reports clean it up.
                }
            }
        });
        log.warn("[upload] failed object={}; deleting {} chunk copies written so far", objectId, copies.size());
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
