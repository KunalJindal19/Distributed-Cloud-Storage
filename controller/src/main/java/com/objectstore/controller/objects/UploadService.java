package com.objectstore.controller.objects;

import com.objectstore.common.Checksums;
import com.objectstore.common.ChunkIds;
import com.objectstore.common.web.ApiException;
import com.objectstore.common.web.ErrorCodes;
import com.objectstore.controller.api.dto.ObjectResponse;
import com.objectstore.common.dto.ChunkPutResponse;
import com.objectstore.controller.cluster.ClusterState;
import com.objectstore.controller.cluster.ApplyQueue;
import com.objectstore.controller.cluster.NamespaceLock;
import com.objectstore.controller.cluster.NamespaceTasks;
import com.objectstore.controller.cluster.Receipts;
import com.objectstore.controller.cluster.Task;
import com.objectstore.controller.cluster.Writer;
import com.objectstore.controller.observability.MessageTrace;
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
 *       that, nothing is recorded and {@code ensure(DELETE, UPLOAD_FAILED)} goes to exactly the pairs
 *       it attempted ({@code EndUpload}).</li>
 *   <li><b>Versioned PUTs.</b> Every PUT carries a fresh operation version ({@code X-Op-Version}),
 *       reserved together with the eligibility check, and the namespace checksum ({@code X-Checksum}).</li>
 *   <li><b>Copies come from the workers.</b> A 2xx answer to a PUT never creates a replica by
 *       itself. The PUT carries {@code X-Change-Ack} and the answer usually carries the worker's
 *       {@code RECEIVED(v)} for the chunk, which becomes an upload receipt; at commit a receipt
 *       becomes a replica, and a copy without one becomes an INSERT UPLOAD op at the PUT's version,
 *       which counts in P, may be read from, and turns into a REPAIR at the same version if its
 *       RECEIVED never comes.</li>
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

    /** A worker that confirmed writing a chunk, and the version its copy was installed at. */
    private record Holder(Target target, long version) {
    }

    /** A chunk and the workers that confirmed writing it. */
    private record StoredChunk(Chunk chunk, List<Holder> holders) {
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

    /** Test hook: called with the object id and its target worker ids just before the commit. */
    public static volatile java.util.function.BiConsumer<String, List<String>> beforeCommitForTests;
    /** Test hook: called with the object id right after the SQLite insert, still under the namespace lock. */
    public static volatile java.util.function.Consumer<String> afterInsertForTests;

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
    private final NamespaceLock namespaceLock;
    private final NamespaceTasks namespaceTasks;
    private final ApplyQueue queue;
    private final Writer writer;
    private MessageTrace trace = MessageTrace.OFF;

    /** The message trace (TRACE_MESSAGES); off unless configured. */
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setTrace(MessageTrace trace) {
        this.trace = trace;
    }

    public UploadService(ControllerProperties properties, MetadataValidator metadataValidator, ClusterState cluster,
                         SafeMode safeMode, PlacementPolicy placement, WorkerClient workers, WorkerIo io,
                         ObjectRepository objects, ObjectViewAssembler views, Clock clock, Receipts receipts,
                         NamespaceLock namespaceLock, NamespaceTasks namespaceTasks, ApplyQueue queue, Writer writer) {
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
        this.namespaceLock = namespaceLock;
        this.namespaceTasks = namespaceTasks;
        this.queue = queue;
        this.writer = writer;
    }

    /**
     * Checks that a write can be accepted at all, before the body is read: not in safe mode, and
     * at least REPLICATION_FACTOR write-eligible workers.
     */
    public void checkWritable() {
        safeMode.requireWritable();
        int eligible = cluster.read(() -> placement.writeEligibleWorkers().size());
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
     * Idempotency-Keys belong to the account that sends them: two clients choosing the same key
     * never see each other's objects. Stored as {@code <owner id>:<key>}.
     */
    static String scopedKey(String ownerId, String idempotencyKey) {
        return idempotencyKey == null || ownerId == null ? idempotencyKey : ownerId + ":" + idempotencyKey;
    }

    /**
     * If an earlier upload already used this key, the object it created, provided the new
     * request is for a file of the same size. A different size under the same key is a client
     * error: 409 {@code conflict}. Only the multipart part's size is consulted: nothing is sent
     * to any worker.
     */
    public java.util.Optional<UploadResult> replay(String idempotencyKey, String ownerId,
                                                   java.util.function.LongSupplier newSize) {
        if (idempotencyKey == null) {
            return java.util.Optional.empty();
        }
        return objects.findIdempotent(scopedKey(ownerId, idempotencyKey)).map(previous -> {
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

    /** Stores an object owned by {@code ownerId} (the uploading account). */
    public UploadResult upload(MultipartFile file, String rawMetadata, String idempotencyKey, String ownerId) {
        Map<String, String> metadata = metadataValidator.parseForUpload(rawMetadata);
        String name = objectName(file.getOriginalFilename());
        String contentType = contentType(file.getContentType(), name);

        String objectId = UUID.randomUUID().toString();
        long startedNanos = System.nanoTime();
        Set<WrittenCopy> attempted = ConcurrentHashMap.newKeySet();
        Set<String> exclude = ConcurrentHashMap.newKeySet();
        boolean committed = false;

        cluster.write(() -> cluster.beginUpload(objectId));
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
                List<Holder> holders = storeChunk(objectId, index, chunkId, piece, checksum, exclude, attempted);
                stored.add(new StoredChunk(new Chunk(chunkId, objectId, index, piece.length, checksum), holders));
            }

            Instant now = clock.instant();
            StoredObject object = new StoredObject(objectId, name, size, contentType, Checksums.hex(objectDigest),
                    stored.size(), now, now, ownerId);
            var beforeCommit = beforeCommitForTests;
            if (beforeCommit != null) {
                beforeCommit.accept(objectId, stored.stream().flatMap(c -> c.holders().stream())
                        .map(h -> h.target().workerId()).distinct().toList());
            }
            try {
                commit(object, metadata, stored, scopedKey(ownerId, idempotencyKey));
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
            if (!committed) {
                endUpload(objectId, attempted);
            }
        }
    }

    /**
     * Commits the namespace (v4 §5.6 step 4): under the namespace lock, the idempotency check, the
     * durable SQLite transaction and queuing {@code CommitObject}; then, with no lock held, a wait
     * of at most UPLOAD_COMMIT_WAIT for the task. If the wait times out the object is still
     * durable, so the upload answers 201 anyway (counted as {@code upload_commit_wait_timeouts});
     * a download meanwhile waits for the mirror (DOWNLOAD_SETTLE_WAIT).
     *
     * <p>The task records the copies (§5.6 step 5): a receipt becomes a replica, a copy without one
     * becomes an UPLOAD pending copy, a copy whose worker died or re-registered meanwhile is not
     * recorded at all; never a replica just because the PUT answered 2xx.
     */
    private void commit(StoredObject object, Map<String, String> metadata, List<StoredChunk> stored, String idempotencyKey) {
        List<NamespaceTasks.CommittedChunk> chunks = stored.stream()
                .map(entry -> new NamespaceTasks.CommittedChunk(entry.chunk(), entry.holders().stream()
                        .map(h -> new NamespaceTasks.Target(h.target().workerId(), h.target().epoch(), h.version())).toList()))
                .toList();
        Task task = namespaceTasks.commitObject(object.objectId(), chunks);
        namespaceLock.lock();
        try {
            // Every commit holds the namespace lock, so this check and the insert cannot interleave
            // with another commit using the same key.
            if (idempotencyKey != null) {
                objects.findIdempotent(idempotencyKey).ifPresent(winner -> {
                    throw new LostIdempotencyRace(winner);
                });
            }
            try {
                objects.insert(object, metadata, stored.stream().map(StoredChunk::chunk).toList(), idempotencyKey);
            } catch (ObjectRepository.IdempotencyConflictException lost) {
                throw new LostIdempotencyRace(objects.findIdempotent(idempotencyKey).orElseThrow());
            }
            var afterInsert = afterInsertForTests;
            if (afterInsert != null) {
                afterInsert.accept(object.objectId());
            }
            putUninterruptibly(task);
        } finally {
            namespaceLock.unlock();
        }
        try {
            writer.await(task, properties.uploadCommitWait());
        } catch (java.util.concurrent.TimeoutException e) {
            cluster.increment("upload_commit_wait_timeouts");
            log.warn("[upload] object={} is committed but its CommitObject task did not finish within {} s; answering anyway",
                    object.objectId(), properties.uploadCommitWaitSeconds());
        } catch (RuntimeException e) {
            log.warn("[upload] object={} is committed; applying it to memory failed ({}), so it is reconciled",
                    object.objectId(), e.toString());
        }
    }

    /** §5.6 step 6: an upload that did not commit queues {@code EndUpload} (DELETEs, garbage, receipts). */
    private void endUpload(String objectId, Set<WrittenCopy> attempted) {
        List<NamespaceTasks.Attempt> attempts = attempted.stream()
                .map(copy -> new NamespaceTasks.Attempt(copy.chunkId(), copy.target().workerId()))
                .toList();
        putUninterruptibly(namespaceTasks.endUpload(objectId, attempts));
        if (!attempts.isEmpty()) {
            log.warn("[upload] failed object={}; DELETE queued for {} chunk copies written so far", objectId, attempts.size());
        }
    }

    private void putUninterruptibly(Task task) {
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
    }

    /**
     * Stores one chunk on REPLICATION_FACTOR different workers. The intended holders are written
     * in parallel; a failed target is excluded for the rest of the upload and the next one on the
     * ring is tried instead.
     */
    private List<Holder> storeChunk(String objectId, int index, String chunkId, byte[] data, String checksum,
                                    Set<String> exclude, Set<WrittenCopy> attempted) {
        int rf = properties.replicationFactor();
        List<Target> ring = cluster.read(() -> {
            List<Target> targets = new ArrayList<>();
            for (WorkerInfo worker : placement.ringForUpload(objectId, index, exclude)) {
                targets.add(new Target(worker.workerId, worker.url, worker.bootId, worker.epoch));
            }
            return targets;
        });
        List<Holder> holders = new ArrayList<>(rf);
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
            List<Long> versions = io.mapAll(batch, target -> tryStore(target, chunkId, data, checksum, attempted));
            for (int i = 0; i < batch.size(); i++) {
                Long version = versions.get(i);
                if (version != null && version > 0) {
                    holders.add(new Holder(batch.get(i), version));
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

    /**
     * One PUT (v5 §7): a fresh version is reserved under the cluster read lock together with the
     * eligibility check (ALIVE, reported, not resyncing, same epoch), so a PUT that reaches a worker
     * which started resyncing afterwards has {@code v < t0}: it was either committed before the fence
     * (and listed) or is rejected as stale. A 409, 503 or 500 counts as a failed target.
     *
     * @return the version the copy was installed at, or -1
     */
    private long tryStore(Target target, String chunkId, byte[] data, String checksum, Set<WrittenCopy> attempted) {
        long version = cluster.read(() -> {
            WorkerInfo worker = cluster.worker(target.workerId());
            return worker != null && worker.epoch == target.epoch() && placement.isWriteEligible(worker)
                    ? cluster.versions().next() : -1L;
        });
        if (version < 0) {
            log.info("[upload] chunk={} worker={} is no longer write-eligible; replacing it", chunkId, target.workerId());
            return -1;
        }
        // Recorded before the call: a write that timed out may still have reached the disk.
        attempted.add(new WrittenCopy(target, chunkId));
        try {
            String changeAck = cluster.read(() -> receipts.changeAck(target.workerId()));
            trace.putChunk(target.workerId(), chunkId, data.length);
            ChunkPutResponse answer = workers.putChunk(target.url(), chunkId, data, checksum, version, changeAck);
            trace.putAnswer(target.workerId(), chunkId, answer == null || answer.changes() == null ? null
                    : answer.changes().items(), null);
            if (answer != null && answer.changes() != null) {
                receipts.acceptPiggyback(target.workerId(), answer.changes().bootId(), answer.changes().items());
            }
            return version;
        } catch (WorkerException e) {
            trace.putAnswer(target.workerId(), chunkId, null, e.getMessage());
            log.warn("[upload] chunk={} worker={} write failed ({}); replacing it for this upload",
                    chunkId, target.workerId(), e.getMessage());
            WorkerInfo worker = cluster.worker(target.workerId());
            if (worker != null) {
                worker.lastDataError = System.nanoTime();          // a volatile field: no lock
            }
            return -1;
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
