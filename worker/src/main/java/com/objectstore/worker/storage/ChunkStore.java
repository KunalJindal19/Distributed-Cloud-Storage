package com.objectstore.worker.storage;

import com.objectstore.common.Checksums;
import com.objectstore.common.ChunkIds;
import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.ChunkInfo;
import com.objectstore.common.dto.ReportEntry;
import com.objectstore.common.dto.WorkerStats;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.reporting.ChangeLog;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * The worker as a versioned store (v5 §4; the DataNode's block storage). Each chunk is two files
 * directly under {@code DATA_DIR/chunks}: the data, named after the chunk id, and a
 * {@code <chunk_id>.meta} sidecar ({@code <sha256> <size>}). Copies that failed verification move to
 * {@code DATA_DIR/quarantine/<chunk_id>.<n>.bad} (same volume: an atomic rename), are never indexed
 * and never deleted by a command.
 *
 * <p><b>Records and versions.</b> In memory every chunk has at most one record: a copy in
 * {@link #index} (size, checksum, version) or a tombstone (the version it was deleted at). Every
 * PUT and DELETE carries an operation version and passes {@link #check} under the chunk's stripe
 * lock: newer than both the record and the fence → applied; equal to the record of the same kind →
 * REPEAT (the same result is logged again, so a lost answer is re-delivered); anything else →
 * STALE ({@code CMD_FAILED(stale)}, nothing changes). Tombstones are kept until a fence covers them,
 * never just because their {@code DELETED} was acknowledged.
 *
 * <p><b>The fence</b> is adopted from the controller under all stripes ({@link #adoptFence}); until
 * the first registration of this process ({@code fenceSet = false}) every versioned operation is
 * STALE.
 *
 * <p><b>Local observations</b> (a corrupt copy, a vanished file, a file placed by hand) use the odd
 * {@code localVersion = max(record, fence) | 1}: never equal to a controller version, they never
 * complete a controller operation. A verdict found outside the stripe is applied only under it,
 * after checking that the record has not changed and re-hashing.
 *
 * <p><b>Installing a copy</b>: data to a temporary file + fsync; sidecar to a temporary file +
 * fsync; sidecar renamed into place + directory fsync; data renamed into place, the commit point;
 * directory fsync; index; {@code RECEIVED(v)}. A failure before the commit point cleans up and
 * answers 500; at or after it the worker fail-stops ({@link FailStop}). A copy is never
 * overwritten in place.
 *
 * <p>Every change is appended to the {@link ChangeLog} under the stripe, after the index reflects it.
 */
@Component
public class ChunkStore {

    private static final Logger log = LoggerFactory.getLogger(ChunkStore.class);
    private static final String META_SUFFIX = ".meta";
    private static final String TMP_SUFFIX = ".tmp";
    private static final String BAD_SUFFIX = ".bad";
    private static final long WRITE_HEADROOM = 1_048_576;
    private static final int LOCK_STRIPES = 64;
    /** A file modified more recently than this may be a write in progress: reconciliation leaves it alone. */
    public static final java.time.Duration SETTLE_TIME = java.time.Duration.ofSeconds(5);

    /** The result of the version check (v5 §4.2). */
    public enum Verdict { ACCEPT, REPEAT, STALE }

    public enum PutOutcome { CREATED, REVERIFIED, REPEAT, STALE }

    public enum DeleteOutcome { DELETED, REPEAT, STALE, IO_ERROR, REFUSED }

    public enum VerifyOutcome { PASS, FAILED, MISSING, UNAVAILABLE }

    /** A chunk as read from disk; only bytes that match their sidecar are ever returned. */
    public record StoredChunk(byte[] data, String checksum, long version) {
    }

    /**
     * A chunk read for a transfer, without the stripe: {@code version} of the record when the read
     * began (-1 if not indexed), the bytes (null if the file is missing), the sidecar checksum, and
     * whether the bytes match the sidecar.
     */
    public record LocalCopy(long version, byte[] data, String sidecarChecksum, boolean intact) {
    }

    public record Meta(String checksum, long size) {
    }

    /** One chunk in the index: its size, the checksum from its sidecar, and its record version. */
    public record Entry(long size, String checksum, long version) {
    }

    /** What one reconciliation of the index with the directory changed. */
    public record Reconciled(int added, int removed) {

        public boolean isEmpty() {
            return added + removed == 0;
        }
    }

    /** Test hook: called at named steps of the install, delete and quarantine paths. */
    @FunctionalInterface
    public interface Faults {
        void at(String step) throws IOException;

        Faults NONE = step -> { };
    }

    public static class InvalidChunkIdException extends RuntimeException {
        public InvalidChunkIdException(String chunkId) {
            super("'" + chunkId + "' is not a valid chunk id");
        }
    }

    public static class ChecksumMismatchException extends RuntimeException {
        public ChecksumMismatchException(String expected, String actual) {
            super("body hashes to " + actual + " but X-Checksum says " + expected);
        }
    }

    public static class InsufficientStorageException extends RuntimeException {
        public InsufficientStorageException(long needed, long free) {
            super("need " + needed + " bytes but only " + free + " are free");
        }
    }

    /** 503: the worker is shutting down or has fail-stopped. */
    public static class UnavailableException extends RuntimeException {
        public UnavailableException(String message) {
            super(message);
        }
    }

    /** 409 chunk_corrupt: the copy failed verification and is in quarantine. */
    public static class CorruptChunkException extends RuntimeException {
        public CorruptChunkException(String chunkId) {
            super("chunk " + chunkId + " is corrupt on this worker (quarantined)");
        }
    }

    /** 500: a disk error before the commit point; nothing changed. */
    public static class StoreFailedException extends RuntimeException {
        public StoreFailedException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private final Path chunkDir;
    private final Path quarantineDir;
    private final ChangeLog changes;
    private final FailStop failStop;
    private final MutationGuard guard;
    private final ReentrantLock[] locks = new ReentrantLock[LOCK_STRIPES];
    private final ConcurrentHashMap<String, Entry> index = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> tombstones = new ConcurrentHashMap<>();
    /** Chunks whose last record is a quarantine (a GET answers 409 chunk_corrupt). */
    private final Set<String> quarantinedIds = ConcurrentHashMap.newKeySet();
    /** (chunk, version) pairs being received right now. */
    private final Set<String> incoming = ConcurrentHashMap.newKeySet();
    private final AtomicLong chunkCount = new AtomicLong();
    private final AtomicLong usedBytes = new AtomicLong();
    private final AtomicLong quarantinedChunks = new AtomicLong();
    private final AtomicLong quarantinedBytes = new AtomicLong();
    private final AtomicLong quarantineNumber = new AtomicLong();
    /** STORAGE_CAPACITY in bytes (chunks + quarantine); 0: no cap, only the volume's free space counts. */
    private final long capacity;
    private volatile long fence;
    private volatile boolean fenceSet;
    private volatile Faults faults = Faults.NONE;

    @Autowired
    public ChunkStore(WorkerProperties properties, ChangeLog changes, FailStop failStop, MutationGuard guard) {
        this.chunkDir = properties.chunkDir().toAbsolutePath().normalize();
        this.quarantineDir = properties.quarantineDir().toAbsolutePath().normalize();
        this.capacity = properties.storageCapacity().toBytes();
        this.changes = changes;
        this.failStop = failStop;
        this.guard = guard;
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new ReentrantLock(true);              // fair (v5 §2)
        }
    }

    /** For tests: no exit on fail-stop, no shutdown drain. */
    public ChunkStore(WorkerProperties properties, ChangeLog changes) {
        this(properties, changes, new FailStop(), new MutationGuard(0));
    }

    public void setFaultsForTests(Faults faults) {
        this.faults = faults == null ? Faults.NONE : faults;
    }

    // ---- start-up recovery (v5 §4.7) ---------------------------------------------------------

    /**
     * One walk of the chunk directory: temporary files and sidecars without data (left by a crash
     * before a commit point) are deleted; data without a sidecar (only from v4 or placed by hand) is
     * hashed and gets one; every data + sidecar pair is indexed at version 0. The quarantine is
     * counted. There is no fence and no tombstone until the worker registers.
     */
    @PostConstruct
    void initialise() throws IOException {
        Files.createDirectories(chunkDir);
        Files.createDirectories(quarantineDir);
        int tmp = 0;
        int rebuilt = 0;
        int strayMeta = 0;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(chunkDir)) {
            for (Path file : files) {
                String name = file.getFileName().toString();
                if (name.endsWith(TMP_SUFFIX)) {
                    Files.deleteIfExists(file);
                    tmp++;
                } else if (name.endsWith(META_SUFFIX)) {
                    String chunkId = name.substring(0, name.length() - META_SUFFIX.length());
                    if (!ChunkIds.isValid(chunkId) || !Files.exists(chunkDir.resolve(chunkId))) {
                        Files.deleteIfExists(file);
                        strayMeta++;
                    }
                } else if (ChunkIds.isValid(name)) {
                    Optional<Meta> meta = readMeta(name);
                    if (meta.isEmpty()) {
                        byte[] data = Files.readAllBytes(file);
                        meta = Optional.of(new Meta(Checksums.sha256Hex(data), data.length));
                        writeMetaReplacing(name, meta.get());
                        rebuilt++;
                    }
                    indexPut(name, new Entry(meta.get().size(), meta.get().checksum(), 0));
                }
            }
        }
        FsyncSupport.fsyncDirectory(chunkDir);
        long maxNumber = 0;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(quarantineDir)) {
            for (Path file : files) {
                String name = file.getFileName().toString();
                if (!name.endsWith(BAD_SUFFIX)) {
                    continue;
                }
                quarantinedChunks.incrementAndGet();
                quarantinedBytes.addAndGet(Files.size(file));
                String[] parts = name.split("\\.");
                if (parts.length >= 3) {
                    try {
                        maxNumber = Math.max(maxNumber, Long.parseLong(parts[parts.length - 2]));
                    } catch (NumberFormatException ignored) {
                        // a file renamed by hand
                    }
                }
            }
        }
        quarantineNumber.set(maxNumber);
        if (tmp + rebuilt + strayMeta > 0) {
            log.warn("[store] start-up recovery: {} temporary files removed, {} sidecars rebuilt, {} stray sidecars removed",
                    tmp, rebuilt, strayMeta);
        }
        log.info("[store] chunk directory {} ready with {} chunks ({} bytes); {} quarantined copies ({} bytes)",
                chunkDir, chunkCount.get(), usedBytes.get(), quarantinedChunks.get(), quarantinedBytes.get());
        if (capacity > 0) {
            long stored = usedBytes.get() + quarantinedBytes.get();
            if (stored > capacity) {
                log.warn("[store] STORAGE_CAPACITY is {} bytes but {} are already stored: this worker takes no new"
                        + " copies (it still serves and deletes the ones it has)", capacity, stored);
            } else {
                log.info("[store] STORAGE_CAPACITY {} bytes: {} stored, {} left", capacity, stored, capacity - stored);
            }
        }
    }

    // ---- versions and the fence ----------------------------------------------------------------

    /** The version check (v5 §4.2). Must be called under the chunk's stripe. */
    Verdict check(String chunkId, long version, boolean put) {
        if (!fenceSet) {
            return Verdict.STALE;
        }
        Entry entry = index.get(chunkId);
        Long tomb = tombstones.get(chunkId);
        long current = entry != null ? entry.version() : tomb != null ? tomb : 0;
        if (version <= Math.max(current, fence)) {
            boolean sameKind = put ? entry != null : entry == null && tomb != null;
            return version == current && sameKind ? Verdict.REPEAT : Verdict.STALE;
        }
        return Verdict.ACCEPT;
    }

    /** The odd version of a local observation: {@code max(record, fence) | 1}. Under the stripe. */
    long localVersion(String chunkId) {
        Entry entry = index.get(chunkId);
        Long tomb = tombstones.get(chunkId);
        long current = entry != null ? entry.version() : tomb != null ? tomb : 0;
        return Math.max(current, fence) | 1;
    }

    public long fence() {
        return fence;
    }

    public boolean isFenceSet() {
        return fenceSet;
    }

    /**
     * Adopts a fence (v5 §5.3): takes every stripe in index order (so every mutation in progress
     * finishes first), sets the fence, removes the tombstones it covers, runs {@code underStripes}
     * (the change log's {@code beginHold}, for a heartbeat resync) and releases the stripes.
     *
     * @return what {@code underStripes} returned, or -1 if it is null
     */
    public long adoptFence(long t0, LongSupplier underStripes) {
        for (ReentrantLock lock : locks) {
            lock.lock();
        }
        try {
            fence = t0;
            fenceSet = true;
            int before = tombstones.size();
            tombstones.values().removeIf(version -> version <= t0);
            long result = underStripes == null ? -1 : underStripes.getAsLong();
            log.info("[store] fence {} adopted; {} tombstones it covers removed ({} left)", t0,
                    before - tombstones.size(), tombstones.size());
            return result;
        } finally {
            for (int i = locks.length - 1; i >= 0; i--) {
                locks[i].unlock();
            }
        }
    }

    public int tombstoneCount() {
        return tombstones.size();
    }

    /** The tombstone version of a chunk, or null. */
    public Long tombstone(String chunkId) {
        return tombstones.get(chunkId);
    }

    // ---- receiving (PUT) -----------------------------------------------------------------------

    /** Marks {@code (chunk, version)} as being received; false if it already is (409 in_progress). */
    public boolean beginIncoming(String chunkId, long version) {
        return incoming.add(chunkId + "@" + version);
    }

    public void endIncoming(String chunkId, long version) {
        incoming.remove(chunkId + "@" + version);
    }

    /** True if new mutations are refused (shutting down or fail-stopped). */
    public boolean isClosing() {
        return guard.isClosing() || failStop.failed();
    }

    /**
     * Installs a copy at {@code version} (v5 §4.3). The caller read the body (at most one chunk).
     *
     * @throws ChecksumMismatchException    if the bytes do not hash to {@code expectedChecksum}
     * @throws InsufficientStorageException if the volume lacks room
     * @throws UnavailableException         if the worker is closing or failed (503)
     * @throws StoreFailedException         on a disk error before the commit point (500)
     */
    public PutOutcome put(String chunkId, long version, byte[] data, String expectedChecksum) {
        Path target = pathFor(chunkId);
        failStop.check();
        String actual = Checksums.sha256Hex(data);
        if (!actual.equals(expectedChecksum)) {
            throw new ChecksumMismatchException(expectedChecksum, actual);
        }
        long onVolume = volumeFreeBytes();
        if (onVolume < data.length + WRITE_HEADROOM) {
            throw new InsufficientStorageException(data.length + WRITE_HEADROOM, onVolume);
        }
        if (capacity > 0) {
            // A copy already held (a repeat, a re-verify) needs no new room under the cap.
            Entry held = index.get(chunkId);
            long room = capacityLeft() + (held == null ? 0 : held.size());
            if (room < data.length) {
                throw new InsufficientStorageException(data.length, room);
            }
        }
        if (!guard.enter()) {
            throw new UnavailableException("this worker is shutting down");
        }
        try {
            ReentrantLock lock = lockFor(chunkId);
            lock.lock();
            try {
                failStop.check();
                Verdict verdict = check(chunkId, version, true);
                if (verdict == Verdict.REPEAT) {
                    Entry entry = index.get(chunkId);
                    changes.received(chunkId, entry.size(), entry.checksum(), version, null);
                    return PutOutcome.REPEAT;
                }
                if (verdict == Verdict.STALE) {
                    changes.cmdFailed(chunkId, version, InternalApi.FAILED_STALE, null, null,
                            "put not newer than " + describeRecord(chunkId));
                    return PutOutcome.STALE;
                }
                if (index.containsKey(chunkId) || Files.exists(target)) {
                    Optional<Meta> meta = readMeta(chunkId);
                    String stored = hashOrNull(target);
                    if (meta.isPresent() && expectedChecksum.equals(meta.get().checksum())
                            && expectedChecksum.equals(stored) && meta.get().size() == data.length) {
                        // The bytes are already here and intact: no disk write, only the new version.
                        indexPut(chunkId, new Entry(data.length, expectedChecksum, version));
                        tombstones.remove(chunkId);
                        quarantinedIds.remove(chunkId);
                        changes.received(chunkId, data.length, expectedChecksum, version, null);
                        return PutOutcome.REVERIFIED;
                    }
                    log.warn("[store] chunk={} stored copy fails its sidecar or the expected checksum; quarantining it "
                            + "before installing the verified bytes", chunkId);
                    quarantineLocked(chunkId, localVersion(chunkId), "replaced by a verified copy");
                }
                install(chunkId, target, data, expectedChecksum, version);
                return PutOutcome.CREATED;
            } finally {
                lock.unlock();
            }
        } finally {
            guard.exit();
        }
    }

    /** The new-copy order (v5 §4.3 step 4). Under the stripe. */
    private void install(String chunkId, Path target, byte[] data, String checksum, long version) {
        String unique = UUID.randomUUID().toString();
        Path dataTmp = chunkDir.resolve(chunkId + "." + unique + TMP_SUFFIX);
        Path metaTmp = chunkDir.resolve(chunkId + META_SUFFIX + "." + unique + TMP_SUFFIX);
        Path meta = metaPath(chunkId);
        try {
            faults.at("data-temp");
            FsyncSupport.writeAndForce(dataTmp, data);
        } catch (IOException e) {
            deleteQuietly(dataTmp);
            throw new StoreFailedException("could not write chunk " + chunkId, e);
        }
        try {
            faults.at("meta-temp");
            FsyncSupport.writeAndForce(metaTmp, metaBytes(new Meta(checksum, data.length)));
        } catch (IOException e) {
            deleteQuietly(dataTmp);
            deleteQuietly(metaTmp);
            throw new StoreFailedException("could not write the sidecar of " + chunkId, e);
        }
        try {
            faults.at("meta-rename");
            Files.move(metaTmp, meta, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            FsyncSupport.fsyncDirectory(chunkDir);
        } catch (IOException e) {
            deleteQuietly(dataTmp);
            deleteQuietly(metaTmp);
            deleteQuietly(meta);
            throw new StoreFailedException("could not install the sidecar of " + chunkId, e);
        }
        try {
            faults.at("data-rename");                                 // the commit point
            Files.move(dataTmp, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            failStop.trigger(chunkId, "install: data rename (commit point)", e);
            throw new UnavailableException("disk failure while installing " + chunkId);
        }
        try {
            faults.at("after-commit");
            FsyncSupport.fsyncDirectory(chunkDir);
        } catch (IOException e) {
            failStop.trigger(chunkId, "install: directory fsync after the commit point", e);
            throw new UnavailableException("disk failure while installing " + chunkId);
        }
        indexPut(chunkId, new Entry(data.length, checksum, version));
        tombstones.remove(chunkId);
        quarantinedIds.remove(chunkId);
        changes.received(chunkId, data.length, checksum, version, null);
    }

    // ---- deleting (DELETE command, v5 §4.4) ----------------------------------------------------------

    /**
     * Removes the copy as of {@code version}: reason {@code corrupt} quarantines the bytes (if any),
     * any other reason unlinks data and sidecar. The tombstone becomes {@code version} and
     * {@code DELETED(version)} is logged. A failure before the unlink logs
     * {@code CMD_FAILED(io_error)}; at or after it the worker fail-stops.
     */
    public DeleteOutcome delete(String chunkId, long version, boolean corrupt, String cmdId) {
        Path target = pathFor(chunkId);
        if (failStop.failed() || !guard.enter()) {
            return DeleteOutcome.REFUSED;
        }
        try {
            ReentrantLock lock = lockFor(chunkId);
            lock.lock();
            try {
                if (failStop.failed()) {
                    return DeleteOutcome.REFUSED;
                }
                Verdict verdict = check(chunkId, version, false);
                if (verdict == Verdict.REPEAT) {
                    changes.deleted(chunkId, version, quarantinedIds.contains(chunkId)
                            ? InternalApi.DELETED_CORRUPT : InternalApi.DELETED_DELETED);
                    return DeleteOutcome.REPEAT;
                }
                if (verdict == Verdict.STALE) {
                    changes.cmdFailed(chunkId, version, InternalApi.FAILED_STALE, null, cmdId,
                            "delete not newer than " + describeRecord(chunkId));
                    return DeleteOutcome.STALE;
                }
                boolean exists = Files.exists(target);
                try {
                    faults.at("delete-prepare");
                } catch (IOException e) {
                    changes.cmdFailed(chunkId, version, InternalApi.FAILED_IO_ERROR, null, cmdId, e.toString());
                    return DeleteOutcome.IO_ERROR;
                }
                if (corrupt && exists) {
                    quarantineLocked(chunkId, version, "the controller found it corrupt");   // logs DELETED(v, corrupt)
                    return DeleteOutcome.DELETED;
                }
                try {
                    faults.at("delete-unlink");                              // the commit point
                    Files.deleteIfExists(target);
                } catch (IOException e) {
                    failStop.trigger(chunkId, "delete: unlink (commit point)", e);
                    throw new UnavailableException("disk failure while deleting " + chunkId);
                }
                try {
                    Files.deleteIfExists(metaPath(chunkId));
                    FsyncSupport.fsyncDirectory(chunkDir);
                } catch (IOException e) {
                    failStop.trigger(chunkId, "delete: sidecar unlink after the commit point", e);
                    throw new UnavailableException("disk failure while deleting " + chunkId);
                }
                indexRemove(chunkId);
                tombstones.put(chunkId, version);
                changes.deleted(chunkId, version, corrupt ? InternalApi.DELETED_CORRUPT : InternalApi.DELETED_DELETED);
                return DeleteOutcome.DELETED;
            } finally {
                lock.unlock();
            }
        } finally {
            guard.exit();
        }
    }

    // ---- quarantine and other local observations (v5 §4.5) --------------------------------------------

    /**
     * Moves the data to the quarantine, deletes the sidecar, sets the tombstone and logs
     * {@code DELETED(tombVersion, corrupt)}. Under the stripe. The rename is the commit point.
     */
    private void quarantineLocked(String chunkId, long tombVersion, String why) {
        Path data = chunkDir.resolve(chunkId);
        long size = 0;
        boolean moved = false;
        if (Files.exists(data)) {
            Path bad = quarantineDir.resolve(chunkId + "." + quarantineNumber.incrementAndGet() + BAD_SUFFIX);
            try {
                size = Files.size(data);
                faults.at("quarantine-rename");
                Files.move(data, bad, StandardCopyOption.ATOMIC_MOVE);
                moved = true;
            } catch (IOException e) {
                failStop.trigger(chunkId, "quarantine: rename (commit point)", e);
                throw new UnavailableException("disk failure while quarantining " + chunkId);
            }
        }
        try {
            Files.deleteIfExists(metaPath(chunkId));
            FsyncSupport.fsyncDirectory(chunkDir);
            FsyncSupport.fsyncDirectory(quarantineDir);
        } catch (IOException e) {
            failStop.trigger(chunkId, "quarantine: sidecar unlink after the commit point", e);
            throw new UnavailableException("disk failure while quarantining " + chunkId);
        }
        if (moved) {
            quarantinedChunks.incrementAndGet();
            quarantinedBytes.addAndGet(size);
        }
        indexRemove(chunkId);
        tombstones.put(chunkId, tombVersion);
        quarantinedIds.add(chunkId);
        log.warn("[store] chunk={} QUARANTINED ({}); tombstone {}", chunkId, why, tombVersion);
        changes.deleted(chunkId, tombVersion, InternalApi.DELETED_CORRUPT);
    }

    /**
     * Applies a corruption verdict found outside the stripe: under the stripe, if the record still
     * has {@code observedVersion}, the file is hashed again; if it still fails its sidecar (or the
     * sidecar differs from {@code expected}, when given) it is quarantined at {@code localVersion}.
     * A vanished file is recorded as missing instead.
     *
     * @return true if the copy was quarantined (or found missing)
     */
    public boolean quarantineIfBad(String chunkId, long observedVersion, String expected) {
        pathFor(chunkId);
        if (failStop.failed() || !guard.enter()) {
            return false;
        }
        try {
            ReentrantLock lock = lockFor(chunkId);
            lock.lock();
            try {
                Entry entry = index.get(chunkId);
                if (entry == null || entry.version() != observedVersion) {
                    return false;                               // the record changed: the verdict is void
                }
                Path data = chunkDir.resolve(chunkId);
                if (!Files.exists(data)) {
                    missingLocked(chunkId);
                    return true;
                }
                if (intact(chunkId, data, expected)) {
                    return false;                               // passes now (it was replaced meanwhile)
                }
                quarantineLocked(chunkId, localVersion(chunkId), expected == null ? "bytes do not match the sidecar"
                        : "bytes or sidecar do not match the expected checksum");
                return true;
            } finally {
                lock.unlock();
            }
        } finally {
            guard.exit();
        }
    }

    /** An indexed chunk whose file is gone: re-checked under the stripe, then {@code DELETED(odd, missing)}. */
    public boolean observeMissing(String chunkId, long observedVersion) {
        pathFor(chunkId);
        if (failStop.failed() || !guard.enter()) {
            return false;
        }
        try {
            ReentrantLock lock = lockFor(chunkId);
            lock.lock();
            try {
                Entry entry = index.get(chunkId);
                if (entry == null || entry.version() != observedVersion || Files.exists(chunkDir.resolve(chunkId))) {
                    return false;
                }
                missingLocked(chunkId);
                return true;
            } finally {
                lock.unlock();
            }
        } finally {
            guard.exit();
        }
    }

    private void missingLocked(String chunkId) {
        long version = localVersion(chunkId);
        try {
            Files.deleteIfExists(metaPath(chunkId));
        } catch (IOException ignored) {
            // a stray sidecar is removed by the next start-up recovery
        }
        indexRemove(chunkId);
        tombstones.put(chunkId, version);
        log.warn("[store] chunk={} is gone from the disk; tombstone {}", chunkId, version);
        changes.deleted(chunkId, version, InternalApi.DELETED_MISSING);
    }

    /** True if the file hashes to its sidecar (and the sidecar is {@code expected}, when given). */
    private boolean intact(String chunkId, Path data, String expected) {
        Optional<Meta> meta = readMeta(chunkId);
        String actual = hashOrNull(data);
        try {
            return meta.isPresent() && actual != null && meta.get().checksum().equals(actual)
                    && meta.get().size() == Files.size(data) && (expected == null || expected.equals(meta.get().checksum()));
        } catch (IOException e) {
            return false;
        }
    }

    // ---- reading -----------------------------------------------------------------------------------

    /**
     * Reads a chunk for a GET and verifies it against its sidecar. Bytes that fail are quarantined
     * (under the stripe, after a re-check) and never served.
     *
     * @return empty if the chunk is not here
     * @throws CorruptChunkException if the copy is (now) in quarantine
     */
    public Optional<StoredChunk> read(String chunkId) {
        Path target = pathFor(chunkId);
        failStop.check();
        Entry entry = index.get(chunkId);
        if (entry == null) {
            if (quarantinedIds.contains(chunkId)) {
                throw new CorruptChunkException(chunkId);
            }
            return Optional.empty();
        }
        byte[] data;
        try {
            data = Files.readAllBytes(target);
        } catch (NoSuchFileException e) {
            observeMissing(chunkId, entry.version());
            return Optional.empty();
        } catch (IOException e) {
            throw new UncheckedIOException("could not read chunk " + chunkId, e);
        }
        String actual = Checksums.sha256Hex(data);
        Optional<Meta> meta = readMeta(chunkId);
        if (meta.isEmpty() || !meta.get().checksum().equals(actual) || meta.get().size() != data.length) {
            if (quarantineIfBad(chunkId, entry.version(), null)) {
                throw new CorruptChunkException(chunkId);
            }
            throw new UnavailableException("chunk " + chunkId + " changed while it was read; try again");
        }
        return Optional.of(new StoredChunk(data, actual, entry.version()));
    }

    /** Reads a copy for a REPLICATE, without the stripe (v5 §4.6). */
    public LocalCopy readLocal(String chunkId) {
        Path target = pathFor(chunkId);
        failStop.check();
        Entry entry = index.get(chunkId);
        long version = entry == null ? -1 : entry.version();
        byte[] data;
        try {
            data = Files.readAllBytes(target);
        } catch (NoSuchFileException e) {
            return new LocalCopy(version, null, null, false);
        } catch (IOException e) {
            return new LocalCopy(version, null, null, false);
        }
        Optional<Meta> meta = readMeta(chunkId);
        boolean intact = meta.isPresent() && meta.get().checksum().equals(Checksums.sha256Hex(data))
                && meta.get().size() == data.length;
        return new LocalCopy(version, data, meta.map(Meta::checksum).orElse(null), intact);
    }

    /**
     * The VERIFY command (v5 §4.6): hashes the copy and compares it with its sidecar and
     * {@code expected}. Pass → {@code RECEIVED(current version, cmdId)}, logged under the stripe
     * after checking the record did not change. Fail → quarantine. Missing → {@code DELETED(odd,
     * missing)} if it was indexed. Cannot run → nothing is logged.
     */
    public VerifyOutcome verifyCopy(String chunkId, String expected, String cmdId) {
        Path target = pathFor(chunkId);
        if (failStop.failed()) {
            return VerifyOutcome.UNAVAILABLE;
        }
        Entry entry = index.get(chunkId);
        if (entry == null) {
            return VerifyOutcome.MISSING;
        }
        if (!Files.exists(target)) {
            return observeMissing(chunkId, entry.version()) ? VerifyOutcome.MISSING : VerifyOutcome.UNAVAILABLE;
        }
        if (!intact(chunkId, target, expected)) {
            return quarantineIfBad(chunkId, entry.version(), expected) ? VerifyOutcome.FAILED : VerifyOutcome.UNAVAILABLE;
        }
        if (!guard.enter()) {
            return VerifyOutcome.UNAVAILABLE;
        }
        try {
            ReentrantLock lock = lockFor(chunkId);
            lock.lock();
            try {
                Entry now = index.get(chunkId);
                if (now == null || now.version() != entry.version()) {
                    return VerifyOutcome.UNAVAILABLE;            // changed meanwhile: no credit, no harm
                }
                changes.received(chunkId, now.size(), now.checksum(), now.version(), cmdId);
                return VerifyOutcome.PASS;
            } finally {
                lock.unlock();
            }
        } finally {
            guard.exit();
        }
    }

    public Optional<Meta> readMeta(String chunkId) {
        Path meta = metaPath(chunkId);
        try {
            String[] parts = Files.readString(meta, StandardCharsets.US_ASCII).trim().split("\\s+");
            if (parts.length != 2 || !Checksums.isValidSha256Hex(parts[0])) {
                return Optional.empty();
            }
            return Optional.of(new Meta(parts[0], Long.parseLong(parts[1])));
        } catch (IOException | NumberFormatException e) {
            return Optional.empty();
        }
    }

    // ---- the index ---------------------------------------------------------------------------

    private void indexPut(String chunkId, Entry entry) {
        Entry old = index.put(chunkId, entry);
        if (old == null) {
            chunkCount.incrementAndGet();
            usedBytes.addAndGet(entry.size());
        } else {
            usedBytes.addAndGet(entry.size() - old.size());
        }
    }

    private void indexRemove(String chunkId) {
        Entry old = index.remove(chunkId);
        if (old != null) {
            chunkCount.decrementAndGet();
            usedBytes.addAndGet(-old.size());
        }
    }

    /** The index, read-only. */
    public Map<String, Entry> index() {
        return Collections.unmodifiableMap(index);
    }

    public boolean isQuarantined(String chunkId) {
        return quarantinedIds.contains(chunkId);
    }

    /** The resync listing (v5 §5.3): every indexed chunk with its size, checksum and version, sorted. No locks. */
    public List<ReportEntry> reportEntries() {
        List<ReportEntry> entries = new ArrayList<>(index.size());
        index.forEach((chunkId, entry) -> entries.add(new ReportEntry(chunkId, entry.size(), entry.checksum(),
                entry.version())));
        entries.sort(Comparator.comparing(ReportEntry::chunkId));
        return entries;
    }

    /** Ids of all indexed chunks, in no particular order. */
    public List<String> chunkIds() {
        return new ArrayList<>(index.keySet());
    }

    /** Every chunk with its checksum recomputed from the data right now (a disk walk). Used by fsck. */
    public List<ChunkInfo> listVerified() {
        List<ChunkInfo> chunks = new ArrayList<>();
        for (String chunkId : chunkIdsOnDisk()) {
            try {
                byte[] data = Files.readAllBytes(chunkDir.resolve(chunkId));
                chunks.add(new ChunkInfo(chunkId, data.length, Checksums.sha256Hex(data)));
            } catch (NoSuchFileException deletedMeanwhile) {
                // not part of the result
            } catch (IOException e) {
                throw new UncheckedIOException("could not read chunk " + chunkId, e);
            }
        }
        chunks.sort(Comparator.comparing(ChunkInfo::chunkId));
        return chunks;
    }

    /** Names of the data files in the chunk directory (names only: no file is opened). */
    Set<String> chunkIdsOnDisk() {
        Set<String> ids = new HashSet<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(chunkDir)) {
            for (Path file : files) {
                String name = file.getFileName().toString();
                if (ChunkIds.isValid(name)) {
                    ids.add(name);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("could not list chunks", e);
        }
        return ids;
    }

    /**
     * Brings the index in line with the directory (the scanner's first step). A valid chunk file
     * that is not indexed (placed by hand) is indexed at {@code localVersion}, its sidecar rebuilt
     * if missing, and reported {@code RECEIVED(odd)}; an indexed chunk whose file is gone is
     * reported {@code DELETED(odd, missing)}. Each is re-checked under its stripe. Files modified
     * within {@link #SETTLE_TIME} are skipped: they may be writes in progress.
     */
    public Reconciled reconcile() {
        Set<String> onDisk = chunkIdsOnDisk();
        long settledBefore = System.currentTimeMillis() - SETTLE_TIME.toMillis();
        int added = 0;
        int removed = 0;
        for (String chunkId : onDisk) {
            if (index.containsKey(chunkId) || !isSettled(chunkId, settledBefore)) {
                continue;
            }
            if (failStop.failed() || !guard.enter()) {
                break;
            }
            try {
                ReentrantLock lock = lockFor(chunkId);
                lock.lock();
                try {
                    Path file = chunkDir.resolve(chunkId);
                    if (index.containsKey(chunkId) || !Files.exists(file)) {
                        continue;
                    }
                    Optional<Meta> meta = readMeta(chunkId);
                    if (meta.isEmpty()) {
                        byte[] data = Files.readAllBytes(file);
                        meta = Optional.of(new Meta(Checksums.sha256Hex(data), data.length));
                        writeMetaReplacing(chunkId, meta.get());
                        FsyncSupport.fsyncDirectory(chunkDir);
                    }
                    long version = localVersion(chunkId);
                    indexPut(chunkId, new Entry(meta.get().size(), meta.get().checksum(), version));
                    tombstones.remove(chunkId);
                    quarantinedIds.remove(chunkId);
                    changes.received(chunkId, meta.get().size(), meta.get().checksum(), version, null);
                    added++;
                    log.warn("[scanner] chunk={} appeared on disk; indexed at version {}", chunkId, version);
                } catch (IOException e) {
                    log.warn("[scanner] chunk={} could not be added to the index: {}", chunkId, e.toString());
                } finally {
                    lock.unlock();
                }
            } finally {
                guard.exit();
            }
        }
        for (String chunkId : chunkIds()) {
            if (onDisk.contains(chunkId)) {
                continue;
            }
            Entry entry = index.get(chunkId);
            if (entry != null && observeMissing(chunkId, entry.version())) {
                removed++;
            }
        }
        return new Reconciled(added, removed);
    }

    private boolean isSettled(String chunkId, long settledBefore) {
        try {
            return Files.getLastModifiedTime(chunkDir.resolve(chunkId)).toMillis() < settledBefore;
        } catch (IOException gone) {
            return false;
        }
    }

    /**
     * Verifies one chunk against its sidecar for the scanner, reading outside the stripe; a failure
     * is applied by {@link #quarantineIfBad}.
     *
     * @return true if intact; false if quarantined; empty if gone or changed meanwhile
     */
    public Optional<Boolean> scan(String chunkId) {
        Entry entry = index.get(chunkId);
        if (entry == null) {
            return Optional.empty();
        }
        Path file = pathFor(chunkId);
        if (!Files.exists(file)) {
            return Optional.empty();                   // the next reconciliation reports it
        }
        if (intact(chunkId, file, null)) {
            return Optional.of(true);
        }
        return quarantineIfBad(chunkId, entry.version(), null) ? Optional.of(false) : Optional.empty();
    }

    /** O(1): the index's running totals and the free space ({@link #freeBytes}). Never walks the disk. */
    public WorkerStats stats(int activeTransfers) {
        return new WorkerStats(chunkCount.get(), usedBytes.get(), freeBytes(), activeTransfers,
                quarantinedChunks.get(), quarantinedBytes.get(), tombstones.size());
    }

    /**
     * The free space this worker reports and writes against: the volume's, and with a STORAGE_CAPACITY
     * no more than the capacity minus what it stores (chunks + quarantine). Never negative.
     */
    public long freeBytes() {
        return Math.min(volumeFreeBytes(), capacityLeft());
    }

    /** Room left under STORAGE_CAPACITY; {@code Long.MAX_VALUE} without a cap. */
    private long capacityLeft() {
        if (capacity <= 0) {
            return Long.MAX_VALUE;
        }
        return Math.max(0, capacity - usedBytes.get() - quarantinedBytes.get());
    }

    private long volumeFreeBytes() {
        try {
            FileStore store = Files.getFileStore(chunkDir);
            return store.getUsableSpace();
        } catch (IOException e) {
            throw new UncheckedIOException("could not query free space of " + chunkDir, e);
        }
    }

    public Path chunkDir() {
        return chunkDir;
    }

    public Path quarantineDir() {
        return quarantineDir;
    }

    private String describeRecord(String chunkId) {
        Entry entry = index.get(chunkId);
        Long tomb = tombstones.get(chunkId);
        String record = entry != null ? "copy v" + entry.version() : tomb != null ? "tombstone v" + tomb : "no record";
        return record + " / fence " + (fenceSet ? fence : "none");
    }

    private static byte[] metaBytes(Meta meta) {
        return (meta.checksum() + " " + meta.size() + "\n").getBytes(StandardCharsets.US_ASCII);
    }

    /** Writes a sidecar where none (or an unreadable one) is: recovery and reconciliation only. */
    private void writeMetaReplacing(String chunkId, Meta meta) throws IOException {
        Path target = metaPath(chunkId);
        Path tmp = chunkDir.resolve(chunkId + META_SUFFIX + "." + UUID.randomUUID() + TMP_SUFFIX);
        try {
            FsyncSupport.writeAndForce(tmp, metaBytes(meta));
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    private static String hashOrNull(Path file) {
        try {
            return Checksums.sha256Hex(file);
        } catch (IOException e) {
            return null;
        }
    }

    private static void deleteQuietly(Path file) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException ignored) {
            // start-up recovery removes it
        }
    }

    private Path pathFor(String chunkId) {
        if (!ChunkIds.isValid(chunkId)) {
            throw new InvalidChunkIdException(chunkId);
        }
        return chunkDir.resolve(chunkId);
    }

    private Path metaPath(String chunkId) {
        return pathFor(chunkId).resolveSibling(chunkId + META_SUFFIX);
    }

    private ReentrantLock lockFor(String chunkId) {
        return locks[Math.floorMod(chunkId.hashCode(), locks.length)];
    }
}
