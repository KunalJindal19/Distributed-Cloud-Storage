package com.objectstore.worker.storage;

import com.objectstore.common.Checksums;
import com.objectstore.common.ChunkIds;
import com.objectstore.common.dto.ChunkInfo;
import com.objectstore.common.dto.WorkerStats;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.reporting.IbrBuffer;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Disk operations for chunks (the DataNode's block storage). Each chunk is two files directly
 * under {@code DATA_DIR/chunks}: the data, named exactly after the chunk id, and a
 * {@code <chunk_id>.meta} sidecar holding its SHA-256 and size (HDFS's {@code .meta} file).
 *
 * <p>Guarantees:
 * <ul>
 *   <li><b>No partial files.</b> Data and meta are each written to a unique temporary name, forced
 *       to the device and renamed; the directory is forced afterwards. A crash never leaves a
 *       half-written file under a chunk id, and a 201 means the bytes are on the platter.</li>
 *   <li><b>No silent corruption on write.</b> The bytes are hashed and compared with the checksum
 *       the sender declared before anything touches the disk. Writing a chunk that already
 *       exists verifies the stored data; rotten data is replaced by the new, verified bytes.</li>
 *   <li><b>No corruption served.</b> A read re-hashes the data and compares it with the sidecar.</li>
 *   <li><b>No path traversal.</b> Only ids matching the strict chunk id pattern are resolved.</li>
 * </ul>
 *
 * <p><b>The in-memory index.</b> Every chunk on disk is in {@link #index} with its size and
 * sidecar checksum, plus running totals, so statistics are O(1) and listing needs no disk walk.
 * The index is built in {@link #initialise()} (the only full walk at start-up) and changed only
 * under the chunk's stripe lock, in this order: the disk operation, its fsync, the index, then
 * the change event. The block scanner reconciles the index with the directory for files added or
 * removed behind the store's back. {@link #knownBad} holds the chunks whose data failed
 * verification in this process.
 *
 * <p>Every change is reported to the {@link IbrBuffer}, so the controller learns about it in the
 * next incremental block report.
 */
@Component
public class ChunkStore {

    private static final Logger log = LoggerFactory.getLogger(ChunkStore.class);
    private static final String META_SUFFIX = ".meta";
    private static final String TMP_SUFFIX = ".tmp";
    private static final long WRITE_HEADROOM = 1_048_576;
    private static final int LOCK_STRIPES = 64;
    /** A file modified more recently than this may be a write in progress: reconciliation leaves it alone. */
    public static final java.time.Duration SETTLE_TIME = java.time.Duration.ofSeconds(5);
    /**
     * The checksum listed for a chunk whose data failed verification ({@link #knownBad}): it
     * matches nothing, so the controller treats the copy as corrupt.
     */
    public static final String UNVERIFIED = "unverified";

    public enum WriteResult { CREATED, EXISTS }

    /** A chunk as read from disk. {@code corrupt} is set when the bytes no longer match the sidecar. */
    public record StoredChunk(byte[] data, String checksum, boolean corrupt) {
    }

    public record Meta(String checksum, long size) {
    }

    /** One chunk in the index: its size and the checksum from its sidecar. */
    public record Entry(long size, String checksum) {
    }

    /** What one reconciliation of the index with the directory changed. */
    public record Reconciled(int added, int removed) {

        public boolean isEmpty() {
            return added + removed == 0;
        }
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

    public static class ChunkConflictException extends RuntimeException {
        public ChunkConflictException(String chunkId) {
            super("chunk " + chunkId + " already exists with different content");
        }
    }

    public static class InsufficientStorageException extends RuntimeException {
        public InsufficientStorageException(long needed, long free) {
            super("need " + needed + " bytes but only " + free + " are free");
        }
    }

    private final Path chunkDir;
    private final IbrBuffer ibr;
    private final ReentrantLock[] locks = new ReentrantLock[LOCK_STRIPES];
    private final ConcurrentHashMap<String, Entry> index = new ConcurrentHashMap<>();
    private final AtomicLong chunkCount = new AtomicLong();
    private final AtomicLong usedBytes = new AtomicLong();
    private final Set<String> knownBad = ConcurrentHashMap.newKeySet();

    public ChunkStore(WorkerProperties properties, IbrBuffer ibr) {
        this.chunkDir = properties.chunkDir().toAbsolutePath().normalize();
        this.ibr = ibr;
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new ReentrantLock();
        }
    }

    /**
     * Start-up: one walk of the chunk directory that removes temporary files left by a crash,
     * rebuilds a missing or unreadable sidecar from the data (the data was verified before it was
     * renamed into place), drops sidecars whose data is gone, and fills the index.
     */
    @PostConstruct
    void initialise() throws IOException {
        Files.createDirectories(chunkDir);
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
                        writeMeta(name, meta.get());
                        rebuilt++;
                    }
                    indexPut(name, new Entry(meta.get().size(), meta.get().checksum()));
                }
            }
        }
        FsyncSupport.fsyncDirectory(chunkDir);
        if (tmp + rebuilt + strayMeta > 0) {
            log.warn("[store] start-up cleanup: {} temporary files removed, {} sidecars rebuilt, {} stray sidecars removed",
                    tmp, rebuilt, strayMeta);
        }
        log.info("[store] chunk directory {} ready with {} chunks ({} bytes)", chunkDir, chunkCount.get(), usedBytes.get());
    }

    // ---- writes, reads, deletes ------------------------------------------------------------

    /**
     * Stores a chunk. Idempotent: writing identical content again verifies the stored data and
     * reports {@link WriteResult#EXISTS} (the chunk is announced again). If the stored data no
     * longer matches its sidecar it is rotten: it is replaced by the new, verified bytes and
     * {@link WriteResult#CREATED} is reported.
     *
     * @throws InvalidChunkIdException      if the id is not a well-formed chunk id
     * @throws InsufficientStorageException if the volume lacks room for the chunk plus headroom
     * @throws ChecksumMismatchException    if the bytes do not hash to {@code expectedChecksum}
     * @throws ChunkConflictException       if a file with this id exists and holds different content
     */
    public WriteResult write(String chunkId, byte[] data, String expectedChecksum) {
        Path target = pathFor(chunkId);
        long free = freeBytes();
        if (free < data.length + WRITE_HEADROOM) {
            throw new InsufficientStorageException(data.length + WRITE_HEADROOM, free);
        }
        String actual = Checksums.sha256Hex(data);
        if (!actual.equals(expectedChecksum)) {
            throw new ChecksumMismatchException(expectedChecksum, actual);
        }
        ReentrantLock lock = lockFor(chunkId);
        lock.lock();
        try {
            if (Files.exists(target)) {
                Optional<Meta> meta = readMeta(chunkId);
                if (meta.isPresent() && !meta.get().checksum().equals(expectedChecksum)) {
                    throw new ChunkConflictException(chunkId);
                }
                String stored = hashFile(target);
                if (stored.equals(expectedChecksum)) {
                    if (meta.isEmpty()) {
                        writeMeta(chunkId, new Meta(actual, data.length));
                        FsyncSupport.fsyncDirectory(chunkDir);
                    }
                    indexPut(chunkId, new Entry(data.length, actual));
                    knownBad.remove(chunkId);
                    ibr.received(new ChunkInfo(chunkId, data.length, actual)); // re-announced
                    return WriteResult.EXISTS;
                }
                if (meta.isEmpty()) {
                    throw new ChunkConflictException(chunkId); // no sidecar: rotten or different, unknowable
                }
                log.warn("[store] chunk={} stored data is rotten (disk={}); replacing it with the verified bytes",
                        chunkId, stored);
            }
            writeNew(chunkId, target, data, actual);
            indexPut(chunkId, new Entry(data.length, actual));
            knownBad.remove(chunkId);
            ibr.received(new ChunkInfo(chunkId, data.length, actual));
            return WriteResult.CREATED;
        } catch (IOException e) {
            throw new UncheckedIOException("could not write chunk " + chunkId, e);
        } finally {
            lock.unlock();
        }
    }

    /** The normal write path: unique temporary file, fsync, atomic rename, sidecar, directory fsync. */
    private void writeNew(String chunkId, Path target, byte[] data, String checksum) throws IOException {
        Path tmp = chunkDir.resolve(chunkId + "." + UUID.randomUUID() + TMP_SUFFIX);
        try {
            FsyncSupport.writeAndForce(tmp, data);
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
        } finally {
            Files.deleteIfExists(tmp);
        }
        writeMeta(chunkId, new Meta(checksum, data.length));
        FsyncSupport.fsyncDirectory(chunkDir);
    }

    /**
     * Reads a chunk and verifies it against its sidecar. A mismatch is returned as
     * {@code corrupt = true}, the chunk joins {@link #knownBad} and is reported once; the bytes are
     * never served.
     */
    public Optional<StoredChunk> read(String chunkId) {
        Path target = pathFor(chunkId);
        byte[] data;
        try {
            data = Files.readAllBytes(target);
        } catch (NoSuchFileException e) {
            return Optional.empty();
        } catch (IOException e) {
            throw new UncheckedIOException("could not read chunk " + chunkId, e);
        }
        String actual = Checksums.sha256Hex(data);
        Optional<Meta> meta = readMeta(chunkId);
        if (meta.isEmpty() || !meta.get().checksum().equals(actual) || meta.get().size() != data.length) {
            markBad(chunkId, "read");
            return Optional.of(new StoredChunk(data, meta.map(Meta::checksum).orElse(actual), true));
        }
        return Optional.of(new StoredChunk(data, actual, false));
    }

    /** Removes the data and the sidecar. Reports {@code deleted} even if there was nothing to delete. */
    public boolean delete(String chunkId) {
        Path target = pathFor(chunkId);
        ReentrantLock lock = lockFor(chunkId);
        lock.lock();
        try {
            boolean removed = Files.deleteIfExists(target);
            Files.deleteIfExists(metaPath(chunkId));
            FsyncSupport.fsyncDirectory(chunkDir);
            indexRemove(chunkId);
            knownBad.remove(chunkId);
            ibr.deleted(chunkId);
            return removed;
        } catch (IOException e) {
            throw new UncheckedIOException("could not delete chunk " + chunkId, e);
        } finally {
            lock.unlock();
        }
    }

    /** The chunk's data failed verification: remember it, and report it the first time. */
    boolean markBad(String chunkId, String by) {
        if (knownBad.add(chunkId)) {
            if (!"scanner".equals(by)) {          // the scanner logs its own [scanner] CORRUPT line
                log.warn("[store] chunk={} failed verification ({}); reported bad", chunkId, by);
            }
            ibr.bad(chunkId);
            return true;
        }
        return false;
    }

    /** The chunk's data verified again (the file was replaced): no longer known bad. */
    boolean clearBad(String chunkId) {
        return knownBad.remove(chunkId);
    }

    public boolean isKnownBad(String chunkId) {
        return knownBad.contains(chunkId);
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
        return java.util.Collections.unmodifiableMap(index);
    }

    /**
     * Every chunk with its size and sidecar checksum, from the index (no disk access), sorted by
     * id. A chunk in {@link #knownBad} is listed with {@link #UNVERIFIED} as its checksum.
     */
    public List<ChunkInfo> list() {
        List<ChunkInfo> chunks = new ArrayList<>(index.size());
        index.forEach((chunkId, entry) -> chunks.add(new ChunkInfo(chunkId, entry.size(),
                knownBad.contains(chunkId) ? UNVERIFIED : entry.checksum())));
        chunks.sort(Comparator.comparing(ChunkInfo::chunkId));
        return chunks;
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
     * that is not in the index (copied in by hand) is added, its sidecar rebuilt from the data if
     * missing, and reported {@code received}; an indexed chunk whose data file is gone (deleted by
     * hand) is removed and reported {@code deleted}. Each is re-checked under its stripe lock.
     * Files modified within {@link #SETTLE_TIME} are skipped: they may be writes in progress.
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
                    writeMeta(chunkId, meta.get());
                    FsyncSupport.fsyncDirectory(chunkDir);
                }
                indexPut(chunkId, new Entry(meta.get().size(), meta.get().checksum()));
                ibr.received(new ChunkInfo(chunkId, meta.get().size(), meta.get().checksum()));
                added++;
                log.warn("[scanner] chunk={} appeared on disk; added to the index", chunkId);
            } catch (IOException e) {
                log.warn("[scanner] chunk={} could not be added to the index: {}", chunkId, e.toString());
            } finally {
                lock.unlock();
            }
        }
        for (String chunkId : chunkIds()) {
            if (onDisk.contains(chunkId)) {
                continue;
            }
            ReentrantLock lock = lockFor(chunkId);
            lock.lock();
            try {
                if (!index.containsKey(chunkId) || Files.exists(chunkDir.resolve(chunkId))) {
                    continue;
                }
                indexRemove(chunkId);
                knownBad.remove(chunkId);
                ibr.deleted(chunkId);
                removed++;
                log.warn("[scanner] chunk={} is gone from the disk; removed from the index", chunkId);
            } finally {
                lock.unlock();
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
     * Verifies one chunk's data against its sidecar on disk (the scanner's second step).
     *
     * @return true if it matches; false if it does not (or its sidecar is missing or invalid);
     *         empty if the chunk is gone
     */
    public Optional<Boolean> verify(String chunkId) {
        Path file = pathFor(chunkId);
        try {
            String actual = Checksums.sha256Hex(file);
            Optional<Meta> meta = readMeta(chunkId);
            return Optional.of(meta.isPresent() && meta.get().checksum().equals(actual)
                    && meta.get().size() == Files.size(file));
        } catch (NoSuchFileException gone) {
            return Optional.empty();
        } catch (IOException e) {
            log.warn("[scanner] chunk={} could not be read: {}", chunkId, e.toString());
            return Optional.of(false);
        }
    }

    /** Re-verifies a chunk under its stripe lock (so a write in progress cannot look like damage). */
    public Optional<Boolean> verifyLocked(String chunkId) {
        ReentrantLock lock = lockFor(chunkId);
        lock.lock();
        try {
            return verify(chunkId);
        } finally {
            lock.unlock();
        }
    }

    /** O(1): the index's running totals and the volume's free space. Never walks the disk. */
    public WorkerStats stats(int activeTransfers) {
        return new WorkerStats(chunkCount.get(), usedBytes.get(), freeBytes(), activeTransfers);
    }

    public long freeBytes() {
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

    private void writeMeta(String chunkId, Meta meta) throws IOException {
        Path target = metaPath(chunkId);
        Path tmp = chunkDir.resolve(chunkId + META_SUFFIX + "." + UUID.randomUUID() + TMP_SUFFIX);
        try {
            FsyncSupport.writeAndForce(tmp, (meta.checksum() + " " + meta.size() + "\n").getBytes(StandardCharsets.US_ASCII));
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    private static String hashFile(Path file) {
        try {
            return Checksums.sha256Hex(file);
        } catch (IOException e) {
            throw new UncheckedIOException("could not hash " + file, e);
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
