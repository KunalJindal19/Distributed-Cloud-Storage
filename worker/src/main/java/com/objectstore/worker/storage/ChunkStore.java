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
import java.util.List;
import java.util.Optional;
import java.util.UUID;
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
 *       the sender declared before anything touches the disk.</li>
 *   <li><b>No corruption served.</b> A read re-hashes the data and compares it with the sidecar.</li>
 *   <li><b>No path traversal.</b> Only ids matching the strict chunk id pattern are resolved.</li>
 * </ul>
 *
 * Every change is reported to the {@link IbrBuffer}, so the controller learns about it in the
 * next incremental block report.
 */
@Component
public class ChunkStore {

    private static final Logger log = LoggerFactory.getLogger(ChunkStore.class);
    private static final String META_SUFFIX = ".meta";
    private static final String TMP_SUFFIX = ".tmp";
    private static final long WRITE_HEADROOM = 1_048_576;
    private static final int LOCK_STRIPES = 64;
    /**
     * A data file without a valid sidecar is normally a write in progress (the data is renamed
     * into place a moment before its sidecar). One that has been like that for longer than this
     * is damaged or foreign, and is reported with {@link #UNVERIFIED} as its checksum.
     */
    static final java.time.Duration SETTLE_TIME = java.time.Duration.ofSeconds(5);
    /** The checksum reported for a chunk whose sidecar is missing or unreadable; matches nothing. */
    public static final String UNVERIFIED = "unverified";

    public enum WriteResult { CREATED, EXISTS }

    /** A chunk as read from disk. {@code corrupt} is set when the bytes no longer match the sidecar. */
    public record StoredChunk(byte[] data, String checksum, boolean corrupt) {
    }

    public record Meta(String checksum, long size) {
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

    public ChunkStore(WorkerProperties properties, IbrBuffer ibr) {
        this.chunkDir = properties.chunkDir().toAbsolutePath().normalize();
        this.ibr = ibr;
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new ReentrantLock();
        }
    }

    /**
     * Start-up cleanup: remove temporary files left by a crash, rebuild a missing or unreadable
     * sidecar from the data (the data was verified before it was renamed into place), and drop
     * sidecars whose data is gone.
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
                } else if (ChunkIds.isValid(name) && readMeta(name).isEmpty()) {
                    byte[] data = Files.readAllBytes(file);
                    writeMeta(name, new Meta(Checksums.sha256Hex(data), data.length));
                    rebuilt++;
                }
            }
        }
        FsyncSupport.fsyncDirectory(chunkDir);
        if (tmp + rebuilt + strayMeta > 0) {
            log.warn("[store] start-up cleanup: {} temporary files removed, {} sidecars rebuilt, {} stray sidecars removed",
                    tmp, rebuilt, strayMeta);
        }
        log.info("[store] chunk directory {} ready with {} chunks", chunkDir, list().size());
    }

    /**
     * Stores a chunk. Idempotent: writing identical content again reports {@link WriteResult#EXISTS}.
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
                String existing = meta.map(Meta::checksum).orElseGet(() -> hashFile(target));
                if (existing.equals(expectedChecksum)) {
                    if (meta.isEmpty()) {
                        writeMeta(chunkId, new Meta(actual, data.length));
                    }
                    return WriteResult.EXISTS;
                }
                throw new ChunkConflictException(chunkId);
            }
            Path tmp = chunkDir.resolve(chunkId + "." + UUID.randomUUID() + TMP_SUFFIX);
            try {
                FsyncSupport.writeAndForce(tmp, data);
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
            } finally {
                Files.deleteIfExists(tmp);
            }
            writeMeta(chunkId, new Meta(actual, data.length));
            FsyncSupport.fsyncDirectory(chunkDir);
            ibr.received(new ChunkInfo(chunkId, data.length, actual));
            return WriteResult.CREATED;
        } catch (IOException e) {
            throw new UncheckedIOException("could not write chunk " + chunkId, e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reads a chunk and verifies it against its sidecar. A mismatch is returned as
     * {@code corrupt = true} and reported; the bytes are never served.
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
            ibr.bad(chunkId);
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
            ibr.deleted(chunkId);
            return removed;
        } catch (IOException e) {
            throw new UncheckedIOException("could not delete chunk " + chunkId, e);
        } finally {
            lock.unlock();
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

    /**
     * Every chunk on disk with the size and checksum from its sidecar. Cheap: no data is read. A
     * settled data file without a valid sidecar is listed with checksum {@link #UNVERIFIED}, so
     * the controller treats it as a corrupt copy (if it knows the chunk) or an orphan (if not).
     */
    public List<ChunkInfo> list() {
        List<ChunkInfo> chunks = new ArrayList<>();
        for (String chunkId : chunkIds()) {
            Optional<Meta> meta = readMeta(chunkId);
            if (meta.isPresent()) {
                chunks.add(new ChunkInfo(chunkId, meta.get().size(), meta.get().checksum()));
            } else if (isSettled(chunkId)) {
                chunks.add(new ChunkInfo(chunkId, sizeOf(chunkId), UNVERIFIED));
            }
        }
        chunks.sort(Comparator.comparing(ChunkInfo::chunkId));
        return chunks;
    }

    /** True if the data file has not been modified for {@link #SETTLE_TIME}. */
    public boolean isSettled(String chunkId) {
        try {
            long modified = Files.getLastModifiedTime(pathFor(chunkId)).toMillis();
            return System.currentTimeMillis() - modified > SETTLE_TIME.toMillis();
        } catch (IOException e) {
            return false;
        }
    }

    private long sizeOf(String chunkId) {
        try {
            return Files.size(pathFor(chunkId));
        } catch (IOException e) {
            return -1;
        }
    }

    /** Every chunk with its checksum recomputed from the data right now. Used by fsck. */
    public List<ChunkInfo> listVerified() {
        List<ChunkInfo> chunks = new ArrayList<>();
        for (String chunkId : chunkIds()) {
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

    /** Ids of all data files on disk, in no particular order. */
    public List<String> chunkIds() {
        List<String> ids = new ArrayList<>();
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

    /** Hashes the data of one chunk from disk; empty if it is gone. */
    public Optional<String> hashOnDisk(String chunkId) {
        Path target = pathFor(chunkId);
        if (!Files.exists(target)) {
            return Optional.empty();
        }
        return Optional.of(hashFile(target));
    }

    public WorkerStats stats(int activeTransfers) {
        long used = 0;
        long count = 0;
        for (ChunkInfo chunk : list()) {
            used += chunk.size();
            count++;
        }
        return new WorkerStats(count, used, freeBytes(), activeTransfers);
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
