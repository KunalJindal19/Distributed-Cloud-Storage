package com.objectstore.worker.storage;

import com.objectstore.common.Checksums;
import com.objectstore.common.ChunkIds;
import com.objectstore.common.dto.ChunkInfo;
import com.objectstore.worker.config.WorkerProperties;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
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
 * Disk operations for chunks. One file per chunk, named exactly after the chunk id, directly under
 * {@code DATA_DIR/chunks}.
 *
 * <p>Guarantees:
 * <ul>
 *   <li><b>No partial files.</b> A chunk is written to a temporary file, flushed to the device and
 *       then atomically renamed, so a crash can never leave a half-written file under a chunk id.</li>
 *   <li><b>No silent corruption on write.</b> The bytes are hashed and compared with the checksum
 *       the controller sent before anything touches the disk.</li>
 *   <li><b>No path traversal.</b> Only ids matching the strict chunk id pattern are ever resolved.</li>
 * </ul>
 */
@Component
public class ChunkStore {

    private static final Logger log = LoggerFactory.getLogger(ChunkStore.class);
    private static final String TMP_SUFFIX = ".tmp";
    private static final int LOCK_STRIPES = 64;

    public enum WriteResult { CREATED, EXISTS }

    /** A chunk read from disk together with the checksum of the bytes that were actually read. */
    public record StoredChunk(byte[] data, String checksum) {
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

    private final Path chunkDir;
    private final ReentrantLock[] locks = new ReentrantLock[LOCK_STRIPES];

    public ChunkStore(WorkerProperties properties) {
        this.chunkDir = properties.chunkDir().toAbsolutePath().normalize();
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new ReentrantLock();
        }
    }

    @PostConstruct
    void initialise() throws IOException {
        Files.createDirectories(chunkDir);
        // Temporary files can only be left behind by a crash in the middle of a write.
        try (DirectoryStream<Path> leftovers = Files.newDirectoryStream(chunkDir, "*" + TMP_SUFFIX)) {
            for (Path leftover : leftovers) {
                Files.deleteIfExists(leftover);
                log.warn("[store] removed leftover temporary file {}", leftover.getFileName());
            }
        }
        log.info("[store] chunk directory {} ready", chunkDir);
    }

    /**
     * Stores a chunk. Idempotent: writing identical content again reports {@link WriteResult#EXISTS}.
     *
     * @throws InvalidChunkIdException   if the id is not a well-formed chunk id
     * @throws ChecksumMismatchException if the bytes do not hash to {@code expectedChecksum}
     * @throws ChunkConflictException    if a file with this id exists and holds different content
     */
    public WriteResult write(String chunkId, byte[] data, String expectedChecksum) {
        Path target = pathFor(chunkId);
        String actual = Checksums.sha256Hex(data);
        if (!actual.equals(expectedChecksum)) {
            throw new ChecksumMismatchException(expectedChecksum, actual);
        }
        ReentrantLock lock = lockFor(chunkId);
        lock.lock();
        try {
            if (Files.exists(target)) {
                if (Checksums.sha256Hex(target).equals(expectedChecksum)) {
                    return WriteResult.EXISTS;
                }
                throw new ChunkConflictException(chunkId);
            }
            Path tmp = chunkDir.resolve(chunkId + "." + UUID.randomUUID() + TMP_SUFFIX);
            try {
                try (FileChannel channel = FileChannel.open(tmp, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                    ByteBuffer buffer = ByteBuffer.wrap(data);
                    while (buffer.hasRemaining()) {
                        channel.write(buffer);
                    }
                    channel.force(true);
                }
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
                syncDirectory();
            } finally {
                Files.deleteIfExists(tmp);
            }
            return WriteResult.CREATED;
        } catch (IOException e) {
            throw new UncheckedIOException("could not write chunk " + chunkId, e);
        } finally {
            lock.unlock();
        }
    }

    /** Reads a chunk and hashes what was read (never a cached value), or empty if it is absent. */
    public Optional<StoredChunk> read(String chunkId) {
        Path target = pathFor(chunkId);
        try {
            byte[] data = Files.readAllBytes(target);
            return Optional.of(new StoredChunk(data, Checksums.sha256Hex(data)));
        } catch (NoSuchFileException e) {
            return Optional.empty();
        } catch (IOException e) {
            throw new UncheckedIOException("could not read chunk " + chunkId, e);
        }
    }

    /** @return {@code true} if a file was removed, {@code false} if there was none */
    public boolean delete(String chunkId) {
        Path target = pathFor(chunkId);
        ReentrantLock lock = lockFor(chunkId);
        lock.lock();
        try {
            return Files.deleteIfExists(target);
        } catch (IOException e) {
            throw new UncheckedIOException("could not delete chunk " + chunkId, e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Every chunk on disk with its size and a checksum recomputed from the file right now. This is
     * what lets the controller's scrubber find silently damaged files. Temporary files and anything
     * that is not named like a chunk are ignored.
     */
    public List<ChunkInfo> list() {
        List<ChunkInfo> chunks = new ArrayList<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(chunkDir)) {
            for (Path file : files) {
                String name = file.getFileName().toString();
                if (!ChunkIds.isValid(name)) {
                    continue;
                }
                try {
                    byte[] data = Files.readAllBytes(file);
                    chunks.add(new ChunkInfo(name, data.length, Checksums.sha256Hex(data)));
                } catch (NoSuchFileException deletedMeanwhile) {
                    // Deleted between listing and reading: simply not part of the result.
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("could not list chunks", e);
        }
        chunks.sort(Comparator.comparing(ChunkInfo::chunkId));
        return chunks;
    }

    private Path pathFor(String chunkId) {
        if (!ChunkIds.isValid(chunkId)) {
            throw new InvalidChunkIdException(chunkId);
        }
        return chunkDir.resolve(chunkId);
    }

    private ReentrantLock lockFor(String chunkId) {
        return locks[Math.floorMod(chunkId.hashCode(), locks.length)];
    }

    /** Makes the rename itself durable. Not every platform allows opening a directory; best effort. */
    private void syncDirectory() {
        try (FileChannel directory = FileChannel.open(chunkDir, StandardOpenOption.READ)) {
            directory.force(true);
        } catch (IOException ignored) {
            // Unsupported on this file system; the data file itself has already been forced.
        }
    }
}
