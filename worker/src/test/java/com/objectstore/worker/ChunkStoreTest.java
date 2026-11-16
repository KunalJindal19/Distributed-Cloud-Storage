package com.objectstore.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.objectstore.common.Checksums;
import com.objectstore.common.dto.ChunkInfo;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.storage.ChunkStore;
import com.objectstore.worker.storage.ChunkStore.WriteResult;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;

class ChunkStoreTest {

    private static final String CHUNK_A = "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a-00000";
    private static final String CHUNK_B = "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a-00001";

    @TempDir
    Path dataDir;

    private ChunkStore store;
    private Path chunkDir;

    @BeforeEach
    void setUp() {
        WorkerProperties properties = new WorkerProperties(
                "worker1", dataDir, "http://controller:8000", "", 2, 1_048_576, false);
        chunkDir = properties.chunkDir();
        store = new ChunkStore(properties);
        ReflectionTestUtils.invokeMethod(store, "initialise");
    }

    @Test
    void writesTheExactBytesUnderTheChunkId() throws IOException {
        byte[] data = bytes(1_048_576, 7);

        WriteResult result = store.write(CHUNK_A, data, Checksums.sha256Hex(data));

        assertThat(result).isEqualTo(WriteResult.CREATED);
        assertThat(Files.readAllBytes(chunkDir.resolve(CHUNK_A))).isEqualTo(data);
        assertThat(filesIn(chunkDir)).containsExactly(CHUNK_A);
    }

    @Test
    void rejectsAWrongChecksumAndLeavesNothingBehind() throws IOException {
        byte[] data = bytes(1000, 1);

        assertThatThrownBy(() -> store.write(CHUNK_A, data, "0".repeat(64)))
                .isInstanceOf(ChunkStore.ChecksumMismatchException.class);

        assertThat(filesIn(chunkDir)).isEmpty();
    }

    @Test
    void writingIdenticalContentTwiceIsIdempotent() {
        byte[] data = bytes(1000, 1);
        String checksum = Checksums.sha256Hex(data);

        assertThat(store.write(CHUNK_A, data, checksum)).isEqualTo(WriteResult.CREATED);
        assertThat(store.write(CHUNK_A, data, checksum)).isEqualTo(WriteResult.EXISTS);
    }

    @Test
    void writingDifferentContentToAnExistingIdIsAConflict() throws IOException {
        byte[] original = bytes(1000, 1);
        byte[] other = bytes(1000, 2);
        store.write(CHUNK_A, original, Checksums.sha256Hex(original));

        assertThatThrownBy(() -> store.write(CHUNK_A, other, Checksums.sha256Hex(other)))
                .isInstanceOf(ChunkStore.ChunkConflictException.class);

        assertThat(Files.readAllBytes(chunkDir.resolve(CHUNK_A))).isEqualTo(original);
    }

    @Test
    void readReturnsTheChecksumOfWhatIsOnDiskNow() throws IOException {
        byte[] data = bytes(5000, 3);
        store.write(CHUNK_A, data, Checksums.sha256Hex(data));
        byte[] damaged = data.clone();
        damaged[100] ^= 0x5A;
        Files.write(chunkDir.resolve(CHUNK_A), damaged);

        ChunkStore.StoredChunk read = store.read(CHUNK_A).orElseThrow();

        assertThat(read.data()).isEqualTo(damaged);
        assertThat(read.checksum()).isEqualTo(Checksums.sha256Hex(damaged)).isNotEqualTo(Checksums.sha256Hex(data));
    }

    @Test
    void readOfAnUnknownChunkIsEmpty() {
        assertThat(store.read(CHUNK_A)).isEmpty();
    }

    @Test
    void deleteReportsWhetherAFileWasRemoved() {
        byte[] data = bytes(10, 1);
        store.write(CHUNK_A, data, Checksums.sha256Hex(data));

        assertThat(store.delete(CHUNK_A)).isTrue();
        assertThat(store.delete(CHUNK_A)).isFalse();
        assertThat(store.read(CHUNK_A)).isEmpty();
    }

    @Test
    void listIsSortedHashesFromDiskAndIgnoresForeignFiles() throws IOException {
        byte[] a = bytes(300, 1);
        byte[] b = bytes(700, 2);
        store.write(CHUNK_B, b, Checksums.sha256Hex(b));
        store.write(CHUNK_A, a, Checksums.sha256Hex(a));
        Files.write(chunkDir.resolve(CHUNK_A + ".deadbeef.tmp"), new byte[] {1, 2, 3});
        Files.write(chunkDir.resolve("notes.txt"), new byte[] {1});

        List<ChunkInfo> listed = store.list();

        assertThat(listed).containsExactly(
                new ChunkInfo(CHUNK_A, 300, Checksums.sha256Hex(a)),
                new ChunkInfo(CHUNK_B, 700, Checksums.sha256Hex(b)));
    }

    @Test
    void refusesIdsThatCouldEscapeTheChunkDirectory() throws IOException {
        byte[] data = bytes(10, 1);
        String checksum = Checksums.sha256Hex(data);

        for (String evil : List.of("../escape", "..", "a/b", CHUNK_A + "/../../x", "test-00000")) {
            assertThatThrownBy(() -> store.write(evil, data, checksum))
                    .isInstanceOf(ChunkStore.InvalidChunkIdException.class);
            assertThatThrownBy(() -> store.read(evil)).isInstanceOf(ChunkStore.InvalidChunkIdException.class);
            assertThatThrownBy(() -> store.delete(evil)).isInstanceOf(ChunkStore.InvalidChunkIdException.class);
        }
        assertThat(filesIn(dataDir)).containsExactly("chunks");
        assertThat(filesIn(chunkDir)).isEmpty();
    }

    @Test
    void leftoverTemporaryFilesAreRemovedAtStartUp() throws IOException {
        Files.write(chunkDir.resolve(CHUNK_A + ".crashed.tmp"), new byte[] {1, 2, 3});

        ReflectionTestUtils.invokeMethod(store, "initialise");

        assertThat(filesIn(chunkDir)).isEmpty();
    }

    private static byte[] bytes(int length, int seed) {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = (byte) (i * 31 + seed);
        }
        return data;
    }

    private static List<String> filesIn(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.map(p -> p.getFileName().toString()).sorted().toList();
        }
    }
}
