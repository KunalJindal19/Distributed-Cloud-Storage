package com.objectstore.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.objectstore.common.Checksums;
import com.objectstore.common.dto.ChunkInfo;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.reporting.IbrBuffer;
import com.objectstore.worker.storage.BlockScanner;
import com.objectstore.worker.storage.ChunkStore;
import com.objectstore.worker.storage.ChunkStore.WriteResult;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

    private WorkerProperties properties;
    private IbrBuffer ibr;
    private ChunkStore store;
    private Path chunkDir;

    @BeforeEach
    void setUp() {
        properties = new WorkerProperties("worker1", dataDir, "http://controller:8000", "", 1_048_576,
                2, 3, 30, 0.5, 10_000, 10, 30, 2, 10, false);
        chunkDir = properties.chunkDir();
        ibr = new IbrBuffer(properties);
        store = newStore();
    }

    private ChunkStore newStore() {
        ChunkStore created = new ChunkStore(properties, ibr);
        ReflectionTestUtils.invokeMethod(created, "initialise");
        return created;
    }

    @Test
    void writesTheDataAndASidecarAndReportsTheReceipt() throws IOException {
        byte[] data = bytes(1_048_576, 7);
        String sha = Checksums.sha256Hex(data);

        assertThat(store.write(CHUNK_A, data, sha)).isEqualTo(WriteResult.CREATED);

        assertThat(Files.readAllBytes(chunkDir.resolve(CHUNK_A))).isEqualTo(data);
        assertThat(Files.readString(chunkDir.resolve(CHUNK_A + ".meta"))).isEqualTo(sha + " 1048576\n");
        assertThat(filesIn(chunkDir)).containsExactly(CHUNK_A, CHUNK_A + ".meta");
        assertThat(ibr.drain().received()).containsExactly(new ChunkInfo(CHUNK_A, 1_048_576, sha));
    }

    @Test
    void rejectsAWrongChecksumAndLeavesNothingBehind() throws IOException {
        assertThatThrownBy(() -> store.write(CHUNK_A, bytes(1000, 1), "0".repeat(64)))
                .isInstanceOf(ChunkStore.ChecksumMismatchException.class);

        assertThat(filesIn(chunkDir)).isEmpty();
        assertThat(ibr.hasEvents()).isFalse();
    }

    @Test
    void identicalContentIsIdempotentAndDifferentContentIsAConflict() throws IOException {
        byte[] original = bytes(1000, 1);
        byte[] other = bytes(1000, 2);
        store.write(CHUNK_A, original, Checksums.sha256Hex(original));

        assertThat(store.write(CHUNK_A, original, Checksums.sha256Hex(original))).isEqualTo(WriteResult.EXISTS);
        assertThatThrownBy(() -> store.write(CHUNK_A, other, Checksums.sha256Hex(other)))
                .isInstanceOf(ChunkStore.ChunkConflictException.class);
        assertThat(Files.readAllBytes(chunkDir.resolve(CHUNK_A))).isEqualTo(original);
    }

    @Test
    void readVerifiesAgainstTheSidecarAndReportsDamage() throws IOException {
        byte[] data = bytes(5000, 3);
        store.write(CHUNK_A, data, Checksums.sha256Hex(data));
        ibr.drain();

        assertThat(store.read(CHUNK_A).orElseThrow().corrupt()).isFalse();

        byte[] damaged = data.clone();
        damaged[100] ^= 0x5A;
        Files.write(chunkDir.resolve(CHUNK_A), damaged);

        assertThat(store.read(CHUNK_A).orElseThrow().corrupt()).isTrue();
        assertThat(ibr.drain().bad()).containsExactly(CHUNK_A);
    }

    @Test
    void aDamagedSidecarMakesTheCopyCorruptToo() throws IOException {
        byte[] data = bytes(500, 3);
        store.write(CHUNK_A, data, Checksums.sha256Hex(data));
        Files.writeString(chunkDir.resolve(CHUNK_A + ".meta"), "garbage");

        assertThat(store.read(CHUNK_A).orElseThrow().corrupt()).isTrue();
        assertThat(store.list()).isEmpty(); // possibly a write in progress: not reported yet

        Files.setLastModifiedTime(chunkDir.resolve(CHUNK_A),
                java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis() - 60_000));
        assertThat(store.list()).containsExactly(new ChunkInfo(CHUNK_A, 500, ChunkStore.UNVERIFIED));
    }

    @Test
    void deleteRemovesBothFilesAndAlwaysReportsTheDeletion() throws IOException {
        byte[] data = bytes(10, 1);
        store.write(CHUNK_A, data, Checksums.sha256Hex(data));
        ibr.drain();

        assertThat(store.delete(CHUNK_A)).isTrue();
        assertThat(store.delete(CHUNK_A)).isFalse();
        assertThat(filesIn(chunkDir)).isEmpty();
        assertThat(ibr.drain().deleted()).containsExactly(CHUNK_A);
    }

    @Test
    void listUsesTheSidecarsAndListVerifiedRehashes() throws IOException {
        byte[] a = bytes(300, 1);
        byte[] b = bytes(700, 2);
        store.write(CHUNK_B, b, Checksums.sha256Hex(b));
        store.write(CHUNK_A, a, Checksums.sha256Hex(a));
        byte[] damaged = a.clone();
        damaged[0] ^= 1;
        Files.write(chunkDir.resolve(CHUNK_A), damaged);

        assertThat(store.list()).containsExactly(
                new ChunkInfo(CHUNK_A, 300, Checksums.sha256Hex(a)),
                new ChunkInfo(CHUNK_B, 700, Checksums.sha256Hex(b)));
        assertThat(store.listVerified()).containsExactly(
                new ChunkInfo(CHUNK_A, 300, Checksums.sha256Hex(damaged)),
                new ChunkInfo(CHUNK_B, 700, Checksums.sha256Hex(b)));
    }

    @Test
    void startUpCleanupRemovesTempFilesRebuildsSidecarsAndDropsStrayOnes() throws IOException {
        byte[] data = bytes(400, 9);
        Files.write(chunkDir.resolve(CHUNK_A), data);                                   // data without sidecar
        Files.writeString(chunkDir.resolve(CHUNK_B + ".meta"), "x 1\n");                 // sidecar without data
        Files.write(chunkDir.resolve(CHUNK_A + ".deadbeef.tmp"), new byte[] {1, 2, 3}); // crash leftover

        store = newStore();

        assertThat(filesIn(chunkDir)).containsExactly(CHUNK_A, CHUNK_A + ".meta");
        assertThat(store.readMeta(CHUNK_A).orElseThrow().checksum()).isEqualTo(Checksums.sha256Hex(data));
    }

    @Test
    void refusesIdsThatCouldEscapeTheChunkDirectory() throws IOException {
        byte[] data = bytes(10, 1);
        String checksum = Checksums.sha256Hex(data);

        for (String evil : List.of("../escape", "..", "a/b", CHUNK_A + "/../../x", "test-00000")) {
            assertThatThrownBy(() -> store.write(evil, data, checksum)).isInstanceOf(ChunkStore.InvalidChunkIdException.class);
            assertThatThrownBy(() -> store.read(evil)).isInstanceOf(ChunkStore.InvalidChunkIdException.class);
            assertThatThrownBy(() -> store.delete(evil)).isInstanceOf(ChunkStore.InvalidChunkIdException.class);
        }
        assertThat(filesIn(dataDir)).containsExactly("chunks");
    }

    @Test
    void theScannerReportsABadChunkOnceUntilTheNextFullReport() throws IOException {
        byte[] data = bytes(2000, 5);
        store.write(CHUNK_A, data, Checksums.sha256Hex(data));
        ibr.drain();
        Files.write(chunkDir.resolve(CHUNK_A), "XXXX".getBytes(StandardCharsets.US_ASCII), java.nio.file.StandardOpenOption.WRITE);
        BlockScanner scanner = new BlockScanner(store, ibr, properties);

        assertThat(scanner.scanOnce()).isEqualTo(1);
        assertThat(ibr.drain().bad()).containsExactly(CHUNK_A);
        scanner.scanOnce();
        assertThat(ibr.hasEvents()).isFalse();
        scanner.afterFullReport();
        scanner.scanOnce();
        assertThat(ibr.drain().bad()).containsExactly(CHUNK_A);
        assertThat(Files.exists(chunkDir.resolve(CHUNK_A))).isTrue(); // never deletes
    }

    @Test
    void theDiskProbePassesOnAHealthyVolumeAndAnInterruptedProbeIsNotADiskFailure() {
        com.objectstore.worker.storage.DiskCheck check = new com.objectstore.worker.storage.DiskCheck(properties);
        assertThat(check.probeOnce()).isTrue();

        Thread.currentThread().interrupt(); // as on shutdown: the probe's file I/O is aborted
        try {
            assertThat(check.probeOnce()).as("an interrupted probe must not count as a broken disk").isTrue();
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void theIbrBufferOverflowsIntoAFullReport() {
        WorkerProperties tiny = new WorkerProperties("worker1", dataDir, "http://c", "", 1_048_576,
                2, 3, 30, 0.5, 2, 10, 30, 2, 10, false);
        IbrBuffer small = new IbrBuffer(tiny);
        small.deleted(CHUNK_A);
        small.deleted(CHUNK_B);
        assertThat(small.takeOverflow()).isFalse();
        small.bad("3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a-00002");

        assertThat(small.takeOverflow()).isTrue();
        assertThat(small.hasEvents()).isFalse();
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
