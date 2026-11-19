package com.objectstore.worker.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.Checksums;
import com.objectstore.common.dto.ChunkInfo;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.TestSupport;
import com.objectstore.worker.reporting.ChangeLog;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;

/** Block 11 (v4 §3.3, §3.5): reconciliation of the index with the disk, verification, the rate limit. */
class BlockScannerTest {

    private static final String OBJECT = "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a";
    private static final int MIB = 1_048_576;

    @TempDir
    Path dataDir;

    private ChangeLog changes;
    private ChunkStore store;
    private BlockScanner scanner;
    private Path chunkDir;

    private static String chunk(int i) {
        return "%s-%05d".formatted(OBJECT, i);
    }

    private static WorkerProperties properties(Path dataDir, long rate) {
        return TestSupport.properties(dataDir, rate, 100_000);
    }

    private void open(long rate) {
        WorkerProperties properties = properties(dataDir, rate);
        changes = new ChangeLog(properties);
        store = new ChunkStore(properties, changes);
        ReflectionTestUtils.invokeMethod(store, "initialise");
        scanner = new BlockScanner(store, properties);
        chunkDir = properties.chunkDir();
    }

    @BeforeEach
    void setUp() {
        open(1L << 30);
    }

    private byte[] write(int i, int size) {
        byte[] data = new byte[size];
        new java.util.Random(i).nextBytes(data);
        store.write(chunk(i), data, Checksums.sha256Hex(data));
        return data;
    }

    private void age(String fileName) throws IOException {
        Files.setLastModifiedTime(chunkDir.resolve(fileName), FileTime.fromMillis(System.currentTimeMillis() - 60_000));
    }

    @Test
    void aFileRemovedByHandIsReportedDeletedWithinOnePass() throws Exception {
        write(0, 1000);
        write(1, 1000);
        TestSupport.drain(changes);
        Files.delete(chunkDir.resolve(chunk(0)));
        Files.delete(chunkDir.resolve(chunk(0) + ".meta"));

        scanner.scanOnce();

        assertThat(TestSupport.drain(changes).deleted()).containsExactly(chunk(0));
        assertThat(store.index()).containsOnlyKeys(chunk(1));
        assertThat(store.stats(0).chunks()).isEqualTo(1);
        assertThat(store.stats(0).usedBytes()).isEqualTo(1000);
    }

    @Test
    void aFileAddedByHandIsIndexedAndReportedReceivedWithItsSidecarRebuilt() throws Exception {
        byte[] data = new byte[700];
        new java.util.Random(9).nextBytes(data);
        Files.write(chunkDir.resolve(chunk(5)), data);                    // no sidecar
        age(chunk(5));
        byte[] other = new byte[300];
        Files.write(chunkDir.resolve(chunk(6)), other);
        Files.writeString(chunkDir.resolve(chunk(6) + ".meta"), Checksums.sha256Hex(other) + " 300\n");
        age(chunk(6));
        byte[] fresh = new byte[10];
        Files.write(chunkDir.resolve(chunk(7)), fresh);                   // still settling: left alone

        scanner.scanOnce();

        assertThat(TestSupport.drain(changes).received()).containsExactlyInAnyOrder(
                new ChunkInfo(chunk(5), 700, Checksums.sha256Hex(data)),
                new ChunkInfo(chunk(6), 300, Checksums.sha256Hex(other)));
        assertThat(store.readMeta(chunk(5)).orElseThrow().checksum()).isEqualTo(Checksums.sha256Hex(data));
        assertThat(store.index()).containsOnlyKeys(chunk(5), chunk(6));
        assertThat(store.stats(0).usedBytes()).isEqualTo(1000);
    }

    @Test
    void aCorruptedFileIsReportedBadExactlyOnceAndClearedWhenReplaced() throws Exception {
        byte[] data = write(0, 2000);
        write(1, 2000);
        TestSupport.drain(changes);
        Files.write(chunkDir.resolve(chunk(0)), "XXXX".getBytes(StandardCharsets.US_ASCII), StandardOpenOption.WRITE);

        assertThat(scanner.scanOnce()).isEqualTo(1);
        assertThat(scanner.scanOnce()).isEqualTo(1);
        assertThat(scanner.scanOnce()).isEqualTo(1);

        assertThat(TestSupport.drain(changes).bad()).containsExactly(chunk(0));          // exactly one BAD over three passes
        assertThat(store.isKnownBad(chunk(0))).isTrue();
        assertThat(Files.exists(chunkDir.resolve(chunk(0)))).as("the scanner never deletes").isTrue();

        Files.write(chunkDir.resolve(chunk(0)), data);                     // the file is replaced by good bytes
        assertThat(scanner.scanOnce()).isZero();
        assertThat(store.isKnownBad(chunk(0))).isFalse();
        assertThat(TestSupport.pending(changes)).isFalse();
    }

    @Test
    void aMissingSidecarAtRuntimeMakesTheChunkBad() throws Exception {
        write(0, 500);
        TestSupport.drain(changes);
        Files.delete(chunkDir.resolve(chunk(0) + ".meta"));

        assertThat(scanner.scanOnce()).isEqualTo(1);

        assertThat(TestSupport.drain(changes).bad()).containsExactly(chunk(0));
        assertThat(store.list()).containsExactly(new ChunkInfo(chunk(0), 500, ChunkStore.UNVERIFIED));
    }

    @Test
    void theRateLimitHolds() throws Exception {
        open(32L * MIB);
        for (int i = 0; i < 64; i++) {
            write(i, MIB);
        }
        long started = System.nanoTime();

        assertThat(scanner.scanOnce()).isZero();

        double seconds = (System.nanoTime() - started) / 1e9;
        assertThat(seconds).as("64 MiB at 32 MiB/s").isGreaterThanOrEqualTo(1.9);
    }
}
