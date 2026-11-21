package com.objectstore.worker.storage;

import static com.objectstore.worker.TestSupport.v;
import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.Checksums;
import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.ChangeType;
import com.objectstore.common.dto.ChunkInfo;
import com.objectstore.worker.TestSupport;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.reporting.ChangeLog;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The scanner (v5 §4.5): reconciliation of the index with the disk (files placed or removed by hand
 * are local observations with odd versions), verification with quarantine, the rate limit.
 */
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

    private void open(long rate) {
        WorkerProperties properties = TestSupport.properties(dataDir, rate, 100_000);
        changes = new ChangeLog(properties);
        store = TestSupport.openStore(properties, changes);
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
        store.put(chunk(i), v(i + 1), data, Checksums.sha256Hex(data));
        return data;
    }

    private void age(String fileName) throws IOException {
        Files.setLastModifiedTime(chunkDir.resolve(fileName), FileTime.fromMillis(System.currentTimeMillis() - 60_000));
    }

    private List<Path> quarantine() throws IOException {
        try (Stream<Path> files = Files.list(store.quarantineDir())) {
            return files.toList();
        }
    }

    @Test
    void edge18_aFileRemovedByHandIsReportedMissingAtAnOddVersionWithinOnePass() throws Exception {
        write(0, 1000);
        write(1, 1000);
        TestSupport.drain(changes);
        Files.delete(chunkDir.resolve(chunk(0)));
        Files.delete(chunkDir.resolve(chunk(0) + ".meta"));

        scanner.scanOnce();

        List<Change> deleted = TestSupport.drain(changes).of(ChangeType.DELETED);
        assertThat(deleted).singleElement().satisfies(change -> {
            assertThat(change.chunkId()).isEqualTo(chunk(0));
            assertThat(change.detail()).isEqualTo("missing");
            assertThat(change.version() & 1).as("a local observation is odd").isEqualTo(1);
            assertThat(change.version()).isGreaterThan(v(1));
        });
        assertThat(store.index()).containsOnlyKeys(chunk(1));
        assertThat(store.stats(0).chunks()).isEqualTo(1);
        assertThat(store.stats(0).usedBytes()).isEqualTo(1000);
    }

    @Test
    void edge18_aFileAddedByHandIsIndexedAtAnOddVersionWithItsSidecarRebuilt() throws Exception {
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

        TestSupport.Drained drained = TestSupport.drain(changes);
        assertThat(drained.received()).containsExactlyInAnyOrder(
                new ChunkInfo(chunk(5), 700, Checksums.sha256Hex(data)),
                new ChunkInfo(chunk(6), 300, Checksums.sha256Hex(other)));
        assertThat(drained.of(ChangeType.RECEIVED)).allSatisfy(change ->
                assertThat(change.version() & 1).as("odd: never completes a controller op").isEqualTo(1));
        assertThat(store.readMeta(chunk(5)).orElseThrow().checksum()).isEqualTo(Checksums.sha256Hex(data));
        assertThat(store.index()).containsOnlyKeys(chunk(5), chunk(6));
        assertThat(store.stats(0).usedBytes()).isEqualTo(1000);
    }

    @Test
    void aCorruptedFileIsQuarantinedExactlyOnceAndItsBytesAreKept() throws Exception {
        write(0, 2000);
        write(1, 2000);
        TestSupport.drain(changes);
        Files.write(chunkDir.resolve(chunk(0)), "XXXX".getBytes(StandardCharsets.US_ASCII), StandardOpenOption.WRITE);

        assertThat(scanner.scanOnce()).isEqualTo(1);
        assertThat(scanner.scanOnce()).isZero();                          // no longer indexed

        TestSupport.Drained drained = TestSupport.drain(changes);
        assertThat(drained.quarantined()).containsExactly(chunk(0));
        assertThat(drained.of(ChangeType.DELETED).getFirst().version() & 1).isEqualTo(1);
        assertThat(Files.exists(chunkDir.resolve(chunk(0)))).isFalse();
        assertThat(quarantine()).as("the scanner never destroys bytes").hasSize(1);
        assertThat(quarantine().getFirst().getFileName().toString()).startsWith(chunk(0) + ".").endsWith(".bad");
        assertThat(store.stats(0).quarantinedChunks()).isEqualTo(1);
        assertThat(store.stats(0).quarantinedBytes()).isEqualTo(2000);
        assertThat(store.isQuarantined(chunk(0))).isTrue();
        assertThat(store.index()).containsOnlyKeys(chunk(1));
    }

    @Test
    void aMissingSidecarAtRuntimeQuarantinesTheCopy() throws Exception {
        write(0, 500);
        TestSupport.drain(changes);
        Files.delete(chunkDir.resolve(chunk(0) + ".meta"));

        assertThat(scanner.scanOnce()).isEqualTo(1);

        assertThat(TestSupport.drain(changes).quarantined()).containsExactly(chunk(0));
        assertThat(store.reportEntries()).isEmpty();
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
