package com.objectstore.worker;

import static com.objectstore.worker.TestSupport.FENCE;
import static com.objectstore.worker.TestSupport.v;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.objectstore.common.Checksums;
import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.ChangeType;
import com.objectstore.common.dto.ChunkInfo;
import com.objectstore.common.dto.ReportEntry;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.reporting.ChangeLog;
import com.objectstore.worker.storage.ChunkStore;
import com.objectstore.worker.storage.ChunkStore.DeleteOutcome;
import com.objectstore.worker.storage.ChunkStore.PutOutcome;
import com.objectstore.worker.storage.ChunkStore.VerifyOutcome;
import com.objectstore.worker.storage.FailStop;
import com.objectstore.worker.storage.MutationGuard;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.unit.DataSize;

/** The worker as a versioned store (v5 §4): the version check, tombstones, the fence, quarantine, install order. */
class ChunkStoreTest {

    private static final String CHUNK_A = "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a-00000";
    private static final String CHUNK_B = "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a-00001";

    @TempDir
    Path dataDir;

    private WorkerProperties properties;
    private ChangeLog changes;
    private FailStop failStop;
    private MutationGuard guard;
    private ChunkStore store;
    private Path chunkDir;

    @BeforeEach
    void setUp() {
        properties = TestSupport.properties(dataDir);
        chunkDir = properties.chunkDir();
        changes = new ChangeLog(properties);
        store = newStore(true);
    }

    private ChunkStore newStore(boolean fenced) {
        failStop = new FailStop();
        guard = new MutationGuard(200);
        ChunkStore created = new ChunkStore(properties, changes, failStop, guard);
        ReflectionTestUtils.invokeMethod(created, "initialise");
        if (fenced) {
            created.adoptFence(FENCE, null);
        }
        return created;
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

    private Change only(ChangeType type) {
        List<Change> of = TestSupport.drain(changes).of(type);
        assertThat(of).hasSize(1);
        return of.getFirst();
    }

    // ---- installing a copy ------------------------------------------------------------------------

    @Test
    void writesTheDataAndASidecarAndLogsReceivedAtTheVersion() throws IOException {
        byte[] data = bytes(1000, 1);
        String sha = Checksums.sha256Hex(data);

        assertThat(store.put(CHUNK_A, v(1), data, sha)).isEqualTo(PutOutcome.CREATED);

        assertThat(filesIn(chunkDir)).containsExactly(CHUNK_A, CHUNK_A + ".meta");
        assertThat(Files.readString(chunkDir.resolve(CHUNK_A + ".meta")).trim()).isEqualTo(sha + " 1000");
        Change received = only(ChangeType.RECEIVED);
        assertThat(received.version()).isEqualTo(v(1));
        assertThat(received.checksum()).isEqualTo(sha);
        assertThat(store.index().get(CHUNK_A).version()).isEqualTo(v(1));
    }

    @Test
    void rejectsAWrongChecksumAndLeavesNothingBehind() throws IOException {
        byte[] data = bytes(100, 2);
        assertThatThrownBy(() -> store.put(CHUNK_A, v(1), data, "0".repeat(64)))
                .isInstanceOf(ChunkStore.ChecksumMismatchException.class);
        assertThat(filesIn(chunkDir)).isEmpty();
        assertThat(TestSupport.pending(changes)).isFalse();
    }

    @Test
    void beforeTheFirstFenceEveryVersionedOperationIsStale() {
        store = newStore(false);
        byte[] data = bytes(10, 3);

        assertThat(store.put(CHUNK_A, v(1), data, Checksums.sha256Hex(data))).isEqualTo(PutOutcome.STALE);
        assertThat(store.delete(CHUNK_A, v(2), false, "cmd")).isEqualTo(DeleteOutcome.STALE);

        assertThat(store.index()).isEmpty();
        assertThat(TestSupport.drain(changes).cmdFailed()).extracting(Change::reason).containsExactly("stale", "stale");
    }

    @Test
    void aPutAtOrBelowTheFenceOrTheRecordIsStaleAndChangesNothing() {
        byte[] data = bytes(10, 4);
        String sha = Checksums.sha256Hex(data);
        assertThat(store.put(CHUNK_A, FENCE, data, sha)).isEqualTo(PutOutcome.STALE);   // not above the fence
        assertThat(store.put(CHUNK_A, v(5), data, sha)).isEqualTo(PutOutcome.CREATED);
        TestSupport.drain(changes);

        assertThat(store.put(CHUNK_A, v(3), data, sha)).isEqualTo(PutOutcome.STALE);   // older than the record

        Change failed = only(ChangeType.CMD_FAILED);
        assertThat(failed.reason()).isEqualTo("stale");
        assertThat(failed.version()).isEqualTo(v(3));
        assertThat(store.index().get(CHUNK_A).version()).isEqualTo(v(5));
    }

    @Test
    void aRepeatedPutReLogsTheSameResultSoALostAnswerIsDeliveredAgain() {
        byte[] data = bytes(10, 5);
        String sha = Checksums.sha256Hex(data);
        store.put(CHUNK_A, v(1), data, sha);
        TestSupport.drain(changes);

        assertThat(store.put(CHUNK_A, v(1), data, sha)).isEqualTo(PutOutcome.REPEAT);

        Change received = only(ChangeType.RECEIVED);
        assertThat(received.version()).isEqualTo(v(1));
    }

    @Test
    void edge10_aNewerPutOfTheSameIntactBytesOnlyTakesTheNewVersion() throws IOException {
        byte[] data = bytes(800, 4);
        String sha = Checksums.sha256Hex(data);
        store.put(CHUNK_A, v(1), data, sha);
        long written = Files.getLastModifiedTime(chunkDir.resolve(CHUNK_A)).toMillis();
        TestSupport.drain(changes);

        assertThat(store.put(CHUNK_A, v(2), data, sha)).isEqualTo(PutOutcome.REVERIFIED);

        assertThat(only(ChangeType.RECEIVED).version()).isEqualTo(v(2));
        assertThat(store.index().get(CHUNK_A).version()).isEqualTo(v(2));
        assertThat(Files.getLastModifiedTime(chunkDir.resolve(CHUNK_A)).toMillis()).as("no disk write").isEqualTo(written);
    }

    @Test
    void edge10_aNewerPutOverRottenBytesQuarantinesThemFirstAndInstallsTheVerifiedCopy() throws IOException {
        byte[] data = bytes(800, 5);
        String sha = Checksums.sha256Hex(data);
        store.put(CHUNK_A, v(1), data, sha);
        Files.write(chunkDir.resolve(CHUNK_A), "XXXX".getBytes(StandardCharsets.US_ASCII), StandardOpenOption.WRITE);
        TestSupport.drain(changes);

        assertThat(store.put(CHUNK_A, v(2), data, sha)).isEqualTo(PutOutcome.CREATED);

        TestSupport.Drained drained = TestSupport.drain(changes);
        assertThat(drained.quarantined()).containsExactly(CHUNK_A);                  // DELETED(odd, corrupt) first
        assertThat(drained.of(ChangeType.RECEIVED)).singleElement().extracting(Change::version).isEqualTo(v(2));
        assertThat(drained.changes().getFirst().type()).isEqualTo(ChangeType.DELETED);
        assertThat(Files.readAllBytes(chunkDir.resolve(CHUNK_A))).isEqualTo(data);
        assertThat(filesIn(store.quarantineDir())).hasSize(1);
        assertThat(store.isQuarantined(CHUNK_A)).isFalse();
    }

    @Test
    void theSameChunkAndVersionCanBeReceivedOnlyOnceAtATime() {
        assertThat(store.beginIncoming(CHUNK_A, v(1))).isTrue();
        assertThat(store.beginIncoming(CHUNK_A, v(1))).as("409 in_progress").isFalse();
        assertThat(store.beginIncoming(CHUNK_A, v(2))).isTrue();
        store.endIncoming(CHUNK_A, v(1));
        assertThat(store.beginIncoming(CHUNK_A, v(1))).isTrue();
    }

    @Test
    void storageCapacityCapsTheFreeSpaceAndRefusesCopiesBeyondIt() throws IOException {
        properties = TestSupport.properties(dataDir, 1L << 30, 10_000, DataSize.ofBytes(2500));
        store = newStore(true);
        String chunkC = CHUNK_A.substring(0, CHUNK_A.length() - 1) + "2";
        byte[] a = bytes(1000, 1);
        byte[] b = bytes(1000, 2);
        byte[] c = bytes(1000, 3);
        assertThat(store.freeBytes()).isEqualTo(2500);

        assertThat(store.put(CHUNK_A, v(1), a, Checksums.sha256Hex(a))).isEqualTo(PutOutcome.CREATED);
        assertThat(store.put(CHUNK_B, v(1), b, Checksums.sha256Hex(b))).isEqualTo(PutOutcome.CREATED);
        assertThat(store.freeBytes()).isEqualTo(500);
        assertThat(store.stats(0).freeBytes()).as("what the heartbeat reports").isEqualTo(500);

        assertThatThrownBy(() -> store.put(chunkC, v(1), c, Checksums.sha256Hex(c)))
                .as("507: over the cap").isInstanceOf(ChunkStore.InsufficientStorageException.class);
        assertThat(filesIn(chunkDir)).doesNotContain(chunkC);
        // A copy it already holds needs no new room: a re-verify still succeeds at the cap.
        assertThat(store.put(CHUNK_A, v(2), a, Checksums.sha256Hex(a))).isEqualTo(PutOutcome.REVERIFIED);

        assertThat(store.delete(CHUNK_B, v(2), false, "cmd")).isEqualTo(DeleteOutcome.DELETED);
        assertThat(store.freeBytes()).isEqualTo(1500);
        assertThat(store.put(chunkC, v(1), c, Checksums.sha256Hex(c))).isEqualTo(PutOutcome.CREATED);
    }

    @Test
    void withoutACapTheFreeSpaceIsTheVolumes() throws IOException {
        long volume = Files.getFileStore(chunkDir).getUsableSpace();
        assertThat(store.freeBytes()).isBetween(volume - (64L << 20), volume + (64L << 20));
    }

    // ---- deleting -------------------------------------------------------------------------------------

    @Test
    void aDeleteRemovesBothFilesAndLeavesATombstone() throws IOException {
        byte[] data = bytes(100, 6);
        store.put(CHUNK_A, v(1), data, Checksums.sha256Hex(data));
        TestSupport.drain(changes);

        assertThat(store.delete(CHUNK_A, v(2), false, "cmd")).isEqualTo(DeleteOutcome.DELETED);

        assertThat(filesIn(chunkDir)).isEmpty();
        Change deleted = only(ChangeType.DELETED);
        assertThat(deleted.version()).isEqualTo(v(2));
        assertThat(deleted.detail()).isEqualTo("deleted");
        assertThat(store.tombstone(CHUNK_A)).isEqualTo(v(2));
    }

    @Test
    void aDuplicateDeleteIsHarmlessAndAcknowledgedAgain() {
        byte[] data = bytes(100, 7);
        store.put(CHUNK_A, v(1), data, Checksums.sha256Hex(data));
        store.delete(CHUNK_A, v(2), false, "cmd-1");
        TestSupport.drain(changes);

        assertThat(store.delete(CHUNK_A, v(2), false, "cmd-2")).isEqualTo(DeleteOutcome.REPEAT);

        assertThat(only(ChangeType.DELETED).version()).isEqualTo(v(2));
    }

    @Test
    void anOldDeleteAfterANewerCopyLandedIsStaleAndTheCopySurvives() {
        // The v4 double delete (v5 §3.1): DELETE v2 waited in the executor while a new copy v3 landed.
        byte[] data = bytes(100, 8);
        String sha = Checksums.sha256Hex(data);
        store.put(CHUNK_A, v(1), data, sha);
        store.delete(CHUNK_A, v(2), false, "first");
        store.put(CHUNK_A, v(3), data, sha);
        TestSupport.drain(changes);

        assertThat(store.delete(CHUNK_A, v(2), false, "resent")).isEqualTo(DeleteOutcome.STALE);

        assertThat(store.index()).containsKey(CHUNK_A);
        assertThat(only(ChangeType.CMD_FAILED).reason()).isEqualTo("stale");
    }

    @Test
    void anOldPutAfterANewerDeleteIsRejectedEvenAfterTheDeletedWasAcknowledged() {
        byte[] data = bytes(100, 9);
        String sha = Checksums.sha256Hex(data);
        store.put(CHUNK_A, v(1), data, sha);
        assertThat(store.delete(CHUNK_A, v(4), false, "delete")).isEqualTo(DeleteOutcome.DELETED);
        TestSupport.drain(changes);                                   // DELETED(v4) acknowledged

        assertThat(store.put(CHUNK_A, v(3), data, sha)).as("a delayed REPLICATE v3").isEqualTo(PutOutcome.STALE);

        assertThat(store.index()).doesNotContainKey(CHUNK_A);
        assertThat(store.tombstone(CHUNK_A)).as("tombstones stay until a fence covers them").isEqualTo(v(4));
    }

    @Test
    void aFenceRemovesTheTombstonesItCoversAndItselfRejectsTheirVersions() {
        byte[] data = bytes(100, 10);
        String sha = Checksums.sha256Hex(data);
        store.put(CHUNK_A, v(1), data, sha);
        store.delete(CHUNK_A, v(2), false, "d");
        store.put(CHUNK_B, v(3), data, sha);
        store.delete(CHUNK_B, v(10), false, "d");
        assertThat(store.tombstoneCount()).isEqualTo(2);

        store.adoptFence(v(5), null);

        assertThat(store.tombstoneCount()).isEqualTo(1);
        assertThat(store.tombstone(CHUNK_B)).isEqualTo(v(10));
        assertThat(store.put(CHUNK_A, v(4), data, sha)).as("below the fence").isEqualTo(PutOutcome.STALE);
        assertThat(store.put(CHUNK_A, v(6), data, sha)).isEqualTo(PutOutcome.CREATED);
    }

    @Test
    void aCorruptDeleteQuarantinesTheBytesInsteadOfDeletingThem() throws IOException {
        byte[] data = bytes(300, 11);
        store.put(CHUNK_A, v(1), data, Checksums.sha256Hex(data));
        TestSupport.drain(changes);

        assertThat(store.delete(CHUNK_A, v(2), true, "c")).isEqualTo(DeleteOutcome.DELETED);

        Change deleted = only(ChangeType.DELETED);
        assertThat(deleted.version()).isEqualTo(v(2));
        assertThat(deleted.detail()).isEqualTo("corrupt");
        assertThat(filesIn(chunkDir)).isEmpty();
        assertThat(filesIn(store.quarantineDir())).hasSize(1);
        assertThat(Files.readAllBytes(store.quarantineDir().resolve(filesIn(store.quarantineDir()).getFirst())))
                .isEqualTo(data);
        assertThatThrownBy(() -> store.read(CHUNK_A)).isInstanceOf(ChunkStore.CorruptChunkException.class);
    }

    // ---- local observations ---------------------------------------------------------------------------

    @Test
    void aReadVerifiesAgainstTheSidecarAndQuarantinesDamageAtAnOddVersion() throws IOException {
        byte[] data = bytes(500, 12);
        store.put(CHUNK_A, v(1), data, Checksums.sha256Hex(data));
        assertThat(store.read(CHUNK_A).orElseThrow().data()).isEqualTo(data);
        TestSupport.drain(changes);
        Files.write(chunkDir.resolve(CHUNK_A), "XXXX".getBytes(StandardCharsets.US_ASCII), StandardOpenOption.WRITE);

        assertThatThrownBy(() -> store.read(CHUNK_A)).isInstanceOf(ChunkStore.CorruptChunkException.class);
        assertThatThrownBy(() -> store.read(CHUNK_A)).as("still refused").isInstanceOf(ChunkStore.CorruptChunkException.class);

        Change quarantined = only(ChangeType.DELETED);
        assertThat(quarantined.detail()).isEqualTo("corrupt");
        assertThat(quarantined.version()).isEqualTo(v(1) | 1);       // max(record, fence) | 1
        assertThat(store.tombstone(CHUNK_A)).isEqualTo(v(1) | 1);
    }

    @Test
    void aDamagedSidecarQuarantinesTheCopyToo() throws IOException {
        byte[] data = bytes(300, 13);
        store.put(CHUNK_A, v(1), data, Checksums.sha256Hex(data));
        TestSupport.drain(changes);
        Files.writeString(chunkDir.resolve(CHUNK_A + ".meta"), "not a sidecar\n");

        assertThatThrownBy(() -> store.read(CHUNK_A)).isInstanceOf(ChunkStore.CorruptChunkException.class);

        assertThat(TestSupport.drain(changes).quarantined()).containsExactly(CHUNK_A);
    }

    @Test
    void verifyPassesWithTheCurrentVersionFailsByQuarantineAndReportsMissing() throws IOException {
        byte[] data = bytes(300, 14);
        String sha = Checksums.sha256Hex(data);
        store.put(CHUNK_A, v(1), data, sha);
        store.put(CHUNK_B, v(2), data, sha);
        TestSupport.drain(changes);

        assertThat(store.verifyCopy(CHUNK_A, sha, "verify-1")).isEqualTo(VerifyOutcome.PASS);
        Change answer = only(ChangeType.RECEIVED);
        assertThat(answer.cmdId()).isEqualTo("verify-1");
        assertThat(answer.version()).isEqualTo(v(1));

        assertThat(store.verifyCopy(CHUNK_A, "f".repeat(64), "verify-2")).as("not the expected checksum")
                .isEqualTo(VerifyOutcome.FAILED);
        assertThat(TestSupport.drain(changes).quarantined()).containsExactly(CHUNK_A);

        Files.delete(chunkDir.resolve(CHUNK_B));
        assertThat(store.verifyCopy(CHUNK_B, sha, "verify-3")).isEqualTo(VerifyOutcome.MISSING);
        assertThat(TestSupport.drain(changes).missing()).containsExactly(CHUNK_B);
        assertThat(TestSupport.drain(changes).cmdFailed()).as("VERIFY never logs CMD_FAILED").isEmpty();
    }

    // ---- listing, recovery, ids ---------------------------------------------------------------------------

    @Test
    void theListingCarriesVersionsAndListVerifiedRehashes() throws IOException {
        byte[] a = bytes(300, 1);
        byte[] b = bytes(700, 2);
        store.put(CHUNK_A, v(1), a, Checksums.sha256Hex(a));
        store.put(CHUNK_B, v(2), b, Checksums.sha256Hex(b));

        assertThat(store.reportEntries()).containsExactly(
                new ReportEntry(CHUNK_A, 300, Checksums.sha256Hex(a), v(1)),
                new ReportEntry(CHUNK_B, 700, Checksums.sha256Hex(b), v(2)));

        byte[] damaged = a.clone();
        damaged[0] ^= 1;
        Files.write(chunkDir.resolve(CHUNK_A), damaged);
        assertThat(store.listVerified()).containsExactly(
                new ChunkInfo(CHUNK_A, 300, Checksums.sha256Hex(damaged)),
                new ChunkInfo(CHUNK_B, 700, Checksums.sha256Hex(b)));
    }

    @Test
    void startUpRecoveryCleansCrashLeftoversIndexesAtVersionZeroAndHasNoFence() throws IOException {
        byte[] data = bytes(400, 9);
        Files.write(chunkDir.resolve(CHUNK_A), data);                                       // data without sidecar
        Files.writeString(chunkDir.resolve(CHUNK_B + ".meta"), "x 1\n");                     // sidecar without data
        Files.write(chunkDir.resolve(CHUNK_A + ".deadbeef.tmp"), new byte[] {1, 2, 3});     // data temp file
        Files.write(chunkDir.resolve(CHUNK_B + ".meta.cafe.tmp"), new byte[] {4});          // sidecar temp file
        Files.write(store.quarantineDir().resolve(CHUNK_B + ".7.bad"), new byte[] {1, 2, 3, 4, 5});

        store = newStore(false);

        assertThat(filesIn(chunkDir)).containsExactly(CHUNK_A, CHUNK_A + ".meta");
        assertThat(store.readMeta(CHUNK_A).orElseThrow().checksum()).isEqualTo(Checksums.sha256Hex(data));
        assertThat(store.index().get(CHUNK_A).version()).isZero();
        assertThat(store.isFenceSet()).isFalse();
        assertThat(store.tombstoneCount()).isZero();
        assertThat(store.stats(0).quarantinedChunks()).isEqualTo(1);
        assertThat(store.stats(0).quarantinedBytes()).isEqualTo(5);
    }

    @Test
    void refusesIdsThatCouldEscapeTheChunkDirectory() throws IOException {
        byte[] data = bytes(10, 1);
        String checksum = Checksums.sha256Hex(data);

        for (String evil : List.of("../escape", "..", "a/b", CHUNK_A + "/../../x", "test-00000")) {
            assertThatThrownBy(() -> store.put(evil, v(1), data, checksum)).isInstanceOf(ChunkStore.InvalidChunkIdException.class);
            assertThatThrownBy(() -> store.read(evil)).isInstanceOf(ChunkStore.InvalidChunkIdException.class);
            assertThatThrownBy(() -> store.delete(evil, v(2), false, "x")).isInstanceOf(ChunkStore.InvalidChunkIdException.class);
        }
        assertThat(filesIn(dataDir)).containsExactly("chunks", "quarantine");
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
    void theIndexAndItsTotalsEqualAFreshDiskWalkAfterAnyPutsAndDeletes() throws IOException {
        java.util.Random random = new java.util.Random(11);
        java.util.Map<String, byte[]> contents = new java.util.HashMap<>();
        int version = 1;
        for (int op = 0; op < 400; op++) {
            String chunkId = "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a-%05d".formatted(random.nextInt(40));
            byte[] data = contents.computeIfAbsent(chunkId, id -> bytes(100 + random.nextInt(900), id.hashCode()));
            switch (random.nextInt(4)) {
                case 0, 1 -> store.put(chunkId, v(version++), data, Checksums.sha256Hex(data));
                case 2 -> store.delete(chunkId, v(version++), false, "d");
                default -> {
                    if (Files.exists(chunkDir.resolve(chunkId))) {       // rot it, then put it again
                        Files.write(chunkDir.resolve(chunkId), "rot".getBytes(StandardCharsets.US_ASCII),
                                StandardOpenOption.WRITE);
                        assertThat(store.put(chunkId, v(version++), data, Checksums.sha256Hex(data)))
                                .isEqualTo(PutOutcome.CREATED);
                        assertThat(Files.readAllBytes(chunkDir.resolve(chunkId))).isEqualTo(data);
                    }
                }
            }
        }
        ChunkStore fresh = newStore(false);   // a new process: the index comes from one disk walk (versions 0)
        assertThat(store.index().keySet()).isEqualTo(fresh.index().keySet());
        long bytesOnDisk = 0;
        for (String chunkId : fresh.index().keySet()) {
            bytesOnDisk += Files.size(chunkDir.resolve(chunkId));
            assertThat(store.index().get(chunkId).checksum()).isEqualTo(fresh.index().get(chunkId).checksum());
        }
        assertThat(store.stats(0).chunks()).isEqualTo(fresh.index().size());
        assertThat(store.stats(0).usedBytes()).isEqualTo(bytesOnDisk);
        assertThat(fresh.stats(0).quarantinedChunks()).isEqualTo(store.stats(0).quarantinedChunks());
    }

    @Test
    void statsAreO1AndNeverListTheChunks() {
        byte[] data = bytes(10, 1);
        store.put(CHUNK_A, v(1), data, Checksums.sha256Hex(data));
        ChunkStore spy = org.mockito.Mockito.spy(store);

        assertThat(spy.stats(0).chunks()).isEqualTo(1);

        org.mockito.Mockito.verify(spy, org.mockito.Mockito.never()).reportEntries();
        org.mockito.Mockito.verify(spy, org.mockito.Mockito.never()).listVerified();
        org.mockito.Mockito.verify(spy, org.mockito.Mockito.never()).chunkIds();
    }

    // ---- faults and shutdown ------------------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(strings = {"data-temp", "meta-temp", "meta-rename"})
    void aFaultBeforeTheCommitPointCleansUpAndAnswers500(String step) throws IOException {
        byte[] data = bytes(100, 15);
        store.setFaultsForTests(at -> {
            if (at.equals(step)) {
                throw new IOException("injected at " + at);
            }
        });

        assertThatThrownBy(() -> store.put(CHUNK_A, v(1), data, Checksums.sha256Hex(data)))
                .isInstanceOf(ChunkStore.StoreFailedException.class);

        assertThat(filesIn(chunkDir)).as("no temp file, no sidecar, no data").isEmpty();
        assertThat(store.index()).isEmpty();
        assertThat(TestSupport.pending(changes)).isFalse();
        assertThat(failStop.failed()).isFalse();
        store.setFaultsForTests(null);
        assertThat(store.put(CHUNK_A, v(2), data, Checksums.sha256Hex(data))).isEqualTo(PutOutcome.CREATED);
    }

    @ParameterizedTest
    @ValueSource(strings = {"data-rename", "after-commit"})
    void aFaultAtOrAfterTheCommitPointFailStops(String step) {
        byte[] data = bytes(100, 16);
        store.setFaultsForTests(at -> {
            if (at.equals(step)) {
                throw new IOException("injected at " + at);
            }
        });

        assertThatThrownBy(() -> store.put(CHUNK_A, v(1), data, Checksums.sha256Hex(data)))
                .isInstanceOf(ChunkStore.UnavailableException.class);

        assertThat(failStop.failed()).isTrue();
        assertThat(store.isClosing()).isTrue();
        assertThatThrownBy(() -> store.read(CHUNK_B)).as("reads are refused too").isInstanceOf(ChunkStore.UnavailableException.class);
        assertThat(store.delete(CHUNK_A, v(3), false, "d")).isEqualTo(DeleteOutcome.REFUSED);
    }

    @Test
    void aDeleteFaultBeforeTheUnlinkIsAnIoErrorAndAtTheUnlinkFailStops() {
        byte[] data = bytes(100, 17);
        store.put(CHUNK_A, v(1), data, Checksums.sha256Hex(data));
        TestSupport.drain(changes);
        store.setFaultsForTests(at -> {
            if (at.equals("delete-prepare")) {
                throw new IOException("injected");
            }
        });
        assertThat(store.delete(CHUNK_A, v(2), false, "d")).isEqualTo(DeleteOutcome.IO_ERROR);
        assertThat(only(ChangeType.CMD_FAILED).reason()).isEqualTo("io_error");
        assertThat(store.index()).containsKey(CHUNK_A);

        store.setFaultsForTests(at -> {
            if (at.equals("delete-unlink")) {
                throw new IOException("injected");
            }
        });
        assertThatThrownBy(() -> store.delete(CHUNK_A, v(3), false, "d")).isInstanceOf(ChunkStore.UnavailableException.class);
        assertThat(failStop.failed()).isTrue();
    }

    @Test
    void whileShuttingDownNewMutationsAreRefusedAndOneInProgressFinishes() throws Exception {
        byte[] data = bytes(100, 18);
        java.util.concurrent.CountDownLatch inside = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        store.setFaultsForTests(at -> {
            if (at.equals("meta-temp")) {
                inside.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        java.util.concurrent.atomic.AtomicReference<PutOutcome> outcome = new java.util.concurrent.atomic.AtomicReference<>();
        Thread put = Thread.ofPlatform().start(() -> outcome.set(store.put(CHUNK_A, v(1), data, Checksums.sha256Hex(data))));
        assertThat(inside.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

        Thread closer = Thread.ofPlatform().start(guard::close);
        Thread.sleep(50);
        assertThat(guard.isClosing()).isTrue();
        assertThatThrownBy(() -> store.put(CHUNK_B, v(2), data, Checksums.sha256Hex(data)))
                .isInstanceOf(ChunkStore.UnavailableException.class);
        release.countDown();
        put.join(5000);
        closer.join(5000);

        assertThat(outcome.get()).as("the PUT that had entered finished").isEqualTo(PutOutcome.CREATED);
        assertThat(guard.inFlight()).isZero();
        assertThat(filesIn(chunkDir)).containsExactly(CHUNK_A, CHUNK_A + ".meta");
    }
}
