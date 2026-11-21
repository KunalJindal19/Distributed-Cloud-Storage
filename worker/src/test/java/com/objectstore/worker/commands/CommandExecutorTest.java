package com.objectstore.worker.commands;

import static com.objectstore.worker.TestSupport.v;
import static org.assertj.core.api.Assertions.assertThat;

import com.objectstore.common.Checksums;
import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.ChangeType;
import com.objectstore.common.dto.Command;
import com.objectstore.worker.TestSupport;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.reporting.ChangeLog;
import com.objectstore.worker.reporting.ControllerClients;
import com.objectstore.worker.storage.ChunkStore;
import com.objectstore.worker.storage.MutationGuard;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestClient;

/** The versioned commands (v5 §4.6): REPLICATE source rules, DELETE, VERIFY, the bounded executor. */
class CommandExecutorTest {

    private static final String CHUNK = "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a-00000";
    private static final String UNREACHABLE = "http://127.0.0.1:1";
    private static final byte[] DATA = "some chunk bytes".getBytes(StandardCharsets.US_ASCII);
    private static final String SUM = Checksums.sha256Hex(DATA);

    @TempDir
    Path dataDir;

    private ChangeLog changes;
    private ChunkStore store;
    private CommandExecutor executor;

    private void open(int queueMax) {
        WorkerProperties base = TestSupport.properties(dataDir);
        WorkerProperties properties = new WorkerProperties(base.id(), base.dataDir(), base.controllerUrl(),
                base.advertiseUrl(), base.maxChunkSize(), base.heartbeatIntervalSeconds(),
                base.heartbeatHttpTimeoutSeconds(), base.heartbeatMinGapMs(), base.changeLogMax(),
                base.maxChangesPerMessage(), base.maxChangesPerPutAnswer(), base.reportSegmentSize(),
                base.scanIntervalSeconds(), base.scanMaxBytesPerSec(), base.diskCheckIntervalSeconds(),
                base.connectTimeoutSeconds(), base.readTimeoutSeconds(), base.tombstoneMax(), queueMax,
                base.serverReadTimeoutSeconds(), base.shutdownDrainTimeoutSeconds(), false, false);
        changes = new ChangeLog(properties);
        store = TestSupport.openStore(properties, changes);
        executor = new CommandExecutor(store, changes, new ControllerClients(RestClient.builder(), properties),
                new MutationGuard(0), properties);
    }

    @AfterEach
    void tearDown() {
        if (executor != null) {
            ReflectionTestUtils.invokeMethod(executor, "shutdown");
        }
    }

    private static Command replicate(String cmdId, String checksum, String target) {
        return new Command(cmdId, InternalApi.COMMAND_REPLICATE, CHUNK, checksum, target, null, v(10));
    }

    private List<Change> waitForChanges(int count) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (changes.size() < count && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        Thread.sleep(50);
        return changes.suffix(0, 100);
    }

    @Test
    void aSourceWithoutTheChunkReportsNoLocalCopy() throws Exception {
        open(500);
        executor.submit(replicate("cmd-1", SUM, UNREACHABLE));

        List<Change> logged = waitForChanges(1);

        assertThat(logged).singleElement().satisfies(change -> {
            assertThat(change.type()).isEqualTo(ChangeType.CMD_FAILED);
            assertThat(change.reason()).isEqualTo("no_local_copy");
            assertThat(change.version()).isEqualTo(v(10));
        });
    }

    @Test
    void aSourceWhoseFileVanishedReportsMissingFirstThenNoLocalCopy() throws Exception {
        open(500);
        store.put(CHUNK, v(1), DATA, SUM);
        changes.ack(changes.lastSeq());
        Files.delete(store.chunkDir().resolve(CHUNK));

        executor.submit(replicate("cmd-2", SUM, UNREACHABLE));

        List<Change> logged = waitForChanges(2);
        assertThat(logged).extracting(Change::type).containsExactly(ChangeType.DELETED, ChangeType.CMD_FAILED);
        assertThat(logged.get(0).detail()).isEqualTo("missing");
        assertThat(logged.get(0).version() & 1).isEqualTo(1);
        assertThat(logged.get(1).reason()).isEqualTo("no_local_copy");
    }

    @Test
    void aCorruptLocalCopyIsQuarantinedBeforeCmdFailedLocalCopyCorrupt() throws Exception {
        open(500);
        store.put(CHUNK, v(1), DATA, SUM);
        changes.ack(changes.lastSeq());
        Files.write(store.chunkDir().resolve(CHUNK), "XXXX".getBytes(StandardCharsets.US_ASCII), StandardOpenOption.WRITE);

        executor.submit(replicate("cmd-3", SUM, UNREACHABLE));

        List<Change> logged = waitForChanges(2);
        assertThat(logged).extracting(Change::type).containsExactly(ChangeType.DELETED, ChangeType.CMD_FAILED);
        assertThat(logged.get(0).detail()).isEqualTo("corrupt");
        assertThat(logged.get(1).reason()).isEqualTo("local_copy_corrupt");
    }

    @Test
    void aCopyWhoseSidecarIsNotTheExpectedChecksumIsNotSent() throws Exception {
        open(500);
        store.put(CHUNK, v(1), DATA, SUM);
        changes.ack(changes.lastSeq());

        executor.submit(replicate("cmd-4", "b".repeat(64), UNREACHABLE));

        List<Change> logged = waitForChanges(2);
        assertThat(logged).extracting(Change::type).containsExactly(ChangeType.DELETED, ChangeType.CMD_FAILED);
        assertThat(logged.get(1).reason()).isEqualTo("local_copy_corrupt");
    }

    @Test
    void anUnreachableTargetIsTransferFailedNamingTheTarget() throws Exception {
        open(500);
        store.put(CHUNK, v(1), DATA, SUM);
        changes.ack(changes.lastSeq());

        executor.submit(replicate("cmd-5", SUM, UNREACHABLE));

        List<Change> logged = waitForChanges(1);
        assertThat(logged).singleElement().satisfies(change -> {
            assertThat(change.type()).isEqualTo(ChangeType.CMD_FAILED);
            assertThat(change.reason()).isEqualTo("transfer_failed");
            assertThat(change.target()).isEqualTo(UNREACHABLE);
            assertThat(change.cmdId()).isEqualTo("cmd-5");
        });
    }

    @Test
    void aDeleteCommandRunsTheVersionedDelete() throws Exception {
        open(500);
        store.put(CHUNK, v(1), DATA, SUM);
        changes.ack(changes.lastSeq());

        executor.submit(new Command("del", InternalApi.COMMAND_DELETE, CHUNK, null, null, "excess", v(2)));

        List<Change> logged = waitForChanges(1);
        assertThat(logged).singleElement().satisfies(change -> {
            assertThat(change.type()).isEqualTo(ChangeType.DELETED);
            assertThat(change.version()).isEqualTo(v(2));
        });
        assertThat(store.tombstone(CHUNK)).isEqualTo(v(2));
    }

    @Test
    void verifyAnswersWithReceivedCarryingItsCmdIdAndNeverCmdFailed() throws Exception {
        open(500);
        store.put(CHUNK, v(1), DATA, SUM);
        changes.ack(changes.lastSeq());

        executor.submit(new Command("verify-1", InternalApi.COMMAND_VERIFY, CHUNK, SUM, null, null, 0));
        executor.submit(new Command("verify-2", InternalApi.COMMAND_VERIFY,
                "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a-00009", SUM, null, null, 0));   // not here

        List<Change> logged = waitForChanges(1);
        assertThat(logged).singleElement().satisfies(change -> {
            assertThat(change.type()).isEqualTo(ChangeType.RECEIVED);
            assertThat(change.cmdId()).isEqualTo("verify-1");
            assertThat(change.version()).isEqualTo(v(1));
        });
    }

    @Test
    void aMalformedCommandIsABadCommand() throws Exception {
        open(500);
        executor.submit(new Command("odd", InternalApi.COMMAND_DELETE, CHUNK, null, null, "excess", 7));   // odd version

        List<Change> logged = waitForChanges(1);
        assertThat(logged).singleElement().extracting(Change::reason).isEqualTo("bad_command");
    }

    @Test
    void beyondTheQueueLimitReplicateAndDeleteAreBusyAndVerifyIsDropped() throws Exception {
        open(1);
        store.put(CHUNK, v(1), DATA, SUM);
        changes.ack(changes.lastSeq());
        try (ServerSocket silent = new ServerSocket(0)) {       // accepts connections, never answers
            String target = "http://127.0.0.1:" + silent.getLocalPort();
            for (int i = 0; i < CommandExecutor.MAX_CONCURRENT; i++) {     // one at a time: the queue holds only 1
                assertThat(executor.submit(replicate("busy-" + i, SUM, target))).isTrue();
                long deadline = System.nanoTime() + 5_000_000_000L;
                while (executor.activeTransfers() < i + 1 && System.nanoTime() < deadline) {
                    Thread.sleep(5);
                }
                assertThat(executor.activeTransfers()).isEqualTo(i + 1);
            }
            assertThat(executor.submit(replicate("waits", SUM, target))).isTrue();        // the one waiting slot
            assertThat(executor.submit(new Command("del", InternalApi.COMMAND_DELETE, CHUNK, null, null, "excess", v(3))))
                    .isFalse();
            assertThat(executor.submit(new Command("ver", InternalApi.COMMAND_VERIFY, CHUNK, SUM, null, null, 0))).isFalse();

            List<Change> logged = changes.suffix(0, 100);
            assertThat(logged).singleElement().satisfies(change -> {
                assertThat(change.type()).isEqualTo(ChangeType.CMD_FAILED);
                assertThat(change.reason()).isEqualTo("busy");
                assertThat(change.version()).isEqualTo(v(3));
            });
        }
    }
}
