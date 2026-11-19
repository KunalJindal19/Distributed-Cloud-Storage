package com.objectstore.worker.commands;

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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestClient;

/** Block 12 (v4 §4.5): a REPLICATE that cannot be completed appends CMD_FAILED; an expired one does not. */
class CommandExecutorTest {

    private static final String CHUNK = "3f1c9a52-7d0e-4c1b-9a54-1f2e3d4c5b6a-00000";
    private static final String UNREACHABLE = "http://127.0.0.1:1";

    @TempDir
    Path dataDir;

    private ChangeLog changes;
    private ChunkStore store;
    private CommandExecutor executor;

    @BeforeEach
    void setUp() {
        WorkerProperties properties = TestSupport.properties(dataDir);
        changes = new ChangeLog(properties);
        store = new ChunkStore(properties, changes);
        ReflectionTestUtils.invokeMethod(store, "initialise");
        executor = new CommandExecutor(store, changes, new ControllerClients(RestClient.builder(), properties));
    }

    @AfterEach
    void tearDown() {
        ReflectionTestUtils.invokeMethod(executor, "shutdown");
    }

    private static Command replicate(String cmdId, String checksum, double expiresAt) {
        return new Command(cmdId, InternalApi.COMMAND_REPLICATE, CHUNK, checksum, UNREACHABLE, null, expiresAt);
    }

    private List<Change> waitForChanges(int count) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (changes.size() < count && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        return changes.suffix(0, 100);
    }

    @Test
    void noLocalCopyReportsDeletedAndCmdFailed() throws Exception {
        executor.submit(replicate("cmd-1", "a".repeat(64), InternalApi.nowSeconds() + 60));

        List<Change> logged = waitForChanges(2);

        assertThat(logged).extracting(Change::type).containsExactly(ChangeType.DELETED, ChangeType.CMD_FAILED);
        assertThat(logged.get(1).cmdId()).isEqualTo("cmd-1");
        assertThat(logged.get(1).target()).isEqualTo(UNREACHABLE);
        assertThat(logged.get(1).reason()).isEqualTo("no local copy");
    }

    @Test
    void aCorruptLocalCopyReportsBadAndCmdFailed() throws Exception {
        byte[] data = "some chunk bytes".getBytes(StandardCharsets.US_ASCII);
        String sum = Checksums.sha256Hex(data);
        store.write(CHUNK, data, sum);
        changes.ack(changes.lastSeq());
        Files.write(store.chunkDir().resolve(CHUNK), "XXXX".getBytes(StandardCharsets.US_ASCII), StandardOpenOption.WRITE);

        executor.submit(replicate("cmd-2", sum, InternalApi.nowSeconds() + 60));

        List<Change> logged = waitForChanges(2);
        assertThat(logged).extracting(Change::type).containsExactly(ChangeType.BAD, ChangeType.CMD_FAILED);
        assertThat(logged.get(1).reason()).isEqualTo("local copy corrupt");
    }

    @Test
    void aFailedPutReportsCmdFailed() throws Exception {
        byte[] data = "some chunk bytes".getBytes(StandardCharsets.US_ASCII);
        String sum = Checksums.sha256Hex(data);
        store.write(CHUNK, data, sum);
        changes.ack(changes.lastSeq());

        executor.submit(replicate("cmd-3", sum, InternalApi.nowSeconds() + 60));

        List<Change> logged = waitForChanges(1);
        assertThat(logged).singleElement().satisfies(change -> {
            assertThat(change.type()).isEqualTo(ChangeType.CMD_FAILED);
            assertThat(change.cmdId()).isEqualTo("cmd-3");
            assertThat(change.chunkId()).isEqualTo(CHUNK);
        });
    }

    @Test
    void anExpiredCommandIsDroppedWithoutCmdFailed() throws Exception {
        assertThat(executor.submit(replicate("cmd-4", "a".repeat(64), InternalApi.nowSeconds() - 1))).isFalse();
        Thread.sleep(200);
        assertThat(changes.size()).isZero();
    }
}
