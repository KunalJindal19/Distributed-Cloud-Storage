package com.objectstore.worker;

import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.ChangeType;
import com.objectstore.common.dto.ChunkInfo;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.reporting.ChangeLog;
import com.objectstore.worker.storage.ChunkStore;
import java.nio.file.Path;
import java.util.List;
import org.springframework.test.util.ReflectionTestUtils;

/** Shared helpers for the worker's unit tests. */
public final class TestSupport {

    /** The fence {@link #openStore} adopts; operation versions in tests are above it. */
    public static final long FENCE = 100;

    private TestSupport() {
    }

    public static WorkerProperties properties(Path dataDir, long scanBytesPerSec, int changeLogMax) {
        return new WorkerProperties("worker1", dataDir, "http://controller:8000", "", 1_048_576,
                2, 3, 250, changeLogMax, 2000, 256, 2000, 10, scanBytesPerSec, 30, 2, 10,
                100_000, 500, 20, 3, false, false);
    }

    public static WorkerProperties properties(Path dataDir) {
        return properties(dataDir, 1L << 30, 10_000);
    }

    /** A store after start-up recovery, with the fence {@link #FENCE} adopted (as after a registration). */
    public static ChunkStore openStore(WorkerProperties properties, ChangeLog changes) {
        ChunkStore store = new ChunkStore(properties, changes);
        ReflectionTestUtils.invokeMethod(store, "initialise");
        store.adoptFence(FENCE, null);
        return store;
    }

    /** The {@code n}-th operation version of a test: even and above the fence. */
    public static long v(int n) {
        return FENCE + 2L * n;
    }

    /** Every change not yet acknowledged, which are then acknowledged (as the controller would). */
    public static Drained drain(ChangeLog changes) {
        List<Change> all = changes.suffix(0, Integer.MAX_VALUE);
        changes.ack(changes.lastSeq());
        return new Drained(all);
    }

    public static boolean pending(ChangeLog changes) {
        return changes.size() > 0;
    }

    public record Drained(List<Change> changes) {

        public List<ChunkInfo> received() {
            return changes.stream().filter(c -> c.type() == ChangeType.RECEIVED)
                    .map(c -> new ChunkInfo(c.chunkId(), c.size(), c.checksum())).toList();
        }

        public List<Change> of(ChangeType type) {
            return changes.stream().filter(c -> c.type() == type).toList();
        }

        public List<String> deleted() {
            return ids(ChangeType.DELETED);
        }

        /** Chunks reported {@code DELETED(corrupt)}: quarantined. */
        public List<String> quarantined() {
            return changes.stream().filter(c -> c.type() == ChangeType.DELETED && "corrupt".equals(c.detail()))
                    .map(Change::chunkId).toList();
        }

        /** Chunks reported {@code DELETED(missing)}. */
        public List<String> missing() {
            return changes.stream().filter(c -> c.type() == ChangeType.DELETED && "missing".equals(c.detail()))
                    .map(Change::chunkId).toList();
        }

        public List<Change> cmdFailed() {
            return changes.stream().filter(c -> c.type() == ChangeType.CMD_FAILED).toList();
        }

        private List<String> ids(ChangeType type) {
            return changes.stream().filter(c -> c.type() == type).map(Change::chunkId).toList();
        }
    }
}
