package com.objectstore.worker;

import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.ChangeType;
import com.objectstore.common.dto.ChunkInfo;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.reporting.ChangeLog;
import java.nio.file.Path;
import java.util.List;

/** Shared helpers for the worker's unit tests. */
public final class TestSupport {

    private TestSupport() {
    }

    public static WorkerProperties properties(Path dataDir, long scanBytesPerSec, int changeLogMax) {
        return new WorkerProperties("worker1", dataDir, "http://controller:8000", "", 1_048_576,
                2, 3, 30, 250, changeLogMax, 2000, 256, 2000, 10, scanBytesPerSec, 30, 2, 10, false);
    }

    public static WorkerProperties properties(Path dataDir) {
        return properties(dataDir, 1L << 30, 10_000);
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

        public List<String> deleted() {
            return ids(ChangeType.DELETED);
        }

        public List<String> bad() {
            return ids(ChangeType.BAD);
        }

        public List<Change> cmdFailed() {
            return changes.stream().filter(c -> c.type() == ChangeType.CMD_FAILED).toList();
        }

        private List<String> ids(ChangeType type) {
            return changes.stream().filter(c -> c.type() == type).map(Change::chunkId).toList();
        }
    }
}
