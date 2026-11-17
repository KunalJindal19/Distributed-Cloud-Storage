package com.objectstore.worker.reporting;

import com.objectstore.common.dto.ChunkInfo;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.stereotype.Component;

/**
 * The events that will make up the next incremental block report: chunks received, deleted and
 * found bad since the previous report. Written by the store, the executor and the scanner; drained
 * by the reporter. On overflow it is cleared and a full report is sent instead, which supersedes
 * every buffered event.
 */
@Component
public class IbrBuffer {

    /** The contents of one incremental report. */
    public record Batch(List<ChunkInfo> received, List<String> deleted, List<String> bad) {

        public boolean isEmpty() {
            return received.isEmpty() && deleted.isEmpty() && bad.isEmpty();
        }
    }

    private final Map<String, ChunkInfo> received = new LinkedHashMap<>();
    private final Set<String> deleted = new LinkedHashSet<>();
    private final Set<String> bad = new LinkedHashSet<>();
    private final int max;
    private boolean overflowed;

    public IbrBuffer(com.objectstore.worker.config.WorkerProperties properties) {
        this.max = properties.ibrBufferMax();
    }

    public synchronized void received(ChunkInfo chunk) {
        deleted.remove(chunk.chunkId());
        bad.remove(chunk.chunkId());
        received.put(chunk.chunkId(), chunk);
        checkOverflow();
    }

    public synchronized void deleted(String chunkId) {
        received.remove(chunkId);
        bad.remove(chunkId);
        deleted.add(chunkId);
        checkOverflow();
    }

    public synchronized void bad(String chunkId) {
        received.remove(chunkId);
        bad.add(chunkId);
        checkOverflow();
    }

    public synchronized boolean hasEvents() {
        return !received.isEmpty() || !deleted.isEmpty() || !bad.isEmpty();
    }

    /** Takes everything buffered so far. */
    public synchronized Batch drain() {
        Batch batch = new Batch(new ArrayList<>(received.values()), new ArrayList<>(deleted), new ArrayList<>(bad));
        clear();
        return batch;
    }

    /** A full report supersedes every buffered event. */
    public synchronized void clear() {
        received.clear();
        deleted.clear();
        bad.clear();
    }

    /** True once if the buffer overflowed since the last call. */
    public synchronized boolean takeOverflow() {
        boolean was = overflowed;
        overflowed = false;
        return was;
    }

    private void checkOverflow() {
        if (received.size() + deleted.size() + bad.size() > max) {
            clear();
            overflowed = true;
        }
    }
}
