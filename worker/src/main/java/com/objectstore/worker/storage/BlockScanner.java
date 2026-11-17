package com.objectstore.worker.storage;

import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.reporting.IbrBuffer;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * The DataNode block scanner: every SCAN_INTERVAL it re-hashes each chunk on disk, one at a
 * time, and compares the result with the sidecar. A mismatch is reported as {@code bad} (the
 * controller then replaces the copy and only afterwards orders its deletion). The scanner itself
 * never deletes anything.
 *
 * <p>A bad chunk is reported once per process lifetime, plus once more after each full report,
 * so a lost report does not hide the damage for ever and a known problem is not repeated every
 * ten seconds.
 */
@Component
public class BlockScanner implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(BlockScanner.class);

    private final ChunkStore store;
    private final IbrBuffer ibr;
    private final WorkerProperties properties;
    private final Set<String> reported = ConcurrentHashMap.newKeySet();

    public BlockScanner(ChunkStore store, IbrBuffer ibr, WorkerProperties properties) {
        this.store = store;
        this.ibr = ibr;
        this.properties = properties;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(properties.scanInterval());
                scanOnce();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                log.error("[scanner] pass failed", e);
            }
        }
    }

    /** One pass over every chunk. */
    public int scanOnce() {
        int corrupt = 0;
        for (String chunkId : store.chunkIds()) {
            Optional<String> onDisk = store.hashOnDisk(chunkId);
            Optional<ChunkStore.Meta> meta = store.readMeta(chunkId);
            if (onDisk.isEmpty()) {
                continue; // deleted meanwhile
            }
            if (meta.isEmpty() && !store.isSettled(chunkId)) {
                continue; // no sidecar yet: a write in progress
            }
            if (meta.isEmpty() || !onDisk.get().equals(meta.get().checksum())) {
                corrupt++;
                if (reported.add(chunkId)) {
                    log.warn("[scanner] CORRUPT chunk={} disk={} meta={}", chunkId, onDisk.get(),
                            meta.map(ChunkStore.Meta::checksum).orElse("missing or unreadable"));
                    ibr.bad(chunkId);
                }
            } else {
                reported.remove(chunkId);
            }
        }
        return corrupt;
    }

    /** After a full report the controller may have lost earlier {@code bad} events: report again once. */
    public void afterFullReport() {
        reported.clear();
    }
}
