package com.objectstore.worker.storage;

import com.objectstore.worker.config.WorkerProperties;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * The DataNode block scanner. At most one pass per SCAN_INTERVAL; each pass:
 *
 * <ol>
 *   <li><b>Reconciles</b> the store's index with the chunk directory (names only): a chunk file
 *       copied in by hand is indexed and reported {@code received}; an indexed chunk whose file
 *       was deleted by hand is dropped and reported {@code deleted} ({@link ChunkStore#reconcile()}).</li>
 *   <li><b>Verifies</b> every indexed chunk against its sidecar, reading at most
 *       SCAN_MAX_BYTES_PER_SEC (a token bucket refilled each second). A chunk that fails and is
 *       not already known bad becomes known bad and is reported {@code bad}, exactly once; one
 *       that verifies again (its file was replaced) is no longer known bad.</li>
 * </ol>
 *
 * The controller replaces a bad copy and only afterwards orders its deletion. The scanner itself
 * never deletes anything.
 */
@Component
public class BlockScanner implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(BlockScanner.class);

    private final ChunkStore store;
    private final WorkerProperties properties;

    public BlockScanner(ChunkStore store, com.objectstore.worker.reporting.IbrBuffer ibr, WorkerProperties properties) {
        this.store = store;
        this.properties = properties;
    }

    @Override
    public void run() {
        long nextPass = System.nanoTime() + properties.scanInterval().toNanos();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                long wait = nextPass - System.nanoTime();
                if (wait > 0) {
                    Thread.sleep(wait / 1_000_000, (int) (wait % 1_000_000));
                }
                nextPass = System.nanoTime() + properties.scanInterval().toNanos();
                scanOnce();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                log.error("[scanner] pass failed", e);
            }
        }
    }

    /**
     * One pass: reconciliation, then rate-limited verification of every indexed chunk.
     *
     * @return number of chunks that failed verification in this pass
     */
    public int scanOnce() throws InterruptedException {
        ChunkStore.Reconciled reconciled = store.reconcile();
        if (!reconciled.isEmpty()) {
            log.info("[scanner] reconciled: {} added, {} removed", reconciled.added(), reconciled.removed());
        }
        TokenBucket bucket = new TokenBucket(properties.scanMaxBytesPerSec());
        int failed = 0;
        for (String chunkId : store.chunkIds()) {
            ChunkStore.Entry entry = store.index().get(chunkId);
            if (entry == null) {
                continue; // deleted meanwhile
            }
            bucket.acquire(entry.size());
            Optional<Boolean> ok = store.verify(chunkId);
            if (ok.isPresent() && !ok.get()) {
                ok = store.verifyLocked(chunkId); // not a write in progress?
            }
            if (ok.isEmpty()) {
                continue; // gone: the next reconciliation reports it
            }
            if (!ok.get()) {
                failed++;
                if (store.markBad(chunkId, "scanner")) {
                    log.warn("[scanner] CORRUPT chunk={} (data does not match its sidecar); reported bad", chunkId);
                }
            } else if (store.clearBad(chunkId)) {
                log.info("[scanner] chunk={} verifies again (its file was replaced)", chunkId);
            }
        }
        return failed;
    }
}
