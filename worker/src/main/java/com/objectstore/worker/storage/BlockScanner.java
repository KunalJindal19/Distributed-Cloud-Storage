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
 *       placed by hand is indexed at an odd local version and reported {@code RECEIVED(odd)}; an
 *       indexed chunk whose file was deleted by hand is reported {@code DELETED(odd, missing)}
 *       ({@link ChunkStore#reconcile()}).</li>
 *   <li><b>Verifies</b> every indexed chunk against its sidecar, reading at most
 *       SCAN_MAX_BYTES_PER_SEC (a token bucket refilled each second), outside the stripe. A chunk that
 *       fails is quarantined under its stripe after a re-check ({@code DELETED(odd, corrupt)}); the
 *       controller then repairs it from a good copy.</li>
 * </ol>
 *
 * The scanner never deletes bytes: a quarantined copy is kept for diagnosis.
 */
@Component
public class BlockScanner implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(BlockScanner.class);

    private final ChunkStore store;
    private final WorkerProperties properties;

    public BlockScanner(ChunkStore store, WorkerProperties properties) {
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
            Optional<Boolean> ok = store.scan(chunkId);
            if (ok.isPresent() && !ok.get()) {
                failed++;
                log.warn("[scanner] CORRUPT chunk={} (data does not match its sidecar); quarantined", chunkId);
            }
        }
        return failed;
    }
}
