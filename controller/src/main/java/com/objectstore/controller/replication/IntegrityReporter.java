package com.objectstore.controller.replication;

import com.objectstore.controller.cluster.WorkerClient;
import com.objectstore.controller.cluster.WorkerException;
import com.objectstore.controller.config.WorkerIo;
import com.objectstore.controller.domain.ReplicaLocation;
import com.objectstore.controller.domain.ReplicaStatus;
import com.objectstore.controller.repository.ChunkRepository;
import java.time.Clock;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Check-on-read: what happens when a copy turns out to be unusable while it is being read (by a
 * download or by the repair loop).
 *
 * <p>The copy is marked CORRUPT or MISSING so that it no longer counts as a good copy, which makes
 * the chunk under-replicated and therefore a job for the repair loop. A corrupt file is also
 * deleted from its worker right away. The bookkeeping runs in the background: the reader moves on
 * to the next copy immediately and is never held up by it.
 */
@Component
public class IntegrityReporter {

    private static final Logger log = LoggerFactory.getLogger(IntegrityReporter.class);

    private final ChunkRepository chunks;
    private final WorkerClient workers;
    private final ReplicaGuard guard;
    private final WorkerIo io;
    private final Clock clock;

    public IntegrityReporter(ChunkRepository chunks, WorkerClient workers, ReplicaGuard guard, WorkerIo io,
                             Clock clock) {
        this.chunks = chunks;
        this.workers = workers;
        this.guard = guard;
        this.io = io;
        this.clock = clock;
    }

    /** The bytes a worker returned do not hash to the chunk's checksum. */
    public void reportCorrupt(ReplicaLocation copy, Instant observedAt, String reason) {
        log.warn("[integrity] chunk={} worker={} -> CORRUPT ({})", copy.chunkId(), copy.workerId(), reason);
        io.execute(() -> downgrade(copy, ReplicaStatus.CORRUPT, observedAt));
    }

    /** A worker that is supposed to hold a copy answered 404, an error, or not at all. */
    public void reportMissing(ReplicaLocation copy, Instant observedAt, String reason) {
        log.warn("[integrity] chunk={} worker={} -> MISSING ({})", copy.chunkId(), copy.workerId(), reason);
        io.execute(() -> downgrade(copy, ReplicaStatus.MISSING, observedAt));
    }

    private void downgrade(ReplicaLocation copy, ReplicaStatus status, Instant observedAt) {
        try {
            guard.run(() -> {
                // Only the record that was actually read from: if the copy was replaced in the
                // meantime (purged and re-created by a repair), the new one is left alone.
                boolean changed = chunks.downgradeIfStillOk(
                        copy.chunkId(), copy.workerId(), status, observedAt, clock.instant());
                if (changed && status == ReplicaStatus.CORRUPT) {
                    try {
                        workers.deleteChunk(copy.workerUrl(), copy.chunkId());
                    } catch (WorkerException e) {
                        // The repair loop deletes it again when it purges the record.
                        log.debug("[integrity] could not delete corrupt chunk={} on worker={}: {}",
                                copy.chunkId(), copy.workerId(), e.getMessage());
                    }
                }
            });
        } catch (RuntimeException e) {
            log.error("[integrity] could not record chunk={} worker={} as {}", copy.chunkId(), copy.workerId(), status, e);
        }
    }
}
