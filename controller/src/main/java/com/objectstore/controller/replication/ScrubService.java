package com.objectstore.controller.replication;

import com.objectstore.common.ChunkIds;
import com.objectstore.common.dto.ChunkInfo;
import com.objectstore.controller.cluster.WorkerClient;
import com.objectstore.controller.cluster.WorkerException;
import com.objectstore.controller.cluster.WorkerOnlineEvent;
import com.objectstore.controller.cluster.WorkerRegistry;
import com.objectstore.controller.config.WorkerIo;
import com.objectstore.controller.domain.ReplicaStatus;
import com.objectstore.controller.domain.Worker;
import com.objectstore.controller.domain.WorkerReplica;
import com.objectstore.controller.objects.InFlightUploads;
import com.objectstore.controller.repository.ChunkRepository;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

/**
 * The scrubber: finds damage nobody has asked about. Every SCRUB_INTERVAL, and immediately when a
 * worker comes online, it asks each live worker what is <em>really</em> on its disk (ids, sizes
 * and checksums computed from the files) and compares that with the database.
 *
 * <table>
 *   <caption>What the comparison leads to</caption>
 *   <tr><th>Database</th><th>Disk</th><th>Action</th></tr>
 *   <tr><td>copy recorded</td><td>file with the right checksum</td><td>status OK (restores a copy marked bad earlier)</td></tr>
 *   <tr><td>copy recorded</td><td>file with a wrong checksum</td><td>status CORRUPT, file deleted</td></tr>
 *   <tr><td>copy recorded as OK</td><td>no file</td><td>status MISSING</td></tr>
 *   <tr><td>nothing recorded</td><td>file</td><td>orphan: file deleted</td></tr>
 * </table>
 *
 * The scrubber only <em>detects</em>. Making a replacement copy is the repair loop's job.
 */
@Service
public class ScrubService {

    private static final Logger log = LoggerFactory.getLogger(ScrubService.class);

    /** What one pass over one worker found. */
    public record ScrubReport(int checked, int corrupt, int missing, int orphans, int restored) {

        boolean foundSomething() {
            return corrupt + missing + orphans + restored > 0;
        }
    }

    private final ChunkRepository chunks;
    private final WorkerRegistry registry;
    private final WorkerClient workers;
    private final ReplicaGuard guard;
    private final InFlightUploads inFlight;
    private final WorkerIo io;
    private final Clock clock;

    /** One scrub at a time, whether it was started by the timer or by a worker coming online. */
    private final ReentrantLock oneAtATime = new ReentrantLock();

    public ScrubService(ChunkRepository chunks, WorkerRegistry registry, WorkerClient workers, ReplicaGuard guard,
                        InFlightUploads inFlight, WorkerIo io, Clock clock) {
        this.chunks = chunks;
        this.registry = registry;
        this.workers = workers;
        this.guard = guard;
        this.inFlight = inFlight;
        this.io = io;
        this.clock = clock;
    }

    /** One pass of the scrub loop: every live worker in turn. A worker that fails is skipped. */
    public void runOnce() {
        for (Worker worker : registry.aliveWorkers()) {
            scrubQuietly(worker);
        }
    }

    /** A worker has just (re)appeared: check its disk now rather than at the next tick. */
    @EventListener
    public void onWorkerOnline(WorkerOnlineEvent event) {
        io.execute(() -> registry.aliveWorkers().stream()
                .filter(worker -> worker.workerId().equals(event.workerId()))
                .findFirst()
                .ifPresent(this::scrubQuietly));
    }

    private void scrubQuietly(Worker worker) {
        try {
            scrubWorker(worker);
        } catch (WorkerException e) {
            // Whether the worker is dead is the health monitor's call, not ours.
            log.warn("[scrub] worker={} skipped: {}", worker.workerId(), e.getMessage());
        } catch (RuntimeException e) {
            log.error("[scrub] worker={} failed unexpectedly", worker.workerId(), e);
        }
    }

    /**
     * Compares one worker's disk with the database.
     *
     * <p>The worker is listed <em>without</em> holding the replica guard (hashing every file takes
     * a while), so the database may change while the listing is under way. Two snapshots keep the
     * comparison honest: the copies recorded <em>before</em> the listing and those recorded after
     * it. A record is only judged if it is identical in both, i.e. it existed, unchanged, for the
     * whole time the listing took. Anything newer is simply left for the next pass.
     *
     * @throws WorkerException if the worker could not be listed
     */
    public ScrubReport scrubWorker(Worker worker) {
        oneAtATime.lock();
        try {
            String workerId = worker.workerId();
            long epoch = registry.epochOf(workerId);
            Map<String, WorkerReplica> before = byChunkId(chunks.findReplicasOnWorker(workerId));
            Map<String, ChunkInfo> disk = workers.listChunks(worker.url(), workerId);

            ScrubReport report = guard.call(() -> reconcile(worker, before, disk));

            registry.markVerified(workerId, epoch);
            if (report.foundSomething()) {
                log.info("[scrub] worker={} checked={} corrupt={} missing={} orphans={} restored={}", workerId,
                        report.checked(), report.corrupt(), report.missing(), report.orphans(), report.restored());
            } else {
                log.debug("[scrub] worker={} checked={} all intact", workerId, report.checked());
            }
            return report;
        } finally {
            oneAtATime.unlock();
        }
    }

    private ScrubReport reconcile(Worker worker, Map<String, WorkerReplica> before, Map<String, ChunkInfo> disk) {
        String workerId = worker.workerId();
        // Order matters: uploads in flight first, database second (see InFlightUploads).
        Set<String> uploading = inFlight.snapshot();
        Map<String, WorkerReplica> now = byChunkId(chunks.findReplicasOnWorker(workerId));

        int checked = 0;
        int corrupt = 0;
        int missing = 0;
        int orphans = 0;
        int restored = 0;

        for (WorkerReplica record : now.values()) {
            String chunkId = record.chunkId();
            if (!record.equals(before.get(chunkId))) {
                continue; // created or changed while the worker was being listed
            }
            checked++;
            ChunkInfo file = disk.get(chunkId);
            if (file == null) {
                if (record.status() == ReplicaStatus.OK) {
                    chunks.updateStatus(chunkId, workerId, ReplicaStatus.MISSING, clock.instant());
                    missing++;
                    log.warn("[scrub] chunk={} worker={} -> MISSING (no such file on the worker's disk)", chunkId, workerId);
                }
            } else if (file.size() == record.expectedSize() && file.checksum().equals(record.expectedChecksum())) {
                if (record.status() != ReplicaStatus.OK) {
                    chunks.updateStatus(chunkId, workerId, ReplicaStatus.OK, clock.instant());
                    restored++;
                    log.info("[scrub] chunk={} worker={} -> OK (intact file found, was {})", chunkId, workerId, record.status());
                }
            } else {
                if (record.status() != ReplicaStatus.CORRUPT) {
                    chunks.updateStatus(chunkId, workerId, ReplicaStatus.CORRUPT, clock.instant());
                    corrupt++;
                    log.warn("[scrub] chunk={} worker={} -> CORRUPT (checksum on disk {} != recorded {})",
                            chunkId, workerId, file.checksum(), record.expectedChecksum());
                }
                deleteFile(worker, chunkId);
            }
        }

        for (String chunkId : disk.keySet()) {
            if (now.containsKey(chunkId) || uploading.contains(ChunkIds.objectIdOf(chunkId))) {
                continue;
            }
            if (deleteFile(worker, chunkId)) {
                orphans++;
                log.info("[scrub] gc orphan chunk={} worker={}", chunkId, workerId);
            }
        }
        return new ScrubReport(checked, corrupt, missing, orphans, restored);
    }

    private boolean deleteFile(Worker worker, String chunkId) {
        try {
            return workers.deleteChunk(worker.url(), chunkId);
        } catch (WorkerException e) {
            log.debug("[scrub] could not delete chunk={} on worker={}: {}", chunkId, worker.workerId(), e.getMessage());
            return false;
        }
    }

    private static Map<String, WorkerReplica> byChunkId(List<WorkerReplica> records) {
        Map<String, WorkerReplica> byId = new LinkedHashMap<>();
        for (WorkerReplica record : records) {
            byId.put(record.chunkId(), record);
        }
        return byId;
    }
}
