package com.objectstore.controller.replication;

import com.objectstore.common.Checksums;
import com.objectstore.controller.cluster.WorkerClient;
import com.objectstore.controller.cluster.WorkerException;
import com.objectstore.controller.cluster.WorkerRegistry;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.config.WorkerIo;
import com.objectstore.controller.domain.Chunk;
import com.objectstore.controller.domain.ReplicaLocation;
import com.objectstore.controller.domain.Worker;
import com.objectstore.controller.objects.PlacementPolicy;
import com.objectstore.controller.repository.ChunkRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Keeps every chunk at exactly REPLICATION_FACTOR good copies. Runs every REPAIR_INTERVAL.
 *
 * <p>It does not care <em>why</em> a chunk is short of copies. A dead worker, a corrupt file found
 * by the scrubber, a failed read during a download: all of them simply leave the chunk with fewer
 * good copies than required, and the same code fixes it.
 *
 * <p>One pass does three things, in this order:
 * <ol>
 *   <li><b>purge</b>: remove copies known to be bad (CORRUPT / MISSING) from live workers;</li>
 *   <li><b>repair</b>: for every under-replicated chunk, read a good copy, verify it, and write
 *       it to a live worker that does not hold the chunk yet;</li>
 *   <li><b>trim</b>: for every over-replicated chunk (a worker came back after its chunks had
 *       been re-replicated), remove the surplus.</li>
 * </ol>
 */
@Service
public class RepairService {

    private static final Logger log = LoggerFactory.getLogger(RepairService.class);

    private final ChunkRepository chunks;
    private final WorkerRegistry registry;
    private final PlacementPolicy placement;
    private final WorkerClient workers;
    private final IntegrityReporter integrity;
    private final ReplicaGuard guard;
    private final RepairLog repairLog;
    private final WorkerIo io;
    private final ControllerProperties properties;
    private final Clock clock;

    /** Chunks already reported as blocked, so the same message is not repeated every pass. */
    private final Set<String> reportedUnrecoverable = ConcurrentHashMap.newKeySet();
    private final Set<String> reportedNoTarget = ConcurrentHashMap.newKeySet();

    public RepairService(ChunkRepository chunks, WorkerRegistry registry, PlacementPolicy placement,
                         WorkerClient workers, IntegrityReporter integrity, ReplicaGuard guard, RepairLog repairLog,
                         WorkerIo io, ControllerProperties properties, Clock clock) {
        this.chunks = chunks;
        this.registry = registry;
        this.placement = placement;
        this.workers = workers;
        this.integrity = integrity;
        this.guard = guard;
        this.repairLog = repairLog;
        this.io = io;
        this.properties = properties;
        this.clock = clock;
    }

    /** One pass of the repair loop. */
    public void runOnce() {
        if (registry.aliveWorkers().isEmpty()) {
            return; // nothing to read from and nowhere to write to; try again next pass
        }
        purgeStale();
        repairUnderReplicated();
        trimOverReplicated();
    }

    // ---- 1. purge ---------------------------------------------------------------------------

    /**
     * Removes copy records that are CORRUPT or MISSING on live workers, together with whatever
     * file is still there. After this, "copies of a chunk" and "good copies of a chunk" are the
     * same thing again on all live workers, which keeps the counting trivial.
     *
     * <p>Safety rule: a bad copy is only discarded while the chunk still has at least one good
     * copy to rebuild from. If it has none, the records are kept, because a copy marked MISSING
     * after a transient error may well be intact, and the scrubber will restore it to OK.
     */
    int purgeStale() {
        int purged = 0;
        for (ReplicaLocation stale : chunks.findStaleLocations()) {
            if (guard.call(() -> purge(stale.chunkId(), stale.workerId()))) {
                purged++;
            }
        }
        return purged;
    }

    private boolean purge(String chunkId, String workerId) {
        Optional<ReplicaLocation> current = chunks.findLocation(chunkId, workerId);
        if (current.isEmpty() || !current.get().isStale()) {
            return false;
        }
        boolean hasGoodCopy = chunks.findLocationsByChunk(chunkId).stream().anyMatch(ReplicaLocation::isGood);
        if (!hasGoodCopy) {
            return false;
        }
        ReplicaLocation copy = current.get();
        try {
            workers.deleteChunk(copy.workerUrl(), chunkId);
        } catch (WorkerException e) {
            // The record goes regardless; a file left behind is an orphan for the scrubber.
            log.debug("[repair] could not delete chunk={} on worker={}: {}", chunkId, workerId, e.getMessage());
        }
        chunks.deleteReplica(chunkId, workerId);
        repairLog.record(RepairLog.PURGED, chunkId, workerId, null, "copy was " + copy.status());
        return true;
    }

    // ---- 2. repair --------------------------------------------------------------------------

    void repairUnderReplicated() {
        List<String> underReplicated = chunks.findUnderReplicated(properties.replicationFactor());
        Set<String> current = new HashSet<>(underReplicated);
        reportedUnrecoverable.retainAll(current);
        reportedNoTarget.retainAll(current);
        if (underReplicated.isEmpty()) {
            return;
        }
        Semaphore permits = new Semaphore(properties.repairConcurrency());
        io.mapAll(underReplicated, chunkId -> {
            permits.acquireUninterruptibly();
            try {
                return repairChunk(chunkId);
            } catch (RuntimeException e) {
                log.error("[repair] chunk={} failed unexpectedly", chunkId, e);
                return false;
            } finally {
                permits.release();
            }
        });
    }

    /**
     * Brings one chunk back to REPLICATION_FACTOR good copies if that is possible right now.
     *
     * @return {@code true} if at least one new copy was made
     */
    boolean repairChunk(String chunkId) {
        Optional<Chunk> found = chunks.findChunk(chunkId);
        if (found.isEmpty()) {
            return false; // the object was deleted in the meantime
        }
        Chunk chunk = found.get();
        int replicationFactor = properties.replicationFactor();
        List<ReplicaLocation> good = goodCopies(chunkId);
        if (good.size() >= replicationFactor) {
            return false;
        }
        if (good.isEmpty()) {
            unrecoverable(chunkId, "no good copy on a live worker; waiting for a worker that holds one to return");
            return false;
        }

        // Read from a good copy. The bytes are verified here too: a repair must never spread corruption.
        byte[] data = null;
        String source = null;
        for (ReplicaLocation candidate : placement.readOrder(good, registry.replicaCounts())) {
            Instant observedAt = clock.instant();
            try {
                byte[] fetched = workers.getChunk(candidate.workerUrl(), chunkId).data();
                if (fetched.length == chunk.size() && Checksums.sha256Hex(fetched).equals(chunk.checksum())) {
                    data = fetched;
                    source = candidate.workerId();
                    break;
                }
                integrity.reportCorrupt(candidate, observedAt, "checksum mismatch while reading for repair");
            } catch (WorkerException e) {
                integrity.reportMissing(candidate, observedAt, "read for repair failed: " + e.getMessage());
            }
        }
        if (data == null) {
            unrecoverable(chunkId, "none of the remaining copies could be read intact");
            return false;
        }

        boolean copied = false;
        for (int i = good.size(); i < replicationFactor; i++) {
            if (!copyTo(chunk, data, source)) {
                break;
            }
            copied = true;
        }
        if (copied) {
            reportedUnrecoverable.remove(chunkId);
            reportedNoTarget.remove(chunkId);
        }
        return copied;
    }

    /** Writes one more copy. Everything is re-checked under the guard, so two passes can never both add one. */
    private boolean copyTo(Chunk chunk, byte[] data, String source) {
        String chunkId = chunk.chunkId();
        return guard.call(() -> {
            if (chunks.findChunk(chunkId).isEmpty()) {
                return false;
            }
            List<ReplicaLocation> locations = chunks.findLocationsByChunk(chunkId);
            long goodNow = locations.stream().filter(ReplicaLocation::isGood).count();
            if (goodNow >= properties.replicationFactor()) {
                return false;
            }
            Set<String> holders = locations.stream().map(ReplicaLocation::workerId).collect(Collectors.toSet());
            Optional<Worker> target = placement.chooseRepairTarget(
                    registry.aliveWorkers(), holders, registry.replicaCounts());
            if (target.isEmpty()) {
                if (reportedNoTarget.add(chunkId)) {
                    log.warn("[repair] chunk={} has {} good copies but every live worker already holds a copy; waiting",
                            chunkId, goodNow);
                }
                return false;
            }
            Worker worker = target.get();
            try {
                putReplacingStaleFile(worker, chunk, data);
            } catch (WorkerException e) {
                log.warn("[repair] chunk={} could not be copied to worker={}: {}", chunkId, worker.workerId(), e.getMessage());
                return false;
            }
            chunks.insertReplica(chunkId, worker.workerId(), clock.instant());
            repairLog.record(RepairLog.COPIED, chunkId, source, worker.workerId(),
                    "under-replicated: %d of %d good copies".formatted(goodNow, properties.replicationFactor()));
            return true;
        });
    }

    /**
     * A worker refuses (409) to overwrite an existing file with different content. Since a chunk
     * id is bound to one checksum for ever, such a file can only be a damaged leftover: remove it
     * and write the good bytes.
     */
    private void putReplacingStaleFile(Worker worker, Chunk chunk, byte[] data) {
        try {
            workers.putChunk(worker.url(), chunk.chunkId(), data, chunk.checksum());
        } catch (WorkerException e) {
            if (!e.isConflict()) {
                throw e;
            }
            workers.deleteChunk(worker.url(), chunk.chunkId());
            workers.putChunk(worker.url(), chunk.chunkId(), data, chunk.checksum());
        }
    }

    private void unrecoverable(String chunkId, String reason) {
        if (reportedUnrecoverable.add(chunkId)) {
            repairLog.record(RepairLog.UNRECOVERABLE, chunkId, null, null, reason);
        }
    }

    // ---- 3. trim ----------------------------------------------------------------------------

    void trimOverReplicated() {
        for (String chunkId : chunks.findOverReplicated(properties.replicationFactor())) {
            guard.run(() -> trim(chunkId));
        }
    }

    /**
     * Removes surplus good copies of one chunk until exactly REPLICATION_FACTOR remain.
     *
     * <p>Nothing is trimmed until every worker holding a good copy has been verified by the
     * scrubber since it last came online. Otherwise a freshly made, known-good copy could be
     * deleted in favour of an old one on a returned worker that nobody has checked yet.
     */
    private void trim(String chunkId) {
        List<ReplicaLocation> good = new ArrayList<>(goodCopies(chunkId));
        int replicationFactor = properties.replicationFactor();
        if (good.size() <= replicationFactor) {
            return;
        }
        if (!good.stream().allMatch(copy -> registry.isVerified(copy.workerId()))) {
            log.debug("[repair] chunk={} is over-replicated; waiting for the scrubber to verify its workers", chunkId);
            return;
        }
        Map<String, Integer> replicaCounts = registry.replicaCounts();
        while (good.size() > replicationFactor) {
            ReplicaLocation victim = placement.chooseTrimVictim(good, replicaCounts);
            try {
                workers.deleteChunk(victim.workerUrl(), chunkId);
            } catch (WorkerException e) {
                log.warn("[repair] chunk={} surplus copy on worker={} could not be removed: {}",
                        chunkId, victim.workerId(), e.getMessage());
                return; // keep the record; try again next pass
            }
            chunks.deleteReplica(chunkId, victim.workerId());
            repairLog.record(RepairLog.TRIMMED, chunkId, victim.workerId(), null,
                    "over-replicated: %d of %d good copies".formatted(good.size(), replicationFactor));
            replicaCounts.merge(victim.workerId(), -1, Integer::sum);
            good.remove(victim);
        }
    }

    private List<ReplicaLocation> goodCopies(String chunkId) {
        return chunks.findLocationsByChunk(chunkId).stream().filter(ReplicaLocation::isGood).toList();
    }
}
