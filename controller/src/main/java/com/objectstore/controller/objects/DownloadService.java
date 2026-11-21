package com.objectstore.controller.objects;

import com.objectstore.common.Checksums;
import com.objectstore.common.web.ApiException;
import com.objectstore.common.web.ErrorCodes;
import com.objectstore.controller.cluster.ApplyQueue;
import com.objectstore.controller.cluster.ClusterState;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.NamespaceTasks;
import com.objectstore.controller.cluster.WorkerClient;
import com.objectstore.controller.cluster.WorkerClient.FetchedChunk;
import com.objectstore.controller.cluster.WorkerException;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.observability.MessageTrace;
import com.objectstore.controller.domain.Chunk;
import com.objectstore.controller.domain.StoredObject;
import com.objectstore.controller.repository.ChunkRepository;
import com.objectstore.controller.repository.ObjectRepository;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * The read path. The client names an object; where its chunks are is the controller's business.
 *
 * <p>Before the first byte is sent, every chunk must have at least one readable copy, so that
 * the client gets a clean 503 rather than a cut-off body. Then each chunk is fetched with the
 * candidates computed <em>at that moment</em> (a copy may have appeared or disappeared since),
 * and every answer is classified:
 *
 * <table>
 *   <caption>How a worker's answer is classified</caption>
 *   <tr><th>Answer</th><th>Meaning</th><th>Action</th></tr>
 *   <tr><td>connection error, time-out, 5xx, 404</td><td>transient</td><td>note the data error; next candidate</td></tr>
 *   <tr><td>409 {@code chunk_corrupt}</td><td>the worker's own check failed: it quarantined the copy and reports it itself</td><td>next</td></tr>
 *   <tr><td>200, wrong length</td><td>truncated transfer: transient</td><td>next candidate</td></tr>
 *   <tr><td>200, worker's checksum differs from ours</td><td>a copy that does not match the namespace</td><td>{@code ensure(DELETE, CORRUPT)}; next</td></tr>
 *   <tr><td>200, bytes do not hash to the worker's (correct) checksum</td><td>corrupted in transit</td><td>retry the same copy once, then next</td></tr>
 *   <tr><td>200, all checks pass</td><td>good</td><td>use it</td></tr>
 * </table>
 *
 * A copy is never marked corrupt because of a transient error. If every candidate fails, the
 * candidates are recomputed after one second for one more round.
 */
@Service
public class DownloadService {

    private static final Logger log = LoggerFactory.getLogger(DownloadService.class);
    private static final Duration RETRY_ROUND_DELAY = Duration.ofSeconds(1);

    /** Everything needed to stream an object; nothing in it touches the database again. */
    public record DownloadPlan(StoredObject object, List<Chunk> chunks) {
    }

    /** A copy to try: the worker, its address, the epoch it had when we chose it, and the copy's generation. */
    public record Candidate(String workerId, String url, long epoch, long gen) {
    }

    /** Classification of one worker answer. */
    public enum Outcome { GOOD, TRANSIENT, CORRUPT, QUARANTINED, IN_TRANSIT }

    private static final Duration SETTLE_POLL = Duration.ofMillis(100);
    private MessageTrace trace = MessageTrace.OFF;

    /** The message trace (TRACE_MESSAGES); off unless configured. */
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void setTrace(MessageTrace trace) {
        this.trace = trace;
    }

    private final ObjectRepository objects;
    private final ChunkRepository chunks;
    private final ClusterState cluster;
    private final WorkerClient workers;
    private final ApplyQueue queue;
    private final NamespaceTasks namespaceTasks;
    private final ControllerProperties properties;

    public DownloadService(ObjectRepository objects, ChunkRepository chunks, ClusterState cluster, WorkerClient workers,
                           ApplyQueue queue, NamespaceTasks namespaceTasks, ControllerProperties properties) {
        this.objects = objects;
        this.chunks = chunks;
        this.cluster = cluster;
        this.workers = workers;
        this.queue = queue;
        this.namespaceTasks = namespaceTasks;
        this.properties = properties;
    }

    /**
     * Reads the rows from SQLite (no lock), then checks under the read lock that every chunk is
     * known in memory and has a candidate. A chunk that is committed but not yet in memory (its
     * {@code CommitObject} is still queued), or that has no candidate yet, is waited for, polling
     * every 100 ms with no lock held, for at most DOWNLOAD_SETTLE_WAIT (v4 §5.8).
     *
     * @throws ApiException 404 {@code object_not_found} (also if the object was deleted meanwhile),
     *                      or 503 {@code chunk_unavailable} if some chunk still has no readable copy
     */
    public DownloadPlan plan(String objectId) {
        StoredObject object = objects.findById(objectId).orElseThrow(() -> ObjectService.notFound(objectId));
        List<Chunk> chunkRows = chunks.findChunksByObject(objectId);
        long deadline = System.nanoTime() + properties.downloadSettleWait().toNanos();
        while (true) {
            String waiting = cluster.read(() -> {
                for (Chunk chunk : chunkRows) {
                    if (cluster.chunk(chunk.chunkId()) == null || cluster.readCandidates(chunk.chunkId()).isEmpty()) {
                        return chunk.chunkId();
                    }
                }
                return null;
            });
            if (waiting == null) {
                return new DownloadPlan(object, chunkRows);
            }
            if (System.nanoTime() >= deadline) {
                if (!objects.exists(objectId)) {
                    throw ObjectService.notFound(objectId);
                }
                throw unavailable(waiting);
            }
            sleep(SETTLE_POLL);
        }
    }

    /**
     * The bytes of a chunk, guaranteed to match the checksum recorded at upload.
     *
     * @throws ApiException 503 {@code chunk_unavailable} if no copy could be read intact
     */
    public byte[] fetchVerified(Chunk chunk) {
        for (int round = 0; round < 2; round++) {
            if (round > 0) {
                sleep(RETRY_ROUND_DELAY);
            }
            for (Candidate candidate : candidates(chunk.chunkId())) {
                for (int attempt = 0; attempt < 2; attempt++) {
                    Fetch fetch = fetch(candidate, chunk);
                    switch (fetch.outcome()) {
                        case GOOD -> {
                            return fetch.data();
                        }
                        case CORRUPT -> reportCorrupt(chunk.chunkId(), candidate, fetch.reason());
                        case QUARANTINED -> log.warn("[download] chunk={} on {} is quarantined; trying the next copy",
                                chunk.chunkId(), candidate.workerId());
                        case TRANSIENT -> markDataError(candidate.workerId());
                        case IN_TRANSIT -> {
                            log.warn("[download] chunk={} from {} corrupted in transit; {}", chunk.chunkId(),
                                    candidate.workerId(), attempt == 0 ? "retrying" : "trying the next copy");
                            if (attempt == 0) {
                                continue;
                            }
                        }
                    }
                    break;
                }
            }
        }
        throw unavailable(chunk.chunkId());
    }

    /** One worker answer and its classification. */
    record Fetch(Outcome outcome, byte[] data, String reason) {
    }

    private Fetch fetch(Candidate candidate, Chunk chunk) {
        trace.getChunk(candidate.workerId(), chunk.chunkId());
        FetchedChunk fetched;
        try {
            fetched = workers.getChunk(candidate.url(), chunk.chunkId());
        } catch (WorkerException e) {
            if (e.status() != null && e.status() == 409 && e.getMessage().contains(ErrorCodes.CHUNK_CORRUPT)) {
                trace.getAnswer(candidate.workerId(), chunk.chunkId(), "refused: its copy is quarantined");
                return new Fetch(Outcome.QUARANTINED, null, "the worker found its copy corrupt");
            }
            trace.getAnswer(candidate.workerId(), chunk.chunkId(), "failed: " + e.getMessage());
            return new Fetch(Outcome.TRANSIENT, null, e.getMessage());
        }
        Outcome outcome = classify(chunk, fetched);
        trace.getAnswer(candidate.workerId(), chunk.chunkId(), outcome == Outcome.GOOD
                ? MessageTrace.bytes(fetched.data().length) + ", checksum verified" : outcome.name().toLowerCase(java.util.Locale.ROOT));
        return new Fetch(outcome, fetched.data(), "checksum on worker differs from the recorded one");
    }

    /** The decision table for a 200 answer. */
    static Outcome classify(Chunk chunk, FetchedChunk fetched) {
        if (fetched.data().length != chunk.size()) {
            return Outcome.TRANSIENT;
        }
        if (!chunk.checksum().equals(fetched.checksum())) {
            return Outcome.CORRUPT;
        }
        if (!Checksums.sha256Hex(fetched.data()).equals(fetched.checksum())) {
            return Outcome.IN_TRANSIT;
        }
        return Outcome.GOOD;
    }

    private List<Candidate> candidates(String chunkId) {
        return cluster.read(() -> {
            List<Candidate> list = new ArrayList<>();
            for (String workerId : cluster.readCandidates(chunkId)) {
                WorkerInfo worker = cluster.worker(workerId);
                ClusterState.Replica replica = cluster.replica(chunkId, workerId);
                list.add(new Candidate(workerId, worker.url, worker.epoch, replica == null ? -1 : replica.gen));
            }
            return list;
        });
    }

    /**
     * Queues {@code ReportCorrupt}: {@code ensure(DELETE, CORRUPT)} if it is still the same copy (same
     * epoch and generation). The download goes on with the next candidate at once.
     */
    private void reportCorrupt(String chunkId, Candidate candidate, String reason) {
        if (candidate.gen() < 0) {
            return;                       // an UPLOAD op's target: its RECEIVED will tell the controller
        }
        if (!queue.offer(namespaceTasks.reportCorrupt(candidate.workerId(), candidate.epoch(), chunkId, candidate.gen(),
                reason))) {
            cluster.increment("apply_queue_full");
        }
    }

    /** A failed data-path call: a volatile field, written without the lock. */
    private void markDataError(String workerId) {
        WorkerInfo worker = cluster.worker(workerId);
        if (worker != null) {
            worker.lastDataError = System.nanoTime();
        }
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static ApiException unavailable(String chunkId) {
        return ApiException.unavailable(ErrorCodes.CHUNK_UNAVAILABLE,
                "chunk " + chunkId + " has no intact copy on a live worker right now");
    }
}
