package com.objectstore.controller.objects;

import com.objectstore.common.Checksums;
import com.objectstore.common.web.ApiException;
import com.objectstore.common.web.ErrorCodes;
import com.objectstore.controller.cluster.ClusterState;
import com.objectstore.controller.cluster.ClusterState.Replica;
import com.objectstore.controller.cluster.ClusterState.ReplicaState;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.WorkerClient;
import com.objectstore.controller.cluster.WorkerClient.FetchedChunk;
import com.objectstore.controller.cluster.WorkerException;
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
 *   <tr><td>409 {@code chunk_corrupt}</td><td>the worker's own check failed: disk corruption</td><td>mark CORRUPT; next</td></tr>
 *   <tr><td>200, wrong length</td><td>truncated transfer: transient</td><td>next candidate</td></tr>
 *   <tr><td>200, worker's checksum differs from ours</td><td>disk corruption</td><td>mark CORRUPT; next</td></tr>
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

    /** A copy to try: the worker, its address and the boot id it had when we chose it. */
    public record Candidate(String workerId, String url, String bootId) {
    }

    /** Classification of one worker answer. */
    public enum Outcome { GOOD, TRANSIENT, CORRUPT, IN_TRANSIT }

    private final ObjectRepository objects;
    private final ChunkRepository chunks;
    private final ClusterState cluster;
    private final WorkerClient workers;

    public DownloadService(ObjectRepository objects, ChunkRepository chunks, ClusterState cluster, WorkerClient workers) {
        this.objects = objects;
        this.chunks = chunks;
        this.cluster = cluster;
        this.workers = workers;
    }

    /**
     * @throws ApiException 404 {@code object_not_found}, or 503 {@code chunk_unavailable} if some
     *                      chunk currently has no readable copy
     */
    public DownloadPlan plan(String objectId) {
        StoredObject object = objects.findById(objectId).orElseThrow(() -> ObjectService.notFound(objectId));
        List<Chunk> chunkRows = chunks.findChunksByObject(objectId);
        cluster.locked(() -> {
            for (Chunk chunk : chunkRows) {
                if (cluster.readable(chunk.chunkId()).isEmpty()) {
                    throw unavailable(chunk.chunkId());
                }
            }
        });
        return new DownloadPlan(object, chunkRows);
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
                        case CORRUPT -> markCorrupt(chunk.chunkId(), candidate, fetch.reason());
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
        FetchedChunk fetched;
        try {
            fetched = workers.getChunk(candidate.url(), chunk.chunkId());
        } catch (WorkerException e) {
            if (e.status() != null && e.status() == 409 && e.getMessage().contains(ErrorCodes.CHUNK_CORRUPT)) {
                return new Fetch(Outcome.CORRUPT, null, "the worker found its copy corrupt");
            }
            return new Fetch(Outcome.TRANSIENT, null, e.getMessage());
        }
        return new Fetch(classify(chunk, fetched), fetched.data(), "checksum on worker differs from the recorded one");
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
        return cluster.locked(() -> {
            List<Candidate> list = new ArrayList<>();
            for (String workerId : cluster.readable(chunkId)) {
                WorkerInfo worker = cluster.worker(workerId);
                list.add(new Candidate(workerId, worker.url, worker.bootId));
            }
            return list;
        });
    }

    /** Marks a copy CORRUPT, if it is still the copy we read (same worker process, still OK). */
    private void markCorrupt(String chunkId, Candidate candidate, String reason) {
        cluster.locked(() -> {
            WorkerInfo worker = cluster.worker(candidate.workerId());
            Replica replica = cluster.replica(chunkId, candidate.workerId());
            if (worker == null || !candidate.bootId().equals(worker.bootId) || replica == null
                    || replica.state != ReplicaState.OK) {
                return;
            }
            replica.state = ReplicaState.CORRUPT;
            log.warn("[integrity] chunk={} worker={} -> CORRUPT ({})", chunkId, candidate.workerId(), reason);
        });
    }

    private void markDataError(String workerId) {
        cluster.locked(() -> {
            WorkerInfo worker = cluster.worker(workerId);
            if (worker != null) {
                worker.lastDataError = cluster.now();
            }
        });
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
