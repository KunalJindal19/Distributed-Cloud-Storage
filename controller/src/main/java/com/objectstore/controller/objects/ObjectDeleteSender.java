package com.objectstore.controller.objects;

import com.objectstore.common.dto.Command;
import com.objectstore.controller.cluster.ClusterState;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.cluster.Task;
import com.objectstore.controller.cluster.WorkerClient;
import com.objectstore.controller.cluster.WorkerException;
import com.objectstore.controller.cluster.Writer;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.observability.MessageTrace;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Delivers the deletes of a client's object delete directly, through {@code DELETE
 * /internal/chunks/{chunk_id}}, instead of waiting for each holder's next heartbeat reply: the
 * counterpart of the upload, which PUTs its chunks directly.
 *
 * <p>Only the delivery is new. The {@code DeleteObject} task creates the versioned DELETE ops and
 * queues their commands exactly as before; this sender then takes them from the queues of the holders
 * that are current ({@link ClusterState#takeDeletes}, the same hand-out rule as a heartbeat reply,
 * which also marks each op SENT and arms its resend deadline) and calls the endpoint with no lock
 * held. The worker runs the same versioned delete as for the command and reports {@code DELETED(v)}
 * or {@code CMD_FAILED} in its change stream, which completes or retries the op as before. So:
 *
 * <ul>
 *   <li>a holder that is resyncing, STALE, DEAD or being purged gets its deletes in heartbeat replies,
 *       as before (they are never taken here);</li>
 *   <li>a call that certainly did not reach the worker (connection refused, the endpoint missing on an
 *       older worker, the worker shutting down) puts the op back at the same version
 *       ({@code PendingOps.undelivered}): the next heartbeat reply carries it;</li>
 *   <li>a call that may have reached it (time-out, lost answer) leaves the op SENT: the resend timer
 *       sends it again at the same version, which the worker treats as a repeat;</li>
 *   <li>after the first such failure the rest of that worker's deletes go in heartbeat replies.</li>
 * </ul>
 *
 * The client's 204 never waits for any of this: it comes as soon as the object is gone from the
 * namespace. {@code objectstore.direct-object-deletes=false} (DIRECT_OBJECT_DELETES) switches it off;
 * the deletes are then delivered in heartbeat replies only.
 */
@Component
public class ObjectDeleteSender implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(ObjectDeleteSender.class);

    /** Commands taken per lock hold: as many as one heartbeat reply carries. */
    static final int BATCH = 50;

    /** The HTTP call (a seam for tests): the worker's status code, or a {@link WorkerException} without one. */
    @FunctionalInterface
    interface DeleteCall {
        int delete(String workerUrl, String chunkId, long version, String cmdId, String reason);
    }

    /** What became of one call. */
    enum Result {
        /** The worker answered and reported the result in its change stream (204, 409 stale, 500). */
        ANSWERED,
        /** It certainly did not act on it: the op goes back for a heartbeat reply. */
        NOT_DELIVERED,
        /** It may have acted on it: the op stays SENT for the resend timer. */
        UNKNOWN
    }

    private record Batch(String url, List<Command> commands) {
    }

    private final ClusterState cluster;
    private final Writer writer;
    private final DeleteCall call;
    private final MessageTrace trace;
    private final Duration taskWait;
    private final boolean enabled;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    @Autowired
    public ObjectDeleteSender(ClusterState cluster, Writer writer, WorkerClient client, MessageTrace trace,
                              ControllerProperties properties,
                              @Value("${objectstore.direct-object-deletes:true}") boolean enabled) {
        this(cluster, writer, client::deleteChunk, trace, properties.uploadCommitWait(), enabled);
    }

    ObjectDeleteSender(ClusterState cluster, Writer writer, DeleteCall call, MessageTrace trace, Duration taskWait,
                       boolean enabled) {
        this.cluster = cluster;
        this.writer = writer;
        this.call = call;
        this.trace = trace;
        this.taskWait = taskWait;
        this.enabled = enabled;
    }

    public boolean enabled() {
        return enabled;
    }

    /**
     * In the background: waits until {@code task} (the object's {@code DeleteObject}) has been applied,
     * then sends its deletes. If the task is not applied within UPLOAD_COMMIT_WAIT nothing is sent here
     * and the heartbeat replies deliver everything, as before.
     */
    public void sendWhenApplied(Task task, String objectId, Collection<String> chunkIds) {
        if (!enabled || chunkIds.isEmpty()) {
            return;
        }
        Set<String> chunks = Set.copyOf(chunkIds);
        executor.execute(() -> {
            try {
                writer.await(task, taskWait);
            } catch (TimeoutException e) {
                log.info("[delete] object={}: its deletes go in heartbeat replies (the delete task was not applied "
                        + "within {} s)", objectId, taskWait.toSeconds());
                return;
            } catch (RuntimeException e) {
                return;                                // the task failed; the reconciler takes over
            }
            send(objectId, chunks);
        });
    }

    /** Sends the queued deletes of these chunks to every current holder, all holders in parallel. Blocks until done. */
    void send(String objectId, Set<String> chunkIds) {
        List<String> workerIds = cluster.read(() -> cluster.workersSorted().stream().map(w -> w.workerId).toList());
        List<Callable<Void>> perWorker = new ArrayList<>();
        for (String workerId : workerIds) {
            perWorker.add(() -> {
                sendTo(objectId, workerId, chunkIds);
                return null;
            });
        }
        try {
            executor.invokeAll(perWorker);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void sendTo(String objectId, String workerId, Set<String> chunkIds) {
        int answered = 0;
        int back = 0;
        while (true) {
            Batch batch = cluster.write(() -> {
                WorkerInfo worker = cluster.worker(workerId);
                List<Command> taken = cluster.takeDeletes(worker, chunkIds, BATCH);
                return taken.isEmpty() ? null : new Batch(worker.url, taken);
            });
            if (batch == null) {
                break;
            }
            int stoppedAt = -1;
            for (int i = 0; i < batch.commands().size(); i++) {
                Result result = deliver(workerId, batch.url(), batch.commands().get(i));
                if (result == Result.ANSWERED) {
                    answered++;
                    continue;
                }
                // Stop using the direct path for this worker: what is left goes in heartbeat replies.
                stoppedAt = result == Result.NOT_DELIVERED ? i : i + 1;
                break;
            }
            if (stoppedAt >= 0) {
                List<Command> rest = batch.commands().subList(stoppedAt, batch.commands().size());
                back += rest.size();
                putBack(workerId, rest);
                break;
            }
        }
        if (answered > 0 || back > 0) {
            log.info("[delete] object={} worker={}: {} deletes sent directly{}", objectId, workerId, answered,
                    back == 0 ? "" : ", " + back + " left to heartbeat replies");
        }
    }

    /** One call, classified. */
    Result deliver(String workerId, String url, Command command) {
        trace.deleteChunk(workerId, command.chunkId(), command.version());
        int status;
        try {
            status = call.delete(url, command.chunkId(), command.version(), command.cmdId(), command.reason());
        } catch (WorkerException e) {
            boolean neverSent = e.neverSent();
            trace.deleteAnswer(workerId, command.chunkId(), (neverSent ? "not delivered: " : "no answer: ")
                    + e.getMessage());
            return neverSent ? Result.NOT_DELIVERED : Result.UNKNOWN;
        }
        trace.deleteAnswer(workerId, command.chunkId(), switch (status) {
            case 204 -> "deleted";
            case 409 -> "stale (the worker reports it; the delete is reissued)";
            default -> "HTTP " + status;
        });
        return switch (status) {
            // Acted on, and reported in the change stream: DELETED(v), CMD_FAILED(stale) or CMD_FAILED(io_error).
            case 204, 409, 500 -> {
                cluster.increment("deletes_sent_direct");
                yield Result.ANSWERED;
            }
            // Not acted on: 400 (rejected), 404/405 (an older worker without the endpoint), 503 (shutting
            // down or stopped). A 503 after a fail-stop at the commit point is ambiguous, but that worker is
            // restarting and resyncs; the same version can only repeat or be STALE.
            default -> Result.NOT_DELIVERED;
        };
    }

    private void putBack(String workerId, List<Command> commands) {
        cluster.write(() -> {
            for (Command command : commands) {
                cluster.ops().undelivered(command.chunkId(), workerId, command.version());
                cluster.increment("deletes_direct_fallback");
            }
        });
    }

    @Override
    public void destroy() {
        executor.shutdownNow();
    }
}
