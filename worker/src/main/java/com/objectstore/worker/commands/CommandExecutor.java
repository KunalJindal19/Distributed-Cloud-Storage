package com.objectstore.worker.commands;

import com.objectstore.common.ChunkIds;
import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.common.web.ErrorCodes;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.reporting.ChangeLog;
import com.objectstore.worker.reporting.ControllerClients;
import com.objectstore.worker.storage.ChunkStore;
import com.objectstore.worker.storage.MutationGuard;
import jakarta.annotation.PreDestroy;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;

/**
 * Executes the versioned commands delivered in heartbeat replies (v5 §4.6).
 *
 * <ul>
 *   <li>{@code REPLICATE v} (to the source): the copy is read and hashed without a stripe. Missing
 *       → {@code DELETED(odd, missing)} (if it was indexed) then {@code CMD_FAILED(no_local_copy)};
 *       corrupt or its sidecar ≠ {@code expected_checksum} → quarantine then
 *       {@code CMD_FAILED(local_copy_corrupt)}; otherwise PUT to the target with
 *       {@code X-Op-Version: v}. On 2xx, 409 {@code stale_version} or 409 {@code in_progress}
 *       nothing is logged (the target logs the result); on anything else, unreachable included,
 *       {@code CMD_FAILED(transfer_failed, target)}.</li>
 *   <li>{@code DELETE v}: the store's version-checked delete ({@code corrupt} quarantines).</li>
 *   <li>{@code VERIFY}: hash the copy now; pass → {@code RECEIVED(cmd_id)}, fail → quarantine,
 *       missing → {@code DELETED(odd, missing)}. Never {@code CMD_FAILED}: an unanswered VERIFY
 *       just earns no credit.</li>
 * </ul>
 *
 * At most four commands run at a time and at most EXECUTOR_QUEUE_MAX wait; beyond that REPLICATE
 * and DELETE log {@code CMD_FAILED(busy)} and VERIFY is dropped. A waiting command takes its permit
 * with a timed {@code tryAcquire} loop that gives up when the worker is shutting down. A command id
 * is executed at most once; duplicates are harmless anyway, because versions decide.
 */
@Component
public class CommandExecutor {

    private static final Logger log = LoggerFactory.getLogger(CommandExecutor.class);
    private static final int REMEMBERED_COMMANDS = 1000;
    public static final int MAX_CONCURRENT = 4;

    private final ChunkStore store;
    private final ChangeLog changes;
    private final ControllerClients clients;
    private final MutationGuard guard;
    private final int queueMax;
    private final Semaphore permits = new Semaphore(MAX_CONCURRENT, true);
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicInteger activeTransfers = new AtomicInteger();
    private final AtomicInteger waiting = new AtomicInteger();
    private final Map<String, Boolean> seen = new LinkedHashMap<>(256, 0.75f, false) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > REMEMBERED_COMMANDS;
        }
    };

    @Autowired
    public CommandExecutor(ChunkStore store, ChangeLog changes, ControllerClients clients, MutationGuard guard,
                           WorkerProperties properties) {
        this.store = store;
        this.changes = changes;
        this.clients = clients;
        this.guard = guard;
        this.queueMax = properties.executorQueueMax();
    }

    public int activeTransfers() {
        return activeTransfers.get();
    }

    /** Commands accepted and not started yet. */
    public int waiting() {
        return waiting.get();
    }

    /** Accepts a command for execution; returns false if it was refused, malformed or a repeat. */
    public boolean submit(Command command) {
        if (command == null || command.cmdId() == null) {
            return false;
        }
        String problem = malformed(command);
        if (problem != null) {
            log.error("[cmd] bad command {}: {}", command, problem);
            if (command.chunkId() != null && ChunkIds.isValid(command.chunkId())) {
                changes.cmdFailed(command.chunkId(), command.version(), InternalApi.FAILED_BAD_COMMAND, null,
                        command.cmdId(), problem);
            }
            return false;
        }
        synchronized (seen) {
            if (seen.putIfAbsent(command.cmdId(), Boolean.TRUE) != null) {
                return false;
            }
        }
        if (guard.isClosing()) {
            return false;                       // shutting down: the controller resends after the resync
        }
        if (waiting.incrementAndGet() > queueMax) {
            waiting.decrementAndGet();
            if (InternalApi.COMMAND_VERIFY.equals(command.type())) {
                log.warn("[cmd] executor queue full ({}); VERIFY chunk={} dropped", queueMax, command.chunkId());
            } else {
                changes.cmdFailed(command.chunkId(), command.version(), InternalApi.FAILED_BUSY, null, command.cmdId(),
                        "executor queue full");
            }
            return false;
        }
        executor.execute(() -> run(command));
        return true;
    }

    private static String malformed(Command command) {
        if (command.type() == null) {
            return "no type";
        }
        if (command.chunkId() == null || !ChunkIds.isValid(command.chunkId())) {
            return "invalid chunk id";
        }
        return switch (command.type()) {
            case InternalApi.COMMAND_REPLICATE -> !InternalApi.isControllerVersion(command.version()) ? "bad version"
                    : command.targetUrl() == null || command.targetUrl().isBlank() ? "no target_url"
                    : command.expectedChecksum() == null ? "no expected_checksum" : null;
            case InternalApi.COMMAND_DELETE -> !InternalApi.isControllerVersion(command.version()) ? "bad version" : null;
            case InternalApi.COMMAND_VERIFY -> command.expectedChecksum() == null ? "no expected_checksum" : null;
            default -> "unknown type " + command.type();
        };
    }

    private void run(Command command) {
        boolean started = false;
        try {
            while (!(started = permits.tryAcquire(200, TimeUnit.MILLISECONDS))) {
                if (guard.isClosing()) {
                    return;                                 // gives up: the worker is going away
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } finally {
            waiting.decrementAndGet();
        }
        try {
            switch (command.type()) {
                case InternalApi.COMMAND_REPLICATE -> replicate(command);
                case InternalApi.COMMAND_DELETE -> delete(command);
                case InternalApi.COMMAND_VERIFY -> verify(command);
                default -> log.warn("[cmd] unknown command type {} cmd={}", command.type(), command.cmdId());
            }
        } catch (ChunkStore.UnavailableException e) {
            log.warn("[cmd] {} chunk={} not run: {}", command.type(), command.chunkId(), e.getMessage());
        } catch (RuntimeException e) {
            log.error("[cmd] {} chunk={} failed", command.type(), command.chunkId(), e);
        } finally {
            if (started) {
                permits.release();
            }
        }
    }

    private void replicate(Command command) {
        String chunkId = command.chunkId();
        long version = command.version();
        ChunkStore.LocalCopy local = store.readLocal(chunkId);
        if (local.data() == null) {
            if (local.version() >= 0) {
                store.observeMissing(chunkId, local.version());           // DELETED(odd, missing) first
            }
            log.warn("[cmd] replicate chunk={} v={}: no local copy", chunkId, version);
            changes.cmdFailed(chunkId, version, InternalApi.FAILED_NO_LOCAL_COPY, null, command.cmdId(), null);
            return;
        }
        if (local.version() < 0) {
            changes.cmdFailed(chunkId, version, InternalApi.FAILED_NO_LOCAL_COPY, null, command.cmdId(),
                    "the file is not indexed yet");
            return;
        }
        if (!local.intact() || !command.expectedChecksum().equals(local.sidecarChecksum())) {
            log.warn("[cmd] replicate chunk={} v={}: local copy is corrupt or not the expected one; quarantining it, "
                    + "not sending it", chunkId, version);
            boolean quarantined = store.quarantineIfBad(chunkId, local.version(), command.expectedChecksum());
            changes.cmdFailed(chunkId, version, quarantined ? InternalApi.FAILED_LOCAL_COPY_CORRUPT : InternalApi.FAILED_BUSY,
                    null, command.cmdId(), quarantined ? null : "the copy changed while it was read");
            return;
        }
        activeTransfers.incrementAndGet();
        long startedAt = System.nanoTime();
        try {
            clients.transfers().put()
                    .uri(command.targetUrl() + InternalApi.CHUNK_PATH, chunkId)
                    .header(InternalApi.CHECKSUM_HEADER, command.expectedChecksum())
                    .header(InternalApi.OP_VERSION_HEADER, Long.toString(version))
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .body(local.data())
                    .retrieve()
                    .toBodilessEntity();
            log.info("[cmd] replicate ok chunk={} v={} -> {} bytes={} ms={}", chunkId, version, command.targetUrl(),
                    local.data().length, (System.nanoTime() - startedAt) / 1_000_000);
        } catch (HttpClientErrorException e) {
            String body = e.getResponseBodyAsString();
            if (e.getStatusCode().value() == 409
                    && (body.contains(ErrorCodes.STALE_VERSION) || body.contains(ErrorCodes.IN_PROGRESS))) {
                log.info("[cmd] replicate chunk={} v={} -> {}: target answered {} (it logs the result itself)", chunkId,
                        version, command.targetUrl(), body.contains(ErrorCodes.STALE_VERSION) ? "stale_version" : "in_progress");
                return;
            }
            transferFailed(command, e.getStatusCode().value() + " " + body);
        } catch (RestClientException e) {
            transferFailed(command, rootMessage(e));
        } finally {
            activeTransfers.decrementAndGet();
        }
    }

    private void transferFailed(Command command, String why) {
        log.warn("[cmd] replicate failed chunk={} v={} -> {}: {}", command.chunkId(), command.version(),
                command.targetUrl(), why);
        changes.cmdFailed(command.chunkId(), command.version(), InternalApi.FAILED_TRANSFER, command.targetUrl(),
                command.cmdId(), why);
    }

    private void delete(Command command) {
        boolean corrupt = InternalApi.REASON_CORRUPT.equals(command.reason());
        ChunkStore.DeleteOutcome outcome = store.delete(command.chunkId(), command.version(), corrupt, command.cmdId());
        log.info("[cmd] delete chunk={} v={} reason={} ({})", command.chunkId(), command.version(), command.reason(),
                outcome.name().toLowerCase(java.util.Locale.ROOT));
    }

    private void verify(Command command) {
        ChunkStore.VerifyOutcome outcome = store.verifyCopy(command.chunkId(), command.expectedChecksum(), command.cmdId());
        switch (outcome) {
            case PASS -> log.info("[cmd] verify chunk={} passed", command.chunkId());
            case FAILED -> log.warn("[cmd] verify chunk={} FAILED: quarantined", command.chunkId());
            case MISSING -> log.warn("[cmd] verify chunk={}: no copy here", command.chunkId());
            case UNAVAILABLE -> log.warn("[cmd] verify chunk={} could not run now", command.chunkId());
        }
    }

    private static String rootMessage(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        return root.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }

    @PreDestroy
    void shutdown() {
        executor.shutdownNow();
    }
}
