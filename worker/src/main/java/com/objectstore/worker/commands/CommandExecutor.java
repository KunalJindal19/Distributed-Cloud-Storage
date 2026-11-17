package com.objectstore.worker.commands;

import com.objectstore.common.Checksums;
import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Command;
import com.objectstore.worker.reporting.ControllerClients;
import com.objectstore.worker.reporting.IbrBuffer;
import com.objectstore.worker.storage.ChunkStore;
import jakarta.annotation.PreDestroy;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;

/**
 * Executes the commands delivered in heartbeat replies.
 *
 * <ul>
 *   <li>{@code REPLICATE}: read the local copy, verify it against the sidecar and the checksum in
 *       the command, and PUT it directly to the target worker (HDFS {@code DNA_TRANSFER}). Bad
 *       bytes are never sent; they are reported instead. Success is not reported here: the
 *       target's own report tells the controller, and a time-out on the controller covers failure.</li>
 *   <li>{@code DELETE}: remove the local copy (HDFS {@code DNA_INVALIDATE}).</li>
 * </ul>
 *
 * At most four commands run at a time. A command id is executed at most once, and a command whose
 * {@code expires_at} has passed is dropped, on arrival and again right before it runs: a DELETE
 * that was delayed for a long time must not run.
 */
@Component
public class CommandExecutor {

    private static final Logger log = LoggerFactory.getLogger(CommandExecutor.class);
    private static final int REMEMBERED_COMMANDS = 1000;
    private static final int MAX_CONCURRENT = 4;

    private final ChunkStore store;
    private final IbrBuffer ibr;
    private final ControllerClients clients;
    private final Semaphore permits = new Semaphore(MAX_CONCURRENT);
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final AtomicInteger activeTransfers = new AtomicInteger();
    private final Map<String, Boolean> seen = new LinkedHashMap<>(256, 0.75f, false) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > REMEMBERED_COMMANDS;
        }
    };

    public CommandExecutor(ChunkStore store, IbrBuffer ibr, ControllerClients clients) {
        this.store = store;
        this.ibr = ibr;
        this.clients = clients;
    }

    public int activeTransfers() {
        return activeTransfers.get();
    }

    /** Accepts a command for execution; returns false if it is a repeat or already expired. */
    public boolean submit(Command command) {
        if (command == null || command.cmdId() == null || command.type() == null) {
            return false;
        }
        if (isExpired(command)) {
            log.info("[cmd] dropped expired {} chunk={} cmd={}", command.type(), command.chunkId(), command.cmdId());
            return false;
        }
        synchronized (seen) {
            if (seen.putIfAbsent(command.cmdId(), Boolean.TRUE) != null) {
                return false;
            }
        }
        executor.execute(() -> run(command));
        return true;
    }

    private void run(Command command) {
        permits.acquireUninterruptibly();
        try {
            if (isExpired(command)) {
                log.info("[cmd] dropped expired {} chunk={} cmd={}", command.type(), command.chunkId(), command.cmdId());
                return;
            }
            switch (command.type()) {
                case InternalApi.COMMAND_REPLICATE -> replicate(command);
                case InternalApi.COMMAND_DELETE -> delete(command);
                default -> log.warn("[cmd] unknown command type {} cmd={}", command.type(), command.cmdId());
            }
        } catch (RuntimeException e) {
            log.error("[cmd] {} chunk={} failed", command.type(), command.chunkId(), e);
        } finally {
            permits.release();
        }
    }

    private void replicate(Command command) {
        String chunkId = command.chunkId();
        Optional<ChunkStore.StoredChunk> local = store.read(chunkId);
        if (local.isEmpty()) {
            log.warn("[cmd] replicate chunk={}: not on this worker; reporting it as deleted", chunkId);
            ibr.deleted(chunkId);
            return;
        }
        ChunkStore.StoredChunk chunk = local.get();
        String hash = Checksums.sha256Hex(chunk.data());
        if (chunk.corrupt() || !hash.equals(command.checksum())) {
            log.warn("[cmd] replicate chunk={}: local copy is corrupt (disk={} expected={}); reporting it, not sending it",
                    chunkId, hash, command.checksum());
            ibr.bad(chunkId);
            return;
        }
        activeTransfers.incrementAndGet();
        long started = System.nanoTime();
        try {
            clients.transfers().put()
                    .uri(command.targetUrl() + InternalApi.CHUNK_PATH, chunkId)
                    .header(InternalApi.CHECKSUM_HEADER, hash)
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .body(chunk.data())
                    .retrieve()
                    .toBodilessEntity();
            log.info("[cmd] replicate ok chunk={} -> {} bytes={} ms={}", chunkId, command.targetUrl(),
                    chunk.data().length, (System.nanoTime() - started) / 1_000_000);
        } catch (RestClientException e) {
            log.warn("[cmd] replicate failed chunk={} -> {}: {}", chunkId, command.targetUrl(), rootMessage(e));
        } finally {
            activeTransfers.decrementAndGet();
        }
    }

    private void delete(Command command) {
        boolean removed = store.delete(command.chunkId());
        log.info("[cmd] delete chunk={} reason={} ({})", command.chunkId(), command.reason(), removed ? "removed" : "was absent");
    }

    private static boolean isExpired(Command command) {
        return command.expiresAt() > 0 && command.expiresAt() < InternalApi.nowSeconds();
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
