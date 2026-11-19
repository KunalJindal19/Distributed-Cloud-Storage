package com.objectstore.worker.reporting;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.BlockReportRequest;
import com.objectstore.common.dto.ChunkInfo;
import com.objectstore.common.dto.IncrementalReportRequest;
import com.objectstore.common.dto.RegisterRequest;
import com.objectstore.common.dto.RegisterResponse;
import com.objectstore.common.dto.ReportResponse;
import com.objectstore.common.web.ErrorCodes;
import com.objectstore.worker.commands.CommandExecutor;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.storage.BlockScanner;
import com.objectstore.worker.storage.ChunkStore;
import com.objectstore.worker.storage.VersionFile;
import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import tools.jackson.databind.ObjectMapper;

/**
 * The ordered, acknowledged report channel to the controller (HDFS block reports and IBRs).
 *
 * <p>One thread sends reports strictly one at a time, stop-and-wait: the next message is built
 * only after the previous one was acknowledged. A message that was not acknowledged is resent
 * unchanged, with the same sequence number, so the controller can recognise a retry after a lost
 * response. Reports from one worker can therefore never be applied out of order or twice.
 *
 * <p>The same thread registers the worker with the controller first, and again whenever the
 * controller signals that it no longer knows this process.
 */
@Component
public class Reporter implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Reporter.class);
    private static final Duration[] BACKOFF = {
            Duration.ofMillis(500), Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(4), Duration.ofSeconds(5)};
    private static final Duration REGISTER_RETRY = Duration.ofSeconds(2);
    private static final Duration REGISTER_REFUSED_RETRY = Duration.ofSeconds(60);

    private final WorkerProperties properties;
    private final WorkerIdentity identity;
    private final ControllerClients clients;
    private final ChunkStore store;
    private final IbrBuffer ibr;
    private final VersionFile version;
    private final BlockScanner scanner;
    private final CommandExecutor executor;
    private final Environment environment;
    private final ObjectMapper json;

    private volatile boolean fullReportRequested = true;
    private long lastFullReportNanos;

    public Reporter(WorkerProperties properties, WorkerIdentity identity, ControllerClients clients, ChunkStore store,
                    IbrBuffer ibr, VersionFile version, BlockScanner scanner, CommandExecutor executor,
                    Environment environment, ObjectMapper json) {
        this.properties = properties;
        this.identity = identity;
        this.clients = clients;
        this.store = store;
        this.ibr = ibr;
        this.version = version;
        this.scanner = scanner;
        this.executor = executor;
        this.environment = environment;
        this.json = json;
    }

    /** Asks for a full report at the next opportunity (after register, on request, on overflow). */
    public void requestFullReport() {
        fullReportRequested = true;
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                if (!identity.isRegistered()) {
                    registerWithRetry();
                    fullReportRequested = true;
                }
                boolean fullDue = fullReportRequested || ibr.takeOverflow()
                        || System.nanoTime() - lastFullReportNanos >= properties.fullReportInterval().toNanos();
                if (fullDue) {
                    sendFullReport();
                } else if (ibr.hasEvents()) {
                    sendIncrementalReport();
                }
                Thread.sleep(properties.ibrInterval());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ---- registration ---------------------------------------------------------------------

    private void registerWithRetry() throws InterruptedException {
        int attempt = 0;
        while (!identity.isRegistered() && !Thread.currentThread().isInterrupted()) {
            attempt++;
            Duration wait = tryRegister(attempt) ? Duration.ZERO : REGISTER_RETRY;
            if (lastRegisterRefused) {
                wait = REGISTER_REFUSED_RETRY;
            }
            if (!identity.isRegistered()) {
                Thread.sleep(wait);
            }
        }
    }

    private volatile boolean lastRegisterRefused;

    /** @return true if registered */
    private boolean tryRegister(int attempt) {
        lastRegisterRefused = false;
        String url = advertiseUrl();
        RegisterRequest request = new RegisterRequest(
                properties.id(), url, identity.bootId(), version.clusterId().orElse(null),
                identity.lastHeartbeatSeq(), identity.lastReportSeq(), InternalApi.nowSeconds(),
                store.stats(executor.activeTransfers()));
        try {
            RegisterResponse response = clients.reports().post()
                    .uri(InternalApi.REGISTER_PATH)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(request)
                    .retrieve()
                    .body(RegisterResponse.class);
            if (response == null || !response.ok()) {
                log.warn("[register] attempt {}: unexpected answer {}", attempt, response);
                return false;
            }
            // The cluster id is recorded durably before any report can be sent.
            version.adopt(response.clusterId());
            identity.setRegistered(true);
            log.info("[register] registered as {} url={} boot={} cluster={} state={}", properties.id(), url,
                    identity.bootId().substring(0, 8), response.clusterId(), response.state());
            return true;
        } catch (HttpClientErrorException e) {
            String code = errorCode(e);
            if (ErrorCodes.CLUSTER_ID_MISMATCH.equals(code) || ErrorCodes.DUPLICATE_WORKER.equals(code)) {
                lastRegisterRefused = true;
                log.error("[register] CRITICAL controller refused this worker ({}): {}. Keeping all data; "
                        + "retrying every {} s", code, e.getResponseBodyAsString(), REGISTER_REFUSED_RETRY.toSeconds());
            } else {
                log.warn("[register] attempt {} refused: {} {}", attempt, e.getStatusCode().value(), e.getResponseBodyAsString());
            }
            return false;
        } catch (RestClientException e) {
            if (attempt == 1 || attempt % 15 == 0) {
                log.warn("[register] attempt {} failed: {}", attempt, rootMessage(e));
            }
            return false;
        }
    }

    // ---- reports ---------------------------------------------------------------------------

    private void sendFullReport() throws InterruptedException {
        BlockReportRequest message;
        // Built atomically: the snapshot, the buffer clear and the sequence number belong together.
        synchronized (ibr) {
            List<ChunkInfo> snapshot = store.list();
            ibr.clear();
            message = new BlockReportRequest(properties.id(), identity.bootId(), identity.nextReportSeq(),
                    InternalApi.nowSeconds(), snapshot);
        }
        fullReportRequested = false;
        boolean acked = sendUntilAcked(InternalApi.BLOCK_REPORT_PATH, message.seq(),
                sentAt -> new BlockReportRequest(message.workerId(), message.bootId(), message.seq(), sentAt, message.chunks()));
        if (acked) {
            lastFullReportNanos = System.nanoTime();
            log.info("[report] full report seq={} chunks={}", message.seq(), message.chunks().size());
        }
    }

    private void sendIncrementalReport() throws InterruptedException {
        IbrBuffer.Batch batch = ibr.drain();
        if (batch.isEmpty()) {
            return;
        }
        long seq = identity.nextReportSeq();
        boolean acked = sendUntilAcked(InternalApi.IBR_PATH, seq,
                sentAt -> new IncrementalReportRequest(properties.id(), identity.bootId(), seq, sentAt,
                        batch.received(), batch.deleted(), batch.bad()));
        if (acked) {
            log.info("[ibr] seq={} received={} deleted={} bad={}", seq,
                    batch.received().size(), batch.deleted().size(), batch.bad().size());
        }
    }

    private interface MessageFactory {
        Object withSentAt(double sentAt);
    }

    /**
     * Sends one message until the controller acknowledges it. The sequence number never changes
     * between attempts; only {@code sent_at} is refreshed.
     *
     * @return true if acknowledged; false if the message was discarded because this process must
     *         register again (a fresh full report follows)
     */
    private boolean sendUntilAcked(String path, long seq, MessageFactory factory) throws InterruptedException {
        int attempt = 0;
        while (identity.isRegistered() && !Thread.currentThread().isInterrupted()) {
            try {
                ReportResponse response = clients.reports().post()
                        .uri(path)
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(factory.withSentAt(InternalApi.nowSeconds()))
                        .retrieve()
                        .body(ReportResponse.class);
                if (response != null && response.ignored() != null) {
                    log.info("[report] seq={} acknowledged as {}", seq, response.ignored());
                }
                return true;
            } catch (HttpClientErrorException e) {
                String code = errorCode(e);
                if (e.getStatusCode().value() == 404 || ErrorCodes.STALE_BOOT.equals(code)) {
                    log.warn("[report] controller does not know this process ({}); registering again", code);
                    identity.setRegistered(false);
                    return false;
                }
                if (ErrorCodes.TOO_OLD.equals(code)) {
                    log.warn("[report] seq={} arrived too late; resending with a fresh timestamp", seq);
                } else if (ErrorCodes.STALE_SEQ.equals(code)) {
                    // Impossible with stop-and-wait; the controller's view is ahead of ours.
                    log.error("[report] seq={} rejected as stale; sending a full report", seq);
                    fullReportRequested = true;
                    return false;
                } else {
                    log.error("[report] seq={} rejected: {} {}", seq, e.getStatusCode().value(), e.getResponseBodyAsString());
                    return false;
                }
            } catch (RestClientException e) {
                if (attempt == 0 || attempt % 10 == 0) {
                    log.warn("[report] seq={} not acknowledged ({}); retrying", seq, rootMessage(e));
                }
            }
            Thread.sleep(BACKOFF[Math.min(attempt, BACKOFF.length - 1)]);
            attempt++;
        }
        return false;
    }

    // ---- helpers ---------------------------------------------------------------------------

    private String advertiseUrl() {
        String configured = properties.advertiseUrl();
        if (configured != null && !configured.isBlank()) {
            return configured.trim();
        }
        String port = environment.getProperty("local.server.port", environment.getProperty("server.port", "8001"));
        return "http://" + properties.id() + ":" + port;
    }

    private String errorCode(HttpClientErrorException e) {
        try {
            return json.readTree(e.getResponseBodyAsString()).path("error").asString(null);
        } catch (RuntimeException notJson) {
            return null;
        }
    }

    static String rootMessage(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        return root.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }
}
