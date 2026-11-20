package com.objectstore.worker.reporting;

import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.Command;
import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.dto.RegisterRequest;
import com.objectstore.common.dto.RegisterResponse;
import com.objectstore.common.dto.ReportEntry;
import com.objectstore.common.dto.ReportSegment;
import com.objectstore.common.dto.SegmentAck;
import com.objectstore.common.web.ErrorCodes;
import com.objectstore.worker.commands.CommandExecutor;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.storage.ChunkStore;
import com.objectstore.worker.storage.VersionFile;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import tools.jackson.databind.ObjectMapper;

/**
 * The worker's one connection to the controller (thread {@code link}; HDFS's
 * {@code BPServiceActor}): registration, heartbeats carrying the change stream, and full-report
 * segments.
 *
 * <pre>
 * loop:
 *   not registered → attempt++, M = changeLog.beginHold(), register(attempt, change_seq = M)
 *        ok → adopt the cluster id durably, ack(M), start a full report with baseline M (the hold is reused)
 *        cluster_id_mismatch / duplicate_worker → endHold, wait 60 s;   other failure → endHold, wait 2 s
 *   change log overflowed → abandon any report in progress, start a new one
 *   report requested (controller flag, or FULL_REPORT_INTERVAL since the last one) and none in progress → start one
 *   heartbeat: changes = suffix(acked, MAX_CHANGES_PER_MESSAGE), plus the current un-acknowledged segment
 *        200 → ack(changes_received_up_to); a matching segment_ack advances the report (the last one ends the hold);
 *              commands go to the executor unless the heartbeat was ignored as a duplicate
 *        404 / stale_boot → not registered (any report in progress is abandoned)
 *        too_old → resent once at once with a fresh sent_at
 *   sleep: HEARTBEAT_MIN_GAP_MS when changes or segments wait, else HEARTBEAT_INTERVAL; a new change
 *          wakes the link early, but never before HEARTBEAT_MIN_GAP_MS
 * </pre>
 *
 * <p><b>Full reports.</b> The entries come from the store's in-memory index plus its known-bad set,
 * sorted by chunk id, in segments of REPORT_SEGMENT_SIZE, each tagged with the report id (a fresh
 * UUID per report), its index, {@code last}, and {@code baseline = M}. An empty store sends one
 * empty segment with {@code last = true}. Segments are stop-and-wait: segment i+1 leaves only after
 * segment i was acknowledged. A request that arrives while a report is in progress is ignored.
 *
 * <p><b>Hold-back.</b> While a report with baseline M is in progress, no change numbered above M
 * leaves the worker, on any channel. {@link ChangeLog#suffix} enforces it, so callers cannot get it
 * wrong. The listing is taken after {@code beginHold}, so it reflects every change up to M.
 *
 * <p>Only this thread touches the link state; the fields below need no locking.
 */
@Component
public class ControllerLink implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ControllerLink.class);
    private static final long DOWN_LOG_PERIOD_NANOS = TimeUnit.SECONDS.toNanos(30);
    static final Duration REGISTER_RETRY = Duration.ofSeconds(2);
    static final Duration REGISTER_REFUSED_RETRY = Duration.ofSeconds(60);

    /** Test hook: a pause before each full-report segment after the first, per worker id. */
    private static final Map<String, Long> SEGMENT_DELAY_FOR_TESTS = new ConcurrentHashMap<>();
    /** Test hook: the fraction of heartbeat replies treated as lost, per worker id. */
    private static final Map<String, Double> LOST_REPLIES_FOR_TESTS = new ConcurrentHashMap<>();

    private final WorkerProperties properties;
    private final WorkerIdentity identity;
    private final ControllerClients clients;
    private final ChunkStore store;
    private final ChangeLog changes;
    private final VersionFile version;
    private final CommandExecutor executor;
    private final Environment environment;
    private final ObjectMapper json;

    // Written only by the link thread; volatile so the state printer can read them.
    private volatile long acked;              // the controller's "received" for this process
    private volatile Report report;           // the full report in progress, or null
    private boolean reportRequested;          // by the controller, and not yet started
    private volatile long lastReportCompletedNanos;
    private volatile long lastSentNanos;
    private long controllerDownSince;
    private long lastDownLog;
    private volatile long reportsCompleted;
    private volatile String reportInProgress;     // the id of the report in progress, for tests and logs

    public ControllerLink(WorkerProperties properties, WorkerIdentity identity, ControllerClients clients,
                          ChunkStore store, ChangeLog changes, VersionFile version, CommandExecutor executor,
                          Environment environment, ObjectMapper json) {
        this.properties = properties;
        this.identity = identity;
        this.clients = clients;
        this.store = store;
        this.changes = changes;
        this.version = version;
        this.executor = executor;
        this.environment = environment;
        this.json = json;
    }

    /** Test hook: wait {@code millis} before sending each segment after the first (0 turns it off). */
    public static void delaySegmentsForTests(String workerId, long millis) {
        if (millis <= 0) {
            SEGMENT_DELAY_FOR_TESTS.remove(workerId);
        } else {
            SEGMENT_DELAY_FOR_TESTS.put(workerId, millis);
        }
    }

    /** Test hook: treat this fraction of heartbeat replies as lost on the way (0 turns it off). */
    public static void loseRepliesForTests(String workerId, double fraction) {
        if (fraction <= 0) {
            LOST_REPLIES_FOR_TESTS.remove(workerId);
        } else {
            LOST_REPLIES_FOR_TESTS.put(workerId, fraction);
        }
    }

    /** Full reports completed by this process (for tests). */
    public long reportsCompleted() {
        return reportsCompleted;
    }

    /** The highest change number the controller has acknowledged (for the state print). */
    public long acknowledgedUpTo() {
        return acked;
    }

    /** Nanoseconds since the last heartbeat was sent, or -1 if none yet. */
    public long sinceLastHeartbeatNanos() {
        long sent = lastSentNanos;
        return sent == 0 ? -1 : System.nanoTime() - sent;
    }

    /** Nanoseconds since the last full report completed, or -1 if none yet. */
    public long sinceLastReportNanos() {
        long done = lastReportCompletedNanos;
        return done == 0 ? -1 : System.nanoTime() - done;
    }

    /** "segment 2 of 5, baseline #14" for the report in progress, or null. */
    public String reportProgress() {
        Report current = report;
        return current == null ? null
                : "report " + current.shortId() + " segment " + (current.index + 1) + " of " + current.segmentCount
                        + ", baseline #" + current.baseline;
    }

    /** The id of the full report in progress, or null (for tests). */
    public String reportInProgress() {
        return reportInProgress;
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    step();
                } catch (RuntimeException e) {
                    log.error("[link] unexpected failure; continuing", e);
                    Thread.sleep(properties.heartbeatInterval());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void step() throws InterruptedException {
        if (!identity.isRegistered()) {
            Duration wait = register();
            if (!identity.isRegistered()) {
                Thread.sleep(wait);
            }
            return;
        }
        if (changes.takeOverflow()) {
            if (report != null) {
                log.warn("[link] change log overflowed during full report {}; abandoning it", report.shortId());
                report = null;
                reportInProgress = null;
                changes.endHold();
            }
            startReport("change log overflow");
        }
        if (report == null && (reportRequested || periodicReportDue())) {
            startReport(reportRequested ? "requested by the controller" : "periodic");
        }
        boolean delivered = heartbeat();
        pause(delivered);
    }

    // ---- registration ------------------------------------------------------------------------

    /** One registration attempt; returns how long to wait before the next one if it failed. */
    private Duration register() {
        long attempt = identity.nextRegistrationAttempt();
        long baseline = changes.beginHold();
        String url = advertiseUrl();
        RegisterRequest request = new RegisterRequest(properties.id(), url, identity.bootId(),
                version.clusterId().orElse(null), attempt, baseline, identity.lastHeartbeatSeq(),
                InternalApi.nowSeconds(), store.stats(executor.activeTransfers()));
        try {
            RegisterResponse response = clients.registration().post()
                    .uri(InternalApi.REGISTER_PATH)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(request)
                    .retrieve()
                    .body(RegisterResponse.class);
            if (response == null || !response.ok()) {
                changes.endHold();
                log.warn("[link] registration attempt {}: unexpected answer {}", attempt, response);
                return REGISTER_RETRY;
            }
            // The cluster id is recorded durably before anything else is sent.
            version.adopt(response.clusterId());
            changes.ack(baseline);
            acked = baseline;
            identity.setRegistered(true);
            controllerReachable();
            log.info("[link] registered as {} url={} boot={} cluster={} state={} attempt={} change_seq={}",
                    properties.id(), url, identity.bootId().substring(0, 8), response.clusterId(), response.state(),
                    attempt, baseline);
            openReport(baseline, "registration");  // reuses the hold taken for the registration
            return Duration.ZERO;
        } catch (HttpClientErrorException e) {
            changes.endHold();
            String code = errorCode(e);
            if (ErrorCodes.CLUSTER_ID_MISMATCH.equals(code) || ErrorCodes.DUPLICATE_WORKER.equals(code)) {
                log.error("[link] CRITICAL controller refused this worker ({}): {}. Keeping all data; "
                        + "retrying every {} s", code, e.getResponseBodyAsString(), REGISTER_REFUSED_RETRY.toSeconds());
                return REGISTER_REFUSED_RETRY;
            }
            log.warn("[link] registration attempt {} refused: {} {}", attempt, e.getStatusCode().value(),
                    e.getResponseBodyAsString());
            return REGISTER_RETRY;
        } catch (RestClientException | UncheckedIOException e) {
            changes.endHold();
            if (attempt == 1 || attempt % 15 == 0) {
                log.warn("[link] registration attempt {} failed: {}", attempt, rootMessage(e));
            }
            return REGISTER_RETRY;
        }
    }

    private void unregister() {
        identity.setRegistered(false);
        if (report != null) {
            log.info("[link] abandoning full report {}", report.shortId());
            report = null;
            reportInProgress = null;
        }
        changes.endHold();
    }

    // ---- full reports ------------------------------------------------------------------------

    private boolean periodicReportDue() {
        return lastReportCompletedNanos != 0
                && System.nanoTime() - lastReportCompletedNanos >= properties.fullReportInterval().toNanos();
    }

    private void startReport(String reason) {
        openReport(changes.beginHold(), reason);
    }

    /** The listing is taken after the hold began, so it reflects every change up to {@code baseline}. */
    private void openReport(long baseline, String reason) {
        List<ReportEntry> entries = store.reportEntries();
        report = new Report(baseline, entries, Math.max(1, properties.reportSegmentSize()));
        reportInProgress = report.id;
        reportRequested = false;
        log.info("[link] full report {} started ({}): {} chunks in {} segments, baseline {}", report.shortId(), reason,
                entries.size(), report.segmentCount, baseline);
    }

    private void segmentAcknowledged(SegmentAck ack) {
        if (report == null || ack == null || !report.id.equals(ack.reportId()) || ack.index() != report.index) {
            return;
        }
        if (report.isLast()) {
            changes.endHold();
            lastReportCompletedNanos = System.nanoTime();
            reportsCompleted++;
            log.info("[link] full report {} done: {} chunks, baseline {}", report.shortId(), report.entries.size(),
                    report.baseline);
            report = null;
            reportInProgress = null;
        } else {
            report.index++;
            long delay = SEGMENT_DELAY_FOR_TESTS.getOrDefault(properties.id(), 0L);
            report.readyAtNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delay);
        }
    }

    // ---- heartbeats --------------------------------------------------------------------------

    /** Sends one heartbeat; true if the controller answered it. */
    private boolean heartbeat() {
        long seq = identity.nextHeartbeatSeq();
        List<Change> items = changes.suffix(acked, properties.maxChangesPerMessage());
        ReportSegment segment = report != null && report.isReady() ? report.current() : null;
        boolean reportWasOpen = report != null;
        for (int tries = 0; tries < 2; tries++) {
            HeartbeatRequest request = new HeartbeatRequest(properties.id(), identity.bootId(), seq,
                    InternalApi.nowSeconds(), store.stats(executor.activeTransfers()), items, segment);
            lastSentNanos = System.nanoTime();
            try {
                HeartbeatResponse response = clients.lifeline().post()
                        .uri(InternalApi.HEARTBEAT_PATH)
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(request)
                        .retrieve()
                        .body(HeartbeatResponse.class);
                controllerReachable();
                double lose = LOST_REPLIES_FOR_TESTS.getOrDefault(properties.id(), 0.0);
                if (lose > 0 && java.util.concurrent.ThreadLocalRandom.current().nextDouble() < lose) {
                    return false;                        // test hook: the reply "never arrived"
                }
                if (response != null) {
                    handle(response, reportWasOpen);
                }
                return true;
            } catch (HttpClientErrorException e) {
                String code = errorCode(e);
                if (e.getStatusCode().value() == 404 || ErrorCodes.STALE_BOOT.equals(code)) {
                    log.warn("[link] controller does not know this process ({}); registering again",
                            code == null ? e.getStatusCode().value() : code);
                    unregister();
                    return true;
                }
                if (ErrorCodes.TOO_OLD.equals(code) && tries == 0) {
                    log.warn("[link] heartbeat seq={} arrived too late; resending it with a fresh timestamp", seq);
                    continue;
                }
                log.warn("[link] heartbeat seq={} rejected: {} {}", seq, e.getStatusCode().value(),
                        e.getResponseBodyAsString());
                return false;
            } catch (RestClientException e) {
                controllerUnreachable(e);
                return false;
            }
        }
        return false;
    }

    private void handle(HeartbeatResponse response, boolean reportWasOpen) {
        long received = response.changesReceivedUpTo();
        if (received <= changes.lastSeq()) {
            changes.ack(received);
            acked = received;
        }
        segmentAcknowledged(response.segmentAck());
        if (response.ignored() == null && response.commands() != null) {
            for (Command command : response.commands()) {
                executor.submit(command);
            }
        }
        if (response.requestFullReport()) {
            if (reportWasOpen) {
                log.debug("[link] report request ignored: a full report is in progress");
            } else {
                reportRequested = true;
            }
        }
    }

    /**
     * Waits for the next heartbeat: never before HEARTBEAT_MIN_GAP_MS; at the latest after
     * HEARTBEAT_INTERVAL; in between as soon as a change or a report segment is waiting.
     */
    private void pause(boolean delivered) throws InterruptedException {
        if (!identity.isRegistered()) {
            return;                                           // register at once
        }
        long fullEnd = lastSentNanos + properties.heartbeatInterval().toNanos();
        if (!delivered) {
            sleepUntil(fullEnd);
            return;
        }
        long minGapEnd = lastSentNanos + properties.heartbeatMinGap().toNanos();
        sleepUntil(minGapEnd);
        long until = fullEnd;
        if (report != null) {
            until = Math.min(until, Math.max(minGapEnd, report.readyAtNanos));
        }
        if (System.nanoTime() < until) {
            changes.awaitSendable(acked, until);
        }
    }

    private static void sleepUntil(long deadlineNanos) throws InterruptedException {
        long left = deadlineNanos - System.nanoTime();
        if (left > 0) {
            TimeUnit.NANOSECONDS.sleep(left);
        }
    }

    // ---- helpers -----------------------------------------------------------------------------

    private void controllerReachable() {
        if (controllerDownSince != 0) {
            log.info("[link] controller reachable again");
            controllerDownSince = 0;
        }
    }

    private void controllerUnreachable(RestClientException e) {
        long now = System.nanoTime();
        if (controllerDownSince == 0 || now - lastDownLog > DOWN_LOG_PERIOD_NANOS) {
            log.warn("[link] controller unreachable ({}); will keep trying", rootMessage(e));
            lastDownLog = now;
        }
        if (controllerDownSince == 0) {
            controllerDownSince = now;
        }
    }

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

    /** A full report in progress: the listing, split into stop-and-wait segments. */
    private static final class Report {
        final String id = UUID.randomUUID().toString();
        final long baseline;
        final List<ReportEntry> entries;
        final int segmentSize;
        final int segmentCount;
        int index;                       // the current, not yet acknowledged, segment
        long readyAtNanos = Long.MIN_VALUE;

        Report(long baseline, List<ReportEntry> entries, int segmentSize) {
            this.baseline = baseline;
            this.entries = entries;
            this.segmentSize = segmentSize;
            this.segmentCount = Math.max(1, (entries.size() + segmentSize - 1) / segmentSize);
        }

        boolean isLast() {
            return index == segmentCount - 1;
        }

        boolean isReady() {
            return readyAtNanos == Long.MIN_VALUE || System.nanoTime() - readyAtNanos >= 0;
        }

        ReportSegment current() {
            int from = Math.min(entries.size(), index * segmentSize);
            int to = Math.min(entries.size(), from + segmentSize);
            return new ReportSegment(id, index, isLast(), baseline, List.copyOf(entries.subList(from, to)));
        }

        String shortId() {
            return id.substring(0, 8);
        }
    }
}
