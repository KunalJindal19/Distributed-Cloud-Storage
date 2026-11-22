package com.objectstore.controller.observability;

import com.objectstore.common.ChunkIds;
import com.objectstore.common.InternalApi;
import com.objectstore.common.dto.Change;
import com.objectstore.common.dto.ChangeType;
import com.objectstore.common.dto.Command;
import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.dto.RegisterRequest;
import com.objectstore.common.dto.RegisterResponse;
import com.objectstore.common.dto.ReportSegment;
import com.objectstore.common.web.ApiException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Prints every message the controller receives from or sends to a worker, one aligned
 * {@code [msg]} line each, for showing the protocol at work (TRACE_MESSAGES):
 *
 * <ul>
 *   <li>{@code off} (default): nothing is printed;</li>
 *   <li>{@code changes}: everything except heartbeats that carry nothing (no change, no report
 *       segment, no command, no flag) and their equally empty replies;</li>
 *   <li>{@code all}: every message, including the plain "I am alive" heartbeats.</li>
 * </ul>
 *
 * <pre>
 * [msg] ← worker2  REGISTER    attempt 1 · boot 5d2e1a90 · change_seq 0 · cluster 1076c9e0  ⇒ accepted, ALIVE
 * [msg] ← worker2  HEARTBEAT   #14 · 2 changes #3–#4: RECEIVED 3f1c9a52/0, RECEIVED 3f1c9a52/1
 * [msg] → worker2  REPLY       #14 · ack #4 · REPLICATE 7a0b11c2/0 → worker4
 * [msg] → worker1  PUT CHUNK   3f1c9a52/2 · 1.0 MB
 * [msg] ← worker1  PUT ANSWER  3f1c9a52/2 stored · 1 change #57: RECEIVED 3f1c9a52/2
 * </pre>
 *
 * Chunk ids are shortened to {@code <first 8 characters of the object id>/<index>}. Lines are
 * printed by the HTTP thread after the cluster lock was released. Heartbeats are counted per
 * worker whatever the mode, for the periodic state print.
 */
@Component
public class MessageTrace {

    private static final Logger log = LoggerFactory.getLogger("trace");
    private static final int MAX_LISTED = 6;

    public enum Mode { OFF, CHANGES, ALL }

    private final Mode mode;
    private final Map<String, AtomicLong> heartbeats = new ConcurrentHashMap<>();

    public MessageTrace(@Value("${objectstore.trace-messages:off}") String mode) {
        this.mode = parse(mode);
    }

    /** No tracing (unit tests). */
    public static final MessageTrace OFF = new MessageTrace("off");

    private static Mode parse(String value) {
        try {
            return Mode.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (RuntimeException e) {
            return Mode.OFF;
        }
    }

    public boolean enabled() {
        return mode != Mode.OFF;
    }

    /** Heartbeats received from each worker since the last call (then reset). */
    public Map<String, Long> takeHeartbeatCounts() {
        Map<String, Long> counts = new TreeMap<>();
        heartbeats.forEach((worker, n) -> counts.put(worker, n.getAndSet(0)));
        return counts;
    }

    // ---- registration and heartbeats -------------------------------------------------------

    public void register(RegisterRequest request, RegisterResponse response, RuntimeException refused) {
        if (!enabled()) {
            return;
        }
        String outcome = refused != null ? "refused: " + reason(refused)
                : "accepted, " + (response == null ? "?" : response.state() + ", fence " + response.floor()
                        + " attempt " + response.attempt());
        log.info("[msg] ← {}  REGISTER    attempt {} · boot {} · change_seq {} · cluster {}  ⇒ {}", pad(request.workerId()),
                request.attempt(), shortBoot(request.bootId()), request.changeSeq(),
                request.clusterId() == null ? "(new volume)" : shortBoot(request.clusterId()), outcome);
    }

    public void heartbeat(HeartbeatRequest request, HeartbeatResponse response, RuntimeException refused) {
        heartbeats.computeIfAbsent(request.workerId(), w -> new AtomicLong()).incrementAndGet();
        if (!enabled()) {
            return;
        }
        List<Change> changes = request.changes() == null ? List.of() : request.changes();
        ReportSegment segment = request.segment();
        boolean empty = changes.isEmpty() && segment == null;
        boolean replyEmpty = response == null || ((response.commands() == null || response.commands().isEmpty())
                && response.resync() == null && response.ignored() == null && response.segmentAck() == null);
        if (mode == Mode.CHANGES && empty && replyEmpty && refused == null) {
            return;
        }
        StringBuilder in = new StringBuilder("#").append(request.seq());
        if (!changes.isEmpty()) {
            in.append(" · ").append(changes(changes));
        }
        if (segment != null) {
            in.append(" · resync listing ").append(shortBoot(segment.reportId())).append(" (fence ").append(segment.floor())
                    .append(" attempt ").append(segment.attempt()).append(") segment ").append(segment.index())
                    .append(segment.last() ? " (last)" : "").append(", ").append(segment.entries() == null ? 0 : segment.entries().size())
                    .append(" chunks, baseline #").append(segment.baseline());
        }
        if (request.resyncWanted() != null) {
            in.append(" · resync wanted (").append(request.resyncWanted()).append(")");
        }
        if (empty) {
            in.append(" · alive");
        }
        log.info("[msg] ← {}  HEARTBEAT   {}", pad(request.workerId()), in);
        if (refused != null) {
            log.info("[msg] → {}  REPLY       #{} · refused: {}", pad(request.workerId()), request.seq(), reason(refused));
            return;
        }
        StringBuilder out = new StringBuilder("#").append(request.seq());
        if (response.ignored() != null) {
            out.append(" · ignored (").append(response.ignored()).append(")");
        }
        out.append(" · ack #").append(response.changesReceivedUpTo());
        if (response.segmentAck() != null) {
            out.append(" · segment ").append(response.segmentAck().index()).append(" ")
                    .append(response.segmentAck().status() == null ? "ok" : response.segmentAck().status());
        }
        if (response.resync() != null) {
            out.append(" · RESYNC fence ").append(response.resync().t0()).append(" attempt ").append(response.resync().attempt());
        }
        List<Command> commands = response.commands() == null ? List.of() : response.commands();
        for (Command command : commands.subList(0, Math.min(commands.size(), MAX_LISTED))) {
            out.append(" · ").append(command(command));
        }
        if (commands.size() > MAX_LISTED) {
            out.append(" · … ").append(commands.size() - MAX_LISTED).append(" more commands");
        }
        log.info("[msg] → {}  REPLY       {}", pad(request.workerId()), out);
    }

    // ---- the data path ------------------------------------------------------------------------

    public void putChunk(String workerId, String chunkId, int size) {
        if (enabled()) {
            log.info("[msg] → {}  PUT CHUNK   {} · {}", pad(workerId), chunk(chunkId), bytes(size));
        }
    }

    /** A direct {@code DELETE /internal/chunks/{chunk_id}} (a client's object delete). */
    public void deleteChunk(String workerId, String chunkId, long version) {
        if (enabled()) {
            log.info("[msg] → {}  DEL CHUNK   {} v{}", pad(workerId), chunk(chunkId), version);
        }
    }

    public void deleteAnswer(String workerId, String chunkId, String outcome) {
        if (enabled()) {
            log.info("[msg] ← {}  DEL ANSWER  {} {}", pad(workerId), chunk(chunkId), outcome);
        }
    }

    public void putAnswer(String workerId, String chunkId, List<Change> piggyback, String failure) {
        if (!enabled()) {
            return;
        }
        if (failure != null) {
            log.info("[msg] ← {}  PUT ANSWER  {} failed: {}", pad(workerId), chunk(chunkId), failure);
            return;
        }
        log.info("[msg] ← {}  PUT ANSWER  {} stored{}", pad(workerId), chunk(chunkId),
                piggyback == null || piggyback.isEmpty() ? "" : " · " + changes(piggyback));
    }

    public void getChunk(String workerId, String chunkId) {
        if (enabled()) {
            log.info("[msg] → {}  GET CHUNK   {}", pad(workerId), chunk(chunkId));
        }
    }

    public void getAnswer(String workerId, String chunkId, String outcome) {
        if (enabled()) {
            log.info("[msg] ← {}  GET ANSWER  {} {}", pad(workerId), chunk(chunkId), outcome);
        }
    }

    // ---- formatting ---------------------------------------------------------------------------

    private static String changes(List<Change> changes) {
        StringBuilder text = new StringBuilder();
        long first = changes.get(0).seq();
        long last = changes.get(changes.size() - 1).seq();
        text.append(changes.size()).append(changes.size() == 1 ? " change #" + first : " changes #" + first + "–#" + last)
                .append(": ");
        List<String> listed = new ArrayList<>();
        for (Change change : changes.subList(0, Math.min(changes.size(), MAX_LISTED))) {
            listed.add(change.type() + " " + chunk(change.chunkId()) + " v" + change.version()
                    + (change.type() == ChangeType.CMD_FAILED ? " (" + change.reason() + ")"
                    : change.type() == ChangeType.DELETED && change.detail() != null
                            && !"deleted".equals(change.detail()) ? " (" + change.detail() + ")"
                    : change.cmdId() != null && change.type() == ChangeType.RECEIVED ? " (verified)" : ""));
        }
        text.append(String.join(", ", listed));
        if (changes.size() > MAX_LISTED) {
            Map<ChangeType, Integer> rest = new EnumMap<>(ChangeType.class);
            for (Change change : changes.subList(MAX_LISTED, changes.size())) {
                rest.merge(change.type(), 1, Integer::sum);
            }
            text.append(", … and ").append(rest);
        }
        return text.toString();
    }

    private static String command(Command command) {
        if (InternalApi.COMMAND_REPLICATE.equals(command.type())) {
            String target = command.targetUrl() == null ? "?" : command.targetUrl().replaceFirst("^https?://", "").replaceFirst(":\\d+$", "");
            return "REPLICATE " + chunk(command.chunkId()) + " v" + command.version() + " → " + target;
        }
        if (InternalApi.COMMAND_VERIFY.equals(command.type())) {
            return "VERIFY " + chunk(command.chunkId());
        }
        return command.type() + " " + chunk(command.chunkId()) + " v" + command.version()
                + (command.reason() == null ? "" : " (" + command.reason() + ")");
    }


    /** {@code <object id prefix>/<index>}: short enough to follow a chunk through the log. */
    public static String chunk(String chunkId) {
        if (!ChunkIds.isValid(chunkId)) {
            return String.valueOf(chunkId);
        }
        return chunkId.substring(0, 8) + "/" + Integer.parseInt(chunkId.substring(chunkId.length() - 5));
    }

    private static String reason(RuntimeException e) {
        if (e instanceof ApiException api) {
            return api.status().value() + " " + api.code();
        }
        return e.getClass().getSimpleName();
    }

    private static String shortBoot(String id) {
        return id == null ? "?" : id.length() > 8 ? id.substring(0, 8) : id;
    }

    private static String pad(String workerId) {
        return String.format("%-8s", workerId);
    }

    public static String bytes(long n) {
        if (n < 1024) {
            return n + " B";
        }
        if (n < 1024 * 1024) {
            return String.format("%.1f KB", n / 1024.0);
        }
        if (n < 1024L * 1024 * 1024) {
            return String.format("%.1f MB", n / (1024.0 * 1024));
        }
        return String.format("%.1f GB", n / (1024.0 * 1024 * 1024));
    }
}
