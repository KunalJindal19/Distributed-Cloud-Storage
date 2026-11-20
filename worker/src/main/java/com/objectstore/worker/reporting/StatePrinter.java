package com.objectstore.worker.reporting;

import com.objectstore.common.ChunkIds;
import com.objectstore.worker.commands.CommandExecutor;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.storage.ChunkStore;
import jakarta.annotation.PreDestroy;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Prints this worker's own state every STATE_PRINT_INTERVAL seconds (0, the default, turns it
 * off), for showing what a DataNode holds and how its link to the controller is doing:
 *
 * <pre>
 * [state] ═══ worker2 at 10:42:07 ═══ registered · process 5d2e1a90 · 7 chunks · 6.4 MB · 54.2 GB free
 * [state]   link: heartbeat #812 sent 0.4 s ago · changes acknowledged up to #144 · 0 waiting to be sent
 * [state]   full report: none in progress (last one 2.1 min ago)
 * [state]   chunks by object: 3f1c9a52 #0 #1 · 7a0b11c2 #0 · 9e2f0b13 #0 #1 #2
 * [state]   known bad: none · copies being sent to other workers: 0
 * </pre>
 */
@Component
public class StatePrinter {

    private static final Logger log = LoggerFactory.getLogger("state");
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final int LINE_WIDTH = 110;
    private static final int MAX_OBJECTS = 40;

    private final WorkerProperties properties;
    private final WorkerIdentity identity;
    private final ChunkStore store;
    private final ChangeLog changes;
    private final ControllerLink link;
    private final CommandExecutor executor;
    private final double intervalSeconds;
    private volatile Thread thread;

    public StatePrinter(WorkerProperties properties, WorkerIdentity identity, ChunkStore store, ChangeLog changes,
                        ControllerLink link, CommandExecutor executor,
                        @Value("${worker.state-print-interval-seconds:0}") double intervalSeconds) {
        this.properties = properties;
        this.identity = identity;
        this.store = store;
        this.changes = changes;
        this.link = link;
        this.executor = executor;
        this.intervalSeconds = intervalSeconds;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (intervalSeconds <= 0 || !properties.backgroundEnabled() || thread != null) {
            return;
        }
        long millis = Math.round(intervalSeconds * 1000);
        thread = Thread.ofPlatform().name("state-printer").daemon(true).start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(millis);
                    lines().forEach(line -> log.info("[state] {}", line));
                } catch (InterruptedException e) {
                    return;
                } catch (RuntimeException e) {
                    log.warn("[state] could not print the worker state: {}", e.toString());
                }
            }
        });
    }

    @PreDestroy
    void stop() {
        if (thread != null) {
            thread.interrupt();
        }
    }

    List<String> lines() {
        List<String> lines = new ArrayList<>();
        Map<String, ChunkStore.Entry> index = store.index();
        var stats = store.stats(executor.activeTransfers());
        lines.add("═══ " + properties.id() + " at " + LocalTime.now().format(TIME) + " ═══ "
                + (identity.isRegistered() ? "registered" : "NOT registered (looking for the controller)")
                + " · process " + identity.bootId().substring(0, 8) + " · " + stats.chunks() + " chunks · "
                + bytes(stats.usedBytes()) + " · " + bytes(stats.freeBytes()) + " free");

        long since = link.sinceLastHeartbeatNanos();
        lines.add("  link: " + (since < 0 ? "no heartbeat sent yet" : "heartbeat #" + identity.lastHeartbeatSeq() + " sent "
                + seconds(since) + " ago") + " · changes acknowledged up to #" + link.acknowledgedUpTo() + " (last change #"
                + changes.lastSeq() + ") · " + changes.size() + " waiting to be acknowledged");
        String report = link.reportProgress();
        long lastReport = link.sinceLastReportNanos();
        lines.add("  full report: " + (report != null ? "in progress, " + report
                : "none in progress" + (lastReport < 0 ? "" : " (last one " + seconds(lastReport) + " ago)")));

        // chunk ids are <object id>-<index>: group them by object
        Map<String, List<Integer>> byObject = new TreeMap<>();
        for (String chunkId : index.keySet()) {
            if (!ChunkIds.isValid(chunkId)) {
                continue;
            }
            byObject.computeIfAbsent(chunkId.substring(0, 36), id -> new ArrayList<>())
                    .add(Integer.parseInt(chunkId.substring(37)));
        }
        List<String> groups = new ArrayList<>();
        int listed = 0;
        for (Map.Entry<String, List<Integer>> object : byObject.entrySet()) {
            if (listed++ == MAX_OBJECTS) {
                groups.add("… and " + (byObject.size() - MAX_OBJECTS) + " more objects");
                break;
            }
            StringBuilder group = new StringBuilder(object.getKey().substring(0, 8));
            object.getValue().stream().sorted().forEach(i -> group.append(" #").append(i));
            groups.add(group.toString());
        }
        if (groups.isEmpty()) {
            lines.add("  chunks by object: none");
        } else {
            StringBuilder line = new StringBuilder("  chunks by object: ");
            String indent = " ".repeat(line.length());
            for (int i = 0; i < groups.size(); i++) {
                String piece = (i == 0 || line.toString().isBlank() ? "" : " · ") + groups.get(i);
                if (line.length() + piece.length() > LINE_WIDTH && !line.toString().isBlank()) {
                    lines.add(line.toString());
                    line = new StringBuilder(indent);
                    piece = groups.get(i);
                }
                line.append(piece);
            }
            lines.add(line.toString());
        }

        List<String> bad = index.keySet().stream().filter(ChunkIds::isValid).filter(store::isKnownBad)
                .map(c -> c.substring(0, 8) + " #" + Integer.parseInt(c.substring(37))).sorted().toList();
        lines.add("  known bad: " + (bad.isEmpty() ? "none" : String.join(", ", bad)) + " · copies being sent to other workers: "
                + executor.activeTransfers());
        return lines;
    }

    private static String seconds(long nanos) {
        double s = nanos / 1e9;
        return s < 120 ? String.format("%.1f s", s) : String.format("%.1f min", s / 60);
    }

    private static String bytes(long n) {
        if (n < 1024 * 1024) {
            return String.format("%.1f KB", n / 1024.0);
        }
        if (n < 1024L * 1024 * 1024) {
            return String.format("%.1f MB", n / (1024.0 * 1024));
        }
        return String.format("%.1f GB", n / (1024.0 * 1024 * 1024));
    }
}
