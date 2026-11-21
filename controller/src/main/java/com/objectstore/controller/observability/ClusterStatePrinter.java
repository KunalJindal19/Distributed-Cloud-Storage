package com.objectstore.controller.observability;

import com.objectstore.common.ChunkIds;
import com.objectstore.controller.cluster.ClusterState;
import com.objectstore.controller.cluster.PendingOps;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.domain.StoredObject;
import com.objectstore.controller.repository.ObjectRepository;
import jakarta.annotation.PreDestroy;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Prints the controller's view of the cluster every STATE_PRINT_INTERVAL seconds (0, the default,
 * turns it off), for showing what happens inside while workers leave and return:
 *
 * <pre>
 * [state] ═══ cluster at 10:42:07 ═══ 4/4 workers ALIVE · 3 objects · 7 chunks · 14 copies · every chunk has 2 copies
 * [state]   worker    state   copies  heartbeats  last seen  queued cmds  copying out
 * [state]   worker1   ALIVE        4           5      0.6 s            0            0
 * [state]   placement of the 3 newest objects   ● copy   ✗ corrupt   ○ surplus, being deleted   ⇢ copy on its way
 * [state]   object                  chunk  worker1  worker2  worker3  worker4
 * [state]   report.pdf                 #0     ●        ●
 * [state]                              #1              ●        ●
 * [state]   repairs: 0 chunks short of copies · 0 missing · 0 copies on their way · deletions 0 waiting, 0 queued, 0 sent
 * </pre>
 *
 * "heartbeats" counts those received since the previous print. The object names come from
 * SQLite, read before the cluster lock is taken; the rest is one snapshot under the read lock.
 */
@Component
public class ClusterStatePrinter {

    private static final Logger log = LoggerFactory.getLogger("state");
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final int MAX_OBJECTS = 8;
    private static final int MAX_CHUNKS = 8;

    private final ClusterState cluster;
    private final ObjectRepository objects;
    private final MessageTrace trace;
    private final ControllerProperties properties;
    private final double intervalSeconds;
    private volatile Thread thread;

    public ClusterStatePrinter(ClusterState cluster, ObjectRepository objects, MessageTrace trace,
                               ControllerProperties properties,
                               @Value("${objectstore.state-print-interval-seconds:0}") double intervalSeconds) {
        this.cluster = cluster;
        this.objects = objects;
        this.trace = trace;
        this.properties = properties;
        this.intervalSeconds = intervalSeconds;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (intervalSeconds <= 0 || thread != null) {
            return;
        }
        long millis = Math.round(intervalSeconds * 1000);
        thread = Thread.ofPlatform().name("state-printer").daemon(true).start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(millis);
                    print();
                } catch (InterruptedException e) {
                    return;
                } catch (RuntimeException e) {
                    log.warn("[state] could not print the cluster state: {}", e.toString());
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

    private record WorkerRow(String id, String state, int copies, double lastSeen, int queued, int sending, boolean reported) {
    }

    private record ChunkRow(String label, int index, Map<String, String> marks) {
    }

    void print() {
        // SQLite first, with no lock held: the newest objects and their names.
        List<StoredObject> newest = objects.list(MAX_OBJECTS, 0);
        long objectCount = objects.count();
        Map<String, Long> heartbeats = trace.takeHeartbeatCounts();
        List<String> lines = cluster.read(() -> snapshot(newest, objectCount, heartbeats));
        lines.forEach(line -> log.info("[state] {}", line));
    }

    private List<String> snapshot(List<StoredObject> newest, long objectCount, Map<String, Long> heartbeats) {
        List<String> lines = new ArrayList<>();
        List<WorkerInfo> workers = cluster.workersSorted();
        long alive = workers.stream().filter(w -> w.state == ClusterState.WorkerState.ALIVE).count();
        long[] h = cluster.health();          // missing, under, over, ok, corrupt, excess
        int rf = properties.replicationFactor();
        String health = cluster.isSafeMode() ? "SAFE MODE (waiting for the workers' reports)"
                : h[0] > 0 ? h[0] + " chunks have no live copy"
                : h[1] > 0 ? h[1] + " chunks are short of copies, being repaired"
                : h[2] > 0 ? h[2] + " chunks have a surplus copy, being trimmed"
                : "every chunk has " + rf + " copies";
        lines.add("═══ cluster at " + LocalTime.now().format(TIME) + " ═══ " + alive + "/" + workers.size() + " workers ALIVE · "
                + objectCount + " objects · " + cluster.chunks().size() + " chunks · " + h[3] + " copies · " + health);

        lines.add(String.format("  %-8s  %-5s  %6s  %10s  %9s  %11s  %11s", "worker", "state", "copies", "heartbeats",
                "last seen", "queued cmds", "copying out"));
        for (WorkerInfo w : workers) {
            lines.add(String.format("  %-8s  %-5s  %6d  %10d  %9s  %11d  %11d%s", w.workerId, w.state, cluster.chunksOn(w.workerId).size(),
                    heartbeats.getOrDefault(w.workerId, 0L), String.format("%.1f s", cluster.secondsSince(w.lastSeen)),
                    w.commands.size(), cluster.ops().streamsOf(w.workerId), w.resyncing
                            ? "   (resyncing: fence " + w.fenceFloor + " attempt " + w.attempt + ")"
                            : w.hasReported || w.isDead() ? "" : "   (registered, resync not done yet)"));
        }

        if (!newest.isEmpty()) {
            List<String> ids = workers.stream().map(w -> w.workerId).toList();
            lines.add("  placement of the " + (newest.size() == 1 ? "newest object" : newest.size() + " newest objects")
                    + "   ● copy   ✗ corrupt   ○ surplus, being deleted   ⇢ copy on its way");
            StringBuilder header = new StringBuilder(String.format("  %-22s %6s", "object", "chunk"));
            ids.forEach(id -> header.append(String.format("  %-7s", id)));
            lines.add(header.toString());
            for (StoredObject object : newest) {
                List<ChunkRow> rows = new ArrayList<>();
                int shown = Math.min(object.chunkCount(), MAX_CHUNKS);
                for (int i = 0; i < shown; i++) {
                    String chunkId = ChunkIds.of(object.objectId(), i);
                    Map<String, String> marks = new LinkedHashMap<>();
                    for (String holder : cluster.replicasOf(chunkId).keySet()) {
                        marks.put(holder, switch (cluster.replicaStatus(chunkId, holder)) {
                            case "CORRUPT" -> "✗";
                            case "EXCESS" -> "○";
                            default -> "●";
                        });
                    }
                    for (PendingOps.Op op : cluster.ops().opsFor(chunkId)) {
                        if (op.kind() == PendingOps.Kind.INSERT) {
                            marks.putIfAbsent(op.workerId, "⇢");
                        }
                    }
                    rows.add(new ChunkRow(i == 0 ? truncate(object.name()) : "", i, marks));
                }
                if (object.chunkCount() == 0) {
                    lines.add(String.format("  %-22s %6s  (empty object, no chunks)", truncate(object.name()), "-"));
                }
                for (ChunkRow row : rows) {
                    StringBuilder line = new StringBuilder(String.format("  %-22s %6s", row.label(), "#" + row.index()));
                    ids.forEach(id -> line.append(String.format("  %-7s", "   " + row.marks().getOrDefault(id, " "))));
                    lines.add(line.toString().stripTrailing());
                }
                if (object.chunkCount() > shown) {
                    lines.add(String.format("  %-22s %6s  … %d more chunks", "", "", object.chunkCount() - shown));
                }
            }
        }
        Map<String, Integer> deletes = cluster.ops().deleteCounts();
        lines.add("  repairs: " + h[1] + " chunks short of copies · " + h[0] + " with no live copy · " + cluster.ops().inserts()
                + " copies on their way · surplus " + h[2] + " · deletions " + deletes.get("waiting") + " waiting, "
                + deletes.get("queued") + " queued, " + deletes.get("delivered") + " sent · VERIFYs outstanding "
                + cluster.verifyOutstanding() + " · surplus waiting for VERIFY " + cluster.postponedSize());
        return lines;
    }

    private static String truncate(String name) {
        return name.length() <= 22 ? name : name.substring(0, 21) + "…";
    }
}
