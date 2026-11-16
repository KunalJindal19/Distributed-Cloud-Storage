package com.objectstore.controller.replication;

import com.objectstore.controller.api.dto.ClusterStatusResponse.RepairEvent;
import com.objectstore.controller.util.Timestamps;
import java.time.Clock;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * The last 100 actions of the repair loop, newest last. Kept in memory only: it is a window for
 * operators (status endpoint, dashboard), not state the system depends on.
 */
@Component
public class RepairLog {

    private static final Logger log = LoggerFactory.getLogger(RepairLog.class);
    private static final int CAPACITY = 100;

    public static final String COPIED = "copied";
    public static final String TRIMMED = "trimmed";
    public static final String PURGED = "purged";
    public static final String UNRECOVERABLE = "unrecoverable";

    private final Deque<RepairEvent> events = new ArrayDeque<>(CAPACITY);
    private final Clock clock;

    public RepairLog(Clock clock) {
        this.clock = clock;
    }

    public void record(String event, String chunkId, String source, String target, String reason) {
        RepairEvent entry = new RepairEvent(Timestamps.format(clock.instant()), event, chunkId, source, target, reason);
        synchronized (events) {
            if (events.size() == CAPACITY) {
                events.removeFirst();
            }
            events.addLast(entry);
        }
        String line = "[repair] {} chunk={} src={} dst={} ({})";
        if (UNRECOVERABLE.equals(event)) {
            log.warn(line, event, chunkId, dash(source), dash(target), reason);
        } else {
            log.info(line, event, chunkId, dash(source), dash(target), reason);
        }
    }

    public List<RepairEvent> snapshot() {
        synchronized (events) {
            return List.copyOf(events);
        }
    }

    private static String dash(String value) {
        return value == null ? "-" : value;
    }
}
