package com.objectstore.worker.reporting;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/**
 * What identifies this worker <em>process</em> to the controller: a boot id generated once per
 * start, and two sequence counters (heartbeats and reports have their own), both starting at 0.
 * Together they let the controller recognise messages from an old process and replayed or
 * reordered messages.
 */
@Component
public class WorkerIdentity {

    private final String bootId = UUID.randomUUID().toString();
    private final AtomicLong hbSeq = new AtomicLong(-1);
    private final AtomicLong reportSeq = new AtomicLong(-1);
    private volatile boolean registered;

    public String bootId() {
        return bootId;
    }

    public long nextHeartbeatSeq() {
        return hbSeq.incrementAndGet();
    }

    public long nextReportSeq() {
        return reportSeq.incrementAndGet();
    }

    public long lastHeartbeatSeq() {
        return hbSeq.get();
    }

    public long lastReportSeq() {
        return reportSeq.get();
    }

    /** True while the controller knows this process. Cleared when it answers 404 or stale_boot. */
    public boolean isRegistered() {
        return registered;
    }

    public void setRegistered(boolean registered) {
        this.registered = registered;
    }
}
