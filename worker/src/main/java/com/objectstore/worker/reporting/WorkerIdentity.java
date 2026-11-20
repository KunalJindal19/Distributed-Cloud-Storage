package com.objectstore.worker.reporting;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/**
 * What identifies this worker <em>process</em> to the controller: a boot id generated once per
 * start, the heartbeat sequence counter (the first heartbeat is 0) and the registration attempt
 * counter (the first attempt is 1). Together they let the controller recognise messages from an
 * old process, replayed or reordered heartbeats, and a delayed old registration.
 */
@Component
public class WorkerIdentity {

    private final String bootId = UUID.randomUUID().toString();
    private final AtomicLong hbSeq = new AtomicLong(-1);
    private final AtomicLong attempts = new AtomicLong();
    private volatile boolean registered;

    public String bootId() {
        return bootId;
    }

    public long nextHeartbeatSeq() {
        return hbSeq.incrementAndGet();
    }

    public long nextRegistrationAttempt() {
        return attempts.incrementAndGet();
    }

    public long lastHeartbeatSeq() {
        return hbSeq.get();
    }

    public long lastRegistrationAttempt() {
        return attempts.get();
    }

    /** True while the controller knows this process. Cleared when it answers 404 or stale_boot. */
    public boolean isRegistered() {
        return registered;
    }

    public void setRegistered(boolean registered) {
        this.registered = registered;
    }
}
