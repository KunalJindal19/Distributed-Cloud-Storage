package com.objectstore.worker.storage;

import com.objectstore.worker.config.WorkerProperties;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Graceful shutdown with one closing flag and one in-flight counter (v5 §4.8). Every mutation (a
 * PUT after its body is read, a DELETE, a quarantine, a VERIFY, a reconcile change) calls
 * {@link #enter()} before its first temporary file or stripe and {@link #exit()} in
 * {@code finally}. {@code enter()} increments first and then checks the flag, so a mutation either
 * sees the flag or is counted before the drain starts: no race.
 *
 * <p>On shutdown (this lifecycle stops first, before the web server) the flag is set, new PUTs
 * and commands are refused, and the stop waits up to SHUTDOWN_DRAIN_TIMEOUT for the counter to
 * reach 0. A mutation that already entered finishes normally, including its clean-up on failure.
 * If the wait times out the process stops anyway: start-up recovery removes leftovers and the
 * registration resync reports the truth.
 */
@Component
public class MutationGuard implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(MutationGuard.class);

    private final long drainMillis;
    private final AtomicInteger inFlight = new AtomicInteger();
    private volatile boolean closing;
    private volatile boolean running;

    @Autowired
    public MutationGuard(WorkerProperties properties) {
        this(properties.shutdownDrainTimeout().toMillis());
    }

    public MutationGuard(long drainMillis) {
        this.drainMillis = drainMillis;
    }

    /** Starts a mutation; false (and nothing counted) if the worker is closing. */
    public boolean enter() {
        inFlight.incrementAndGet();
        if (closing) {
            exit();
            return false;
        }
        return true;
    }

    public void exit() {
        if (inFlight.decrementAndGet() == 0) {
            synchronized (this) {
                notifyAll();
            }
        }
    }

    public boolean isClosing() {
        return closing;
    }

    public int inFlight() {
        return inFlight.get();
    }

    /** Sets the flag and waits for the mutations in progress; true if they all finished in time. */
    public boolean close() {
        closing = true;
        long deadline = System.currentTimeMillis() + drainMillis;
        synchronized (this) {
            while (inFlight.get() > 0) {
                long left = deadline - System.currentTimeMillis();
                if (left <= 0) {
                    log.warn("[store] shutdown: {} mutation(s) still running after {} ms; stopping anyway",
                            inFlight.get(), drainMillis);
                    return false;
                }
                try {
                    wait(left);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return inFlight.get() == 0;
                }
            }
        }
        log.info("[store] shutdown: no mutation in progress; closing");
        return true;
    }

    @Override
    public void start() {
        running = true;
    }

    @Override
    public void stop() {
        close();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Stops first: before the web server's graceful shutdown and before any other lifecycle. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}
