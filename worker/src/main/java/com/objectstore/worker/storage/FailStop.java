package com.objectstore.worker.storage;

import com.objectstore.worker.config.WorkerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * Fail-stop (v5 §4.8). A disk failure at or after a commit point leaves the disk state ambiguous,
 * so the worker must not go on: it sets {@link #failed()} (every mutation and every chunk read is
 * refused with 503 and heartbeats stop), logs an ERROR naming the chunk and the step, and exits
 * with code 70 from a separate thread. Compose's {@code restart: unless-stopped} brings it back;
 * start-up recovery and the registration resync make it consistent again.
 */
@Component
public class FailStop {

    private static final Logger log = LoggerFactory.getLogger(FailStop.class);
    public static final int EXIT_CODE = 70;

    private final boolean exit;
    private final ApplicationContext context;
    private volatile boolean failed;
    private volatile String reason;

    @Autowired
    public FailStop(WorkerProperties properties, ApplicationContext context) {
        this.exit = properties.exitOnFailStop();
        this.context = context;
    }

    /** For tests: never exits the JVM. */
    public FailStop() {
        this.exit = false;
        this.context = null;
    }

    public boolean failed() {
        return failed;
    }

    public String reason() {
        return reason;
    }

    /** Throws 503 once the worker has failed. */
    public void check() {
        if (failed) {
            throw new ChunkStore.UnavailableException("this worker stopped after a disk failure (" + reason + ")");
        }
    }

    /** The disk state is ambiguous after {@code step} for {@code chunkId}: stop everything and exit. */
    public synchronized void trigger(String chunkId, String step, Throwable cause) {
        if (failed) {
            return;
        }
        reason = "chunk " + chunkId + " at step '" + step + "'";
        failed = true;
        log.error("[store] FAIL-STOP chunk={} step='{}': {}; refusing every operation and exiting with code {}",
                chunkId, step, cause == null ? "-" : cause.toString(), EXIT_CODE);
        if (exit) {
            Thread.ofPlatform().name("fail-stop").start(() -> {
                try {
                    if (context != null) {
                        SpringApplication.exit(context, () -> EXIT_CODE);
                    }
                } finally {
                    System.exit(EXIT_CODE);
                }
            });
        }
    }
}
