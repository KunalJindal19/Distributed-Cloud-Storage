package com.objectstore.worker;

import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.reporting.Lifeline;
import com.objectstore.worker.reporting.Reporter;
import com.objectstore.worker.storage.BlockScanner;
import com.objectstore.worker.storage.DiskCheck;
import com.objectstore.worker.storage.VersionFile;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Starts the worker's background loops once the HTTP server accepts requests: the reporter
 * (which registers first), the heartbeat lifeline, the block scanner and the disk check. A worker
 * whose volume belongs to another worker starts none of them.
 */
@Component
public class BackgroundTasks {

    private static final Logger log = LoggerFactory.getLogger(BackgroundTasks.class);

    private final WorkerProperties properties;
    private final VersionFile version;
    private final Reporter reporter;
    private final Lifeline lifeline;
    private final BlockScanner scanner;
    private final DiskCheck diskCheck;
    private final List<Thread> threads = new ArrayList<>();

    public BackgroundTasks(WorkerProperties properties, VersionFile version, Reporter reporter, Lifeline lifeline,
                           BlockScanner scanner, DiskCheck diskCheck) {
        this.properties = properties;
        this.version = version;
        this.reporter = reporter;
        this.lifeline = lifeline;
        this.scanner = scanner;
        this.diskCheck = diskCheck;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (!properties.backgroundEnabled()) {
            return;
        }
        if (version.isMisconfigured()) {
            log.error("[worker] CRITICAL not joining the cluster: the volume belongs to another worker");
            return;
        }
        threads.add(Thread.ofPlatform().name("reporter").daemon(true).start(reporter));
        threads.add(Thread.ofPlatform().name("lifeline").daemon(true).start(lifeline));
        threads.add(Thread.ofPlatform().name("block-scanner").daemon(true).start(scanner));
        threads.add(Thread.ofPlatform().name("disk-check").daemon(true).start(diskCheck));
    }

    @PreDestroy
    void stop() {
        threads.forEach(Thread::interrupt);
    }
}
