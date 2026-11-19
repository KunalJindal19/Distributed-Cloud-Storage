package com.objectstore.worker;

import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.reporting.ControllerLink;
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
 * Starts the worker's background loops once the HTTP server accepts requests: the controller
 * link (registration, heartbeats with the change stream, full-report segments), the block scanner
 * and the disk check. A worker whose volume belongs to another worker starts none of them.
 */
@Component
public class BackgroundTasks {

    private static final Logger log = LoggerFactory.getLogger(BackgroundTasks.class);

    private final WorkerProperties properties;
    private final VersionFile version;
    private final ControllerLink link;
    private final BlockScanner scanner;
    private final DiskCheck diskCheck;
    private final List<Thread> threads = new ArrayList<>();

    public BackgroundTasks(WorkerProperties properties, VersionFile version, ControllerLink link,
                           BlockScanner scanner, DiskCheck diskCheck) {
        this.properties = properties;
        this.version = version;
        this.link = link;
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
        threads.add(Thread.ofPlatform().name("link").daemon(true).start(link));
        threads.add(Thread.ofPlatform().name("block-scanner").daemon(true).start(scanner));
        threads.add(Thread.ofPlatform().name("disk-check").daemon(true).start(diskCheck));
    }

    @PreDestroy
    void stop() {
        threads.forEach(Thread::interrupt);
    }
}
