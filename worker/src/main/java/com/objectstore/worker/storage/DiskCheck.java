package com.objectstore.worker.storage;

import com.objectstore.worker.config.WorkerProperties;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * The DataNode disk checker. Every DISK_CHECK_INTERVAL it writes, forces, reads back and deletes
 * {@code /data/.probe}. If that fails the volume is unusable: the process exits, the controller
 * declares the worker DEAD and re-replicates its chunks elsewhere, and Docker restarts the
 * container (which exits again if the disk is still broken).
 */
@Component
public class DiskCheck implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(DiskCheck.class);

    private final Path probe;
    private final WorkerProperties properties;

    public DiskCheck(WorkerProperties properties) {
        this.probe = properties.dataDir().resolve(".probe");
        this.properties = properties;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(properties.diskCheckInterval());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (!probeOnce()) {
                log.error("[diskcheck] CRITICAL volume {} failed the write/read probe; exiting so the "
                        + "controller repairs this worker's chunks elsewhere", properties.dataDir());
                Runtime.getRuntime().halt(1);
            }
        }
    }

    public boolean probeOnce() {
        byte[] payload = ("probe " + System.nanoTime()).getBytes(StandardCharsets.US_ASCII);
        try {
            Files.deleteIfExists(probe);
            FsyncSupport.writeAndForce(probe, payload);
            byte[] back = Files.readAllBytes(probe);
            Files.delete(probe);
            return Arrays.equals(payload, back);
        } catch (IOException e) {
            log.error("[diskcheck] probe failed: {}", e.toString());
            return false;
        }
    }
}
