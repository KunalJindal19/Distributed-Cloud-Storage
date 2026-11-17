package com.objectstore.worker.storage;

import com.objectstore.worker.config.WorkerProperties;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * {@code /data/VERSION}, the worker's equivalent of a DataNode's VERSION file: which cluster the
 * data on this volume belongs to, and which worker wrote it.
 *
 * <ul>
 *   <li>A volume whose {@code worker_id} is not ours was mounted into the wrong container. The
 *       worker refuses to register, so nothing can ever delete or report that data.</li>
 *   <li>A {@code cluster_id} that the controller does not recognise means the controller's
 *       database was wiped or belongs to another cluster. The controller refuses the worker
 *       ({@code cluster_id_mismatch}), which protects the data from orphan collection.</li>
 * </ul>
 */
@Component
public class VersionFile {

    private static final Logger log = LoggerFactory.getLogger(VersionFile.class);

    private final Path file;
    private final String workerId;
    private final ObjectMapper json;
    private volatile String clusterId;
    private volatile boolean misconfigured;

    public VersionFile(WorkerProperties properties, ObjectMapper json) {
        this.file = properties.dataDir().resolve("VERSION");
        this.workerId = properties.id();
        this.json = json;
    }

    @PostConstruct
    void load() throws IOException {
        Files.createDirectories(file.getParent());
        if (!Files.exists(file)) {
            return;
        }
        JsonNode node;
        try {
            node = json.readTree(Files.readAllBytes(file));
        } catch (RuntimeException e) {
            log.error("[version] {} is unreadable; treating the volume as new", file, e);
            return;
        }
        String owner = node.path("worker_id").asString(null);
        if (owner != null && !owner.equals(workerId)) {
            misconfigured = true;
            log.error("[version] CRITICAL volume belongs to {} but WORKER_ID is {}; refusing to register "
                    + "so the data stays untouched", owner, workerId);
            return;
        }
        clusterId = node.path("cluster_id").asString(null);
        log.info("[version] volume of {} in cluster {}", workerId, clusterId);
    }

    /** True if the volume belongs to another worker; the worker then only serves {@code /health}. */
    public boolean isMisconfigured() {
        return misconfigured;
    }

    public Optional<String> clusterId() {
        return Optional.ofNullable(clusterId);
    }

    /** Records the cluster id, durably, before the first report is sent. */
    public synchronized void adopt(String newClusterId) {
        if (newClusterId.equals(clusterId)) {
            return;
        }
        ObjectNode node = json.createObjectNode();
        node.put("cluster_id", newClusterId);
        node.put("worker_id", workerId);
        try {
            Path tmp = file.resolveSibling("VERSION." + System.nanoTime() + ".tmp");
            try (FileChannel channel = FileChannel.open(tmp, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                channel.write(java.nio.ByteBuffer.wrap(json.writeValueAsBytes(node)));
                channel.force(true);
            }
            Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            FsyncSupport.fsyncDirectory(file.getParent());
        } catch (IOException e) {
            throw new UncheckedIOException("could not write " + file, e);
        }
        clusterId = newClusterId;
        log.info("[version] adopted cluster {}", newClusterId);
    }
}
