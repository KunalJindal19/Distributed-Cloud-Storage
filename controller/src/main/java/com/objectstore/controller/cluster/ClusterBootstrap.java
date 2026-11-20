package com.objectstore.controller.cluster;

import com.objectstore.controller.domain.Chunk;
import com.objectstore.controller.repository.ChunkRepository;
import com.objectstore.controller.repository.ClusterMetaRepository;
import com.objectstore.controller.repository.ObjectRepository;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * What happens before the controller serves its first request: load the cluster id (creating one
 * on the very first start), mirror the {@code chunks} table into memory, and enter safe mode.
 * Where the copies are is learnt from the workers' block reports afterwards.
 */
@Component
public class ClusterBootstrap {

    private static final Logger log = LoggerFactory.getLogger(ClusterBootstrap.class);

    private final ClusterState cluster;
    private final ClusterMetaRepository meta;
    private final ChunkRepository chunks;
    private final ObjectRepository objects;

    public ClusterBootstrap(ClusterState cluster, ClusterMetaRepository meta, ChunkRepository chunks,
                            ObjectRepository objects) {
        this.cluster = cluster;
        this.meta = meta;
        this.chunks = chunks;
        this.objects = objects;
    }

    @PostConstruct
    void load() {
        // SQLite first, with no lock held; then one write block for the mirror.
        String clusterId = meta.clusterId();
        java.util.List<Chunk> rows = chunks.findAll();
        java.util.List<String> garbage = objects.garbageObjectIds();
        cluster.write(() -> {
            cluster.setClusterId(clusterId);
            for (Chunk chunk : rows) {
                cluster.putChunk(chunk.chunkId(), new ClusterState.ChunkInfo(
                        chunk.objectId(), chunk.index(), chunk.size(), chunk.checksum()));
            }
            garbage.forEach(cluster::addGarbage);
            cluster.setSafeMode(true);
            log.info("[safemode] ON: cluster {} with {} chunks; waiting for block reports", cluster.clusterId(),
                    cluster.chunks().size());
        });
    }
}
