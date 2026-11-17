package com.objectstore.controller.cluster;

import com.objectstore.controller.domain.Chunk;
import com.objectstore.controller.repository.ChunkRepository;
import com.objectstore.controller.repository.ClusterMetaRepository;
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

    public ClusterBootstrap(ClusterState cluster, ClusterMetaRepository meta, ChunkRepository chunks) {
        this.cluster = cluster;
        this.meta = meta;
        this.chunks = chunks;
    }

    @PostConstruct
    void load() {
        cluster.locked(() -> {
            cluster.setClusterId(meta.clusterId());
            for (Chunk chunk : chunks.findAll()) {
                cluster.putChunk(chunk.chunkId(), new ClusterState.ChunkInfo(
                        chunk.objectId(), chunk.index(), chunk.size(), chunk.checksum()));
            }
            cluster.setSafeMode(true);
            log.info("[safemode] ON: cluster {} with {} chunks; waiting for block reports", cluster.clusterId(),
                    cluster.chunks().size());
        });
    }
}
