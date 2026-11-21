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
 * on the very first start), increment and commit the controller term (so every version this run
 * issues is above every version of an earlier run, v5 §3.2), mirror the {@code chunks} table into
 * memory, and enter safe mode. Where the copies are is learnt from the workers' resyncs afterwards.
 */
@Component
public class ClusterBootstrap {

    private static final Logger log = LoggerFactory.getLogger(ClusterBootstrap.class);

    private final ClusterState cluster;
    private final ClusterMetaRepository meta;
    private final ChunkRepository chunks;
    private final ObjectRepository objects;
    private final OpVersions versions;

    public ClusterBootstrap(ClusterState cluster, ClusterMetaRepository meta, ChunkRepository chunks,
                            ObjectRepository objects, OpVersions versions) {
        this.cluster = cluster;
        this.meta = meta;
        this.chunks = chunks;
        this.objects = objects;
        this.versions = versions;
    }

    @PostConstruct
    void load() {
        // SQLite first, with no lock held; then one write block for the mirror.
        String clusterId = meta.clusterId();
        long term = meta.nextTerm();                       // committed before any version is issued
        versions.startTerm(term);
        java.util.List<Chunk> rows = chunks.findAll();
        java.util.List<String> failedUploads = objects.uncommittedUploadIds();
        java.util.List<String> deleted = objects.deletedObjectIds();
        cluster.write(() -> {
            cluster.setClusterId(clusterId);
            for (Chunk chunk : rows) {
                cluster.putChunk(chunk.chunkId(), new ClusterState.ChunkInfo(
                        chunk.objectId(), chunk.index(), chunk.size(), chunk.checksum()));
            }
            failedUploads.forEach(id -> cluster.addGarbage(id, PendingOps.Reason.UPLOAD_FAILED));
            deleted.forEach(id -> cluster.addGarbage(id, PendingOps.Reason.OBJECT_DELETED));
            cluster.setSafeMode(true);
            log.info("[safemode] ON: cluster {} term {} with {} chunks; waiting for the workers' resyncs",
                    cluster.clusterId(), term, cluster.chunks().size());
        });
    }
}
