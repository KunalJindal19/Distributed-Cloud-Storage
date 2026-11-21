package com.objectstore.controller.objects;

import com.objectstore.controller.api.dto.ChunkResponse;
import com.objectstore.controller.api.dto.LocationResponse;
import com.objectstore.controller.api.dto.ObjectResponse;
import com.objectstore.controller.api.dto.ObjectSummary;
import com.objectstore.controller.cluster.ClusterState;
import com.objectstore.controller.cluster.ClusterState.WorkerInfo;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.domain.Chunk;
import com.objectstore.controller.domain.StoredObject;
import com.objectstore.controller.repository.ChunkRepository;
import com.objectstore.controller.repository.ObjectRepository;
import com.objectstore.controller.util.Timestamps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

/** Builds the JSON views of an object: the namespace from the database, the locations from the block map. */
@Component
public class ObjectViewAssembler {

    private final ObjectRepository objects;
    private final ChunkRepository chunks;
    private final ClusterState cluster;
    private final ControllerProperties properties;

    public ObjectViewAssembler(ObjectRepository objects, ChunkRepository chunks, ClusterState cluster,
                               ControllerProperties properties) {
        this.objects = objects;
        this.chunks = chunks;
        this.cluster = cluster;
        this.properties = properties;
    }

    public ObjectResponse describe(StoredObject object) {
        List<Chunk> chunkRows = chunks.findChunksByObject(object.objectId());
        Map<String, String> metadata = objects.metadataOf(object.objectId());
        return cluster.read(() -> {
            boolean replicationOk = true;
            List<ChunkResponse> views = new ArrayList<>();
            for (Chunk chunk : chunkRows) {
                List<LocationResponse> locations = new ArrayList<>();
                for (String workerId : cluster.replicasOf(chunk.chunkId()).keySet()) {
                    WorkerInfo worker = cluster.worker(workerId);
                    locations.add(new LocationResponse(workerId, cluster.replicaStatus(chunk.chunkId(), workerId),
                            worker == null ? "DEAD" : worker.state.name()));
                }
                replicationOk &= cluster.live(chunk.chunkId()).size() >= properties.replicationFactor();
                views.add(new ChunkResponse(chunk.chunkId(), chunk.index(), chunk.size(), chunk.checksum(), locations));
            }
            return new ObjectResponse(
                    object.objectId(), object.name(), object.size(), object.contentType(), object.checksum(),
                    object.chunkCount(), Timestamps.format(object.createdAt()), Timestamps.format(object.updatedAt()),
                    replicationOk, metadata, views);
        });
    }

    public ObjectSummary summarise(StoredObject object) {
        return new ObjectSummary(object.objectId(), object.name(), object.size(), object.contentType(),
                object.chunkCount(), Timestamps.format(object.createdAt()), Timestamps.format(object.updatedAt()));
    }
}
