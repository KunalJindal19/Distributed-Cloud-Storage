package com.objectstore.controller.objects;

import com.objectstore.controller.api.dto.ChunkResponse;
import com.objectstore.controller.api.dto.LocationResponse;
import com.objectstore.controller.api.dto.ObjectResponse;
import com.objectstore.controller.api.dto.ObjectSummary;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.domain.Chunk;
import com.objectstore.controller.domain.ReplicaLocation;
import com.objectstore.controller.domain.StoredObject;
import com.objectstore.controller.repository.ChunkRepository;
import com.objectstore.controller.repository.ObjectRepository;
import com.objectstore.controller.util.Timestamps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

/** Builds the JSON views of an object from the metadata database. Never contacts a worker. */
@Component
public class ObjectViewAssembler {

    private final ObjectRepository objects;
    private final ChunkRepository chunks;
    private final ControllerProperties properties;

    public ObjectViewAssembler(ObjectRepository objects, ChunkRepository chunks, ControllerProperties properties) {
        this.objects = objects;
        this.chunks = chunks;
        this.properties = properties;
    }

    public ObjectResponse describe(StoredObject object) {
        Map<String, List<ReplicaLocation>> locationsByChunk = chunks.findLocationsByObject(object.objectId()).stream()
                .collect(Collectors.groupingBy(ReplicaLocation::chunkId));

        boolean replicationOk = true;
        List<ChunkResponse> chunkViews = new ArrayList<>();
        for (Chunk chunk : chunks.findChunksByObject(object.objectId())) {
            List<ReplicaLocation> locations = locationsByChunk.getOrDefault(chunk.chunkId(), List.of());
            long goodCopies = locations.stream().filter(ReplicaLocation::isGood).count();
            replicationOk &= goodCopies >= properties.replicationFactor();
            chunkViews.add(new ChunkResponse(
                    chunk.chunkId(),
                    chunk.index(),
                    chunk.size(),
                    chunk.checksum(),
                    locations.stream()
                            .map(l -> new LocationResponse(l.workerId(), l.status().name(), l.workerStatus().name()))
                            .toList()));
        }
        return new ObjectResponse(
                object.objectId(),
                object.name(),
                object.size(),
                object.contentType(),
                object.checksum(),
                object.chunkCount(),
                Timestamps.format(object.createdAt()),
                Timestamps.format(object.updatedAt()),
                replicationOk,
                objects.metadataOf(object.objectId()),
                chunkViews);
    }

    public ObjectSummary summarise(StoredObject object) {
        return new ObjectSummary(
                object.objectId(),
                object.name(),
                object.size(),
                object.contentType(),
                object.chunkCount(),
                Timestamps.format(object.createdAt()),
                Timestamps.format(object.updatedAt()));
    }
}
