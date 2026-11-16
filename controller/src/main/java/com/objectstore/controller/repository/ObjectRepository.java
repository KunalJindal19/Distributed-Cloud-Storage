package com.objectstore.controller.repository;

import com.objectstore.controller.domain.Chunk;
import com.objectstore.controller.domain.NewReplica;
import com.objectstore.controller.domain.StoredObject;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionOperations;

@Repository
public class ObjectRepository {

    private static final RowMapper<StoredObject> OBJECT = (rs, rowNum) -> new StoredObject(
            rs.getString("object_id"),
            rs.getString("name"),
            rs.getLong("size"),
            rs.getString("content_type"),
            rs.getString("checksum"),
            rs.getInt("chunk_count"),
            Instant.ofEpochMilli(rs.getLong("created_at")),
            Instant.ofEpochMilli(rs.getLong("updated_at")));

    public enum PatchOutcome { UPDATED, NOT_FOUND, TOO_MANY_KEYS }

    private final JdbcClient jdbc;
    private final JdbcOperations batch;
    private final TransactionOperations tx;

    public ObjectRepository(JdbcClient jdbc, JdbcOperations batch, TransactionOperations tx) {
        this.jdbc = jdbc;
        this.batch = batch;
        this.tx = tx;
    }

    /**
     * Records a completely uploaded object in one transaction: the object, its custom metadata,
     * its chunks and where every copy lives. Either all of it becomes visible or none of it.
     */
    public void insert(StoredObject object, Map<String, String> metadata, List<Chunk> chunks,
                       List<NewReplica> replicas) {
        tx.executeWithoutResult(status -> {
            long createdAt = object.createdAt().toEpochMilli();
            jdbc.sql("""
                    INSERT INTO objects (object_id, name, size, content_type, checksum, chunk_count, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """)
                    .params(object.objectId(), object.name(), object.size(), object.contentType(),
                            object.checksum(), object.chunkCount(), createdAt, object.updatedAt().toEpochMilli())
                    .update();
            batch.batchUpdate(
                    "INSERT INTO object_metadata (object_id, meta_key, meta_value) VALUES (?, ?, ?)",
                    metadata.entrySet().stream()
                            .map(e -> new Object[] {object.objectId(), e.getKey(), e.getValue()})
                            .toList());
            batch.batchUpdate(
                    "INSERT INTO chunks (chunk_id, object_id, idx, size, checksum) VALUES (?, ?, ?, ?, ?)",
                    chunks.stream()
                            .map(c -> new Object[] {c.chunkId(), c.objectId(), c.index(), c.size(), c.checksum()})
                            .toList());
            batch.batchUpdate(
                    "INSERT INTO chunk_replicas (chunk_id, worker_id, status, updated_at) VALUES (?, ?, 'OK', ?)",
                    replicas.stream()
                            .map(r -> new Object[] {r.chunkId(), r.workerId(), createdAt})
                            .toList());
        });
    }

    public Optional<StoredObject> findById(String objectId) {
        return jdbc.sql("SELECT * FROM objects WHERE object_id = ?").param(objectId).query(OBJECT).optional();
    }

    public boolean exists(String objectId) {
        return jdbc.sql("SELECT COUNT(*) FROM objects WHERE object_id = ?").param(objectId)
                .query(Long.class).single() > 0;
    }

    public long count() {
        return jdbc.sql("SELECT COUNT(*) FROM objects").query(Long.class).single();
    }

    /** Newest first; the object id breaks ties so paging is stable. */
    public List<StoredObject> list(int limit, long offset) {
        return jdbc.sql("SELECT * FROM objects ORDER BY created_at DESC, object_id ASC LIMIT ? OFFSET ?")
                .params(limit, offset)
                .query(OBJECT)
                .list();
    }

    /** Custom metadata of an object, sorted by key. */
    public Map<String, String> metadataOf(String objectId) {
        Map<String, String> metadata = new LinkedHashMap<>();
        jdbc.sql("SELECT meta_key, meta_value FROM object_metadata WHERE object_id = ? ORDER BY meta_key")
                .param(objectId)
                .query((rs, rowNum) -> metadata.put(rs.getString("meta_key"), rs.getString("meta_value")))
                .list();
        return metadata;
    }

    /**
     * Adds/overwrites {@code upserts} and removes {@code removals} in one transaction, then bumps
     * {@code updated_at}. If the object would end up with more than {@code maxKeys} keys nothing
     * is changed at all.
     */
    public PatchOutcome patchMetadata(String objectId, Map<String, String> upserts, Set<String> removals,
                                      int maxKeys, Instant now) {
        return tx.execute(status -> {
            if (!exists(objectId)) {
                return PatchOutcome.NOT_FOUND;
            }
            for (String key : removals) {
                jdbc.sql("DELETE FROM object_metadata WHERE object_id = ? AND meta_key = ?")
                        .params(objectId, key)
                        .update();
            }
            for (Map.Entry<String, String> entry : upserts.entrySet()) {
                jdbc.sql("""
                        INSERT INTO object_metadata (object_id, meta_key, meta_value) VALUES (?, ?, ?)
                        ON CONFLICT (object_id, meta_key) DO UPDATE SET meta_value = excluded.meta_value
                        """)
                        .params(objectId, entry.getKey(), entry.getValue())
                        .update();
            }
            long keys = jdbc.sql("SELECT COUNT(*) FROM object_metadata WHERE object_id = ?").param(objectId)
                    .query(Long.class).single();
            if (keys > maxKeys) {
                status.setRollbackOnly();
                return PatchOutcome.TOO_MANY_KEYS;
            }
            // Strictly increasing, even for two patches inside the same millisecond.
            jdbc.sql("UPDATE objects SET updated_at = MAX(?, updated_at + 1) WHERE object_id = ?")
                    .params(now.toEpochMilli(), objectId)
                    .update();
            return PatchOutcome.UPDATED;
        });
    }

    /** Deletes the object; its metadata, chunks and copy records go with it (ON DELETE CASCADE). */
    public boolean delete(String objectId) {
        return jdbc.sql("DELETE FROM objects WHERE object_id = ?").param(objectId).update() > 0;
    }
}
