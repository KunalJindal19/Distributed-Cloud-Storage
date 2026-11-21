package com.objectstore.controller.repository;

import com.objectstore.controller.domain.Chunk;
import com.objectstore.controller.domain.StoredObject;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionOperations;

/** The namespace: {@code objects}, {@code object_metadata} and {@code idempotency_keys}. */
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

    /** The object an Idempotency-Key was first used for, and the size that was uploaded with it. */
    public record IdempotentUpload(String key, String objectId, long size) {
    }

    /** Thrown when an insert loses the race for an Idempotency-Key. */
    public static class IdempotencyConflictException extends RuntimeException {
        public IdempotencyConflictException(String key) {
            super("Idempotency-Key " + key + " was used concurrently");
        }
    }

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
     * its chunks and, if given, the idempotency key. Either all of it becomes visible or none.
     *
     * @throws IdempotencyConflictException if another upload with the same key committed first
     */
    public void insert(StoredObject object, Map<String, String> metadata, List<Chunk> chunks, String idempotencyKey) {
        try {
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
                if (idempotencyKey != null) {
                    jdbc.sql("INSERT INTO idempotency_keys (key, object_id, size, created_at) VALUES (?, ?, ?, ?)")
                            .params(idempotencyKey, object.objectId(), object.size(), createdAt)
                            .update();
                }
                jdbc.sql("DELETE FROM upload_intents WHERE object_id = ?").param(object.objectId()).update();
            });
        } catch (DuplicateKeyException e) {
            throw new IdempotencyConflictException(idempotencyKey);
        }
    }

    public Optional<IdempotentUpload> findIdempotent(String key) {
        return jdbc.sql("SELECT key, object_id, size FROM idempotency_keys WHERE key = ?")
                .param(key)
                .query((rs, rowNum) -> new IdempotentUpload(rs.getString("key"), rs.getString("object_id"), rs.getLong("size")))
                .optional();
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

    public List<String> allObjectIds() {
        return jdbc.sql("SELECT object_id FROM objects").query(String.class).list();
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

    /**
     * Deletes the object (metadata, chunks and idempotency keys go with it, ON DELETE CASCADE) and
     * leaves a tombstone, in one transaction.
     */
    public boolean delete(String objectId, Instant now) {
        return Boolean.TRUE.equals(tx.execute(status -> {
            if (jdbc.sql("DELETE FROM objects WHERE object_id = ?").param(objectId).update() == 0) {
                return false;
            }
            jdbc.sql("INSERT OR IGNORE INTO deleted_objects (object_id, deleted_at) VALUES (?, ?)")
                    .params(objectId, now.toEpochMilli())
                    .update();
            return true;
        }));
    }

    /** Records that an upload is about to write chunks for {@code objectId}. Removed when it commits. */
    public void recordUploadIntent(String objectId, Instant now) {
        jdbc.sql("INSERT INTO upload_intents (object_id, started_at) VALUES (?, ?)")
                .params(objectId, now.toEpochMilli())
                .update();
    }

    /**
     * Object ids whose chunks are certainly garbage: uploads that never committed, and deleted
     * objects. Read at start-up, when no upload is in progress.
     */
    public List<String> garbageObjectIds() {
        return jdbc.sql("SELECT object_id FROM upload_intents UNION SELECT object_id FROM deleted_objects")
                .query(String.class)
                .list();
    }

    /** Objects whose upload started but never committed (their chunks are upload-failed garbage). */
    public List<String> uncommittedUploadIds() {
        return jdbc.sql("SELECT object_id FROM upload_intents").query(String.class).list();
    }

    /** Objects that were deleted (their chunks are deleted-object garbage). */
    public List<String> deletedObjectIds() {
        return jdbc.sql("SELECT object_id FROM deleted_objects").query(String.class).list();
    }
}
