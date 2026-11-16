package com.objectstore.controller.repository;

import com.objectstore.controller.domain.Chunk;
import com.objectstore.controller.domain.ReplicaLocation;
import com.objectstore.controller.domain.ReplicaStatus;
import com.objectstore.controller.domain.WorkerReplica;
import com.objectstore.controller.domain.WorkerStatus;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Chunks and their physical copies ({@code chunks} and {@code chunk_replicas}). */
@Repository
public class ChunkRepository {

    private static final RowMapper<Chunk> CHUNK = (rs, rowNum) -> new Chunk(
            rs.getString("chunk_id"),
            rs.getString("object_id"),
            rs.getInt("idx"),
            rs.getInt("size"),
            rs.getString("checksum"));

    private static final RowMapper<ReplicaLocation> LOCATION = (rs, rowNum) -> new ReplicaLocation(
            rs.getString("chunk_id"),
            rs.getString("worker_id"),
            rs.getString("worker_url"),
            ReplicaStatus.valueOf(rs.getString("status")),
            WorkerStatus.valueOf(rs.getString("worker_status")),
            Instant.ofEpochMilli(rs.getLong("updated_at")));

    private static final String SELECT_LOCATION = """
            SELECT r.chunk_id, r.worker_id, r.status, r.updated_at, w.url AS worker_url, w.status AS worker_status
            FROM chunk_replicas r
            JOIN workers w ON w.worker_id = r.worker_id
            """;

    /**
     * Counts good copies per chunk. Both conditions sit in the JOINs, not in a WHERE clause, so a
     * chunk with no good copy at all still appears, with a count of zero.
     */
    private static final String GOOD_COPIES_PER_CHUNK = """
            SELECT c.chunk_id
            FROM chunks c
            LEFT JOIN chunk_replicas r ON r.chunk_id = c.chunk_id AND r.status = 'OK'
            LEFT JOIN workers w ON w.worker_id = r.worker_id AND w.status = 'ALIVE'
            GROUP BY c.chunk_id
            """;

    private final JdbcClient jdbc;

    public ChunkRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    // ---- chunks ---------------------------------------------------------------------------

    public Optional<Chunk> findChunk(String chunkId) {
        return jdbc.sql("SELECT * FROM chunks WHERE chunk_id = ?").param(chunkId).query(CHUNK).optional();
    }

    /** The chunks of an object in the order they must be concatenated. */
    public List<Chunk> findChunksByObject(String objectId) {
        return jdbc.sql("SELECT * FROM chunks WHERE object_id = ? ORDER BY idx").param(objectId).query(CHUNK).list();
    }

    public long countChunks() {
        return jdbc.sql("SELECT COUNT(*) FROM chunks").query(Long.class).single();
    }

    // ---- copies ---------------------------------------------------------------------------

    public List<ReplicaLocation> findLocationsByObject(String objectId) {
        return jdbc.sql(SELECT_LOCATION + """
                        JOIN chunks c ON c.chunk_id = r.chunk_id
                        WHERE c.object_id = ?
                        ORDER BY c.idx, r.worker_id
                        """)
                .param(objectId)
                .query(LOCATION)
                .list();
    }

    public List<ReplicaLocation> findLocationsByChunk(String chunkId) {
        return jdbc.sql(SELECT_LOCATION + "WHERE r.chunk_id = ? ORDER BY r.worker_id")
                .param(chunkId)
                .query(LOCATION)
                .list();
    }

    public Optional<ReplicaLocation> findLocation(String chunkId, String workerId) {
        return jdbc.sql(SELECT_LOCATION + "WHERE r.chunk_id = ? AND r.worker_id = ?")
                .params(chunkId, workerId)
                .query(LOCATION)
                .optional();
    }

    /** Known-bad copies (CORRUPT or MISSING) on workers that are alive. */
    public List<ReplicaLocation> findStaleLocations() {
        return jdbc.sql(SELECT_LOCATION + """
                        WHERE r.status <> 'OK' AND w.status = 'ALIVE'
                        ORDER BY r.chunk_id, r.worker_id
                        """)
                .query(LOCATION)
                .list();
    }

    /** Every copy recorded for one worker together with the checksum and size its file should have. */
    public List<WorkerReplica> findReplicasOnWorker(String workerId) {
        return jdbc.sql("""
                        SELECT r.chunk_id, r.status, r.updated_at, c.checksum, c.size
                        FROM chunk_replicas r
                        JOIN chunks c ON c.chunk_id = r.chunk_id
                        WHERE r.worker_id = ?
                        ORDER BY r.chunk_id
                        """)
                .param(workerId)
                .query((rs, rowNum) -> new WorkerReplica(
                        rs.getString("chunk_id"),
                        ReplicaStatus.valueOf(rs.getString("status")),
                        Instant.ofEpochMilli(rs.getLong("updated_at")),
                        rs.getString("checksum"),
                        rs.getLong("size")))
                .list();
    }

    /** Ids of chunks with fewer than {@code replicationFactor} good copies. */
    public List<String> findUnderReplicated(int replicationFactor) {
        return jdbc.sql(GOOD_COPIES_PER_CHUNK + "HAVING COUNT(w.worker_id) < ? ORDER BY c.chunk_id")
                .param(replicationFactor)
                .query(String.class)
                .list();
    }

    /** Ids of chunks with more than {@code replicationFactor} good copies. */
    public List<String> findOverReplicated(int replicationFactor) {
        return jdbc.sql(GOOD_COPIES_PER_CHUNK + "HAVING COUNT(w.worker_id) > ? ORDER BY c.chunk_id")
                .param(replicationFactor)
                .query(String.class)
                .list();
    }

    /** Number of copy records per status (all workers, alive or not). */
    public Map<ReplicaStatus, Long> countReplicasByStatus() {
        Map<ReplicaStatus, Long> counts = new EnumMap<>(ReplicaStatus.class);
        for (ReplicaStatus status : ReplicaStatus.values()) {
            counts.put(status, 0L);
        }
        jdbc.sql("SELECT status, COUNT(*) AS n FROM chunk_replicas GROUP BY status")
                .query((rs, rowNum) -> counts.put(ReplicaStatus.valueOf(rs.getString("status")), rs.getLong("n")))
                .list();
        return counts;
    }

    public void insertReplica(String chunkId, String workerId, Instant now) {
        jdbc.sql("INSERT INTO chunk_replicas (chunk_id, worker_id, status, updated_at) VALUES (?, ?, 'OK', ?)")
                .params(chunkId, workerId, now.toEpochMilli())
                .update();
    }

    public boolean deleteReplica(String chunkId, String workerId) {
        return jdbc.sql("DELETE FROM chunk_replicas WHERE chunk_id = ? AND worker_id = ?")
                .params(chunkId, workerId)
                .update() > 0;
    }

    /** Sets the status of a copy. {@code updated_at} always moves forward, so a change is detectable. */
    public boolean updateStatus(String chunkId, String workerId, ReplicaStatus status, Instant now) {
        return jdbc.sql("""
                        UPDATE chunk_replicas SET status = ?, updated_at = MAX(?, updated_at + 1)
                        WHERE chunk_id = ? AND worker_id = ?
                        """)
                .params(status.name(), now.toEpochMilli(), chunkId, workerId)
                .update() > 0;
    }

    /**
     * Downgrades a copy from OK to {@code status}, but only if the record is still the one that
     * was read from: a copy that was replaced after {@code observedAt} is left alone.
     */
    public boolean downgradeIfStillOk(String chunkId, String workerId, ReplicaStatus status, Instant observedAt,
                                      Instant now) {
        return jdbc.sql("""
                        UPDATE chunk_replicas SET status = ?, updated_at = MAX(?, updated_at + 1)
                        WHERE chunk_id = ? AND worker_id = ? AND status = 'OK' AND updated_at <= ?
                        """)
                .params(status.name(), now.toEpochMilli(), chunkId, workerId, observedAt.toEpochMilli())
                .update() > 0;
    }
}
