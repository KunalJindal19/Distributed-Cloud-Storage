package com.objectstore.controller.repository;

import com.objectstore.controller.domain.Chunk;
import java.util.List;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** The {@code chunks} table: which pieces an object consists of. Where they are is not persisted. */
@Repository
public class ChunkRepository {

    private static final RowMapper<Chunk> CHUNK = (rs, rowNum) -> new Chunk(
            rs.getString("chunk_id"),
            rs.getString("object_id"),
            rs.getInt("idx"),
            rs.getInt("size"),
            rs.getString("checksum"));

    private final JdbcClient jdbc;

    public ChunkRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public List<Chunk> findAll() {
        return jdbc.sql("SELECT * FROM chunks ORDER BY object_id, idx").query(CHUNK).list();
    }

    /** The chunks of an object in the order they must be concatenated. */
    public List<Chunk> findChunksByObject(String objectId) {
        return jdbc.sql("SELECT * FROM chunks WHERE object_id = ? ORDER BY idx").param(objectId).query(CHUNK).list();
    }

    public long countChunks() {
        return jdbc.sql("SELECT COUNT(*) FROM chunks").query(Long.class).single();
    }
}
