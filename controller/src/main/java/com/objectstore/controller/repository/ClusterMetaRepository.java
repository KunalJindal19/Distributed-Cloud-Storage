package com.objectstore.controller.repository;

import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionOperations;

/** The {@code cluster_meta} table: the cluster id and the controller term (v5 §3.2). */
@Repository
public class ClusterMetaRepository {

    private static final String CLUSTER_ID = "cluster_id";
    private static final String TERM = "term";

    private final JdbcClient jdbc;
    private final TransactionOperations tx;

    public ClusterMetaRepository(JdbcClient jdbc, TransactionOperations tx) {
        this.jdbc = jdbc;
        this.tx = tx;
    }

    /** The cluster id, created durably on the first start. */
    public String clusterId() {
        return tx.execute(status -> {
            Optional<String> existing = jdbc.sql("SELECT value FROM cluster_meta WHERE key = ?")
                    .param(CLUSTER_ID).query(String.class).optional();
            if (existing.isPresent()) {
                return existing.get();
            }
            String created = UUID.randomUUID().toString();
            jdbc.sql("INSERT INTO cluster_meta (key, value) VALUES (?, ?)").params(CLUSTER_ID, created).update();
            return created;
        });
    }

    /**
     * Increments the controller term and commits it before returning, so every version this
     * controller run issues is larger than any version of an earlier run.
     */
    public long nextTerm() {
        return tx.execute(status -> {
            Optional<String> existing = jdbc.sql("SELECT value FROM cluster_meta WHERE key = ?")
                    .param(TERM).query(String.class).optional();
            long term = existing.map(Long::parseLong).orElse(0L) + 1;
            if (existing.isPresent()) {
                jdbc.sql("UPDATE cluster_meta SET value = ? WHERE key = ?").params(Long.toString(term), TERM).update();
            } else {
                jdbc.sql("INSERT INTO cluster_meta (key, value) VALUES (?, ?)").params(TERM, Long.toString(term)).update();
            }
            return term;
        });
    }
}
