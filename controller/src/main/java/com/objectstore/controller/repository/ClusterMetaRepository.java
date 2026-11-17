package com.objectstore.controller.repository;

import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionOperations;

/** The {@code cluster_meta} table: one row holding the cluster id. */
@Repository
public class ClusterMetaRepository {

    private static final String CLUSTER_ID = "cluster_id";

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
}
