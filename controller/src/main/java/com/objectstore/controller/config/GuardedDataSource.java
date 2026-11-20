package com.objectstore.controller.config;

import com.objectstore.controller.cluster.SqliteGuard;
import com.zaxxer.hikari.HikariDataSource;
import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;
import org.springframework.jdbc.datasource.DelegatingDataSource;

/** The metadata database's data source, checking {@link SqliteGuard} on every connection. */
public class GuardedDataSource extends DelegatingDataSource implements Closeable {

    private final HikariDataSource pool;

    public GuardedDataSource(HikariDataSource pool) {
        super(pool);
        this.pool = pool;
    }

    @Override
    public Connection getConnection() throws SQLException {
        SqliteGuard.check();
        return super.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        SqliteGuard.check();
        return super.getConnection(username, password);
    }

    @Override
    public void close() {
        pool.close();
    }
}
